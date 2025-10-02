# Teams Auto Attendant Data Export Script for Azure Automation Runbook
# This script gets all Teams auto attendants and writes the data to a SQL table

# Connect to Azure using Managed Identity
try {
    Connect-AzAccount -Identity -ErrorAction Stop
    Write-Output "Connected to Azure using Managed Identity"
} catch {
    $errorMessage = $_.Exception.Message
    Write-Error "Failed to connect to Azure: $errorMessage"
    throw
}

# Connect to Microsoft Teams
try {
    Connect-MicrosoftTeams -Identity -ErrorAction Stop
    Write-Output "Connected to Microsoft Teams"
} catch {
    $errorMessage = $_.Exception.Message
    Write-Error "Failed to connect to Microsoft Teams: $errorMessage"
    throw
}

# SQL Info
$SQLServer = "msoevendor.database.windows.net"
$Database = "CEProjectData"
$Table = "dbo.msoe_teams_phone_system_aas"

# Get access token for Azure SQL
try {
    $SQLAccessTokenObj = Get-AzAccessToken -ResourceUrl "https://database.windows.net"
    $SQLAccessTokenText = $SQLAccessTokenObj.Token
    Write-Output "SQL token acquired"
} catch {
    $errorMessage = $_.Exception.Message
    Write-Error "Could not get SQL access token - $errorMessage"
    throw
}

# Get access token for Microsoft Graph API
try {
    $GraphAccessToken = Get-AzAccessToken -ResourceUrl "https://graph.microsoft.com" 
    $GraphToken = $GraphAccessToken.Token
    Write-Output "Graph API token acquired"
} catch {
    $errorMessage = $_.Exception.Message
    Write-Error "Could not get Graph API token: $errorMessage"
    throw
}

# Initialize SQL Connection
try {
    $SQLConnection = New-Object System.Data.SqlClient.SqlConnection
    $SQLConnection.ConnectionString = "Server=$SQLServer;Database=$Database;Integrated Security=False;Encrypt=True;TrustServerCertificate=False;"
    $SQLConnection.AccessToken = $SQLAccessTokenText
    $SQLConnection.Open()
    Write-Output "Connected to Azure SQL"
} catch {
    $errorMessage = $_.Exception.Message
    Write-Error "Could not connect to SQL DB: $errorMessage"
    throw
}

# Initialize Graph API Request Headers
$GraphHeaders = @{
    "Authorization" = "Bearer $GraphToken"
    "Content-Type" = "application/json"
}

# Function to truncate the SQL table
function Clear-SQLTable {
    param (
        [System.Data.SqlClient.SqlConnection]$Connection,
        [string]$TableName
    )
    
    try {
        $truncateCommand = $Connection.CreateCommand()
        $truncateCommand.CommandText = "DELETE FROM $TableName"
        $rowsAffected = $truncateCommand.ExecuteNonQuery()
        Write-Output "Table $TableName cleared. $rowsAffected rows deleted."
        return $true
    } catch {
        $errorMessage = $_.Exception.Message
        Write-Error "Failed to clear table $TableName - $errorMessage"
        return $false
    }
}

# Function to extract Target ID as string
function Get-TargetIdAsString {
    param(
        [Parameter(Mandatory=$false)]
        [object]$Target
    )
    
    if ($null -eq $Target) {
        return "N/A"
    }
    
    try {
        # Check if it's a Target object with an Id property
        if ($Target.PSObject.Properties.Name -contains "Id") {
            return $Target.Id.ToString()
        } elseif ($Target -is [string]) {
            # If it's already a string, just return it
            return $Target
        } else {
            # If we can't determine the type, convert to string
            return $Target.ToString()
        }
    }
    catch {
        Write-Output "Error extracting Target ID: $_"
        return "N/A"
    }
}

# Function to safely convert any value to string
function ConvertTo-SafeString {
    param(
        [Parameter(Mandatory=$false)]
        [object]$Value
    )
    
    if ($null -eq $Value) {
        return "N/A"
    }
    
    try {
        if ($Value -is [TimeSpan]) {
            return $Value.ToString()
        } elseif ($Value -is [string]) {
            if ([string]::IsNullOrEmpty($Value)) {
                return "N/A"
            }
            return $Value
        } else {
            return $Value.ToString()
        }
    }
    catch {
        Write-Output "Error converting value to string: $_"
        return "N/A"
    }
}

# Function to safely truncate text to fit database column limits
function ConvertTo-SafeTruncatedString {
    param(
        [Parameter(Mandatory=$false)]
        [object]$Value,
        [int]$MaxLength = 255
    )
    
    if ($null -eq $Value) {
        return "N/A"
    }
    
    try {
        $stringValue = ""
        
        # Handle different value types
        if ($Value -is [Array]) {
            # Convert array to joined string
            $stringValue = ($Value | Where-Object { $_ -ne $null } | ForEach-Object { $_.ToString() }) -join "; "
            Write-Output "  Info: Converted array to string: '$stringValue'"
        } elseif ($Value -is [TimeSpan]) {
            $stringValue = $Value.ToString()
        } elseif ($Value -is [string]) {
            if ([string]::IsNullOrEmpty($Value)) {
                return "N/A"
            }
            $stringValue = $Value
        } elseif ($Value -is [System.Collections.IEnumerable] -and $Value -isnot [string]) {
            # Handle other collections
            $items = @()
            foreach ($item in $Value) {
                if ($item -ne $null) {
                    $items += $item.ToString()
                }
            }
            $stringValue = $items -join "; "
            Write-Output "  Info: Converted collection to string: '$stringValue'"
        } else {
            $stringValue = $Value.ToString()
        }
        
        # Ensure we have a string value
        if ([string]::IsNullOrEmpty($stringValue)) {
            return "N/A"
        }
        
        # Truncate if longer than MaxLength
        if ($stringValue.Length -gt $MaxLength) {
            # Try to break at last complete word within limit
            $truncateAt = $MaxLength - 3  # Reserve space for "..."
            $lastSpace = $stringValue.LastIndexOf(' ', $truncateAt)
            
            if ($lastSpace -gt ($truncateAt * 0.8)) {  # If we can break at 80% or more of desired length
                $truncated = $stringValue.Substring(0, $lastSpace) + "..."
            } else {
                $truncated = $stringValue.Substring(0, $truncateAt) + "..."
            }
            
            Write-Output "  Warning: Text truncated from $($stringValue.Length) to $($truncated.Length) characters"
            return $truncated
        }
        
        return $stringValue
    }
    catch {
        Write-Output "Error converting value to truncated string: $_"
        Write-Output "Value type: $($Value.GetType().Name)"
        return "N/A"
    }
}

# Function to get resource account display name from GUID
function Get-ResourceAccountDisplayName {
    param(
        [string]$AccountGUID,
        [array]$ResourceAccounts,
        [array]$AllAutoAttendants = @(),
        [array]$AllCallQueues = @(),
        [hashtable]$GraphHeaders = @{}
    )
    
    if ([string]::IsNullOrEmpty($AccountGUID) -or $AccountGUID -eq "N/A") {
        return "N/A"
    }
    
    Write-Host "    Attempting to resolve GUID: $AccountGUID"
    
    try {
        # Method 1: Try to find a matching resource account by ObjectId/Id
        Write-Host "    Method 1: Searching resource accounts..."
        $matchedAccount = $null
        foreach ($account in $ResourceAccounts) {
            $id = $null
            
            # Check different possible ID properties
            if ($account.PSObject.Properties.Name -contains "ObjectId") { 
                $id = $account.ObjectId 
            } elseif ($account.PSObject.Properties.Name -contains "id") { 
                $id = $account.id 
            } elseif ($account.PSObject.Properties.Name -contains "Identity") { 
                $id = $account.Identity 
            } elseif ($account.PSObject.Properties.Name -contains "Guid") { 
                $id = $account.Guid 
            }
            
            if ($id -eq $AccountGUID) {
                $matchedAccount = $account
                Write-Host "    Found matching resource account by ID"
                break
            }
        }
        
        if ($matchedAccount) {
            $displayName = $null
            if ($matchedAccount.PSObject.Properties.Name -contains "DisplayName") { 
                $displayName = $matchedAccount.DisplayName 
            } elseif ($matchedAccount.PSObject.Properties.Name -contains "displayName") { 
                $displayName = $matchedAccount.displayName 
            } elseif ($matchedAccount.PSObject.Properties.Name -contains "Name") { 
                $displayName = $matchedAccount.Name 
            }
            
            if (-not [string]::IsNullOrEmpty($displayName)) {
                Write-Host "    Resolved GUID '$AccountGUID' to display name: '$displayName'"
                return [string]$displayName
            }
        }
        
        # Method 2: Try to find it as another auto attendant
        if ($AllAutoAttendants.Count -gt 0) {
            Write-Host "    Method 2: Searching auto attendants..."
            foreach ($aa in $AllAutoAttendants) {
                if ($aa.Identity -eq $AccountGUID) {
                    Write-Host "    Resolved GUID '$AccountGUID' to auto attendant name: '$($aa.Name)'"
                    return [string]$aa.Name
                }
            }
        }
        
        # Method 3: Try to find it in pre-loaded call queues
        if ($AllCallQueues.Count -gt 0) {
            Write-Host "    Method 3: Searching pre-loaded call queues..."
            foreach ($queue in $AllCallQueues) {
                if ($queue.Identity -eq $AccountGUID) {
                    Write-Host "    Resolved GUID '$AccountGUID' to call queue name: '$($queue.Name)'"
                    return [string]$queue.Name
                }
            }
        }
        
        # Method 4: Try to look it up as a call queue directly
        Write-Host "    Method 4: Direct call queue lookup..."
        try {
            $callQueue = Get-CsCallQueue -Identity $AccountGUID -ErrorAction SilentlyContinue
            if ($callQueue -and $callQueue.Name) {
                Write-Host "    Resolved GUID '$AccountGUID' to call queue name: '$($callQueue.Name)'"
                return [string]$callQueue.Name
            }
        } catch {
            Write-Host "    Call queue lookup failed: $_"
        }
        
        # Method 5: Try Graph API direct lookup (if headers provided)
        if ($GraphHeaders.Count -gt 0) {
            Write-Host "    Method 5: Trying Graph API lookup..."
            try {
                $userGraphUri = "https://graph.microsoft.com/v1.0/users/$AccountGUID"
                $userResponse = Invoke-RestMethod -Uri $userGraphUri -Headers $GraphHeaders -Method Get -ErrorAction SilentlyContinue
                
                if ($userResponse) {
                    $displayName = $null
                    if ($userResponse.PSObject.Properties.Name -contains "displayName") {
                        $displayName = $userResponse.displayName
                    } elseif ($userResponse.PSObject.Properties.Name -contains "userPrincipalName") {
                        $displayName = $userResponse.userPrincipalName
                    }
                    
                    if (-not [string]::IsNullOrEmpty($displayName)) {
                        Write-Host "    Resolved GUID '$AccountGUID' via Graph API to: '$displayName'"
                        return [string]$displayName
                    }
                }
            } catch {
                Write-Host "    Graph API lookup failed: $_"
            }
        }
        
        # Method 6: Try using Get-CsOnlineUser directly with the GUID
        Write-Host "    Method 6: Direct user lookup..."
        try {
            $user = Get-CsOnlineUser -Identity $AccountGUID -ErrorAction SilentlyContinue
            if ($user -and $user.DisplayName) {
                Write-Host "    Resolved GUID '$AccountGUID' via Get-CsOnlineUser to: '$($user.DisplayName)'"
                return [string]$user.DisplayName
            }
        } catch {
            Write-Host "    Direct user lookup failed: $_"
        }
        
        Write-Host "    Could not resolve GUID '$AccountGUID' using any method"
        return "Unknown Account ($AccountGUID)"
        
    } catch {
        Write-Host "    Error resolving GUID '$AccountGUID': $_"
        return "Unknown Account ($AccountGUID)"
    }
}

# Function to get shared voicemail group display name from GUID
function Get-SharedVoicemailGroupName {
    param(
        [string]$GroupGUID,
        [hashtable]$GraphHeaders = @{}
    )
    
    if ([string]::IsNullOrEmpty($GroupGUID) -or $GroupGUID -eq "N/A") {
        return "N/A"
    }
    
    Write-Host "    Attempting to resolve shared voicemail group GUID: $GroupGUID"
    
    try {
        if ($GraphHeaders.Count -gt 0) {
            # Try Graph API groups lookup
            Write-Host "    Trying Graph API groups lookup..."
            try {
                $groupGraphUri = "https://graph.microsoft.com/v1.0/groups/$GroupGUID"
                $groupResponse = Invoke-RestMethod -Uri $groupGraphUri -Headers $GraphHeaders -Method Get -ErrorAction SilentlyContinue
                
                if ($groupResponse -and $groupResponse.displayName) {
                    Write-Host "    Resolved group GUID '$GroupGUID' to: '$($groupResponse.displayName)'"
                    return [string]$groupResponse.displayName
                }
            } catch {
                Write-Host "    Graph API groups lookup failed: $_"
            }
            
            # Try Graph API directory objects lookup as fallback
            Write-Host "    Trying Graph API directory objects lookup..."
            try {
                $dirObjectUri = "https://graph.microsoft.com/v1.0/directoryObjects/$GroupGUID"
                $dirObjectResponse = Invoke-RestMethod -Uri $dirObjectUri -Headers $GraphHeaders -Method Get -ErrorAction SilentlyContinue
                
                if ($dirObjectResponse -and $dirObjectResponse.displayName) {
                    Write-Host "    Resolved group GUID '$GroupGUID' via directory objects to: '$($dirObjectResponse.displayName)'"
                    return [string]$dirObjectResponse.displayName
                }
            } catch {
                Write-Host "    Graph API directory objects lookup failed: $_"
            }
        }
        
        Write-Host "    Could not resolve shared voicemail group GUID '$GroupGUID'"
        return "Unknown Group ($GroupGUID)"
        
    } catch {
        Write-Host "    Error resolving shared voicemail group GUID '$GroupGUID': $_"
        return "Unknown Group ($GroupGUID)"
    }
}

# Clear the table before inserting new data
$tableCleared = Clear-SQLTable -Connection $SQLConnection -TableName $Table
if (-not $tableCleared) {
    Write-Warning "Could not clear the table. Proceeding with data insertion anyway."
}

# Get all Teams resource accounts (both auto attendants and call queues)
Write-Output "Retrieving all Teams resource accounts..."
try {
    # First try using Get-CsOnlineUser with the department filter
    $resourceAccounts = Get-CsOnlineUser -Filter "Department -eq 'Microsoft Communication Application Instance'" -ErrorAction SilentlyContinue
    
    if ($resourceAccounts -and $resourceAccounts.Count -gt 0) {
        Write-Output "Found $($resourceAccounts.Count) Teams resource accounts using Get-CsOnlineUser"
    } else {
        # Fallback to Graph API if the cmdlet fails
        Write-Output "No resource accounts found with Get-CsOnlineUser. Trying Graph API..."
        
        $filter = "department eq 'Microsoft Communication Application Instance'"
        $resourceAccountsUri = "https://graph.microsoft.com/v1.0/users?`$filter=$([System.Web.HttpUtility]::UrlEncode($filter))&`$top=999"
        $response = Invoke-RestMethod -Uri $resourceAccountsUri -Headers $GraphHeaders -Method Get
        
        if ($response -and $response.value) {
            $resourceAccounts = $response.value
            Write-Output "Found $($resourceAccounts.Count) Teams resource accounts via Graph API"
        } else {
            # Final fallback - try to get application instances
            Write-Output "No resource accounts found with Graph API. Trying to get application instances..."
            $resourceAccounts = Get-CsOnlineApplicationInstance -ErrorAction SilentlyContinue
            if ($resourceAccounts) {
                Write-Output "Found $($resourceAccounts.Count) application instances"
            } else {
                Write-Output "No application instances found. Proceeding with empty resource account list."
                $resourceAccounts = @()
            }
        }
    }
} catch {
    Write-Output "Error retrieving resource accounts: $_"
    $resourceAccounts = @()
}

# Get all call queues for better GUID resolution
Write-Output "Retrieving all call queues for GUID resolution..."
try {
    $allCallQueues = Get-CsCallQueue -ErrorAction SilentlyContinue
    if ($allCallQueues) {
        Write-Output "Found $($allCallQueues.Count) call queues"
        # Show sample for debugging
        for ($i = 0; $i -lt [Math]::Min(3, $allCallQueues.Count); $i++) {
            Write-Output "  Queue $($i+1): Name='$($allCallQueues[$i].Name)', Identity='$($allCallQueues[$i].Identity)'"
        }
    } else {
        Write-Output "No call queues found"
        $allCallQueues = @()
    }
} catch {
    Write-Output "Error retrieving call queues: $_"
    $allCallQueues = @()
}

# Create a lookup table for resource accounts by display name
$resourceAccountsByName = @{}
foreach ($account in $resourceAccounts) {
    # Handle different property names based on whether we got objects from PowerShell cmdlets or Graph API
    $displayName = if ($account.PSObject.Properties.Name -contains "DisplayName") { 
        $account.DisplayName 
    } elseif ($account.PSObject.Properties.Name -contains "displayName") { 
        $account.displayName 
    } else { 
        $null 
    }
    
    $upn = if ($account.PSObject.Properties.Name -contains "UserPrincipalName") { 
        $account.UserPrincipalName 
    } elseif ($account.PSObject.Properties.Name -contains "userPrincipalName") { 
        $account.userPrincipalName 
    } else { 
        $null 
    }
    
    if (-not [string]::IsNullOrEmpty($displayName) -and -not [string]::IsNullOrEmpty($upn)) {
        $resourceAccountsByName[$displayName] = $account
    }
}
Write-Output "Created lookup table with $($resourceAccountsByName.Count) resource accounts by display name"

# STATIC MAPPING SECTION - Add specific mappings here
Write-Output "Applying static mappings for specific auto attendants..."

# Static mapping for "Call Demo Lab" -> "Call Demo Lab AA"
$staticMappings = @{
    "Call Demo Lab" = "Call Demo Lab AA"
}

# Apply static mappings by finding the resource account and adding it to our lookup
foreach ($aaName in $staticMappings.Keys) {
    $targetResourceAccountName = $staticMappings[$aaName]
    Write-Output "Applying static mapping: '$aaName' -> '$targetResourceAccountName'"
    
    # Find the target resource account
    $targetAccount = $null
    foreach ($account in $resourceAccounts) {
        $displayName = if ($account.PSObject.Properties.Name -contains "DisplayName") { 
            $account.DisplayName 
        } elseif ($account.PSObject.Properties.Name -contains "displayName") { 
            $account.displayName 
        } else { 
            $null 
        }
        
        if ($displayName -eq $targetResourceAccountName) {
            $targetAccount = $account
            Write-Output "  Found target resource account: '$targetResourceAccountName'"
            break
        }
    }
    
    if ($targetAccount) {
        # Override or add the mapping
        $resourceAccountsByName[$aaName] = $targetAccount
        Write-Output "  Static mapping applied successfully: '$aaName' will use resource account '$targetResourceAccountName'"
    } else {
        Write-Warning "  Could not find resource account '$targetResourceAccountName' for static mapping. Mapping will be skipped."
    }
}

Write-Output "Static mappings complete. Updated lookup table has $($resourceAccountsByName.Count) entries."

# Retrieve all auto attendants
Write-Output "Retrieving all auto attendants..."
try {
    $autoAttendants = Get-CsAutoAttendant -ErrorAction Stop
    Write-Output "Found $($autoAttendants.Count) auto attendants to process"
    
    # Additional debugging - show some auto attendant IDs
    Write-Output "Sample auto attendant identities for debugging:"
    for ($i = 0; $i -lt [Math]::Min(3, $autoAttendants.Count); $i++) {
        Write-Output "  AA $($i+1): Name='$($autoAttendants[$i].Name)', Identity='$($autoAttendants[$i].Identity)'"
    }
    
    $processed = 0
    
    foreach ($aa in $autoAttendants) {
        Write-Output "Processing auto attendant: '$($aa.Name)'"
        
        try {
            # Initialize values
            $resourceAccountUPN = "N/A"
            $redirectResourceAccount = "N/A"
            $sharedVoicemailGroup = "N/A"
            $phoneNumber = "N/A"
            $businessHoursStartTime = "N/A"
            $businessHoursEndTime = "N/A"
            $businessDays = "N/A"
            $businessHoursGreetingText = "N/A"
            $afterHoursGreetingText = "N/A"
            $language = if ($aa.LanguageId) { $aa.LanguageId } else { "en-US" }
            $timeZone = if ($aa.TimeZoneId) { $aa.TimeZoneId } else { "UTC" }
            $enableVoiceResponse = $aa.EnableVoiceResponse
            
            # Look up resource account by display name - this now includes our static mappings
            if ($resourceAccountsByName.ContainsKey($aa.Name)) {
                $matchedAccount = $resourceAccountsByName[$aa.Name]
                
                # Get UPN based on the object type
                if ($matchedAccount.PSObject.Properties.Name -contains "UserPrincipalName") {
                    $resourceAccountUPN = $matchedAccount.UserPrincipalName
                } elseif ($matchedAccount.PSObject.Properties.Name -contains "userPrincipalName") {
                    $resourceAccountUPN = $matchedAccount.userPrincipalName
                }
                
                Write-Output "  Found resource account with matching display name: $resourceAccountUPN"
                
                # Check if this was a static mapping
                if ($staticMappings.ContainsKey($aa.Name)) {
                    Write-Output "  NOTE: This mapping was applied via static configuration"
                }
                
                # Try to get phone number
                if ($matchedAccount.PSObject.Properties.Name -contains "LineURI" -and $matchedAccount.LineURI) {
                    $phoneNumber = $matchedAccount.LineURI
                    Write-Output "  Found phone number from LineURI: $phoneNumber"
                } elseif ($matchedAccount.PSObject.Properties.Name -contains "PhoneNumber" -and $matchedAccount.PhoneNumber) {
                    $phoneNumber = $matchedAccount.PhoneNumber
                    Write-Output "  Found phone number from PhoneNumber: $phoneNumber"
                } elseif ($matchedAccount.PSObject.Properties.Name -contains "businessPhones" -and 
                        $matchedAccount.businessPhones -and 
                        $matchedAccount.businessPhones.Count -gt 0) {
                    $phoneNumber = $matchedAccount.businessPhones[0]
                    Write-Output "  Found phone number from businessPhones: $phoneNumber"
                } elseif ($matchedAccount.PSObject.Properties.Name -contains "mobilePhone" -and $matchedAccount.mobilePhone) {
                    $phoneNumber = $matchedAccount.mobilePhone
                    Write-Output "  Found phone number from mobilePhone: $phoneNumber"
                }
            } else {
                Write-Output "  No resource account found with matching display name"
            }
            
            # Get business hours info
            if ($aa.CallHandlingAssociations -and $aa.CallHandlingAssociations.Count -gt 0) {
                foreach ($assoc in $aa.CallHandlingAssociations) {
                    if ($assoc.Type -eq "AfterHours") {
                        $schedule = Get-CsOnlineSchedule -Id $assoc.ScheduleId -ErrorAction SilentlyContinue
                        if ($schedule -and $schedule.WeeklyRecurrentSchedule) {
                            $daysArray = @()
                            $timeRange = $null
                            
                            # Extract days and time
                            if ($schedule.WeeklyRecurrentSchedule.MondayHours.Count -gt 0) { 
                                $daysArray += "Monday"
                                if (-not $timeRange) { $timeRange = $schedule.WeeklyRecurrentSchedule.MondayHours[0] }
                            }
                            if ($schedule.WeeklyRecurrentSchedule.TuesdayHours.Count -gt 0) { 
                                $daysArray += "Tuesday"
                                if (-not $timeRange) { $timeRange = $schedule.WeeklyRecurrentSchedule.TuesdayHours[0] }
                            }
                            if ($schedule.WeeklyRecurrentSchedule.WednesdayHours.Count -gt 0) { 
                                $daysArray += "Wednesday"
                                if (-not $timeRange) { $timeRange = $schedule.WeeklyRecurrentSchedule.WednesdayHours[0] }
                            }
                            if ($schedule.WeeklyRecurrentSchedule.ThursdayHours.Count -gt 0) { 
                                $daysArray += "Thursday"
                                if (-not $timeRange) { $timeRange = $schedule.WeeklyRecurrentSchedule.ThursdayHours[0] }
                            }
                            if ($schedule.WeeklyRecurrentSchedule.FridayHours.Count -gt 0) { 
                                $daysArray += "Friday"
                                if (-not $timeRange) { $timeRange = $schedule.WeeklyRecurrentSchedule.FridayHours[0] }
                            }
                            if ($schedule.WeeklyRecurrentSchedule.SaturdayHours.Count -gt 0) { 
                                $daysArray += "Saturday"
                                if (-not $timeRange) { $timeRange = $schedule.WeeklyRecurrentSchedule.SaturdayHours[0] }
                            }
                            if ($schedule.WeeklyRecurrentSchedule.SundayHours.Count -gt 0) { 
                                $daysArray += "Sunday"
                                if (-not $timeRange) { $timeRange = $schedule.WeeklyRecurrentSchedule.SundayHours[0] }
                            }
                            
                            if ($timeRange) {
                                $businessHoursStartTime = ConvertTo-SafeString -Value $timeRange.Start
                                $businessHoursEndTime = ConvertTo-SafeString -Value $timeRange.End
                            }
                            
                            $businessDays = $daysArray -join ","
                        }
                        
                        # Find after hours call flow
                        $afterHoursFlow = $aa.CallFlows | Where-Object { $_.Id -eq $assoc.CallFlowId }
                        if ($afterHoursFlow -and $afterHoursFlow.Greetings -and $afterHoursFlow.Greetings.Count -gt 0) {
                            foreach ($greeting in $afterHoursFlow.Greetings) {
                                if ($greeting.TextToSpeechPrompt) {
                                    $afterHoursGreetingText = $greeting.TextToSpeechPrompt
                                    break
                                }
                            }
                            
                            # Check for shared voicemail target
                            if ($afterHoursFlow.Menu -and $afterHoursFlow.Menu.MenuOptions) {
                                foreach ($option in $afterHoursFlow.Menu.MenuOptions) {
                                    if ($option.Action -eq "TransferCallToTarget" -and $option.CallTarget.Type -eq "SharedVoicemail") {
                                        $sharedVoicemailGroupGUID = Get-TargetIdAsString -Target $option.CallTarget.Id
                                        Write-Output "  Found shared voicemail group GUID: $sharedVoicemailGroupGUID"
                                        Write-Output "  Attempting to resolve shared voicemail group GUID to group name..."
                                        
                                        # Convert GUID to group name using our new function
                                        $sharedVoicemailGroup = Get-SharedVoicemailGroupName -GroupGUID $sharedVoicemailGroupGUID -GraphHeaders $GraphHeaders
                                        Write-Output "  Shared voicemail group resolved to: $sharedVoicemailGroup"
                                        break
                                    }
                                }
                            }
                        }
                    }
                }
            }
            
            # Get default call flow info for redirect resource account and greeting
            if ($aa.DefaultCallFlow -and $aa.DefaultCallFlow.Greetings -and $aa.DefaultCallFlow.Greetings.Count -gt 0) {
                foreach ($greeting in $aa.DefaultCallFlow.Greetings) {
                    if ($greeting.TextToSpeechPrompt) {
                        $businessHoursGreetingText = $greeting.TextToSpeechPrompt
                        break
                    }
                }
                
                # Find redirect target and convert GUID to display name
                if ($aa.DefaultCallFlow.Menu -and $aa.DefaultCallFlow.Menu.MenuOptions) {
                    foreach ($option in $aa.DefaultCallFlow.Menu.MenuOptions) {
                        if ($option.Action -eq "TransferCallToTarget" -and $option.CallTarget.Type -eq "ApplicationEndpoint") {
                            $appId = Get-TargetIdAsString -Target $option.CallTarget.Id
                            Write-Output "  Found redirect target GUID: $appId"
                            Write-Output "  Target Type: $($option.CallTarget.Type)"
                            Write-Output "  Action: $($option.Action)"
                            Write-Output "  Attempting to resolve this GUID to a display name..."
                            
                            # Convert GUID to display name using our improved function
                            $redirectResourceAccount = Get-ResourceAccountDisplayName -AccountGUID $appId -ResourceAccounts $resourceAccounts -AllAutoAttendants $autoAttendants -AllCallQueues $allCallQueues -GraphHeaders $GraphHeaders
                            Write-Output "  Redirect resource account resolved to: $redirectResourceAccount"
                            break
                        }
                    }
                }
            }
            
            # Get authorized users as a string
            $authorizedUsersArray = @()
            if ($aa.AuthorizedUsers -and $aa.AuthorizedUsers.Count -gt 0) {
                foreach ($userId in $aa.AuthorizedUsers) {
                    # Try to find user in our resource accounts first
                    $authorizedUser = $null
                    foreach ($account in $resourceAccounts) {
                        $id = if ($account.PSObject.Properties.Name -contains "ObjectId") { 
                            $account.ObjectId 
                        } elseif ($account.PSObject.Properties.Name -contains "id") { 
                            $account.id 
                        } else { 
                            $null 
                        }
                        
                        if ($id -eq $userId) {
                            $authorizedUser = $account
                            break
                        }
                    }
                    
                    if ($authorizedUser) {
                        if ($authorizedUser.PSObject.Properties.Name -contains "UserPrincipalName") {
                            $authorizedUsersArray += $authorizedUser.UserPrincipalName
                        } elseif ($authorizedUser.PSObject.Properties.Name -contains "userPrincipalName") {
                            $authorizedUsersArray += $authorizedUser.userPrincipalName
                        } else {
                            $authorizedUsersArray += $userId
                        }
                    } else {
                        # Fallback to direct Graph API request if not found in our collection
                        try {
                            $userGraphUri = "https://graph.microsoft.com/v1.0/users/$userId"
                            $userResponse = Invoke-RestMethod -Uri $userGraphUri -Headers $GraphHeaders -Method Get -ErrorAction SilentlyContinue
                            
                            if ($userResponse -and $userResponse.userPrincipalName) {
                                $authorizedUsersArray += $userResponse.userPrincipalName
                            } else {
                                $authorizedUsersArray += $userId
                            }
                        }
                        catch {
                            # If Graph API fails, use the ID
                            $authorizedUsersArray += $userId
                        }
                    }
                }
            }
            $authorizedUsers = $authorizedUsersArray -join ";"
            
            # Ensure we have values for all parameters (not null) with truncation - Updated for 1000 char limit
            $resourceAccountUPN = ConvertTo-SafeTruncatedString -Value $resourceAccountUPN -MaxLength 100
            $redirectResourceAccount = ConvertTo-SafeTruncatedString -Value $redirectResourceAccount -MaxLength 100
            $sharedVoicemailGroup = ConvertTo-SafeTruncatedString -Value $sharedVoicemailGroup -MaxLength 100
            $businessHoursGreetingText = ConvertTo-SafeTruncatedString -Value $businessHoursGreetingText -MaxLength 1000
            $afterHoursGreetingText = ConvertTo-SafeTruncatedString -Value $afterHoursGreetingText -MaxLength 1000
            $timeZone = ConvertTo-SafeTruncatedString -Value $timeZone -MaxLength 50
            $language = ConvertTo-SafeTruncatedString -Value $language -MaxLength 20
            $businessHoursStartTime = ConvertTo-SafeTruncatedString -Value $businessHoursStartTime -MaxLength 20
            $businessHoursEndTime = ConvertTo-SafeTruncatedString -Value $businessHoursEndTime -MaxLength 20
            $businessDays = ConvertTo-SafeTruncatedString -Value $businessDays -MaxLength 100
            $authorizedUsers = ConvertTo-SafeTruncatedString -Value $authorizedUsers -MaxLength 500
            $phoneNumber = ConvertTo-SafeTruncatedString -Value $phoneNumber -MaxLength 50
            
            # Convert boolean to integer for SQL
            $enableVoiceResponseValue = if ($enableVoiceResponse -eq $true) { 1 } else { 0 }
            
            # Display time values for debugging
            Write-Output "  Business Hours Start Time: $businessHoursStartTime"
            Write-Output "  Business Hours End Time: $businessHoursEndTime"
            
            # Debug: Check all parameter types before SQL insert
            Write-Output "  Debug: Parameter types for '$($aa.Name)':"
            Write-Output "    AA_Name: $($aa.Name.GetType().Name) = '$($aa.Name)'"
            Write-Output "    ResourceAccountUPN: $($resourceAccountUPN.GetType().Name) = '$resourceAccountUPN'"
            Write-Output "    RedirectResourceAccount: $($redirectResourceAccount.GetType().Name) = '$redirectResourceAccount'"
            Write-Output "    SharedVoicemailGroup: $($sharedVoicemailGroup.GetType().Name) = '$sharedVoicemailGroup'"
            Write-Output "    BusinessHoursGreetingText length: $($businessHoursGreetingText.Length)"
            Write-Output "    AfterHoursGreetingText length: $($afterHoursGreetingText.Length)"
            Write-Output "    AuthorizedUsers: $($authorizedUsers.GetType().Name) = '$authorizedUsers'"
            Write-Output "    PhoneNumber: $($phoneNumber.GetType().Name) = '$phoneNumber'"
            
            # Insert SQL command
            $query = "INSERT INTO $Table (AA_Name, ResourceAccountUPN, RedirectResourceAccount, " +
                     "SharedVoicemailGroup, BusinessHoursGreetingText, AfterHoursGreetingText, " +
                     "TimeZone, Language, BusinessHoursStartTime, BusinessHoursEndTime, " +
                     "BusinessDays, EnableVoiceResponse, AuthorizedUsers, PhoneNumber) " +
                     "VALUES (@AA_Name, @ResourceAccountUPN, @RedirectResourceAccount, " +
                     "@SharedVoicemailGroup, @BusinessHoursGreetingText, @AfterHoursGreetingText, " +
                     "@TimeZone, @Language, @BusinessHoursStartTime, @BusinessHoursEndTime, " +
                     "@BusinessDays, @EnableVoiceResponse, @AuthorizedUsers, @PhoneNumber)"

            # Add parameters using explicit array-safe conversion and parameter creation
            # Force all values to be strings and add explicit parameter creation
            $safeAAName = if ($aa.Name -is [Array]) { ($aa.Name -join "; ") } else { [string]$aa.Name }
            $safeResourceAccountUPN = if ($resourceAccountUPN -is [Array]) { ($resourceAccountUPN -join "; ") } else { [string]$resourceAccountUPN }
            $safeRedirectResourceAccount = if ($redirectResourceAccount -is [Array]) { ($redirectResourceAccount -join "; ") } else { [string]$redirectResourceAccount }
            $safeSharedVoicemailGroup = if ($sharedVoicemailGroup -is [Array]) { ($sharedVoicemailGroup -join "; ") } else { [string]$sharedVoicemailGroup }
            $safeBusinessHoursGreetingText = if ($businessHoursGreetingText -is [Array]) { ($businessHoursGreetingText -join "; ") } else { [string]$businessHoursGreetingText }
            $safeAfterHoursGreetingText = if ($afterHoursGreetingText -is [Array]) { ($afterHoursGreetingText -join "; ") } else { [string]$afterHoursGreetingText }
            $safeTimeZone = if ($timeZone -is [Array]) { ($timeZone -join "; ") } else { [string]$timeZone }
            $safeLanguage = if ($language -is [Array]) { ($language -join "; ") } else { [string]$language }
            $safeBusinessHoursStartTime = if ($businessHoursStartTime -is [Array]) { ($businessHoursStartTime -join "; ") } else { [string]$businessHoursStartTime }
            $safeBusinessHoursEndTime = if ($businessHoursEndTime -is [Array]) { ($businessHoursEndTime -join "; ") } else { [string]$businessHoursEndTime }
            $safeBusinessDays = if ($businessDays -is [Array]) { ($businessDays -join "; ") } else { [string]$businessDays }
            $safeAuthorizedUsers = if ($authorizedUsers -is [Array]) { ($authorizedUsers -join "; ") } else { [string]$authorizedUsers }
            $safePhoneNumber = if ($phoneNumber -is [Array]) { ($phoneNumber -join "; ") } else { [string]$phoneNumber }
            $safeEnableVoiceResponse = [int]$enableVoiceResponseValue

            Write-Output "  Final check - All parameters are now strings (or int for EnableVoiceResponse)"

            $cmd = $SQLConnection.CreateCommand()
            $cmd.CommandText = $query
            
            # Add parameters using a more explicit method - Updated for 1000 char greeting fields
            $param1 = $cmd.Parameters.Add("@AA_Name", [System.Data.SqlDbType]::NVarChar, 100)
            $param1.Value = $safeAAName

            $param2 = $cmd.Parameters.Add("@ResourceAccountUPN", [System.Data.SqlDbType]::NVarChar, 100)
            $param2.Value = $safeResourceAccountUPN

            $param3 = $cmd.Parameters.Add("@RedirectResourceAccount", [System.Data.SqlDbType]::NVarChar, 100)
            $param3.Value = $safeRedirectResourceAccount

            $param4 = $cmd.Parameters.Add("@SharedVoicemailGroup", [System.Data.SqlDbType]::NVarChar, 100)
            $param4.Value = $safeSharedVoicemailGroup

            $param5 = $cmd.Parameters.Add("@BusinessHoursGreetingText", [System.Data.SqlDbType]::NVarChar, 1000)
            $param5.Value = $safeBusinessHoursGreetingText

            $param6 = $cmd.Parameters.Add("@AfterHoursGreetingText", [System.Data.SqlDbType]::NVarChar, 1000)
            $param6.Value = $safeAfterHoursGreetingText

            $param7 = $cmd.Parameters.Add("@TimeZone", [System.Data.SqlDbType]::NVarChar, 50)
            $param7.Value = $safeTimeZone

            $param8 = $cmd.Parameters.Add("@Language", [System.Data.SqlDbType]::NVarChar, 20)
            $param8.Value = $safeLanguage

            $param9 = $cmd.Parameters.Add("@BusinessHoursStartTime", [System.Data.SqlDbType]::NVarChar, 20)
            $param9.Value = $safeBusinessHoursStartTime

            $param10 = $cmd.Parameters.Add("@BusinessHoursEndTime", [System.Data.SqlDbType]::NVarChar, 20)
            $param10.Value = $safeBusinessHoursEndTime

            $param11 = $cmd.Parameters.Add("@BusinessDays", [System.Data.SqlDbType]::NVarChar, 100)
            $param11.Value = $safeBusinessDays

            $param12 = $cmd.Parameters.Add("@EnableVoiceResponse", [System.Data.SqlDbType]::Int)
            $param12.Value = $safeEnableVoiceResponse

            $param13 = $cmd.Parameters.Add("@AuthorizedUsers", [System.Data.SqlDbType]::NVarChar, 500)
            $param13.Value = $safeAuthorizedUsers

            $param14 = $cmd.Parameters.Add("@PhoneNumber", [System.Data.SqlDbType]::NVarChar, 50)
            $param14.Value = $safePhoneNumber
            
            $cmd.ExecuteNonQuery() | Out-Null
            Write-Output "Inserted: $($aa.Name) - Redirect: $redirectResourceAccount - Shared Voicemail: $sharedVoicemailGroup"
            $processed++
        }
        catch {
            $errorMessage = $_.Exception.Message
            Write-Warning "Insert failed for $($aa.Name) - $errorMessage"
            Write-Warning "Stack Trace: $($_.ScriptStackTrace)"
        }
    }
}
catch {
    $errorMessage = $_.Exception.Message
    Write-Error "Error retrieving auto attendants: $errorMessage"
    throw
}

Write-Output "Auto attendant export completed. Total processed: $processed"

if ($SQLConnection.State -eq 'Open') {
    $SQLConnection.Close()
    Write-Output "SQL connection closed"
}

# Disconnect from Teams
Disconnect-MicrosoftTeams -Confirm:$false
Write-Output "Disconnected from Teams. Script execution completed."
