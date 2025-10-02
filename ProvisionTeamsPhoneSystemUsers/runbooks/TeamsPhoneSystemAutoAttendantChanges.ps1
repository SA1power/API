# Function to get Group information (from bulk script)
function Get-GroupInfo {
    param(
        [Parameter(Mandatory=$true)]
        [string] $GroupId,
        [Parameter(Mandatory=$true)]
        [hashtable] $Headers
    )
    $result = @{
        Email       = "N/A"
        DisplayName = "N/A"
        Members     = @()
    }
    try {
        $groupUri      = "https://graph.microsoft.com/v1.0/groups/$GroupId"
        $groupResponse = Invoke-RestMethod -Uri $groupUri -Headers $Headers -Method Get -ErrorAction SilentlyContinue
        
        if ($groupResponse) {
            $result.Email       = if ($groupResponse.mail) { $groupResponse.mail } else { $groupResponse.proxyAddresses | Where-Object { $_ -like "SMTP:*" } | ForEach-Object { $_.Substring(5) } | Select-Object -First 1 }
            $result.DisplayName = $groupResponse.displayName
            
            $membersUri      = "https://graph.microsoft.com/v1.0/groups/$GroupId/members"
            $membersResponse = Invoke-RestMethod -Uri $membersUri -Headers $Headers -Method Get -ErrorAction SilentlyContinue
            
            if ($membersResponse -and $membersResponse.value) {
                foreach ($member in $membersResponse.value) {
                    if ($member.mail) {
                        $result.Members += $member.mail
                    }
                    elseif ($member.userPrincipalName) {
                        $result.Members += $member.userPrincipalName
                    }
                }
            }
            
            $nextLink = $membersResponse.'@odata.nextLink'
            while ($nextLink) {
                $membersResponse = Invoke-RestMethod -Uri $nextLink -Headers $Headers -Method Get -ErrorAction SilentlyContinue
                
                if ($membersResponse -and $membersResponse.value) {
                    foreach ($member in $membersResponse.value) {
                        if ($member.mail) {
                            $result.Members += $member.mail
                        }
                        elseif ($member.userPrincipalName) {
                            $result.Members += $member.userPrincipalName
                        }
                    }
                }
                $nextLink = $membersResponse.'@odata.nextLink'
            }
        }
    }
    catch {
        Write-Output ("Error retrieving group info for " + $GroupId + " - " + $_)
    }
    return $result
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
        return $null
    }
    
    try {
        if ($Value -is [TimeSpan]) {
            return $Value.ToString()
        } elseif ($Value -is [string]) {
            if ([string]::IsNullOrEmpty($Value)) {
                return $null
            }
            return $Value.Trim()
        } else {
            return $Value.ToString().Trim()
        }
    }
    catch {
        Write-Output "Error converting value to string: $_"
        return $null
    }
}

# Function to get resource account display name from GUID (matching export script exactly)
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
    
    try {
        # Method 1: Try to find a matching resource account by ObjectId/Id
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
                return [string]$displayName
            }
        }
        
        # Method 2: Try to find it as another auto attendant
        if ($AllAutoAttendants.Count -gt 0) {
            foreach ($aa in $AllAutoAttendants) {
                if ($aa.Identity -eq $AccountGUID) {
                    return [string]$aa.Name
                }
            }
        }
        
        # Method 3: Try to find it in pre-loaded call queues
        if ($AllCallQueues.Count -gt 0) {
            foreach ($queue in $AllCallQueues) {
                if ($queue.Identity -eq $AccountGUID) {
                    return [string]$queue.Name
                }
            }
        }
        
        # Method 4: Try to look it up as a call queue directly
        try {
            $callQueue = Get-CsCallQueue -Identity $AccountGUID -WarningAction SilentlyContinue -ErrorAction SilentlyContinue
            if ($callQueue -and $callQueue.Name) {
                return [string]$callQueue.Name
            }
        } catch {
            # Call queue lookup failed, continue
        }
        
        # Method 5: Try Graph API direct lookup (if headers provided)
        if ($GraphHeaders.Count -gt 0) {
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
                        return [string]$displayName
                    }
                }
            } catch {
                # Graph API lookup failed, continue
            }
        }
        
        # Method 6: Try using Get-CsOnlineUser directly with the GUID
        try {
            $user = Get-CsOnlineUser -Identity $AccountGUID -ErrorAction SilentlyContinue
            if ($user -and $user.DisplayName) {
                return [string]$user.DisplayName
            }
        } catch {
            # Direct user lookup failed, continue
        }
        
        return "Unknown Account ($AccountGUID)"
        
    } catch {
        return "Unknown Account ($AccountGUID)"
    }
}

# Function to get shared voicemail group display name from GUID (matching export script exactly)
function Get-SharedVoicemailGroupName {
    param(
        [string]$GroupGUID,
        [hashtable]$GraphHeaders = @{}
    )
    
    if ([string]::IsNullOrEmpty($GroupGUID) -or $GroupGUID -eq "N/A") {
        return "N/A"
    }
    
    try {
        if ($GraphHeaders.Count -gt 0) {
            # Try Graph API groups lookup
            try {
                $groupGraphUri = "https://graph.microsoft.com/v1.0/groups/$GroupGUID"
                $groupResponse = Invoke-RestMethod -Uri $groupGraphUri -Headers $GraphHeaders -Method Get -ErrorAction SilentlyContinue
                
                if ($groupResponse -and $groupResponse.displayName) {
                    return [string]$groupResponse.displayName
                }
            } catch {
                # Graph API groups lookup failed, continue
            }
            
            # Try Graph API directory objects lookup as fallback
            try {
                $dirObjectUri = "https://graph.microsoft.com/v1.0/directoryObjects/$GroupGUID"
                $dirObjectResponse = Invoke-RestMethod -Uri $dirObjectUri -Headers $GraphHeaders -Method Get -ErrorAction SilentlyContinue
                
                if ($dirObjectResponse -and $dirObjectResponse.displayName) {
                    return [string]$dirObjectResponse.displayName
                }
            } catch {
                # Graph API directory objects lookup failed, continue
            }
        }
        
        return "Unknown Group ($GroupGUID)"
        
    } catch {
        return "Unknown Group ($GroupGUID)"
    }
}

# Teams Auto Attendant Change Detection - Production Beta
# Monitors all auto attendants for changes and logs to database
# Auto-triggers auto attendant data refresh when changes are detected

# Configuration
$SQLServer = "msoevendor.database.windows.net"
$Database = "CEProjectData"
$SourceTable = "dbo.msoe_teams_phone_system_aas"
$LogTable = "dbo.msoe_teams_phone_system_aas_change_log"

# Auto-trigger configuration
$AutomationAccountName = "VendorAutomationAccount"
$ResourceGroupName = "Infrastructure"
$TargetRunbookName = "MSOE_Teams_Phone_System_Auto_Attendants"
$SubscriptionId = "fc7ad0bc-429f-488b-9488-3ed508182348"

# Required fields to monitor
$TrackedFields = @(
    "ResourceAccountUPN", 
    "RedirectResourceAccount", 
    "SharedVoicemailGroup", 
    "BusinessHoursGreetingText", 
    "AfterHoursGreetingText", 
    "TimeZone",
    "Language",
    "BusinessHoursStartTime", 
    "BusinessHoursEndTime",
    "BusinessDays",
    "EnableVoiceResponse",
    "AuthorizedUsers",
    "PhoneNumber"
)

Write-Output "=== Teams Auto Attendant Change Detection - Production Beta ==="
Write-Output "$(Get-Date): Starting change detection for all auto attendants..."

try {
    # Connect to Azure & Teams
    Write-Output "$(Get-Date): Connecting to Azure..."
    Connect-AzAccount -Identity | Out-Null
    $token = (Get-AzAccessToken -ResourceUrl "https://database.windows.net").Token
    Write-Output "$(Get-Date): Azure connected"

    # Set the subscription context explicitly for runbook triggering
    Set-AzContext -SubscriptionId $SubscriptionId | Out-Null
    Write-Output "$(Get-Date): Set context to subscription: $SubscriptionId"

    Write-Output "$(Get-Date): Connecting to Microsoft Teams..."
    Connect-MicrosoftTeams -Identity | Out-Null
    Write-Output "$(Get-Date): Teams connected"

    # Create SQL Connection
    Write-Output "$(Get-Date): Connecting to SQL Database..."
    $connection = New-Object System.Data.SqlClient.SqlConnection
    $connection.ConnectionString = "Server=$SQLServer;Database=$Database;Integrated Security=False;Encrypt=True;TrustServerCertificate=False;Connection Timeout=30;"
    $connection.AccessToken = $token
    $connection.Open()
    Write-Output "$(Get-Date): SQL connected"

    # Get current SQL data for all auto attendants
    Write-Output "$(Get-Date): Loading current SQL data from $SourceTable..."
    $cmd = $connection.CreateCommand()
    $cmd.CommandTimeout = 60
    $cmd.CommandText = "SELECT * FROM $SourceTable"
    $reader = $cmd.ExecuteReader()
    $table = New-Object System.Data.DataTable
    $table.Load($reader)
    $reader.Close()

    if ($table.Rows.Count -eq 0) {
        Write-Output "$(Get-Date): ERROR - No auto attendants found in SQL database!"
        return
    }

    Write-Output "$(Get-Date): Loaded $($table.Rows.Count) records from SQL"

    # Get access token for Microsoft Graph API
    Write-Output "$(Get-Date): Getting Graph API token..."
    $GraphAccessToken = Get-AzAccessToken -ResourceUrl "https://graph.microsoft.com"
    $GraphToken = $GraphAccessToken.Token
    $GraphHeaders = @{
        "Authorization" = "Bearer $GraphToken"
        "Content-Type" = "application/json"
    }

    # Pre-cache Teams resource accounts
    Write-Output "$(Get-Date): Caching Teams resource accounts..."
    $resourceAccounts = @()
    try {
        # First try using Get-CsOnlineUser with the department filter
        $resourceAccounts = Get-CsOnlineUser -Filter "Department -eq 'Microsoft Communication Application Instance'" -ErrorAction SilentlyContinue
        
        if ($resourceAccounts -and $resourceAccounts.Count -gt 0) {
            Write-Output "$(Get-Date): Cached $($resourceAccounts.Count) Teams resource accounts"
        } else {
            # Fallback to Graph API if the cmdlet fails
            $filter = "department eq 'Microsoft Communication Application Instance'"
            $resourceAccountsUri = "https://graph.microsoft.com/v1.0/users?`$filter=$([System.Web.HttpUtility]::UrlEncode($filter))&`$top=999"
            $response = Invoke-RestMethod -Uri $resourceAccountsUri -Headers $GraphHeaders -Method Get
            
            if ($response -and $response.value) {
                $resourceAccounts = $response.value
                Write-Output "$(Get-Date): Cached $($resourceAccounts.Count) Teams resource accounts via Graph API"
            } else {
                $resourceAccounts = @()
                Write-Output "$(Get-Date): No resource accounts found"
            }
        }
    }
    catch {
        Write-Warning "$(Get-Date): Failed to cache resource accounts: $($_.Exception.Message)"
        $resourceAccounts = @()
    }

    # Pre-cache call queues for better GUID resolution
    Write-Output "$(Get-Date): Caching call queues..."
    $allCallQueues = @()
    try {
        $allCallQueues = Get-CsCallQueue -WarningAction SilentlyContinue -ErrorAction SilentlyContinue
        if ($allCallQueues) {
            Write-Output "$(Get-Date): Cached $($allCallQueues.Count) call queues"
        } else {
            Write-Output "$(Get-Date): No call queues found"
            $allCallQueues = @()
        }
    } catch {
        Write-Warning "$(Get-Date): Failed to cache call queues: $($_.Exception.Message)"
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

    # STATIC MAPPING SECTION - Add specific mappings here (same as export script)
    Write-Output "$(Get-Date): Applying static mappings for specific auto attendants..."
    
    # Static mapping for "Call Demo Lab" -> "Call Demo Lab AA"
    $staticMappings = @{
        "Call Demo Lab" = "Call Demo Lab AA"
    }

    # Apply static mappings by finding the resource account and adding it to our lookup
    foreach ($aaName in $staticMappings.Keys) {
        $targetResourceAccountName = $staticMappings[$aaName]
        
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
                break
            }
        }
        
        if ($targetAccount) {
            # Override or add the mapping
            $resourceAccountsByName[$aaName] = $targetAccount
            Write-Output "$(Get-Date): Static mapping applied: '$aaName' -> '$targetResourceAccountName'"
        }
    }

    Write-Output "$(Get-Date): Resource account lookup table has $($resourceAccountsByName.Count) entries"

    # Get all auto attendants
    Write-Output "$(Get-Date): Retrieving all auto attendants..."
    $autoAttendants = Get-CsAutoAttendant -ErrorAction Stop
    
    if ($autoAttendants.Count -eq 0) {
        Write-Output "$(Get-Date): ERROR - No auto attendants found in Teams!"
        return
    }

    Write-Output "$(Get-Date): Found $($autoAttendants.Count) auto attendants to process"

    # Process each auto attendant with progress tracking
    $totalChanges = 0
    $aasWithChanges = 0
    $processedCount = 0
    $errorCount = 0

    foreach ($aa in $autoAttendants) {
        $processedCount++
        $aaName = $aa.Name
        
        # Progress indicator every 10 auto attendants or for first auto attendant
        if ($processedCount % 10 -eq 0 -or $processedCount -eq 1) {
            Write-Output "$(Get-Date): Processing auto attendant $processedCount of $($autoAttendants.Count) ($([math]::Round(($processedCount/$autoAttendants.Count)*100,1))%) - $aaName"
        }

        try {
            # Find corresponding SQL row
            $sqlRow = $table.Rows | Where-Object { $_["AA_Name"] -eq $aaName }
            if (-not $sqlRow) {
                continue  # Skip auto attendants not in SQL database
            }

            # Initialize values (using "N/A" as default to match export script)
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

            # Look up resource account by display name (including static mappings)
            if ($resourceAccountsByName.ContainsKey($aa.Name)) {
                $matchedAccount = $resourceAccountsByName[$aa.Name]
                
                # Get UPN based on the object type
                if ($matchedAccount.PSObject.Properties.Name -contains "UserPrincipalName") {
                    $resourceAccountUPN = $matchedAccount.UserPrincipalName
                } elseif ($matchedAccount.PSObject.Properties.Name -contains "userPrincipalName") {
                    $resourceAccountUPN = $matchedAccount.userPrincipalName
                }
                
                # Try to get phone number
                if ($matchedAccount.PSObject.Properties.Name -contains "LineURI" -and $matchedAccount.LineURI) {
                    $phoneNumber = $matchedAccount.LineURI
                } elseif ($matchedAccount.PSObject.Properties.Name -contains "PhoneNumber" -and $matchedAccount.PhoneNumber) {
                    $phoneNumber = $matchedAccount.PhoneNumber
                } elseif ($matchedAccount.PSObject.Properties.Name -contains "businessPhones" -and 
                        $matchedAccount.businessPhones -and 
                        $matchedAccount.businessPhones.Count -gt 0) {
                    $phoneNumber = $matchedAccount.businessPhones[0]
                } elseif ($matchedAccount.PSObject.Properties.Name -contains "mobilePhone" -and $matchedAccount.mobilePhone) {
                    $phoneNumber = $matchedAccount.mobilePhone
                }
            }

            # Get business hours info (same logic as export script)
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
                                        $resolvedGroupName = Get-SharedVoicemailGroupName -GroupGUID $sharedVoicemailGroupGUID -GraphHeaders $GraphHeaders
                                        # Ensure we get a string result
                                        if ($resolvedGroupName -and $resolvedGroupName -ne "N/A") {
                                            $sharedVoicemailGroup = [string]$resolvedGroupName
                                        }
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
                            $redirectResourceAccount = Get-ResourceAccountDisplayName -AccountGUID $appId -ResourceAccounts $resourceAccounts -AllAutoAttendants $autoAttendants -AllCallQueues $allCallQueues -GraphHeaders $GraphHeaders
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
            $authorizedUsers = if ($authorizedUsersArray.Count -gt 0) { $authorizedUsersArray -join ";" } else { $null }

            # Create comparison map (ensure all values are properly converted to strings)
            $compareMap = @{
                "ResourceAccountUPN" = if ($resourceAccountUPN -and $resourceAccountUPN -ne "N/A") { [string]$resourceAccountUPN } else { "N/A" }
                "RedirectResourceAccount" = if ($redirectResourceAccount -and $redirectResourceAccount -ne "N/A") { 
                    # Ensure it's a string, not an array
                    if ($redirectResourceAccount -is [array]) {
                        ($redirectResourceAccount | Where-Object { $_ -ne $null } | ForEach-Object { $_.ToString() }) -join ";"
                    } else {
                        [string]$redirectResourceAccount
                    }
                } else { "N/A" }
                "SharedVoicemailGroup" = if ($sharedVoicemailGroup -and $sharedVoicemailGroup -ne "N/A") { 
                    # Ensure it's a string, not an array
                    if ($sharedVoicemailGroup -is [array]) {
                        ($sharedVoicemailGroup | Where-Object { $_ -ne $null } | ForEach-Object { $_.ToString() }) -join ";"
                    } else {
                        [string]$sharedVoicemailGroup
                    }
                } else { "N/A" }
                "BusinessHoursGreetingText" = if ($businessHoursGreetingText -and $businessHoursGreetingText -ne "N/A") { [string]$businessHoursGreetingText } else { "N/A" }
                "AfterHoursGreetingText" = if ($afterHoursGreetingText -and $afterHoursGreetingText -ne "N/A") { [string]$afterHoursGreetingText } else { "N/A" }
                "TimeZone" = if ($timeZone -and $timeZone -ne "N/A") { [string]$timeZone } else { "N/A" }
                "Language" = if ($language -and $language -ne "N/A") { [string]$language } else { "N/A" }
                "BusinessHoursStartTime" = if ($businessHoursStartTime -and $businessHoursStartTime -ne "N/A") { [string]$businessHoursStartTime } else { "N/A" }
                "BusinessHoursEndTime" = if ($businessHoursEndTime -and $businessHoursEndTime -ne "N/A") { [string]$businessHoursEndTime } else { "N/A" }
                "BusinessDays" = if ($businessDays -and $businessDays -ne "N/A") { [string]$businessDays } else { "N/A" }
                "EnableVoiceResponse" = if ($enableVoiceResponse -eq $true) { "1" } else { "0" }
                "AuthorizedUsers" = if ($authorizedUsers -and $authorizedUsers -ne "") { [string]$authorizedUsers } else { "N/A" }
                "PhoneNumber" = if ($phoneNumber -and $phoneNumber -ne "N/A") { [string]$phoneNumber } else { "N/A" }
            }

            # Debug output for first few auto attendants to see what's actually in the API
            if ($processedCount -le 2) {
                Write-Output "=== DEBUG API PROPERTIES FOR $aaName ==="
                Write-Output "RedirectResourceAccount Type: $($redirectResourceAccount.GetType().Name) Value: '$redirectResourceAccount'"
                Write-Output "SharedVoicemailGroup Type: $($sharedVoicemailGroup.GetType().Name) Value: '$sharedVoicemailGroup'"
                Write-Output "=== END DEBUG ==="
            }

            # Compare fields and detect changes (with detailed debugging for problematic fields)
            $changed = $false
            $changedFields = @{}

            foreach ($field in $TrackedFields) {
                # Handle DBNull values from SQL (convert to "N/A" to match export script logic)
                $sqlValue = if ($sqlRow[$field] -eq [DBNull]::Value -or $sqlRow[$field] -eq $null) { 
                    "N/A" 
                } else { 
                    $sqlRow[$field].ToString().Trim()
                }
                
                $newValue = if ($compareMap[$field]) { 
                    $compareMap[$field].ToString().Trim() 
                } else { 
                    "N/A"
                }

                # Special handling for certain problematic fields
                if ($field -eq "SharedVoicemailGroup" -or $field -eq "RedirectResourceAccount") {
                    # Extra debugging for these fields only if there's a change
                    if ($processedCount -le 3 -and $sqlNormalized -ne $newNormalized) {
                        Write-Output "DEBUG - ${aaName} - ${field}:"
                        Write-Output "  SQL: '$sqlNormalized' | API: '$newNormalized'"
                    }
                }

                # Improved comparison logic to handle "N/A" values consistently
                $valuesAreDifferent = $false
                
                # Normalize both values for comparison (treat null, empty, and "N/A" as equivalent)
                $sqlNormalized = if ([string]::IsNullOrWhiteSpace($sqlValue) -or $sqlValue -eq "N/A") { "N/A" } else { $sqlValue }
                $newNormalized = if ([string]::IsNullOrWhiteSpace($newValue) -or $newValue -eq "N/A") { "N/A" } else { $newValue }
                
                if ($sqlNormalized -ne $newNormalized) {
                    $valuesAreDifferent = $true
                }

                if ($valuesAreDifferent) {
                    $changed = $true
                    $changedFields[$field] = @{ 
                        old = $sqlNormalized
                        new = $newNormalized
                    }
                }
            }

            # Log changes if detected
            if ($changed) {
                $aasWithChanges++
                $totalChanges += $changedFields.Keys.Count
                Write-Output "$(Get-Date): Change detected for $aaName. Fields changed: $($changedFields.Keys -join ', ')"
                
                # Log the change using transaction logic
                # Convert to Central Standard Time
                $centralTime = [System.TimeZoneInfo]::ConvertTimeBySystemTimeZoneId((Get-Date), "Central Standard Time")
                $now = $centralTime.ToString("yyyy-MM-dd HH:mm:ss")
                $transaction = $connection.BeginTransaction()
                
                try {
                    # Get column names from the table structure
                    $columns = $table.Columns | ForEach-Object { $_.ColumnName }

                    # Build the INSERT command with proper parameterization
                    $insertCmd = $connection.CreateCommand()
                    $insertCmd.Transaction = $transaction
                    $insertCmd.CommandTimeout = 30
                    
                    # Build column and parameter lists
                    $colList = @()
                    $paramList = @()
                    
                    for ($i = 0; $i -lt $columns.Count; $i++) {
                        $colList += "[$($columns[$i])]"  # Bracket column names for SQL
                        $paramList += "@param$i"         # Use numbered parameters
                    }
                    $colList += "[ChangeType]", "[ChangeDate]"
                    $paramList += "@paramChangeType", "@paramChangeDate"
                    
                    $insertCmd.CommandText = "INSERT INTO $LogTable ($($colList -join ', ')) VALUES ($($paramList -join ', '))"
                    
                    # Add parameters with "original -> 'oldValue' | change -> 'newValue'" format for changed fields
                    for ($i = 0; $i -lt $columns.Count; $i++) {
                        $colName = $columns[$i]
                        
                        if ($changedFields.ContainsKey($colName)) {
                            # Format: "original -> 'oldValue' | change -> 'newValue'"
                            $oldValue = $changedFields[$colName].old
                            $newValue = $changedFields[$colName].new
                            $formattedValue = "original -> '$oldValue' | change -> '$newValue'"
                            $insertCmd.Parameters.AddWithValue("@param$i", $formattedValue) | Out-Null
                        } else {
                            # Unchanged field - use current value from SQL
                            $value = if ($sqlRow[$colName] -eq [DBNull]::Value) { [DBNull]::Value } else { $sqlRow[$colName] }
                            $insertCmd.Parameters.AddWithValue("@param$i", $value) | Out-Null
                        }
                    }
                    $insertCmd.Parameters.AddWithValue("@paramChangeType", "change") | Out-Null
                    $insertCmd.Parameters.AddWithValue("@paramChangeDate", $now) | Out-Null
                    
                    # Execute the insert
                    $insertCmd.ExecuteNonQuery() | Out-Null
                    
                    # Commit the transaction
                    $transaction.Commit()
                    
                    # Log the specific changes
                    foreach ($field in $changedFields.Keys) {
                        $change = $changedFields[$field]
                        Write-Output "$(Get-Date):   ${field}: '$($change.old)' -> '$($change.new)'"
                    }
                }
                catch {
                    if ($transaction) { $transaction.Rollback() }
                    $errorCount++
                    Write-Error "$(Get-Date): Failed to log changes for $aaName : $($_.Exception.Message)"
                }
                finally {
                    if ($insertCmd) { $insertCmd.Dispose() }
                    if ($transaction) { $transaction.Dispose() }
                }
            }
        }
        catch {
            $errorCount++
            Write-Error "$(Get-Date): Failed to process auto attendant $aaName : $($_.Exception.Message)"
            continue
        }
    }

    # Final summary first
    Write-Output ""
    Write-Output "=== FINAL SUMMARY ==="
    Write-Output "$(Get-Date): Processing complete!"
    Write-Output "$(Get-Date): Total auto attendants processed: $($autoAttendants.Count)"
    Write-Output "$(Get-Date): Auto attendants with changes: $aasWithChanges"
    Write-Output "$(Get-Date): Total field changes detected: $totalChanges"
    Write-Output "$(Get-Date): All change log SQL insertions completed successfully"

    # Auto-trigger auto attendant data refresh ONLY after all processing and SQL insertions are complete
    if ($aasWithChanges -gt 0) {
        Write-Output ""
        Write-Output "=== AUTO-TRIGGERING AUTO ATTENDANT DATA REFRESH ==="
        Write-Output "$(Get-Date): All change detection and logging complete"
        Write-Output "$(Get-Date): Changes detected ($aasWithChanges auto attendants with $totalChanges total changes)"
        Write-Output "$(Get-Date): Now triggering runbook '$TargetRunbookName' to refresh auto attendant data..."
        
        try {
            # Start the target runbook (no parameters passed)
            $job = Start-AzAutomationRunbook -AutomationAccountName $AutomationAccountName `
                                          -ResourceGroupName $ResourceGroupName `
                                          -Name $TargetRunbookName

            if ($job -and $job.JobId) {
                Write-Output "$(Get-Date): Auto attendant data refresh runbook started successfully!"
                Write-Output "$(Get-Date): Job ID: $($job.JobId)"
                Write-Output "$(Get-Date): Job Status: $($job.Status)"
                Write-Output "$(Get-Date): This will refresh the auto attendant data to reflect the detected changes."
            } else {
                Write-Warning "$(Get-Date): Auto attendant data refresh runbook may not have started properly - no job object returned"
            }
        }
        catch {
            Write-Error "$(Get-Date): Failed to trigger auto attendant data refresh runbook: $($_.Exception.Message)"
            Write-Output "$(Get-Date): You may need to manually run '$TargetRunbookName' to update the auto attendant data"
        }
    } else {
        Write-Output ""
        Write-Output "$(Get-Date): No changes detected - auto attendant data refresh not needed"
    }

    # Final summary
    Write-Output ""
    Write-Output "=== FINAL SUMMARY ==="
    Write-Output "$(Get-Date): Processing complete!"
    Write-Output "$(Get-Date): Total auto attendants processed: $($autoAttendants.Count)"
    Write-Output "$(Get-Date): Auto attendants with changes: $aasWithChanges"
    Write-Output "$(Get-Date): Total field changes detected: $totalChanges"
    if ($aasWithChanges -gt 0) {
        Write-Output "$(Get-Date): Auto attendant data refresh runbook triggered automatically after all SQL operations completed"
    }

}
catch {
    Write-Error "$(Get-Date): Script error: $($_.Exception.Message)"
    Write-Error "$(Get-Date): Stack trace: $($_.Exception.StackTrace)"
}
finally {
    # Cleanup
    if ($connection -and $connection.State -eq "Open") {
        $connection.Close()
    }
    
    try {
        Disconnect-MicrosoftTeams -Confirm:$false -ErrorAction SilentlyContinue
    }
    catch {
        # Ignore disconnect errors
    }
    
    Write-Output "$(Get-Date): Auto attendant change detection complete!"
}