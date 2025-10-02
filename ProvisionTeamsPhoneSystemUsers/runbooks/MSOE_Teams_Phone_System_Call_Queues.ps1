# Teams Call Queue Data Export Script for Azure Automation Runbook
# This script gets all Teams call queues and writes the data to a SQL table

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
$Table = "dbo.msoe_teams_phone_system_cqs"

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

# Function to safely truncate text to prevent SQL errors
function Get-SafeTruncatedText {
    param(
        [string]$Text,
        [int]$MaxLength = 255
    )
    
    if ([string]::IsNullOrEmpty($Text)) {
        return "N/A"
    }
    
    if ($Text.Length -gt $MaxLength) {
        # Reserve 3 characters for ellipsis
        $truncateLength = $MaxLength - 3
        if ($truncateLength -gt 0) {
            $truncatedText = $Text.Substring(0, $truncateLength) + "..."
            Write-Warning "Text truncated: Original length $($Text.Length), truncated to $($truncatedText.Length)"
            return $truncatedText
        } else {
            Write-Warning "Max length too small for ellipsis, using first $MaxLength characters"
            return $Text.Substring(0, $MaxLength)
        }
    }
    
    return $Text
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

# Function to get group name from GUID
function Get-GroupNameFromGUID {
    param(
        [string]$GroupGUID
    )
    
    if ([string]::IsNullOrEmpty($GroupGUID) -or $GroupGUID -eq "N/A") {
        return "N/A"
    }
    
    try {
        # First try to get the group using Graph API
        $groupUri = "https://graph.microsoft.com/v1.0/groups/$GroupGUID"
        $response = Invoke-RestMethod -Uri $groupUri -Headers $GraphHeaders -Method Get -ErrorAction SilentlyContinue
        
        if ($response -and $response.displayName) {
            $groupName = [string]$response.displayName
            Write-Host "  Resolved group GUID '$GroupGUID' to name: '$groupName'"
            return $groupName
        }
        
        # Fallback to try as a distribution list via PowerShell cmdlets
        $distributionList = Get-DistributionGroup -Identity $GroupGUID -ErrorAction SilentlyContinue
        if ($distributionList -and $distributionList.DisplayName) {
            $groupName = [string]$distributionList.DisplayName
            Write-Host "  Resolved distribution list GUID '$GroupGUID' to name: '$groupName'"
            return $groupName
        }
        
        # Additional fallback to try as a unified group
        $unifiedGroup = Get-UnifiedGroup -Identity $GroupGUID -ErrorAction SilentlyContinue
        if ($unifiedGroup -and $unifiedGroup.DisplayName) {
            $groupName = [string]$unifiedGroup.DisplayName
            Write-Host "  Resolved unified group GUID '$GroupGUID' to name: '$groupName'"
            return $groupName
        }
        
        Write-Host "  Could not resolve group GUID '$GroupGUID' to a name"
        return "Unknown Group ($GroupGUID)"
        
    } catch {
        Write-Host "  Error resolving group GUID '$GroupGUID': $_"
        return "Unknown Group ($GroupGUID)"
    }
}

# Function to get user UPN from an ObjectId
function Get-UserUPNFromObjectId {
    param(
        [string]$ObjectId
    )
    
    try {
        $user = Get-CsOnlineUser -Identity $ObjectId -ErrorAction SilentlyContinue
        if ($user -and $user.UserPrincipalName) {
            return $user.UserPrincipalName
        } else {
            return "Unknown User"
        }
    }
    catch {
        Write-Host "Error retrieving user info for ObjectId '$ObjectId': $_"
        return "Unknown User"
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
        } 
        # If it's already a string, just return it
        if ($Target -is [string]) {
            return $Target
        }
        # If we can't determine the type, convert to string
        return $Target.ToString()
    }
    catch {
        Write-Host "Error extracting Target ID: $_"
        return "N/A"
    }
}

# Function to get phone number from resource account UPN
function Get-PhoneNumberFromUPN {
    param(
        [string]$UPN
    )
    
    if ([string]::IsNullOrEmpty($UPN) -or $UPN -eq "N/A") {
        return "N/A"
    }
    
    try {
        Write-Host "    Searching for phone number for UPN: $UPN"
        
        # Try to get the user using Get-CsOnlineUser
        $user = Get-CsOnlineUser -Identity $UPN -ErrorAction SilentlyContinue
        
        if ($user) {
            Write-Host "    Found user object for $UPN"
            if ($user.LineURI) {
                Write-Host "    Found LineURI: $($user.LineURI)"
                # Line URI is typically in format tel:+1234567890;ext=123
                # Extract just the number
                $phoneNumber = $user.LineURI
                $phoneNumber = $phoneNumber -replace "tel:", ""
                $phoneNumber = $phoneNumber -replace ";ext=\d+", ""
                Write-Host "    Processed phone number: $phoneNumber"
                return $phoneNumber
            } else {
                Write-Host "    No LineURI found for $UPN"
            }
        } else {
            Write-Host "    No user object found with Get-CsOnlineUser for $UPN"
        }
        
        # Fallback to Graph API if needed
        Write-Host "    Trying Graph API fallback for $UPN"
        $encodedUPN = [System.Web.HttpUtility]::UrlEncode($UPN)
        $userUri = "https://graph.microsoft.com/v1.0/users/$encodedUPN"
        $response = Invoke-RestMethod -Uri $userUri -Headers $GraphHeaders -Method Get -ErrorAction SilentlyContinue
        
        if ($response) {
            Write-Host "    Found Graph API response for $UPN"
            if ($response.businessPhones -and $response.businessPhones.Count -gt 0) {
                $businessPhone = $response.businessPhones[0]
                Write-Host "    Found business phone: $businessPhone"
                return $businessPhone
            } else {
                Write-Host "    No business phones found in Graph API for $UPN"
            }
        } else {
            Write-Host "    No Graph API response for $UPN"
        }
        
        Write-Host "    No phone number found for $UPN"
        return "N/A"
    }
    catch {
        Write-Host "Error retrieving phone number for UPN '$UPN': $_"
        return "N/A"
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
    
    if (-not [string]::IsNullOrEmpty($displayName)) {
        $resourceAccountsByName[$displayName] = $account
    }
}
Write-Output "Created lookup table with $($resourceAccountsByName.Count) resource accounts by display name"

# Create a static mapping for known mismatched call queue names
$staticQueueResourceMapping = @{
    "7159 Emergency Public Safety" = "Emergency - Public Safety"
    "Q_test_cq" = "test_cq"
    "Q_HelpdeskCQ1" = "HelpdeskCQ1"
    "Q_PhoneDemoLab" = "Phone Demo Lab"
    "Q_PilotCallQueue-IN_TESTING" = "AlphaPilotCallQueue"
    "Q_Sysadmin_MFA_CQ" = "SysAdmin_MFA_CQ"
    "7169 Call Public Safety" = "Call Public Safety"
    "7447 Call SHIP Service" = "Q Ship Service"
    "7161 Call Parking Hotline" = "Q Parking Hotline"
}
Write-Output "Created static mapping for special call queue names with $($staticQueueResourceMapping.Count) entries"

# Retrieve all auto attendants to create a lookup for matching call queues
Write-Output "Retrieving all auto attendants for call queue mapping..."
try {
    $autoAttendants = Get-CsAutoAttendant -ErrorAction SilentlyContinue
    if ($autoAttendants) {
        Write-Output "Found $($autoAttendants.Count) auto attendants for potential call queue mapping"
    } else {
        Write-Output "No auto attendants found for call queue mapping"
        $autoAttendants = @()
    }
} catch {
    Write-Output "Error retrieving auto attendants: $_"
    $autoAttendants = @()
}

# Create a mapping between Queue and AA names
# e.g., "Q Admissions" -> "Call Admissions"
$queueToAAMapping = @{}
try {
    $tempCallQueues = Get-CsCallQueue -ErrorAction SilentlyContinue
    if ($tempCallQueues) {
        foreach ($tempCq in $tempCallQueues) {
            $queueName = $tempCq.Name
            if ($queueName -match "^Q\s+(.+)$") {
                $aaName = "Call $($matches[1])"
                $queueToAAMapping[$queueName] = $aaName
            }
        }
        Write-Output "Created mapping between $($queueToAAMapping.Count) call queues and potential auto attendants"
    } else {
        Write-Output "No call queues found for AA mapping"
    }
} catch {
    Write-Output "Error creating queue to AA mapping: $_"
}

# Retrieve all call queues
Write-Output "Retrieving all call queues..."
try {
    $callQueues = Get-CsCallQueue -ErrorAction Stop
    Write-Output "Found $($callQueues.Count) call queues to process"
    
    # List all call queue names for debugging
    Write-Output "Call queue names found:"
    foreach ($cq in $callQueues) {
        Write-Output "  - '$($cq.Name)'"
    }
    
    $processed = 0
    
    foreach ($cq in $callQueues) {
        Write-Output "Processing call queue: '$($cq.Name)'"
        
        try {
            # Initialize values
            $resourceAccountUPN = "N/A"
            $callerIdResourceAccountUPN = "N/A"
            $agentGroupName = "N/A"
            $overflowSharedVoicemailGroupName = "N/A"
            $timeoutSharedVoicemailGroupName = "N/A"
            $noAgentsSharedVoicemailGroupName = "N/A"
            $overflowGreetingText = "N/A"
            $timeoutGreetingText = "N/A"
            $noAgentsGreetingText = "N/A"
            $language = if ($cq.LanguageId) { $cq.LanguageId } else { "en-US" }
            $phoneNumber = "N/A"
            $callerIdPhoneNumber = "N/A"
            
            # Get overflow, timeout, no agents greeting text with safe truncation (using reasonable max lengths)
            if ($cq.OverflowSharedVoicemailTextToSpeechPrompt) {
                $overflowGreetingText = Get-SafeTruncatedText -Text $cq.OverflowSharedVoicemailTextToSpeechPrompt -MaxLength 1000
            }
            
            if ($cq.TimeoutSharedVoicemailTextToSpeechPrompt) {
                $timeoutGreetingText = Get-SafeTruncatedText -Text $cq.TimeoutSharedVoicemailTextToSpeechPrompt -MaxLength 1000
            }
            
            if ($cq.NoAgentSharedVoicemailTextToSpeechPrompt) {
                $noAgentsGreetingText = Get-SafeTruncatedText -Text $cq.NoAgentSharedVoicemailTextToSpeechPrompt -MaxLength 1000
            }
            
            # First check if there's a static mapping for this call queue
            $matchedAccount = $null
            if ($staticQueueResourceMapping.ContainsKey($cq.Name)) {
                $raName = $staticQueueResourceMapping[$cq.Name]
                Write-Output "  Using static mapping for '$($cq.Name)' -> '$raName'"
                
                # Look up the resource account by name
                if ($resourceAccountsByName.ContainsKey($raName)) {
                    $matchedAccount = $resourceAccountsByName[$raName]
                    Write-Output "  Found resource account with static mapping: $raName"
                } else {
                    Write-Output "  Static mapping found, but resource account '$raName' doesn't exist"
                }
            } elseif ($resourceAccountsByName.ContainsKey($cq.Name)) {
                # If no static mapping or it didn't find a match, try exact name matching
                $matchedAccount = $resourceAccountsByName[$cq.Name]
                Write-Output "  Found resource account with matching display name: $($cq.Name)"
            } else {
                Write-Output "  No resource account found with matching display name or static mapping"
            }
            
            # Get the resource account UPN and phone number if we found a match
            if ($matchedAccount) {
                # Get UPN based on the object type
                if ($matchedAccount.PSObject.Properties.Name -contains "UserPrincipalName") {
                    $resourceAccountUPN = $matchedAccount.UserPrincipalName
                } elseif ($matchedAccount.PSObject.Properties.Name -contains "userPrincipalName") {
                    $resourceAccountUPN = $matchedAccount.userPrincipalName
                }
                
                Write-Output "  Using resource account with UPN: $resourceAccountUPN"
                
                # Get the phone number for this resource account
                if ($resourceAccountUPN -ne "N/A") {
                    $phoneNumber = Get-PhoneNumberFromUPN -UPN $resourceAccountUPN
                    Write-Output "  Retrieved phone number for resource account: $phoneNumber"
                }
            }
            
            # Get Caller ID resource account - find the corresponding Auto Attendant
            if ($queueToAAMapping.ContainsKey($cq.Name)) {
                $aaName = $queueToAAMapping[$cq.Name]
                Write-Output "  Found matching Auto Attendant name: $aaName"
                
                # Find the AA in the list of auto attendants
                $matchingAA = $autoAttendants | Where-Object { $_.Name -eq $aaName }
                if ($matchingAA) {
                    Write-Output "  Found matching Auto Attendant: $($matchingAA.Name)"
                    
                    # Find the resource account for this AA
                    if ($resourceAccountsByName.ContainsKey($aaName)) {
                        $aaAccount = $resourceAccountsByName[$aaName]
                        
                        # Get the UPN
                        if ($aaAccount.PSObject.Properties.Name -contains "UserPrincipalName") {
                            $callerIdResourceAccountUPN = $aaAccount.UserPrincipalName
                        } elseif ($aaAccount.PSObject.Properties.Name -contains "userPrincipalName") {
                            $callerIdResourceAccountUPN = $aaAccount.userPrincipalName
                        }
                        
                        Write-Output "  Set CallerID to AA resource account: $callerIdResourceAccountUPN"
                    } else {
                        Write-Output "  Could not find resource account for Auto Attendant: $aaName"
                    }
                } else {
                    Write-Output "  Could not find matching Auto Attendant object with name: $aaName"
                }
            } else {
                # Fallback to standard OboResourceAccountIds if no matching AA found
                if ($cq.OboResourceAccountIds -and $cq.OboResourceAccountIds.Count -gt 0) {
                    $callerIdObjectId = $cq.OboResourceAccountIds[0]
                    
                    # Look for caller ID account in our resource accounts first
                    $callerIdAccount = $null
                    foreach ($account in $resourceAccounts) {
                        $id = if ($account.PSObject.Properties.Name -contains "ObjectId") { 
                            $account.ObjectId 
                        } elseif ($account.PSObject.Properties.Name -contains "id") { 
                            $account.id 
                        } else { 
                            $null 
                        }
                        
                        if ($id -eq $callerIdObjectId) {
                            $callerIdAccount = $account
                            break
                        }
                    }
                    
                    if ($callerIdAccount) {
                        if ($callerIdAccount.PSObject.Properties.Name -contains "UserPrincipalName") {
                            $callerIdResourceAccountUPN = $callerIdAccount.UserPrincipalName
                        } elseif ($callerIdAccount.PSObject.Properties.Name -contains "userPrincipalName") {
                            $callerIdResourceAccountUPN = $callerIdAccount.userPrincipalName
                        }
                    } else {
                        # Fallback to direct lookup
                        $callerIdAccount = Get-CsOnlineApplicationInstance -Identity $callerIdObjectId -ErrorAction SilentlyContinue
                        if ($callerIdAccount) {
                            $callerIdResourceAccountUPN = $callerIdAccount.UserPrincipalName
                        }
                    }
                }
            }
            
            # Get the phone number for the caller ID resource account (after it's been determined)
            if ($callerIdResourceAccountUPN -ne "N/A") {
                Write-Output "  Attempting to get caller ID phone number for: $callerIdResourceAccountUPN"
                $callerIdPhoneNumber = Get-PhoneNumberFromUPN -UPN $callerIdResourceAccountUPN
                Write-Output "  Retrieved caller ID phone number: $callerIdPhoneNumber"
            } else {
                Write-Output "  No caller ID resource account found, skipping phone number lookup"
            }
            
            # Get agent group name (convert GUID to group name)
            if ($cq.DistributionLists -and $cq.DistributionLists.Count -gt 0) {
                $agentGroupGUID = $cq.DistributionLists[0]
                $agentGroupName = Get-GroupNameFromGUID -GroupGUID $agentGroupGUID
                Write-Host "  Agent Group: $agentGroupName"
            }
            
            # Handle Target objects properly for voicemail settings and convert GUIDs to group names
            if ($cq.OverflowAction -eq "SharedVoicemail" -and $cq.OverflowActionTarget) {
                $overflowSharedVoicemailGUID = Get-TargetIdAsString -Target $cq.OverflowActionTarget
                $overflowSharedVoicemailGroupName = Get-GroupNameFromGUID -GroupGUID $overflowSharedVoicemailGUID
                Write-Host "  Overflow Shared Voicemail Group: $overflowSharedVoicemailGroupName"
            }
            
            if ($cq.TimeoutAction -eq "SharedVoicemail" -and $cq.TimeoutActionTarget) {
                $timeoutSharedVoicemailGUID = Get-TargetIdAsString -Target $cq.TimeoutActionTarget
                $timeoutSharedVoicemailGroupName = Get-GroupNameFromGUID -GroupGUID $timeoutSharedVoicemailGUID
                Write-Host "  Timeout Shared Voicemail Group: $timeoutSharedVoicemailGroupName"
            }
            
            if ($cq.NoAgentAction -eq "SharedVoicemail" -and $cq.NoAgentActionTarget) {
                $noAgentsSharedVoicemailGUID = Get-TargetIdAsString -Target $cq.NoAgentActionTarget
                $noAgentsSharedVoicemailGroupName = Get-GroupNameFromGUID -GroupGUID $noAgentsSharedVoicemailGUID
                Write-Host "  No Agents Shared Voicemail Group: $noAgentsSharedVoicemailGroupName"
            }
            
            # Get authorized users as UPNs
            $authorizedUsersArray = @()
            if ($cq.AuthorizedUsers -and $cq.AuthorizedUsers.Count -gt 0) {
                foreach ($userId in $cq.AuthorizedUsers) {
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
                        # Fallback to direct lookup
                        $userUPN = Get-UserUPNFromObjectId -ObjectId $userId
                        if ($userUPN -ne "Unknown User") {
                            $authorizedUsersArray += $userUPN
                        }
                    }
                }
            }
            $authorizedUsers = $authorizedUsersArray -join ";"
            
            # Apply safe truncation to all text fields with appropriate max lengths
            $callQueueName = Get-SafeTruncatedText -Text $cq.Name -MaxLength 100
            $resourceAccountUPN = Get-SafeTruncatedText -Text $resourceAccountUPN -MaxLength 255
            $callerIdResourceAccountUPN = Get-SafeTruncatedText -Text $callerIdResourceAccountUPN -MaxLength 255
            $callerIdPhoneNumber = Get-SafeTruncatedText -Text $callerIdPhoneNumber -MaxLength 50
            $agentGroupName = Get-SafeTruncatedText -Text $agentGroupName -MaxLength 255
            $overflowSharedVoicemailGroupName = Get-SafeTruncatedText -Text $overflowSharedVoicemailGroupName -MaxLength 255
            $timeoutSharedVoicemailGroupName = Get-SafeTruncatedText -Text $timeoutSharedVoicemailGroupName -MaxLength 255
            $noAgentsSharedVoicemailGroupName = Get-SafeTruncatedText -Text $noAgentsSharedVoicemailGroupName -MaxLength 255
            $language = Get-SafeTruncatedText -Text $language -MaxLength 10
            $authorizedUsers = Get-SafeTruncatedText -Text $authorizedUsers -MaxLength 1000
            $phoneNumber = Get-SafeTruncatedText -Text $phoneNumber -MaxLength 50
            
            # Ensure we have values for all parameters (not null) and convert to strings
            $resourceAccountUPN = if ([string]::IsNullOrEmpty($resourceAccountUPN)) { "N/A" } else { [string]$resourceAccountUPN }
            $callerIdResourceAccountUPN = if ([string]::IsNullOrEmpty($callerIdResourceAccountUPN)) { "N/A" } else { [string]$callerIdResourceAccountUPN }
            $callerIdPhoneNumber = if ([string]::IsNullOrEmpty($callerIdPhoneNumber)) { "N/A" } else { [string]$callerIdPhoneNumber }
            $agentGroupName = if ([string]::IsNullOrEmpty($agentGroupName)) { "N/A" } else { [string]$agentGroupName }
            $overflowSharedVoicemailGroupName = if ([string]::IsNullOrEmpty($overflowSharedVoicemailGroupName)) { "N/A" } else { [string]$overflowSharedVoicemailGroupName }
            $timeoutSharedVoicemailGroupName = if ([string]::IsNullOrEmpty($timeoutSharedVoicemailGroupName)) { "N/A" } else { [string]$timeoutSharedVoicemailGroupName }
            $noAgentsSharedVoicemailGroupName = if ([string]::IsNullOrEmpty($noAgentsSharedVoicemailGroupName)) { "N/A" } else { [string]$noAgentsSharedVoicemailGroupName }
            $language = if ([string]::IsNullOrEmpty($language)) { "en-US" } else { [string]$language }
            $overflowGreetingText = if ([string]::IsNullOrEmpty($overflowGreetingText)) { "N/A" } else { [string]$overflowGreetingText }
            $timeoutGreetingText = if ([string]::IsNullOrEmpty($timeoutGreetingText)) { "N/A" } else { [string]$timeoutGreetingText }
            $noAgentsGreetingText = if ([string]::IsNullOrEmpty($noAgentsGreetingText)) { "N/A" } else { [string]$noAgentsGreetingText }
            $authorizedUsers = if ([string]::IsNullOrEmpty($authorizedUsers)) { "N/A" } else { [string]$authorizedUsers }
            $phoneNumber = if ([string]::IsNullOrEmpty($phoneNumber)) { "N/A" } else { [string]$phoneNumber }
            
            # Final safety check - ensure all values are strings and not arrays
            if ($agentGroupName -is [Array]) { $agentGroupName = $agentGroupName[0] }
            if ($overflowSharedVoicemailGroupName -is [Array]) { $overflowSharedVoicemailGroupName = $overflowSharedVoicemailGroupName[0] }
            if ($timeoutSharedVoicemailGroupName -is [Array]) { $timeoutSharedVoicemailGroupName = $timeoutSharedVoicemailGroupName[0] }
            if ($noAgentsSharedVoicemailGroupName -is [Array]) { $noAgentsSharedVoicemailGroupName = $noAgentsSharedVoicemailGroupName[0] }
            if ($callerIdPhoneNumber -is [Array]) { $callerIdPhoneNumber = $callerIdPhoneNumber[0] }
            
            # Insert SQL command - Updated to include CallerIDPhoneNumber column
            $query = "INSERT INTO $Table (CallQueueName, ResourceAccountUPN, CallerIDResourceAccountUPN, CallerIDPhoneNumber, AgentGroupID, " +
                     "OverflowSharedVoicemailID, TimeoutSharedVoicemailID, NoAgentsSharedVoicemailID, Language, " +
                     "OverflowGreetingText, TimeoutGreetingText, NoAgentsGreetingText, UseDefaultMusicOnHold, " +
                     "AgentAlertTime, OverflowMaxCalls, TimeoutSeconds, AuthorizedUsers, PhoneNumber) " +
                     "VALUES (@CallQueueName, @ResourceAccountUPN, @CallerIDResourceAccountUPN, @CallerIDPhoneNumber, @AgentGroupID, " +
                     "@OverflowSharedVoicemailID, @TimeoutSharedVoicemailID, @NoAgentsSharedVoicemailID, @Language, " +
                     "@OverflowGreetingText, @TimeoutGreetingText, @NoAgentsGreetingText, @UseDefaultMusicOnHold, " +
                     "@AgentAlertTime, @OverflowMaxCalls, @TimeoutSeconds, @AuthorizedUsers, @PhoneNumber)"

            $cmd = $SQLConnection.CreateCommand()
            $cmd.CommandText = $query
            
            # Add parameters with proper type conversions - using group names instead of GUIDs
            $cmd.Parameters.AddWithValue("@CallQueueName", $callQueueName) | Out-Null
            $cmd.Parameters.AddWithValue("@ResourceAccountUPN", $resourceAccountUPN) | Out-Null
            $cmd.Parameters.AddWithValue("@CallerIDResourceAccountUPN", $callerIdResourceAccountUPN) | Out-Null
            $cmd.Parameters.AddWithValue("@CallerIDPhoneNumber", $callerIdPhoneNumber) | Out-Null
            $cmd.Parameters.AddWithValue("@AgentGroupID", $agentGroupName) | Out-Null
            $cmd.Parameters.AddWithValue("@OverflowSharedVoicemailID", $overflowSharedVoicemailGroupName) | Out-Null
            $cmd.Parameters.AddWithValue("@TimeoutSharedVoicemailID", $timeoutSharedVoicemailGroupName) | Out-Null
            $cmd.Parameters.AddWithValue("@NoAgentsSharedVoicemailID", $noAgentsSharedVoicemailGroupName) | Out-Null
            $cmd.Parameters.AddWithValue("@Language", $language) | Out-Null
            $cmd.Parameters.AddWithValue("@OverflowGreetingText", $overflowGreetingText) | Out-Null
            $cmd.Parameters.AddWithValue("@TimeoutGreetingText", $timeoutGreetingText) | Out-Null
            $cmd.Parameters.AddWithValue("@NoAgentsGreetingText", $noAgentsGreetingText) | Out-Null
            $cmd.Parameters.AddWithValue("@PhoneNumber", $phoneNumber) | Out-Null
            
            # Convert boolean and integer values to proper types
            $useDefaultMusicOnHold = if ($cq.UseDefaultMusicOnHold -eq $true) { 1 } else { 0 }
            $cmd.Parameters.AddWithValue("@UseDefaultMusicOnHold", $useDefaultMusicOnHold) | Out-Null
            
            $agentAlertTime = if ($null -ne $cq.AgentAlertTime) { [int]$cq.AgentAlertTime } else { 0 }
            $cmd.Parameters.AddWithValue("@AgentAlertTime", $agentAlertTime) | Out-Null
            
            $overflowMaxCalls = if ($null -ne $cq.OverflowThreshold) { [int]$cq.OverflowThreshold } else { 0 }
            $cmd.Parameters.AddWithValue("@OverflowMaxCalls", $overflowMaxCalls) | Out-Null
            
            $timeoutSeconds = if ($null -ne $cq.TimeoutThreshold) { [int]$cq.TimeoutThreshold } else { 0 }
            $cmd.Parameters.AddWithValue("@TimeoutSeconds", $timeoutSeconds) | Out-Null
            
            $cmd.Parameters.AddWithValue("@AuthorizedUsers", $authorizedUsers) | Out-Null
            
            $cmd.ExecuteNonQuery() | Out-Null
            Write-Output "Inserted: $($cq.Name) - Agent Group: $agentGroupName, Phone: $phoneNumber, Caller ID Phone: $callerIdPhoneNumber"
            $processed++
        }
        catch {
            $errorMessage = $_.Exception.Message
            Write-Warning "Insert failed for $($cq.Name) - $errorMessage"
            Write-Warning "Stack Trace: $($_.ScriptStackTrace)"
        }
    }
}
catch {
    $errorMessage = $_.Exception.Message
    Write-Error "Error retrieving call queues: $errorMessage"
    throw
}

Write-Output "Call queue export completed. Total processed: $processed"

if ($SQLConnection.State -eq 'Open') {
    $SQLConnection.Close()
    Write-Output "SQL connection closed"
}

# Disconnect from Teams
Disconnect-MicrosoftTeams -Confirm:$false
Write-Output "Disconnected from Teams. Script execution completed."
