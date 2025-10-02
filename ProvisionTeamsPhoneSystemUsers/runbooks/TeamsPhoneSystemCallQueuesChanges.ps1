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
            return $groupName
        }
        
        # Fallback to try as a distribution list via PowerShell cmdlets
        $distributionList = Get-DistributionGroup -Identity $GroupGUID -ErrorAction SilentlyContinue
        if ($distributionList -and $distributionList.DisplayName) {
            $groupName = [string]$distributionList.DisplayName
            return $groupName
        }
        
        # Additional fallback to try as a unified group
        $unifiedGroup = Get-UnifiedGroup -Identity $GroupGUID -ErrorAction SilentlyContinue
        if ($unifiedGroup -and $unifiedGroup.DisplayName) {
            $groupName = [string]$unifiedGroup.DisplayName
            return $groupName
        }
        
        return "Unknown Group ($GroupGUID)"
        
    } catch {
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
        # Try to get the user using Get-CsOnlineUser
        $user = Get-CsOnlineUser -Identity $UPN -ErrorAction SilentlyContinue
        
        if ($user) {
            if ($user.LineURI) {
                # Line URI is typically in format tel:+1234567890;ext=123
                # Extract just the number
                $phoneNumber = $user.LineURI
                $phoneNumber = $phoneNumber -replace "tel:", ""
                $phoneNumber = $phoneNumber -replace ";ext=\d+", ""
                return $phoneNumber
            }
        }
        
        # Fallback to Graph API if needed
        $encodedUPN = [System.Web.HttpUtility]::UrlEncode($UPN)
        $userUri = "https://graph.microsoft.com/v1.0/users/$encodedUPN"
        $response = Invoke-RestMethod -Uri $userUri -Headers $GraphHeaders -Method Get -ErrorAction SilentlyContinue
        
        if ($response) {
            if ($response.businessPhones -and $response.businessPhones.Count -gt 0) {
                $businessPhone = $response.businessPhones[0]
                return $businessPhone
            }
        }
        
        return "N/A"
    }
    catch {
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
        return $null
    }
}

# Function to safely truncate text to fit database column limits
function ConvertTo-SafeTruncatedString {
    param(
        [Parameter(Mandatory=$false)]
        [object]$Value,
        [int]$MaxLength = 1000
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
            
            return $truncated
        }
        
        return $stringValue
    }
    catch {
        return "N/A"
    }
}

# Teams Call Queue Change Detection - Production Beta
# Monitors all call queues for changes and logs to database
# Auto-triggers call queue data refresh when changes are detected

# Configuration
$SQLServer = "msoevendor.database.windows.net"
$Database = "CEProjectData"
$SourceTable = "dbo.msoe_teams_phone_system_cqs"
$LogTable = "dbo.msoe_teams_phone_system_cqs_change_log"

# Auto-trigger configuration
$AutomationAccountName = "VendorAutomationAccount"
$ResourceGroupName = "Infrastructure"
$TargetRunbookName = "MSOE_Teams_Phone_System_Call_Queues"
$SubscriptionId = "fc7ad0bc-429f-488b-9488-3ed508182348"

# Required fields to monitor
$TrackedFields = @(
    "ResourceAccountUPN", 
    "CallerIDResourceAccountUPN", 
    "CallerIDPhoneNumber", 
    "AgentGroupID", 
    "OverflowSharedVoicemailID", 
    "TimeoutSharedVoicemailID",
    "NoAgentsSharedVoicemailID",
    "Language",
    "OverflowGreetingText", 
    "TimeoutGreetingText",
    "NoAgentsGreetingText",
    "UseDefaultMusicOnHold",
    "AgentAlertTime",
    "OverflowMaxCalls",
    "TimeoutSeconds",
    "AuthorizedUsers",
    "PhoneNumber"
)

Write-Output "=== Teams Call Queue Change Detection - Production Beta ==="
Write-Output "$(Get-Date): Starting change detection for all call queues..."

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

    # Get current SQL data for all call queues
    Write-Output "$(Get-Date): Loading current SQL data from $SourceTable..."
    $cmd = $connection.CreateCommand()
    $cmd.CommandTimeout = 60
    $cmd.CommandText = "SELECT * FROM $SourceTable"
    $reader = $cmd.ExecuteReader()
    $table = New-Object System.Data.DataTable
    $table.Load($reader)
    $reader.Close()

    if ($table.Rows.Count -eq 0) {
        Write-Output "$(Get-Date): ERROR - No call queues found in SQL database!"
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

    # Get all Teams resource accounts (both auto attendants and call queues)
    Write-Output "$(Get-Date): Retrieving all Teams resource accounts..."
    try {
        # First try using Get-CsOnlineUser with the department filter
        $resourceAccounts = Get-CsOnlineUser -Filter "Department -eq 'Microsoft Communication Application Instance'" -ErrorAction SilentlyContinue
        
        if ($resourceAccounts -and $resourceAccounts.Count -gt 0) {
            Write-Output "$(Get-Date): Found $($resourceAccounts.Count) Teams resource accounts using Get-CsOnlineUser"
        } else {
            # Fallback to Graph API if the cmdlet fails
            $filter = "department eq 'Microsoft Communication Application Instance'"
            $resourceAccountsUri = "https://graph.microsoft.com/v1.0/users?`$filter=$([System.Web.HttpUtility]::UrlEncode($filter))&`$top=999"
            $response = Invoke-RestMethod -Uri $resourceAccountsUri -Headers $GraphHeaders -Method Get
            
            if ($response -and $response.value) {
                $resourceAccounts = $response.value
                Write-Output "$(Get-Date): Found $($resourceAccounts.Count) Teams resource accounts via Graph API"
            } else {
                $resourceAccounts = @()
                Write-Output "$(Get-Date): No resource accounts found"
            }
        }
    } catch {
        Write-Warning "$(Get-Date): Error retrieving resource accounts: $_"
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

    # Retrieve all auto attendants to create a lookup for matching call queues
    Write-Output "$(Get-Date): Retrieving all auto attendants for call queue mapping..."
    try {
        $autoAttendants = Get-CsAutoAttendant -ErrorAction SilentlyContinue
        if ($autoAttendants) {
            Write-Output "$(Get-Date): Found $($autoAttendants.Count) auto attendants for potential call queue mapping"
        } else {
            Write-Output "$(Get-Date): No auto attendants found for call queue mapping"
            $autoAttendants = @()
        }
    } catch {
        Write-Output "$(Get-Date): Error retrieving auto attendants: $_"
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
            Write-Output "$(Get-Date): Created mapping between $($queueToAAMapping.Count) call queues and potential auto attendants"
        } else {
            Write-Output "$(Get-Date): No call queues found for AA mapping"
        }
    } catch {
        Write-Output "$(Get-Date): Error creating queue to AA mapping: $_"
    }

    # Get all call queues
    Write-Output "$(Get-Date): Retrieving all call queues..."
    $callQueues = Get-CsCallQueue -ErrorAction Stop
    
    if ($callQueues.Count -eq 0) {
        Write-Output "$(Get-Date): ERROR - No call queues found in Teams!"
        return
    }

    Write-Output "$(Get-Date): Found $($callQueues.Count) call queues to process"

    # Process each call queue with progress tracking
    $totalChanges = 0
    $queuesWithChanges = 0
    $processedCount = 0
    $errorCount = 0

    foreach ($cq in $callQueues) {
        $processedCount++
        $queueName = $cq.Name
        
        # Progress indicator every 10 call queues or for first queue
        if ($processedCount % 10 -eq 0 -or $processedCount -eq 1) {
            Write-Output "$(Get-Date): Processing call queue $processedCount of $($callQueues.Count) ($([math]::Round(($processedCount/$callQueues.Count)*100,1))%) - $queueName"
        }

        try {
            # Find corresponding SQL row
            $sqlRow = $table.Rows | Where-Object { $_["CallQueueName"] -eq $queueName }
            if (-not $sqlRow) {
                continue  # Skip call queues not in SQL database
            }

            # Initialize current values (same logic as bulk export)
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
            
            # Get overflow, timeout, no agents greeting text (truncate if too long)
            if ($cq.OverflowSharedVoicemailTextToSpeechPrompt) {
                $overflowGreetingText = ConvertTo-SafeTruncatedString -Value $cq.OverflowSharedVoicemailTextToSpeechPrompt -MaxLength 1000
            }
            
            if ($cq.TimeoutSharedVoicemailTextToSpeechPrompt) {
                $timeoutGreetingText = ConvertTo-SafeTruncatedString -Value $cq.TimeoutSharedVoicemailTextToSpeechPrompt -MaxLength 1000
            }
            
            if ($cq.NoAgentSharedVoicemailTextToSpeechPrompt) {
                $noAgentsGreetingText = ConvertTo-SafeTruncatedString -Value $cq.NoAgentSharedVoicemailTextToSpeechPrompt -MaxLength 1000
            }
            
            # First check if there's a static mapping for this call queue
            $matchedAccount = $null
            if ($staticQueueResourceMapping.ContainsKey($cq.Name)) {
                $raName = $staticQueueResourceMapping[$cq.Name]
                
                # Look up the resource account by name
                if ($resourceAccountsByName.ContainsKey($raName)) {
                    $matchedAccount = $resourceAccountsByName[$raName]
                }
            } elseif ($resourceAccountsByName.ContainsKey($cq.Name)) {
                # If no static mapping or it didn't find a match, try exact name matching
                $matchedAccount = $resourceAccountsByName[$cq.Name]
            }
            
            # Get the resource account UPN and phone number if we found a match
            if ($matchedAccount) {
                # Get UPN based on the object type
                if ($matchedAccount.PSObject.Properties.Name -contains "UserPrincipalName") {
                    $resourceAccountUPN = $matchedAccount.UserPrincipalName
                } elseif ($matchedAccount.PSObject.Properties.Name -contains "userPrincipalName") {
                    $resourceAccountUPN = $matchedAccount.userPrincipalName
                }
                
                # Get the phone number for this resource account
                if ($resourceAccountUPN -ne "N/A") {
                    $phoneNumber = Get-PhoneNumberFromUPN -UPN $resourceAccountUPN
                }
            }
            
            # Get Caller ID resource account - find the corresponding Auto Attendant
            if ($queueToAAMapping.ContainsKey($cq.Name)) {
                $aaName = $queueToAAMapping[$cq.Name]
                
                # Find the AA in the list of auto attendants
                $matchingAA = $autoAttendants | Where-Object { $_.Name -eq $aaName }
                if ($matchingAA) {
                    # Find the resource account for this AA
                    if ($resourceAccountsByName.ContainsKey($aaName)) {
                        $aaAccount = $resourceAccountsByName[$aaName]
                        
                        # Get the UPN
                        if ($aaAccount.PSObject.Properties.Name -contains "UserPrincipalName") {
                            $callerIdResourceAccountUPN = $aaAccount.UserPrincipalName
                        } elseif ($aaAccount.PSObject.Properties.Name -contains "userPrincipalName") {
                            $callerIdResourceAccountUPN = $aaAccount.userPrincipalName
                        }
                    }
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
                $callerIdPhoneNumber = Get-PhoneNumberFromUPN -UPN $callerIdResourceAccountUPN
            }
            
            # Get agent group name (convert GUID to group name)
            if ($cq.DistributionLists -and $cq.DistributionLists.Count -gt 0) {
                $agentGroupGUID = $cq.DistributionLists[0]
                $agentGroupName = Get-GroupNameFromGUID -GroupGUID $agentGroupGUID
            }
            
            # Handle Target objects properly for voicemail settings and convert GUIDs to group names
            if ($cq.OverflowAction -eq "SharedVoicemail" -and $cq.OverflowActionTarget) {
                $overflowSharedVoicemailGUID = Get-TargetIdAsString -Target $cq.OverflowActionTarget
                $overflowSharedVoicemailGroupName = Get-GroupNameFromGUID -GroupGUID $overflowSharedVoicemailGUID
            }
            
            if ($cq.TimeoutAction -eq "SharedVoicemail" -and $cq.TimeoutActionTarget) {
                $timeoutSharedVoicemailGUID = Get-TargetIdAsString -Target $cq.TimeoutActionTarget
                $timeoutSharedVoicemailGroupName = Get-GroupNameFromGUID -GroupGUID $timeoutSharedVoicemailGUID
            }
            
            if ($cq.NoAgentAction -eq "SharedVoicemail" -and $cq.NoAgentActionTarget) {
                $noAgentsSharedVoicemailGUID = Get-TargetIdAsString -Target $cq.NoAgentActionTarget
                $noAgentsSharedVoicemailGroupName = Get-GroupNameFromGUID -GroupGUID $noAgentsSharedVoicemailGUID
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
            $authorizedUsers = if ($authorizedUsersArray.Count -gt 0) { $authorizedUsersArray -join ";" } else { $null }

            # Create comparison map (ensure all values are properly converted to strings)
            $compareMap = @{
                "ResourceAccountUPN" = if ($resourceAccountUPN -and $resourceAccountUPN -ne "N/A") { [string]$resourceAccountUPN } else { "N/A" }
                "CallerIDResourceAccountUPN" = if ($callerIdResourceAccountUPN -and $callerIdResourceAccountUPN -ne "N/A") { [string]$callerIdResourceAccountUPN } else { "N/A" }
                "CallerIDPhoneNumber" = if ($callerIdPhoneNumber -and $callerIdPhoneNumber -ne "N/A") { [string]$callerIdPhoneNumber } else { "N/A" }
                "AgentGroupID" = if ($agentGroupName -and $agentGroupName -ne "N/A") { [string]$agentGroupName } else { "N/A" }
                "OverflowSharedVoicemailID" = if ($overflowSharedVoicemailGroupName -and $overflowSharedVoicemailGroupName -ne "N/A") { [string]$overflowSharedVoicemailGroupName } else { "N/A" }
                "TimeoutSharedVoicemailID" = if ($timeoutSharedVoicemailGroupName -and $timeoutSharedVoicemailGroupName -ne "N/A") { [string]$timeoutSharedVoicemailGroupName } else { "N/A" }
                "NoAgentsSharedVoicemailID" = if ($noAgentsSharedVoicemailGroupName -and $noAgentsSharedVoicemailGroupName -ne "N/A") { [string]$noAgentsSharedVoicemailGroupName } else { "N/A" }
                "Language" = if ($language -and $language -ne "N/A") { [string]$language } else { "N/A" }
                "OverflowGreetingText" = if ($overflowGreetingText -and $overflowGreetingText -ne "N/A") { [string]$overflowGreetingText } else { "N/A" }
                "TimeoutGreetingText" = if ($timeoutGreetingText -and $timeoutGreetingText -ne "N/A") { [string]$timeoutGreetingText } else { "N/A" }
                "NoAgentsGreetingText" = if ($noAgentsGreetingText -and $noAgentsGreetingText -ne "N/A") { [string]$noAgentsGreetingText } else { "N/A" }
                "UseDefaultMusicOnHold" = if ($cq.UseDefaultMusicOnHold -eq $true) { "1" } else { "0" }
                "AgentAlertTime" = if ($null -ne $cq.AgentAlertTime) { [string]$cq.AgentAlertTime } else { "0" }
                "OverflowMaxCalls" = if ($null -ne $cq.OverflowThreshold) { [string]$cq.OverflowThreshold } else { "0" }
                "TimeoutSeconds" = if ($null -ne $cq.TimeoutThreshold) { [string]$cq.TimeoutThreshold } else { "0" }
                "AuthorizedUsers" = if ($authorizedUsers -and $authorizedUsers -ne "") { [string]$authorizedUsers } else { "N/A" }
                "PhoneNumber" = if ($phoneNumber -and $phoneNumber -ne "N/A") { [string]$phoneNumber } else { "N/A" }
            }

            # Compare fields and detect changes
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
                $queuesWithChanges++
                $totalChanges += $changedFields.Keys.Count
                Write-Output "$(Get-Date): Change detected for $queueName. Fields changed: $($changedFields.Keys -join ', ')"
                
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
                    Write-Error "$(Get-Date): Failed to log changes for $queueName : $($_.Exception.Message)"
                }
                finally {
                    if ($insertCmd) { $insertCmd.Dispose() }
                    if ($transaction) { $transaction.Dispose() }
                }
            }
        }
        catch {
            $errorCount++
            Write-Error "$(Get-Date): Failed to process call queue $queueName : $($_.Exception.Message)"
            continue
        }
    }

    # Final summary first
    Write-Output ""
    Write-Output "=== FINAL SUMMARY ==="
    Write-Output "$(Get-Date): Processing complete!"
    Write-Output "$(Get-Date): Total call queues processed: $($callQueues.Count)"
    Write-Output "$(Get-Date): Call queues with changes: $queuesWithChanges"
    Write-Output "$(Get-Date): Total field changes detected: $totalChanges"
    Write-Output "$(Get-Date): All change log SQL insertions completed successfully"

    # Auto-trigger call queue data refresh ONLY after all processing and SQL insertions are complete
    if ($queuesWithChanges -gt 0) {
        Write-Output ""
        Write-Output "=== AUTO-TRIGGERING CALL QUEUE DATA REFRESH ==="
        Write-Output "$(Get-Date): All change detection and logging complete"
        Write-Output "$(Get-Date): Changes detected ($queuesWithChanges call queues with $totalChanges total changes)"
        Write-Output "$(Get-Date): Now triggering runbook '$TargetRunbookName' to refresh call queue data..."
        
        try {
            # Use the exact same simple approach as the working auto attendant script
            $job = Start-AzAutomationRunbook -AutomationAccountName $AutomationAccountName `
                                          -ResourceGroupName $ResourceGroupName `
                                          -Name $TargetRunbookName

            if ($job -and $job.JobId) {
                Write-Output "$(Get-Date): Call queue data refresh runbook started successfully!"
                Write-Output "$(Get-Date): Job ID: $($job.JobId)"
                Write-Output "$(Get-Date): Job Status: $($job.Status)"
                Write-Output "$(Get-Date): This will refresh the call queue data to reflect the detected changes."
            } else {
                Write-Warning "$(Get-Date): Call queue data refresh runbook may not have started properly - no job object returned"
            }
        }
        catch {
            Write-Error "$(Get-Date): Failed to trigger call queue data refresh runbook: $($_.Exception.Message)"
            Write-Output "$(Get-Date): You may need to manually run '$TargetRunbookName' to update the call queue data"
        }
    } else {
        Write-Output ""
        Write-Output "$(Get-Date): No changes detected - call queue data refresh not needed"
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
    
    Write-Output "$(Get-Date): Call queue change detection complete!"
}