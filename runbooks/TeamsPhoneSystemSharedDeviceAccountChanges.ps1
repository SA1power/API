# Function to get Group information (for Call Queue membership)
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
            
            # Modified to include all members regardless of owner status
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
                
                # Also modified here for pagination
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
        } else {
            Write-Output "Could not retrieve group information for group ID: $GroupId"
        }
    }
    catch {
        # using concatenation to avoid interpolation issues
        Write-Output ("Error retrieving group info for " + $GroupId + ": " + $_)
    }
    return $result
}

# Function to get location name for a user
function Get-UserLocationName {
    param(
        [Parameter(Mandatory=$true)]
        [string] $UserUpn
    )
    
    $locationName = $null
    
    # Try Get-CsOnlineUser to get the user's information
    try {
        $user = Get-CsOnlineUser -Identity $UserUpn -ErrorAction Stop -WarningAction SilentlyContinue
        
        if ($user -and $user.LineUri) {
            $telNumber = $user.LineUri -replace "tel:", ""
            
            # Get the phone number assignment details
            $phoneNumberInfo = Get-CsPhoneNumberAssignment -TelephoneNumber $telNumber -ErrorAction SilentlyContinue -WarningAction SilentlyContinue
            
            if ($phoneNumberInfo -and $phoneNumberInfo.LocationId) {
                $locationId = $phoneNumberInfo.LocationId
                
                # Look up this location ID
                $location = Get-CsOnlineLisLocation -WarningAction SilentlyContinue | Where-Object { $_.LocationId -eq $locationId }
                if ($location -and $location.CompanyName) {
                    $locationName = $location.CompanyName
                    Write-Verbose "Found location for $UserUpn - $locationName"
                }
            }
        }
    }
    catch {
        Write-Verbose "Error getting location for $UserUpn - $_"
    }
    
    return $locationName
}

# Helper function to safely extract string value from policy objects
function Get-PolicyStringValue {
    param (
        [Parameter(Mandatory=$false)]
        $PolicyObject
    )
    
    if ($null -eq $PolicyObject) {
        return $null
    }
    
    # If it's already a string, return it
    if ($PolicyObject -is [string]) {
        return $PolicyObject
    }
    
    # If it's an object with Identity property
    if ($PolicyObject -is [System.Object] -and ($PolicyObject.PSObject.Properties.Name -contains "Identity")) {
        return $PolicyObject.Identity.ToString()
    }
    
    # If it's an object with Name property
    if ($PolicyObject -is [System.Object] -and ($PolicyObject.PSObject.Properties.Name -contains "Name")) {
        return $PolicyObject.Name.ToString()
    }
    
    # As a last resort, convert to string
    return $PolicyObject.ToString()
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

# Function to log a change or new/deleted account to the database
function Write-ChangeToDatabase {
    param(
        [Parameter(Mandatory=$true)]
        [System.Data.SqlClient.SqlConnection] $Connection,
        [Parameter(Mandatory=$true)]
        [string] $LogTable,
        [Parameter(Mandatory=$true)]
        [array] $Columns,
        [Parameter(Mandatory=$true)]
        [hashtable] $AccountData,
        [Parameter(Mandatory=$true)]
        [string] $ChangeType,
        [Parameter(Mandatory=$false)]
        [hashtable] $ChangedFields = @{}
    )
    
    $centralTime = [System.TimeZoneInfo]::ConvertTimeBySystemTimeZoneId((Get-Date), "Central Standard Time")
    $now = $centralTime.ToString("yyyy-MM-dd HH:mm:ss")
    $transaction = $Connection.BeginTransaction()
    
    try {
        # Build the INSERT command with proper parameterization
        $insertCmd = $Connection.CreateCommand()
        $insertCmd.Transaction = $transaction
        $insertCmd.CommandTimeout = 30
        
        # Build column and parameter lists
        $colList = @()
        $paramList = @()
        
        for ($i = 0; $i -lt $Columns.Count; $i++) {
            $colList += "[$($Columns[$i])]"  # Bracket column names for SQL
            $paramList += "@param$i"         # Use numbered parameters
        }
        $colList += "[ChangeType]", "[ChangeDate]"
        $paramList += "@paramChangeType", "@paramChangeDate"
        
        $insertCmd.CommandText = "INSERT INTO $LogTable ($($colList -join ', ')) VALUES ($($paramList -join ', '))"
        
        # Add parameters
        for ($i = 0; $i -lt $Columns.Count; $i++) {
            $colName = $Columns[$i]
            
            if ($ChangeType -eq "change" -and $ChangedFields.ContainsKey($colName)) {
                # Format: "original -> 'oldValue' | change -> 'newValue'"
                $oldValue = $ChangedFields[$colName].old
                $newValue = $ChangedFields[$colName].new
                $formattedValue = "original -> '$oldValue' | change -> '$newValue'"
                $insertCmd.Parameters.AddWithValue("@param$i", $formattedValue) | Out-Null
            } else {
                # For new/deleted accounts or unchanged fields, use the account data
                $value = if ($AccountData.ContainsKey($colName)) { 
                    if ($AccountData[$colName] -eq $null -or $AccountData[$colName] -eq "") { 
                        [DBNull]::Value 
                    } else { 
                        $AccountData[$colName] 
                    }
                } else { 
                    [DBNull]::Value 
                }
                $insertCmd.Parameters.AddWithValue("@param$i", $value) | Out-Null
            }
        }
        $insertCmd.Parameters.AddWithValue("@paramChangeType", $ChangeType) | Out-Null
        $insertCmd.Parameters.AddWithValue("@paramChangeDate", $now) | Out-Null
        
        # Execute the insert
        $insertCmd.ExecuteNonQuery() | Out-Null
        
        # Commit the transaction
        $transaction.Commit()
        return $true
    }
    catch {
        if ($transaction) { $transaction.Rollback() }
        Write-Error "Failed to log $ChangeType for account: $($_.Exception.Message)"
        return $false
    }
    finally {
        if ($insertCmd) { $insertCmd.Dispose() }
        if ($transaction) { $transaction.Dispose() }
    }
}

# Function to build account data hashtable from live account
function Get-AccountDataFromLive {
    param(
        [Parameter(Mandatory=$true)]
        $Account,
        [Parameter(Mandatory=$true)]
        [hashtable] $DeviceCallQueueMemberships,
        [Parameter(Mandatory=$true)]
        [array] $Columns
    )
    
    $upn = $Account.UserPrincipalName
    $lineURI = $Account.LineURI
    $displayName = $Account.DisplayName
    
    # Extract policy values using the helper function
    $teamsIPPhonePolicy = Get-PolicyStringValue -PolicyObject $Account.TeamsIPPhonePolicy
    $callingPolicy = Get-PolicyStringValue -PolicyObject $Account.TeamsCallingPolicy
    $callerIdPolicy = Get-PolicyStringValue -PolicyObject $Account.CallingLineIdentity
    
    # Check if this device is a member of any call queues
    $callQueueMembership = if ($DeviceCallQueueMemberships.ContainsKey($upn)) {
        $DeviceCallQueueMemberships[$upn] -join ";"
    } else {
        $null
    }
    
    # Get the Location ID for this user
    $locationName = Get-UserLocationName -UserUpn $upn
    
    # Build account data hashtable matching SQL column structure
    $accountData = @{}
    
    # Map the live data to column names (adjust these based on your actual SQL column names)
    foreach ($col in $Columns) {
        switch ($col) {
            "UPN" { $accountData[$col] = $upn }
            "Display_Name" { $accountData[$col] = if ($displayName) { [string]$displayName } else { "N/A" } }
            "Line_URI" { $accountData[$col] = if ($lineURI) { [string]$lineURI } else { "N/A" } }
            "TeamsIPPhonePolicy" { $accountData[$col] = if ($teamsIPPhonePolicy) { [string]$teamsIPPhonePolicy } else { "N/A" } }
            "Calling Policy" { $accountData[$col] = if ($callingPolicy) { [string]$callingPolicy } else { "N/A" } }
            "Caller ID Policy" { $accountData[$col] = if ($callerIdPolicy) { [string]$callerIdPolicy } else { "N/A" } }
            "Call Queue Membership" { $accountData[$col] = if ($callQueueMembership) { [string]$callQueueMembership } else { "N/A" } }
            "Location ID" { $accountData[$col] = if ($locationName) { [string]$locationName } else { "N/A" } }
            default { $accountData[$col] = "N/A" }  # Default for any other columns
        }
    }
    
    return $accountData
}

# Teams Shared Device Accounts Change Detection - Production Beta (REVISED)
# Now detects NEW accounts, DELETED accounts, and CHANGED accounts
# Auto-triggers shared device account data refresh when changes are detected

# Configuration
$SQLServer = "msoevendor.database.windows.net"
$Database = "CEProjectData"
$SourceTable = "dbo.MSOE_Teams_Phone_System_Shared_Device_Accounts"
$LogTable = "dbo.MSOE_Teams_Phone_System_Shared_Device_Accounts_Change_Log"

# Auto-trigger configuration
$AutomationAccountName = "VendorAutomationAccount"
$ResourceGroupName = "Infrastructure"
$TargetRunbookName = "MSOE_Teams_Phone_System_Shared_Device_Accounts"
$SubscriptionId = "fc7ad0bc-429f-488b-9488-3ed508182348"

# Required fields to monitor
$TrackedFields = @(
    "Display_Name", 
    "Line_URI", 
    "TeamsIPPhonePolicy", 
    "Calling Policy", 
    "Caller ID Policy", 
    "Call Queue Membership",
    "Location ID"
)

Write-Output "=== Teams Shared Device Accounts Change Detection - Production Beta (REVISED) ==="
Write-Output "$(Get-Date): Starting comprehensive change detection (NEW/CHANGED/DELETED accounts)..."

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

    # Get current SQL data for all shared device accounts
    Write-Output "$(Get-Date): Loading current SQL data from $SourceTable..."
    $cmd = $connection.CreateCommand()
    $cmd.CommandTimeout = 60
    $cmd.CommandText = "SELECT * FROM $SourceTable"
    $reader = $cmd.ExecuteReader()
    $table = New-Object System.Data.DataTable
    $table.Load($reader)
    $reader.Close()

    Write-Output "$(Get-Date): Loaded $($table.Rows.Count) records from SQL"

    # Get column names for logging
    $columns = $table.Columns | ForEach-Object { $_.ColumnName }

    # Get access token for Microsoft Graph API
    Write-Output "$(Get-Date): Getting Graph API token..."
    $GraphAccessToken = Get-AzAccessToken -ResourceUrl "https://graph.microsoft.com"
    $GraphToken = $GraphAccessToken.Token
    $GraphHeaders = @{
        "Authorization" = "Bearer $GraphToken"
        "Content-Type" = "application/json"
    }

    # ===== CALL QUEUE SECTION: GET CALL QUEUE GROUPS AND THEIR MEMBERS =====
    Write-Output "$(Get-Date): Retrieving all call queues to extract group IDs and members..."

    # Hashtable to store device-to-call-queue mappings
    $deviceCallQueueMemberships = @{}

    try {
        $callQueues = Get-CsCallQueue -ErrorAction Stop -WarningAction SilentlyContinue
        if ($callQueues) {
            Write-Output "$(Get-Date): Found $($callQueues.Count) call queues to check for groups"
        } else {
            Write-Output "$(Get-Date): No call queues found"
            $callQueues = @()
        }
    } catch {
        Write-Output "$(Get-Date): Error retrieving call queues: $_"
        $callQueues = @()
    }

    # Process each call queue to extract distribution lists and members
    $processedGroups = @{}

    foreach ($cq in $callQueues) {
        if ($cq.DistributionLists -and $cq.DistributionLists.Count -gt 0) {
            foreach ($groupId in $cq.DistributionLists) {
                if ($processedGroups.ContainsKey($groupId)) {
                    continue
                }
                
                $groupInfo = Get-GroupInfo -GroupId $groupId -Headers $GraphHeaders
                
                if ($groupInfo.DisplayName -eq "N/A" -and $groupInfo.Email -eq "N/A" -and $groupInfo.Members.Count -eq 0) {
                    continue
                }
                
                $processedGroups[$groupId] = $true
                
                # For each member of this group, add this call queue to their memberships
                foreach ($member in $groupInfo.Members) {
                    # Look for shared device accounts (UPN starting with tsd_)
                    if ($member -like "tsd_*") {
                        if (-not $deviceCallQueueMemberships.ContainsKey($member)) {
                            $deviceCallQueueMemberships[$member] = @()
                        }
                        
                        # Add the call queue name to the device's list of memberships if not already there
                        if (-not $deviceCallQueueMemberships[$member].Contains($cq.Name)) {
                            $deviceCallQueueMemberships[$member] += $cq.Name
                        }
                    }
                }
            }
        }
    }
    Write-Output "$(Get-Date): Processed call queue memberships for $($deviceCallQueueMemberships.Count) shared device accounts"
    # ===== END OF CALL QUEUE SECTION =====

    # Get all shared device accounts from Teams
    Write-Output "$(Get-Date): Retrieving all shared device accounts from Teams..."
    $sharedDeviceAccounts = Get-CsOnlineUser -WarningAction SilentlyContinue | Where-Object { $_.UserPrincipalName -like "tsd_*" }
    
    if ($null -eq $sharedDeviceAccounts) {
        $sharedDeviceAccounts = @()
    }

    $liveAccountCount = @($sharedDeviceAccounts).Count
    $sqlAccountCount = $table.Rows.Count
    Write-Output "$(Get-Date): Found $liveAccountCount shared device accounts in Teams"
    Write-Output "$(Get-Date): Found $sqlAccountCount shared device accounts in SQL"

    # Initialize counters
    $totalChanges = 0
    $newAccounts = 0
    $deletedAccounts = 0
    $changedAccounts = 0
    $processedCount = 0
    $errorCount = 0

    # Create lookup hashtables for efficient comparison
    $liveAccountsHash = @{}
    foreach ($account in $sharedDeviceAccounts) {
        $liveAccountsHash[$account.UserPrincipalName] = $account
    }

    $sqlAccountsHash = @{}
    foreach ($row in $table.Rows) {
        $sqlAccountsHash[$row["UPN"]] = $row
    }

    Write-Output ""
    Write-Output "=== PHASE 1: DETECTING NEW AND CHANGED ACCOUNTS ==="

    # Process each live shared device account
    foreach ($account in $sharedDeviceAccounts) {
        $processedCount++
        $upn = $account.UserPrincipalName
        
        # Progress indicator every 10 accounts or for first account
        if ($processedCount % 10 -eq 0 -or $processedCount -eq 1) {
            Write-Output "$(Get-Date): Processing account $processedCount of $liveAccountCount ($([math]::Round(($processedCount/$liveAccountCount)*100,1))%) - $upn"
        }

        try {
            # Check if this account exists in SQL
            $sqlRow = $sqlAccountsHash[$upn]
            
            if (-not $sqlRow) {
                # NEW ACCOUNT DETECTED
                $newAccounts++
                Write-Output "$(Get-Date): NEW ACCOUNT DETECTED: $upn"
                
                # Get account data for logging
                $accountData = Get-AccountDataFromLive -Account $account -DeviceCallQueueMemberships $deviceCallQueueMemberships -Columns $columns
                
                # Log the new account
                $success = Write-ChangeToDatabase -Connection $connection -LogTable $LogTable -Columns $columns -AccountData $accountData -ChangeType "new"
                if ($success) {
                    Write-Output "$(Get-Date):   Successfully logged new account: $upn"
                } else {
                    $errorCount++
                    Write-Output "$(Get-Date):   Failed to log new account: $upn"
                }
                
                continue
            }

            # EXISTING ACCOUNT - CHECK FOR CHANGES
            # Get current live values
            $accountData = Get-AccountDataFromLive -Account $account -DeviceCallQueueMemberships $deviceCallQueueMemberships -Columns $columns

            # Compare tracked fields and detect changes
            $changed = $false
            $changedFields = @{}

            foreach ($field in $TrackedFields) {
                # Handle DBNull values from SQL (convert to "N/A" to match export script logic)
                $sqlValue = if ($sqlRow[$field] -eq [DBNull]::Value -or $sqlRow[$field] -eq $null) { 
                    "N/A" 
                } else { 
                    $sqlRow[$field].ToString().Trim()
                }
                
                $newValue = if ($accountData[$field]) { 
                    $accountData[$field].ToString().Trim() 
                } else { 
                    "N/A"
                }

                # Improved comparison logic to handle "N/A" values consistently
                $valuesAreDifferent = $false
                
                # Normalize both values for comparison (treat null, empty, and "N/A" as equivalent)
                $sqlNormalized = if ([string]::IsNullOrWhiteSpace($sqlValue) -or $sqlValue -eq "N/A") { "N/A" } else { $sqlValue }
                $newNormalized = if ([string]::IsNullOrWhiteSpace($newValue) -or $newValue -eq "N/A") { "N/A" } else { $newValue }
                
                # Special handling for Call Queue Membership - sort lists for comparison
                if ($field -eq "Call Queue Membership") {
                    $sqlArray = if ($sqlNormalized -eq "N/A") { @() } else { ($sqlNormalized -split ";") | Sort-Object }
                    $newArray = if ($newNormalized -eq "N/A") { @() } else { ($newNormalized -split ";") | Sort-Object }
                    
                    $sqlSorted = if ($sqlArray.Count -eq 0) { "N/A" } else { $sqlArray -join ";" }
                    $newSorted = if ($newArray.Count -eq 0) { "N/A" } else { $newArray -join ";" }
                    
                    if ($sqlSorted -ne $newSorted) {
                        $valuesAreDifferent = $true
                    }
                } else {
                    if ($sqlNormalized -ne $newNormalized) {
                        $valuesAreDifferent = $true
                    }
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
                $changedAccounts++
                $totalChanges += $changedFields.Keys.Count
                Write-Output "$(Get-Date): CHANGES DETECTED for $upn. Fields changed: $($changedFields.Keys -join ', ')"
                
                # Log the changes
                $success = Write-ChangeToDatabase -Connection $connection -LogTable $LogTable -Columns $columns -AccountData $accountData -ChangeType "change" -ChangedFields $changedFields
                if ($success) {
                    # Log the specific changes
                    foreach ($field in $changedFields.Keys) {
                        $change = $changedFields[$field]
                        Write-Output "$(Get-Date):   ${field}: '$($change.old)' -> '$($change.new)'"
                    }
                } else {
                    $errorCount++
                }
            }
        }
        catch {
            $errorCount++
            Write-Error "$(Get-Date): Failed to process shared device account $upn : $($_.Exception.Message)"
            continue
        }
    }

    Write-Output ""
    Write-Output "=== PHASE 2: DETECTING DELETED ACCOUNTS ==="

    # Check for accounts in SQL that no longer exist in Teams (DELETED ACCOUNTS)
    foreach ($row in $table.Rows) {
        $sqlUpn = $row["UPN"]
        
        if (-not $liveAccountsHash.ContainsKey($sqlUpn)) {
            # DELETED ACCOUNT DETECTED
            $deletedAccounts++
            Write-Output "$(Get-Date): DELETED ACCOUNT DETECTED: $sqlUpn"
            
            # Build account data from SQL row for logging
            $accountData = @{}
            foreach ($col in $columns) {
                $accountData[$col] = if ($row[$col] -eq [DBNull]::Value) { "N/A" } else { $row[$col] }
            }
            
            # Log the deleted account
            $success = Write-ChangeToDatabase -Connection $connection -LogTable $LogTable -Columns $columns -AccountData $accountData -ChangeType "deleted"
            if ($success) {
                Write-Output "$(Get-Date):   Successfully logged deleted account: $sqlUpn"
            } else {
                $errorCount++
                Write-Output "$(Get-Date):   Failed to log deleted account: $sqlUpn"
            }
        }
    }

    # Final summary
    Write-Output ""
    Write-Output "=== FINAL SUMMARY ==="
    Write-Output "$(Get-Date): Processing complete!"
    Write-Output "$(Get-Date): Live accounts in Teams: $liveAccountCount"
    Write-Output "$(Get-Date): Stored accounts in SQL: $sqlAccountCount"
    Write-Output "$(Get-Date): NEW accounts detected: $newAccounts"
    Write-Output "$(Get-Date): CHANGED accounts detected: $changedAccounts"
    Write-Output "$(Get-Date): DELETED accounts detected: $deletedAccounts"
    Write-Output "$(Get-Date): Total field changes detected: $totalChanges"
    Write-Output "$(Get-Date): Errors encountered: $errorCount"

    $totalImpactedAccounts = $newAccounts + $changedAccounts + $deletedAccounts

    # Auto-trigger shared device account data refresh if any changes detected
    if ($totalImpactedAccounts -gt 0) {
        Write-Output ""
        Write-Output "=== AUTO-TRIGGERING SHARED DEVICE ACCOUNT DATA REFRESH ==="
        Write-Output "$(Get-Date): All change detection and logging complete"
        Write-Output "$(Get-Date): Changes detected ($totalImpactedAccounts total impacted accounts)"
        Write-Output "$(Get-Date): Now triggering runbook '$TargetRunbookName' to refresh shared device account data..."
        
        try {
            $job = Start-AzAutomationRunbook -AutomationAccountName $AutomationAccountName `
                                          -ResourceGroupName $ResourceGroupName `
                                          -Name $TargetRunbookName

            if ($job -and $job.JobId) {
                Write-Output "$(Get-Date): Shared device account data refresh runbook started successfully!"
                Write-Output "$(Get-Date): Job ID: $($job.JobId)"
                Write-Output "$(Get-Date): Job Status: $($job.Status)"
                Write-Output "$(Get-Date): This will refresh the shared device account data to reflect all detected changes."
            } else {
                Write-Warning "$(Get-Date): Shared device account data refresh runbook may not have started properly - no job object returned"
            }
        }
        catch {
            Write-Error "$(Get-Date): Failed to trigger shared device account data refresh runbook: $($_.Exception.Message)"
            Write-Output "$(Get-Date): You may need to manually run '$TargetRunbookName' to update the shared device account data"
        }
    } else {
        Write-Output ""
        Write-Output "$(Get-Date): No changes detected - shared device account data refresh not needed"
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
    
    Write-Output "$(Get-Date): Comprehensive shared device account change detection complete!"
}