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

# Teams User Change Detection - Production Beta
# Monitors all voice-enabled users for changes and logs to database
# Auto-triggers user data refresh when changes are detected

# Configuration
$SQLServer = "msoevendor.database.windows.net"
$Database = "CEProjectData"
$SourceTable = "dbo.MSOE_Teams_Phone_System_Users"
$LogTable = "dbo.MSOE_Teams_Phone_System_Users_Change_Log"

# Auto-trigger configuration
$AutomationAccountName = "VendorAutomationAccount"
$ResourceGroupName = "Infrastructure"
$TargetRunbookName = "MSOE_Teams_Phone_System_Users_Basic"
$SubscriptionId = "fc7ad0bc-429f-488b-9488-3ed508182348"

# Required fields to monitor
$TrackedFields = @(
    "Line_URI", 
    "Dial Plan", 
    "Location ID", 
    "Emergency Call Routing Policy", 
    "Emergency Calling Policy", 
    "Voice Routing Policy",
    "Caller ID Policy",
    "Voice Applications Policy", 
    "Call Queue Membership"
)

Write-Output "=== Teams User Change Detection - Production Beta ==="
Write-Output "$(Get-Date): Starting change detection for all voice-enabled users..."

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

    # Get current SQL data for all users
    Write-Output "$(Get-Date): Loading current SQL data from $SourceTable..."
    $cmd = $connection.CreateCommand()
    $cmd.CommandTimeout = 60
    $cmd.CommandText = "SELECT * FROM $SourceTable"
    $reader = $cmd.ExecuteReader()
    $table = New-Object System.Data.DataTable
    $table.Load($reader)
    $reader.Close()

    if ($table.Rows.Count -eq 0) {
        Write-Output "$(Get-Date): ERROR - No users found in SQL database!"
        return
    }

    Write-Output "$(Get-Date): Loaded $($table.Rows.Count) records from SQL"

    # Pre-cache location data for GUID-to-string mapping
    Write-Output "$(Get-Date): Caching location data..."
    $locationCache = @{}
    try {
        $locations = Get-CsOnlineLisLocation -ErrorAction Stop
        foreach ($loc in $locations) {
            if ($loc.LocationId -and $loc.CompanyName) {
                $locationCache[$loc.LocationId] = $loc.CompanyName
            }
        }
        Write-Output "$(Get-Date): Cached $($locationCache.Count) locations"
    }
    catch {
        Write-Warning "$(Get-Date): Failed to cache locations: $($_.Exception.Message)"
        $locationCache = @{}
    }

    # Pre-cache phone assignments
    Write-Output "$(Get-Date): Caching phone assignments..."
    $phoneAssignmentCache = @{}
    try {
        $assignments = Get-CsPhoneNumberAssignment -ErrorAction Stop
        foreach ($assignment in $assignments) {
            if ($assignment.TelephoneNumber -and $assignment.LocationId) {
                $cleanNumber = $assignment.TelephoneNumber -replace "tel:", ""
                $phoneAssignmentCache[$cleanNumber] = $assignment.LocationId
            }
        }
        Write-Output "$(Get-Date): Cached $($phoneAssignmentCache.Count) phone assignments"
    }
    catch {
        Write-Warning "$(Get-Date): Failed to cache phone assignments: $($_.Exception.Message)"
        $phoneAssignmentCache = @{}
    }

    # Pre-cache call queue memberships (using bulk script method)
    Write-Output "$(Get-Date): Caching call queue memberships..."
    $callQueueMembershipCache = @{}
    try {
        # Get access token for Microsoft Graph API (same as bulk script)
        $GraphAccessToken = Get-AzAccessToken -ResourceUrl "https://graph.microsoft.com"
        $GraphToken = $GraphAccessToken.Token
        $GraphHeaders = @{
            "Authorization" = "Bearer $GraphToken"
            "Content-Type" = "application/json"
        }
        
        # Get call queues and process their distribution lists (exact bulk script method)
        $callQueues = Get-CsCallQueue -WarningAction SilentlyContinue -InformationAction SilentlyContinue -ErrorAction Stop
        $processedGroups = @{}
        
        foreach ($cq in $callQueues) {
            if ($cq.DistributionLists -and $cq.DistributionLists.Count -gt 0) {
                foreach ($groupId in $cq.DistributionLists) {
                    if ($processedGroups.ContainsKey($groupId)) {
                        continue
                    }
                    
                    # Get group info using Graph API (same as bulk script)
                    $groupInfo = Get-GroupInfo -GroupId $groupId -Headers $GraphHeaders
                    $processedGroups[$groupId] = $true
                    
                    # For each member, add this call queue to their memberships
                    foreach ($member in $groupInfo.Members) {
                        if (-not $callQueueMembershipCache.ContainsKey($member)) {
                            $callQueueMembershipCache[$member] = @()
                        }
                        
                        if (-not $callQueueMembershipCache[$member].Contains($cq.Name)) {
                            $callQueueMembershipCache[$member] += $cq.Name
                        }
                    }
                }
            }
        }
        Write-Output "$(Get-Date): Cached call queue memberships for $($callQueueMembershipCache.Keys.Count) users"
    }
    catch {
        Write-Warning "$(Get-Date): Failed to cache call queue memberships: $($_.Exception.Message)"
        $callQueueMembershipCache = @{}
    }

    # Get all voice-enabled Teams users (exclude resource accounts)
    Write-Output "$(Get-Date): Retrieving all voice-enabled Teams users..."
    $allUsers = Get-CsOnlineUser | Where-Object { $_.EnterpriseVoiceEnabled -eq $true }
    
    # Filter out obvious resource accounts but keep regular users
    $teamsUsers = $allUsers | Where-Object { 
        # Exclude obvious resource account patterns
        -not ($_.DisplayName -like "*Auto Attendant*" -or 
              $_.DisplayName -like "*Call Queue*" -or
              $_.DisplayName -like "*Resource Account*" -or
              $_.UserPrincipalName -like "*aa-*" -or
              $_.UserPrincipalName -like "*cq-*" -or
              $_.UserPrincipalName -like "*resourceaccount*")
    }
    
    if ($teamsUsers.Count -eq 0) {
        Write-Output "$(Get-Date): ERROR - No voice-enabled user accounts found in Teams!"
        Write-Output "$(Get-Date): Total voice-enabled accounts found: $($allUsers.Count)"
        Write-Output "$(Get-Date): Accounts after filtering: $($teamsUsers.Count)"
        return
    }

    Write-Output "$(Get-Date): Found $($allUsers.Count) total voice-enabled accounts, filtered to $($teamsUsers.Count) user accounts (excluding resource accounts)"

    # Process each user with progress tracking
    $totalChanges = 0
    $usersWithChanges = 0
    $processedCount = 0
    $errorCount = 0

    foreach ($user in $teamsUsers) {
        $processedCount++
        $upn = $user.UserPrincipalName
        
        # Progress indicator every 25 users or for first user
        if ($processedCount % 25 -eq 0 -or $processedCount -eq 1) {
            Write-Output "$(Get-Date): Processing user $processedCount of $($teamsUsers.Count) ($([math]::Round(($processedCount/$teamsUsers.Count)*100,1))%) - $upn"
        }

        try {
            # Find corresponding SQL row
            $sqlRow = $table.Rows | Where-Object { $_["UPN"] -eq $upn }
            if (-not $sqlRow) {
                continue  # Skip users not in SQL database
            }

            # Process location lookup (using the same method as bulk script)
            $locationName = $null
            if ($user.LineURI) {
                try {
                    $telNumber = $user.LineURI -replace "tel:", ""
                    $phoneNumberInfo = Get-CsPhoneNumberAssignment -TelephoneNumber $telNumber -ErrorAction SilentlyContinue
                    
                    if ($phoneNumberInfo -and $phoneNumberInfo.LocationId) {
                        $locationId = $phoneNumberInfo.LocationId
                        $location = Get-CsOnlineLisLocation | Where-Object { $_.LocationId -eq $locationId }
                        if ($location -and $location.CompanyName) {
                            $locationName = $location.CompanyName
                        }
                    }
                }
                catch {
                    # Location lookup failed, keep as null
                }
            }

            # Process call queue membership (using exact same method as bulk script)
            $userCallQueues = $null
            if ($callQueueMembershipCache.Count -gt 0) {
                $upn = $user.UserPrincipalName
                
                # Try to match by UPN first
                if ($callQueueMembershipCache.ContainsKey($upn)) {
                    $userCallQueues = ($callQueueMembershipCache[$upn] | Sort-Object) -join ";"
                }
                # If not found by UPN, try to match by email (if we have Graph data)
                # For now, just use UPN matching since that's what the bulk script primarily uses
            }

            # Extract Voice Applications Policy using the same method as bulk script
            $voiceApplicationsPolicy = $null
            if ($user.TeamsVoiceApplicationsPolicy) {
                if ($user.TeamsVoiceApplicationsPolicy -is [System.Object] -and $user.TeamsVoiceApplicationsPolicy.Identity) {
                    $voiceApplicationsPolicy = $user.TeamsVoiceApplicationsPolicy.Identity.ToString()
                } 
                elseif ($user.TeamsVoiceApplicationsPolicy -is [System.Object] -and $user.TeamsVoiceApplicationsPolicy.Name) {
                    $voiceApplicationsPolicy = $user.TeamsVoiceApplicationsPolicy.Name.ToString()
                }
                else {
                    $voiceApplicationsPolicy = $user.TeamsVoiceApplicationsPolicy.ToString()
                }
            }

            # Create comparison map (using corrected methods)
            $compareMap = @{
                "Line_URI" = if ($user.LineURI) { $user.LineURI.ToString().Trim() } else { $null }
                "Dial Plan" = if ($user.TenantDialPlan) { $user.TenantDialPlan.ToString().Trim() } else { $null }
                "Location ID" = if ($locationName) { $locationName.ToString().Trim() } else { $null }
                "Emergency Call Routing Policy" = if ($user.TeamsEmergencyCallRoutingPolicy) { $user.TeamsEmergencyCallRoutingPolicy.ToString().Trim() } else { $null }
                "Emergency Calling Policy" = if ($user.TeamsEmergencyCallingPolicy) { $user.TeamsEmergencyCallingPolicy.ToString().Trim() } else { $null }
                "Voice Routing Policy" = if ($user.OnlineVoiceRoutingPolicy) { $user.OnlineVoiceRoutingPolicy.ToString().Trim() } else { $null }
                "Caller ID Policy" = if ($user.CallerIdPolicy) { $user.CallerIdPolicy.ToString().Trim() } else { $null }
                "Voice Applications Policy" = if ($voiceApplicationsPolicy) { $voiceApplicationsPolicy.ToString().Trim() } else { $null }
                "Call Queue Membership" = if ($userCallQueues) { $userCallQueues.ToString().Trim() } else { $null }
            }

            # Debug output for first few users to see what's actually in the API
            if ($processedCount -le 3) {
                Write-Output "=== DEBUG API PROPERTIES FOR $upn ==="
                Write-Output "LineURI: '$($user.LineURI)'"
                Write-Output "Location lookup result: '$locationName'"
                Write-Output "Raw TeamsVoiceApplicationsPolicy: '$($user.TeamsVoiceApplicationsPolicy)'"
                Write-Output "Processed VoiceApplicationsPolicy: '$voiceApplicationsPolicy'"
                Write-Output "Call Queue lookup result: '$userCallQueues'"
                Write-Output "UPN for CQ lookup: '$upn'"
                Write-Output "Cache contains UPN: $($callQueueMembershipCache.ContainsKey($upn))"
                if ($callQueueMembershipCache.ContainsKey($upn)) {
                    Write-Output "Cached queues: $($callQueueMembershipCache[$upn] -join ', ')"
                }
                Write-Output "=== END DEBUG ==="
            }

            # Compare fields and detect changes (with detailed debugging for problematic fields)
            $changed = $false
            $changedFields = @{}

            foreach ($field in $TrackedFields) {
                # Handle DBNull values from SQL
                $sqlValue = if ($sqlRow[$field] -eq [DBNull]::Value -or $sqlRow[$field] -eq $null) { 
                    $null 
                } else { 
                    $sqlRow[$field].ToString().Trim()
                }
                
                $newValue = if ($compareMap[$field]) { 
                    $compareMap[$field].ToString().Trim() 
                } else { 
                    $null 
                }

                # Special handling for the three problematic fields
                if ($field -eq "Location ID" -or $field -eq "Voice Applications Policy" -or $field -eq "Call Queue Membership") {
                    # Extra debugging for these fields
                    if ($processedCount -le 5) {  # Only debug first 5 users to avoid log spam
                        Write-Output "DEBUG - ${upn} - ${field}:"
                        Write-Output "  SQL Raw: '$($sqlRow[$field])'"
                        Write-Output "  SQL Processed: '$sqlValue'"
                        Write-Output "  API Raw: '$($compareMap[$field])'"
                        Write-Output "  API Processed: '$newValue'"
                    }
                }

                # Improved comparison logic to reduce false positives
                $valuesAreDifferent = $false
                
                # Handle null/empty comparisons more precisely
                $sqlIsEmpty = [string]::IsNullOrWhiteSpace($sqlValue)
                $newIsEmpty = [string]::IsNullOrWhiteSpace($newValue)
                
                if ($sqlIsEmpty -and $newIsEmpty) {
                    # Both are empty/null - no change
                    continue
                }
                elseif ($sqlIsEmpty -and -not $newIsEmpty) {
                    # SQL is empty but API has value - this is a change
                    $valuesAreDifferent = $true
                }
                elseif (-not $sqlIsEmpty -and $newIsEmpty) {
                    # SQL has value but API is empty - this is a change
                    $valuesAreDifferent = $true
                }
                elseif ($sqlValue -ne $newValue) {
                    # Both have values but they're different - this is a change
                    $valuesAreDifferent = $true
                }

                if ($valuesAreDifferent) {
                    $changed = $true
                    $changedFields[$field] = @{ 
                        old = if ($sqlValue) { $sqlValue } else { "[EMPTY]" }
                        new = if ($newValue) { $newValue } else { "[EMPTY]" }
                    }
                }
            }

            # Log changes if detected
            if ($changed) {
                $usersWithChanges++
                $totalChanges += $changedFields.Keys.Count
                Write-Output "$(Get-Date): Change detected for $upn. Fields changed: $($changedFields.Keys -join ', ')"
                
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
                    Write-Error "$(Get-Date): Failed to log changes for $upn : $($_.Exception.Message)"
                }
                finally {
                    if ($insertCmd) { $insertCmd.Dispose() }
                    if ($transaction) { $transaction.Dispose() }
                }
            }
        }
        catch {
            $errorCount++
            Write-Error "$(Get-Date): Failed to process user $upn : $($_.Exception.Message)"
            continue
        }
    }

    # Final summary first
    Write-Output ""
    Write-Output "=== FINAL SUMMARY ==="
    Write-Output "$(Get-Date): Processing complete!"
    Write-Output "$(Get-Date): Total users processed: $($teamsUsers.Count)"
    Write-Output "$(Get-Date): Users with changes: $usersWithChanges"
    Write-Output "$(Get-Date): Total field changes detected: $totalChanges"
    Write-Output "$(Get-Date): All change log SQL insertions completed successfully"

    # Auto-trigger user data refresh ONLY after all processing and SQL insertions are complete
    if ($usersWithChanges -gt 0) {
        Write-Output ""
        Write-Output "=== AUTO-TRIGGERING USER DATA REFRESH ==="
        Write-Output "$(Get-Date): All change detection and logging complete"
        Write-Output "$(Get-Date): Changes detected ($usersWithChanges users with $totalChanges total changes)"
        Write-Output "$(Get-Date): Now triggering runbook '$TargetRunbookName' to refresh user data..."
        
        try {
            # Start the target runbook (no parameters passed)
            $job = Start-AzAutomationRunbook -AutomationAccountName $AutomationAccountName `
                                          -ResourceGroupName $ResourceGroupName `
                                          -Name $TargetRunbookName

            if ($job -and $job.JobId) {
                Write-Output "$(Get-Date): User data refresh runbook started successfully!"
                Write-Output "$(Get-Date): Job ID: $($job.JobId)"
                Write-Output "$(Get-Date): Job Status: $($job.Status)"
                Write-Output "$(Get-Date): This will refresh the user data to reflect the detected changes."
            } else {
                Write-Warning "$(Get-Date): User data refresh runbook may not have started properly - no job object returned"
            }
        }
        catch {
            Write-Error "$(Get-Date): Failed to trigger user data refresh runbook: $($_.Exception.Message)"
            Write-Output "$(Get-Date): You may need to manually run '$TargetRunbookName' to update the user data"
        }
    } else {
        Write-Output ""
        Write-Output "$(Get-Date): No changes detected - user data refresh not needed"
    }

    # Final summary
    Write-Output ""
    Write-Output "=== FINAL SUMMARY ==="
    Write-Output "$(Get-Date): Processing complete!"
    Write-Output "$(Get-Date): Total users processed: $($teamsUsers.Count)"
    Write-Output "$(Get-Date): Users with changes: $usersWithChanges"
    Write-Output "$(Get-Date): Total field changes detected: $totalChanges"
    if ($usersWithChanges -gt 0) {
        Write-Output "$(Get-Date): User data refresh runbook triggered automatically after all SQL operations completed"
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
    
    Write-Output "$(Get-Date): Change detection complete!"
}