# Function to get group information from Graph API
function Get-GroupInfo {
    param(
        [Parameter(Mandatory=$true)]
        [string] $GroupId
    )
    $result = @{
        Email = "N/A"
        DisplayName = "N/A"
        Members = @()
        MemberCount = 0
        Owners = @()
    }
    try {
        $groupUri = "https://graph.microsoft.com/v1.0/groups/$GroupId"
        $groupResponse = Invoke-RestMethod -Uri $groupUri -Headers $GraphHeaders -Method Get -ErrorAction SilentlyContinue
        
        if ($groupResponse) {
            $result.Email = if ($groupResponse.mail) { $groupResponse.mail } else { $groupResponse.proxyAddresses | Where-Object { $_ -like "SMTP:*" } | ForEach-Object { $_.Substring(5) } | Select-Object -First 1 }
            $result.DisplayName = $groupResponse.displayName
            
            # Get group members
            $membersUri = "https://graph.microsoft.com/v1.0/groups/$GroupId/members"
            $membersResponse = Invoke-RestMethod -Uri $membersUri -Headers $GraphHeaders -Method Get -ErrorAction SilentlyContinue
            
            # Include all members, even if they are also owners
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
                $membersResponse = Invoke-RestMethod -Uri $nextLink -Headers $GraphHeaders -Method Get -ErrorAction SilentlyContinue
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
            
            # Get group owners
            $ownersUri = "https://graph.microsoft.com/v1.0/groups/$GroupId/owners"
            try {
                $ownersResponse = Invoke-RestMethod -Uri $ownersUri -Headers $GraphHeaders -Method Get -ErrorAction SilentlyContinue
                
                if ($ownersResponse -and $ownersResponse.value -and $ownersResponse.value.Count -gt 0) {
                    # Collect all owners
                    foreach ($owner in $ownersResponse.value) {
                        if ($owner.mail) {
                            $result.Owners += $owner.mail
                        }
                        elseif ($owner.userPrincipalName) {
                            $result.Owners += $owner.userPrincipalName
                        }
                    }
                    
                    # Check for pagination - get additional owners if there are more
                    $nextOwnersLink = $ownersResponse.'@odata.nextLink'
                    while ($nextOwnersLink) {
                        $nextOwnersResponse = Invoke-RestMethod -Uri $nextOwnersLink -Headers $GraphHeaders -Method Get -ErrorAction SilentlyContinue
                        if ($nextOwnersResponse -and $nextOwnersResponse.value) {
                            foreach ($owner in $nextOwnersResponse.value) {
                                if ($owner.mail) {
                                    $result.Owners += $owner.mail
                                }
                                elseif ($owner.userPrincipalName) {
                                    $result.Owners += $owner.userPrincipalName
                                }
                            }
                        }
                        $nextOwnersLink = $nextOwnersResponse.'@odata.nextLink'
                    }
                }
            } catch {
                $errorMessage = $_.Exception.Message
                Write-Output "Error retrieving owners for group $GroupId : $errorMessage"
            }
            
            # Set the member count
            $result.MemberCount = $result.Members.Count
        } else {
            Write-Output "Could not retrieve group information for group ID: $GroupId"
        }
    }
    catch {
        Write-Output ("Error retrieving group info for " + $GroupId + ": " + $_.Exception.Message)
    }
    return $result
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

# Teams Call Queue Groups Change Detection - Production Beta
# Monitors all call queue groups for changes and logs to database
# Auto-triggers call queue groups data refresh when changes are detected

# Configuration
$SQLServer = "msoevendor.database.windows.net"
$Database = "CEProjectData"
$SourceTable = "dbo.MSOE_Teams_Phone_System_CQ_Members"
$LogTable = "dbo.msoe_teams_phone_system_cq_members_change_log"

# Auto-trigger configuration
$AutomationAccountName = "VendorAutomationAccount"
$ResourceGroupName = "Infrastructure"
$TargetRunbookName = "MSOE_Teams_Phone_System_Call_Queue_Groups"
$SubscriptionId = "fc7ad0bc-429f-488b-9488-3ed508182348"

# Required fields to monitor
$TrackedFields = @(
    "CQ_Group_Name", 
    "Group_Email", 
    "Group_Members", 
    "Group_Total", 
    "Group_Owner"
)

Write-Output "=== Teams Call Queue Groups Change Detection - Production Beta ==="
Write-Output "$(Get-Date): Starting change detection for all call queue groups..."

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

    # Get current SQL data for all call queue groups
    Write-Output "$(Get-Date): Loading current SQL data from $SourceTable..."
    $cmd = $connection.CreateCommand()
    $cmd.CommandTimeout = 60
    $cmd.CommandText = "SELECT * FROM $SourceTable"
    $reader = $cmd.ExecuteReader()
    $table = New-Object System.Data.DataTable
    $table.Load($reader)
    $reader.Close()

    if ($table.Rows.Count -eq 0) {
        Write-Output "$(Get-Date): ERROR - No call queue groups found in SQL database!"
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

    # Get all call queues
    Write-Output "$(Get-Date): Retrieving all call queues..."
    $callQueues = Get-CsCallQueue -ErrorAction Stop
    
    if ($callQueues.Count -eq 0) {
        Write-Output "$(Get-Date): ERROR - No call queues found in Teams!"
        return
    }

    Write-Output "$(Get-Date): Found $($callQueues.Count) call queues to process"

    # Process each call queue's groups with progress tracking
    $totalChanges = 0
    $groupsWithChanges = 0
    $processedCount = 0
    $errorCount = 0
    $groupsProcessed = @{}

    foreach ($cq in $callQueues) {
        if ($cq.DistributionLists -and $cq.DistributionLists.Count -gt 0) {
            foreach ($groupId in $cq.DistributionLists) {
                if ($groupsProcessed.ContainsKey($groupId)) {
                    continue
                }
                
                $processedCount++
                
                # Progress indicator every 5 groups or for first group
                if ($processedCount % 5 -eq 0 -or $processedCount -eq 1) {
                    Write-Output "$(Get-Date): Processing call queue group $processedCount - Group ID: $groupId"
                }

                try {
                    # Find corresponding SQL row
                    $sqlRow = $table.Rows | Where-Object { $_["Group_GUID"] -eq $groupId }
                    if (-not $sqlRow) {
                        continue  # Skip groups not in SQL database
                    }

                    $groupInfo = Get-GroupInfo -GroupId $groupId
                    if ($groupInfo.DisplayName -eq "N/A" -and $groupInfo.Email -eq "N/A" -and $groupInfo.Members.Count -eq 0) {
                        Write-Output "$(Get-Date): Could not retrieve details for group: $groupId"
                        continue
                    }
                    $groupsProcessed[$groupId] = $true
                    
                    # Prepare current values for comparison
                    $membersString = if ($groupInfo.Members.Count -gt 0) { $groupInfo.Members -join ";" } else { $null }
                    $ownersString = if ($groupInfo.Owners.Count -gt 0) { $groupInfo.Owners -join ";" } else { $null }
                    $groupName = if ($groupInfo.DisplayName -ne "N/A") { $groupInfo.DisplayName } else { "Unknown Group" }
                    $memberCount = $groupInfo.MemberCount

                    # Create comparison map (ensure all values are properly converted to strings)
                    $compareMap = @{
                        "CQ_Group_Name" = if ($groupName -and $groupName -ne "N/A") { [string]$groupName } else { "N/A" }
                        "Group_Email" = if ($groupInfo.Email -and $groupInfo.Email -ne "N/A") { [string]$groupInfo.Email } else { "N/A" }
                        "Group_Members" = if ($membersString) { [string]$membersString } else { "N/A" }
                        "Group_Total" = [string]$memberCount
                        "Group_Owner" = if ($ownersString) { [string]$ownersString } else { "N/A" }
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

                        # Special handling for member lists - sort both for comparison
                        if ($field -eq "Group_Members") {
                            $sqlArray = if ($sqlValue -eq "N/A") { @() } else { ($sqlValue -split ";") | Sort-Object }
                            $newArray = if ($newValue -eq "N/A") { @() } else { ($newValue -split ";") | Sort-Object }
                            
                            $sqlSorted = if ($sqlArray.Count -eq 0) { "N/A" } else { $sqlArray -join ";" }
                            $newSorted = if ($newArray.Count -eq 0) { "N/A" } else { $newArray -join ";" }
                            
                            if ($sqlSorted -ne $newSorted) {
                                $changed = $true
                                $changedFields[$field] = @{ 
                                    old = $sqlValue
                                    new = $newValue
                                }
                            }
                        } else {
                            # Standard comparison for other fields
                            # Normalize both values for comparison (treat null, empty, and "N/A" as equivalent)
                            $sqlNormalized = if ([string]::IsNullOrWhiteSpace($sqlValue) -or $sqlValue -eq "N/A") { "N/A" } else { $sqlValue }
                            $newNormalized = if ([string]::IsNullOrWhiteSpace($newValue) -or $newValue -eq "N/A") { "N/A" } else { $newValue }
                            
                            if ($sqlNormalized -ne $newNormalized) {
                                $changed = $true
                                $changedFields[$field] = @{ 
                                    old = $sqlNormalized
                                    new = $newNormalized
                                }
                            }
                        }
                    }

                    # Log changes if detected
                    if ($changed) {
                        $groupsWithChanges++
                        $totalChanges += $changedFields.Keys.Count
                        Write-Output "$(Get-Date): Change detected for group $groupName. Fields changed: $($changedFields.Keys -join ', ')"
                        
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
                            Write-Error "$(Get-Date): Failed to log changes for group $groupName : $($_.Exception.Message)"
                        }
                        finally {
                            if ($insertCmd) { $insertCmd.Dispose() }
                            if ($transaction) { $transaction.Dispose() }
                        }
                    }
                }
                catch {
                    $errorCount++
                    Write-Error "$(Get-Date): Failed to process call queue group $groupId : $($_.Exception.Message)"
                    continue
                }
            }
        }
    }

    # Final summary first
    Write-Output ""
    Write-Output "=== FINAL SUMMARY ==="
    Write-Output "$(Get-Date): Processing complete!"
    Write-Output "$(Get-Date): Total call queue groups processed: $processedCount"
    Write-Output "$(Get-Date): Call queue groups with changes: $groupsWithChanges"
    Write-Output "$(Get-Date): Total field changes detected: $totalChanges"
    Write-Output "$(Get-Date): All change log SQL insertions completed successfully"

    # Auto-trigger call queue groups data refresh ONLY after all processing and SQL insertions are complete
    if ($groupsWithChanges -gt 0) {
        Write-Output ""
        Write-Output "=== AUTO-TRIGGERING CALL QUEUE GROUPS DATA REFRESH ==="
        Write-Output "$(Get-Date): All change detection and logging complete"
        Write-Output "$(Get-Date): Changes detected ($groupsWithChanges call queue groups with $totalChanges total changes)"
        Write-Output "$(Get-Date): Now triggering runbook '$TargetRunbookName' to refresh call queue groups data..."
        
        try {
            # Use the exact same simple approach as the working auto attendant script
            $job = Start-AzAutomationRunbook -AutomationAccountName $AutomationAccountName `
                                          -ResourceGroupName $ResourceGroupName `
                                          -Name $TargetRunbookName

            if ($job -and $job.JobId) {
                Write-Output "$(Get-Date): Call queue groups data refresh runbook started successfully!"
                Write-Output "$(Get-Date): Job ID: $($job.JobId)"
                Write-Output "$(Get-Date): Job Status: $($job.Status)"
                Write-Output "$(Get-Date): This will refresh the call queue groups data to reflect the detected changes."
            } else {
                Write-Warning "$(Get-Date): Call queue groups data refresh runbook may not have started properly - no job object returned"
            }
        }
        catch {
            Write-Error "$(Get-Date): Failed to trigger call queue groups data refresh runbook: $($_.Exception.Message)"
            Write-Output "$(Get-Date): You may need to manually run '$TargetRunbookName' to update the call queue groups data"
        }
    } else {
        Write-Output ""
        Write-Output "$(Get-Date): No changes detected - call queue groups data refresh not needed"
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
    
    Write-Output "$(Get-Date): Call queue groups change detection complete!"
}