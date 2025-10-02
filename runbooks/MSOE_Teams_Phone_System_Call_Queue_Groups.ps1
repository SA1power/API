# Teams Call Queue Group Members Export Script for Azure Automation Runbook
# This script gets all Teams call queue groups and their members and writes to a SQL table

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
$Database  = "CEProjectData"
# ‚άκ Corrected table name casing here:
$Table     = "dbo.MSOE_Teams_Phone_System_CQ_Members"

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
    $GraphToken       = $GraphAccessToken.Token
    Write-Output "Graph API token acquired"
} catch {
    $errorMessage = $_.Exception.Message
    Write-Error "Could not get Graph API token: $errorMessage"
    throw
}

# Initialize SQL Connection
try {
    $SQLConnection           = New-Object System.Data.SqlClient.SqlConnection
    $SQLConnection.ConnectionString = "Server=$SQLServer;Database=$Database;Integrated Security=False;Encrypt=True;TrustServerCertificate=False;"
    $SQLConnection.AccessToken      = $SQLAccessTokenText
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
    "Content-Type"  = "application/json"
}

function Clear-SQLTable {
    param (
        [System.Data.SqlClient.SqlConnection] $Connection,
        [string]                         $TableName
    )
    try {
        $truncateCommand        = $Connection.CreateCommand()
        $truncateCommand.CommandText = "DELETE FROM $TableName"
        $rowsAffected           = $truncateCommand.ExecuteNonQuery()
        Write-Output "Table $TableName cleared. $rowsAffected rows deleted."
        return $true
    } catch {
        $errorMessage = $_.Exception.Message
        Write-Error "Failed to clear table $TableName - $errorMessage"
        return $false
    }
}

function Get-GroupInfo {
    param(
        [Parameter(Mandatory=$true)]
        [string] $GroupId
    )
    $result = @{
        Email       = "N/A"
        DisplayName = "N/A"
        Members     = @()
        MemberCount = 0
        Owners      = @()
    }
    try {
        $groupUri      = "https://graph.microsoft.com/v1.0/groups/$GroupId"
        $groupResponse = Invoke-RestMethod -Uri $groupUri -Headers $GraphHeaders -Method Get -ErrorAction SilentlyContinue
        
        if ($groupResponse) {
            $result.Email       = if ($groupResponse.mail) { $groupResponse.mail } else { $groupResponse.proxyAddresses | Where-Object { $_ -like "SMTP:*" } | ForEach-Object { $_.Substring(5) } | Select-Object -First 1 }
            $result.DisplayName = $groupResponse.displayName
            
            # Get group members
            $membersUri      = "https://graph.microsoft.com/v1.0/groups/$GroupId/members"
            $membersResponse = Invoke-RestMethod -Uri $membersUri -Headers $GraphHeaders -Method Get -ErrorAction SilentlyContinue
            
            # This section has been modified to include all members, even if they are also owners
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
                    # This section has also been modified to include all members
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
            
            # Get group owners - now collecting all owners
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
                    
                    Write-Output "Group $($result.DisplayName) has $($result.Owners.Count) owners"
                } else {
                    Write-Output "No owners found for group $($result.DisplayName)"
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
        # using concatenation to avoid interpolation issues
        Write-Output ("Error retrieving group info for " + $GroupId + ": " + $_.Exception.Message)
    }
    return $result
}

# Check if Group_Total column exists and add it if it doesn't
try {
    $checkColumnQuery = "SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = 'dbo' AND TABLE_NAME = 'MSOE_Teams_Phone_System_CQ_Members' AND COLUMN_NAME = 'Group_Total'"
    $checkColumnCmd = $SQLConnection.CreateCommand()
    $checkColumnCmd.CommandText = $checkColumnQuery
    $columnExists = [int]$checkColumnCmd.ExecuteScalar()

    if ($columnExists -eq 0) {
        Write-Output "Group_Total column does not exist, adding it to the table..."
        $addColumnQuery = "ALTER TABLE $Table ADD Group_Total INT NULL"
        $addColumnCmd = $SQLConnection.CreateCommand()
        $addColumnCmd.CommandText = $addColumnQuery
        $addColumnCmd.ExecuteNonQuery() | Out-Null
        Write-Output "Group_Total column added successfully"
    } else {
        Write-Output "Group_Total column already exists"
    }
} catch {
    $errorMessage = $_.Exception.Message
    Write-Warning "Failed to check or add Group_Total column: $errorMessage"
    # Continue with the script regardless
}

# Check if Group_Owner column exists and add it if it doesn't
try {
    $checkOwnerColumnQuery = "SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = 'dbo' AND TABLE_NAME = 'MSOE_Teams_Phone_System_CQ_Members' AND COLUMN_NAME = 'Group_Owner'"
    $checkOwnerColumnCmd = $SQLConnection.CreateCommand()
    $checkOwnerColumnCmd.CommandText = $checkOwnerColumnQuery
    $ownerColumnExists = [int]$checkOwnerColumnCmd.ExecuteScalar()

    if ($ownerColumnExists -eq 0) {
        Write-Output "Group_Owner column does not exist, adding it to the table..."
        $addOwnerColumnQuery = "ALTER TABLE $Table ADD Group_Owner NVARCHAR(MAX) NULL"
        $addOwnerColumnCmd = $SQLConnection.CreateCommand()
        $addOwnerColumnCmd.CommandText = $addOwnerColumnQuery
        $addOwnerColumnCmd.ExecuteNonQuery() | Out-Null
        Write-Output "Group_Owner column added successfully"
    } else {
        Write-Output "Group_Owner column already exists"
    }
} catch {
    $errorMessage = $_.Exception.Message
    Write-Warning "Failed to check or add Group_Owner column: $errorMessage"
    # Continue with the script regardless
}

# Clear the table
$tableCleared = Clear-SQLTable -Connection $SQLConnection -TableName $Table
if (-not $tableCleared) {
    Write-Warning "Could not clear the table. Proceeding with data insertion anyway."
}

# Retrieve call queues
Write-Output "Retrieving all call queues to extract group IDs..."
try {
    $callQueues = Get-CsCallQueue -ErrorAction Stop
    if ($callQueues) {
        Write-Output "Found $($callQueues.Count) call queues to check for groups"
    } else {
        Write-Output "No call queues found"
        $callQueues = @()
    }
} catch {
    $errorMessage = $_.Exception.Message
    Write-Output "Error retrieving call queues: $errorMessage"
    $callQueues = @()
}

# Process call queues
$processed       = 0
$groupsProcessed = @{}

foreach ($cq in $callQueues) {
    if ($cq.DistributionLists -and $cq.DistributionLists.Count -gt 0) {
        foreach ($groupId in $cq.DistributionLists) {
            if ($groupsProcessed.ContainsKey($groupId)) {
                Write-Output "Group $groupId already processed"
                continue
            }
            Write-Output "Processing group for call queue '$($cq.Name)': $groupId"
            $groupInfo = Get-GroupInfo -GroupId $groupId
            if ($groupInfo.DisplayName -eq "N/A" -and $groupInfo.Email -eq "N/A" -and $groupInfo.Members.Count -eq 0) {
                Write-Output "Could not retrieve details for group: $groupId"
                continue
            }
            $groupsProcessed[$groupId] = $true
            
            $membersString = $groupInfo.Members -join ";"
            if ([string]::IsNullOrEmpty($membersString)) {
                $membersString = "N/A"
            }
            
            $ownersString = $groupInfo.Owners -join ";"
            if ([string]::IsNullOrEmpty($ownersString)) {
                $ownersString = "N/A"
            }
            
            $groupName = if ($groupInfo.DisplayName -ne "N/A") { $groupInfo.DisplayName } else { $cq.Name + " Group" }
            $memberCount = $groupInfo.MemberCount
            
            Write-Output "Group '$groupName' has $memberCount total members and $(if($groupInfo.Owners.Count -gt 0){$groupInfo.Owners.Count} else {'no'}) owners"
            
            $query = "INSERT INTO $Table (CQ_Group_Name, Group_GUID, Group_Email, Group_Members, Group_Total, Group_Owner) VALUES (@CQ_Group_Name, @Group_GUID, @Group_Email, @Group_Members, @Group_Total, @Group_Owner)"
            
            $cmd = $SQLConnection.CreateCommand()
            $cmd.CommandText = $query
            $cmd.Parameters.AddWithValue("@CQ_Group_Name", $groupName)      | Out-Null
            $cmd.Parameters.AddWithValue("@Group_GUID",    $groupId)        | Out-Null
            $cmd.Parameters.AddWithValue("@Group_Email",   $groupInfo.Email)| Out-Null
            $cmd.Parameters.AddWithValue("@Group_Members", $membersString)  | Out-Null
            $cmd.Parameters.AddWithValue("@Group_Total",   $memberCount)    | Out-Null
            $cmd.Parameters.AddWithValue("@Group_Owner",   $ownersString)   | Out-Null
            
            try {
                $cmd.ExecuteNonQuery() | Out-Null
                Write-Output "Inserted group '$groupName' with $memberCount members and owners: $ownersString"
                $processed++
            }
            catch {
                $errorMessage = $_.Exception.Message
                Write-Warning "Insert failed for group '$groupName': $errorMessage"
            }
        }
    } else {
        Write-Output "Call queue '$($cq.Name)' has no distribution lists/groups assigned"
    }
}

Write-Output "Call queue group members export completed. Total groups processed: $processed"

if ($SQLConnection.State -eq 'Open') {
    $SQLConnection.Close()
    Write-Output "SQL connection closed"
}

Disconnect-MicrosoftTeams -Confirm:$false
Write-Output "Disconnected from Teams. Script execution completed."
