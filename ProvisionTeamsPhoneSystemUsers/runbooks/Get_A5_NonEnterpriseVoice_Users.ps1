# ==============================================================================
# Azure Automation Runbook using Managed Identity
# Retrieves A5 users who are NOT Enterprise Voice enabled and inserts into Azure SQL
# Based on the original Teams Phone System Users runbook
# ==============================================================================

# Set more verbose error handling
$ErrorActionPreference = "Continue"
$VerbosePreference = "Continue"

# Connect to Azure using Managed Identity
try {
    Connect-AzAccount -Identity -ErrorAction Stop
    Write-Output "Connected to Azure using Managed Identity"
} catch {
    $errorMessage = $_.Exception.Message
    Write-Error "Failed to connect to Azure - $errorMessage"
    throw
}

# Connect to Microsoft Teams
try {
    Connect-MicrosoftTeams -Identity -ErrorAction Stop
    Write-Output "Connected to Microsoft Teams"
} catch {
    $errorMessage = $_.Exception.Message
    Write-Error "Failed to connect to Microsoft Teams - $errorMessage"
    throw
}

# SQL Info
$SQLServer = "msoevendor.database.windows.net"
$Database = "CEProjectData"
$Table = "dbo.MSOE_A5_Users_No_Enterprise_Voice"

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
    Write-Error "Could not get Graph API token - $errorMessage"
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
    Write-Error "Could not connect to SQL DB - $errorMessage"
    throw
}

# Initialize Graph API Request Headers
$GraphHeaders = @{
    "Authorization" = "Bearer $GraphToken"
    "Content-Type" = "application/json"
}

# Function to ensure UPN column exists in the existing table
function Initialize-SQLTable {
    param (
        [System.Data.SqlClient.SqlConnection]$Connection,
        [string]$TableName
    )
    
    try {
        # Extract just the table name without schema for INFORMATION_SCHEMA queries
        $tableNameOnly = $TableName.Replace('dbo.', '')
        
        # Check if table exists
        $checkTableCmd = $Connection.CreateCommand()
        $checkTableCmd.CommandText = "SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '$tableNameOnly' AND TABLE_SCHEMA = 'dbo'"
        $tableExists = [int]$checkTableCmd.ExecuteScalar() -gt 0
        
        if (-not $tableExists) {
            Write-Error "Table $TableName does not exist and cannot be created due to permissions."
            return $false
        } else {
            Write-Output "Table $TableName exists"
            
            # Check if UPN column exists
            $checkColumnCmd = $Connection.CreateCommand()
            $checkColumnCmd.CommandText = "SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '$tableNameOnly' AND TABLE_SCHEMA = 'dbo' AND COLUMN_NAME = 'UPN'"
            $upnColumnExists = [int]$checkColumnCmd.ExecuteScalar() -gt 0
            
            if (-not $upnColumnExists) {
                Write-Error "UPN column does not exist in table $TableName and cannot be added due to permissions."
                return $false
            } else {
                Write-Output "UPN column exists in table $TableName"
            }
        }
        return $true
    } catch {
        $errorMessage = $_.Exception.Message
        Write-Error "Failed to validate table $TableName - $errorMessage"
        return $false
    }
}

# Function to get current count from SQL table
function Get-SQLTableCount {
    param (
        [System.Data.SqlClient.SqlConnection]$Connection,
        [string]$TableName
    )
    
    try {
        $countCommand = $Connection.CreateCommand()
        $countCommand.CommandText = "SELECT COUNT(*) FROM $TableName"
        $currentCount = [int]$countCommand.ExecuteScalar()
        Write-Output "Current count in ${TableName}: $currentCount"
        return $currentCount
    } catch {
        $errorMessage = $_.Exception.Message
        Write-Warning "Failed to get count from table $TableName - $errorMessage"
        return 0
    }
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

# Validate the existing table
$tableValidated = Initialize-SQLTable -Connection $SQLConnection -TableName $Table
if (-not $tableValidated) {
    Write-Error "Could not validate the existing table. Exiting."
    throw "Table validation failed"
}

# Get current count from the SQL table
$currentSQLCount = Get-SQLTableCount -Connection $SQLConnection -TableName $Table

# Use the specific A5 Faculty SKU ID that works
$A5FacultyLicenseSku = "e97c048c-37a4-45fb-ab50-922fbf07a370"

# Get all users with A5 Faculty licenses using Graph API with pagination
Write-Output "Getting all users with A5 Faculty licenses using SKU ID: $A5FacultyLicenseSku"
$a5FacultyUsers = @()

try {
    # Get users with the specific A5 Faculty SKU using the working SKU ID - handle pagination
    $usersGraphUri = "https://graph.microsoft.com/v1.0/users?`$filter=assignedLicenses/any(x:x/skuId eq $A5FacultyLicenseSku)&`$select=userPrincipalName&`$top=999"
    
    do {
        $a5UsersResponse = Invoke-RestMethod -Uri $usersGraphUri -Headers $GraphHeaders -Method Get
        
        if ($a5UsersResponse.value) {
            $msoeUsers = $a5UsersResponse.value | Where-Object { $_.userPrincipalName -like "*@msoe.edu" } | Select-Object -ExpandProperty userPrincipalName
            $a5FacultyUsers += $msoeUsers
            Write-Output "Retrieved $($a5UsersResponse.value.Count) users from Graph API (batch). MSOE users in this batch: $($msoeUsers.Count)"
        }
        
        # Check for next page
        $usersGraphUri = $a5UsersResponse.'@odata.nextLink'
        
    } while ($usersGraphUri)
    
    Write-Output "Found $($a5FacultyUsers.Count) total msoe.edu users with A5 Faculty licenses"
    
    if ($a5FacultyUsers.Count -eq 0) {
        Write-Output "No A5 Faculty users found with the specified SKU"
    }
} catch {
    $errorMessage = $_.Exception.Message
    Write-Error "Error getting A5 Faculty users from Graph API: $errorMessage"
    throw
}

# Get Teams users who are A5 Faculty licensed and NOT Enterprise Voice enabled
Write-Output "Checking Enterprise Voice status for A5 Faculty users..."
$teamsUsersWithoutEV = @()

foreach ($upn in $a5FacultyUsers) {
    try {
        $csUser = Get-CsOnlineUser -Identity $upn -ErrorAction SilentlyContinue
        
        if ($csUser) {
            # Check if Enterprise Voice is disabled
            if ($csUser.EnterpriseVoiceEnabled -eq $false) {
                $teamsUsersWithoutEV += $csUser
                Write-Verbose "A5 Faculty user without Enterprise Voice: $upn"
            }
        } else {
            Write-Warning "Could not find Teams user for: $upn"
        }
    } catch {
        Write-Warning "Error checking Teams user $upn - $_"
    }
}

Write-Output "Found $($teamsUsersWithoutEV.Count) A5 Faculty users who are NOT Enterprise Voice enabled"

# Count A5 Faculty users without Enterprise Voice
$a5FacultyUserCount = $teamsUsersWithoutEV.Count
Write-Output "A5 Faculty user count without Enterprise Voice: $a5FacultyUserCount"
Write-Output "Current SQL table count: $currentSQLCount"

# Compare counts to determine if update is needed
if ($a5FacultyUserCount -eq $currentSQLCount) {
    Write-Output "No changes detected. A5 Faculty user count ($a5FacultyUserCount) matches current SQL table count ($currentSQLCount)."
    Write-Output "Skipping table update."
} else {
    Write-Output "Changes detected. A5 Faculty user count ($a5FacultyUserCount) differs from current SQL table count ($currentSQLCount)."
    Write-Output "Proceeding with table update..."
    
    # Clear the table before inserting new data
    $tableCleared = Clear-SQLTable -Connection $SQLConnection -TableName $Table
    if (-not $tableCleared) {
        Write-Warning "Could not clear the table. Proceeding with data insertion anyway."
    }

    $processed = 0

    foreach ($user in $teamsUsersWithoutEV) {
        $upn = $user.UserPrincipalName
        
        Write-Verbose "Processing A5 Faculty user without Enterprise Voice: $upn"
        
        # Create a simple parameter for just the UPN
        $safeParams = @{}
        $safeParams["UPN"] = $upn.ToString()
        
        # Simple insert SQL command - just UPN
        $query = "INSERT INTO $Table (UPN) VALUES (@UPN)"

        try {
            $cmd = $SQLConnection.CreateCommand()
            $cmd.CommandText = $query
            
            # Add UPN parameter
            $cmd.Parameters.AddWithValue("@UPN", $safeParams["UPN"]) | Out-Null
            
            $cmd.ExecuteNonQuery() | Out-Null
            
            Write-Verbose "Inserted A5 Faculty user without Enterprise Voice: $upn"
            $processed++
            
        } catch {
            $errorMessage = $_.Exception.Message
            Write-Warning "Insert failed for $upn - $errorMessage"
        }
    }

    Write-Output "Upload complete. Total A5 Faculty users without Enterprise Voice processed: $processed"
}

# Cleanup
if ($SQLConnection.State -eq 'Open') {
    $SQLConnection.Close()
    Write-Output "SQL connection closed"
}

Disconnect-MicrosoftTeams -Confirm:$false
Write-Output "Disconnected from Teams. Script execution completed."