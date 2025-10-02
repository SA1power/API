# ==============================================================================
# Azure Automation Runbook using Managed Identity
# Retrieves TSD users (starting with "tsd_") who have Teams Shared Device license 
# assigned but are NOT Enterprise Voice enabled and inserts into Azure SQL table: 
# MSOE_Teams_Shared_Device_Users_No_Enterprise_Voice
# 
# Teams Shared Device Faculty SKU: 420c7602-7f70-4895-9394-d3d679ea36fb (verified)
# Based on the Teams Phone System Users runbook template
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
$Table = "dbo.MSOE_Teams_Shared_Device_Users_No_Enterprise_Voice"

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

# Teams Shared Device License SKU ID 
# SKU ID for "Microsoft Teams Shared Devices for faculty" license
# Verified SKU from tenant
$TeamsSharedDeviceSku = "420c7602-7f70-4895-9394-d3d679ea36fb"

# Get all users starting with "tsd_" who have Teams Shared Device license using Graph API with pagination
Write-Output "Getting all users starting with 'tsd_' with Teams Shared Device license using SKU ID: $TeamsSharedDeviceSku"
$tsdUsersWithLicense = @()

try {
    # First, get all users starting with "tsd_"
    $usersGraphUri = "https://graph.microsoft.com/v1.0/users?`$filter=startswith(userPrincipalName,'tsd_')&`$select=userPrincipalName,assignedLicenses&`$top=999"
    
    do {
        $tsdUsersResponse = Invoke-RestMethod -Uri $usersGraphUri -Headers $GraphHeaders -Method Get
        
        if ($tsdUsersResponse.value) {
            # Filter users who have the Teams Shared Device license
            $usersWithSharedDeviceLicense = $tsdUsersResponse.value | Where-Object {
                $_.assignedLicenses | Where-Object { $_.skuId -eq $TeamsSharedDeviceSku }
            } | Select-Object -ExpandProperty userPrincipalName
            
            $tsdUsersWithLicense += $usersWithSharedDeviceLicense
            Write-Output "Retrieved $($tsdUsersResponse.value.Count) TSD users from Graph API (batch). Users with Shared Device license in this batch: $($usersWithSharedDeviceLicense.Count)"
        }
        
        # Check for next page
        $usersGraphUri = $tsdUsersResponse.'@odata.nextLink'
        
    } while ($usersGraphUri)
    
    Write-Output "Found $($tsdUsersWithLicense.Count) total TSD users with Teams Shared Device license"
    
    if ($tsdUsersWithLicense.Count -eq 0) {
        Write-Output "No TSD users found with the Teams Shared Device license"
    }
} catch {
    $errorMessage = $_.Exception.Message
    Write-Error "Error getting TSD users from Graph API: $errorMessage"
    throw
}

# Get Teams users who have Teams Shared Device license and are NOT Enterprise Voice enabled
Write-Output "Checking Enterprise Voice status for TSD users..."
$teamsUsersWithoutEV = @()

foreach ($upn in $tsdUsersWithLicense) {
    try {
        $csUser = Get-CsOnlineUser -Identity $upn -ErrorAction SilentlyContinue
        
        if ($csUser) {
            # Check if Enterprise Voice is disabled (shared devices typically should not have it)
            if ($csUser.EnterpriseVoiceEnabled -eq $false) {
                $teamsUsersWithoutEV += $upn
                Write-Verbose "TSD user without Enterprise Voice: $upn"
            } else {
                Write-Warning "TSD user WITH Enterprise Voice (unexpected): $upn"
            }
        } else {
            # If user not found in Teams, still include them as they don't have Enterprise Voice
            $teamsUsersWithoutEV += $upn
            Write-Verbose "TSD user not found in Teams (no Enterprise Voice): $upn"
        }
    } catch {
        Write-Warning "Error checking Teams user $upn - $_"
    }
}

Write-Output "Found $($teamsUsersWithoutEV.Count) TSD users without Enterprise Voice"

# Count TSD users with Teams Shared Device license without Enterprise Voice
$tsdUserCount = $teamsUsersWithoutEV.Count
Write-Output "TSD user count with Teams Shared Device license (without Enterprise Voice): $tsdUserCount"
Write-Output "Current SQL table count: $currentSQLCount"

# Compare counts to determine if update is needed
if ($tsdUserCount -eq $currentSQLCount) {
    Write-Output "No changes detected. TSD user count ($tsdUserCount) matches current SQL table count ($currentSQLCount)."
    Write-Output "Skipping table update."
} else {
    Write-Output "Changes detected. TSD user count ($tsdUserCount) differs from current SQL table count ($currentSQLCount)."
    Write-Output "Proceeding with table update..."
    
    # Clear the table before inserting new data
    $tableCleared = Clear-SQLTable -Connection $SQLConnection -TableName $Table
    if (-not $tableCleared) {
        Write-Warning "Could not clear the table. Proceeding with data insertion anyway."
    }

    $processed = 0

    foreach ($upn in $teamsUsersWithoutEV) {
        Write-Verbose "Processing TSD user with Shared Device license (no Enterprise Voice): $upn"
        
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
            
            Write-Verbose "Inserted TSD user with Shared Device license (no Enterprise Voice): $upn"
            $processed++
            
        } catch {
            $errorMessage = $_.Exception.Message
            Write-Warning "Insert failed for $upn - $errorMessage"
        }
    }

    Write-Output "Upload complete. Total TSD users with Teams Shared Device license (without Enterprise Voice) processed: $processed"
}

# Cleanup
if ($SQLConnection.State -eq 'Open') {
    $SQLConnection.Close()
    Write-Output "SQL connection closed"
}

Disconnect-MicrosoftTeams -Confirm:$false
Write-Output "Disconnected from Teams. Script execution completed."