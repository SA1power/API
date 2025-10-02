# Script for Azure Automation Runbook using Managed Identity
# Filters Teams users with UPN starting with "tsd_", and inserts into Azure SQL
# Includes Call Queue membership information and Location ID

# Set more verbose error handling
$ErrorActionPreference = "Continue"
$VerbosePreference = "Continue"

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
        $user = Get-CsOnlineUser -Identity $UserUpn -ErrorAction Stop
        
        if ($user -and $user.LineUri) {
            $telNumber = $user.LineUri -replace "tel:", ""
            
            # Get the phone number assignment details
            $phoneNumberInfo = Get-CsPhoneNumberAssignment -TelephoneNumber $telNumber -ErrorAction SilentlyContinue
            
            if ($phoneNumberInfo -and $phoneNumberInfo.LocationId) {
                $locationId = $phoneNumberInfo.LocationId
                
                # Look up this location ID
                $location = Get-CsOnlineLisLocation | Where-Object { $_.LocationId -eq $locationId }
                if ($location -and $location.CompanyName) {
                    $locationName = $location.CompanyName
                    # Fixed colon issue in string output
                    Write-Verbose "Found location for $UserUpn - $locationName"
                }
            }
        }
    }
    catch {
        # Fixed colon issue in string output
        Write-Verbose "Error getting location for $UserUpn - $_"
    }
    
    return $locationName
}

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
$Table = "dbo.MSOE_Teams_Phone_System_Shared_Device_Accounts"

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

# Initialize Graph API Request Headers
$GraphHeaders = @{
    "Authorization" = "Bearer $GraphToken"
    "Content-Type" = "application/json"
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

# Clear the table before inserting new data
$tableCleared = Clear-SQLTable -Connection $SQLConnection -TableName $Table
if (-not $tableCleared) {
    Write-Warning "Could not clear the table. Proceeding with data insertion anyway."
}

# ===== CALL QUEUE SECTION: GET CALL QUEUE GROUPS AND THEIR MEMBERS =====
Write-Output "Retrieving all call queues to extract group IDs and members..."

# Hashtable to store device-to-call-queue mappings
$deviceCallQueueMemberships = @{}

try {
    $callQueues = Get-CsCallQueue -ErrorAction Stop
    if ($callQueues) {
        Write-Output "Found $($callQueues.Count) call queues to check for groups"
    } else {
        Write-Output "No call queues found"
        $callQueues = @()
    }
} catch {
    Write-Output "Error retrieving call queues: $_"
    $callQueues = @()
}

# Process each call queue to extract distribution lists and members
$processedGroups = @{}

foreach ($cq in $callQueues) {
    if ($cq.DistributionLists -and $cq.DistributionLists.Count -gt 0) {
        foreach ($groupId in $cq.DistributionLists) {
            if ($processedGroups.ContainsKey($groupId)) {
                Write-Output "Group $groupId already processed"
                continue
            }
            
            Write-Output "Processing group for call queue '$($cq.Name)': $groupId"
            $groupInfo = Get-GroupInfo -GroupId $groupId -Headers $GraphHeaders
            
            if ($groupInfo.DisplayName -eq "N/A" -and $groupInfo.Email -eq "N/A" -and $groupInfo.Members.Count -eq 0) {
                Write-Output "Could not retrieve details for group: $groupId"
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
                        Write-Output "Shared device account $member is a member of call queue: $($cq.Name)"
                    }
                }
            }
        }
    } else {
        Write-Output "Call queue '$($cq.Name)' has no distribution lists/groups assigned"
    }
}

Write-Output "Processed all call queues. Found call queue memberships for $($deviceCallQueueMemberships.Count) shared device accounts."
# ===== END OF CALL QUEUE SECTION =====

# Get Teams users that start with tsd_
Write-Output "Fetching Teams shared device accounts..."
$sharedDeviceAccounts = Get-CsOnlineUser | Where-Object { $_.UserPrincipalName -like "tsd_*" }

# Debug output for troubleshooting
if ($null -eq $sharedDeviceAccounts) {
    Write-Warning "No accounts found with UPN starting with tsd_"
    if ($SQLConnection.State -eq 'Open') {
        $SQLConnection.Close()
    }
    return
}

$accountCount = @($sharedDeviceAccounts).Count
Write-Output "Found $accountCount shared device accounts"

# Check if the Call Queue Membership column exists, if not add it
try {
    $checkColumnCmd = $SQLConnection.CreateCommand()
    $checkColumnCmd.CommandText = "SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'MSOE_Teams_Phone_System_Shared_Device_Accounts' AND COLUMN_NAME = 'Call Queue Membership'"
    $columnExists = [int]$checkColumnCmd.ExecuteScalar() -gt 0
    
    if (-not $columnExists) {
        Write-Output "Adding 'Call Queue Membership' column to table..."
        $addColumnCmd = $SQLConnection.CreateCommand()
        $addColumnCmd.CommandText = "ALTER TABLE $Table ADD [Call Queue Membership] NVARCHAR(MAX)"
        $addColumnCmd.ExecuteNonQuery() | Out-Null
        Write-Output "Column added successfully"
    } else {
        Write-Output "'Call Queue Membership' column already exists"
    }
} catch {
    $errorMessage = $_.Exception.Message
    Write-Warning "Error checking or adding column: $errorMessage"
}

# Check if the Location ID column exists, if not add it
try {
    $checkLocationColumnCmd = $SQLConnection.CreateCommand()
    $checkLocationColumnCmd.CommandText = "SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'MSOE_Teams_Phone_System_Shared_Device_Accounts' AND COLUMN_NAME = 'Location ID'"
    $locationColumnExists = [int]$checkLocationColumnCmd.ExecuteScalar() -gt 0
    
    if (-not $locationColumnExists) {
        Write-Output "Adding 'Location ID' column to table..."
        $addLocationColumnCmd = $SQLConnection.CreateCommand()
        $addLocationColumnCmd.CommandText = "ALTER TABLE $Table ADD [Location ID] NVARCHAR(255)"
        $addLocationColumnCmd.ExecuteNonQuery() | Out-Null
        Write-Output "Location ID column added successfully"
    } else {
        Write-Output "'Location ID' column already exists"
    }
} catch {
    $errorMessage = $_.Exception.Message
    Write-Warning "Error checking or adding Location ID column: $errorMessage"
}

$processed = 0
$successful = 0
foreach ($account in $sharedDeviceAccounts) {
    $upn = $account.UserPrincipalName
    $lineURI = $account.LineURI
    $displayName = $account.DisplayName
    
    # Extract policy values using the helper function
    $teamsIPPhonePolicy = Get-PolicyStringValue -PolicyObject $account.TeamsIPPhonePolicy
    $callingPolicy = Get-PolicyStringValue -PolicyObject $account.TeamsCallingPolicy
    $callerIdPolicy = Get-PolicyStringValue -PolicyObject $account.CallingLineIdentity
    
    Write-Output "Processing account $upn"
    Write-Output "TeamsIPPhonePolicy = $teamsIPPhonePolicy"
    Write-Output "TeamsCallingPolicy = $callingPolicy" 
    Write-Output "CallingLineIdentity = $callerIdPolicy"
    
    # Check if this device is a member of any call queues
    $callQueueMembership = [DBNull]::Value
    
    if ($deviceCallQueueMemberships.ContainsKey($upn)) {
        $callQueueMembership = $deviceCallQueueMemberships[$upn] -join ";"
        Write-Output "Device $upn is a member of call queues: $callQueueMembership"
    } else {
        Write-Output "Device $upn is not a member of any call queues"
    }
    
    # Get the Location ID for this user
    $locationName = Get-UserLocationName -UserUpn $upn
    if ($null -eq $locationName) {
        Write-Output "No location found for $upn"
        $locationName = [DBNull]::Value
    } else {
        # Fixed colon issue in string output
        Write-Output "Location for $upn - $locationName"
    }
    
    # Create a hashtable with safe parameter values
    $safeParams = @{}
    $safeParams["UPN"] = $upn.ToString()
    $safeParams["DisplayName"] = if ($displayName) { $displayName.ToString() } else { [DBNull]::Value }
    $safeParams["LineURI"] = if ($lineURI) { $lineURI.ToString() } else { [DBNull]::Value }
    $safeParams["TeamsIPPhonePolicy"] = if ($teamsIPPhonePolicy) { $teamsIPPhonePolicy.ToString() } else { [DBNull]::Value }
    $safeParams["CallingPolicy"] = if ($callingPolicy) { $callingPolicy.ToString() } else { [DBNull]::Value }
    $safeParams["CallerIdPolicy"] = if ($callerIdPolicy) { $callerIdPolicy.ToString() } else { [DBNull]::Value }
    $safeParams["CallQueueMembership"] = if ($callQueueMembership -ne [DBNull]::Value) { $callQueueMembership.ToString() } else { [DBNull]::Value }
    $safeParams["LocationID"] = if ($locationName -ne [DBNull]::Value) { $locationName.ToString() } else { [DBNull]::Value }
    
    # Create query with Call Queue Membership and Location ID columns
    $query = "INSERT INTO $Table (UPN, Display_Name, Line_URI, TeamsIPPhonePolicy, [Calling Policy], [Caller ID Policy], [Call Queue Membership], [Location ID]) " +
             "VALUES (@UPN, @DisplayName, @LineURI, @TeamsIPPhonePolicy, @CallingPolicy, @CallerIdPolicy, @CallQueueMembership, @LocationID)"

    try {
        $cmd = $SQLConnection.CreateCommand()
        $cmd.CommandText = $query
        
        # Add parameters from the safe parameters hashtable
        foreach ($paramName in $safeParams.Keys) {
            $cmd.Parameters.AddWithValue("@$paramName", $safeParams[$paramName]) | Out-Null
        }
        
        $result = $cmd.ExecuteNonQuery()
        
        if ($result -gt 0) {
            $successful++
            # Log appropriate message based on insert success
            $locationOutput = if ($locationName -ne [DBNull]::Value) { "Location - $locationName" } else { "No Location" }
            Write-Output "Successfully inserted record for $upn - $locationOutput"
        } else {
            Write-Warning "Insert for $upn affected 0 rows"
        }
    } catch {
        $errorMessage = $_.Exception.Message
        Write-Error "Insert failed for $upn - $errorMessage"
        
        # Avoid using colons in debug output
        Write-Output "Debug - Parameter Types"
        foreach ($paramName in $safeParams.Keys) {
            if ($safeParams[$paramName] -ne [DBNull]::Value) {
                Write-Output "$paramName type = $($safeParams[$paramName].GetType().Name)"
            }
        }
    }
    $processed++
}

Write-Output "Upload complete. Total processed $processed, Successfully inserted $successful"
Write-Output "Total call queue memberships: $($deviceCallQueueMemberships.Count) shared device accounts"

if ($SQLConnection.State -eq 'Open') {
    $SQLConnection.Close()
    Write-Output "SQL connection closed"
}

Disconnect-MicrosoftTeams -Confirm:$false
Write-Output "Disconnected from Teams. Script execution completed."
