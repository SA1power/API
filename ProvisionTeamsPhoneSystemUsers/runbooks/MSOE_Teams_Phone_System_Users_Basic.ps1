# POLICY BASELINE MAPPING TABLE
# ==============================================================================
# +----------------------------+---------------------+----------------------------------------------------+----+
# | Policy Baseline Name       | Scope               | Policy Map Detail                                  | ID |
# +----------------------------+---------------------+----------------------------------------------------+----+
# | 0-custom                   | custom              | If user does not have the default policy baseline  | 0  |
# | 1-default                  | default             | If user has all defaults:                          | 1  |
# |                            |                     | Tag:MSOE_CPark1,Tag:MSOEDP1,Tag:MSOE_Global_ECRP, |    |
# |                            |                     | Tag:MSOE_Global_ECP,Tag:DRROUTINGPOLICYDOMINT     |    |
# | 2-International Dialing    | Tag:MSOEDP2         | If user has this policy then 2                     | 2  |
# | 3-Voice Application Policy1| Tag:MSOEVAP1        | If user has this policy then 3                     | 3  |
# | 4-SharedDeviceCallingPolicy| Tag:SharedDevices   | If user has this policy then 4                     | 4  |
# | 5-TeamsIPPhonePolicy       | Tag:SharedDevice    | If user has this policy then 5                     | 5  |
# +----------------------------+---------------------+----------------------------------------------------+----+
# If user has three or more of these mapped policies "2", "3", "4" then add them up (like 2+3+4=9)
# and so on. For mapping purposes use the policy baseline names for 0-5 and for 6+ use the
# numerical value as the policy baseline name.
# ==============================================================================
# Script for Azure Automation Runbook using Managed Identity
# Filters Teams users with Enterprise Voice, and inserts into Azure SQL with Policy Baseline mapping
# Now includes Call Queue membership information, Location ID, and IPPhone column
# Set more verbose error handling
$ErrorActionPreference = "Continue"
$VerbosePreference = "Continue"

# Define list of users who should have IPPhone set to "MP56"
$IPPhoneUsers = @(
    "massoels@msoe.edu",
    "bozicevich@msoe.edu",
    "fyfe@msoe.edu",
    "grohmann@msoe.edu",
    "curtisc@msoe.edu",
    "walz@msoe.edu",
    "reuter@msoe.edu",
    "cotton@msoe.edu",
    "listinsky@msoe.edu",
    "bethly@msoe.edu",
    "kochj@msoe.edu",
    "kasprzycki@msoe.edu",
    "purcell@msoe.edu",
    "collard@msoe.edu",
    "rome@msoe.edu",
    "peterjan@msoe.edu"
)

# Function to determine policy baseline based on user policies
function Get-PolicyBaseline {
    param (
        [Parameter(Mandatory=$true)]
        [object]$User
    )
    
    # Initialize policy baseline
    $policyBaseline = "0" # Default to custom
    
    # Extract string values from policy objects for comparison
    $callParkPolicy = ""
    $dialPlan = ""
    $emergencyCallRoutingPolicy = ""
    $emergencyCallingPolicy = ""
    $voiceRoutingPolicy = ""
    $voiceApplicationsPolicy = ""
    $callingPolicy = ""
    
    # Extract Call Park Policy value
    if ($User.TeamsCallParkPolicy) {
        if ($User.TeamsCallParkPolicy -is [System.Object] -and $User.TeamsCallParkPolicy.Identity) {
            $callParkPolicy = $User.TeamsCallParkPolicy.Identity
        } 
        elseif ($User.TeamsCallParkPolicy -is [System.Object] -and $User.TeamsCallParkPolicy.Name) {
            $callParkPolicy = $User.TeamsCallParkPolicy.Name
        }
        else {
            $callParkPolicy = $User.TeamsCallParkPolicy.ToString()
        }
    }
    
    # Extract Dial Plan value
    if ($User.TenantDialPlan) {
        if ($User.TenantDialPlan -is [System.Object] -and $User.TenantDialPlan.Identity) {
            $dialPlan = $User.TenantDialPlan.Identity
        }
        elseif ($User.TenantDialPlan -is [System.Object] -and $User.TenantDialPlan.Name) {
            $dialPlan = $User.TenantDialPlan.Name
        }
        else {
            $dialPlan = $User.TenantDialPlan.ToString()
        }
    }
    
    # Extract Emergency Call Routing Policy value
    if ($User.TeamsEmergencyCallRoutingPolicy) {
        if ($User.TeamsEmergencyCallRoutingPolicy -is [System.Object] -and $User.TeamsEmergencyCallRoutingPolicy.Identity) {
            $emergencyCallRoutingPolicy = $User.TeamsEmergencyCallRoutingPolicy.Identity
        }
        elseif ($User.TeamsEmergencyCallRoutingPolicy -is [System.Object] -and $User.TeamsEmergencyCallRoutingPolicy.Name) {
            $emergencyCallRoutingPolicy = $User.TeamsEmergencyCallRoutingPolicy.Name
        }
        else {
            $emergencyCallRoutingPolicy = $User.TeamsEmergencyCallRoutingPolicy.ToString()
        }
    }
    
    # Extract Emergency Calling Policy value
    if ($User.TeamsEmergencyCallingPolicy) {
        if ($User.TeamsEmergencyCallingPolicy -is [System.Object] -and $User.TeamsEmergencyCallingPolicy.Identity) {
            $emergencyCallingPolicy = $User.TeamsEmergencyCallingPolicy.Identity
        }
        elseif ($User.TeamsEmergencyCallingPolicy -is [System.Object] -and $User.TeamsEmergencyCallingPolicy.Name) {
            $emergencyCallingPolicy = $User.TeamsEmergencyCallingPolicy.Name
        }
        else {
            $emergencyCallingPolicy = $User.TeamsEmergencyCallingPolicy.ToString()
        }
    }
    
    # Extract Voice Routing Policy value
    if ($User.OnlineVoiceRoutingPolicy) {
        if ($User.OnlineVoiceRoutingPolicy -is [System.Object] -and $User.OnlineVoiceRoutingPolicy.Identity) {
            $voiceRoutingPolicy = $User.OnlineVoiceRoutingPolicy.Identity
        }
        elseif ($User.OnlineVoiceRoutingPolicy -is [System.Object] -and $User.OnlineVoiceRoutingPolicy.Name) {
            $voiceRoutingPolicy = $User.OnlineVoiceRoutingPolicy.Name
        }
        else {
            $voiceRoutingPolicy = $User.OnlineVoiceRoutingPolicy.ToString()
        }
    }
    
    # Extract Voice Applications Policy value
    if ($User.TeamsVoiceApplicationsPolicy) {
        if ($User.TeamsVoiceApplicationsPolicy -is [System.Object] -and $User.TeamsVoiceApplicationsPolicy.Identity) {
            $voiceApplicationsPolicy = $User.TeamsVoiceApplicationsPolicy.Identity
        }
        elseif ($User.TeamsVoiceApplicationsPolicy -is [System.Object] -and $User.TeamsVoiceApplicationsPolicy.Name) {
            $voiceApplicationsPolicy = $User.TeamsVoiceApplicationsPolicy.Name
        }
        else {
            $voiceApplicationsPolicy = $User.TeamsVoiceApplicationsPolicy.ToString()
        }
    }
    
    # Extract Calling Policy value
    if ($User.TeamsCallingPolicy) {
        if ($User.TeamsCallingPolicy -is [System.Object] -and $User.TeamsCallingPolicy.Identity) {
            $callingPolicy = $User.TeamsCallingPolicy.Identity
        }
        elseif ($User.TeamsCallingPolicy -is [System.Object] -and $User.TeamsCallingPolicy.Name) {
            $callingPolicy = $User.TeamsCallingPolicy.Name
        }
        else {
            $callingPolicy = $User.TeamsCallingPolicy.ToString()
        }
    }
    
    # Check if user has all defaults
    $hasCallParkDefault = ($callParkPolicy -like "*MSOE_CPark1*")
    $hasDialPlanDefault = ($dialPlan -like "*MSOEDP1*")
    $hasEmergencyCallRoutingDefault = ($emergencyCallRoutingPolicy -like "*MSOE_Global_ECRP*")
    $hasEmergencyCallingDefault = ($emergencyCallingPolicy -like "*MSOE_Global_ECP*")
    $hasVoiceRoutingDefault = ($voiceRoutingPolicy -like "*DRROUTINGPOLICYDOMINT*")
    
    # Check if all default policies are present
    $hasAllDefaults = $hasCallParkDefault -and $hasDialPlanDefault -and 
                      $hasEmergencyCallRoutingDefault -and $hasEmergencyCallingDefault -and
                      $hasVoiceRoutingDefault
    
    if ($hasAllDefaults) {
        $policyBaseline = "1" # Default
    }
    
    # Check for specific policy values
    if ($dialPlan -like "*MSOEDP2*") {
        $policyBaseline = "2" # International Dialing
    }
    elseif ($voiceApplicationsPolicy -like "*MSOEVAP1*") {
        $policyBaseline = "3" # Voice Application Policy 1
    }
    elseif ($callingPolicy -like "*SharedDevices*") {
        $policyBaseline = "4" # SharedDeviceCallingPolicy
    }
    elseif (($callingPolicy -like "*SharedDevice*") -and ($callingPolicy -notlike "*SharedDevices*")) {
        $policyBaseline = "5" # TeamsIPPhonePolicy
    }
    
    # Check for multiple policies (2,3,4)
    $policySum = 0
    if ($dialPlan -like "*MSOEDP2*") { $policySum += 2 }
    if ($voiceApplicationsPolicy -like "*MSOEVAP1*") { $policySum += 3 }
    if ($callingPolicy -like "*SharedDevices*") { $policySum += 4 }
    
    # If user has multiple of these mapped policies, use the sum as the baseline ID
    if ($policySum > 5) {
        $policyBaseline = $policySum.ToString()
    }
    
    # For debugging - use Write-Host instead of Write-Output
    Write-Host "Policy Analysis for $($User.UserPrincipalName):"
    Write-Host "  CallPark: $callParkPolicy"
    Write-Host "  DialPlan: $dialPlan" 
    Write-Host "  EmergencyCallRouting: $emergencyCallRoutingPolicy"
    Write-Host "  EmergencyCalling: $emergencyCallingPolicy"
    Write-Host "  VoiceRouting: $voiceRoutingPolicy"
    Write-Host "  VoiceApplications: $voiceApplicationsPolicy"
    Write-Host "  CallingPolicy: $callingPolicy"
    Write-Host "  Has All Defaults: $hasAllDefaults"
    Write-Host "  Policy Baseline: $policyBaseline"
    
    # Return only the numeric policy baseline ID
    return $policyBaseline
}

# Function to get Group information (from Call Queue script)
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
            Write-Output "Could not retrieve group information for group ID - $GroupId"
        }
    }
    catch {
        # using concatenation to avoid interpolation issues
        Write-Output ("Error retrieving group info for " + $GroupId + " - " + $_)
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
                    # Avoid colons in string output
                    Write-Verbose "Found location for $UserUpn - $locationName"
                }
            }
        }
    }
    catch {
        # Avoid colons in string output
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
$Table = "dbo.MSOE_Teams_Phone_System_Users" # Updated to match the full table name

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

# Hashtable to store user-to-call-queue mappings
$userCallQueueMemberships = @{}

try {
    $callQueues = Get-CsCallQueue -ErrorAction Stop
    if ($callQueues) {
        Write-Output "Found $($callQueues.Count) call queues to check for groups"
    } else {
        Write-Output "No call queues found"
        $callQueues = @()
    }
} catch {
    Write-Output "Error retrieving call queues - $_"
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
            
            Write-Output "Processing group for call queue '$($cq.Name)' - $groupId"
            $groupInfo = Get-GroupInfo -GroupId $groupId -Headers $GraphHeaders
            
            if ($groupInfo.DisplayName -eq "N/A" -and $groupInfo.Email -eq "N/A" -and $groupInfo.Members.Count -eq 0) {
                Write-Output "Could not retrieve details for group - $groupId"
                continue
            }
            
            $processedGroups[$groupId] = $true
            
            # For each member of this group, add this call queue to their memberships
            foreach ($member in $groupInfo.Members) {
                if (-not $userCallQueueMemberships.ContainsKey($member)) {
                    $userCallQueueMemberships[$member] = @()
                }
                
                # Add the call queue name to the user's list of memberships if not already there
                if (-not $userCallQueueMemberships[$member].Contains($cq.Name)) {
                    $userCallQueueMemberships[$member] += $cq.Name
                    Write-Output "User $member is a member of call queue - $($cq.Name)"
                }
            }
        }
    } else {
        Write-Output "Call queue '$($cq.Name)' has no distribution lists/groups assigned"
    }
}

Write-Output "Processed all call queues. Found call queue memberships for $($userCallQueueMemberships.Count) users."
# ===== END OF CALL QUEUE SECTION =====

# Check if the Location ID column exists, if not add it
try {
    $checkLocationColumnCmd = $SQLConnection.CreateCommand()
    $checkLocationColumnCmd.CommandText = "SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'MSOE_Teams_Phone_System_Users' AND COLUMN_NAME = 'Location ID'"
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
    Write-Warning "Error checking or adding Location ID column - $errorMessage"
}

# Check if the IPPhone column exists, if not add it
try {
    $checkIPPhoneColumnCmd = $SQLConnection.CreateCommand()
    $checkIPPhoneColumnCmd.CommandText = "SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'MSOE_Teams_Phone_System_Users' AND COLUMN_NAME = 'IPPhone'"
    $ipPhoneColumnExists = [int]$checkIPPhoneColumnCmd.ExecuteScalar() -gt 0
    
    if (-not $ipPhoneColumnExists) {
        Write-Output "Adding 'IPPhone' column to table..."
        $addIPPhoneColumnCmd = $SQLConnection.CreateCommand()
        $addIPPhoneColumnCmd.CommandText = "ALTER TABLE $Table ADD [IPPhone] NVARCHAR(255)"
        $addIPPhoneColumnCmd.ExecuteNonQuery() | Out-Null
        Write-Output "IPPhone column added successfully"
    } else {
        Write-Output "'IPPhone' column already exists"
    }
} catch {
    $errorMessage = $_.Exception.Message
    Write-Warning "Error checking or adding IPPhone column - $errorMessage"
}

# Get Teams users
Write-Output "Fetching Teams users..."
$teamsUsers = Get-CsOnlineUser |
    Where-Object {
        $_.EnterpriseVoiceEnabled -eq $true -and
        $_.UserPrincipalName -notlike "tsd_*" -and
        $_.UserType -ne "ResourceAccount" -and
        $_.Department -notlike '*Microsoft Communication Application Instance*' -and
        $_.UserPrincipalName -notmatch "autoattendant|callqueue"
    }

Write-Output "Processing all users with Enterprise Voice enabled"
$processed = 0

foreach ($user in $teamsUsers) {
    $upn = $user.UserPrincipalName
    $lineURI = $user.LineURI
    $displayName = $user.DisplayName
    $enabled = if ($user.AccountEnabled) { 'y' } else { 'n' }
    $department = $user.Department
    $title = $user.Title
    
    # Check if this user should have IPPhone set to "MP56"
    $ipPhoneValue = [DBNull]::Value
    if ($IPPhoneUsers -contains $upn) {
        $ipPhoneValue = "MP56"
        Write-Output "Setting IPPhone to MP56 for user: $upn"
    }
    
    # Default values for phone numbers
    $businessPhone = [DBNull]::Value
    $mobilePhone = [DBNull]::Value
    
    # Get user phone numbers from Graph API
    try {
        $userGraphUri = "https://graph.microsoft.com/v1.0/users/$upn"
        $graphResponse = Invoke-RestMethod -Uri $userGraphUri -Headers $GraphHeaders -Method Get
        
        # Extract phone numbers if available
        if ($graphResponse.businessPhones -and $graphResponse.businessPhones.Count -gt 0) {
            # Handle array values - take the first one
            $businessPhone = if ($graphResponse.businessPhones -is [Array]) {
                $graphResponse.businessPhones[0].ToString()
            } else {
                $graphResponse.businessPhones.ToString()
            }
        }
        
        if ($graphResponse.mobilePhone) {
            $mobilePhone = $graphResponse.mobilePhone.ToString()
        }
        
        Write-Output "Retrieved phone info for - $upn"
    } catch {
        $errorMessage = $_.Exception.Message
        Write-Warning "Failed to retrieve phone info for $upn - $errorMessage"
        # Continue with the process even if we can't get phone numbers
    }

    # Get Teams policies for the user
    try {
        # Initialize policy variables with default values
        $callParkPolicy = [DBNull]::Value
        $callerIDPolicy = [DBNull]::Value
        $callingPolicy = [DBNull]::Value
        $dialPlan = [DBNull]::Value
        $emergencyCallRoutingPolicy = [DBNull]::Value
        $emergencyCallingPolicy = [DBNull]::Value
        $voiceRoutingPolicy = [DBNull]::Value
        $voiceApplicationsPolicy = [DBNull]::Value

        # Get Call Park Policy - Extract the string value
        if ($user.TeamsCallParkPolicy) {
            # If it's an object, try to get the Identity or Name property
            if ($user.TeamsCallParkPolicy -is [System.Object] -and $user.TeamsCallParkPolicy.Identity) {
                $callParkPolicy = $user.TeamsCallParkPolicy.Identity.ToString()
            } 
            elseif ($user.TeamsCallParkPolicy -is [System.Object] -and $user.TeamsCallParkPolicy.Name) {
                $callParkPolicy = $user.TeamsCallParkPolicy.Name.ToString()
            }
            # If it's already a string or can be converted to one
            else {
                $callParkPolicy = $user.TeamsCallParkPolicy.ToString()
            }
        }

        # Get Caller ID Policy - Extract the string value
        if ($user.CallingLineIdentity) {
            if ($user.CallingLineIdentity -is [System.Object] -and $user.CallingLineIdentity.Identity) {
                $callerIDPolicy = $user.CallingLineIdentity.Identity.ToString()
            }
            elseif ($user.CallingLineIdentity -is [System.Object] -and $user.CallingLineIdentity.Name) {
                $callerIDPolicy = $user.CallingLineIdentity.Name.ToString()
            }
            else {
                $callerIDPolicy = $user.CallingLineIdentity.ToString()
            }
        }

        # Get Calling Policy - Extract the string value
        if ($user.TeamsCallingPolicy) {
            if ($user.TeamsCallingPolicy -is [System.Object] -and $user.TeamsCallingPolicy.Identity) {
                $callingPolicy = $user.TeamsCallingPolicy.Identity.ToString()
            }
            elseif ($user.TeamsCallingPolicy -is [System.Object] -and $user.TeamsCallingPolicy.Name) {
                $callingPolicy = $user.TeamsCallingPolicy.Name.ToString()
            }
            else {
                $callingPolicy = $user.TeamsCallingPolicy.ToString()
            }
        }

        # Get Dial Plan - Extract the string value
        if ($user.TenantDialPlan) {
            if ($user.TenantDialPlan -is [System.Object] -and $user.TenantDialPlan.Identity) {
                $dialPlan = $user.TenantDialPlan.Identity.ToString()
            }
            elseif ($user.TenantDialPlan -is [System.Object] -and $user.TenantDialPlan.Name) {
                $dialPlan = $user.TenantDialPlan.Name.ToString()
            }
            else {
                $dialPlan = $user.TenantDialPlan.ToString()
            }
        }

        # Get Emergency Call Routing Policy - Extract the string value
        if ($user.TeamsEmergencyCallRoutingPolicy) {
            if ($user.TeamsEmergencyCallRoutingPolicy -is [System.Object] -and $user.TeamsEmergencyCallRoutingPolicy.Identity) {
                $emergencyCallRoutingPolicy = $user.TeamsEmergencyCallRoutingPolicy.Identity.ToString()
            }
            elseif ($user.TeamsEmergencyCallRoutingPolicy -is [System.Object] -and $user.TeamsEmergencyCallRoutingPolicy.Name) {
                $emergencyCallRoutingPolicy = $user.TeamsEmergencyCallRoutingPolicy.Name.ToString()
            }
            else {
                $emergencyCallRoutingPolicy = $user.TeamsEmergencyCallRoutingPolicy.ToString()
            }
        }

        # Get Emergency Calling Policy - Extract the string value
        if ($user.TeamsEmergencyCallingPolicy) {
            if ($user.TeamsEmergencyCallingPolicy -is [System.Object] -and $user.TeamsEmergencyCallingPolicy.Identity) {
                $emergencyCallingPolicy = $user.TeamsEmergencyCallingPolicy.Identity.ToString()
            }
            elseif ($user.TeamsEmergencyCallingPolicy -is [System.Object] -and $user.TeamsEmergencyCallingPolicy.Name) {
                $emergencyCallingPolicy = $user.TeamsEmergencyCallingPolicy.Name.ToString()
            }
            else {
                $emergencyCallingPolicy = $user.TeamsEmergencyCallingPolicy.ToString()
            }
        }

        # Get Voice Routing Policy - Extract the string value
        if ($user.OnlineVoiceRoutingPolicy) {
            if ($user.OnlineVoiceRoutingPolicy -is [System.Object] -and $user.OnlineVoiceRoutingPolicy.Identity) {
                $voiceRoutingPolicy = $user.OnlineVoiceRoutingPolicy.Identity.ToString()
            }
            elseif ($user.OnlineVoiceRoutingPolicy -is [System.Object] -and $user.OnlineVoiceRoutingPolicy.Name) {
                $voiceRoutingPolicy = $user.OnlineVoiceRoutingPolicy.Name.ToString()
            }
            else {
                $voiceRoutingPolicy = $user.OnlineVoiceRoutingPolicy.ToString()
            }
        }

        # Get Voice Applications Policy - Extract the string value
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

        Write-Output "Retrieved Teams policies for - $upn"
    } catch {
        $errorMessage = $_.Exception.Message
        Write-Warning "Failed to retrieve Teams policies for $upn - $errorMessage"
        # Continue with the process even if we can't get policies
    }

    # Calculate policy baseline
    $policyBaseline = Get-PolicyBaseline -User $user
    
    # Make sure policyBaseline is a string, not an array
    if ($policyBaseline -is [Array]) {
        # If it's an array, take the last element (which should be the actual number)
        $policyBaseline = $policyBaseline[-1].ToString()
    } else {
        # Otherwise ensure it's a string
        $policyBaseline = $policyBaseline.ToString()
    }

    # Get Call Queue Memberships for this user
    $callQueueMembership = [DBNull]::Value
    
    # Try to match by UPN first
    if ($userCallQueueMemberships.ContainsKey($upn)) {
        $callQueueMembership = $userCallQueueMemberships[$upn] -join ";"
    }
    # If not found by UPN, try to match by email
    elseif ($graphResponse.mail -and $userCallQueueMemberships.ContainsKey($graphResponse.mail)) {
        $callQueueMembership = $userCallQueueMemberships[$graphResponse.mail] -join ";"
    }
    
    if ($callQueueMembership -ne [DBNull]::Value) {
        Write-Output "User $upn is a member of call queues - $callQueueMembership"
    } else {
        Write-Output "User $upn is not a member of any call queues"
    }

    # Get Location ID for this user
    $locationName = Get-UserLocationName -UserUpn $upn
    if ($null -eq $locationName) {
        Write-Output "No location found for $upn"
        $locationName = [DBNull]::Value
    } else {
        # Avoid colon in string output
        Write-Output "Location for $upn - $locationName"
    }

    # Create a hashtable with safe parameter values
    $safeParams = @{}
    $safeParams["UPN"] = $upn.ToString()
    $safeParams["BusinessPhone"] = if ($businessPhone -ne [DBNull]::Value) { $businessPhone.ToString() } else { [DBNull]::Value }
    $safeParams["MobilePhone"] = if ($mobilePhone -ne [DBNull]::Value) { $mobilePhone.ToString() } else { [DBNull]::Value }
    $safeParams["LineURI"] = if ($lineURI) { $lineURI.ToString() } else { [DBNull]::Value }
    $safeParams["DisplayName"] = if ($displayName) { $displayName.ToString() } else { [DBNull]::Value }
    $safeParams["AccountEnabled"] = if ($enabled) { $enabled.ToString() } else { [DBNull]::Value }
    $safeParams["Department"] = if ($department) { $department.ToString() } else { [DBNull]::Value }
    $safeParams["JobDescription"] = if ($title) { $title.ToString() } else { [DBNull]::Value }
    $safeParams["CallParkPolicy"] = if ($callParkPolicy -ne [DBNull]::Value) { $callParkPolicy.ToString() } else { [DBNull]::Value }
    $safeParams["CallerIDPolicy"] = if ($callerIDPolicy -ne [DBNull]::Value) { $callerIDPolicy.ToString() } else { [DBNull]::Value }
    $safeParams["CallingPolicy"] = if ($callingPolicy -ne [DBNull]::Value) { $callingPolicy.ToString() } else { [DBNull]::Value }
    $safeParams["DialPlan"] = if ($dialPlan -ne [DBNull]::Value) { $dialPlan.ToString() } else { [DBNull]::Value }
    $safeParams["EmergencyCallRoutingPolicy"] = if ($emergencyCallRoutingPolicy -ne [DBNull]::Value) { $emergencyCallRoutingPolicy.ToString() } else { [DBNull]::Value }
    $safeParams["EmergencyCallingPolicy"] = if ($emergencyCallingPolicy -ne [DBNull]::Value) { $emergencyCallingPolicy.ToString() } else { [DBNull]::Value }
    $safeParams["VoiceRoutingPolicy"] = if ($voiceRoutingPolicy -ne [DBNull]::Value) { $voiceRoutingPolicy.ToString() } else { [DBNull]::Value }
    $safeParams["VoiceApplicationsPolicy"] = if ($voiceApplicationsPolicy -ne [DBNull]::Value) { $voiceApplicationsPolicy.ToString() } else { [DBNull]::Value }
    $safeParams["PolicyBaseline"] = $policyBaseline
    $safeParams["CallQueueMembership"] = if ($callQueueMembership -ne [DBNull]::Value) { $callQueueMembership.ToString() } else { [DBNull]::Value }
    $safeParams["LocationID"] = if ($locationName -ne [DBNull]::Value) { $locationName.ToString() } else { [DBNull]::Value }
    $safeParams["IPPhone"] = if ($ipPhoneValue -ne [DBNull]::Value) { $ipPhoneValue.ToString() } else { [DBNull]::Value }
    
    # Insert SQL command with added policy columns, policy baseline, Call Queue Membership, Location ID, and IPPhone
    $query = "INSERT INTO $Table (UPN, Business_Phone, Mobile_Phone, Line_URI, Display_Name, Account_Enabled, Department, Job_Description, " +
             "[Call Park Policy], [Caller ID Policy], [Calling Policy], [Dial Plan], [Emergency Call Routing Policy], " +
             "[Emergency Calling Policy], [Voice Routing Policy], [Voice Applications Policy], Policy_Baseline, " +
             "[Call Queue Membership], [Location ID], [IPPhone]) " +
             "VALUES (@UPN, @BusinessPhone, @MobilePhone, @LineURI, @DisplayName, @AccountEnabled, @Department, @JobDescription, " +
             "@CallParkPolicy, @CallerIDPolicy, @CallingPolicy, @DialPlan, @EmergencyCallRoutingPolicy, " +
             "@EmergencyCallingPolicy, @VoiceRoutingPolicy, @VoiceApplicationsPolicy, @PolicyBaseline, " +
             "@CallQueueMembership, @LocationID, @IPPhone)"

    try {
        $cmd = $SQLConnection.CreateCommand()
        $cmd.CommandText = $query
        
        # Add parameters from the safe parameters hashtable
        foreach ($paramName in $safeParams.Keys) {
            $cmd.Parameters.AddWithValue("@$paramName", $safeParams[$paramName]) | Out-Null
        }
        
        $cmd.ExecuteNonQuery() | Out-Null
        
        # Log appropriate message based on insert success
        $locationOutput = if ($locationName -ne [DBNull]::Value) { "Location - $locationName" } else { "No Location" }
        $queueOutput = if ($callQueueMembership -ne [DBNull]::Value) { "Call Queues - $callQueueMembership" } else { "No Call Queues" }
        $ipPhoneOutput = if ($ipPhoneValue -ne [DBNull]::Value) { "IPPhone - $ipPhoneValue" } else { "No IPPhone" }
        
        Write-Output "Inserted: $upn with Policy Baseline: $policyBaseline, $locationOutput, $queueOutput, $ipPhoneOutput"
    } catch {
        $errorMessage = $_.Exception.Message
        Write-Warning "Insert failed for $upn - $errorMessage"
        
        # Debug output of parameters for troubleshooting
        Write-Output "Debug - Parameter Types for $($upn):"
        foreach ($paramName in $safeParams.Keys) {
            if ($safeParams[$paramName] -ne [DBNull]::Value) {
                Write-Output "$($paramName) - $($safeParams[$paramName].GetType().FullName)"
            }
        }
    }

    $processed++
}

Write-Output "Upload complete. Total processed: $processed users"
Write-Output "Total call queue memberships found: $($userCallQueueMemberships.Count) users"
Write-Output "IPPhone set to MP56 for $($IPPhoneUsers.Count) users"

# Cleanup
if ($SQLConnection.State -eq 'Open') {
    $SQLConnection.Close()
    Write-Output "SQL connection closed"
}

Disconnect-MicrosoftTeams -Confirm:$false
Write-Output "Disconnected from Teams. Script execution completed."
