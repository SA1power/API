# v47 Enhanced with comprehensive deprovisioning
# Function to add user to distribution group using Graph API (matching working runbook pattern)
function Add-UserToDistributionGroup {
    param(
        [Parameter(Mandatory=$true)]
        [string]$GroupId,
        [Parameter(Mandatory=$true)]
        [string]$UserUPN,
        [Parameter(Mandatory=$true)]
        [hashtable]$Headers,
        [string]$GroupName = "Unknown"
    )
    
    try {
        # First, get the user's Graph ID
        $userGraphUri = "https://graph.microsoft.com/v1.0/users/$UserUPN"
        $userResponse = Invoke-RestMethod -Uri $userGraphUri -Headers $Headers -Method Get -ErrorAction Stop
        $userObjectId = $userResponse.id
        
        # Check if user is already a member
        $membersUri = "https://graph.microsoft.com/v1.0/groups/$GroupId/members"
        $membersResponse = Invoke-RestMethod -Uri $membersUri -Headers $Headers -Method Get -ErrorAction SilentlyContinue
        
        $isAlreadyMember = $false
        if ($membersResponse -and $membersResponse.value) {
            foreach ($member in $membersResponse.value) {
                if ($member.id -eq $userObjectId -or $member.userPrincipalName -eq $UserUPN) {
                    $isAlreadyMember = $true
                    break
                }
            }
        }
        
        if ($isAlreadyMember) {
            return $true
        }
        
        # Add the user to the group using Graph API
        $addMemberUri = "https://graph.microsoft.com/v1.0/groups/$GroupId/members/`$ref"
        $addMemberBody = @{
            "@odata.id" = "https://graph.microsoft.com/v1.0/directoryObjects/$userObjectId"
        } | ConvertTo-Json
        
        Invoke-RestMethod -Uri $addMemberUri -Headers $Headers -Method Post -Body $addMemberBody -ErrorAction Stop
        return $true
        
    } catch {
        return $false
    }
}

# Function to get group information using Graph API
function Get-GroupInfoViaGraph {
    param(
        [Parameter(Mandatory=$true)]
        [string]$GroupId,
        [Parameter(Mandatory=$true)]
        [hashtable]$Headers
    )
    
    $result = @{
        Found = $false
    }
    
    try {
        $groupUri = "https://graph.microsoft.com/v1.0/groups/$GroupId"
        $groupResponse = Invoke-RestMethod -Uri $groupUri -Headers $Headers -Method Get -ErrorAction Stop
        
        if ($groupResponse) {
            $result.Found = $true
        }
    } catch {
        # Silently handle errors for cleaner logging
    }
    
    return $result
}

# Function to safely handle null values for SQL parameters
function Get-SafeSqlValue {
    param($Value, [int]$MaxLength = 0)
    if ($null -eq $Value -or $Value -eq "") {
        return [DBNull]::Value
    }
    
    $stringValue = $Value.ToString()
    
    # Truncate if MaxLength is specified and value exceeds it
    if ($MaxLength -gt 0 -and $stringValue.Length -gt $MaxLength) {
        $stringValue = $stringValue.Substring(0, $MaxLength)
        Write-Warning "Value truncated to $MaxLength characters: $stringValue"
    }
    
    return $stringValue
}

# Function to assign Teams Premium license to user
function Assign-TeamsPremiumLicense {
    param(
        [Parameter(Mandatory=$true)]
        [string]$UserUPN,
        [Parameter(Mandatory=$true)]
        [hashtable]$Headers
    )
    
    $licenseSkuId = "960a972f-d017-4a17-8f64-b42c8035bc7d"  # Teams_Premium_for_Faculty
    
    try {
        # First check if user already has the license
        $userLicensesUri = "https://graph.microsoft.com/v1.0/users/$UserUPN/licenseDetails"
        $currentLicenses = Invoke-RestMethod -Uri $userLicensesUri -Headers $Headers -Method Get -ErrorAction Stop
        
        $hasLicense = $false
        foreach ($license in $currentLicenses.value) {
            if ($license.skuId -eq $licenseSkuId) {
                $hasLicense = $true
                Write-Output "â User $UserUPN already has Teams Premium license"
                return $true
            }
        }
        
        if (-not $hasLicense) {
            # Assign the license
            $assignLicenseUri = "https://graph.microsoft.com/v1.0/users/$UserUPN/assignLicense"
            $licenseBody = @{
                addLicenses = @(
                    @{
                        skuId = $licenseSkuId
                    }
                )
                removeLicenses = @()
            } | ConvertTo-Json -Depth 3
            
            Invoke-RestMethod -Uri $assignLicenseUri -Headers $Headers -Method Post -Body $licenseBody -ContentType "application/json" -ErrorAction Stop
            Write-Output "â Successfully assigned Teams Premium license to $UserUPN"
            
            # Wait a moment for license to propagate
            Start-Sleep -Seconds 5
            return $true
        }
        
    } catch {
        $errorMsg = $_.Exception.Message
        Write-Warning "â Failed to assign Teams Premium license to ${UserUPN}: $errorMsg"
        return $false
    }
}

# Azure Automation Runbook: Process TempUsers table and provision Teams Phone users
# Set error handling preference
$ErrorActionPreference = 'Stop'
$VerbosePreference = "Continue"

# Initialize script completion flag
$script:completedSuccessfully = $false

# PARAMETERS
$SqlServer = "msoevendor.database.windows.net"
$Database = "CEProjectData"
$TempUsersTable = "dbo.tempusers"
$ChangeLogTable = "dbo.TempUsers_Change_Log"

# Connect to Azure using Managed Identity
try {
    Connect-AzAccount -Identity -ErrorAction Stop
    Write-Output "Connected to Azure using Managed Identity"
} catch {
    $errorMessage = $_.Exception.Message
    Write-Error "Failed to connect to Azure: $errorMessage"
    throw
}

# Get access token for Azure SQL
try {
    $SQLAccessTokenObj = Get-AzAccessToken -ResourceUrl "https://database.windows.net"
    $SQLAccessTokenText = $SQLAccessTokenObj.Token
    Write-Output "SQL token acquired"
} catch {
    $errorMessage = $_.Exception.Message
    Write-Error "Could not get SQL access token: $errorMessage"
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

# Initialize Graph API Request Headers
$GraphHeaders = @{
    "Authorization" = "Bearer $GraphToken"
    "Content-Type" = "application/json"
}

# Initialize SQL Connection
try {
    $SQLConnection = New-Object System.Data.SqlClient.SqlConnection
    $SQLConnection.ConnectionString = "Server=$SqlServer;Database=$Database;Integrated Security=False;Encrypt=True;TrustServerCertificate=False;"
    $SQLConnection.AccessToken = $SQLAccessTokenText
    $SQLConnection.Open()
    Write-Output "Connected to Azure SQL"
} catch {
    $errorMessage = $_.Exception.Message
    Write-Error "Could not connect to SQL DB: $errorMessage"
    throw
}

# Check if there are any records in tempusers table first
try {
    $countCmd = $SQLConnection.CreateCommand()
    $countCmd.CommandText = "SELECT COUNT(*) FROM $TempUsersTable"
    $recordCount = [int]$countCmd.ExecuteScalar()
    
    if ($recordCount -eq 0) {
        Write-Output "No records found in tempusers table. Exiting runbook."
        $SQLConnection.Close()
        Write-Output "SQL connection closed. Runbook execution completed with no work to do."
        return
    }
    
    Write-Output "Found $recordCount records in tempusers table. Proceeding with processing..."
} catch {
    $errorMessage = $_.Exception.Message
    Write-Error "Could not check record count in tempusers table: $errorMessage"
    $SQLConnection.Close()
    throw
}

# Connect to Microsoft Teams
try {
    Connect-MicrosoftTeams -Identity -ErrorAction Stop
    Write-Output "Connected to Microsoft Teams"
} catch {
    $errorMessage = $_.Exception.Message
    Write-Error "Failed to connect to Microsoft Teams: $errorMessage"
    $SQLConnection.Close()
    throw
}

try {
    # Fetch all rows from tempusers
    $cmd = $SQLConnection.CreateCommand()
    $cmd.CommandText = "SELECT * FROM $TempUsersTable"
    $reader = $cmd.ExecuteReader()

    # Store results in array to close reader before processing
    $tempUsers = @()
    while ($reader.Read()) {
        $tempUser = @{
            UPN = $reader["UPN"]
            LineURI = $reader["Line_URI"]
            Deprovision = $reader["Deprovision"]
            AccountEnabled = $reader["Account_Enabled"]
            IPPhone = $reader["IPPhone"]
            PolicyBaseline = $reader["Policy_Baseline"]
            CallParkPolicy = $reader["Call Park Policy"]
            CallerIDPolicy = $reader["Caller ID Policy"]
            DialPlan = $reader["Dial Plan"]
            LocationID = $reader["Location ID"]
            ECRP = $reader["Emergency Call Routing Policy"]
            ECP = $reader["Emergency Calling Policy"]
            VoiceRoutingPolicy = $reader["Voice Routing Policy"]
            VoiceApplicationsPolicy = $reader["Voice Applications Policy"]
            CQGroupName = $reader["Call Queue Group Name"]
            CQGroupGUID = $reader["Call_Queue_Group_GUID"]
            AAUsers = $reader["AA_AuthorizedUser"]
            CQUsers = $reader["CQ_AuthorizedUser"]
        }
        $tempUsers += $tempUser
    }
    $reader.Close()

    Write-Output "Found $($tempUsers.Count) users to process"

    # Track successfully processed users for removal from tempusers table
    $successfullyProcessedUsers = @()

    # Process each user
    foreach ($tempUser in $tempUsers) {
        $UPN = $tempUser.UPN
        $LineURI = $tempUser.LineURI
        $Deprovision = $tempUser.Deprovision
        $AccountEnabled = $tempUser.AccountEnabled
        $IPPhone = $tempUser.IPPhone
        $PolicyBaseline = $tempUser.PolicyBaseline
        $CallParkPolicy = $tempUser.CallParkPolicy
        $CallerIDPolicy = $tempUser.CallerIDPolicy
        $DialPlan = $tempUser.DialPlan
        $LocationID = $tempUser.LocationID
        $ECRP = $tempUser.ECRP
        $ECP = $tempUser.ECP
        $VoiceRoutingPolicy = $tempUser.VoiceRoutingPolicy
        $VoiceApplicationsPolicy = $tempUser.VoiceApplicationsPolicy
        $CQGroupName = $tempUser.CQGroupName
        $CQGroupGUID = $tempUser.CQGroupGUID
        $AAUsers = $tempUser.AAUsers
        $CQUsers = $tempUser.CQUsers

        try {
            if ($Deprovision -eq 'Y') {
                # Enhanced Deprovision user
                Write-Output "Deprovisioning $UPN..."
                
                # Step 1: Remove phone number and disable Enterprise Voice
                try {
                    Remove-CsPhoneNumberAssignment -Identity $UPN -RemoveAll -ErrorAction Stop
                    Write-Output "â Successfully removed phone number assignments for $UPN"
                } catch {
                    Write-Warning "Remove-CsPhoneNumberAssignment failed, trying Set-CsUser approach: $($_.Exception.Message)"
                    Set-CsUser -Identity $UPN -EnterpriseVoiceEnabled $false -LineURI $null -ErrorAction Stop
                    Write-Output "â Successfully disabled Enterprise Voice for $UPN using Set-CsUser"
                }
                
                # Step 2: Remove all Teams voice policies
                Write-Output "Removing Teams voice policies for $UPN..."
                
                try {
                    Grant-CsTeamsCallParkPolicy -Identity $UPN -PolicyName $null -ErrorAction SilentlyContinue
                    Write-Output "  Removed Call Park Policy"
                } catch {
                    Write-Warning "  Could not remove Call Park Policy: $($_.Exception.Message)"
                }
                
                try {
                    Grant-CsTenantDialPlan -Identity $UPN -PolicyName $null -ErrorAction SilentlyContinue
                    Write-Output "  Removed Dial Plan"
                } catch {
                    Write-Warning "  Could not remove Dial Plan: $($_.Exception.Message)"
                }
                
                try {
                    Grant-CsTeamsEmergencyCallRoutingPolicy -Identity $UPN -PolicyName $null -ErrorAction SilentlyContinue
                    Write-Output "  Removed Emergency Call Routing Policy"
                } catch {
                    Write-Warning "  Could not remove Emergency Call Routing Policy: $($_.Exception.Message)"
                }
                
                try {
                    Grant-CsTeamsEmergencyCallingPolicy -Identity $UPN -PolicyName $null -ErrorAction SilentlyContinue
                    Write-Output "  Removed Emergency Calling Policy"
                } catch {
                    Write-Warning "  Could not remove Emergency Calling Policy: $($_.Exception.Message)"
                }
                
                try {
                    Grant-CsOnlineVoiceRoutingPolicy -Identity $UPN -PolicyName $null -ErrorAction SilentlyContinue
                    Write-Output "  Removed Voice Routing Policy"
                } catch {
                    Write-Warning "  Could not remove Voice Routing Policy: $($_.Exception.Message)"
                }
                
                try {
                    Grant-CsTeamsVoiceApplicationsPolicy -Identity $UPN -PolicyName $null -ErrorAction SilentlyContinue
                    Write-Output "  Removed Voice Applications Policy"
                } catch {
                    Write-Warning "  Could not remove Voice Applications Policy: $($_.Exception.Message)"
                }
                
                try {
                    Grant-CsCallingLineIdentity -Identity $UPN -PolicyName $null -ErrorAction SilentlyContinue
                    Write-Output "  Removed Caller ID Policy"
                } catch {
                    Write-Warning "  Could not remove Caller ID Policy: $($_.Exception.Message)"
                }
                
                # Step 3: Remove from Call Queue Groups ONLY (using data from tempusers table)
                if ($CQGroupGUID -and $CQGroupGUID -ne [DBNull]::Value -and $CQGroupGUID.ToString().Trim() -ne "") {
                    Write-Output "Removing $UPN from Call Queue groups..."
                    
                    $callQueueGuidsRaw = $CQGroupGUID.ToString()
                    $callQueueGuids = @()
                    if ($callQueueGuidsRaw.Contains(';')) {
                        $callQueueGuids = $callQueueGuidsRaw.Split(';', [StringSplitOptions]::RemoveEmptyEntries)
                    } else {
                        $callQueueGuids = @($callQueueGuidsRaw.Trim())
                    }
                    
                    $callQueueGroupsRaw = if ($CQGroupName -and $CQGroupName -ne [DBNull]::Value) { 
                        $CQGroupName.ToString() 
                    } else { "" }
                    
                    $callQueueGroups = @()
                    if ($callQueueGroupsRaw -ne "") {
                        if ($callQueueGroupsRaw.Contains(';')) {
                            $callQueueGroups = $callQueueGroupsRaw.Split(';', [StringSplitOptions]::RemoveEmptyEntries)
                        } else {
                            $callQueueGroups = @($callQueueGroupsRaw.Trim())
                        }
                    }
                    
                    for ($i = 0; $i -lt $callQueueGuids.Length; $i++) {
                        $groupGuid = $callQueueGuids[$i].ToString().Trim()
                        $groupName = if ($i -lt $callQueueGroups.Length) { 
                            $callQueueGroups[$i].ToString().Trim() 
                        } else { 
                            "Unknown Group" 
                        }
                        
                        if ($groupGuid -and $groupGuid -ne "") {
                            try {
                                # Get user's object ID
                                $userGraphUri = "https://graph.microsoft.com/v1.0/users/$UPN"
                                $userResponse = Invoke-RestMethod -Uri $userGraphUri -Headers $GraphHeaders -Method Get -ErrorAction Stop
                                $userObjectId = $userResponse.id
                                
                                # Remove user from group
                                $removeMemberUri = "https://graph.microsoft.com/v1.0/groups/$groupGuid/members/$userObjectId/`$ref"
                                Invoke-RestMethod -Uri $removeMemberUri -Headers $GraphHeaders -Method Delete -ErrorAction Stop
                                Write-Output "  â Removed from Call Queue Group: $groupName"
                                
                            } catch {
                                if ($_.Exception.Response.StatusCode -eq 'NotFound') {
                                    Write-Output "  User not found in Call Queue Group: $groupName"
                                } else {
                                    Write-Warning "  â Failed to remove from Call Queue Group '$groupName': $($_.Exception.Message)"
                                }
                            }
                        }
                    }
                }
                
                # Step 4: Remove as authorized user from Auto Attendants
                if ($AAUsers -and $AAUsers -ne [DBNull]::Value -and $AAUsers.ToString().Trim() -ne "") {
                    $autoAttendants = $AAUsers.ToString().Split(';', [StringSplitOptions]::RemoveEmptyEntries)
                    Write-Output "Removing $UPN as authorized user from Auto Attendants..."
                    
                    foreach ($aaName in $autoAttendants) {
                        $aaNameString = $aaName.ToString().Trim()
                        if ($aaNameString -and $aaNameString -ne "") {
                            try {
                                $autoAttendant = Get-CsAutoAttendant | Where-Object {$_.Name -eq $aaNameString -or $_.Name -like "*$aaNameString*"} | Select-Object -First 1
                                
                                if ($autoAttendant) {
                                    # Get user GUID
                                    $userGuid = $null
                                    try {
                                        $userObject = Get-CsOnlineUser -Identity $UPN -ErrorAction Stop
                                        $userGuid = [guid]$userObject.Identity
                                    } catch {
                                        try {
                                            $userGraphUri = "https://graph.microsoft.com/v1.0/users/$UPN"
                                            $userResponse = Invoke-RestMethod -Uri $userGraphUri -Headers $GraphHeaders -Method Get -ErrorAction Stop
                                            $userGuid = [guid]$userResponse.id
                                        } catch {
                                            Write-Warning "Could not get GUID for user $UPN"
                                        }
                                    }
                                    
                                    if ($userGuid) {
                                        $currentAuthorizedUsers = if ($autoAttendant.AuthorizedUsers) { 
                                            @($autoAttendant.AuthorizedUsers)
                                        } else { 
                                            @() 
                                        }
                                        
                                        # Filter out the user being deprovisioned
                                        $updatedAuthorizedUsers = @($currentAuthorizedUsers | Where-Object { $_.ToString() -ne $userGuid.ToString() })
                                        
                                        if ($updatedAuthorizedUsers.Count -lt $currentAuthorizedUsers.Count) {
                                            $autoAttendant.AuthorizedUsers = $updatedAuthorizedUsers
                                            Set-CsAutoAttendant -Instance $autoAttendant -ErrorAction Stop
                                            Write-Output "  â Removed as authorized user from Auto Attendant: $($autoAttendant.Name)"
                                        } else {
                                            Write-Output "  User was not authorized for Auto Attendant: $($autoAttendant.Name)"
                                        }
                                    }
                                } else {
                                    Write-Warning "  Auto Attendant '$aaNameString' not found"
                                }
                            } catch {
                                Write-Warning "  â Failed to remove from Auto Attendant '$aaNameString': $($_.Exception.Message)"
                            }
                        }
                    }
                }
                
                # Step 5: Remove as authorized user from Call Queues
                if ($CQUsers -and $CQUsers -ne [DBNull]::Value -and $CQUsers.ToString().Trim() -ne "") {
                    $rawManagers = $CQUsers.ToString()
                    Write-Output "Removing $UPN as authorized user from Call Queues..."
                    
                    $callQueueReferences = @()
                    if ($rawManagers.Contains(';')) {
                        $callQueueReferences = $rawManagers.Split(';', [StringSplitOptions]::RemoveEmptyEntries)
                    } else {
                        $callQueueReferences = @($rawManagers.Trim())
                    }
                    
                    foreach ($cqReference in $callQueueReferences) {
                        $cqName = $cqReference.ToString().Trim()
                        if ($cqName -and $cqName -ne "") {
                            try {
                                $originalWarningPreference = $WarningPreference
                                $WarningPreference = 'SilentlyContinue'
                                
                                $callQueue = Get-CsCallQueue | Where-Object {$_.Name -eq $cqName} | Select-Object -First 1
                                
                                if (-not $callQueue) {
                                    $callQueue = Get-CsCallQueue | Where-Object {$_.Name -like "*$cqName*"} | Select-Object -First 1
                                }
                                
                                if ($callQueue) {
                                    # Get user identity
                                    $userIdentity = $null
                                    try {
                                        $userObject = Get-CsOnlineUser -Identity $UPN -ErrorAction Stop
                                        $userIdentity = $userObject.Identity
                                    } catch {
                                        try {
                                            $userGraphUri = "https://graph.microsoft.com/v1.0/users/$UPN"
                                            $userResponse = Invoke-RestMethod -Uri $userGraphUri -Headers $GraphHeaders -Method Get -ErrorAction Stop
                                            $userIdentity = $userResponse.id
                                        } catch {
                                            Write-Warning "Could not get Identity for user $UPN"
                                        }
                                    }
                                    
                                    if ($userIdentity) {
                                        $currentAuthorizedUsers = if ($callQueue.AuthorizedUsers) { 
                                            @($callQueue.AuthorizedUsers)
                                        } else { 
                                            @() 
                                        }
                                        
                                        # Filter out the user being deprovisioned
                                        $updatedAuthorizedUsers = @($currentAuthorizedUsers | Where-Object { $_.ToString() -ne $userIdentity.ToString() })
                                        
                                        if ($updatedAuthorizedUsers.Count -lt $currentAuthorizedUsers.Count) {
                                            Set-CsCallQueue -Identity $callQueue.Identity -AuthorizedUsers $updatedAuthorizedUsers -ErrorAction Stop
                                            Write-Output "  â Removed as authorized user from Call Queue: $($callQueue.Name)"
                                        } else {
                                            Write-Output "  User was not authorized for Call Queue: $($callQueue.Name)"
                                        }
                                    }
                                } else {
                                    Write-Warning "  Call Queue '$cqName' not found"
                                }
                                
                                $WarningPreference = $originalWarningPreference
                                
                            } catch {
                                $WarningPreference = $originalWarningPreference
                                Write-Warning "  â Failed to remove from Call Queue '$cqName': $($_.Exception.Message)"
                            }
                        }
                    }
                }
                
                # Step 6: Remove Teams Premium license
                Write-Output "Removing Teams Premium license from $UPN..."
                $licenseSkuId = "960a972f-d017-4a17-8f64-b42c8035bc7d"  # Teams_Premium_for_Faculty
                
                try {
                    # Check if user has the license
                    $userLicensesUri = "https://graph.microsoft.com/v1.0/users/$UPN/licenseDetails"
                    $currentLicenses = Invoke-RestMethod -Uri $userLicensesUri -Headers $GraphHeaders -Method Get -ErrorAction Stop
                    
                    $hasLicense = $false
                    foreach ($license in $currentLicenses.value) {
                        if ($license.skuId -eq $licenseSkuId) {
                            $hasLicense = $true
                            break
                        }
                    }
                    
                    if ($hasLicense) {
                        # Remove the license
                        $removeLicenseUri = "https://graph.microsoft.com/v1.0/users/$UPN/assignLicense"
                        $licenseBody = @{
                            addLicenses = @()
                            removeLicenses = @($licenseSkuId)
                        } | ConvertTo-Json -Depth 3
                        
                        Invoke-RestMethod -Uri $removeLicenseUri -Headers $GraphHeaders -Method Post -Body $licenseBody -ContentType "application/json" -ErrorAction Stop
                        Write-Output "â Successfully removed Teams Premium license from $UPN"
                    } else {
                        Write-Output "  User does not have Teams Premium license"
                    }
                    
                } catch {
                    Write-Warning "â Failed to remove Teams Premium license from ${UPN}: $($_.Exception.Message)"
                }
                
                $changeType = "Deprovision"
                Write-Output "Successfully deprovisioned $UPN"
            }
            else {
                # Provision/modify user
                Write-Output "Provisioning/modifying $UPN..."
                
                # Step 1: Enable Enterprise Voice
                try {
                    Set-CsUser -Identity $UPN -EnterpriseVoiceEnabled $true -ErrorAction Stop
                    Write-Output "â Successfully enabled Enterprise Voice for $UPN"
                } catch {
                    Write-Warning "Set-CsUser failed, trying Set-CsPhoneNumberAssignment: $($_.Exception.Message)"
                    Set-CsPhoneNumberAssignment -Identity $UPN -EnterpriseVoiceEnabled $true -ErrorAction Stop
                    Write-Output "â Successfully enabled Enterprise Voice for $UPN (alternative method)"
                }

                # Step 2: Assign phone number (if provided)
                if ($LineURI -and $LineURI -ne [DBNull]::Value -and $LineURI.ToString().Trim() -ne "") {
                    $phoneNumber = $LineURI.ToString()
                    
                    # Convert to proper tel:+ format
                    if ($phoneNumber.StartsWith("tel:+")) {
                        $formattedLineURI = $phoneNumber
                        $cleanPhoneNumber = $phoneNumber.Replace("tel:", "")
                    } elseif ($phoneNumber.StartsWith("tel:")) {
                        $number = $phoneNumber.Replace("tel:", "")
                        if (-not $number.StartsWith("+")) {
                            $number = "+$number"
                        }
                        $formattedLineURI = "tel:$number"
                        $cleanPhoneNumber = $number
                    } elseif ($phoneNumber.StartsWith("+")) {
                        $formattedLineURI = "tel:$phoneNumber"
                        $cleanPhoneNumber = $phoneNumber
                    } else {
                        $formattedLineURI = "tel:+$phoneNumber"
                        $cleanPhoneNumber = "+$phoneNumber"
                    }
                    
                    Write-Output "Assigning phone number $formattedLineURI to $UPN..."
                    
                    $locationIdString = if ($LocationID -and $LocationID -ne [DBNull]::Value -and $LocationID.ToString().Trim() -ne "") {
                        $LocationID.ToString().Trim()
                    } else {
                        $null
                    }
                    
                    try {
                        if ($locationIdString) {
                            Write-Output "Assigning phone number $cleanPhoneNumber with emergency location $locationIdString to $UPN..."
                            Set-CsPhoneNumberAssignment -Identity $UPN -PhoneNumber $cleanPhoneNumber -PhoneNumberType DirectRouting -LocationId $locationIdString -ErrorAction Stop
                            Write-Output "â Successfully assigned phone number and emergency location using Set-CsPhoneNumberAssignment"
                        } else {
                            Set-CsPhoneNumberAssignment -Identity $UPN -PhoneNumber $cleanPhoneNumber -PhoneNumberType DirectRouting -ErrorAction Stop
                            Write-Output "â Successfully assigned phone number using Set-CsPhoneNumberAssignment"
                        }
                    } catch {
                        Write-Warning "Set-CsPhoneNumberAssignment failed, trying Set-CsUser with LineURI: $($_.Exception.Message)"
                        Set-CsUser -Identity $UPN -LineURI $formattedLineURI -ErrorAction Stop
                        Write-Output "â Successfully assigned phone number using Set-CsUser with LineURI: $formattedLineURI"
                        
                        if ($locationIdString) {
                            Write-Output "Attempting to assign emergency location separately..."
                            try {
                                Set-CsPhoneNumberAssignment -Identity $UPN -LocationId $locationIdString -ErrorAction Stop
                                Write-Output "â Successfully assigned emergency location separately"
                            } catch {
                                Write-Warning "â Failed to assign emergency location separately: $($_.Exception.Message)"
                            }
                        }
                    }
                }

                # Step 3: Assign policies
                if ($CallParkPolicy -and $CallParkPolicy -ne [DBNull]::Value -and $CallParkPolicy.ToString().Trim() -ne "") {
                    Write-Output "Assigning Call Park Policy '$CallParkPolicy' to $UPN"
                    Grant-CsTeamsCallParkPolicy -Identity $UPN -PolicyName $CallParkPolicy.ToString() -ErrorAction Stop
                }
                
                if ($DialPlan -and $DialPlan -ne [DBNull]::Value -and $DialPlan.ToString().Trim() -ne "") {
                    Write-Output "Assigning Dial Plan '$DialPlan' to $UPN"
                    Grant-CsTenantDialPlan -Identity $UPN -PolicyName $DialPlan.ToString() -ErrorAction Stop
                }
                
                if ($ECRP -and $ECRP -ne [DBNull]::Value -and $ECRP.ToString().Trim() -ne "") {
                    Write-Output "Assigning Emergency Call Routing Policy '$ECRP' to $UPN"
                    Grant-CsTeamsEmergencyCallRoutingPolicy -Identity $UPN -PolicyName $ECRP.ToString() -ErrorAction Stop
                }
                
                if ($ECP -and $ECP -ne [DBNull]::Value -and $ECP.ToString().Trim() -ne "") {
                    Write-Output "Assigning Emergency Calling Policy '$ECP' to $UPN"
                    Grant-CsTeamsEmergencyCallingPolicy -Identity $UPN -PolicyName $ECP.ToString() -ErrorAction Stop
                }
                
                if ($VoiceRoutingPolicy -and $VoiceRoutingPolicy -ne [DBNull]::Value -and $VoiceRoutingPolicy.ToString().Trim() -ne "") {
                    Write-Output "Assigning Voice Routing Policy '$VoiceRoutingPolicy' to $UPN"
                    Grant-CsOnlineVoiceRoutingPolicy -Identity $UPN -PolicyName $VoiceRoutingPolicy.ToString() -ErrorAction Stop
                }
                
                if ($VoiceApplicationsPolicy -and $VoiceApplicationsPolicy -ne [DBNull]::Value -and $VoiceApplicationsPolicy.ToString().Trim() -ne "") {
                    Write-Output "Assigning Voice Applications Policy '$VoiceApplicationsPolicy' to $UPN"
                    Grant-CsTeamsVoiceApplicationsPolicy -Identity $UPN -PolicyName $VoiceApplicationsPolicy.ToString() -ErrorAction Stop
                }

                # Step 4: Handle Call Queue Group Memberships using Graph API
                if ($CQGroupName -and $CQGroupName -ne [DBNull]::Value -and $CQGroupName.ToString().Trim() -ne "") {
                    $callQueueGroupsRaw = $CQGroupName.ToString()
                    $callQueueGroups = $callQueueGroupsRaw.Split(';', [StringSplitOptions]::RemoveEmptyEntries)
                    
                    $callQueueGuidsRaw = if ($CQGroupGUID -and $CQGroupGUID -ne [DBNull]::Value) { 
                        $CQGroupGUID.ToString()
                    } else { "" }
                    
                    $callQueueGuids = @()
                    if ($callQueueGuidsRaw -ne "") { 
                        if ($callQueueGuidsRaw.Contains(';')) {
                            $callQueueGuids = $callQueueGuidsRaw.Split(';', [StringSplitOptions]::RemoveEmptyEntries)
                        } else {
                            $callQueueGuids = @($callQueueGuidsRaw.Trim())
                        }
                    }
                    
                    Write-Output "Processing Call Queue Group memberships for $UPN..."
                    
                    for ($i = 0; $i -lt $callQueueGroups.Length; $i++) {
                        $groupName = $callQueueGroups[$i].ToString().Trim()
                        $groupGuid = if ($i -lt $callQueueGuids.Length) { 
                            $callQueueGuids[$i].ToString().Trim() 
                        } else { 
                            $null 
                        }
                        
                        if ($groupName -and $groupName -ne "" -and $groupGuid -and $groupGuid -ne "") {
                            Write-Output "Adding $UPN to Call Queue Group: '$groupName'"
                            
                            $groupInfo = Get-GroupInfoViaGraph -GroupId $groupGuid -Headers $GraphHeaders
                            
                            if ($groupInfo.Found) {
                                $addResult = Add-UserToDistributionGroup -GroupId $groupGuid -UserUPN $UPN -Headers $GraphHeaders -GroupName $groupName
                                
                                if ($addResult) {
                                    Write-Output "â Successfully added $UPN to Call Queue Group: $groupName"
                                } else {
                                    Write-Warning "â Failed to add $UPN to Call Queue Group: $groupName"
                                }
                            } else {
                                Write-Warning "â Could not find Call Queue Group: $groupName"
                            }
                        } else {
                            Write-Warning "Skipping Call Queue Group with missing or empty name/GUID"
                        }
                    }
                }

                # Step 5: Handle Auto Attendant Authorized Users
                if ($AAUsers -and $AAUsers -ne [DBNull]::Value -and $AAUsers.ToString().Trim() -ne "") {
                    $autoAttendants = $AAUsers.ToString().Split(';', [StringSplitOptions]::RemoveEmptyEntries)
                    Write-Output "Processing Auto Attendant authorizations for $UPN..."
                    
                    # Assign Teams Premium license before adding as authorized user
                    $licenseAssigned = Assign-TeamsPremiumLicense -UserUPN $UPN -Headers $GraphHeaders
                    if (-not $licenseAssigned) {
                        Write-Warning "Could not assign Teams Premium license, but continuing with AA authorization..."
                    }
                    
                    foreach ($aaName in $autoAttendants) {
                        $aaNameString = $aaName.ToString().Trim()
                        if ($aaNameString -and $aaNameString -ne "") {
                            Write-Output "Adding $UPN as authorized user to Auto Attendant: $aaNameString"
                            
                            try {
                                $autoAttendant = Get-CsAutoAttendant | Where-Object {$_.Name -eq $aaNameString -or $_.Name -like "*$aaNameString*"} | Select-Object -First 1
                                
                                if ($autoAttendant) {
                                    $userGuid = $null
                                    try {
                                        $userObject = Get-CsOnlineUser -Identity $UPN -ErrorAction Stop
                                        $userGuid = [guid]$userObject.Identity
                                    } catch {
                                        try {
                                            $userGraphUri = "https://graph.microsoft.com/v1.0/users/$UPN"
                                            $userResponse = Invoke-RestMethod -Uri $userGraphUri -Headers $GraphHeaders -Method Get -ErrorAction Stop
                                            $userGuid = [guid]$userResponse.id
                                        } catch {
                                            Write-Warning "Could not get GUID for user $UPN"
                                        }
                                    }
                                    
                                    if ($userGuid) {
                                        $currentAuthorizedUsers = if ($autoAttendant.AuthorizedUsers) { 
                                            @($autoAttendant.AuthorizedUsers)
                                        } else { 
                                            @() 
                                        }
                                        
                                        $isAlreadyAuthorized = $false
                                        foreach ($authUser in $currentAuthorizedUsers) {
                                            if ($authUser.ToString() -eq $userGuid.ToString()) {
                                                $isAlreadyAuthorized = $true
                                                break
                                            }
                                        }
                                        
                                        if ($isAlreadyAuthorized) {
                                            Write-Output "â User $UPN already authorized for Auto Attendant '$($autoAttendant.Name)'"
                                        } else {
                                            # Proper array concatenation
                                            $updatedAuthorizedUsers = @($currentAuthorizedUsers) + @($userGuid)
                                            $autoAttendant.AuthorizedUsers = $updatedAuthorizedUsers
                                            Set-CsAutoAttendant -Instance $autoAttendant -ErrorAction Stop
                                            Write-Output "â Successfully added $UPN as authorized user for Auto Attendant '$($autoAttendant.Name)'"
                                        }
                                    } else {
                                        Write-Warning "â Could not retrieve GUID for user $UPN"
                                    }
                                } else {
                                    Write-Warning "â Auto Attendant '$aaNameString' not found"
                                }
                                
                            } catch {
                                Write-Warning "â Failed to configure Auto Attendant authorization for '$aaNameString': $($_.Exception.Message)"
                            }
                        }
                    }
                }

                # Step 6: Handle Call Queue Authorized Users/Managers
                if ($CQUsers -and $CQUsers -ne [DBNull]::Value -and $CQUsers.ToString().Trim() -ne "") {
                    $rawManagers = $CQUsers.ToString()
                    Write-Output "Processing Call Queue management authorizations for $UPN..."
                    
                    # Assign Teams Premium license before adding as authorized user
                    $licenseAssigned = Assign-TeamsPremiumLicense -UserUPN $UPN -Headers $GraphHeaders
                    if (-not $licenseAssigned) {
                        Write-Warning "Could not assign Teams Premium license, but continuing with CQ authorization..."
                    }
                    
                    $callQueueReferences = @()
                    if ($rawManagers.Contains(';')) {
                        $callQueueReferences = $rawManagers.Split(';', [StringSplitOptions]::RemoveEmptyEntries)
                    } else {
                        $callQueueReferences = @($rawManagers.Trim())
                    }
                    
                    foreach ($cqReference in $callQueueReferences) {
                        $cqName = $cqReference.ToString().Trim()
                        if ($cqName -and $cqName -ne "") {
                            Write-Output "Adding $UPN as authorized user to Call Queue: '$cqName'"
                            
                            try {
                                $originalWarningPreference = $WarningPreference
                                $WarningPreference = 'SilentlyContinue'
                                
                                $callQueue = Get-CsCallQueue | Where-Object {$_.Name -eq $cqName} | Select-Object -First 1
                                
                                if (-not $callQueue) {
                                    $callQueue = Get-CsCallQueue | Where-Object {$_.Name -like "*$cqName*"} | Select-Object -First 1
                                }
                                
                                if ($callQueue) {
                                    $userIdentity = $null
                                    try {
                                        $userObject = Get-CsOnlineUser -Identity $UPN -ErrorAction Stop
                                        $userIdentity = $userObject.Identity
                                    } catch {
                                        try {
                                            $userGraphUri = "https://graph.microsoft.com/v1.0/users/$UPN"
                                            $userResponse = Invoke-RestMethod -Uri $userGraphUri -Headers $GraphHeaders -Method Get -ErrorAction Stop
                                            $userIdentity = $userResponse.id
                                        } catch {
                                            Write-Warning "Could not get Identity for user $UPN"
                                        }
                                    }
                                    
                                    if ($userIdentity) {
                                        $currentAuthorizedUsers = if ($callQueue.AuthorizedUsers) { 
                                            @($callQueue.AuthorizedUsers)
                                        } else { 
                                            @() 
                                        }
                                        
                                        $isAlreadyAuthorized = $false
                                        foreach ($authUser in $currentAuthorizedUsers) {
                                            if ($authUser.ToString() -eq $userIdentity.ToString()) {
                                                $isAlreadyAuthorized = $true
                                                break
                                            }
                                        }
                                        
                                        if ($isAlreadyAuthorized) {
                                            Write-Output "â User $UPN already authorized for Call Queue '$($callQueue.Name)'"
                                        } else {
                                            # Proper array concatenation
                                            $updatedAuthorizedUsers = @($currentAuthorizedUsers) + @($userIdentity)
                                            Set-CsCallQueue -Identity $callQueue.Identity -AuthorizedUsers $updatedAuthorizedUsers -ErrorAction Stop
                                            Write-Output "â Successfully added $UPN as authorized user for Call Queue '$($callQueue.Name)'"
                                        }
                                    } else {
                                        Write-Warning "â Could not retrieve Identity for user $UPN"
                                    }
                                } else {
                                    Write-Warning "â Call Queue '$cqName' not found"
                                }
                                
                                $WarningPreference = $originalWarningPreference
                                
                            } catch {
                                $WarningPreference = $originalWarningPreference
                                Write-Warning "â Failed to configure Call Queue authorization for '$cqName': $($_.Exception.Message)"
                            }
                        }
                    }
                }

                # Step 7: Verify final configuration
                Write-Output "Verifying configuration for $UPN..."
                try {
                    $user = Get-CsOnlineUser -Identity $UPN -ErrorAction Stop
                    Write-Output "Final configuration for ${UPN}:"
                    Write-Output "  Enterprise Voice Enabled: $($user.EnterpriseVoiceEnabled)"
                    Write-Output "  Line URI: $($user.LineURI)"
                    Write-Output "  Dial Plan: $($user.TenantDialPlan)"
                    Write-Output "  Voice Routing Policy: $($user.OnlineVoiceRoutingPolicy)"
                    
                    try {
                        $phoneAssignment = Get-CsPhoneNumberAssignment -TelephoneNumber $user.LineURI.Replace("tel:", "") -ErrorAction SilentlyContinue
                        if ($phoneAssignment -and $phoneAssignment.LocationId) {
                            $emergencyLocation = Get-CsOnlineLisLocation | Where-Object { $_.LocationId -eq $phoneAssignment.LocationId } | Select-Object -First 1
                            if ($emergencyLocation) {
                                Write-Output "  Emergency Location: $($emergencyLocation.CompanyName)"
                                Write-Output "â Emergency location is assigned"
                            } else {
                                Write-Output "  Emergency Location: LocationId found but location details unavailable"
                                Write-Warning "â Emergency location details not found"
                            }
                        } else {
                            Write-Output "  Emergency Location: Not assigned"
                            Write-Warning "â No emergency location assigned"
                        }
                    } catch {
                        Write-Output "  Emergency Location: Could not verify"
                        Write-Warning "â Could not verify emergency location assignment: $($_.Exception.Message)"
                    }
                    
                    if ($user.EnterpriseVoiceEnabled -eq $true) {
                        Write-Output "â Enterprise Voice is enabled"
                    } else {
                        Write-Warning "â Enterprise Voice is not enabled"
                    }
                    
                    if ($user.LineURI) {
                        Write-Output "â Phone number is assigned"
                    } else {
                        Write-Warning "â No phone number found"
                    }
                } catch {
                    Write-Warning "Failed to verify user configuration: $($_.Exception.Message)"
                }

                $changeType = "Provision"
                Write-Output "Successfully provisioned/modified $UPN"
            }

            # Log successful change to TempUsers_Change_Log
            $insertCmd = $SQLConnection.CreateCommand()
            $now = (Get-Date).ToString("yyyy-MM-dd HH:mm:ss")
            
            $insertCmd.CommandText = @"
INSERT INTO $ChangeLogTable
(
    ChangeType, ChangeDate, UPN, Line_URI, Deprovision, Account_Enabled, IPPhone, Policy_Baseline,
    [Call Park Policy], [Caller ID Policy], [Dial Plan], [Location ID],
    [Emergency Call Routing Policy], [Emergency Calling Policy],
    [Voice Routing Policy], [Voice Applications Policy],
    [Call Queue Group Name], [Call_Queue_Group_GUID],
    [AA_AuthorizedUser], [CQ_AuthorizedUser]
)
VALUES
(
    @ChangeType, @ChangeDate, @UPN, @LineURI, @Deprovision, @AccountEnabled, @IPPhone, @PolicyBaseline,
    @CallParkPolicy, @CallerIDPolicy, @DialPlan, @LocationID,
    @ECRP, @ECP,
    @VoiceRoutingPolicy, @VoiceApplicationsPolicy,
    @CQGroupName, @CQGroupGUID,
    @AAUsers, @CQUsers
)
"@

            $insertCmd.Parameters.AddWithValue('@ChangeType', (Get-SafeSqlValue $changeType -MaxLength 500)) | Out-Null
            $insertCmd.Parameters.AddWithValue('@ChangeDate', $now) | Out-Null
            $insertCmd.Parameters.AddWithValue('@UPN', (Get-SafeSqlValue $UPN -MaxLength 100)) | Out-Null
            $insertCmd.Parameters.AddWithValue('@LineURI', (Get-SafeSqlValue $LineURI -MaxLength 50)) | Out-Null
            $insertCmd.Parameters.AddWithValue('@Deprovision', (Get-SafeSqlValue $Deprovision -MaxLength 50)) | Out-Null
            $insertCmd.Parameters.AddWithValue('@AccountEnabled', (Get-SafeSqlValue $AccountEnabled -MaxLength 50)) | Out-Null
            $insertCmd.Parameters.AddWithValue('@IPPhone', (Get-SafeSqlValue $IPPhone -MaxLength 100)) | Out-Null
            $insertCmd.Parameters.AddWithValue('@PolicyBaseline', (Get-SafeSqlValue $PolicyBaseline -MaxLength 50)) | Out-Null
            $insertCmd.Parameters.AddWithValue('@CallParkPolicy', (Get-SafeSqlValue $CallParkPolicy -MaxLength 100)) | Out-Null
            $insertCmd.Parameters.AddWithValue('@CallerIDPolicy', (Get-SafeSqlValue $CallerIDPolicy -MaxLength 100)) | Out-Null
            $insertCmd.Parameters.AddWithValue('@DialPlan', (Get-SafeSqlValue $DialPlan -MaxLength 100)) | Out-Null
            $insertCmd.Parameters.AddWithValue('@LocationID', (Get-SafeSqlValue $LocationID -MaxLength 100)) | Out-Null
            $insertCmd.Parameters.AddWithValue('@ECRP', (Get-SafeSqlValue $ECRP -MaxLength 100)) | Out-Null
            $insertCmd.Parameters.AddWithValue('@ECP', (Get-SafeSqlValue $ECP -MaxLength 100)) | Out-Null
            $insertCmd.Parameters.AddWithValue('@VoiceRoutingPolicy', (Get-SafeSqlValue $VoiceRoutingPolicy -MaxLength 100)) | Out-Null
            $insertCmd.Parameters.AddWithValue('@VoiceApplicationsPolicy', (Get-SafeSqlValue $VoiceApplicationsPolicy -MaxLength 100)) | Out-Null
            $insertCmd.Parameters.AddWithValue('@CQGroupName', (Get-SafeSqlValue $CQGroupName -MaxLength 400)) | Out-Null
            $insertCmd.Parameters.AddWithValue('@CQGroupGUID', (Get-SafeSqlValue $CQGroupGUID -MaxLength 400)) | Out-Null
            $insertCmd.Parameters.AddWithValue('@AAUsers', (Get-SafeSqlValue $AAUsers -MaxLength 400)) | Out-Null
            $insertCmd.Parameters.AddWithValue('@CQUsers', (Get-SafeSqlValue $CQUsers -MaxLength 400)) | Out-Null

            $insertCmd.ExecuteNonQuery() | Out-Null
            Write-Output "Logged change for $UPN in Change_Log table"
            
            # Add to successful processing list for removal from tempusers table
            $successfullyProcessedUsers += $UPN

        } catch {
            $errorMessage = $_.Exception.Message
            Write-Warning "Failed to process ${UPN}: $errorMessage"
            
            # Log the error to change log table as well
            try {
                $errorCmd = $SQLConnection.CreateCommand()
                $errorCmd.CommandText = @"
INSERT INTO $ChangeLogTable
(ChangeType, ChangeDate, UPN, Line_URI, Deprovision, Account_Enabled, IPPhone, Policy_Baseline,
[Call Park Policy], [Caller ID Policy], [Dial Plan], [Location ID],
[Emergency Call Routing Policy], [Emergency Calling Policy],
[Voice Routing Policy], [Voice Applications Policy],
[Call Queue Group Name], [Call_Queue_Group_GUID],
[AA_AuthorizedUser], [CQ_AuthorizedUser])
VALUES
('Error', @ChangeDate, @UPN, @LineURI, @Deprovision, @AccountEnabled, @IPPhone, @PolicyBaseline,
@CallParkPolicy, @CallerIDPolicy, @DialPlan, @LocationID,
@ECRP, @ECP, @VoiceRoutingPolicy, @VoiceApplicationsPolicy,
@CQGroupName, @CQGroupGUID, @AAUsers, @CQUsers)
"@
                
                $errorCmd.Parameters.AddWithValue('@ChangeDate', (Get-Date).ToString("yyyy-MM-dd HH:mm:ss")) | Out-Null
                $errorCmd.Parameters.AddWithValue('@UPN', (Get-SafeSqlValue $UPN -MaxLength 100)) | Out-Null
                $errorCmd.Parameters.AddWithValue('@LineURI', (Get-SafeSqlValue $LineURI -MaxLength 50)) | Out-Null
                $errorCmd.Parameters.AddWithValue('@Deprovision', (Get-SafeSqlValue $Deprovision -MaxLength 50)) | Out-Null
                $errorCmd.Parameters.AddWithValue('@AccountEnabled', (Get-SafeSqlValue $AccountEnabled -MaxLength 50)) | Out-Null
                $errorCmd.Parameters.AddWithValue('@IPPhone', (Get-SafeSqlValue $IPPhone -MaxLength 100)) | Out-Null
                $errorCmd.Parameters.AddWithValue('@PolicyBaseline', (Get-SafeSqlValue $PolicyBaseline -MaxLength 50)) | Out-Null
                $errorCmd.Parameters.AddWithValue('@CallParkPolicy', (Get-SafeSqlValue $CallParkPolicy -MaxLength 100)) | Out-Null
                $errorCmd.Parameters.AddWithValue('@CallerIDPolicy', (Get-SafeSqlValue $CallerIDPolicy -MaxLength 100)) | Out-Null
                $errorCmd.Parameters.AddWithValue('@DialPlan', (Get-SafeSqlValue $DialPlan -MaxLength 100)) | Out-Null
                $errorCmd.Parameters.AddWithValue('@LocationID', (Get-SafeSqlValue $LocationID -MaxLength 100)) | Out-Null
                $errorCmd.Parameters.AddWithValue('@ECRP', (Get-SafeSqlValue $ECRP -MaxLength 100)) | Out-Null
                $errorCmd.Parameters.AddWithValue('@ECP', (Get-SafeSqlValue $ECP -MaxLength 100)) | Out-Null
                $errorCmd.Parameters.AddWithValue('@VoiceRoutingPolicy', (Get-SafeSqlValue $VoiceRoutingPolicy -MaxLength 100)) | Out-Null
                $errorCmd.Parameters.AddWithValue('@VoiceApplicationsPolicy', (Get-SafeSqlValue $VoiceApplicationsPolicy -MaxLength 100)) | Out-Null
                $errorCmd.Parameters.AddWithValue('@CQGroupName', (Get-SafeSqlValue $CQGroupName -MaxLength 400)) | Out-Null
                $errorCmd.Parameters.AddWithValue('@CQGroupGUID', (Get-SafeSqlValue $CQGroupGUID -MaxLength 400)) | Out-Null
                $errorCmd.Parameters.AddWithValue('@AAUsers', (Get-SafeSqlValue $AAUsers -MaxLength 400)) | Out-Null
                $errorCmd.Parameters.AddWithValue('@CQUsers', (Get-SafeSqlValue $CQUsers -MaxLength 400)) | Out-Null
                
                $errorCmd.ExecuteNonQuery() | Out-Null
                Write-Output "Logged error for ${UPN} in Change_Log table"
            } catch {
                Write-Warning "Failed to log error for ${UPN} to Change_Log table: $($_.Exception.Message)"
            }
        }
    }

    Write-Output "Processing completed. Processed $($tempUsers.Count) users total."
    
    # Remove successfully processed records from tempusers table
    if ($successfullyProcessedUsers.Count -gt 0) {
        Write-Output "Removing $($successfullyProcessedUsers.Count) successfully processed records from tempusers table..."
        
        foreach ($processedUPN in $successfullyProcessedUsers) {
            try {
                $deleteCmd = $SQLConnection.CreateCommand()
                $deleteCmd.CommandText = "DELETE FROM $TempUsersTable WHERE UPN = @UPN"
                $deleteCmd.Parameters.AddWithValue('@UPN', $processedUPN) | Out-Null
                
                $rowsDeleted = $deleteCmd.ExecuteNonQuery()
                if ($rowsDeleted -gt 0) {
                    Write-Output "Successfully removed $processedUPN from tempusers table"
                } else {
                    Write-Warning "No rows deleted for $processedUPN - record may have already been removed"
                }
            } catch {
                Write-Warning "Failed to remove $processedUPN from tempusers table: $($_.Exception.Message)"
            }
        }
        
        Write-Output "Cleanup completed. Removed $($successfullyProcessedUsers.Count) records from tempusers table."
    } else {
        Write-Output "No records were successfully processed, so no records removed from tempusers table."
    }

    # Set script completion flag for successful processing
    $script:completedSuccessfully = $true

} catch {
    $errorMessage = $_.Exception.Message
    Write-Error "Critical error during processing: $errorMessage"
    $script:completedSuccessfully = $false
    throw
} finally {
    # Cleanup connections
    try {
        if ($SQLConnection.State -eq 'Open') {
            $SQLConnection.Close()
            Write-Output "SQL connection closed"
        }
    } catch {
        Write-Warning "Error closing SQL connection: $($_.Exception.Message)"
    }

    try {
        Disconnect-MicrosoftTeams -Confirm:$false
        Write-Output "Disconnected from Microsoft Teams"
    } catch {
        Write-Warning "Error disconnecting from Teams: $($_.Exception.Message)"
    }

    Write-Output "Runbook execution completed."
}

# Start follow-up runbooks (outside of try-catch-finally)
if ($script:completedSuccessfully) {
    Write-Output "â Script completed successfully. Starting follow-up runbooks..."
    
    # Azure Automation Account details
    $automationAccount = "VendorAutomationAccount"
    $resourceGroup = "Infrastructure"
    
    Write-Output "Using Automation Account: $automationAccount in Resource Group: $resourceGroup"
    
    # Start Get_A5_NonEnterpriseVoice_Users runbook first
    try {
        $runbook1Result = Start-AzAutomationRunbook -AutomationAccountName $automationAccount -Name "Get_A5_NonEnterpriseVoice_Users" -ResourceGroupName $resourceGroup -ErrorAction Stop
        Write-Output "â Successfully started Get_A5_NonEnterpriseVoice_Users runbook (Job ID: $($runbook1Result.JobId))"
    } catch {
        Write-Warning "â Failed to start Get_A5_NonEnterpriseVoice_Users runbook: $($_.Exception.Message)"
    }
    
    # Start MSOE_Teams_Phone_System_Users_Basic runbook second
    try {
        $runbook2Result = Start-AzAutomationRunbook -AutomationAccountName $automationAccount -Name "MSOE_Teams_Phone_System_Users_Basic" -ResourceGroupName $resourceGroup -ErrorAction Stop
        Write-Output "â Successfully started MSOE_Teams_Phone_System_Users_Basic runbook (Job ID: $($runbook2Result.JobId))"
    } catch {
        Write-Warning "â Failed to start MSOE_Teams_Phone_System_Users_Basic runbook: $($_.Exception.Message)"
    }
} else {
    Write-Output "â ï¸  Script did not complete successfully. Skipping follow-up runbooks."
}