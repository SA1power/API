# Script version: v3.4 - Streamlined without license detection
# Description: Provisions or deprovisions Teams shared devices, assigns policies, assigns phone numbers, adds to call queue groups, logs results
# Updates: Location name-to-GUID mapping, removed unnecessary license checks, optimized performance, triggers update runbook on completion

$ErrorActionPreference = "Stop"
$VerbosePreference = "Continue"
$WarningPreference = "Continue"

# === CONFIGURATION ===
$SqlServer = "msoevendor.database.windows.net"
$Database = "CEProjectData"
$TempTSDTable = "dbo.TempTSD"
$ChangeLogTable = "dbo.TempTSD_Change_Log"

Write-Output "========================================="
Write-Output "TEAMS SHARED DEVICE PROVISIONING RUNBOOK"
Write-Output "Version: 3.4 - Streamlined"
Write-Output "Start Time: $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')"
Write-Output "========================================="
Write-Output ""
Write-Output "ð CONFIGURATION -"
Write-Output "  - SQL Server - $SqlServer"
Write-Output "  - Database - $Database"
Write-Output "  - Processing Table - $TempTSDTable"
Write-Output "  - Log Table - $ChangeLogTable"
Write-Output ""
Write-Output "ð LOCATION MAPPINGS -"
Write-Output "  - MSOE - Public Safety â a5d8e31f-7ea0-4fb1-9867-66dd9c58d710"
Write-Output "  Note: Add more mappings in Get-MappedLocationID function as needed"
Write-Output ""

# === FUNCTIONS ===

# Location ID mapping for known locations
function Get-MappedLocationID {
    param([string]$LocationInput)
    
    # Map known location names to their GUIDs
    # To find Location IDs in your tenant, run:
    #   Get-CsOnlineLisLocation | Select-Object LocationId, Description, Location, CompanyName
    # Or for a specific location:
    #   Get-CsOnlineLisLocation | Where-Object {$_.Description -like "*Public Safety*"}
    
    $locationMap = @{
        "MSOE - Public Safety" = "a5d8e31f-7ea0-4fb1-9867-66dd9c58d710"
        # Add more mappings here as needed, for example:
        # "Main Campus" = "12345678-1234-1234-1234-123456789012"
        # "Building A" = "87654321-4321-4321-4321-210987654321"
    }
    
    # Check if input is a known location name
    if ($locationMap.ContainsKey($LocationInput)) {
        Write-Verbose "    - Mapped location '$LocationInput' to GUID: $($locationMap[$LocationInput])"
        return $locationMap[$LocationInput]
    }
    
    # Return as-is if not in mapping (might already be a GUID)
    return $LocationInput
}

function Get-SafeSqlValue {
    param($Value, [int]$MaxLength = 0)
    if ($null -eq $Value -or $Value -eq "") { return [DBNull]::Value }
    $stringValue = $Value.ToString()
    if ($MaxLength -gt 0 -and $stringValue.Length -gt $MaxLength) {
        $stringValue = $stringValue.Substring(0, $MaxLength)
    }
    return $stringValue
}

function Test-LocationID {
    param(
        [string]$LocationId
    )
    try {
        # Try to get the location to verify it exists
        # First try as a GUID/ID
        $location = Get-CsOnlineLisLocation -LocationId $LocationId -ErrorAction SilentlyContinue
        if ($location) {
            Write-Verbose "    - Location ID '$LocationId' verified"
            return $true
        }
        
        # If not found, try to list all locations and match by description
        # This is a fallback in case the location ID format is different
        $allLocations = Get-CsOnlineLisLocation -ErrorAction SilentlyContinue
        foreach ($loc in $allLocations) {
            if ($loc.LocationId -eq $LocationId -or $loc.Description -eq $LocationId) {
                Write-Verbose "    - Location found: $($loc.Description)"
                return $true
            }
        }
        
        return $false
    } catch {
        Write-Verbose "    - Location ID '$LocationId' not found"
        return $false
    }
}

function Add-DeviceToDistributionGroup {
    param(
        [Parameter(Mandatory=$true)] [string]$GroupId,
        [Parameter(Mandatory=$true)] [string]$DeviceUPN,
        [Parameter(Mandatory=$true)] [hashtable]$Headers,
        [string]$GroupName = "Unknown"
    )
    try {
        $deviceUri = "https://graph.microsoft.com/v1.0/users/$DeviceUPN"
        $deviceResp = Invoke-RestMethod -Uri $deviceUri -Headers $Headers -Method Get -ErrorAction Stop
        $deviceObjectId = $deviceResp.id

        $membersUri = "https://graph.microsoft.com/v1.0/groups/$GroupId/members"
        $membersResp = Invoke-RestMethod -Uri $membersUri -Headers $Headers -Method Get -ErrorAction SilentlyContinue

        $isMember = $false
        if ($membersResp.value) {
            foreach ($m in $membersResp.value) {
                if ($m.id -eq $deviceObjectId -or $m.userPrincipalName -eq $DeviceUPN) {
                    $isMember = $true
                    break
                }
            }
        }

        if ($isMember) {
            Write-Output "    - ð Already a member of $GroupName"
            return $true
        }

        $addUri = "https://graph.microsoft.com/v1.0/groups/$GroupId/members/`$ref"
        $body = @{ "@odata.id" = "https://graph.microsoft.com/v1.0/directoryObjects/$deviceObjectId" } | ConvertTo-Json -Compress
        Invoke-RestMethod -Uri $addUri -Headers $Headers -Method Post -Body $body -ErrorAction Stop
        Write-Output "    - â Added to $GroupName"
        return $true
    } catch {
        Write-Warning "    - â Failed to add to $GroupName - $($_.Exception.Message)"
        return $false
    }
}

# === AZURE + GRAPH CONNECTIONS ===
try {
    Write-Output "ð ESTABLISHING CONNECTIONS..."
    Connect-AzAccount -Identity | Out-Null
    Write-Output "  - â Azure connection established"
    
    $SQLToken = (Get-AzAccessToken -ResourceUrl "https://database.windows.net").Token
    Write-Output "  - â SQL token acquired"
    
    $GraphToken = (Get-AzAccessToken -ResourceUrl "https://graph.microsoft.com").Token
    Write-Output "  - â Graph token acquired"
    
    Connect-MicrosoftTeams -Identity | Out-Null
    Write-Output "  - â Teams connection established"
} catch {
    Write-Error "â FATAL: Failed to establish connections - $($_.Exception.Message)"
    throw
}

$GraphHeaders = @{
    "Authorization" = "Bearer $GraphToken"
    "Content-Type"  = "application/json"
}

# === OPEN SQL CONNECTION ===
try {
    $SqlConn = New-Object System.Data.SqlClient.SqlConnection
    $SqlConn.ConnectionString = "Server=$SqlServer;Database=$Database;Integrated Security=False;Encrypt=True;TrustServerCertificate=False;"
    $SqlConn.AccessToken = $SQLToken
    $SqlConn.Open()
    Write-Output "  - â SQL connection established"
} catch {
    Write-Error "â FATAL: Failed to connect to SQL - $($_.Exception.Message)"
    throw
}

# === FETCH DEVICES ===
Write-Output ""
Write-Output "ð FETCHING DEVICES FROM DATABASE..."
$readCmd = $SqlConn.CreateCommand()
$readCmd.CommandText = "SELECT * FROM $TempTSDTable"
$reader = $readCmd.ExecuteReader()

$devices = @()
while ($reader.Read()) {
    $devices += [PSCustomObject]@{
        UPN                = $reader["UPN"]
        LineURI            = $reader["Line_URI"]
        Deprovision        = $reader["Deprovision"]
        AccountEnabled     = $reader["Account_Enabled"]
        IPPhone            = $reader["IPPhone"]
        PolicyBaseline     = $reader["Policy_Baseline"]
        TeamsIPPhonePolicy = $reader["TeamsIPPhonePolicy"]
        CallParkPolicy     = $reader["Call Park Policy"]
        CallingPolicy      = $reader["Calling Policy"]
        CallerIDPolicy     = $reader["Caller ID Policy"]
        DialPlan           = $reader["Dial Plan"]
        LocationID         = $reader["Location ID"]
        ECRP               = $reader["Emergency Call Routing Policy"]
        ECP                = $reader["Emergency Calling Policy"]
        VoiceRoutingPolicy = $reader["Voice Routing Policy"]
        CQGroupName        = $reader["Call Queue Group Name"]
        CQGroupGUID        = $reader["Call_Queue_Group_GUID"]
    }
}
$reader.Close()

Write-Output "  - ð Found $($devices.Count) shared device(s) to process"

# === PROCESS EACH DEVICE ===
Write-Output ""
Write-Output "ð PROCESSING DEVICES..."
$successCount = 0
$warningCount = 0
$failureCount = 0

foreach ($device in $devices) {
    $UPN = $device.UPN
    $changeType = if ($device.Deprovision -eq "Y") { "Deprovision" } else { "Provision" }
    $deviceWarnings = @()
    
    Write-Output ""
    Write-Output "========================================="
    Write-Output "Device - $UPN"
    Write-Output "Action - $changeType"
    Write-Output "========================================="

    try {
        # Check if user exists
        Write-Output "  ð Validating user..."
        $exists = Get-CsOnlineUser -Identity $UPN -ErrorAction SilentlyContinue
        if (-not $exists) { 
            throw "User $UPN not found in Teams"
        }
        Write-Output "    - User exists in Teams"
        Write-Output "    - Current state - EV=$($exists.EnterpriseVoiceEnabled), LineURI=$($exists.LineURI)"
        
        # Check for Teams Shared Device license (improved detection)
        $hasSharedDeviceLicense = $false
        if ($exists.AssignedLicenses) {
            # Check the actual license SKUs from Graph API
            try {
                $userUri = "https://graph.microsoft.com/v1.0/users/$UPN`?`$select=assignedLicenses"
                $userLicenses = Invoke-RestMethod -Uri $userUri -Headers $GraphHeaders -Method Get -ErrorAction SilentlyContinue
                
                # Check for Teams Shared Device SKU: 420c7602-7f70-4895-9394-d3d679ea36fb
                $tsdSku = "420c7602-7f70-4895-9394-d3d679ea36fb"
                foreach ($license in $userLicenses.assignedLicenses) {
                    if ($license.skuId -eq $tsdSku) {
                        $hasSharedDeviceLicense = $true
                        Write-Output "    - â Teams Shared Device license confirmed"
                        break
                    }
                }
            } catch {
                Write-Verbose "    - Could not verify license via Graph API"
            }
        }
        
        if (-not $hasSharedDeviceLicense) {
            $deviceWarnings += "No Teams Shared Device license detected"
            Write-Warning "    - â ï¸ No Teams Shared Device license detected"
        }

        if ($changeType -eq "Deprovision") {
            Write-Output "  ð§ DEPROVISIONING..."
            
            if (-not $exists.LineURI -and -not $exists.EnterpriseVoiceEnabled) {
                Write-Output "    - Already deprovisioned, skipping"
            } else {
                Remove-CsPhoneNumberAssignment -Identity $UPN -RemoveAll -ErrorAction Stop
                Write-Output "    - â Phone number removed"
            }
            
        } else {
            Write-Output "  ð§ PROVISIONING..."
            
            # Check if already provisioned
            $currentNumber = $exists.LineURI -replace "^tel:", "" -replace ";.*", ""
            $requestedNumber = if ($device.LineURI -ne [DBNull]::Value) { 
                $device.LineURI.ToString().Replace("tel:", "") 
            } else { 
                "" 
            }
            
            if ($requestedNumber -and -not $requestedNumber.StartsWith("+")) { 
                $requestedNumber = "+$requestedNumber" 
            }
            
            $needPhoneAssignment = $true
            if ($currentNumber -eq $requestedNumber -and $exists.EnterpriseVoiceEnabled) {
                Write-Output "    - Phone already assigned - $currentNumber"
                $needPhoneAssignment = $false
            }
            
            # Assign Phone Number if needed
            if ($needPhoneAssignment -and $device.LineURI -and $device.LineURI -ne [DBNull]::Value) {
                $tel = $requestedNumber
                Write-Output "    - Assigning phone - $tel"
                
                # Validate and map Location ID if provided
                $useLocationId = $false
                $mappedLocationId = $null
                if ($device.LocationID -ne [DBNull]::Value -and $device.LocationID) {
                    # Map location name to GUID if needed
                    $mappedLocationId = Get-MappedLocationID -LocationInput $device.LocationID
                    
                    # Test if the mapped location ID is valid
                    if (Test-LocationID -LocationId $mappedLocationId) {
                        $useLocationId = $true
                        Write-Output "      Location ID verified - $mappedLocationId"
                        if ($mappedLocationId -ne $device.LocationID) {
                            Write-Output "        (Mapped from '$($device.LocationID)')"
                        }
                    } else {
                        $deviceWarnings += "Location ID '$($device.LocationID)' not found"
                        Write-Warning "      â ï¸ Location ID not valid, will assign without location"
                        Write-Warning "        Original: $($device.LocationID)"
                        if ($mappedLocationId -ne $device.LocationID) {
                            Write-Warning "        Tried mapping to: $mappedLocationId"
                        }
                        
                        # In verbose mode, show available locations for troubleshooting
                        if ($VerbosePreference -eq "Continue") {
                            try {
                                $availableLocations = Get-CsOnlineLisLocation -ErrorAction SilentlyContinue | Select-Object -First 5
                                if ($availableLocations) {
                                    Write-Verbose "        Available locations (first 5):"
                                    foreach ($loc in $availableLocations) {
                                        Write-Verbose "          - $($loc.LocationId): $($loc.Description)"
                                    }
                                }
                            } catch {
                                Write-Verbose "        Could not retrieve available locations"
                            }
                        }
                    }
                }
                
                # Determine phone number type
                $phoneNumberType = "DirectRouting"
                if ($device.PolicyBaseline -match "CallingPlan") {
                    $phoneNumberType = "CallingPlan"
                } elseif ($device.PolicyBaseline -match "OperatorConnect") {
                    $phoneNumberType = "OperatorConnect"
                }
                
                try {
                    # Apply Voice Routing Policy first if using Direct Routing
                    if ($phoneNumberType -eq "DirectRouting" -and $device.VoiceRoutingPolicy -ne [DBNull]::Value) {
                        Grant-CsOnlineVoiceRoutingPolicy -Identity $UPN -PolicyName $device.VoiceRoutingPolicy -ErrorAction Stop
                        Write-Output "      Voice routing policy applied - $($device.VoiceRoutingPolicy)"
                    }
                    
                    # Assign phone number
                    if ($useLocationId) {
                        Set-CsPhoneNumberAssignment -Identity $UPN -PhoneNumber $tel -PhoneNumberType $phoneNumberType -LocationId $mappedLocationId -ErrorAction Stop
                    } else {
                        Set-CsPhoneNumberAssignment -Identity $UPN -PhoneNumber $tel -PhoneNumberType $phoneNumberType -ErrorAction Stop
                    }
                    Write-Output "      â Phone assigned successfully"
                    
                    # Quick verification (reduced wait time)
                    Start-Sleep -Seconds 2
                    $verify = Get-CsOnlineUser -Identity $UPN
                    if ($verify.EnterpriseVoiceEnabled -and $verify.LineURI) {
                        Write-Output "      â Verified - EV enabled, LineURI=$($verify.LineURI)"
                    }
                } catch {
                    Write-Error "      â Phone assignment failed - $($_.Exception.Message)"
                    throw
                }
            } elseif (-not $device.LineURI -or $device.LineURI -eq [DBNull]::Value) {
                $deviceWarnings += "No phone number specified"
                Write-Warning "    - â ï¸ No phone number to assign"
            }

            # Assign Policies
            Write-Output "  ð APPLYING POLICIES..."
            $policiesApplied = 0
            $policiesSkipped = 0
            $policiesFailed = @()
            
            # Teams IP Phone Policy
            if ($device.TeamsIPPhonePolicy -ne [DBNull]::Value -and $device.TeamsIPPhonePolicy) {
                try {
                    Grant-CsTeamsIPPhonePolicy -Identity $UPN -PolicyName $device.TeamsIPPhonePolicy -ErrorAction Stop
                    Write-Output "    - â Teams IP Phone - $($device.TeamsIPPhonePolicy)"
                    $policiesApplied++
                } catch {
                    $policiesFailed += "TeamsIPPhone"
                    $deviceWarnings += "Failed to apply Teams IP Phone Policy"
                    Write-Warning "    - â Teams IP Phone failed - $($_.Exception.Message)"
                }
            } else { $policiesSkipped++ }
            
            # Call Park Policy
            if ($device.CallParkPolicy -ne [DBNull]::Value -and $device.CallParkPolicy) {
                try {
                    Grant-CsTeamsCallParkPolicy -Identity $UPN -PolicyName $device.CallParkPolicy -ErrorAction Stop
                    Write-Output "    - â Call Park - $($device.CallParkPolicy)"
                    $policiesApplied++
                } catch {
                    $policiesFailed += "CallPark"
                    $deviceWarnings += "Failed to apply Call Park Policy"
                    Write-Warning "    - â Call Park failed - $($_.Exception.Message)"
                }
            } else { $policiesSkipped++ }
            
            # Calling Policy
            if ($device.CallingPolicy -ne [DBNull]::Value -and $device.CallingPolicy) {
                try {
                    Grant-CsTeamsCallingPolicy -Identity $UPN -PolicyName $device.CallingPolicy -ErrorAction Stop
                    Write-Output "    - â Calling - $($device.CallingPolicy)"
                    $policiesApplied++
                } catch {
                    $policiesFailed += "Calling"
                    $deviceWarnings += "Failed to apply Calling Policy"
                    Write-Warning "    - â Calling failed - $($_.Exception.Message)"
                }
            } else { $policiesSkipped++ }
            
            # Caller ID Policy
            if ($device.CallerIDPolicy -ne [DBNull]::Value -and $device.CallerIDPolicy) {
                try {
                    # Try new cmdlet first, fall back to old if needed
                    try {
                        Grant-CsTeamsCallingLineIdentity -Identity $UPN -PolicyName $device.CallerIDPolicy -ErrorAction Stop
                    } catch {
                        Grant-CsCallingLineIdentity -Identity $UPN -PolicyName $device.CallerIDPolicy -ErrorAction Stop
                    }
                    Write-Output "    - â Caller ID - $($device.CallerIDPolicy)"
                    $policiesApplied++
                } catch {
                    $policiesFailed += "CallerID"
                    $deviceWarnings += "Failed to apply Caller ID Policy"
                    Write-Warning "    - â Caller ID failed - $($_.Exception.Message)"
                }
            } else { $policiesSkipped++ }
            
            # Dial Plan
            if ($device.DialPlan -ne [DBNull]::Value -and $device.DialPlan) {
                try {
                    Grant-CsTenantDialPlan -Identity $UPN -PolicyName $device.DialPlan -ErrorAction Stop
                    Write-Output "    - â Dial Plan - $($device.DialPlan)"
                    $policiesApplied++
                } catch {
                    $policiesFailed += "DialPlan"
                    $deviceWarnings += "Failed to apply Dial Plan"
                    Write-Warning "    - â Dial Plan failed - $($_.Exception.Message)"
                }
            } else { $policiesSkipped++ }
            
            # Emergency Call Routing Policy
            if ($device.ECRP -ne [DBNull]::Value -and $device.ECRP) {
                try {
                    Grant-CsTeamsEmergencyCallRoutingPolicy -Identity $UPN -PolicyName $device.ECRP -ErrorAction Stop
                    Write-Output "    - â Emergency Routing - $($device.ECRP)"
                    $policiesApplied++
                } catch {
                    $policiesFailed += "ECRP"
                    $deviceWarnings += "Failed to apply Emergency Call Routing Policy"
                    Write-Warning "    - â Emergency Routing failed - $($_.Exception.Message)"
                }
            } else { $policiesSkipped++ }
            
            # Emergency Calling Policy
            if ($device.ECP -ne [DBNull]::Value -and $device.ECP) {
                try {
                    Grant-CsTeamsEmergencyCallingPolicy -Identity $UPN -PolicyName $device.ECP -ErrorAction Stop
                    Write-Output "    - â Emergency Calling - $($device.ECP)"
                    $policiesApplied++
                } catch {
                    $policiesFailed += "ECP"
                    $deviceWarnings += "Failed to apply Emergency Calling Policy"
                    Write-Warning "    - â Emergency Calling failed - $($_.Exception.Message)"
                }
            } else { $policiesSkipped++ }
            
            # Voice Routing Policy (if not already applied)
            if ($device.VoiceRoutingPolicy -ne [DBNull]::Value -and $device.VoiceRoutingPolicy) {
                $currentVRP = (Get-CsOnlineUser -Identity $UPN).OnlineVoiceRoutingPolicy
                if ($currentVRP -ne $device.VoiceRoutingPolicy) {
                    try {
                        Grant-CsOnlineVoiceRoutingPolicy -Identity $UPN -PolicyName $device.VoiceRoutingPolicy -ErrorAction Stop
                        Write-Output "    - â Voice Routing - $($device.VoiceRoutingPolicy)"
                        $policiesApplied++
                    } catch {
                        $policiesFailed += "VoiceRouting"
                        $deviceWarnings += "Failed to apply Voice Routing Policy"
                        Write-Warning "    - â Voice Routing failed: $($_.Exception.Message)"
                    }
                } else {
                    Write-Output "    - â¹ï¸ Voice Routing already set"
                    $policiesApplied++
                }
            } else { $policiesSkipped++ }
            
            Write-Output "    Summary - Applied=$policiesApplied, Skipped=$policiesSkipped, Failed=$($policiesFailed.Count)"
            
            if ($policiesFailed.Count -gt 0) {
                $deviceWarnings += "$($policiesFailed.Count) policies failed"
            }

            # Call Queue Group Assignment
            if ($device.CQGroupName -and $device.CQGroupGUID -and $device.CQGroupGUID -ne [DBNull]::Value) {
                Write-Output "  ð¥ CALL QUEUE GROUPS..."
                $names = $device.CQGroupName -split ';'
                $guids = $device.CQGroupGUID -split ';'

                for ($i = 0; $i -lt $names.Count; $i++) {
                    $groupName = $names[$i].Trim()
                    $groupId = if ($i -lt $guids.Count) { $guids[$i].Trim() } else { "" }

                    if ($groupId) {
                        $success = Add-DeviceToDistributionGroup -GroupId $groupId -DeviceUPN $UPN -Headers $GraphHeaders -GroupName $groupName
                        if (-not $success) {
                            $deviceWarnings += "Failed to add to group - $groupName"
                        }
                    }
                }
            }
            
            # Final verification
            Write-Output "  â FINAL STATUS..."
            Start-Sleep -Seconds 2
            $finalCheck = Get-CsOnlineUser -Identity $UPN
            Write-Output "    - Enterprise Voice - $($finalCheck.EnterpriseVoiceEnabled)"
            Write-Output "    - Line URI - $($finalCheck.LineURI)"
            Write-Output "    - Voice Routing - $($finalCheck.OnlineVoiceRoutingPolicy)"
            
            if ($finalCheck.EnterpriseVoiceEnabled -and $finalCheck.LineURI) {
                Write-Output "    - â Device fully provisioned"
            } else {
                $deviceWarnings += "Provisioning incomplete"
                Write-Warning "    - â ï¸ Device partially provisioned"
            }
        }

        # Log to database
        $logCmd = $SqlConn.CreateCommand()
        $logCmd.CommandText = @"
INSERT INTO $ChangeLogTable
(ChangeType, ChangeDate, UPN, Line_URI, Deprovision, Account_Enabled, IPPhone, Policy_Baseline,
TeamsIPPhonePolicy, [Call Park Policy], [Calling Policy], [Caller ID Policy], [Dial Plan], [Location ID],
[Emergency Call Routing Policy], [Emergency Calling Policy], [Voice Routing Policy], [Call Queue Group Name], [Call_Queue_Group_GUID])
VALUES
(@ChangeType, @ChangeDate, @UPN, @LineURI, @Deprovision, @AccountEnabled, @IPPhone, @PolicyBaseline,
@TeamsIPPhonePolicy, @CallParkPolicy, @CallingPolicy, @CallerIDPolicy, @DialPlan, @LocationID,
@ECRP, @ECP, @VoiceRoutingPolicy, @CQGroupName, @CQGroupGUID)
"@
        
        $logCmd.Parameters.AddWithValue("@ChangeType", $changeType) | Out-Null
        $logCmd.Parameters.AddWithValue("@ChangeDate", (Get-Date).ToString("yyyy-MM-dd HH:mm:ss")) | Out-Null
        $logCmd.Parameters.AddWithValue("@UPN", (Get-SafeSqlValue $device.UPN)) | Out-Null
        $logCmd.Parameters.AddWithValue("@LineURI", (Get-SafeSqlValue $device.LineURI)) | Out-Null
        $logCmd.Parameters.AddWithValue("@Deprovision", (Get-SafeSqlValue $device.Deprovision)) | Out-Null
        $logCmd.Parameters.AddWithValue("@AccountEnabled", (Get-SafeSqlValue $device.AccountEnabled)) | Out-Null
        $logCmd.Parameters.AddWithValue("@IPPhone", (Get-SafeSqlValue $device.IPPhone)) | Out-Null
        $logCmd.Parameters.AddWithValue("@PolicyBaseline", (Get-SafeSqlValue $device.PolicyBaseline)) | Out-Null
        $logCmd.Parameters.AddWithValue("@TeamsIPPhonePolicy", (Get-SafeSqlValue $device.TeamsIPPhonePolicy)) | Out-Null
        $logCmd.Parameters.AddWithValue("@CallParkPolicy", (Get-SafeSqlValue $device.CallParkPolicy)) | Out-Null
        $logCmd.Parameters.AddWithValue("@CallingPolicy", (Get-SafeSqlValue $device.CallingPolicy)) | Out-Null
        $logCmd.Parameters.AddWithValue("@CallerIDPolicy", (Get-SafeSqlValue $device.CallerIDPolicy)) | Out-Null
        $logCmd.Parameters.AddWithValue("@DialPlan", (Get-SafeSqlValue $device.DialPlan)) | Out-Null
        $logCmd.Parameters.AddWithValue("@LocationID", (Get-SafeSqlValue $device.LocationID)) | Out-Null
        $logCmd.Parameters.AddWithValue("@ECRP", (Get-SafeSqlValue $device.ECRP)) | Out-Null
        $logCmd.Parameters.AddWithValue("@ECP", (Get-SafeSqlValue $device.ECP)) | Out-Null
        $logCmd.Parameters.AddWithValue("@VoiceRoutingPolicy", (Get-SafeSqlValue $device.VoiceRoutingPolicy)) | Out-Null
        $logCmd.Parameters.AddWithValue("@CQGroupName", (Get-SafeSqlValue $device.CQGroupName)) | Out-Null
        $logCmd.Parameters.AddWithValue("@CQGroupGUID", (Get-SafeSqlValue $device.CQGroupGUID)) | Out-Null
        
        $logCmd.ExecuteNonQuery() | Out-Null

        # Delete from TempTSD
        $delCmd = $SqlConn.CreateCommand()
        $delCmd.CommandText = "DELETE FROM $TempTSDTable WHERE UPN = @UPN"
        $delCmd.Parameters.AddWithValue("@UPN", $UPN) | Out-Null
        $delCmd.ExecuteNonQuery() | Out-Null
        
        # Update counters
        if ($deviceWarnings.Count -gt 0) {
            $warningCount++
            Write-Output ""
            Write-Output "  â ï¸ COMPLETED WITH WARNINGS -"
            foreach ($warning in $deviceWarnings) {
                Write-Output "    - $warning"
            }
        } else {
            $successCount++
            Write-Output ""
            Write-Output "  â DEVICE SUCCESSFULLY PROCESSED"
        }
    }
    catch {
        $failureCount++
        Write-Error "â FAILED - $($_.Exception.Message)"
        
        # Log error
        try {
            $errorLogCmd = $SqlConn.CreateCommand()
            $errorLogCmd.CommandText = @"
INSERT INTO $ChangeLogTable
(ChangeType, ChangeDate, UPN, Line_URI, Deprovision, [Call Queue Group Name])
VALUES
(@ChangeType, @ChangeDate, @UPN, @LineURI, @Deprovision, @ErrorMsg)
"@
            $errorLogCmd.Parameters.AddWithValue("@ChangeType", "ERROR") | Out-Null
            $errorLogCmd.Parameters.AddWithValue("@ChangeDate", (Get-Date).ToString("yyyy-MM-dd HH:mm:ss")) | Out-Null
            $errorLogCmd.Parameters.AddWithValue("@UPN", $UPN) | Out-Null
            $errorLogCmd.Parameters.AddWithValue("@LineURI", (Get-SafeSqlValue $device.LineURI)) | Out-Null
            $errorLogCmd.Parameters.AddWithValue("@Deprovision", (Get-SafeSqlValue $device.Deprovision)) | Out-Null
            $errorLogCmd.Parameters.AddWithValue("@ErrorMsg", $_.Exception.Message.Substring(0, [Math]::Min(255, $_.Exception.Message.Length))) | Out-Null
            $errorLogCmd.ExecuteNonQuery() | Out-Null
        } catch {
            Write-Warning "Could not log error - $_"
        }
        
        continue
    }
}

# === TRIGGER GET_TSD_NONENERPRISEVOICE_USERS RUNBOOK ===
Write-Output ""
Write-Output "ð TRIGGERING UPDATE RUNBOOK..."
try {
    # Get automation account context from the current runbook execution
    $automationAccountName = "VendorAutomationAccount"
    
    # Try different methods to get the resource group
    $resourceGroupName = $null
    
    # Method 1: Try environment variable
    if ($env:AUTOMATION_RESOURCE_GROUP) {
        $resourceGroupName = $env:AUTOMATION_RESOURCE_GROUP
        Write-Verbose "  - Resource group from env: $resourceGroupName"
    }
    
    # Method 2: Try to get from current automation account
    if (-not $resourceGroupName) {
        try {
            # Get the automation account to find its resource group
            $automationAccounts = Get-AzAutomationAccount -ErrorAction SilentlyContinue
            foreach ($account in $automationAccounts) {
                if ($account.AutomationAccountName -eq $automationAccountName) {
                    $resourceGroupName = $account.ResourceGroupName
                    Write-Verbose "  - Resource group from account: $resourceGroupName"
                    break
                }
            }
        } catch {
            Write-Verbose "  - Could not enumerate automation accounts"
        }
    }
    
    # Method 3: Hardcoded fallback
    if (-not $resourceGroupName) {
        $resourceGroupName = "VendorOperations"
        Write-Verbose "  - Using default resource group: $resourceGroupName"
    }
    
    Write-Output "  - Starting Get_TSD_NonEnterpriseVoice_Users runbook..."
    Write-Output "    Automation Account: $automationAccountName"
    Write-Output "    Resource Group: $resourceGroupName"
    
    # Start the runbook
    # Note: The runbook name must match EXACTLY including underscores
    $runbookName = "Get_TSD_NonEnterpriseVoice_Users"
    
    Write-Output "  - Attempting to start runbook: $runbookName"
    
    $job = Start-AzAutomationRunbook `
        -AutomationAccountName $automationAccountName `
        -Name $runbookName `
        -ResourceGroupName $resourceGroupName `
        -ErrorAction Stop
    
    if ($job) {
        Write-Output "  - â Update runbook triggered successfully"
        Write-Output "    Job ID: $($job.JobId)"
    } else {
        Write-Warning "  - â ï¸ Runbook started but no job ID returned"
    }
} catch {
    Write-Warning "  - â ï¸ Could not trigger update runbook - $($_.Exception.Message)"
    
    # Provide more detailed error information
    if ($_.Exception.InnerException) {
        Write-Warning "    Inner exception: $($_.Exception.InnerException.Message)"
    }
    
    # Try alternative method - using Az cmdlet with subscription context
    try {
        Write-Output "  - Trying alternative method..."
        
        # Get current subscription context
        $context = Get-AzContext
        if ($context) {
            Write-Output "    Subscription: $($context.Subscription.Name)"
            
            # Try with explicit subscription and resource group
            $job = Start-AzAutomationRunbook `
                -ResourceGroupName "VendorOperations" `
                -AutomationAccountName "VendorAutomationAccount" `
                -Name "Get_TSD_NonEnterpriseVoice_Users" `
                -ErrorAction Stop
            
            if ($job) {
                Write-Output "  - â Update runbook triggered via alternative method"
                Write-Output "    Job ID: $($job.JobId)"
            }
        }
    } catch {
        Write-Warning "    Alternative method also failed: $($_.Exception.Message)"
        Write-Warning ""
        Write-Warning "    â ï¸ MANUAL ACTION REQUIRED:"
        Write-Warning "    Please run 'Get_TSD_NonEnterpriseVoice_Users' manually in Azure Portal"
        Write-Warning "    Path: Automation Accounts â VendorAutomationAccount â Runbooks"
    }
}

# === CLEANUP ===
Write-Output ""
Write-Output "========================================="
Write-Output "FINALIZING..."
Write-Output "========================================="

if ($SqlConn.State -eq "Open") { 
    $SqlConn.Close()
    Write-Output "  - SQL connection closed"
}

Disconnect-MicrosoftTeams -Confirm:$false
Write-Output "  - Teams connection closed"

Write-Output ""
Write-Output "========================================="
Write-Output "SUMMARY"
Write-Output "========================================="
Write-Output "  Total Processed - $($devices.Count)"
Write-Output "  - â Successful - $successCount"
Write-Output "  - â ï¸ With Warnings - $warningCount"  
Write-Output "  - â Failed - $failureCount"
Write-Output "  End Time - $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')"
Write-Output "========================================="

if ($warningCount -gt 0 -or $failureCount -gt 0) {
    Write-Output ""
    Write-Output "ð COMMON ISSUES -"
    Write-Output "  - Location ID not found - Add mapping in Get-MappedLocationID function"
    Write-Output "  - Policy not found - Verify policy name matches exactly"
    Write-Output "  - Phone assignment failed - Check Direct Routing configuration"
    Write-Output ""
    Write-Output "ð TO ADD NEW LOCATION MAPPINGS -"
    Write-Output "  1. Run: Get-CsOnlineLisLocation | Select LocationId, Description"
    Write-Output "  2. Add to Get-MappedLocationID function in this runbook"
}

Write-Output ""
Write-Output "â Teams Shared Device provisioning runbook complete."