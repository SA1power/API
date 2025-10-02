# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Repository Overview

This repository contains Azure infrastructure code for MSOE's Teams Phone System provisioning and management. It consists of two main Azure Function Apps and a collection of PowerShell runbooks that automate Teams phone system operations.

## Architecture

### Azure Function Apps

The repository contains two separate .NET 8.0 isolated Azure Function Apps:

1. **AvailablePhoneNumberAPI** (`/AvailablePhoneNumberAPI/`)
   - HTTP-triggered function: `GetAvailablePhoneNumbers`
   - Anonymous authentication
   - Deployed to Azure Function App: `AvailablePhoneNumberAPI`

2. **ProvisionTeamsPhoneSystemUsers** (`/ProvisionTeamsPhoneSystemUsers/`)
   - HTTP-triggered functions:
     - `TriggerRunbook` - Triggers Azure Automation runbooks for user provisioning
     - `TriggerSharedDeviceRunbook` - Triggers runbooks for shared device provisioning
   - Anonymous authentication
   - Contains 14 PowerShell runbooks in `/runbooks/` subdirectory

### Azure Automation Runbooks

Located in `ProvisionTeamsPhoneSystemUsers/runbooks/`, these PowerShell scripts are deployed to Azure Automation Account `VendorAutomationAccount` in the `Infrastructure` resource group:

**Provisioning Runbooks:**
- `Provision_Teams_Phone_System_Users.ps1` - Main user provisioning logic
- `Provision_Teams_Shared_Devices.ps1` - Shared device provisioning
- `MSOE_Teams_Phone_System_Users_Basic.ps1` - Basic user management
- `MSOE_Teams_Phone_System_Shared_Device_Accounts.ps1` - Shared device account management

**Configuration Runbooks:**
- `MSOE_Teams_Phone_System_Auto_Attendants.ps1` - Auto attendant configuration
- `MSOE_Teams_Phone_System_Call_Queue_Groups.ps1` - Call queue group management
- `MSOE_Teams_Phone_System_Call_Queues.ps1` - Call queue configuration

**Change Tracking Runbooks:**
- `TeamsPhoneSystemUserChanges.ps1` - Track user changes
- `TeamsPhoneSystemSharedDeviceAccountChanges.ps1` - Track shared device changes
- `TeamsPhoneSystemAutoAttendantChanges.ps1` - Track auto attendant changes
- `TeamsPhoneSystemCallQueueGroupChanges.ps1` - Track call queue group changes
- `TeamsPhoneSystemCallQueuesChanges.ps1` - Track call queue changes

**Utility Runbooks:**
- `Get_A5_NonEnterpriseVoice_Users.ps1` - Query A5 licensed users without Enterprise Voice
- `Get_TSD_NonEnterpriseVoice_Users.ps1` - Query TSD users without Enterprise Voice

## Development Commands

### Building Azure Functions

**Build a function app:**
```bash
cd AvailablePhoneNumberAPI
dotnet build --configuration Release

# Or for ProvisionTeamsPhoneSystemUsers
cd ProvisionTeamsPhoneSystemUsers
dotnet build --configuration Release
```

**Run locally:**
```bash
cd AvailablePhoneNumberAPI
func start

# Or for ProvisionTeamsPhoneSystemUsers
cd ProvisionTeamsPhoneSystemUsers
func start
```

### Working with Azure Automation Runbooks

**Export runbooks from Azure:**
```powershell
$token = (az account get-access-token --query accessToken -o tsv)
$headers = @{Authorization="Bearer $token"}
$runbookName = "Provision_Teams_Phone_System_Users"
$url = "https://management.azure.com/subscriptions/{subscriptionId}/resourceGroups/Infrastructure/providers/Microsoft.Automation/automationAccounts/VendorAutomationAccount/runbooks/$runbookName/content?api-version=2023-11-01"
Invoke-RestMethod -Uri $url -Headers $headers -Method Get | Out-File "$runbookName.ps1" -Encoding UTF8
```

**List all runbooks in Azure:**
```bash
az rest --method get --url "https://management.azure.com/subscriptions/{subscriptionId}/resourceGroups/Infrastructure/providers/Microsoft.Automation/automationAccounts/VendorAutomationAccount/runbooks?api-version=2023-11-01" --query "value[].name" -o table
```

## Deployment

### Automated Deployment via GitHub Actions

Both Function Apps deploy automatically on push to `main` branch:

- Workflow: `.github/workflows/main_availablephonenumberapi.yml`
- Builds with .NET 8.0
- Deploys to Azure using OIDC authentication
- Target: Azure Function App `AvailablePhoneNumberAPI`

**Important:** The workflows expect the function app code to be in the repository root for deployment. The `AZURE_FUNCTIONAPP_PACKAGE_PATH` is set to `'.'`.

### Azure Resources

- **Subscription:** Microsoft Azure Enterprise (`fc7ad0bc-429f-488b-9488-3ed508182348`)
- **Resource Group:** Infrastructure
- **Automation Account:** VendorAutomationAccount
- **Function Apps:**
  - AvailablePhoneNumberAPI (Production slot)
  - ProvisionTeamsPhoneSystemUsers (Production slot)

## Key Integration Points

### Azure Automation Integration

The `TriggerRunbook` and `TriggerSharedDeviceRunbook` functions are HTTP triggers that initiate Azure Automation runbooks. These runbooks:
- Use Microsoft Graph API to manage Teams configurations
- Add users to distribution groups
- Configure phone numbers, call queues, and auto attendants
- Track changes for auditing purposes

### Microsoft Graph API

The provisioning runbooks heavily use Microsoft Graph API for:
- User management (`/v1.0/users`)
- Group membership (`/v1.0/groups/{id}/members`)
- Teams configuration endpoints

## Repository Structure Notes

- Each Function App has its own `.csproj`, `host.json`, and `local.settings.json`
- GitHub Actions workflows are duplicated in each Function App directory
- Runbooks are stored with the `ProvisionTeamsPhoneSystemUsers` app but deployed separately to Azure Automation
- The root contains a static `index.html` (legacy, purpose unclear)
