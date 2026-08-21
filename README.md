# MSTools

**Version:** 1.0.3  
**Author:** Sozezzo Astra  

---

## 📘 Overview

**MSTools** is a **PowerShell library** designed as an **extension to [dbatools](https://dbatools.io/)**.  
It enhances SQL Server administration, monitoring, and automation by adding functions for:

- Advanced SQL Server management  
- Job, credential, and object comparison  
- Server communication and automation  
- File and folder operations  
- Script deployment and replication  
- Backup and clone utilities  
- Logging and recovery support  

Although initially created to complement **DBA operations**, MSTools also includes a variety of utilities that go **beyond SQL Server**, making it a general-purpose PowerShell toolkit.

---

## ⚙️ Installation

### 1. Requirements

- **PowerShell 5.1 or later**  
- **dbatools** module installed  
- Administrator privileges (recommended)

### 2. How to Install

1. Copy the latest distribution folder from:
   
   ```
   \\dist\\MSTools\\
   ```
   
   The latest version folder contains the most up-to-date module build (e.g., `MSTools_1.0.2`).

2. Copy or extract this folder into one of the PowerShell module paths, for example:
   
   ```
   C:\Program Files\WindowsPowerShell\Modules\
   ```

3. Import the module manually or automatically:
   
   ```powershell
   Import-Module MSTools -Force
   ```

To verify installation:

```powershell
Get-Command -Module MSTools
```

---

## 🧰 Security & Execution Policy

### Allow PowerShell Scripts to Run

If PowerShell blocks the script execution, you need to relax the execution policy:

```powershell
Set-ExecutionPolicy RemoteSigned -Scope LocalMachine -Force
```

This allows all locally created scripts to run while keeping remote scripts restricted unless signed.

---

## 🔐 Certificates and Encryption

If the environment requires digital signatures and you want to **bypass** the signature requirement (e.g., internal or test systems):

1. Open PowerShell **as Administrator**.  
2. Run:
   
   ```powershell
   Set-ExecutionPolicy Unrestricted -Scope Process
   ```
   
   This temporarily allows unsigned scripts to run for the current session only.

Alternatively, you can **sign the module** later with your own certificate if your organization enforces code signing.

---

## 🏗️ Rebuilding the Module

The module can be rebuilt easily using the batch file provided in the `\build` folder.

1. Navigate to the build folder:
   
   ```
   cd .\build
   ```
2. Run:
   
   ```
   rebuild.bat
   ```

The script will automatically:

- Clean the old build output  
- Rebuild the PowerShell module structure  
- Copy the output into `\dist\MSTools\`  
- Increment the version (if defined in the build script)

---

## 📦 Updating the Module

To update the module on a server:

1. Replace the old folder under:
   
   ```
   C:\Program Files\WindowsPowerShell\Modules\MSTools\
   ```
   
   with the new version from `\dist\MSTools\`.

2. Restart PowerShell and import again:
   
   ```powershell
   Import-Module MSTools -Force
   ```

---

## 🧩 Included Functions

Below is the full list of functions currently available in MSTools **v1.0.2**:

| Category                    | Functions                                                                                                                                                                                                                                                                                               |
| --------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Backup & Maintenance**    | `Backup-MSDatabaseExt`, `Invoke-MSExecuteBatchWithRetry`, `Compress-MSFolder`                                                                                                                                                                                                                           |
| **DBA Comparison Tools**    | `Compare-MSAgentJob`, `Compare-MSCredential`, `Compare-MSDbObject`                                                                                                                                                                                                                                   |
| **Clone / Copy Operations** | `Invoke-MSCloneConstraint`, `Invoke-MSCloneData`, `Invoke-MSCloneData2`, `Invoke-MSCloneDatabaseStructure`, `Invoke-MSCloneFunctions`, `Invoke-MSCloneIndexes`, `Invoke-MSClonePkFk`, `Invoke-MSClonePrimaryKey`, `Invoke-MSCloneProgrammables`, `Invoke-MSCloneUser`, `Invoke-MSCloneViews` |
| **Export Utilities**        | `Export-MSAgentAlert`, `Export-MSDatabaseMail`, `Export-MSDbCompareReport`, `Export-MSJobCategoryScripts`, `Export-MSJobOperatorScripts`, `Export-MSJobScripts`, `Export-MSJobScriptsCluster`                                                                                                    |
| **SQL Server Management**   | `Set-MSDbCollation`, `Invoke-MSDbCollationRebuild`, `Set-MSDbServiceBroker`, `Set-MSDbSnapshotIsolation`, `Set-MSDbTrustworthy`, `Update-MSJobCluster`                                                                                                                                            |
| **Server Discovery**        | `Get-MSPrimaryServerName`, `Get-MSSecondaryServerName`                                                                                                                                                                                                                                                |
| **Automation & Tools**      | `Copy-MSDbProcedure`, `New-MSTargetDatabase`, `Test-MSFolderOrStop`, `Repair-MSModule`, `Import-MSTools`, `Write-MSLog`, `Get-MSLastFileModifiedDate`                                                                                                                                                          |

---

## 🧾 Logging

All major functions use the internal logger:

```powershell
Write-MSLog -Message "Backup completed successfully." -Level Info
```

Logs are timestamped and may include optional levels:

- `Info`
- `Warning`
- `Error`
- `Debug`

---

## 📄 Changelog

See [`changelog.md`](changelog.md) for version history and feature details.

---

## 🧠 Author & License

**Author:** Sozezzo Astra  
**Version:** 1.0.3  
**License:** MIT  

This project was created to simplify and unify PowerShell automation for database administrators and system engineers — bridging **SQL Server management**, **system tasks**, and **DevOps workflows** into one cohesive toolkit.

---

> *“MSTools — bringing clarity, consistency, and control to PowerShell automation.”*
