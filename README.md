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
   
   The latest version folder contains the most up-to-date module build (e.g., `MSTools_1.0.3`).

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

### 3. Install / Update from GitHub

The `Import-MSTools` function can install or update the module directly from a
local/network path or (by default) from GitHub:

```powershell
Import-MSTools
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

Or update in place from the source with:

```powershell
Import-MSTools
```

---

## 🧩 Included Functions

Below is the full list of functions currently available in MSTools **v1.0.3**, grouped by category.

### 💾 Backup & Restore

| Function | Description |
| --- | --- |
| `Backup-MSDatabaseExt` | Performs filtered SQL Server backups (Full, Diff, or Log) using dbatools. |
| `Backup-MSDatabaseLogEx` | Smart transaction log backup based on log usage and reuse signals. |
| `Test-MSSqlBackupReadiness` | Deep diagnostic that explains why a backup may fail on a SQL Server instance. |

### 🧬 Database Cloning & Copy

| Function | Description |
| --- | --- |
| `Invoke-MSCloneDatabaseStructure` | Scripts the base schema (schemas, table types, sequences, tables) from a source. |
| `Invoke-MSCloneConstraint` | Scripts and deploys only table constraints (FK, UNIQUE, CHECK, DEFAULT). |
| `Invoke-MSCloneData` | Copies data for all (selected) user tables from source to destination. |
| `Invoke-MSCloneData2` | Copies data for selected/all user tables from source to destination. |
| `Invoke-MSCloneFunctions` | Scripts and deploys user-defined functions (UDFs) to a destination database. |
| `Invoke-MSCloneIndexes` | Scripts and deploys only non-constraint indexes. |
| `Invoke-MSClonePkFk` | Exports and deploys indexes, primary keys, and foreign keys. |
| `Invoke-MSClonePrimaryKey` | Scripts and deploys only primary keys (PK). |
| `Invoke-MSCloneProgrammables` | Scripts and deploys UDFs, views, procedures, synonyms, and DDL triggers. |
| `Invoke-MSCloneUser` | Clones database users, optionally recreating and adding missing logins. |
| `Invoke-MSCloneViews` | Scripts and deploys views to a destination database. |
| `Copy-MSDatabaseToSimple` | Copies a database, sets recovery to SIMPLE, changes owner to sa, and shrinks. |
| `Copy-MSDbProcedure` | Copies user stored procedures between databases/servers. |
| `New-MSTargetDatabase` | Ensures a clean target database exists on a destination SQL Server. |

### 🖥️ Database Configuration

| Function | Description |
| --- | --- |
| `Set-MSAutoShrink` | Enables or disables AUTO_SHRINK across instances and databases. |
| `Set-MSDbCollation` | Changes the default collation of one or more databases. |
| `Invoke-MSDbCollationRebuild` | Copies a database to a new/same instance with a different collation. |
| `Set-MSDbServiceBroker` | Enables, disables, or resets (NEW_BROKER) Service Broker state. |
| `Set-MSDbSnapshotIsolation` | Enables or disables ALLOW_SNAPSHOT_ISOLATION on a database. |
| `Set-MSDbTrustworthy` | Sets the database TRUSTWORTHY option. |
| `Set-MSPageVerify` | Sets the PAGE_VERIFY option for databases. |
| `Set-MSParallelismCTFP` | Sets the Cost Threshold for Parallelism (CTFP). |
| `Set-MSParallelismGoodMaxDop` | Sets or recommends a safe MAXDOP value based on NUMA topology. |
| `Set-MSLoginState` | Enables, disables, or drops SQL logins and kills their connections. |

### 🩺 Database Integrity

| Function | Description |
| --- | --- |
| `Invoke-MSCheckDb` | Runs DBCC CHECKDB safely with controlled modes. |

### 🔀 Comparison Tools

| Function | Description |
| --- | --- |
| `Compare-MSAgentJob` | Compares SQL Agent jobs between two instances (ignoring volatile noise). |
| `Compare-MSAlwaysOnAgentJob` | Compares Agent jobs between AlwaysOn primary and secondary replicas. |
| `Compare-MSCredential` | Compares SQL Server credentials (metadata only) between two instances. |
| `Compare-MSDbObject` | Compares object presence and key metadata between two databases. |
| `Compare-MSFolder` | Compares the content of two folders. |
| `Get-MSFirstStringDifference` | Returns the first difference between two strings. |

### 📤 Export Utilities

| Function | Description |
| --- | --- |
| `Export-MSAgentAlert` | Exports SQL Server Agent Alerts as CREATE scripts. |
| `Export-MSDatabaseMail` | Exports Database Mail (accounts, profiles, mappings, config) to one .sql file. |
| `Export-MSDbCompareReport` | Generates an HTML report comparing two databases. |
| `Export-MSJobCategoryScripts` | Exports T-SQL to recreate SQL Agent categories into one .sql file. |
| `Export-MSJobOperatorScripts` | Exports SQL Agent job operators to a .sql file. |
| `Export-MSJobScripts` | Exports SQL Agent jobs as .sql scripts from one or more instances. |
| `Export-MSJobScriptsCluster` | Exports Agent job scripts from primary and secondary cluster servers. |

### ⚙️ SQL Agent Jobs

| Function | Description |
| --- | --- |
| `Copy-MSAgentJob` | Updates SQL Agent jobs from a source to a destination. |
| `Set-MSAgentJobOwner` | Sets the owner of SQL Agent jobs to a target login. |

### 🌐 Always On / High Availability

| Function | Description |
| --- | --- |
| `Get-MSAlwaysOnHealth` | Produces a read-only health report for SQL Server Always On. |
| `New-MSAlwaysOnHealthReport` | Builds an Always On health report (structured data + HTML). |
| `Get-MSPrimaryServerName` | Returns the current primary replica for an Always On AG target. |
| `Get-MSSecondaryServerName` | Returns the secondary replica name(s) for an AlwaysOn AG. |
| `Sync-MSAlwaysOnLogin` | Compares and synchronizes logins between AlwaysOn replicas. |

### 🔗 Linked Servers

| Function | Description |
| --- | --- |
| `Copy-MSLinkedServerData` | Copies the data of a linked server's tables/views into the target database. |
| `Get-MSLinkedServerObjectInfo` | Analyzes tables and views exposed by a linked server. |
| `New-MSLinkedServerDatabaseStructure` | Creates a database mirroring the structure of linked server tables. |
| `New-MSLinkedServerLogin` | Creates/configures a SQL login for linked server access. |

### 🔌 Connectivity & Network

| Function | Description |
| --- | --- |
| `Test-MSSqlConnectivity` | Verifies everything required to connect to a SQL Server instance remotely. |
| `Invoke-MSTcpPortScan` | Parallel TCP port scanner using runspaces (PS 5.1 compatible). |
| `Test-MSWmiAccess` | Tests whether a user account can access WMI on a remote server. |
| `Get-MSWmiCredential` | Prompts for (and optionally validates) an Active Directory credential. |
| `Grant-MSWmiAccess` | Grants a service account required WMI permissions on the local machine. |

### 🧮 Query & Batch Execution

| Function | Description |
| --- | --- |
| `Invoke-MSMultiInstanceQuery` | Executes the same query against multiple instances and databases. |
| `Invoke-MSExecuteBatchWithRetry` | Executes a .sql file in GO-split batches, retrying failed batches. |
| `Invoke-MSRestartLocalServer` | Restarts the local server immediately. |

### 🔑 Connection String Utilities

| Function | Description |
| --- | --- |
| `Add-MSConnectionStringPart` | Appends one segment to a connection string builder. |
| `Split-MSConnectionString` | Splits a connection string into individual tag/value segments. |
| `Find-MSNextConnectionStringSeparator` | Finds the next semicolon separator in a connection string. |
| `Set-MSConnectionStringTagValue` | Sets or adds a tag/value pair inside a connection string. |
| `Remove-MSConnectionStringTag` | Removes a tag from a connection string. |
| `Test-MSConnectionStringPasswordKey` | Tests whether a connection-string key represents a password field. |

### 📁 File & Folder Operations

| Function | Description |
| --- | --- |
| `Compress-MSFolder` | Compresses a folder using .NET built-in ZIP compression. |
| `Get-MSFileAgeInSeconds` | Returns seconds since a file's last modification. |
| `Get-MSLastFileModifiedDate` | Gets the most recent file modification date from a folder. |
| `Test-MSFolderOrStop` | Ensures a folder exists, creating it if missing. |

### 🗃️ Variable Store

| Function | Description |
| --- | --- |
| `Save-MSVariable` | Saves or updates a variable in the MS store. |
| `Get-MSVariable` | Retrieves a variable value from the MS store. |
| `Remove-MSVariable` | Removes a variable from the MS store. |
| `Clear-MSVariableStore` | Deletes the entire MS variable store. |

### 🖧 System & Environment

| Function | Description |
| --- | --- |
| `Get-MSInstalledApps` | Retrieves installed applications with fine-grained filtering. |
| `Set-MSLockScreenTimeout` | Sets the Windows Console Lock Display Off Timeout. |
| `Test-MSIsAdmin` | Checks if the current session is running as Administrator. |

### 📧 Reporting & Notifications

| Function | Description |
| --- | --- |
| `Convert-MSArrayToHtml` | Converts an array of objects into an HTML table or document. |
| `Send-MSToolsMail` | Sends an email using the legacy Send-MailMessage cmdlet. |

### 🧱 Module & Logging

| Function | Description |
| --- | --- |
| `Import-MSTools` | Installs or updates MSTools from a local/network path or GitHub. |
| `Repair-MSModule` | Forces a module to reload by removing and re-importing it. |
| `Write-MSLog` | Writes timestamped log messages to a primary and optional extra log. |

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
