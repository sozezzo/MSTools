function Copy-MSDatabaseToSimple {
<#
.SYNOPSIS
Copies a SQL Server database using dbatools, then sets recovery model to SIMPLE, changes owner to sa, backs up to NUL, and shrinks the database.

.DESCRIPTION
Copy-MSDatabaseToSimple wraps the following dbatools operations:

1. Optionally backs up and shrinks the source database log file if the source database is not in SIMPLE recovery model.
2. Copies the database using Copy-DbaDatabase in Backup/Restore mode.
3. Sets the destination database recovery model to SIMPLE.
4. Changes the destination database owner to sa.
5. Runs a backup to NUL on the destination database.
6. Shrinks the destination database.

This is mainly useful for DEV, TEST, LAB, or temporary database copies.

WARNING:
Changing the recovery model to SIMPLE breaks the log backup chain.
Do not use this blindly on production databases that require point-in-time recovery.

.PARAMETER Source
Source SQL Server instance.

.PARAMETER Destination
Destination SQL Server instance.

.PARAMETER Database
Database name to copy and configure.

.PARAMETER SharedPath
Shared backup path accessible by both source and destination SQL Server services.

.PARAMETER Owner
Target database owner on the destination. Default is sa.

.PARAMETER ShrinkSourceLog
If specified, checks the source database recovery model.
If the source database is not SIMPLE, backs up the source transaction log and shrinks the source log file.

.EXAMPLE
Copy-MSDatabaseToSimple `
    -Source "MySourceServerDb" `
    -Destination "MyDestinationDb" `
    -Database "MyDatabase" `
    -SharedPath "\\MyServerFile\MySharedFolder\"

.EXAMPLE
Copy-MSDatabaseToSimple `
    -Source "MySourceServerDb" `
    -Destination "MyDestinationDb" `
    -Database "MyDatabase" `
    -SharedPath "\\MyServerFile\MySharedFolder\" `
    -ShrinkSourceLog

Copies the database and also backs up/shrinks the source log file if the source database is not SIMPLE.
#>

    [CmdletBinding()]
    param (
        [Parameter(Mandatory)]
        [string]$Source,

        [Parameter(Mandatory)]
        [string]$Destination,

        [Parameter(Mandatory)]
        [string]$Database,

        [Parameter(Mandatory)]
        [string]$SharedPath,

        [Parameter()]
        [string]$Owner = "sa",

        [Parameter()]
        [switch]$ShrinkSourceLog
    )

    Write-Host "Starting copy process for database [$Database]." -ForegroundColor Cyan
    Write-Host "Source      : $Source"
    Write-Host "Destination : $Destination"
    Write-Host "SharedPath  : $SharedPath"
    Write-Host ""

    if ($ShrinkSourceLog) {

        Write-Host "Checking source database recovery model for [$Database]..." -ForegroundColor Cyan

        $sourceDb = Get-DbaDatabase `
            -SqlInstance $Source `
            -Database $Database

        if (-not $sourceDb) {
            throw "Database [$Database] was not found on source instance [$Source]."
        }

        $sourceRecoveryModel = $sourceDb.RecoveryModel

        Write-Host "Source database recovery model is [$sourceRecoveryModel]."

        if ($sourceRecoveryModel -eq "Simple") {
            Write-Host "Source database [$Database] is already SIMPLE. Skipping source log backup and shrink." -ForegroundColor Yellow
        }
        else {
            Write-Host "Backing up source transaction log for [$Database]..." -ForegroundColor Cyan

            Backup-DbaDatabase `
                -SqlInstance $Source `
                -Database $Database `
                -Type Log `
                -Path $SharedPath

            Write-Host "Shrinking source log file for [$Database]..." -ForegroundColor Cyan

            Invoke-DbaDbShrink `
                -SqlInstance $Source `
                -Database $Database `
                -FileType Log `
                -PercentFreeSpace 0
        }

        Write-Host ""
    }

    Write-Host "Copying database [$Database] from [$Source] to [$Destination]..." -ForegroundColor Cyan

    Copy-DbaDatabase `
        -Source $Source `
        -Destination $Destination `
        -SharedPath $SharedPath `
        -Database $Database `
        -BackupRestore `
        -Force

    Write-Host "Setting destination recovery model to SIMPLE for [$Database]..." -ForegroundColor Cyan

    Set-DbaDbRecoveryModel `
        -SqlInstance $Destination `
        -Database $Database `
        -RecoveryModel Simple `
        -Confirm:$false

    Write-Host "Changing destination database owner to [$Owner] for [$Database]..." -ForegroundColor Cyan

    Set-DbaDbOwner `
        -SqlInstance $Destination `
        -Database $Database `
        -TargetLogin $Owner `
        -Confirm:$false

    Write-Host "Running destination backup to NUL for [$Database]..." -ForegroundColor Cyan

    Backup-DbaDatabase `
        -SqlInstance $Destination `
        -Database $Database `
        -FilePath NUL

    Write-Host "Shrinking destination database [$Database]..." -ForegroundColor Cyan

    Invoke-DbaDbShrink `
        -SqlInstance $Destination `
        -Database $Database `
        -PercentFreeSpace 0

    Write-Host ""
    Write-Host "Completed database copy and post-configuration for [$Database]." -ForegroundColor Green
}