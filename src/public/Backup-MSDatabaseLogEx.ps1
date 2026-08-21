function Backup-MSDatabaseLogEx {
<#
.SYNOPSIS
Smart transaction log backup based on log usage and reuse signals.

.DESCRIPTION
Backup-MSDatabaseLogEx evaluates transaction log usage for one or more SQL Server
instances and performs log backups based on configurable decision rules.

The function determines whether a backup is required using the following logic:

1. Forced execution (-Force)
2. log_reuse_wait_desc = LOG_BACKUP
3. Log usage percentage reaching or exceeding a defined threshold

The threshold can be calculated using:
- Current log size usage percentage, OR
- An effective maximum log size (MIN between configured log max size and -MaxLogSizeMB parameter)

If -Path is not specified, the log backup is written to NUL, allowing log truncation
without generating a physical backup file.

System databases are excluded automatically. Databases in SIMPLE recovery model,
offline state, or secondary Availability Group replicas are skipped.

The function returns a detailed audit object per database including:
- Log size metrics
- Threshold basis and reference percentage
- Reuse wait status
- Last log backup date
- Decision reason and execution result

.PARAMETER SqlInstance
One or more SQL Server instances to evaluate.

.PARAMETER Database
Optional list of specific databases to include.

.PARAMETER ExcludeDatabase
Optional list of databases to exclude from evaluation.

.PARAMETER LogUsedPercentThreshold
Percentage threshold used to trigger a backup.
Default is 30.

.PARAMETER MaxLogSizeMB
Optional global maximum log size reference (in MB).
When specified, EffectiveMaxLogMB = MIN(MaxLogSizeMB, ConfiguredMaxLogMB).
If not specified, current log size percentage is used.

.PARAMETER Path
Destination folder for log backup files.
If not specified, backup is written to NUL.

.PARAMETER Force
Forces log backup regardless of usage or reuse wait conditions.

.EXAMPLE
Backup-MSDatabaseLogEx -SqlInstance "SQL01"

Evaluates all user databases on SQL01 and performs log backup
if usage reaches the default 30% threshold.

.EXAMPLE
Backup-MSDatabaseLogEx -SqlInstance "SQL01" -LogUsedPercentThreshold 50 -Path "\\backup\logs"

Backs up logs when usage exceeds 50% and writes backup files
to the specified path.

.EXAMPLE
Backup-MSDatabaseLogEx -SqlInstance "SQL01" -MaxLogSizeMB 2048

Uses an effective maximum log size of 2GB as reference
when calculating threshold percentage.

.EXAMPLE
Backup-MSDatabaseLogEx -SqlInstance "SQL01" -Force

Forces log backup for all eligible databases.

.NOTES
Author  : Sozezzo Astra
Purpose : Controlled log growth management with audit output
Requires: dbatools module
Supports: ShouldProcess (-WhatIf, -Confirm)
#>

    [CmdletBinding(SupportsShouldProcess)]
    param(
        [Parameter(Mandatory)]
        [string[]] $SqlInstance,

        [string[]] $Database,

        [string[]] $ExcludeDatabase,

        [int] $LogUsedPercentThreshold = 30,

        # Global reference max size (MB) for ALL databases
        # EffectiveMaxLogMB = MIN(MaxLogSizeMB, ConfiguredMaxLogMB)
        [nullable[int]] $MaxLogSizeMB,

        # If $null => backup to NUL
        [string] $Path,

        [switch] $Force
    )

    foreach ($instance in $SqlInstance) {

        Write-Host "Processing instance [$instance]"
        Write-Host   "INSTANCE [$instance] - start"

        $databases = Get-DbaDatabase -SqlInstance $instance -ExcludeSystem |
                     Where-Object { (-not $Database -or $_.Name -in $Database) }

        foreach ($db in $databases) {

            Write-Host "DB [$($db.Name)] - start evaluation"

            # ---- Defaults (always returned) ----
            $decision       = 'SKIP'
            $decisionOrder  = 0
            $reason         = 'NONE'
            $result         = 'Skipped'
            $errorMessage   = $null

            $currentTotalLogMB         = $null
            $usedLogMB                 = $null
            $usedPercentCurrent        = $null
            $configuredMaxLogMB        = $null
            $effectiveMaxLogMB         = $null
            $usedPercentOfEffectiveMax = $null
            $thresholdBasis            = $null
            $reuseWait                 = $null
            $lastBackup                = $null

            try {
                # ---- Eligibility gate ----

                if ($ExcludeDatabase -and $db.Name -in $ExcludeDatabase) {
                    Write-Host "DB [$($db.Name)] - skipped (excluded)"
                    $reason = 'EXCLUDED_DATABASE'
                    throw 'SKIP'
                }

                if ($db.RecoveryModel -eq 'Simple') {
                    Write-Host "DB [$($db.Name)] - skipped (SIMPLE recovery)"
                    $reason = 'SIMPLE_RECOVERY'
                    throw 'SKIP'
                }

                if ($db.Status -ne 'Normal') {
                    Write-Host "DB [$($db.Name)] - skipped (status=$($db.Status))"
                    $reason = 'DB_NOT_ONLINE'
                    throw 'SKIP'
                }

                if ($db.AvailabilityGroup -and $db.AvailabilityGroupRole -ne 'Primary') {
                    Write-Host "DB [$($db.Name)] - skipped (AG role=$($db.AvailabilityGroupRole))"
                    $reason = 'AG_SECONDARY'
                    throw 'SKIP'
                }

                # ---- Collect log signals ----

                Write-Host "DB [$($db.Name)] - collecting log metrics"

                $q = @"
;WITH LogFiles AS (
    SELECT max_size_pages =
        CASE WHEN df.max_size = -1 THEN NULL ELSE df.max_size END
    FROM sys.database_files df
    WHERE df.type = 1
)
SELECT
    total_log_size_mb     = lsu.total_log_size_in_bytes / 1024.0 / 1024.0,
    used_log_percent      = lsu.used_log_space_in_percent,
    log_reuse_wait_desc   = d.log_reuse_wait_desc,
    configured_max_log_mb =
        SUM(CASE WHEN lf.max_size_pages IS NULL
                 THEN NULL
                 ELSE (lf.max_size_pages * 8.0 / 1024.0) END)
FROM sys.dm_db_log_space_usage lsu
JOIN sys.databases d ON d.database_id = DB_ID()
LEFT JOIN LogFiles lf ON 1 = 1
GROUP BY
    lsu.total_log_size_in_bytes,
    lsu.used_log_space_in_percent,
    d.log_reuse_wait_desc;
"@

                $logInfo = Invoke-DbaQuery -SqlInstance $instance -Database $db.Name -Query $q -ErrorAction Stop

                $lastLogBackup = Invoke-DbaQuery -SqlInstance $instance -Database msdb -Query @"
SELECT MAX(backup_finish_date) AS last_log_backup
FROM msdb.dbo.backupset
WHERE database_name = '$($db.Name)'
AND type = 'L';
"@ -ErrorAction Stop

                $currentTotalLogMB  = [double]$logInfo.total_log_size_mb
                $usedPercentCurrent = [double]$logInfo.used_log_percent
                $reuseWait          = [string]$logInfo.log_reuse_wait_desc
                $configuredMaxLogMB = if ($logInfo.configured_max_log_mb -is [System.DBNull]) {
                                            $null
                                      } else {
                                            [double]$logInfo.configured_max_log_mb
                                      }

                $usedLogMB  = $currentTotalLogMB * ($usedPercentCurrent / 100.0)
                $lastBackup = $lastLogBackup.last_log_backup

                Write-Host ("DB [{0}] - LogSize={1:N1}MB Used={2:N1}% UsedMB={3:N1}MB ReuseWait={4}" -f `
                    $db.Name, $currentTotalLogMB, $usedPercentCurrent, $usedLogMB, $reuseWait)

                # ---- Effective max size ----

                if ($MaxLogSizeMB -and $configuredMaxLogMB) {
                    $effectiveMaxLogMB = [double]([Math]::Min($MaxLogSizeMB.Value, $configuredMaxLogMB))
                }
                elseif ($MaxLogSizeMB) {
                    $effectiveMaxLogMB = [double]$MaxLogSizeMB.Value
                }
                elseif ($configuredMaxLogMB) {
                    $effectiveMaxLogMB = [double]$configuredMaxLogMB
                }

                Write-Host ("DB [{0}] - ConfiguredMax={1} ParamMax={2} EffectiveMax={3}" -f `
                    $db.Name, $configuredMaxLogMB, $MaxLogSizeMB, $effectiveMaxLogMB)

                # ---- Threshold basis ----

                if ($effectiveMaxLogMB) {
                    $usedPercentOfEffectiveMax = ($usedLogMB / $effectiveMaxLogMB) * 100.0
                    $thresholdBasis = 'EFFECTIVE_MAX'
                }
                else {
                    $usedPercentOfEffectiveMax = $usedPercentCurrent
                    $thresholdBasis = 'CURRENT_SIZE'
                }

                Write-Host ("DB [{0}] - ThresholdBasis={1} UsedRef={2:N1}% Threshold={3}%" -f `
                    $db.Name, $thresholdBasis, $usedPercentOfEffectiveMax, $LogUsedPercentThreshold)

                # ---- Decision matrix ----

                if ($Force) {
                    Write-Host "DB [$($db.Name)] - decision=BACKUP (FORCED)"
                    $decision      = 'BACKUP'
                    $decisionOrder = 1
                    $reason        = 'FORCED'
                }
                elseif ($reuseWait -eq 'LOG_BACKUP') {
                    Write-Host "DB [$($db.Name)] - decision=BACKUP (LOG_BACKUP)"
                    $decision      = 'BACKUP'
                    $decisionOrder = 2
                    $reason        = 'LOG_BACKUP'
                }
                elseif ($usedPercentOfEffectiveMax -ge $LogUsedPercentThreshold) {
                    Write-Host "DB [$($db.Name)] - decision=BACKUP (THRESHOLD)"
                    $decision      = 'BACKUP'
                    $decisionOrder = 3
                    $reason        = 'LOG_USED_THRESHOLD'
                }
                else {
                    Write-Host "DB [$($db.Name)] - decision=SKIP (below threshold)"
                    $reason = 'BELOW_THRESHOLD'
                    throw 'SKIP'
                }

                # ---- Execute backup ----

                if ($decision -eq 'BACKUP') {

                    $backupTarget = if ($Path) { $Path } else { 'NUL' }
                    Write-Host "DB [$($db.Name)] - executing backup to [$backupTarget]"

                    if ($PSCmdlet.ShouldProcess("$instance / $($db.Name)", "Backup LOG to $backupTarget")) {

                        if ($Path) {
                            Write-Host "DB [$($db.Name)] - Backup-DbaDatabase -Path"
                            Backup-DbaDatabase `
                                -SqlInstance $instance `
                                -Database $db.Name `
                                -Type Log `
                                -Path $Path `
                                -ErrorAction Stop
                        }
                        else {
                            Write-Host "DB [$($db.Name)] - Backup-DbaDatabase -FilePath NUL"
                            Backup-DbaDatabase `
                                -SqlInstance $instance `
                                -Database $db.Name `
                                -Type Log `
                                -FilePath 'NUL' `
                                -ErrorAction Stop
                        }

                        Write-Host "DB [$($db.Name)] - backup completed"
                        $result = 'Success'
                    }
                    else {
                        Write-Host "DB [$($db.Name)] - WhatIf (backup skipped)"
                        $result = 'WhatIf'
                    }
                }
            }
            catch {
                if ($_ -ne 'SKIP') {
                    Write-Host "DB [$($db.Name)] - ERROR: $($_.Exception.Message)"
                    $result = 'Failed'
                    $errorMessage = $_.Exception.Message
                }
            }

            # ---- Audit output ----

            [pscustomobject]@{
                Instance           = $instance
                Database           = $db.Name
                RecoveryModel      = $db.RecoveryModel
                AGRole             = $db.AvailabilityGroupRole

                CurrentTotalLogMB  = $currentTotalLogMB
                UsedLogMB          = $usedLogMB
                UsedPercentCurrent = $usedPercentCurrent

                ConfiguredMaxLogMB = $configuredMaxLogMB
                MaxLogSizeMBParam  = $MaxLogSizeMB
                EffectiveMaxLogMB  = $effectiveMaxLogMB
                UsedPercentRef     = $usedPercentOfEffectiveMax
                ThresholdBasis     = $thresholdBasis
                ThresholdPercent   = $LogUsedPercentThreshold

                ReuseWait          = $reuseWait
                LastLogBackup      = $lastBackup

                Decision           = $decision
                DecisionOrder      = $decisionOrder
                Reason             = $reason

                BackupTarget       = if ($Path) { $Path } else { 'NUL' }
                Result             = $result
                Error              = $errorMessage
                Timestamp          = Get-Date
            }

            Write-Host "DB [$($db.Name)] - end"
        }

        Write-Host "INSTANCE [$instance] - end"
    }
}