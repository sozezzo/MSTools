function Invoke-DbaDbCollationRebuild {
    <#
    .SYNOPSIS
        Copy a database to a new or same instance with a different collation.

    .DESCRIPTION
        Orchestrates: (optional) initial copy, target DB create w/ new collation,
        base schema deploy, (optional) data copy, programmables, PK / Index / FK / constraints, users,
        then diff report.

    .PARAMETER SourceInstance
    .PARAMETER SourceDatabase
    .PARAMETER DestInstance
    .PARAMETER NewDatabase
    .PARAMETER TargetCollation
    .PARAMETER WorkDir
    .PARAMETER Owner
    .PARAMETER RecoveryModel
    .PARAMETER CompatibilityLevel
        Default 150 (SQL 2019 compat). STRING_AGG requires >= 140.

    .PARAMETER DoInitialCopy
        If set, runs Copy-DbaDatabase with the provided settings.

    .PARAMETER InitialCopySharedPath
        UNC share path for -BackupRestore method.

    .PARAMETER InitialCopyMethod
        'BackupRestore' or 'Direct' (pass through to dbatools), default 'BackupRestore'.

    .PARAMETER LogFileName
        Log file path (default: <WorkDir>\<SourceDb>_execution.log)

    .PARAMETER CopyData
        If set, invokes table-by-table data copy with defaults/tunables.

    .PARAMETER DataBatchSize
    .PARAMETER DataNotifyAfter
    .PARAMETER DataKeepIdentity
    .PARAMETER DataKeepNulls
    .PARAMETER DataTruncateDestination
    .PARAMETER DataAutoCreateMissingTables

    .EXAMPLE
        Invoke-DbCollationRebuild -SourceInstance 'SQL01' -SourceDatabase 'AppDb' `
          -DestInstance 'SQL02' -NewDatabase 'AppDb_New' -TargetCollation 'Latin1_General_CI_AS' `
          -WorkDir 'C:\temp\dbatools-collation' -DoInitialCopy -InitialCopySharedPath '\\sql02\backup$' -CopyData

    .NOTES
        Requires: dbatools
    #>
    [CmdletBinding(SupportsShouldProcess, ConfirmImpact='High')]
    param(
        # Core
        [Parameter(Mandatory)][string]$SourceInstance,
        [Parameter(Mandatory)][string]$SourceDatabase,
        [Parameter(Mandatory)][string]$DestInstance,
        [Parameter(Mandatory)][string]$NewDatabase,
        [Parameter(Mandatory)][string]$TargetCollation,
        [Parameter(Mandatory)][string]$WorkDir,

        # DB options
        [ValidateSet('Simple','Full','BulkLogged')]
        [string]$RecoveryModel = 'Simple',
        [string]$Owner = 'sa',
        [ValidateSet(80,90,100,110,120,130,140,150,160)] [int]$CompatibilityLevel = 150,

        # Optional initial copy
        [switch]$DoInitialCopy,
        [ValidateSet('BackupRestore','Direct')] [string]$InitialCopyMethod = 'BackupRestore',
        [string]$InitialCopySharedPath,

        # Logging
        [string]$LogFileName,

        # Data copy options
        [switch]$CopyData,
        [int]$DataBatchSize = 100000,
        [int]$DataNotifyAfter = 100000,
        [switch]$DataKeepIdentity,
        [switch]$DataKeepNulls,
        [switch]$DataTruncateDestination,
        [switch]$DataAutoCreateMissingTables
    )

    begin {
        if (-not (Get-Command Invoke-DbaQuery -ErrorAction SilentlyContinue)) {
            throw "dbatools is required. Install-Module dbatools -Scope CurrentUser"
        }

        # Paths
        if (-not (Test-Path -LiteralPath $WorkDir)) { New-Item -ItemType Directory -Force -Path $WorkDir | Out-Null }
        if (-not $LogFileName) { $LogFileName = Join-Path $WorkDir "$($SourceDatabase)_execution.log" }

        $baseScript      = Join-Path $WorkDir "$($SourceDatabase)_base.sql"
        $progScript      = Join-Path $WorkDir "$($SourceDatabase)_programmables.sql"
        $trigDmlScript   = Join-Path $WorkDir "$($SourceDatabase)_dml_triggers.sql"
        $pkScript        = Join-Path $WorkDir "$($SourceDatabase)_primarykeys.sql"
        $idxFkScript     = Join-Path $WorkDir "$($SourceDatabase)_idx_fk.sql"
        $idxOnlyScript   = Join-Path $WorkDir "$($SourceDatabase)_indexes_only.sql"
        $constraintsSql  = Join-Path $WorkDir "$($SourceDatabase)_constraints.sql"

        $beginScript = Get-Date
        $VerbosePreference = 'SilentlyContinue'
        $ConfirmPreference = 'None'

        Write-Log -Message "Start database collation rebuild: [$SourceInstance].[$SourceDatabase] -> [$DestInstance].[$NewDatabase] ($TargetCollation)" -Level Info -LogFileName $LogFileName -Verbose
    }

    process {
        # 1) Optional: Copy database to destination as <SourceDb>_OLD (or use your own naming)
        if ($DoInitialCopy) {
            if ($InitialCopyMethod -eq 'BackupRestore' -and -not $InitialCopySharedPath) {
                throw "When -DoInitialCopy and -InitialCopyMethod BackupRestore, you must provide -InitialCopySharedPath (UNC)."
            }
            $newName = "${SourceDatabase}_OLD"
            Write-Log -Message "Initial copy via $InitialCopyMethod to [$DestInstance].[$newName]" -Level Info -LogFileName $LogFileName -Verbose

            if ($PSCmdlet.ShouldProcess("$DestInstance / $newName", "Copy-DbaDatabase ($InitialCopyMethod)")) {
                $copyParams = @{
                    Source = $SourceInstance
                    Destination = $DestInstance
                    Database = $SourceDatabase
                    NewName = $newName
                    Force = $true
                    Confirm = $false
                    EnableException = $true
                }
                if ($InitialCopyMethod -eq 'BackupRestore') {
                    $copyParams.BackupRestore = $true
                    $copyParams.SharedPath = $InitialCopySharedPath
                }
                Copy-DbaDatabase @copyParams *>> $LogFileName
            }
        }

        # 2) Create target DB with new collation (drops existing if requested by user via -Confirm)
        Initialize-DbaCloneTargetDatabase -DestInstance $DestInstance -NewDatabase $NewDatabase -TargetCollation $TargetCollation `
            -DropIfExists -PassThru -RecoveryModel $RecoveryModel -Owner $Owner -LogFileName $LogFileName | Out-Null

        # Compatibility for STRING_AGG in compare queries
        if ($PSCmdlet.ShouldProcess("$DestInstance / $NewDatabase", "Set compatibility level $CompatibilityLevel")) {
            Write-Log -Message "Setting DB compatibility to $CompatibilityLevel." -Level Info -LogFileName $LogFileName
            Set-DbaDbCompatibility -SqlInstance $DestInstance -Database $NewDatabase -Compatibility $CompatibilityLevel -Confirm:$false
        }

        # Snapshot Isolation ON (makes indexing/rebuilds safer during bulk ops)
        if ($PSCmdlet.ShouldProcess("$DestInstance / $NewDatabase", "Enable ALLOW_SNAPSHOT_ISOLATION")) {
            Set-DbaDbSnapshotIsolation -SqlInstance $DestInstance -Database $NewDatabase -Enable
        }

        # Enable Service Broker (optional but you had it)
        if ($PSCmdlet.ShouldProcess("$DestInstance / $NewDatabase", "ENABLE_BROKER")) {
            Set-DbaDbServiceBroker -SqlInstance $DestInstance -Database $NewDatabase -EnableBroker
        }

        # 3) Base schema (schemas/types/sequences/tables) w/o COLLATE clauses
        Invoke-DbaCloneDatabase -SourceInstance $SourceInstance -SourceDatabase $SourceDatabase `
            -DestInstance $DestInstance -DestDatabase $NewDatabase -BaseScriptPath $baseScript -LogFileName $LogFileName

        # 4) Optional data copy (table-by-table for resilience)
        if ($CopyData) {
            Invoke-DbaCloneDataCopyAllTables -SourceInstance $SourceInstance -SourceDatabase $SourceDatabase `
                -DestInstance $DestInstance -DestDatabase $NewDatabase `
                -BatchSize $DataBatchSize -NotifyAfter $DataNotifyAfter `
                -KeepIdentity:$DataKeepIdentity -KeepNulls:$DataKeepNulls -TruncateDestination:$DataTruncateDestination `
                -AutoCreateMissingTables:$DataAutoCreateMissingTables `
                -LogFileName $LogFileName | Out-Null
        }

        # 5) Keys / Indexes / FKs / Constraints
        Invoke-DbaClonePrimaryKey -SourceInstance $SourceInstance -SourceDatabase $SourceDatabase `
            -DestInstance $DestInstance -DestDatabase $NewDatabase -PkScriptPath $pkScript -MaxPasses 10 -LogFileName $LogFileName | Out-Null

        Invoke-DbaCloneIndexes -SourceInstance $SourceInstance -SourceDatabase $SourceDatabase `
            -DestInstance $DestInstance -DestDatabase $NewDatabase -IdxScriptPath $idxOnlyScript -MaxPasses 10 -LogFileName $LogFileName | Out-Null

        Invoke-DbaClonePkFk -SourceInstance $SourceInstance -SourceDatabase $SourceDatabase `
            -DestInstance $DestInstance -DestDatabase $NewDatabase -IdxPkFkScriptPath $idxFkScript -MaxPasses 10 -LogFileName $LogFileName | Out-Null

        Invoke-DbaCloneConstraint -SourceInstance $SourceInstance -SourceDatabase $SourceDatabase `
            -DestInstance $DestInstance -DestDatabase $NewDatabase -ConstraintsScriptPath $constraintsSql -MaxPasses 10 -LogFileName $LogFileName | Out-Null

        # 6) Programmables (UDFs/Views/Procs/Synonyms/DDL triggers) and DML triggers
        Invoke-DbaCloneProgrammables -SourceInstance $SourceInstance -SourceDatabase $SourceDatabase `
            -DestInstance $DestInstance -DestDatabase $NewDatabase `
            -ProgrammablesPath $progScript -DmlTriggersPath $trigDmlScript -MaxPasses 10 -LogFileName $LogFileName | Out-Null

        # 7) Users
        Invoke-DbaCloneUser -SourceInstance $SourceInstance -SourceDatabase $SourceDatabase `
            -DestInstance $DestInstance -DestDatabase $NewDatabase -LogFileName $LogFileName | Out-Null

        # 8) Diff
        $diff = Compare-DbaDbObject -SourceInstance $SourceInstance -SourceDatabase $SourceDatabase `
                                  -DestInstance $DestInstance   -DestDatabase   $NewDatabase

        $end = Get-Date
        $dur = $end - $beginScript

        Write-Log -Message "End collation rebuild. Duration: $dur" -Level Info -LogFileName $LogFileName -Verbose

        # Return a summary object + diff
        [pscustomobject]@{
            Source        = "$SourceInstance.$SourceDatabase"
            Destination   = "$DestInstance.$NewDatabase"
            TargetCollation = $TargetCollation
            WorkDir       = $WorkDir
            Log           = $LogFileName
            Duration      = $dur
            Differences   = $diff
        }
    }
}
