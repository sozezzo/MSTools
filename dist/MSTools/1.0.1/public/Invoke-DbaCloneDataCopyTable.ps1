function Invoke-DbaCloneDataCopyTable {
<#
.SYNOPSIS
    Copies data for all (selected) user tables from a source database to a destination database,
    table-by-table, with retry-safe isolation so one failure doesn’t stop the entire run.

.DESCRIPTION
    Invoke-DbaCloneDataCopyTable discovers user tables on the source (optionally filtered by schema/table),
    then iterates table-by-table and pipes each table object into Copy-DbaDbTableData. It supports common
    bulk copy options (BatchSize, NotifyAfter, KeepIdentity, KeepNulls, Truncate) and can optionally
    auto-create missing destination tables. Progress and errors are written via Write-Log. Failures
    are collected and surfaced at the end; use -PassThru for a structured summary.

    Key behaviors:
      - Table-by-table copying: a failure on one table won’t abort the whole job.
      - Filters: include/exclude specific tables and/or limit to selected schemas.
      - Optional destination TRUNCATE before copy (per table) via -TruncateDestination.
      - Optional identity/null-preserving flags are passed through to Copy-DbaDbTableData.
      - Optional auto creation of missing target tables via -AutoCreateMissingTables.
      - Supports -WhatIf / -Confirm (SupportsShouldProcess).
      - Detailed timing and a summary of OK/FAIL tables are logged; -PassThru returns a summary object.

.PARAMETER SourceInstance
    SQL Server instance name hosting the source database (e.g. 'SQL01' or 'SQL01\INST1').

.PARAMETER SourceDatabase
    Name of the source database containing the data to copy.

.PARAMETER DestInstance
    SQL Server instance name hosting the destination database.

.PARAMETER DestDatabase
    Name of the destination database where rows will be inserted.

.PARAMETER BatchSize
    Number of rows per bulk batch sent to the destination. Default: 100000.

.PARAMETER NotifyAfter
    Emits a progress notification after this many rows (passed to Copy-DbaDbTableData). Default: 100000.

.PARAMETER CommandTimeout
    Timeout (seconds) for the underlying command. 0 = unlimited. Default: 0.

.PARAMETER KeepIdentity
    Preserve identity values on insert (equivalent to SET IDENTITY_INSERT ON per table as needed).

.PARAMETER KeepNulls
    Keep NULL values from source instead of using destination defaults.

.PARAMETER TruncateDestination
    Truncate the destination table before each copy. (Alias behavior of -Truncate.) Use with caution.

.PARAMETER AutoCreateMissingTables
    When specified, allows auto-creation of destination tables if they don’t exist (per Copy-DbaDbTableData).

.PARAMETER PassThru
    Return a summary [pscustomobject] with counts, duration, and the list of failed tables.

.PARAMETER Schema
    Optional list of schema names to include (exact, case-insensitive), e.g. 'dbo','sales'.
    If omitted, all schemas are considered.

.PARAMETER IncludeTable
    Optional list of table names to include. Each item may be:
      - 'schema.table' for exact schema-qualified match, or
      - 'table' for an exact (case-insensitive) table-name match in any schema.

.PARAMETER ExcludeTable
    Optional list of tables to exclude, using the same matching rules as -IncludeTable.

.PARAMETER LogFileName
    Optional path to a log file. Verbose streams and Write-Log messages will be appended here.

.INPUTS
    None. All inputs are provided via parameters.

.OUTPUTS
    If -PassThru is provided, outputs a PSCustomObject:
        Source          (string) -> "SourceInstance.SourceDatabase"
        Destination     (string) -> "DestInstance.DestDatabase"
        TablesAttempted (int)
        TablesOk        (int)
        TablesFailed    (int)
        Duration        (TimeSpan)
        FailedList      (IEnumerable<PSCustomObject> with Table, Status, Error)

    Otherwise, no output on success.

.EXAMPLES
    # 1) Copy all user tables with defaults
    Invoke-DbaCloneDataCopyTable `
        -SourceInstance "SQL01" -SourceDatabase "AppDB" `
        -DestInstance   "SQL02" -DestDatabase   "AppDB_Clone"

    # 2) Limit to dbo schema and a few tables; return summary
    Invoke-DbaCloneDataCopyTable `
        -SourceInstance "SQL01" -SourceDatabase "Sales" `
        -DestInstance   "SQL02" -DestDatabase   "Sales_Stage" `
        -Schema 'dbo' -IncludeTable 'dbo.Customers','dbo.Orders' `
        -PassThru

    # 3) Exclude large audit tables and truncate destination before copy
    Invoke-DbaCloneDataCopyTable `
        -SourceInstance "Prod\OLTP" -SourceDatabase "HR" `
        -DestInstance   "Stage\OLTP" -DestDatabase   "HR_Stage" `
        -ExcludeTable 'dbo.Audit','dbo.ChangeLog' `
        -TruncateDestination `
        -LogFileName "E:\logs\HR-data-copy.log"

    # 4) Preserve identity values and NULLs; smaller batches; longer timeout
    Invoke-DbaCloneDataCopyTable `
        -SourceInstance "SQL01" -SourceDatabase "Ops" `
        -DestInstance   "SQL02" -DestDatabase   "Ops_QA" `
        -KeepIdentity -KeepNulls `
        -BatchSize 20000 -NotifyAfter 20000 `
        -CommandTimeout 3600

    # 5) Auto-create missing tables on destination (use carefully)
    Invoke-DbaCloneDataCopyTable `
        -SourceInstance "SQL01" -SourceDatabase "RefData" `
        -DestInstance   "SQL02" -DestDatabase   "RefData_Dev" `
        -AutoCreateMissingTables

    # 6) Dry run (show what would happen)
    Invoke-DbaCloneDataCopyTable `
        -SourceInstance "SQL01" -SourceDatabase "AppDB" `
        -DestInstance   "SQL02" -DestDatabase   "AppDB_QA" `
        -WhatIf

.NOTES
    Requirements:
      - dbatools module:
            Get-DbaDbTable
            Copy-DbaDbTableData
      - A Write-Log function that supports: -Message, -Level, -LogFileName.
      - Permissions:
            Source: SELECT on source tables.
            Destination: INSERT (and ALTER if using TRUNCATE or auto-create); CREATE TABLE if -AutoCreateMissingTables.

    Behavior details:
      - Tables are discovered via Get-DbaDbTable and filtered by -Schema/-IncludeTable/-ExcludeTable.
      - Each table copy is wrapped in try/catch; failures are logged and accumulated in FailedList.
      - -TruncateDestination maps to Copy-DbaDbTableData -Truncate per table.
      - -KeepIdentity and -KeepNulls are passed to Copy-DbaDbTableData.
      - -AutoCreateMissingTables maps to Copy-DbaDbTableData -AutoCreateTable.
      - CommandTimeout=0 disables timeouts (infinite).

    Logging:
      - Writes start/stop timestamps, per-table progress, duration, and OK/FAIL counts via Write-Log.
      - When -LogFileName is specified, verbose streams from Copy-DbaDbTableData are appended to that file.

    Error handling:
      - Exceptions at the outer scope are logged and rethrown.
      - Per-table exceptions are caught; the run continues with subsequent tables.

.LINK
    https://dbatools.io/
#>
	
    [CmdletBinding(SupportsShouldProcess)]
    param(
        [Parameter(Mandatory)][string]$SourceInstance,
        [Parameter(Mandatory)][string]$SourceDatabase,
        [Parameter(Mandatory)][string]$DestInstance,
        [Parameter(Mandatory)][string]$DestDatabase,

        # Performance/config
        [int]$BatchSize = 100000,
        [int]$NotifyAfter = 100000,
        [int]$CommandTimeout = 0,          # 0 = unlimited

        # Behavior flags
        [switch]$KeepIdentity,
        [switch]$KeepNulls,
        [switch]$TruncateDestination,      # same as -Truncate
        [switch]$AutoCreateMissingTables,  # allow auto-create on destination (default off)
        [switch]$PassThru,

        # Filters
        [string[]]$Schema,                 # e.g. 'dbo','sales'
        [string[]]$IncludeTable,           # names without schema -> apply to any schema, or use 'schema.table'
        [string[]]$ExcludeTable,

        # Logging
        [string]$LogFileName
    )

    # Helper to match table names with optional schema
    function _matches {
        param([string]$schema,[string]$name,[string[]]$patterns)
        if (-not $patterns -or $patterns.Count -eq 0) { return $true }
        foreach ($p in $patterns) {
            if ($p -like '*.*') {
                if ("$schema.$name" -ieq $p) { return $true }
            } else {
                if ($name -ieq $p) { return $true }
            }
        }
        return $false
    }

    try {
        Write-Log -Message "Copy all user tables from $SourceDatabase [$SourceInstance] to $DestDatabase [$DestInstance]" -Level Info -LogFileName $LogFileName -Verbose
        if ($LogFileName) { Write-Log -Message "Log file: $LogFileName" -Level Info -LogFileName $LogFileName }

        # Build table list
        $tables = Get-DbaDbTable -SqlInstance $SourceInstance -Database $SourceDatabase

        if ($Schema)       { $tables = $tables | Where-Object { $Schema -contains $_.Schema } }
        if ($IncludeTable) { $tables = $tables | Where-Object { _matches $_.Schema $_.Name $IncludeTable } }
        if ($ExcludeTable) { $tables = $tables | Where-Object { -not (_matches $_.Schema $_.Name $ExcludeTable) } }

        $total = ($tables | Measure-Object).Count
        Write-Log -Message "Tables selected for copy: $total" -Level Info -LogFileName $LogFileName -Verbose

        # Track stats
        $start   = Get-Date
        $ok      = New-Object System.Collections.Generic.List[object]
        $failed  = New-Object System.Collections.Generic.List[object]

        # Copy table-by-table so a single failure doesn't kill the run
        $i = 0
        foreach ($t in $tables) {
            $i++
            $fq = "[$($t.Schema)].[$($t.Name)]"
            Write-Log -Message "[$i/$total] Copying $fq ..." -Level Info -LogFileName $LogFileName -Verbose

            if ($PSCmdlet.ShouldProcess("$DestInstance/$DestDatabase", "Copy $fq")) {
                try {
                    $params = @{
                        Destination          = $DestInstance
                        DestinationDatabase  = $DestDatabase
                        BatchSize            = $BatchSize
                        NotifyAfter          = $NotifyAfter
                        KeepIdentity         = [bool]$KeepIdentity
                        KeepNulls            = [bool]$KeepNulls
                        Truncate             = [bool]$TruncateDestination
                        CommandTimeout       = $CommandTimeout
                        EnableException      = $true
                        Confirm              = $false
                        Verbose              = $false
                    }
                    if ($AutoCreateMissingTables) { $params.AutoCreateTable = $true } else { $params.AutoCreateTable = $false }

                    # Pipe the specific table object
                    $t | Copy-DbaDbTableData @params *>> $LogFileName

                    $ok.Add([pscustomobject]@{ Table=$fq; Status='OK' })
                }
                catch {
                    $msg = $_.Exception.Message
                    Write-Log -Message "FAILED to copy $fq : $msg" -Level Error -LogFileName $LogFileName -Verbose
                    $failed.Add([pscustomobject]@{ Table=$fq; Status='FAIL'; Error=$msg })
                    continue
                }
            }
        }

        $end = Get-Date
        $dur = $end - $start

        Write-Log -Message ("Duration to copy all user tables: {0:c}" -f $dur) -Level Important -LogFileName $LogFileName -Verbose
        Write-Log -Message ("Summary: OK={0}  FAIL={1}" -f $ok.Count,$failed.Count) -Level Alert -LogFileName $LogFileName -Verbose

        if ($failed.Count -gt 0) {
            $failed | ForEach-Object { Write-Log -Message ("  - " + $_.Table + " :: " + $_.Error) -Level Warning -LogFileName $LogFileName }
        }

        if ($PassThru) {
            [pscustomobject]@{
                Source          = "$SourceInstance.$SourceDatabase"
                Destination     = "$DestInstance.$DestDatabase"
                TablesAttempted = $total
                TablesOk        = $ok.Count
                TablesFailed    = $failed.Count
                Duration        = $dur
                FailedList      = $failed
            }
        }
    }
    catch {
        Write-Log -Message ("ERROR (Invoke-DbaCloneDataCopyTable): " + $_.Exception.Message) -Level Error -LogFileName $LogFileName
        throw
    }
}