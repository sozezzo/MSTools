function Invoke-DbaClonePkFk {
<#
.SYNOPSIS
    Exports and deploys Indexes, Primary Keys, and Foreign Keys from a source database
    to a destination database without generating CREATE TABLE statements.

.DESCRIPTION
    Invoke-DbaClonePkFk uses dbatools/SMO to:
      1) Enumerate tables on the source database (optionally filtered).
      2) Collect index objects (including PK indexes) and foreign keys while excluding UNIQUE constraints.
      3) Script only those child objects (indexes/PK/FK) to a .sql file (no table creation).
      4) Execute the generated script on the destination database with a retry mechanism.

    Notes:
      - UNIQUE constraints are intentionally excluded from scripting.
      - Scripting options disable collation and omit checks/defaults/triggers to focus on PK/FK/indexes.
      - The output directory for -IdxPkFkScriptPath is created if needed.
      - The function supports -WhatIf / -Confirm (SupportsShouldProcess).
      - Use -PassThru to receive a summary object with counts and paths.

.PARAMETER SourceInstance
    SQL Server instance name of the source (e.g., "ProdSql01" or "ProdSql01\INST1").

.PARAMETER SourceDatabase
    Name of the source database containing the tables and constraints to script.

.PARAMETER DestInstance
    SQL Server instance name of the destination where the script will be executed.

.PARAMETER DestDatabase
    Name of the destination database that already contains the target tables.

.PARAMETER IdxPkFkScriptPath
    Full path to the output .sql file that will contain the scripted indexes/PK/FKs.
    The parent directory will be created if it does not exist. If a file exists at
    this path, it will be overwritten.

.PARAMETER MaxPasses
    Maximum number of passes used by the retry executor when deploying the script.
    Default: 10.

.PARAMETER Schema
    Optional list of schema names to include (case-insensitive exact match), e.g. 'dbo','sales'.
    If omitted, all schemas are considered.

.PARAMETER IncludeTable
    Optional list of table filters. Each entry can be:
      - 'schema.table' for schema-qualified exact match, or
      - 'table' for an exact match by table name (case-insensitive).
    If omitted, all tables are eligible (subject to -Schema/-ExcludeTable).

.PARAMETER ExcludeTable
    Optional list of tables to exclude using the same matching rules as -IncludeTable.

.PARAMETER LogFileName
    Optional path for log output. If provided, internal calls will append verbose output
    and errors to this file via Write-Log and redirected verbose streams.

.PARAMETER PassThru
    When specified, returns a [pscustomobject] summary with counts and paths.

.INPUTS
    None. All inputs are provided via parameters.

.OUTPUTS
    If -PassThru is used:
        PSCustomObject with properties:
            Source           (string)  -> "SourceInstance.SourceDatabase"
            Destination      (string)  -> "DestInstance.DestDatabase"
            TablesConsidered (int)
            IndexesScripted  (int)
            FKsScripted      (int)
            ScriptPath       (string)
            MaxPasses        (int)

    Otherwise, no output on success.

.EXAMPLES
    # 1) Basic: script from source and deploy to destination
    Invoke-DbaClonePkFk `
        -SourceInstance "ISI-M0BDRS31" -SourceDatabase "AppDB" `
        -DestInstance   "ISI-M0BDRS31" -DestDatabase   "AppDB_Clone" `
        -IdxPkFkScriptPath "C:\temp\clone\AppDB-idx-pk-fk.sql"

    # 2) Limit to dbo schema and a handful of tables, return a summary
    Invoke-DbaClonePkFk `
        -SourceInstance "SQL01" -SourceDatabase "Sales" `
        -DestInstance   "SQL02" -DestDatabase   "Sales_Stage" `
        -Schema 'dbo' `
        -IncludeTable 'dbo.Customers','dbo.Orders' `
        -IdxPkFkScriptPath "D:\scripts\Sales-idxpkfk.sql" `
        -PassThru

    # 3) Exclude specific tables and keep a log
    Invoke-DbaClonePkFk `
        -SourceInstance "SQL01" -SourceDatabase "HR" `
        -DestInstance   "SQL02" -DestDatabase   "HR_Test" `
        -ExcludeTable 'dbo.Audit','dbo.Logs' `
        -IdxPkFkScriptPath "C:\out\HR-idxpkfk.sql" `
        -LogFileName "C:\out\HR-idxpkfk.log"

    # 4) Dry run (see what would be done without executing on destination)
    Invoke-DbaClonePkFk `
        -SourceInstance "SQL01" -SourceDatabase "AppDB" `
        -DestInstance   "SQL02" -DestDatabase   "AppDB_QA" `
        -IdxPkFkScriptPath "C:\out\AppDB-idxpkfk.sql" `
        -WhatIf

    # 5) Increase retry passes for flaky environments
    Invoke-DbaClonePkFk `
        -SourceInstance "SQL01" -SourceDatabase "Ops" `
        -DestInstance   "SQL02" -DestDatabase   "Ops_QA" `
        -IdxPkFkScriptPath "C:\out\Ops-idxpkfk.sql" `
        -MaxPasses 20

.NOTES
    Requirements:
      - dbatools module (Get-DbaDbTable, New-DbaScriptingOption, Export-DbaScript).
      - A Write-Log function that accepts -Message, -Level, -LogFileName (as used here).
      - A helper Invoke-DbaExecuteBatchWithRetry that takes:
            -SqlInstance, -Database, -FilePath, -MaxPasses, -LogFile
        and executes the script with retries.
      - Sufficient permissions on source (to read metadata) and destination (to create indexes/PK/FKs).

    Behavior details:
      - Index selection keeps PK indexes and normal (non-constraint) indexes; UNIQUE constraints are skipped.
      - Foreign keys from all selected tables are included.
      - Scripting options:
            NoCollation=$true, IncludeIfNotExists=$false, AnsiFile=$true,
            ScriptSchema=$true, Indexes=$true, DriPrimaryKey=$true,
            DriForeignKeys=$true, DriUniqueKeys=$false, DriChecks=$false,
            DriDefaults=$false, Triggers=$false
      - The function supports -Verbose for additional console details.
      - Parent directory of -IdxPkFkScriptPath is created if missing.

    Error handling:
      - Errors are logged via Write-Log and rethrown for upstream handling.
      - If the script file already exists, it is removed before writing fresh content.

.LINK
    https://dbatools.io/
#>
	
	
    [CmdletBinding(SupportsShouldProcess)]
    param(
        [Parameter(Mandatory)][string]$SourceInstance,
        [Parameter(Mandatory)][string]$SourceDatabase,
        [Parameter(Mandatory)][string]$DestInstance,
        [Parameter(Mandatory)][string]$DestDatabase,

        [Parameter(Mandatory)][string]$IdxPkFkScriptPath,  # output .sql for indexes + PK + FK
        [int]$MaxPasses = 10,

        # Optional filters
        [string[]]$Schema,       # e.g. 'dbo','sales'
        [string[]]$IncludeTable, # 'dbo.Table1' or 'Table1'
        [string[]]$ExcludeTable,

        # Logging
        [string]$LogFileName,

        # Return a small summary object
        [switch]$PassThru
    )

    function _matchTable([string]$schema, [string]$name, [string[]]$patterns) {
        if (-not $patterns -or $patterns.Count -eq 0) { return $true }
        foreach ($p in $patterns) {
            if ($p -like '*.*') { if ("$schema.$name" -ieq $p) { return $true } }
            else                { if ($name -ieq $p)          { return $true } }
        }
        return $false
    }

    # Ensure output dir exists
    $dir = Split-Path -Path $IdxPkFkScriptPath -Parent
    if ($dir -and -not (Test-Path -LiteralPath $dir)) { New-Item -ItemType Directory -Force -Path $dir | Out-Null }

    try {
        Write-Log -Message "Exporting Indexes + PK + FK (no table creation)" -Level Info -LogFileName $LogFileName -Verbose

        # Get SMO Table objects
        $tables = Get-DbaDbTable -SqlInstance $SourceInstance -Database $SourceDatabase
        if ($Schema)       { $tables = $tables | Where-Object { $Schema -contains $_.Schema } }
        if ($IncludeTable) { $tables = $tables | Where-Object { _matchTable $_.Schema $_.Name $IncludeTable } }
        if ($ExcludeTable) { $tables = $tables | Where-Object { -not (_matchTable $_.Schema $_.Name $ExcludeTable) } }

        # Collect Indexes (incl. PK indexes; exclude UNIQUE constraints) and FKs
        $indexObjects = @()
        $fkObjects    = @()

        foreach ($t in $tables) {
            foreach ($ix in $t.Indexes) {
                # Is PK?
                $isPk = $false
                if ($ix.PSObject.Properties.Name -contains 'IndexKeyType') {
                    $isPk = ($ix.IndexKeyType -eq [Microsoft.SqlServer.Management.Smo.IndexKeyType]::DriPrimaryKey -or
                             "$($ix.IndexKeyType)" -eq 'DriPrimaryKey')
                } else {
                    $isPk = ($ix.Name -like 'PK_%')  # fallback heuristic
                }

                # Is UNIQUE constraint (not a normal unique index)?
                $isUniqueConstraint = $false
                if ($ix.PSObject.Properties.Name -contains 'IsUniqueConstraint') {
                    $isUniqueConstraint = [bool]$ix.IsUniqueConstraint
                } elseif ($ix.PSObject.Properties.Name -contains 'IndexKeyType') {
                    $isUniqueConstraint = ($ix.IndexKeyType -eq [Microsoft.SqlServer.Management.Smo.IndexKeyType]::DriUniqueKey -or
                                           "$($ix.IndexKeyType)" -eq 'DriUniqueKey')
                }

                # Keep PK indexes and regular (non-constraint) indexes; drop UNIQUE constraints
                if ($isPk -or -not $isUniqueConstraint) {
                    $indexObjects += $ix
                }
            }

            foreach ($fk in $t.ForeignKeys) { $fkObjects += $fk }
        }

        # Scripting options – only child objects (no CREATE TABLE)
        $opt = New-DbaScriptingOption
        $opt.NoCollation        = $true
        $opt.IncludeIfNotExists = $false
        $opt.AnsiFile           = $true
        $opt.ScriptSchema       = $true
        $opt.Indexes            = $true
        $opt.DriPrimaryKey      = $true
        $opt.DriForeignKeys     = $true
        $opt.DriUniqueKeys      = $false
        $opt.DriChecks          = $false
        $opt.DriDefaults        = $false
        $opt.Triggers           = $false

        # Export ONLY index + FK objects
        if (Test-Path $IdxPkFkScriptPath) { Remove-Item $IdxPkFkScriptPath -Force -ErrorAction SilentlyContinue }
        if ($indexObjects.Count -gt 0) {
            $indexObjects | Export-DbaScript -FilePath $IdxPkFkScriptPath -Append:$false `
                -ScriptingOptionsObject $opt -EnableException -Confirm:$false -Verbose *>> $LogFileName
        }
        if ($fkObjects.Count -gt 0) {
            $fkObjects | Export-DbaScript -FilePath $IdxPkFkScriptPath -Append:($indexObjects.Count -gt 0) `
                -ScriptingOptionsObject $opt -EnableException -Confirm:$false -Verbose *>> $LogFileName
        }

        Write-Log -Message "Index/PK/FK script: $IdxPkFkScriptPath (Indexes=$($indexObjects.Count), FKs=$($fkObjects.Count))" -Level Info -LogFileName $LogFileName -Verbose

        # Deploy with retry
        if ($PSCmdlet.ShouldProcess("$DestInstance / $DestDatabase", "Deploy Indexes + PK + FK")) {
            Write-Log -Message "Deploying Indexes + PK + FK into $DestDatabase on $DestInstance" -Level Important -LogFileName $LogFileName
            [void](Invoke-DbaExecuteBatchWithRetry -SqlInstance $DestInstance -Database $DestDatabase -FilePath $IdxPkFkScriptPath -MaxPasses $MaxPasses -LogFile $LogFileName)
        }

        Write-Log -Message "Indexes + PK + FK deployment completed." -Level Alert -LogFileName $LogFileName -Verbose

        if ($PassThru) {
            [pscustomobject]@{
                Source          = "$SourceInstance.$SourceDatabase"
                Destination     = "$DestInstance.$DestDatabase"
                TablesConsidered= $tables.Count
                IndexesScripted = $indexObjects.Count
                FKsScripted     = $fkObjects.Count
                ScriptPath      = $IdxPkFkScriptPath
                MaxPasses       = $MaxPasses
            }
        }
    }
    catch {
        Write-Log -Message ("ERROR (Invoke-DbaClonePkFk): " + $_.Exception.Message) -Level Error -LogFileName $LogFileName
        throw
    }
}
