function Invoke-DbaCloneConstraint {
<#
.SYNOPSIS
    Scripts and deploys ONLY table constraints (FK, UNIQUE, CHECK, DEFAULT) from a source database
    to a destination database. Primary keys and non-constraint indexes are excluded.

.DESCRIPTION
    Invoke-DbaCloneConstraint enumerates tables in the source database (optionally filtered),
    collects constraint objects (Foreign Keys, UNIQUE constraints, CHECK constraints, and DEFAULT
    constraints), scripts them into a single .sql file, and then (optionally) deploys the script
    to the destination database with a retry mechanism.

    Key behaviors:
      - EXCLUDES Primary Keys and non-constraint (regular) indexes.
      - INCLUDES Foreign Keys, UNIQUE constraints, CHECK constraints, and DEFAULT constraints.
      - Parent directory for -ConstraintsScriptPath is created if missing.
      - Existing script at -ConstraintsScriptPath is overwritten.
      - Supports -WhatIf and -Confirm via SupportsShouldProcess.
      - Use -PassThru to get a summary object (counts and paths).

.PARAMETER SourceInstance
    SQL Server instance name for the source (e.g. 'SQL01' or 'SQL01\INST1').

.PARAMETER SourceDatabase
    Name of the source database that contains the constraints to script.

.PARAMETER DestInstance
    SQL Server instance name for the destination where constraints will be deployed.

.PARAMETER DestDatabase
    Name of the destination database. Target tables must already exist.

.PARAMETER ConstraintsScriptPath
    Full path to the output .sql file for the scripted constraints (FK, UNIQUE, CHECK, DEFAULT).
    The directory is created if it does not exist. Existing file is overwritten.

.PARAMETER MaxPasses
    Maximum number of retry passes when deploying the script (used by Invoke-DbaExecuteBatchWithRetry).
    Default: 10.

.PARAMETER IncludeTable
    Optional list of table filters. Each entry can be:
      - 'schema.table' for an exact schema-qualified match, or
      - 'table' for an exact table-name match (case-insensitive).
    If omitted, all tables are eligible (subject to -ExcludeTable).

.PARAMETER ExcludeTable
    Optional list of tables to exclude, using the same matching rules as -IncludeTable.

.PARAMETER LogFileName
    Optional log file path. Verbose streams and Write-Log messages are appended here.

.PARAMETER PassThru
    When specified, returns a [pscustomobject] with counts and paths.

.INPUTS
    None. All inputs are provided via parameters.

.OUTPUTS
    If -PassThru is used, outputs a PSCustomObject with:
        Source            (string) -> "SourceInstance.SourceDatabase"
        Destination       (string) -> "DestInstance.DestDatabase"
        TablesConsidered  (int)
        CountFK           (int)
        CountUnique       (int)
        CountCheck        (int)
        CountDefault      (int)
        ScriptPath        (string)
        MaxPasses         (int)

    Otherwise, no output on success.

.EXAMPLES
    # 1) Script constraints and deploy to destination
    Invoke-DbaCloneConstraint `
        -SourceInstance "SQL01" -SourceDatabase "AppDB" `
        -DestInstance   "SQL02" -DestDatabase   "AppDB_Clone" `
        -ConstraintsScriptPath "C:\temp\AppDB-constraints.sql"

    # 2) Include a subset of tables and return summary
    Invoke-DbaCloneConstraint `
        -SourceInstance "SQL01" -SourceDatabase "Sales" `
        -DestInstance   "SQL02" -DestDatabase   "Sales_Stage" `
        -IncludeTable 'dbo.Customers','dbo.Orders' `
        -ConstraintsScriptPath "D:\out\Sales-constraints.sql" `
        -PassThru

    # 3) Exclude large or transient tables and log to file
    Invoke-DbaCloneConstraint `
        -SourceInstance "Prod\OLTP" -SourceDatabase "HR" `
        -DestInstance   "Stage\OLTP" -DestDatabase   "HR_Stage" `
        -ExcludeTable 'dbo.Audit','dbo.ChangeLog' `
        -ConstraintsScriptPath "E:\scripts\HR-constraints.sql" `
        -LogFileName "E:\logs\HR-constraints.log"

    # 4) Dry run (generate script only; do not deploy)
    Invoke-DbaCloneConstraint `
        -SourceInstance "SQL01" -SourceDatabase "AppDB" `
        -DestInstance   "SQL02" -DestDatabase   "AppDB_QA" `
        -ConstraintsScriptPath "C:\out\AppDB-constraints.sql" `
        -WhatIf

    # 5) Increase retry passes for fragile environments
    Invoke-DbaCloneConstraint `
        -SourceInstance "SQL01" -SourceDatabase "Ops" `
        -DestInstance   "SQL02" -DestDatabase   "Ops_QA" `
        -ConstraintsScriptPath "C:\out\Ops-constraints.sql" `
        -MaxPasses 20

.NOTES
    Requirements:
      - dbatools module:
            Get-DbaDbTable
            New-DbaScriptingOption
            Export-DbaScript
      - A Write-Log function supporting: -Message, -Level, -LogFileName.
      - Helper: Invoke-DbaExecuteBatchWithRetry
            Parameters used: -SqlInstance, -Database, -FilePath, -MaxPasses, -LogFile
      - Permissions:
            Source: metadata read on tables/constraints.
            Destination: ALTER TABLE / ADD CONSTRAINT privileges as applicable.

    Scripting options used:
        NoCollation        = $true
        IncludeIfNotExists = $false
        AnsiFile           = $true
        ScriptSchema       = $true
        Indexes            = $false
        DriPrimaryKey      = $false
        DriForeignKeys     = $true
        DriUniqueKeys      = $true
        DriChecks          = $true
        DriDefaults        = $true
        Triggers           = $false

    Behavior & ordering:
      - Objects are grouped and scripted in this order: UNIQUE -> CHECK -> DEFAULT -> FK.
        (FKs are last to reduce dependency failures.)
      - Existing -ConstraintsScriptPath is removed before fresh writing.
      - The function supports -Verbose for additional console output.

    Error handling:
      - Errors are logged via Write-Log and rethrown for upstream handling.

.LINK
    https://dbatools.io/
#>

	
    [CmdletBinding(SupportsShouldProcess)]
    param(
        [Parameter(Mandatory)][string]$SourceInstance,
        [Parameter(Mandatory)][string]$SourceDatabase,
        [Parameter(Mandatory)][string]$DestInstance,
        [Parameter(Mandatory)][string]$DestDatabase,

        [Parameter(Mandatory)][string]$ConstraintsScriptPath,  # output .sql (FK, UNIQUE, CHECK, DEFAULT only)
        [int]$MaxPasses = 10,

        # Optional filters
        [string[]]$IncludeTable,   # 'dbo.Table1' or 'Table1'
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

    # Ensure output directory exists
    $dir = Split-Path -Path $ConstraintsScriptPath -Parent
    if ($dir -and -not (Test-Path -LiteralPath $dir)) { New-Item -ItemType Directory -Force -Path $dir | Out-Null }

    try {
        Write-Log -Message "Exporting ONLY constraints (FK, UNIQUE, CHECK, DEFAULT) — excluding PK and non-constraint indexes" -Level Info -LogFileName $LogFileName -Verbose

        # Get tables and apply filters
        $tables = Get-DbaDbTable -SqlInstance $SourceInstance -Database $SourceDatabase
        if ($IncludeTable) { $tables = $tables | Where-Object { _matchTable $_.Schema $_.Name $IncludeTable } }
        if ($ExcludeTable) { $tables = $tables | Where-Object { -not (_matchTable $_.Schema $_.Name $ExcludeTable) } }

        # Collect constraint objects
        $fkObjects      = @()
        $uniqueObjects  = @()
        $checkObjects   = @()
        $defaultObjects = @()

        foreach ($t in $tables) {

            # --- Foreign Keys ---
            foreach ($fk in $t.ForeignKeys) { $fkObjects += $fk }

            # --- UNIQUE constraints (not unique indexes) ---
            foreach ($ix in $t.Indexes) {
                if ($ix.PSObject.Properties.Name -contains 'IndexKeyType') {
                    if ($ix.IndexKeyType -eq [Microsoft.SqlServer.Management.Smo.IndexKeyType]::DriUniqueKey -or
                        "$($ix.IndexKeyType)" -eq 'DriUniqueKey') {
                        $uniqueObjects += $ix
                    }
                } elseif ($ix.PSObject.Properties.Name -contains 'IsUniqueConstraint') {
                    if ($ix.IsUniqueConstraint) { $uniqueObjects += $ix }
                }
            }

            # --- CHECK constraints ---
            if ($t.PSObject.Properties.Name -contains 'Checks') {
                foreach ($ck in $t.Checks) { $checkObjects += $ck }
            }

            # --- DEFAULT constraints ---
            foreach ($col in $t.Columns) {
                if ($col.DefaultConstraint) { $defaultObjects += $col.DefaultConstraint }
            }
        }

        Write-Log -Message ("Constraints found → FK:{0}  UNIQUE:{1}  CHECK:{2}  DEFAULT:{3}" -f `
            $fkObjects.Count, $uniqueObjects.Count, $checkObjects.Count, $defaultObjects.Count) -Level Info -LogFileName $LogFileName -Verbose

        # Scripting options
        $opt = New-DbaScriptingOption
        $opt.NoCollation        = $true
        $opt.IncludeIfNotExists = $false
        $opt.AnsiFile           = $true
        $opt.ScriptSchema       = $true
        $opt.Indexes            = $false
        $opt.DriPrimaryKey      = $false
        $opt.DriForeignKeys     = $true
        $opt.DriUniqueKeys      = $true
        $opt.DriChecks          = $true
        $opt.DriDefaults        = $true
        $opt.Triggers           = $false

        # Export only constraints
        if (Test-Path $ConstraintsScriptPath) { Remove-Item $ConstraintsScriptPath -Force -ErrorAction SilentlyContinue }

        $append = $false
        foreach ($group in @($uniqueObjects, $checkObjects, $defaultObjects, $fkObjects)) {
            if ($group.Count -gt 0) {
                $group | Export-DbaScript -FilePath $ConstraintsScriptPath -Append:$append `
                    -ScriptingOptionsObject $opt -EnableException -Confirm:$false -Verbose *>> $LogFileName
                $append = $true
            }
        }

        Write-Log -Message "Constraints script written: $ConstraintsScriptPath" -Level Info -LogFileName $LogFileName -Verbose

        # Deploy with retry
        if ($PSCmdlet.ShouldProcess("$DestInstance / $DestDatabase", "Deploy constraints (FK, UNIQUE, CHECK, DEFAULT)")) {
            Write-Log -Message "Deploying constraints into $DestDatabase on $DestInstance" -Level Important -LogFileName $LogFileName
            [void](Invoke-DbaExecuteBatchWithRetry -SqlInstance $DestInstance -Database $DestDatabase -FilePath $ConstraintsScriptPath -MaxPasses $MaxPasses -LogFile $LogFileName)
        }

        Write-Log -Message "Constraints deployment completed." -Level Alert -LogFileName $LogFileName -Verbose

        if ($PassThru) {
            [pscustomobject]@{
                Source           = "$SourceInstance.$SourceDatabase"
                Destination      = "$DestInstance.$DestDatabase"
                TablesConsidered = $tables.Count
                CountFK          = $fkObjects.Count
                CountUnique      = $uniqueObjects.Count
                CountCheck       = $checkObjects.Count
                CountDefault     = $defaultObjects.Count
                ScriptPath       = $ConstraintsScriptPath
                MaxPasses        = $MaxPasses
            }
        }
    }
    catch {
        Write-Log -Message ("ERROR (Invoke-DbaCloneConstraint): " + $_.Exception.Message) -Level Error -LogFileName $LogFileName
        throw
    }
}
