function Invoke-DbaClonePrimaryKey {
<#
.SYNOPSIS
    Scripts and deploys ONLY Primary Keys (PK) from a source database to a destination database.

.DESCRIPTION
    Invoke-DbaClonePrimaryKey enumerates tables in the source database (optionally filtered),
    identifies PKs reliably across SMO versions, scripts ONLY the PK definitions to a .sql file,
    and (optionally) deploys them to the destination database with a retry mechanism.

    Key behaviors:
      - EXCLUDES foreign keys, UNIQUE constraints, non-constraint indexes, CHECK/DEFAULT constraints, triggers.
      - Uses SMO IndexKeyType when available; falls back to a heuristic (IsUnique + 'PK_%' name) for older SMO.
      - Creates the parent directory for -ScriptPath when missing, overwrites existing file.
      - Supports -WhatIf / -Confirm via SupportsShouldProcess.
      - Use -PassThru to return a summary object (counts and paths).

.PARAMETER SourceInstance
    SQL Server instance hosting the source database (e.g., 'SQL01' or 'SQL01\INST1').

.PARAMETER SourceDatabase
    Name of the source database to read PK definitions from.

.PARAMETER DestInstance
    SQL Server instance hosting the destination database.

.PARAMETER DestDatabase
    Name of the destination database. Target tables must already exist.

.PARAMETER ScriptPath
    Full path to the output .sql file that will contain ONLY PK definitions.
    The directory is created if needed; an existing file is overwritten.

.PARAMETER MaxPasses
    Maximum number of retry passes for the deployment executor (Invoke-DbaExecuteBatchWithRetry).
    Default: 10.

.PARAMETER Schema
    Optional list of schema names to include (exact, case-insensitive), e.g. 'dbo','sales'.
    If omitted, all schemas are considered.

.PARAMETER IncludeTable
    Optional list of table filters. Each entry can be:
      - 'schema.table' for an exact schema-qualified match, or
      - 'table' for an exact (case-insensitive) table-name match across any schema.

.PARAMETER ExcludeTable
    Optional list of tables to exclude using the same matching rules as -IncludeTable.

.PARAMETER PassThru
    When specified, returns a [pscustomobject] with counts and paths.

.INPUTS
    None. Parameters are provided by the caller.

.OUTPUTS
    If -PassThru is provided, outputs a PSCustomObject:
        Source           (string) -> "SourceInstance.SourceDatabase"
        Destination      (string) -> "DestInstance.DestDatabase"
        TablesConsidered (int)
        PksScripted      (int)
        ScriptPath       (string)
        MaxPasses        (int)

    Otherwise, no output on success.

.EXAMPLES
    # 1) Script and deploy all PKs
    Invoke-DbaClonePrimaryKey `
        -SourceInstance "SQL01" -SourceDatabase "AppDB" `
        -DestInstance   "SQL02" -DestDatabase   "AppDB_Clone" `
        -ScriptPath "C:\out\AppDB-PKs.sql"

    # 2) Limit to dbo schema and a subset of tables; return summary
    Invoke-DbaClonePrimaryKey `
        -SourceInstance "SQL01" -SourceDatabase "Sales" `
        -DestInstance   "SQL02" -DestDatabase   "Sales_Stage" `
        -Schema 'dbo' `
        -IncludeTable 'dbo.Customers','dbo.Orders' `
        -ScriptPath "D:\scripts\Sales-PKs.sql" `
        -PassThru

    # 3) Exclude transient tables and log to file
    Invoke-DbaClonePrimaryKey `
        -SourceInstance "Prod\OLTP" -SourceDatabase "HR" `
        -DestInstance   "Stage\OLTP" -DestDatabase   "HR_Stage" `
        -ExcludeTable 'dbo.Audit','dbo.ChangeLog' `
        -ScriptPath "E:\out\HR-PKs.sql" `

    # 4) Dry run (generate script only, do not deploy)
    Invoke-DbaClonePrimaryKey `
        -SourceInstance "SQL01" -SourceDatabase "AppDB" `
        -DestInstance   "SQL02" -DestDatabase   "AppDB_QA" `
        -ScriptPath "C:\out\AppDB-PKs.sql" `
        -WhatIf

    # 5) Increase deployment retries for flaky networks
    Invoke-DbaClonePrimaryKey `
        -SourceInstance "SQL01" -SourceDatabase "Ops" `
        -DestInstance   "SQL02" -DestDatabase   "Ops_QA" `
        -ScriptPath "C:\out\Ops-PKs.sql" `
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
            Source: metadata read on tables/indexes.
            Destination: ALTER TABLE / ADD CONSTRAINT privileges.

    Scripting options used:
        NoCollation        = $true
        IncludeIfNotExists = $true
        AnsiFile           = $true
        ScriptSchema       = $true
        Indexes            = $true
        DriPrimaryKey      = $true
        DriForeignKeys     = $false
        DriUniqueKeys      = $false
        DriChecks          = $false
        DriDefaults        = $false
        Triggers           = $false

    Behavior & detection:
      - Prefers SMO IndexKeyType == DriPrimaryKey; falls back to ($ix.IsUnique -and name like 'PK_%') if IndexKeyType is missing.
      - Existing -ScriptPath is removed before writing a fresh file.
      - The function supports -Verbose for additional console output.

    Error handling:
      - (Optional) try/catch can wrap the entire function; inner code logs via Write-Log.
      - Deployment uses Invoke-DbaExecuteBatchWithRetry for robustness.

.LINK
    https://dbatools.io/
#>
	
    [CmdletBinding(SupportsShouldProcess)]
    param(
        [Parameter(Mandatory)][string]$SourceInstance,
        [Parameter(Mandatory)][string]$SourceDatabase,
        [Parameter(Mandatory)][string]$DestInstance,
        [Parameter(Mandatory)][string]$DestDatabase,

        [Parameter(Mandatory)][string]$ScriptPath,   # output .sql for PKs only
        [int]$MaxPasses = 10,

        # Optional filters
        [string[]]$Schema,
        [string[]]$IncludeTable,
        [string[]]$ExcludeTable,

        [switch]$PassThru
    )

    function _matchTable([string]$schema,[string]$name,[string[]]$patterns) {
        if (-not $patterns -or $patterns.Count -eq 0) { return $true }
        foreach ($p in $patterns) {
            if ($p -like '*.*') { if ("$schema.$name" -ieq $p) { return $true } }
            else                { if ($name -ieq $p)          { return $true } }
        }
        return $false
    }

    $LogFile = if ($Global:WriteLog_LogFileName) { $Global:WriteLog_LogFileName } else { $null }

    # Ensure output dir exists
    $dir = Split-Path -Path $ScriptPath -Parent
    if ($dir -and -not (Test-Path -LiteralPath $dir)) {
        New-Item -ItemType Directory -Force -Path $dir | Out-Null
    }

    #try {
        Write-Log -Message "Exporting ONLY Primary Keys (no table creation)" -Level Info  

        # Collect tables
        $tables = Get-DbaDbTable -SqlInstance $SourceInstance -Database $SourceDatabase
        if ($Schema)       { $tables = $tables | Where-Object { $Schema -contains $_.Schema } }
        if ($IncludeTable) { $tables = $tables | Where-Object { _matchTable $_.Schema $_.Name $IncludeTable } }
        if ($ExcludeTable) { $tables = $tables | Where-Object { -not (_matchTable $_.Schema $_.Name $ExcludeTable) } }

        # Collect PK indexes by IndexKeyType
        $pkIndexes = @()
        foreach ($t in $tables) {
            foreach ($ix in $t.Indexes) {
                # Works across SMO versions:
                if ($ix.PSObject.Properties.Name -contains 'IndexKeyType') {
                    if ($ix.IndexKeyType -eq [Microsoft.SqlServer.Management.Smo.IndexKeyType]::DriPrimaryKey -or
                        "$($ix.IndexKeyType)" -eq 'DriPrimaryKey') {
                        $pkIndexes += $ix
                    }
                } else {
                    # Fallback heuristics if IndexKeyType missing (older SMO): PKs are unique & usually system-named 'PK_%'
                    if ($ix.IsUnique -and ($ix.Name -like 'PK_%')) { $pkIndexes += $ix }
                }
            }
        }

        Write-Log -Message "Found $($pkIndexes.Count) primary keys to script." -Level Info  

        # Scripting options: just PKs
        $opt = New-DbaScriptingOption
        $opt.NoCollation        = $true
        $opt.IncludeIfNotExists = $false
        $opt.AnsiFile           = $true
        $opt.ScriptSchema       = $true
        $opt.Indexes            = $true          # needed to emit PK index/constraint
        $opt.DriPrimaryKey      = $true

        # Everything else OFF
        $opt.DriForeignKeys     = $false
        $opt.DriUniqueKeys      = $false
        $opt.DriChecks          = $false
        $opt.DriDefaults        = $false
        $opt.Triggers           = $false

        # Export only PKs (no tables piped!)
        if (Test-Path $ScriptPath) { Remove-Item $ScriptPath -Force -ErrorAction SilentlyContinue }
        if ($pkIndexes.Count -gt 0) {
            $pkIndexes | Export-DbaScript -FilePath $ScriptPath -Append:$false `
                -ScriptingOptionsObject $opt -EnableException -Confirm:$false -Verbose *>> $LogFile
        }

        Write-Log -Message "Primary Key script written: $ScriptPath" -Level Info  

        # Deploy with retry
        if ($PSCmdlet.ShouldProcess("$DestInstance / $DestDatabase", "Deploy Primary Keys")) {
            Write-Log -Message "Deploying Primary Keys into $DestDatabase on $DestInstance" -Level Warning  
            [void](Invoke-DbaExecuteBatchWithRetry -SqlInstance $DestInstance -Database $DestDatabase -FilePath $ScriptPath -MaxPasses $MaxPasses)
        }

        Write-Log -Message "Primary Key deployment completed." -Level Warning  

        if ($PassThru) {
            [pscustomobject]@{
                Source          = "$SourceInstance.$SourceDatabase"
                Destination     = "$DestInstance.$DestDatabase"
                TablesConsidered= $tables.Count
                PksScripted     = $pkIndexes.Count
                ScriptPath      = $ScriptPath
                MaxPasses       = $MaxPasses
            }
        }
    #}
    #catch {
    #    Write-Log -Message ("ERROR (Invoke-DbaClonePrimaryKey): " + $_.Exception.Message) -Level Error -LogFileName $LogFile
    #    throw
    #}
}
