function Invoke-DbaCloneIndexes {
<#
.SYNOPSIS
    Scripts and deploys ONLY non-constraint indexes from a source database to a destination database.

.DESCRIPTION
    Invoke-DbaCloneIndexes enumerates tables on the source database (optionally filtered),
    collects ONLY non-constraint indexes (i.e., excludes Primary Key indexes and UNIQUE constraints),
    scripts them to a single .sql file, and (optionally) deploys them to the destination database.
    No CREATE TABLE statements are generated or executed.

    Key behaviors:
      - Primary Key indexes are excluded.
      - UNIQUE constraints are excluded (true non-constraint indexes are included, even if unique).
      - The script folder is created if it doesn’t exist; any existing script at -ScriptPath is overwritten.
      - Supports -WhatIf/-Confirm via SupportsShouldProcess.
      - Use -PassThru to return a summary object with counts and paths.

.PARAMETER SourceInstance
    SQL Server instance that hosts the source database (e.g. 'SQL01' or 'SQL01\INST1').

.PARAMETER SourceDatabase
    Name of the source database from which indexes will be scripted.

.PARAMETER DestInstance
    SQL Server instance that hosts the destination database where indexes will be deployed.

.PARAMETER DestDatabase
    Name of the destination database. Tables must already exist.

.PARAMETER ScriptPath
    Full path to the output .sql file containing ONLY non-constraint index statements.
    The parent directory is created when missing; an existing file is overwritten.

.PARAMETER MaxPasses
    Maximum number of retry passes for deployment (used by Invoke-DbaExecuteBatchWithRetry).
    Default: 10.

.PARAMETER Schema
    Optional list of schema names to include (exact, case-insensitive). Example: 'dbo','sales'.
    If omitted, all schemas are considered.

.PARAMETER IncludeTable
    Optional list of table filters. Each item can be:
      - 'schema.table' for an exact schema-qualified match, or
      - 'table' for an exact match by table name (case-insensitive).
    If omitted, all tables are eligible (subject to -Schema/-ExcludeTable).

.PARAMETER ExcludeTable
    Optional list of tables to exclude using the same matching rules as -IncludeTable.

.PARAMETER LogFileName
    Optional log file path. Verbose streams and messages from Write-Log will be appended here.

.PARAMETER PassThru
    When specified, returns a summary [pscustomobject] with counts and output path.

.INPUTS
    None. Parameters are provided by the caller.

.OUTPUTS
    If -PassThru is set, outputs a PSCustomObject:
        Source           (string) -> "SourceInstance.SourceDatabase"
        Destination      (string) -> "DestInstance.DestDatabase"
        TablesConsidered (int)
        IndexesScripted  (int)
        ScriptPath       (string)
        MaxPasses        (int)

    Otherwise, no output on success.

.EXAMPLES
    # 1) Basic: script indexes and deploy to destination
    Invoke-DbaCloneIndexes `
        -SourceInstance "SQL01" -SourceDatabase "AppDB" `
        -DestInstance   "SQL02" -DestDatabase   "AppDB_Clone" `
        -ScriptPath "C:\temp\AppDB-indexes.sql"

    # 2) Limit to dbo schema, include specific tables, return a summary
    Invoke-DbaCloneIndexes `
        -SourceInstance "SQL01" -SourceDatabase "Sales" `
        -DestInstance   "SQL02" -DestDatabase   "Sales_Stage" `
        -Schema 'dbo' `
        -IncludeTable 'dbo.Orders','dbo.OrderLines' `
        -ScriptPath "D:\out\Sales-indexes.sql" `
        -PassThru

    # 3) Exclude large audit tables  
    Invoke-DbaCloneIndexes `
        -SourceInstance "Prod\OLTP" -SourceDatabase "HR" `
        -DestInstance   "Stage\OLTP" -DestDatabase   "HR_Stage" `
        -ExcludeTable 'dbo.Audit','dbo.ChangeLog' `
        -ScriptPath "E:\scripts\HR-indexes.sql" `
         

    # 4) Dry run (script only, do not deploy)
    Invoke-DbaCloneIndexes `
        -SourceInstance "SQL01" -SourceDatabase "AppDB" `
        -DestInstance   "SQL02" -DestDatabase   "AppDB_QA" `
        -ScriptPath "C:\out\AppDB-indexes.sql" `
        -WhatIf

    # 5) Increase deployment retries for flaky networks
    Invoke-DbaCloneIndexes `
        -SourceInstance "SQL01" -SourceDatabase "Ops" `
        -DestInstance   "SQL02" -DestDatabase   "Ops_QA" `
        -ScriptPath "C:\out\Ops-indexes.sql" `
        -MaxPasses 20

.NOTES
    Requirements:
      - dbatools module:
            Get-DbaDbTable
            New-DbaScriptingOption
            Export-DbaScript
      - A Write-Log function that supports: -Message, -Level, -LogFileName.
      - Helper: Invoke-DbaExecuteBatchWithRetry
            Parameters used: -SqlInstance, -Database, -FilePath, -MaxPasses, -LogFile
      - Permissions: metadata read on source; CREATE/ALTER INDEX privileges on destination.

    Scripting options used:
        NoCollation=$true
        IncludeIfNotExists=$false
        AnsiFile=$true
        ScriptSchema=$true
        Indexes=$true
        DriPrimaryKey=$false
        DriForeignKeys=$false
        DriUniqueKeys=$false
        DriChecks=$false
        DriDefaults=$false
        Triggers=$false

    Error handling:
      - Exceptions are logged via Write-Log and rethrown.
      - Existing -ScriptPath is removed before writing a fresh file.

.LINK
    https://dbatools.io/
#>

	
    [CmdletBinding(SupportsShouldProcess)]
    param(
        [Parameter(Mandatory)][string]$SourceInstance,
        [Parameter(Mandatory)][string]$SourceDatabase,
        [Parameter(Mandatory)][string]$DestInstance,
        [Parameter(Mandatory)][string]$DestDatabase,

        [Parameter(Mandatory)][string]$ScriptPath,   # output .sql for indexes ONLY
        [int]$MaxPasses = 10,

        # Optional filters
        [string[]]$Schema,         # e.g. 'dbo','sales'
        [string[]]$IncludeTable,   # 'dbo.Table1' or 'Table1'
        [string[]]$ExcludeTable,

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

    $LogFileName = if ($Global:WriteLog_LogFileName) { $Global:WriteLog_LogFileName } else { $null }

    # Ensure output dir exists
    $dir = Split-Path -Path $ScriptPath -Parent
    if ($dir -and -not (Test-Path -LiteralPath $dir)) { New-Item -ItemType Directory -Force -Path $dir | Out-Null }

    try {
        Write-Log -Message "Exporting ONLY non-constraint indexes (exclude PK and UNIQUE constraints)" -Level Info 

        # Get tables (SMO objects) and apply filters
        $tables = Get-DbaDbTable -SqlInstance $SourceInstance -Database $SourceDatabase
        if ($Schema)       { $tables = $tables | Where-Object { $Schema -contains $_.Schema } }
        if ($IncludeTable) { $tables = $tables | Where-Object { _matchTable $_.Schema $_.Name $IncludeTable } }
        if ($ExcludeTable) { $tables = $tables | Where-Object { -not (_matchTable $_.Schema $_.Name $ExcludeTable) } }

        # Collect ONLY non-constraint indexes
        $indexObjects = @()
        foreach ($t in $tables) {
            foreach ($ix in $t.Indexes) {
                # exclude primary keys
                $isPk = $false
                if ($ix.PSObject.Properties.Name -contains 'IndexKeyType') {
                    $isPk = ($ix.IndexKeyType -eq [Microsoft.SqlServer.Management.Smo.IndexKeyType]::DriPrimaryKey -or
                             "$($ix.IndexKeyType)" -eq 'DriPrimaryKey')
                } else {
                    $isPk = ($ix.Name -like 'PK_%')
                }

                # exclude UNIQUE constraints (keep true indexes that are unique but NOT constraints)
                $isUniqueConstraint = $false
                if ($ix.PSObject.Properties.Name -contains 'IsUniqueConstraint') {
                    $isUniqueConstraint = [bool]$ix.IsUniqueConstraint
                } elseif ($ix.PSObject.Properties.Name -contains 'IndexKeyType') {
                    $isUniqueConstraint = ($ix.IndexKeyType -eq [Microsoft.SqlServer.Management.Smo.IndexKeyType]::DriUniqueKey -or
                                           "$($ix.IndexKeyType)" -eq 'DriUniqueKey')
                }

                if (-not $isPk -and -not $isUniqueConstraint) {
                    $indexObjects += $ix
                }
            }
        }

        Write-Log -Message "Found $($indexObjects.Count) indexes to script." -Level Info 

        # Scripting options: indexes only
        $opt = New-DbaScriptingOption
        $opt.NoCollation        = $true
        $opt.IncludeIfNotExists = $false
        $opt.AnsiFile           = $true
        $opt.ScriptSchema       = $true

        $opt.Indexes            = $true      # emit CREATE/ALTER INDEX
        # make sure constraint-related options are OFF
        $opt.DriPrimaryKey      = $false
        $opt.DriForeignKeys     = $false
        $opt.DriUniqueKeys      = $false
        $opt.DriChecks          = $false
        $opt.DriDefaults        = $false
        $opt.Triggers           = $false

        # Export only indexes (no tables piped!)
        if (Test-Path $ScriptPath) { Remove-Item $ScriptPath -Force -ErrorAction SilentlyContinue }
        if ($indexObjects.Count -gt 0) {
            $indexObjects | Export-DbaScript -FilePath $ScriptPath -Append:$false `
                -ScriptingOptionsObject $opt -EnableException -Confirm:$false -Verbose *>> $LogFileName
        }

        Write-Log -Message "Indexes-only script written: $ScriptPath" -Level Info  

        # Deploy with retry
        if ($PSCmdlet.ShouldProcess("$DestInstance / $DestDatabase", "Deploy indexes only")) {
            Write-Log -Message "Deploying indexes into $DestDatabase on $DestInstance" -Level Warning
            [void](Invoke-DbaExecuteBatchWithRetry -SqlInstance $DestInstance -Database $DestDatabase -FilePath $ScriptPath -MaxPasses $MaxPasses )
        }

        Write-Log -Message "Indexes-only deployment completed." -Level Warning 

        if ($PassThru) {
            [pscustomobject]@{
                Source          = "$SourceInstance.$SourceDatabase"
                Destination     = "$DestInstance.$DestDatabase"
                TablesConsidered= $tables.Count
                IndexesScripted = $indexObjects.Count
                ScriptPath      = $ScriptPath
                MaxPasses       = $MaxPasses
            }
        }
    }
    catch {
        Write-Log -Message ("ERROR (Invoke-DbaCloneIndexes): " + $_.Exception.Message) -Level Error
        throw
    }
}
