function Get-MSLinkedServerObjectInfo {
<#
.SYNOPSIS
    Analyzes the tables and views exposed by a linked server and returns one
    row per object with structural and size information.

.DESCRIPTION
    For every table/view on a linked server (read with the provider-agnostic
    sp_tables_ex / sp_columns_ex, so it works for SQL Server, Oracle, MySQL or
    file OLE DB sources) it reports:

      - the object list (schema, name, type, column count);
      - DUPLICATE column names (same name exposed more than once);
      - columns whose data type is INCOMPATIBLE to be selected/used over a
        linked server (LOB / CLR / provider-specific types, e.g. text, ntext,
        image, xml, sql_variant, geography, CLOB, BLOB, LONG, ...);
      - the ROW COUNT (counted locally to avoid wrong remote-aggregate pushdown;
        skip with -NoRowCount);
      - the maximum row size in bytes (sum of the DISTINCT columns' declared
        widths - a duplicate column name is counted only once);
      - an estimated total size (row count x max row size), when a row count is
        available.

    At the end it also prints the grand total (objects, rows, estimated bytes).

    The connection to -SqlInstance uses the current login (Integrated Security)
    unless -SqlCredential is supplied.

.PARAMETER SqlInstance
    SQL Server instance that hosts the linked server.

.PARAMETER LinkedServer
    Linked server to analyze.

.PARAMETER ObjectFilter
    Optional list of names to include (wildcards allowed). Each pattern is
    matched against the SCHEMA name, the object (table/view) name, and the
    "schema.name" form - so you can filter by schema, by object name, or both.
    Empty = all.

.PARAMETER NoRowCount
    Do not compute the row count / estimated total size (faster on large,
    remote objects).

.PARAMETER CheckpointPath
    JSON file used to save progress after every object. With -Continue this file
    is read to skip objects already analyzed and to retry the ones that failed.
    Defaults to a file in the temp folder named after the instance and linked
    server.

.PARAMETER Continue
    Resume from -CheckpointPath: keep objects already analyzed successfully
    (OK / Warning) and re-run the ones that previously FAILED (plus any new
    ones). By default the analysis starts from scratch and the old checkpoint is
    discarded.

.PARAMETER ThrottleLimit
    Number of objects to analyze in parallel (default 1 = serial). Each worker
    opens its own connection to -SqlInstance and runs the slow linked-server
    queries independently, which greatly speeds up servers with many objects.
    Values > 1 require PowerShell 7+ (uses ForEach-Object -Parallel); on Windows
    PowerShell 5.1 the analysis automatically falls back to serial. The
    checkpoint is still written from a single thread, so -Continue keeps working.

.PARAMETER QueryTimeoutSeconds
    Maximum seconds to wait for each linked-server query (column list and row
    count) before it is aborted. When a query times out or fails, the object is
    still returned as a record with Status = 'Error' / 'Warning' and a Message
    describing what happened, instead of hanging or being skipped. Default 60.
    Use 0 for no timeout (wait indefinitely - not recommended).

.PARAMETER SqlCredential
    Optional SQL credential for connecting to -SqlInstance. When omitted, the
    current login (Integrated Security) is used.

.OUTPUTS
    [pscustomobject] per object: LinkedServer, Type, Schema, Name, Columns,
    DuplicateColumns, IncompatibleColumns, LobColumns, RowCount, MaxRowBytes,
    EstTotalBytes, Status, Message.

.EXAMPLE
    Get-MSLinkedServerObjectInfo -SqlInstance 'SQL01' -LinkedServer 'ORA_ERP' -Verbose |
        Format-Table Type, Schema, Name, Columns, DuplicateColumns, IncompatibleColumns, RowCount, MaxRowBytes -AutoSize

    Analyzes every table/view and shows the result as a table.

.EXAMPLE
    # HTML report (uses Convert-MSArrayToHtml)
    $info = Get-MSLinkedServerObjectInfo -SqlInstance 'SQL01' -LinkedServer 'ORA_ERP' -Verbose
    $html = Convert-MSArrayToHtml -InputObject $info -Title 'Linked server ORA_ERP - object analysis' -NoFragment
    $html | Out-File 'C:\Temp\linkedserver-analysis.html' -Encoding utf8
    Start-Process 'C:\Temp\linkedserver-analysis.html'

    Analyzes every table/view and writes / opens an HTML report of the results.
#>
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]
        [string]$SqlInstance,

        [Parameter(Mandatory = $true)]
        [string]$LinkedServer,

        [string[]]$ObjectFilter,

        [switch]$NoRowCount,

        [string]$CheckpointPath,

        [switch]$Continue,

        [ValidateRange(1, 64)]
        [int]$ThrottleLimit = 1,

        [ValidateRange(0, 86400)]
        [int]$QueryTimeoutSeconds = 60,

        [pscredential]$SqlCredential
    )

    # ---------------------------------------------------------------------
    # Helpers
    # ---------------------------------------------------------------------
    function Get-BracketName {
        param([string]$Name)
        return '[' + ($Name -replace '\]', ']]') + ']'
    }
    function Get-LiteralString {
        param([string]$Value)
        return "N'" + ($Value -replace "'", "''") + "'"
    }
    function Get-RowString {
        param($Row, [string]$Column)
        $v = $Row.$Column
        if ($null -eq $v -or $v -is [System.DBNull]) { return '' }
        return [string]$v
    }
    function Get-RowInt {
        param($Row, [string]$Column, [int]$Default = 0)
        $v = $Row.$Column
        if ($null -eq $v -or $v -is [System.DBNull]) { return $Default }
        try { return [int]$v } catch { return $Default }
    }

    # A data type is flagged as incompatible for linked-server SELECT when its
    # ODBC code is unknown, or its provider type name is a LOB / CLR / special type.
    function Test-IncompatibleType {
        param([int]$OdbcType, [string]$TypeName)
        $known = @(1, -8, 12, -9, -1, -10, -7, -6, 5, 4, -5, 6, 7, 8, 2, 3, 9, 10, 11, 91, 92, 93, -2, -3, -4, -11)
        if ($OdbcType -notin $known) { return $true }
        if ($TypeName -match '(?i)(text|ntext|image|xml|xmltype|sql_variant|hierarchyid|geometry|geography|timestamp|rowversion|clob|nclob|blob|bfile|long|cursor)') {
            return $true
        }
        return $false
    }

    # Maximum declared width of a column in bytes. Unbounded / LOB columns return
    # the sentinel (int max) so the caller can count them separately.
    function Get-ColumnMaxByte {
        param([int]$OdbcType, [int]$Size, [int]$Decimals)
        switch ($OdbcType) {
            1   { [Math]::Max($Size, 1) }                                   # char
            12  { [Math]::Max($Size, 1) }                                   # varchar
            -1  { if ($Size -gt 0) { $Size } else { [int]::MaxValue } }     # text/longvarchar
            -8  { [Math]::Max($Size, 1) * 2 }                               # nchar
            -9  { [Math]::Max($Size, 1) * 2 }                               # nvarchar
            -10 { if ($Size -gt 0) { $Size * 2 } else { [int]::MaxValue } } # ntext
            -7  { 1 }    # bit
            -6  { 1 }    # tinyint
            5   { 2 }    # smallint
            4   { 4 }    # int
            -5  { 8 }    # bigint
            6   { 8 }    # float
            7   { 4 }    # real
            8   { 8 }    # double
            2   { [int]([Math]::Floor([Math]::Max($Size, 1) / 2) + 1) }     # numeric
            3   { [int]([Math]::Floor([Math]::Max($Size, 1) / 2) + 1) }     # decimal
            9   { 8 }    # datetime
            10  { 5 }    # time
            11  { 8 }    # timestamp
            91  { 3 }    # date
            92  { 5 }    # time
            93  { 8 }    # datetime2
            -2  { [Math]::Max($Size, 1) }                                   # binary
            -3  { [Math]::Max($Size, 1) }                                   # varbinary
            -4  { if ($Size -gt 0) { $Size } else { [int]::MaxValue } }     # image/longvarbinary
            -11 { 16 }   # uniqueidentifier
            default { if ($Size -gt 0) { $Size } else { 8000 } }
        }
    }

    $results = New-Object System.Collections.Generic.List[object]

    # Resolve the checkpoint (JSON progress) file.
    if ([string]::IsNullOrWhiteSpace($CheckpointPath)) {
        $safeSrv = ($SqlInstance  -replace '[^\w.\-]', '_')
        $safeLnk = ($LinkedServer -replace '[^\w.\-]', '_')
        $CheckpointPath = Join-Path ([System.IO.Path]::GetTempPath()) ("Get-MSLinkedServerObjectInfo_{0}_{1}.json" -f $safeSrv, $safeLnk)
    }

    function Save-Checkpoint {
        try {
            $dir = Split-Path -Path $CheckpointPath -Parent
            if ($dir -and -not (Test-Path -LiteralPath $dir)) { New-Item -ItemType Directory -Path $dir -Force | Out-Null }
            if ($results.Count -eq 0) {
                Set-Content -LiteralPath $CheckpointPath -Value '[]' -Encoding UTF8
            }
            else {
                ($results | ConvertTo-Json -Depth 5) | Set-Content -LiteralPath $CheckpointPath -Encoding UTF8
            }
        }
        catch {
            Write-Warning "Could not write checkpoint '$CheckpointPath': $($_.Exception.Message)"
        }
    }

    # On -Continue load previously saved results; otherwise discard any old checkpoint.
    $savedByKey = @{}
    if ($Continue -and (Test-Path -LiteralPath $CheckpointPath)) {
        try {
            $loaded = Get-Content -LiteralPath $CheckpointPath -Raw | ConvertFrom-Json
            foreach ($r in @($loaded)) {
                if ($null -eq $r) { continue }
                if (-not ($r.PSObject.Properties['Type'] -and $r.PSObject.Properties['Name'])) { continue }
                $savedByKey["{0}|{1}|{2}" -f $r.Type, $r.Schema, $r.Name] = $r
            }
            Write-Verbose "Resuming from checkpoint '$CheckpointPath' ($($savedByKey.Count) saved object(s))."
        }
        catch {
            Write-Warning "Could not read checkpoint '$CheckpointPath': $($_.Exception.Message). Starting fresh."
        }
    }
    elseif (-not $Continue -and (Test-Path -LiteralPath $CheckpointPath)) {
        Remove-Item -LiteralPath $CheckpointPath -Force -ErrorAction SilentlyContinue
    }

    try {
        $connectParams = @{
            SqlInstance            = $SqlInstance
            TrustServerCertificate = $true
            ErrorAction            = 'Stop'
        }
        if ($SqlCredential) { $connectParams['SqlCredential'] = $SqlCredential }

        Write-Verbose "Connecting to [$SqlInstance]..."
        $conn = Connect-DbaInstance @connectParams

        $lsCheck = @(Invoke-DbaQuery -SqlInstance $conn -ErrorAction Stop -Query (
            "SELECT name FROM sys.servers WHERE is_linked = 1 AND name = $(Get-LiteralString $LinkedServer)"
        ))
        if ($lsCheck.Count -eq 0) {
            throw "Linked server '$LinkedServer' was not found on [$SqlInstance]."
        }

        Write-Verbose "Enumerating objects on linked server '$LinkedServer'..."
        $objects = @(Invoke-DbaQuery -SqlInstance $conn -ErrorAction Stop -Query (
            "EXEC sp_tables_ex @table_server = $(Get-LiteralString $LinkedServer);"
        ))
        $objects = @($objects | Where-Object {
            (Get-RowString $_ 'TABLE_TYPE').Trim().ToUpper() -in @('TABLE', 'VIEW')
        })

        if ($ObjectFilter) {
            $objects = @($objects | Where-Object {
                $nm  = Get-RowString $_ 'TABLE_NAME'
                $sch = Get-RowString $_ 'TABLE_SCHEM'
                $candidates = @($nm)
                if ($sch) { $candidates += $sch; $candidates += "$sch.$nm" }

                $match = $false
                foreach ($pattern in $ObjectFilter) {
                    foreach ($candidate in $candidates) {
                        if ($candidate -like $pattern) { $match = $true; break }
                    }
                    if ($match) { break }
                }
                $match
            })
        }

        Write-Verbose ("{0} object(s) to analyze." -f $objects.Count)

        # -----------------------------------------------------------------
        # Decide which objects to process now. On -Continue, objects already
        # analyzed successfully (OK / Warning) are reused as-is; new objects
        # and previously FAILED ones are (re)processed.
        # -----------------------------------------------------------------
        $toProcess = New-Object System.Collections.Generic.List[object]
        foreach ($obj in $objects) {
            $srcSchem = Get-RowString $obj 'TABLE_SCHEM'
            $srcName  = Get-RowString $obj 'TABLE_NAME'
            $srcType  = (Get-RowString $obj 'TABLE_TYPE').Trim().ToUpper()

            $objKey = "{0}|{1}|{2}" -f $(if ($srcType -eq 'VIEW') { 'View' } else { 'Table' }), $srcSchem, $srcName
            if ($Continue -and $savedByKey.ContainsKey($objKey)) {
                $prev = $savedByKey[$objKey]
                if ($prev.PSObject.Properties['Status'] -and $prev.Status -in @('OK', 'Warning')) {
                    $results.Add($prev)
                    Write-Verbose "[$srcName] resumed from checkpoint (status $($prev.Status))."
                    continue
                }
                Write-Verbose "[$srcName] retrying (previous status: $($prev.Status))."
            }
            $toProcess.Add($obj)
        }
        Save-Checkpoint

        # -----------------------------------------------------------------
        # Per-object analysis, defined once as TEXT so the exact same logic
        # runs serially (ThrottleLimit = 1) or inside a -Parallel runspace.
        # -----------------------------------------------------------------
        $processObjectText = @'
param($obj, $conn, $LinkedServer, $NoRowCount, $QueryTimeout)

$srcCat   = Get-RowString $obj 'TABLE_CAT'
$srcSchem = Get-RowString $obj 'TABLE_SCHEM'
$srcName  = Get-RowString $obj 'TABLE_NAME'
$srcType  = (Get-RowString $obj 'TABLE_TYPE').Trim().ToUpper()

$objLabel = ($srcSchem + '.' + $srcName).TrimStart('.')

# Common query parameters (a per-query timeout so a slow/blocked object fails
# with a message instead of hanging forever).
$qParams = @{ SqlInstance = $conn; ErrorAction = 'Stop' }
if ($QueryTimeout -gt 0) { $qParams['QueryTimeout'] = $QueryTimeout }

$record = [pscustomobject]@{
    LinkedServer        = $LinkedServer
    Type                = if ($srcType -eq 'VIEW') { 'View' } else { 'Table' }
    Schema              = $srcSchem
    Name                = $srcName
    Columns             = 0
    DuplicateColumns    = ''
    IncompatibleColumns = ''
    LobColumns          = 0
    RowCount            = $null
    MaxRowBytes         = 0
    EstTotalBytes       = $null
    Status              = 'OK'
    Message             = ''
}

try {
    # --- Columns ------------------------------------------------
    $colSql = "EXEC sp_columns_ex @table_server = $(Get-LiteralString $LinkedServer), @table_name = $(Get-LiteralString $srcName)"
    if ($srcSchem) { $colSql += ", @table_schema = $(Get-LiteralString $srcSchem)" }
    if ($srcCat)   { $colSql += ", @table_catalog = $(Get-LiteralString $srcCat)" }
    $colSql += ';'

    $columns = @(Invoke-DbaQuery @qParams -Query $colSql)
    $record.Columns = $columns.Count

    if ($columns.Count -eq 0) {
        $record.Status  = 'Error'
        $record.Message = "No columns returned by the provider for [$objLabel]."
        return $record
    }

    # --- Duplicates + incompatible types + max row size --------
    $dupSeen     = @{}
    $dupList     = New-Object System.Collections.Generic.List[string]
    $incompat    = New-Object System.Collections.Generic.List[string]
    $maxRowBytes = [long]0
    $lob         = 0

    foreach ($c in $columns) {
        $cn = Get-RowString $c 'COLUMN_NAME'
        if ([string]::IsNullOrWhiteSpace($cn)) { continue }

        $key = $cn.ToLowerInvariant()
        if ($dupSeen.ContainsKey($key)) {
            # Duplicate column NAME: record it once, but do NOT count its
            # size / type again (matches the deduped copy that keeps first).
            if ($dupSeen[$key] -eq 1) { $dupList.Add($cn) }
            $dupSeen[$key] = $dupSeen[$key] + 1
            continue
        }
        $dupSeen[$key] = 1

        $odbc = Get-RowInt    $c 'DATA_TYPE'      0
        $size = Get-RowInt    $c 'COLUMN_SIZE'    0
        $dec  = Get-RowInt    $c 'DECIMAL_DIGITS' 0
        $tn   = Get-RowString $c 'TYPE_NAME'

        if (Test-IncompatibleType -OdbcType $odbc -TypeName $tn) {
            $incompat.Add("$cn ($tn)")
        }

        $mb = Get-ColumnMaxByte -OdbcType $odbc -Size $size -Decimals $dec
        if ($mb -ge [int]::MaxValue) { $lob++ } else { $maxRowBytes += [long]$mb }
    }

    $record.DuplicateColumns    = ($dupList -join ', ')
    $record.IncompatibleColumns = (($incompat | Select-Object -Unique) -join ', ')
    $record.LobColumns          = $lob
    $record.MaxRowBytes         = $maxRowBytes

    # --- Row count + estimated total size ----------------------
    if (-not $NoRowCount) {
        try {
            $p2 = if ($srcCat)   { Get-BracketName $srcCat }   else { '' }
            $p3 = if ($srcSchem) { Get-BracketName $srcSchem } else { '' }
            $fourPart = "$(Get-BracketName $LinkedServer).$p2.$p3.$(Get-BracketName $srcName)"

            # Simple COUNT(*) - no table is expected to exceed the int limit.
            $rcRow = Invoke-DbaQuery @qParams -Query "SELECT rc = COUNT(*) FROM $fourPart"
            $rcVal = $rcRow.rc
            if ($null -ne $rcVal -and $rcVal -isnot [System.DBNull]) {
                $record.RowCount = [long]$rcVal
                if ($maxRowBytes -gt 0) {
                    $record.EstTotalBytes = [long]$record.RowCount * [long]$maxRowBytes
                }
            }
        }
        catch {
            $rcErr = $_.Exception.Message
            if ($rcErr -match '(?i)timeout|timed out|execution.*terminated') {
                $record.Status  = 'Warning'
                $record.Message = "Row count TIMED OUT after ${QueryTimeout}s for [$objLabel]: $rcErr"
            }
            else {
                $record.Message = "Row count unavailable for [$objLabel]: $rcErr"
            }
        }
    }
    else {
        $record.Message = 'Row count skipped (-NoRowCount).'
    }

    # --- Status / notes ----------------------------------------
    $notes = New-Object System.Collections.Generic.List[string]
    if ($dupList.Count -gt 0)  { $notes.Add("$($dupList.Count) duplicate column name(s)") }
    if ($incompat.Count -gt 0) { $notes.Add("$($incompat.Count) incompatible type(s)") }
    if ($lob -gt 0)            { $notes.Add("$lob LOB/unbounded column(s)") }

    if ($dupList.Count -gt 0 -or $incompat.Count -gt 0) { $record.Status = 'Warning' }
    if ($notes.Count -gt 0) {
        $joined = ($notes -join '; ')
        $record.Message = if ([string]::IsNullOrWhiteSpace($record.Message)) { $joined } else { "$joined. $($record.Message)" }
    }
}
catch {
    $err = $_.Exception.Message
    $record.Status = 'Error'
    if ($err -match '(?i)timeout|timed out|execution.*terminated') {
        $record.Message = "TIMED OUT after ${QueryTimeout}s reading [$objLabel]: $err"
    }
    else {
        $record.Message = "Error reading [$objLabel]: $err"
    }
}

return $record
'@

        # Decide serial vs. parallel. -Parallel needs PowerShell 7+.
        $useParallel = $ThrottleLimit -gt 1
        if ($useParallel -and $PSVersionTable.PSVersion.Major -lt 7) {
            Write-Warning "Parallel analysis needs PowerShell 7+. Falling back to serial (ThrottleLimit = 1)."
            $useParallel = $false
        }

        if (-not $useParallel) {
            $processObject = [scriptblock]::Create($processObjectText)
            foreach ($obj in $toProcess) {
                $record = & $processObject $obj $conn $LinkedServer $NoRowCount.IsPresent $QueryTimeoutSeconds
                $results.Add($record)
                Write-Verbose ("[{0}] {1} cols={2} rows={3} status={4}" -f $record.Name, $record.Type, $record.Columns, $record.RowCount, $record.Status)
                Save-Checkpoint
            }
        }
        else {
            Write-Verbose ("Analyzing {0} object(s) with parallelism level {1}..." -f $toProcess.Count, $ThrottleLimit)

            # Helper function bodies re-created inside each parallel runspace,
            # plus the values each worker needs (splat + flags).
            $funcDefs = @{
                'Get-BracketName'       = ${function:Get-BracketName}.ToString()
                'Get-LiteralString'     = ${function:Get-LiteralString}.ToString()
                'Get-RowString'         = ${function:Get-RowString}.ToString()
                'Get-RowInt'            = ${function:Get-RowInt}.ToString()
                'Test-IncompatibleType' = ${function:Test-IncompatibleType}.ToString()
                'Get-ColumnMaxByte'     = ${function:Get-ColumnMaxByte}.ToString()
            }
            $connParamsForThread = $connectParams
            $noRowCountFlag      = $NoRowCount.IsPresent
            $queryTimeoutValue   = $QueryTimeoutSeconds

            $toProcess | ForEach-Object -ThrottleLimit $ThrottleLimit -Parallel {
                $obj = $_
                try {
                    # One-time per-runspace setup: import dbatools, re-create the
                    # helper functions, open a connection and compile the analyzer.
                    # Cached in runspace globals so it is paid once per worker, not
                    # once per object.
                    if (-not $global:__lsInit) {
                        Import-Module dbatools -ErrorAction SilentlyContinue
                        $defs = $using:funcDefs
                        foreach ($kv in $defs.GetEnumerator()) {
                            Set-Item -Path "function:global:$($kv.Key)" -Value $kv.Value
                        }
                        $cp = $using:connParamsForThread
                        $global:__lsConn = Connect-DbaInstance @cp
                        $global:__lsSb   = [scriptblock]::Create($using:processObjectText)
                        $global:__lsInit = $true
                    }
                    & $global:__lsSb $obj $global:__lsConn $using:LinkedServer $using:noRowCountFlag $using:queryTimeoutValue
                }
                catch {
                    # Setup / connection failure: still emit a record so the object
                    # is never silently dropped from the report or the checkpoint.
                    [pscustomobject]@{
                        LinkedServer        = $using:LinkedServer
                        Type                = if ("$($obj.TABLE_TYPE)".Trim().ToUpper() -eq 'VIEW') { 'View' } else { 'Table' }
                        Schema              = [string]$obj.TABLE_SCHEM
                        Name                = [string]$obj.TABLE_NAME
                        Columns             = 0
                        DuplicateColumns    = ''
                        IncompatibleColumns = ''
                        LobColumns          = 0
                        RowCount            = $null
                        MaxRowBytes         = 0
                        EstTotalBytes       = $null
                        Status              = 'Error'
                        Message             = "Worker setup/connection failed for [$([string]$obj.TABLE_SCHEM).$([string]$obj.TABLE_NAME)]: $($_.Exception.Message)"
                    }
                }
            } | ForEach-Object {
                # Runs on the MAIN thread as each result streams back, so the
                # checkpoint write stays single-threaded and safe.
                $results.Add($_)
                Write-Verbose ("[{0}] {1} cols={2} rows={3} status={4}" -f $_.Name, $_.Type, $_.Columns, $_.RowCount, $_.Status)
                Save-Checkpoint
            }
        }

        Write-Verbose "Checkpoint saved to '$CheckpointPath'."

        # Grand total across all analyzed tables/views.
        $grandTotalBytes = [long]0
        $grandTotalRows  = [long]0
        foreach ($r in $results) {
            if ($r.PSObject.Properties['EstTotalBytes'] -and $r.EstTotalBytes) { $grandTotalBytes += [long]$r.EstTotalBytes }
            if ($r.PSObject.Properties['RowCount']      -and $r.RowCount)      { $grandTotalRows  += [long]$r.RowCount }
        }
        $mb = [Math]::Round($grandTotalBytes / 1MB, 2)
        $gb = [Math]::Round($grandTotalBytes / 1GB, 2)
        Write-Host ("Total: {0} object(s), {1:N0} row(s), estimated {2:N0} bytes ({3} MB / {4} GB)." -f $results.Count, $grandTotalRows, $grandTotalBytes, $mb, $gb) -ForegroundColor Cyan

        return $results
    }
    catch {
        throw "Get-MSLinkedServerObjectInfo failed: $($_.Exception.Message)"
    }
}
