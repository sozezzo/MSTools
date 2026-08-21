function Copy-MSLinkedServerData {
<#
.SYNOPSIS
    Copies the DATA of a linked server's tables/views into the database created
    by New-MSLinkedServerDatabaseStructure.

.DESCRIPTION
    For every matching linked-server object it streams the rows into the
    matching destination table using SqlBulkCopy, with live progress and
    logging. It reuses the same object-to-destination mapping as
    New-MSLinkedServerDatabaseStructure (empty schema parameter keeps the
    original schema; a value folds the original schema into the table name), and
    the same -ObjectFilter.

    Behaviour:
      - By default the destination table is TRUNCATED before copying.
      - -SkipIfNotEmpty leaves a destination table that already has rows
        untouched (no truncate, no copy).
      - It is "all or nothing" per table: there is NO resume. Because most
        objects have no primary key / timestamp, a partially copied table cannot
        be safely continued. If a table fails or is interrupted, TRUNCATE it
        manually and run the copy again.
      - Errors on one object are logged and the copy CONTINUES with the next.
      - Column mapping is by NAME, so the surrogate "<tablename>_id" identity
        primary key added by the structure step is auto-generated (not copied).

    Progress is shown with Write-Progress and written to the log periodically,
    so a copy that takes hours or days can be monitored and, if needed,
    restarted table by table.

.PARAMETER SqlInstance
    SQL Server instance that hosts the linked server and the destination database.

.PARAMETER LinkedServer
    Linked server to read the data from.

.PARAMETER NewDatabase
    Destination database (the one created by New-MSLinkedServerDatabaseStructure).

.PARAMETER ViewSchema
    Must match what was used for the structure: empty keeps the original schema;
    a value means the object was folded into that schema as "<schema>_<name>".

.PARAMETER TableSchema
    Same as -ViewSchema, for objects that came from TABLEs.

.PARAMETER ObjectFilter
    Optional list of names to include (wildcards allowed; matched against the
    schema name, the object name and "schema.name"). Empty = all.

.PARAMETER SkipIfNotEmpty
    Skip (do not truncate, do not copy) a destination table that already
    contains rows. By default the destination is truncated and re-copied.

.PARAMETER BatchSize
    SqlBulkCopy batch size. Default 10000.

.PARAMETER NotifyAfter
    Raise a progress notification every N rows. Default 10000.

.PARAMETER SqlCredential
    Optional SQL credential. When omitted, the current login (Integrated
    Security) is used.

.OUTPUTS
    One [pscustomobject] per object: LinkedServer, Type, Schema, Name,
    TargetTable, RowsCopied, DurationSeconds, Status, Message.

.EXAMPLE
    Copy-MSLinkedServerData -SqlInstance 'SQL01' -LinkedServer 'ORA_ERP' `
        -NewDatabase 'ERP_Stage' -Verbose

    Truncates and copies every object into ERP_Stage, showing and logging progress.

.EXAMPLE
    Copy-MSLinkedServerData -SqlInstance 'SQL01' -LinkedServer 'ORA_ERP' `
        -NewDatabase 'ERP_Stage' -ObjectFilter 'HR.*' -SkipIfNotEmpty -Verbose

    Copies only the HR objects, skipping any destination table that already has data.
#>
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]
        [string]$SqlInstance,

        [Parameter(Mandatory = $true)]
        [string]$LinkedServer,

        [Parameter(Mandatory = $true)]
        [string]$NewDatabase,

        [string]$ViewSchema = '',

        [string]$TableSchema = '',

        [string[]]$ObjectFilter,

        [switch]$SkipIfNotEmpty,

        [int]$BatchSize = 10000,

        [int]$NotifyAfter = 10000,

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

    try {
        # -----------------------------------------------------------------
        # Connect (current login unless a credential is supplied)
        # -----------------------------------------------------------------
        $connectParams = @{
            SqlInstance            = $SqlInstance
            Database               = $NewDatabase
            TrustServerCertificate = $true
            ErrorAction            = 'Stop'
        }
        if ($SqlCredential) { $connectParams['SqlCredential'] = $SqlCredential }

        Write-MSLog -Level Info -Message "Copy-MSLinkedServerData: connecting to [$SqlInstance] / [$NewDatabase]."
        $conn = Connect-DbaInstance @connectParams

        $lsCheck = @(Invoke-DbaQuery -SqlInstance $conn -ErrorAction Stop -Query (
            "SELECT name FROM sys.servers WHERE is_linked = 1 AND name = $(Get-LiteralString $LinkedServer)"
        ))
        if ($lsCheck.Count -eq 0) {
            throw "Linked server '$LinkedServer' was not found on [$SqlInstance]."
        }

        # -----------------------------------------------------------------
        # Resolve the SqlClient types dbatools is using and build a raw
        # connection string for the SqlBulkCopy read/write connections.
        # -----------------------------------------------------------------
        $ns = $conn.ConnectionContext.SqlConnectionObject.GetType().Namespace
        $SqlConnectionType = ("$ns.SqlConnection") -as [type]
        $SqlBulkCopyType   = ("$ns.SqlBulkCopy") -as [type]
        $SqlCsbType        = ("$ns.SqlConnectionStringBuilder") -as [type]
        if (-not $SqlConnectionType -or -not $SqlBulkCopyType -or -not $SqlCsbType) {
            throw "Could not resolve the SqlClient types from namespace '$ns'."
        }

        $csb = $SqlCsbType::new()
        $csb['Data Source']            = $SqlInstance
        $csb['Initial Catalog']        = $NewDatabase
        $csb['TrustServerCertificate'] = $true
        $csb['Application Name']       = 'MSTools-CopyLinkedServerData'
        if ($SqlCredential) {
            $csb['User ID']  = $SqlCredential.UserName
            $csb['Password'] = $SqlCredential.GetNetworkCredential().Password
        }
        else {
            $csb['Integrated Security'] = $true
        }
        $connString = $csb.ConnectionString

        # -----------------------------------------------------------------
        # Enumerate + filter the linked-server objects
        # -----------------------------------------------------------------
        Write-MSLog -Level Info -Message "Copy-MSLinkedServerData: enumerating objects on '$LinkedServer'."
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

        $total = $objects.Count
        Write-MSLog -Level Info -Message ("Copy-MSLinkedServerData: {0} object(s) to copy." -f $total)

        $results = New-Object System.Collections.Generic.List[object]
        $index   = 0

        foreach ($obj in $objects) {
            $index++

            $srcCat   = Get-RowString $obj 'TABLE_CAT'
            $srcSchem = Get-RowString $obj 'TABLE_SCHEM'
            $srcName  = Get-RowString $obj 'TABLE_NAME'
            $srcType  = (Get-RowString $obj 'TABLE_TYPE').Trim().ToUpper()

            # Destination schema/name - MUST match New-MSLinkedServerDatabaseStructure.
            $schemaParam = if ($srcType -eq 'VIEW') { $ViewSchema } else { $TableSchema }
            if ([string]::IsNullOrWhiteSpace($schemaParam)) {
                $targetSchema = if ([string]::IsNullOrWhiteSpace($srcSchem)) { 'dbo' } else { $srcSchem }
                $targetTable  = $srcName
            }
            else {
                $targetSchema = $schemaParam
                $targetTable  = if ([string]::IsNullOrWhiteSpace($srcSchem)) { $srcName } else { "$($srcSchem)_$($srcName)" }
            }

            $destFull     = "$(Get-BracketName $targetSchema).$(Get-BracketName $targetTable)"
            $destFullLit  = "N'" + ($destFull -replace "'", "''") + "'"

            $record = [pscustomobject]@{
                LinkedServer    = $LinkedServer
                Type            = if ($srcType -eq 'VIEW') { 'View' } else { 'Table' }
                Schema          = $srcSchem
                Name            = $srcName
                TargetTable     = $destFull
                RowsCopied      = 0
                DurationSeconds = 0
                Status          = 'Failed'
                Message         = ''
            }

            Write-Progress -Id 0 -Activity "Copying data from '$LinkedServer'" `
                -Status ("Object {0} of {1}: {2}" -f $index, $total, $destFull) `
                -PercentComplete ([int](($index - 1) / [Math]::Max($total, 1) * 100))

            $reader = $null; $readConn = $null; $writeConn = $null; $bulk = $null
            try {
                # --- Destination table must exist -------------------------
                $existsRow = Invoke-DbaQuery -SqlInstance $conn -Database $NewDatabase -ErrorAction Stop -Query (
                    "SELECT c = CASE WHEN OBJECT_ID($destFullLit,'U') IS NULL THEN 0 ELSE 1 END"
                )
                if ([int]$existsRow.c -ne 1) {
                    $record.Status  = 'Skipped'
                    $record.Message = "Destination table $destFull does not exist - run New-MSLinkedServerDatabaseStructure first."
                    $results.Add($record)
                    Write-MSLog -Level Warning -Message $record.Message
                    continue
                }

                # --- Skip if it already has data (optional) ---------------
                $dataRow = Invoke-DbaQuery -SqlInstance $conn -Database $NewDatabase -ErrorAction Stop -Query (
                    "SELECT c = CASE WHEN EXISTS (SELECT 1 FROM $destFull) THEN 1 ELSE 0 END"
                )
                $hasData = ([int]$dataRow.c -eq 1)
                if ($hasData -and $SkipIfNotEmpty) {
                    $record.Status  = 'Skipped'
                    $record.Message = 'Destination already has data (-SkipIfNotEmpty).'
                    $results.Add($record)
                    Write-MSLog -Level Info -Message "[$destFull] skipped - already has data."
                    continue
                }

                # --- Columns to copy: source columns that also exist in dest
                $colSql = "EXEC sp_columns_ex @table_server = $(Get-LiteralString $LinkedServer), @table_name = $(Get-LiteralString $srcName)"
                if ($srcSchem) { $colSql += ", @table_schema = $(Get-LiteralString $srcSchem)" }
                if ($srcCat)   { $colSql += ", @table_catalog = $(Get-LiteralString $srcCat)" }
                $colSql += ';'
                $srcColumns = @(Invoke-DbaQuery -SqlInstance $conn -ErrorAction Stop -Query $colSql)

                $destColRows = @(Invoke-DbaQuery -SqlInstance $conn -Database $NewDatabase -ErrorAction Stop -Query (
                    "SELECT name FROM sys.columns WHERE object_id = OBJECT_ID($destFullLit,'U')"
                ))
                $destColSet = New-Object 'System.Collections.Generic.HashSet[string]' ([System.StringComparer]::OrdinalIgnoreCase)
                foreach ($dc in $destColRows) { [void]$destColSet.Add((Get-RowString $dc 'name')) }

                $seen = New-Object 'System.Collections.Generic.HashSet[string]' ([System.StringComparer]::OrdinalIgnoreCase)
                $copyColumns = New-Object System.Collections.Generic.List[string]
                foreach ($sc in $srcColumns) {
                    $cn = Get-RowString $sc 'COLUMN_NAME'
                    if ([string]::IsNullOrWhiteSpace($cn)) { continue }
                    if (-not $seen.Add($cn)) { continue }          # keep first of duplicates
                    if ($destColSet.Contains($cn)) { $copyColumns.Add($cn) }
                }

                if ($copyColumns.Count -eq 0) {
                    $record.Status  = 'Skipped'
                    $record.Message = 'No matching columns between source and destination.'
                    $results.Add($record)
                    Write-MSLog -Level Warning -Message "[$destFull] $($record.Message)"
                    continue
                }

                # --- Source row count (for the progress total) ------------
                $p2 = if ($srcCat)   { Get-BracketName $srcCat }   else { '' }
                $p3 = if ($srcSchem) { Get-BracketName $srcSchem } else { '' }
                $sourceFour = "$(Get-BracketName $LinkedServer).$p2.$p3.$(Get-BracketName $srcName)"

                $sourceCount = -1
                try {
                    $scRow = Invoke-DbaQuery -SqlInstance $conn -ErrorAction Stop -Query "SELECT rc = COUNT_BIG(*) FROM $sourceFour"
                    if ($null -ne $scRow.rc -and $scRow.rc -isnot [System.DBNull]) { $sourceCount = [long]$scRow.rc }
                }
                catch { Write-Verbose "[$destFull] could not pre-count source rows: $($_.Exception.Message)" }

                # --- Truncate the destination (default behaviour) ---------
                Write-MSLog -Level Info -Message ("[$destFull] copy starting (source rows: {0})." -f ($(if ($sourceCount -ge 0) { '{0:N0}' -f $sourceCount } else { 'unknown' })))
                Invoke-DbaQuery -SqlInstance $conn -Database $NewDatabase -ErrorAction Stop -Query "TRUNCATE TABLE $destFull;"

                # --- Stream source -> destination with SqlBulkCopy --------
                $selectList = ($copyColumns | ForEach-Object { Get-BracketName $_ }) -join ', '
                $sourceSelect = "SELECT $selectList FROM $sourceFour"

                $readConn = $SqlConnectionType::new($connString)
                $readConn.Open()
                $cmd = $readConn.CreateCommand()
                $cmd.CommandText = $sourceSelect
                $cmd.CommandTimeout = 0
                $reader = $cmd.ExecuteReader()

                $writeConn = $SqlConnectionType::new($connString)
                $writeConn.Open()
                $bulk = $SqlBulkCopyType::new($writeConn)
                $bulk.DestinationTableName = $destFull
                $bulk.BatchSize            = $BatchSize
                $bulk.BulkCopyTimeout      = 0
                $bulk.NotifyAfter          = $NotifyAfter
                foreach ($cn in $copyColumns) { [void]$bulk.ColumnMappings.Add($cn, $cn) }

                $script:mslsc_total   = $sourceCount
                $script:mslsc_dest    = $destFull
                $script:mslsc_lastLog = Get-Date

                $onRows = {
                    param($eventSender, $e)
                    $copied = $e.RowsCopied
                    $tot    = $script:mslsc_total
                    if ($tot -gt 0) {
                        $pct    = [int](($copied / $tot) * 100)
                        $status = "{0:N0} / {1:N0} rows ({2}%)" -f $copied, $tot, $pct
                        Write-Progress -Id 1 -ParentId 0 -Activity ("Copying {0}" -f $script:mslsc_dest) -Status $status -PercentComplete ([Math]::Min($pct, 100))
                    }
                    else {
                        $status = "{0:N0} rows" -f $copied
                        Write-Progress -Id 1 -ParentId 0 -Activity ("Copying {0}" -f $script:mslsc_dest) -Status $status
                    }
                    if (((Get-Date) - $script:mslsc_lastLog).TotalSeconds -ge 30) {
                        $script:mslsc_lastLog = Get-Date
                        try { Write-MSLog -Level Info -Message ("[{0}] progress: {1}" -f $script:mslsc_dest, $status) } catch { }
                    }
                }
                $bulk.add_SqlRowsCopied($onRows)

                $sw = [System.Diagnostics.Stopwatch]::StartNew()
                $bulk.WriteToServer($reader)
                $sw.Stop()

                $bulk.remove_SqlRowsCopied($onRows)
                Write-Progress -Id 1 -ParentId 0 -Activity ("Copying {0}" -f $destFull) -Completed

                # Final row count from the destination (reliable).
                $finalRow = Invoke-DbaQuery -SqlInstance $conn -Database $NewDatabase -ErrorAction Stop -Query "SELECT rc = COUNT_BIG(*) FROM $destFull"
                $rowsCopied = if ($null -ne $finalRow.rc -and $finalRow.rc -isnot [System.DBNull]) { [long]$finalRow.rc } else { 0 }

                $record.RowsCopied      = $rowsCopied
                $record.DurationSeconds = [int]$sw.Elapsed.TotalSeconds
                $record.Status          = 'Copied'
                $record.Message         = "Copied $('{0:N0}' -f $rowsCopied) row(s) in $($sw.Elapsed.ToString('hh\:mm\:ss'))."
                $results.Add($record)
                Write-MSLog -Level Info -Message "[$destFull] $($record.Message)"
            }
            catch {
                # Log and CONTINUE with the next object. The partially copied
                # table (if any) can be truncated manually and retried.
                $record.Status  = 'Failed'
                $record.Message = $_.Exception.Message
                $results.Add($record)
                Write-MSLog -Level Error -Message "[$destFull] copy FAILED: $($_.Exception.Message)"
            }
            finally {
                if ($reader)    { try { $reader.Dispose() }    catch { } }
                if ($bulk)      { try { $bulk.Close() }        catch { } }
                if ($readConn)  { try { $readConn.Dispose() }  catch { } }
                if ($writeConn) { try { $writeConn.Dispose() } catch { } }
            }
        }

        Write-Progress -Id 0 -Activity "Copying data from '$LinkedServer'" -Completed

        $copied  = @($results | Where-Object { $_.Status -eq 'Copied' }).Count
        $skipped = @($results | Where-Object { $_.Status -eq 'Skipped' }).Count
        $failed  = @($results | Where-Object { $_.Status -eq 'Failed' }).Count
        Write-MSLog -Level Info -Message ("Copy-MSLinkedServerData: done. Copied={0} Skipped={1} Failed={2} (Total={3})." -f $copied, $skipped, $failed, $results.Count)

        return $results
    }
    catch {
        throw "Copy-MSLinkedServerData failed: $($_.Exception.Message)"
    }
}
