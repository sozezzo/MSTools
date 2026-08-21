function New-MSLinkedServerDatabaseStructure {
<#
.SYNOPSIS
    Creates a database whose tables mirror the STRUCTURE of the tables and
    (materialized) views exposed by a linked server.

.DESCRIPTION
    Reads the object list of a linked server with the provider-agnostic system
    procedures sp_tables_ex / sp_columns_ex, so it works whether the linked
    server points at SQL Server, Oracle, MySQL, a file, or any other OLE DB
    source.

    For every TABLE/VIEW it creates a table in the destination schema. When
    -TableSchema / -ViewSchema is EMPTY the ORIGINAL source schema is kept
    (and created in the new database); TABLEs keep the original name while
    VIEWs are named "vw_<viewname>". When a value is given, the object is
    placed in that schema and the original schema is folded into the table
    name as "<originalschema>_<name>". Only the structure (columns and their
    types) is created - no data is copied here.

    Because most linked-server objects have no primary key, a surrogate
    identity primary key column named "<tablename>_id" is added to each created
    table. Use -NoPrimaryKey to skip it.

    The new database is created on -SqlInstance (the same instance that hosts
    the linked server). The connection uses the current login (Integrated
    Security) unless -SqlCredential is supplied.

.PARAMETER SqlInstance
    The SQL Server instance that hosts the linked server and where the new
    database is created.

.PARAMETER LinkedServer
    Name of the linked server to read the structure from.

.PARAMETER NewDatabase
    Name of the database to create (created only if it does not already exist).

.PARAMETER ViewSchema
    Destination schema for objects coming from linked-server VIEWs.
      - Empty (default): keep the ORIGINAL source schema (created if needed)
        and name the table "vw_<viewname>", so the view-backed staging table
        stays in its original schema yet is distinct from a same-named table.
      - A value: create the object in that schema and fold the original schema
        into the table name as "<originalschema>_<name>".

.PARAMETER TableSchema
    Destination schema for objects coming from linked-server TABLEs. Same rules
    as -ViewSchema: empty keeps the original schema; a value folds the original
    schema into the table name.

.PARAMETER ObjectFilter
    Optional list of names to include (wildcards allowed). Each pattern is
    matched against the SCHEMA name, the object (table/view) name, and the
    "schema.name" form - so you can filter by schema, by object name, or both.
    When empty (default) every table and view is included.

.PARAMETER NoPrimaryKey
    Do not add the surrogate "<tablename>_id" identity primary key.

.PARAMETER IgnoreDuplicateColumn
    Some providers expose the same column name more than once for an object.
    By default the table is NOT created and a warning is written. With this
    switch, only the FIRST occurrence of each duplicated column is kept and the
    rest are ignored, so the table is still created.

.PARAMETER Overwrite
    Drop and recreate a target table that already exists. By default an existing
    target table is left untouched (skipped).

.PARAMETER SqlCredential
    Optional SQL credential for connecting to -SqlInstance. When omitted, the
    current login (Integrated Security) is used.

.OUTPUTS
    One [pscustomobject] per source object with LinkedServer, SourceType,
    SourceObject, TargetSchema, TargetTable, Columns, PrimaryKey, Status,
    Message.

.EXAMPLE
    New-MSLinkedServerDatabaseStructure -SqlInstance 'SQL01' -LinkedServer 'ORA_ERP' `
        -NewDatabase 'ERP_Stage' -ViewSchema 'view' -Verbose

    Creates ERP_Stage on SQL01. Tables keep their original source schema; views
    are placed in the 'view' schema with the original schema folded into the
    name. Each table gets a <tablename>_id identity primary key.

.EXAMPLE
    New-MSLinkedServerDatabaseStructure -SqlInstance 'SQL01' -LinkedServer 'MYSQL_APP' `
        -NewDatabase 'APP_Stage' -NoPrimaryKey

    Same, but without adding the surrogate primary key.
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

        # Optional list of names to include (wildcards allowed). Matches a
        # schema, an object name, or "schema.name". Empty = all tables and views.
        [string[]]$ObjectFilter,

        [switch]$NoPrimaryKey,

        # When a source object has the same column name more than once:
        #   (default, not set) warn and DO NOT create the table.
        #   (set)              keep the FIRST occurrence and ignore the rest.
        [switch]$IgnoreDuplicateColumn,

        # Drop and recreate a target table that already exists. Default: skip it.
        [switch]$Overwrite,

        [pscredential]$SqlCredential
    )

    # ---------------------------------------------------------------------
    # Small helpers
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

    # Map an ODBC data type (from sp_columns_ex.DATA_TYPE) to a SQL Server type.
    function ConvertTo-MSSqlColumnType {
        param([int]$OdbcType, [int]$Size, [int]$Decimals)

        switch ($OdbcType) {
            1   { if ($Size -le 0 -or $Size -gt 8000) { 'varchar(max)' }  else { "char($Size)" } }      # SQL_CHAR
            -8  { if ($Size -le 0 -or $Size -gt 4000) { 'nvarchar(max)' } else { "nchar($Size)" } }     # SQL_WCHAR
            12  { if ($Size -le 0 -or $Size -gt 8000) { 'varchar(max)' }  else { "varchar($Size)" } }   # SQL_VARCHAR
            -9  { if ($Size -le 0 -or $Size -gt 4000) { 'nvarchar(max)' } else { "nvarchar($Size)" } }  # SQL_WVARCHAR
            -1  { 'varchar(max)' }                                                                       # SQL_LONGVARCHAR
            -10 { 'nvarchar(max)' }                                                                      # SQL_WLONGVARCHAR
            -7  { 'bit' }        # SQL_BIT
            -6  { 'tinyint' }    # SQL_TINYINT
            5   { 'smallint' }   # SQL_SMALLINT
            4   { 'int' }        # SQL_INTEGER
            -5  { 'bigint' }     # SQL_BIGINT
            6   { 'float' }      # SQL_FLOAT
            7   { 'real' }       # SQL_REAL
            8   { 'float' }      # SQL_DOUBLE
            2   { $p = if ($Size -ge 1 -and $Size -le 38) { $Size } else { 38 }
                  $s = if ($Decimals -ge 0 -and $Decimals -le $p) { $Decimals } else { 0 }
                  "numeric($p,$s)" }                                                                     # SQL_NUMERIC
            3   { $p = if ($Size -ge 1 -and $Size -le 38) { $Size } else { 38 }
                  $s = if ($Decimals -ge 0 -and $Decimals -le $p) { $Decimals } else { 0 }
                  "decimal($p,$s)" }                                                                     # SQL_DECIMAL
            9   { 'datetime2' }  # SQL_DATETIME / SQL_DATE (ODBC 2) - datetime2 is safe
            10  { 'time' }       # SQL_TIME (ODBC 2)
            11  { 'datetime2' }  # SQL_TIMESTAMP (ODBC 2)
            91  { 'date' }       # SQL_TYPE_DATE (ODBC 3)
            92  { 'time' }       # SQL_TYPE_TIME (ODBC 3)
            93  { 'datetime2' }  # SQL_TYPE_TIMESTAMP (ODBC 3)
            -2  { if ($Size -le 0 -or $Size -gt 8000) { 'varbinary(max)' } else { "binary($Size)" } }    # SQL_BINARY
            -3  { if ($Size -le 0 -or $Size -gt 8000) { 'varbinary(max)' } else { "varbinary($Size)" } } # SQL_VARBINARY
            -4  { 'varbinary(max)' }   # SQL_LONGVARBINARY
            -11 { 'uniqueidentifier' } # SQL_GUID
            default { 'nvarchar(max)' } # unknown provider type -> safe fallback
        }
    }

    $results = New-Object System.Collections.Generic.List[object]

    try {
        # -----------------------------------------------------------------
        # Connect (current login unless a credential is supplied)
        # -----------------------------------------------------------------
        $connectParams = @{
            SqlInstance            = $SqlInstance
            TrustServerCertificate = $true
            ErrorAction            = 'Stop'
        }
        if ($SqlCredential) { $connectParams['SqlCredential'] = $SqlCredential }

        Write-Verbose "Connecting to [$SqlInstance]..."
        $conn = Connect-DbaInstance @connectParams

        # -----------------------------------------------------------------
        # Validate the linked server
        # -----------------------------------------------------------------
        $lsCheck = @(Invoke-DbaQuery -SqlInstance $conn -ErrorAction Stop -Query (
            "SELECT name, product, provider FROM sys.servers WHERE is_linked = 1 AND name = $(Get-LiteralString $LinkedServer)"
        ))
        if ($lsCheck.Count -eq 0) {
            throw "Linked server '$LinkedServer' was not found on [$SqlInstance]."
        }
        Write-Verbose ("Linked server '{0}' found (product='{1}', provider='{2}')." -f $LinkedServer, (Get-RowString $lsCheck[0] 'product'), (Get-RowString $lsCheck[0] 'provider'))

        # -----------------------------------------------------------------
        # Create the database (if missing). Destination schemas are created on
        # demand per object (they may be the original source schemas).
        # -----------------------------------------------------------------
        Write-Verbose "Ensuring database [$NewDatabase] exists..."
        Invoke-DbaQuery -SqlInstance $conn -Database master -ErrorAction Stop -Query (
            "IF DB_ID($(Get-LiteralString $NewDatabase)) IS NULL CREATE DATABASE $(Get-BracketName $NewDatabase);"
        )

        $ensuredSchemas = New-Object 'System.Collections.Generic.HashSet[string]' ([System.StringComparer]::OrdinalIgnoreCase)

        # -----------------------------------------------------------------
        # Enumerate the linked-server tables and views
        # -----------------------------------------------------------------
        Write-Verbose "Enumerating objects on linked server '$LinkedServer'..."
        $objects = @(Invoke-DbaQuery -SqlInstance $conn -ErrorAction Stop -Query (
            "EXEC sp_tables_ex @table_server = $(Get-LiteralString $LinkedServer);"
        ))

        $objects = @($objects | Where-Object {
            (Get-RowString $_ 'TABLE_TYPE').Trim().ToUpper() -in @('TABLE', 'VIEW')
        })

        # Optional name filter (wildcards allowed). Empty = all tables and views.
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

        Write-Verbose ("{0} table(s)/view(s) to replicate." -f $objects.Count)

        # -----------------------------------------------------------------
        # Create a table for every source object
        # -----------------------------------------------------------------
        foreach ($obj in $objects) {

            $srcCat   = Get-RowString $obj 'TABLE_CAT'
            $srcSchem = Get-RowString $obj 'TABLE_SCHEM'
            $srcName  = Get-RowString $obj 'TABLE_NAME'
            $srcType  = (Get-RowString $obj 'TABLE_TYPE').Trim().ToUpper()

            $sourceObject = (@($srcCat, $srcSchem, $srcName) | Where-Object { $_ }) -join '.'

            # Destination schema / name:
            #   - schema parameter EMPTY -> keep the original source schema. VIEWs
            #                               are named "vw_<name>" (<schema>.vw_<name>)
            #                               so the view-backed staging table stays in
            #                               the original schema yet is distinct from a
            #                               same-named table; TABLEs keep the name as-is.
            #   - schema parameter SET   -> use it, and fold the original schema
            #                               into the table name (<schema>_<name>).
            $schemaParam = if ($srcType -eq 'VIEW') { $ViewSchema } else { $TableSchema }
            if ([string]::IsNullOrWhiteSpace($schemaParam)) {
                $targetSchema = if ([string]::IsNullOrWhiteSpace($srcSchem)) { 'dbo' } else { $srcSchem }
                $targetTable  = if ($srcType -eq 'VIEW') { "vw_$srcName" } else { $srcName }
            }
            else {
                $targetSchema = $schemaParam
                $targetTable  = if ([string]::IsNullOrWhiteSpace($srcSchem)) { $srcName } else { "$($srcSchem)_$($srcName)" }
            }

            $record = [pscustomobject]@{
                LinkedServer = $LinkedServer
                SourceType   = if ($srcType -eq 'VIEW') { 'View' } else { 'Table' }
                SourceObject = $sourceObject
                TargetSchema = $targetSchema
                TargetTable  = $targetTable
                Columns      = 0
                PrimaryKey   = if ($NoPrimaryKey) { '' } else { "$($targetTable)_id" }
                Status       = 'Failed'
                Message      = ''
            }

            try {
                # --- Ensure the destination schema exists -----------------
                if ($ensuredSchemas.Add($targetSchema)) {
                    Invoke-DbaQuery -SqlInstance $conn -Database $NewDatabase -ErrorAction Stop -Query (
                        "IF SCHEMA_ID($(Get-LiteralString $targetSchema)) IS NULL EXEC('CREATE SCHEMA $(Get-BracketName $targetSchema)');"
                    )
                }

                # --- Columns of this source object ------------------------
                $colSql = "EXEC sp_columns_ex @table_server = $(Get-LiteralString $LinkedServer), @table_name = $(Get-LiteralString $srcName)"
                if ($srcSchem) { $colSql += ", @table_schema = $(Get-LiteralString $srcSchem)" }
                if ($srcCat)   { $colSql += ", @table_catalog = $(Get-LiteralString $srcCat)" }
                $colSql += ';'

                $columns = @(Invoke-DbaQuery -SqlInstance $conn -ErrorAction Stop -Query $colSql)
                $record.Columns = $columns.Count

                if ($columns.Count -eq 0) {
                    $record.Status  = 'Skipped'
                    $record.Message = 'No columns returned by the provider.'
                    $results.Add($record)
                    Write-Verbose "[$sourceObject] skipped - no columns."
                    continue
                }

                # --- Skip if the target table already exists --------------
                $targetFull    = "$(Get-BracketName $targetSchema).$(Get-BracketName $targetTable)"
                $targetFullLit = "N'" + ($targetFull -replace "'", "''") + "'"
                $existsRow = Invoke-DbaQuery -SqlInstance $conn -Database $NewDatabase -ErrorAction Stop -Query (
                    "SELECT c = CASE WHEN OBJECT_ID($targetFullLit,'U') IS NULL THEN 0 ELSE 1 END"
                )
                if ((Get-RowInt $existsRow 'c' 0) -eq 1) {
                    if (-not $Overwrite) {
                        $record.Status  = 'Skipped'
                        $record.Message = "Target table $targetFull already exists (use -Overwrite to replace it)."
                        $results.Add($record)
                        Write-Verbose "[$sourceObject] skipped - target already exists."
                        continue
                    }

                    Write-Verbose "[$sourceObject] -Overwrite: dropping existing $targetFull."
                    Invoke-DbaQuery -SqlInstance $conn -Database $NewDatabase -ErrorAction Stop -Query (
                        "IF OBJECT_ID($targetFullLit,'U') IS NOT NULL DROP TABLE $targetFull;"
                    )
                }

                # --- Build the column list --------------------------------
                $columnLines = New-Object System.Collections.Generic.List[string]

                if (-not $NoPrimaryKey) {
                    $pkColumn     = "$($targetTable)_id"
                    $pkConstraint = "PK_$($targetSchema)_$($targetTable)"
                    $columnLines.Add(
                        "    $(Get-BracketName $pkColumn) bigint IDENTITY(1,1) NOT NULL CONSTRAINT $(Get-BracketName $pkConstraint) PRIMARY KEY"
                    )
                }

                # Track column names so duplicates (some providers expose the same
                # column twice) can be handled per -IgnoreDuplicateColumn. Seed with
                # the surrogate PK name so a source column colliding with it is also
                # treated as a duplicate.
                $seenColumns   = New-Object 'System.Collections.Generic.HashSet[string]' ([System.StringComparer]::OrdinalIgnoreCase)
                $duplicateCols = New-Object System.Collections.Generic.List[string]
                if (-not $NoPrimaryKey) { [void]$seenColumns.Add($pkColumn) }

                foreach ($c in ($columns | Sort-Object { Get-RowInt $_ 'ORDINAL_POSITION' 0 })) {
                    $colName  = Get-RowString $c 'COLUMN_NAME'
                    if ([string]::IsNullOrWhiteSpace($colName)) { continue }

                    if (-not $seenColumns.Add($colName)) {
                        # Same column name already used - keep the first, note the duplicate.
                        $duplicateCols.Add($colName)
                        continue
                    }

                    $odbcType = Get-RowInt $c 'DATA_TYPE' 0
                    $size     = Get-RowInt $c 'COLUMN_SIZE' 0
                    $decimals = Get-RowInt $c 'DECIMAL_DIGITS' 0
                    $nullable = Get-RowInt $c 'NULLABLE' 1

                    $sqlType  = ConvertTo-MSSqlColumnType -OdbcType $odbcType -Size $size -Decimals $decimals
                    $nullText = if ($nullable -eq 0) { 'NOT NULL' } else { 'NULL' }

                    $columnLines.Add("    $(Get-BracketName $colName) $sqlType $nullText")
                }

                # Handle duplicate column names found above.
                if ($duplicateCols.Count -gt 0) {
                    $dupText = ($duplicateCols | Select-Object -Unique) -join ', '
                    if (-not $IgnoreDuplicateColumn) {
                        Write-Warning "[$sourceObject] duplicate column name(s): $dupText. Table NOT created (use -IgnoreDuplicateColumn to keep the first occurrence)."
                        $record.Status  = 'Skipped'
                        $record.Message = "Duplicate column(s): $dupText. Table not created (use -IgnoreDuplicateColumn)."
                        $results.Add($record)
                        continue
                    }
                    else {
                        Write-Warning "[$sourceObject] duplicate column name(s) ignored (kept first): $dupText."
                    }
                }

                if ($columnLines.Count -eq 0) {
                    $record.Status  = 'Skipped'
                    $record.Message = 'No usable columns to create.'
                    $results.Add($record)
                    continue
                }

                # --- Create the table -------------------------------------
                $createSql = "CREATE TABLE $(Get-BracketName $targetSchema).$(Get-BracketName $targetTable) (`r`n" +
                             ($columnLines -join ",`r`n") +
                             "`r`n);"

                Write-Verbose "[$sourceObject] creating $(Get-BracketName $targetSchema).$(Get-BracketName $targetTable) ($($columns.Count) column(s))..."
                Invoke-DbaQuery -SqlInstance $conn -Database $NewDatabase -ErrorAction Stop -Query $createSql

                $record.Status  = 'Created'
                $record.Message = if ($duplicateCols.Count -gt 0) {
                    "Created; ignored duplicate column(s): $(($duplicateCols | Select-Object -Unique) -join ', ')."
                }
                else {
                    "Created with $($columns.Count) column(s)."
                }
                $results.Add($record)
            }
            catch {
                $record.Status  = 'Failed'
                $record.Message = $_.Exception.Message
                $results.Add($record)
                Write-Verbose "[$sourceObject] FAILED: $($_.Exception.Message)"
            }
        }

        $created = @($results | Where-Object { $_.Status -eq 'Created' }).Count
        $skipped = @($results | Where-Object { $_.Status -eq 'Skipped' }).Count
        $failed  = @($results | Where-Object { $_.Status -eq 'Failed' }).Count
        Write-Verbose ("Done. Created={0} Skipped={1} Failed={2} (Total={3})." -f $created, $skipped, $failed, $results.Count)

        return $results
    }
    catch {
        throw "New-MSLinkedServerDatabaseStructure failed: $($_.Exception.Message)"
    }
}
