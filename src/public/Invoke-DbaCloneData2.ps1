function Invoke-DbaCloneData2 {
<#
.SYNOPSIS
    Copies data for selected/all user tables from a source database to a destination database.
#>
    [CmdletBinding(SupportsShouldProcess)]
    param(
        [Parameter(Mandatory)][string]$SourceInstance,
        [Parameter(Mandatory)][string]$SourceDatabase,
        [Parameter(Mandatory)][string]$DestInstance,
        [Parameter(Mandatory)][string]$DestDatabase,

        [int]$BatchSize = 100000,
        [int]$NotifyAfter = 100000,
        [int]$CommandTimeout = 0,

        [switch]$KeepIdentity,
        [switch]$KeepNulls,
        [switch]$TruncateDestination,
        [switch]$AutoCreateMissingTables,
        [switch]$PassThru,

        [string[]]$Schema,
        [string[]]$IncludeTable,
        [string[]]$ExcludeTable,

        [switch]$UseOpenRowset,
        [string]$SourceUser,
        [string]$SourcePassword,
        [hashtable]$TypeFixMap
    )

    # -------- Version-compat helpers -----------------------------------
    $script:HasCmdTimeout_Inv = ($cmd = Get-Command Invoke-DbaQuery -ErrorAction SilentlyContinue) -and ($cmd.Parameters.ContainsKey('CommandTimeout'))
    $script:HasCmdTimeout_Cpy = ($cmd = Get-Command Copy-DbaDbTableData -ErrorAction SilentlyContinue) -and ($cmd.Parameters.ContainsKey('CommandTimeout'))

    function Invoke-Dbq {
        param(
            [string]$Instance,[string]$Database,[string]$Query,
            [ValidateSet('DataSet','DataTable','DataRow','PSObject','SingleValue')][string]$As,
            [switch]$EnableException
        )
        $base = @{
            SqlInstance = $Instance
            Database    = $Database
            Query       = $Query
        }
        if ($EnableException) { $base.EnableException = $true }
        if ($As) { $base.As = $As }
        if ($script:HasCmdTimeout_Inv) { $base.CommandTimeout = $CommandTimeout } else { $base.QueryTimeout = $CommandTimeout }
        Invoke-DbaQuery @base
    }

    function Copy-DbaDbTableDataCompat {
        param([object]$InputObject)
        $p = @{
            Destination         = $DestInstance
            DestinationDatabase = $DestDatabase
            BatchSize           = $BatchSize
            NotifyAfter         = $NotifyAfter
            KeepIdentity        = [bool]$KeepIdentity
            KeepNulls           = [bool]$KeepNulls
            Truncate            = [bool]$TruncateDestination
            AutoCreateTable     = [bool]$AutoCreateMissingTables
            EnableException     = $true
            Confirm             = $false
            Verbose             = $false
        }
        if ($script:HasCmdTimeout_Cpy) { $p.CommandTimeout = $CommandTimeout } else { $p.QueryTimeout = $CommandTimeout }
        $InputObject | Copy-DbaDbTableData @p
    }

    # -------- Helpers (PS 5.1-safe) ------------------------------------
    function _matches {
        param([string]$schema,[string]$name,[string[]]$patterns)
        if (-not $patterns -or $patterns.Count -eq 0) { return $true }
        foreach ($p in $patterns) {
            if ($p -like '*.*') { if ("$schema.$name" -ieq $p) { return $true } }
            else { if ($name -ieq $p) { return $true } }
        }
        return $false
    }

    function _normKey([string]$sch,[string]$tbl) {
        if ([string]::IsNullOrWhiteSpace($sch)) { return ("dbo."+$tbl).ToLowerInvariant() }
        return ("$sch.$tbl").ToLowerInvariant()
    }

    function Get-SourceTables {
        $tables = Get-DbaDbTable -SqlInstance $SourceInstance -Database $SourceDatabase
        if ($Schema)       { $tables = $tables | Where-Object { $Schema -contains $_.Schema } }
        if ($IncludeTable) { $tables = $tables | Where-Object { _matches $_.Schema $_.Name $IncludeTable } }
        if ($ExcludeTable) { $tables = $tables | Where-Object { -not (_matches $_.Schema $_.Name $ExcludeTable) } }
        $tables
    }

    function Get-TableSpecForOpenRowset {
        param([string]$Sch,[string]$Tbl)

        $colQ = @"
SELECT 
  c.name         AS ColName,
  t.name         AS TypeName,
  c.column_id    AS ColumnId,
  c.is_identity  AS IsIdentity,
  CASE WHEN t.name IN ('varchar','nvarchar','char','nchar','text','ntext') THEN 1 ELSE 0 END AS IsString
FROM sys.columns c
JOIN sys.types   t ON t.user_type_id = c.user_type_id
WHERE c.object_id = OBJECT_ID(QUOTENAME('$Sch') + '.' + QUOTENAME('$Tbl'))
ORDER BY c.column_id;
"@
        $cols = Invoke-Dbq -Instance $SourceInstance -Database $SourceDatabase -Query $colQ -As PSObject -EnableException
        if (-not $cols -or $cols.Count -eq 0) { throw "No columns for $Sch.$Tbl" }

        $tblKey = _normKey $Sch $Tbl
        $over = @{}
        if ($TypeFixMap -and $TypeFixMap.ContainsKey($tblKey)) { $over = $TypeFixMap[$tblKey] }

        $destCols = ($cols | ForEach-Object { '[' + $_.ColName + ']' }) -join ', '
        $selectCols = ($cols | ForEach-Object {
            $c = $_.ColName
            if ($over.ContainsKey($c)) { $over[$c] }
            elseif ($_.IsString -eq 1) { "S.[$c] COLLATE DATABASE_DEFAULT" }
            else { "S.[$c]" }
        }) -join ', '

        $key = $null
        $id = $cols | Where-Object IsIdentity | Select-Object -First 1
        if ($id) {
            $t = ($id.TypeName).ToLower()
            if ($t -in @('int','bigint','numeric','decimal')) { $key = $id.ColName }
        }

        $hasId = Invoke-Dbq -Instance $DestInstance -Database $DestDatabase -Query @"
SELECT 1
FROM sys.columns
WHERE object_id = OBJECT_ID(QUOTENAME('$Sch').'.'+QUOTENAME('$Tbl')) AND is_identity = 1;
"@ -As SingleValue

        New-Object psobject -Property @{
            DestCols        = $destCols
            SelectCols      = $selectCols
            BatchKey        = $key
            HasIdentityDest = ($hasId -eq 1)
        }
    }

    # Connection string for OPENROWSET
    $openConn = if ($SourceUser -and $SourcePassword) {
        "Server=$SourceInstance;User ID=$SourceUser;Password=$SourcePassword;"
    } else {
        "Server=$SourceInstance;Trusted_Connection=Yes;"
    }

    $mode = if ($UseOpenRowset) { 'OPENROWSET' } else { 'dbatools' }
    Write-Log -Message ("Copy user tables from {0} [{1}] to {2} [{3}] (Mode={4})" -f $SourceDatabase,$SourceInstance,$DestDatabase,$DestInstance,$mode) -Level Info

    $tables = Get-SourceTables
    $total  = ($tables | Measure-Object).Count
    Write-Log -Message ("Tables selected for copy: {0}" -f $total) -Level Info

    $start  = Get-Date
    $ok     = New-Object System.Collections.Generic.List[object]
    $failed = New-Object System.Collections.Generic.List[object]

    $i = 0
    foreach ($t in $tables) {
        $i++
        $sch = $t.Schema
        $tbl = $t.Name
        $fq  = "[$sch].[$tbl]"
        Write-Log -Message ("[{0}/{1}] Copying {2} ..." -f $i,$total,$fq) -Level Info

        if (-not $PSCmdlet.ShouldProcess("$DestInstance/$DestDatabase", "Copy $fq")) { continue }

        try {
            # Two-part name for OBJECT_ID()
            $destTwoPart = "[$sch].[$tbl]"
            $destThree   = "[$DestDatabase].[$sch].[$tbl]"

            $exists  = Invoke-Dbq -Instance $DestInstance -Database $DestDatabase -Query "SELECT 1 WHERE OBJECT_ID('$destTwoPart','U') IS NOT NULL;" -As SingleValue
            if ($exists -ne 1 -and -not $AutoCreateMissingTables) {
                throw "Destination table $fq not found (use -AutoCreateMissingTables to create it with dbatools)."
            }

            if ($TruncateDestination) {
                try {
                    Invoke-Dbq -Instance $DestInstance -Database $DestDatabase -Query "TRUNCATE TABLE $destThree;" -EnableException
                } catch {
                    Invoke-Dbq -Instance $DestInstance -Database $DestDatabase -Query "DELETE FROM $destThree;" -EnableException
                }
            }

            if (-not $UseOpenRowset) {
                # dbatools engine
                Copy-DbaDbTableDataCompat -InputObject $t
            }
            else {
                # OPENROWSET engine
                $spec = Get-TableSpecForOpenRowset -Sch $sch -Tbl $tbl

                $idOn  = ($KeepIdentity -and $spec.HasIdentityDest)
                $idPre = ""; $idSuf = ""
                if ($idOn) { $idPre = "SET IDENTITY_INSERT $destThree ON;"; $idSuf = "SET IDENTITY_INSERT $destThree OFF;" }

                $innerSelect = ("SELECT {0} FROM [{1}].[{2}].[{3}] WITH (NOLOCK)" -f $spec.SelectCols,$SourceDatabase,$sch,$tbl)
                $innerEsc    = $innerSelect.Replace("'", "''")
                $fromClause  = "FROM OPENROWSET('MSOLEDBSQL', '$openConn', '$innerEsc') AS S"

                $effectiveBatchSize = $BatchSize

                if ($effectiveBatchSize -gt 0 -and $spec.BatchKey) {
                    $mmInner = ("SELECT MIN([{0}]) AS MinK, MAX([{0}]) AS MaxK FROM [{1}].[{2}].[{3}] WITH (NOLOCK)" -f $spec.BatchKey,$SourceDatabase,$sch,$tbl)
                    $mmEsc   = $mmInner.Replace("'", "''")
                    $mmQ     = "SELECT * FROM OPENROWSET('MSOLEDBSQL', '$openConn', '$mmEsc') AS B"
                    $mm      = Invoke-Dbq -Instance $DestInstance -Database $DestDatabase -Query $mmQ -As DataRow

                    if ($mm -and $mm.MinK -ne $null -and $mm.MaxK -ne $null) {
                        $startK = [decimal]$mm.MinK; $maxK = [decimal]$mm.MaxK
                        while ($startK -le $maxK) {
                            $endK = $startK + $effectiveBatchSize - 1
                            $whereClause = ("WHERE S.[{0}] BETWEEN {1} AND {2}" -f $spec.BatchKey,$startK,$endK)
                            $tsql = @"
BEGIN TRY
  $idPre
  INSERT INTO $destThree WITH (TABLOCK) ($($spec.DestCols))
  SELECT $($spec.SelectCols)
  $fromClause
  $whereClause;
  $idSuf
END TRY
BEGIN CATCH
  $idSuf
  THROW;
END CATCH;
"@
                            Invoke-Dbq -Instance $DestInstance -Database $DestDatabase -Query $tsql -EnableException
                            Write-Log -Message ("  -> {0} [{1}..{2}] copied" -f $fq,$startK,$endK) -Level Info
                            $startK = $endK + 1
                        }
                    } else {
                        Write-Log -Message ("No batch bounds detected for {0}; falling back to single-shot." -f $fq) -Level Warning
                        $effectiveBatchSize = 0
                    }
                }

                if ($effectiveBatchSize -eq 0 -or -not $spec.BatchKey) {
                    $tsql = @"
BEGIN TRY
  $idPre
  INSERT INTO $destThree WITH (TABLOCK) ($($spec.DestCols))
  SELECT $($spec.SelectCols)
  $fromClause;
  $idSuf
END TRY
BEGIN CATCH
  $idSuf
  THROW;
END CATCH;
"@
                    Invoke-Dbq -Instance $DestInstance -Database $DestDatabase -Query $tsql -EnableException
                }
            }

            $ok.Add([pscustomobject]@{ Table=$fq; Status='OK' })
        }
        catch {
            $msg = $_.Exception.Message
            Write-Log -Message ("FAILED to copy {0} : {1}" -f $fq,$msg) -Level Error
            $failed.Add([pscustomobject]@{ Table=$fq; Status='FAIL'; Error=$msg })
            continue
        }
    }

    $dur = (Get-Date) - $start
    Write-Log -Message ("Duration to copy tables: {0:c}" -f $dur) -Level Warning
    Write-Log -Message ("Summary: OK={0}  FAIL={1}" -f $ok.Count,$failed.Count) -Level Warning

    if ($failed.Count -gt 0) {
        $failed | ForEach-Object { Write-Log -Message ("  - {0} :: {1}" -f $_.Table,$_.Error) -Level Warning }
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
