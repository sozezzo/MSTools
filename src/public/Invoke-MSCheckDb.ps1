function Invoke-MSCheckDb {
<#
.SYNOPSIS
    Executes DBCC CHECKDB safely on SQL Server databases with controlled modes,
    detailed logging, and result objects suitable for monitoring and automation.

.DESCRIPTION
    Invoke-MSCheckDb runs DBCC CHECKDB against one or more SQL Server instances
    and databases, using a controlled execution model designed for day-to-day
    operations and incident escalation.

    Supported CHECKDB modes:
      - PHYSICAL_ONLY  (default, lightweight integrity signal)
      - ESTIMATEONLY   (planning only, no validation)
      - FULL           (full logical + physical validation)

    The function NEVER performs repair operations and NEVER uses TABLOCK.

    A freshness gate is applied by default: if a database has a successful
    CHECKDB within the last N days (default: 7), execution is skipped unless
    ESTIMATEONLY is used.

    Databases that cannot or should not be checked are recorded as 'Skipped'
    (never 'Failed'): OFFLINE (or any non-ONLINE state), READ_ONLY, or a
    database mirroring / Availability Group SECONDARY replica.

    System databases (master, model, msdb) are excluded by default but can be
    included explicitly.

    IMPORTANT: tempdb is ALWAYS excluded.

    DBCC CHECKDB MUST NOT be run on tempdb because:
      - tempdb is recreated at every SQL Server startup
      - it contains no persistent or recoverable data
      - corruption in tempdb indicates engine, memory, or storage failure
      - CHECKDB provides no corrective or diagnostic value for tempdb
      - the only remediation for tempdb corruption is SQL Server restart
        and correction of underlying system issues

    Excluding tempdb is intentional, mandatory, and aligned with Microsoft
    guidance and industry best practices.

.PARAMETER SqlInstance
    One or more SQL Server instances.

.PARAMETER Database
    Optional list of databases to include.

.PARAMETER ExcludedDatabase
    Optional list of databases to exclude.

.PARAMETER Credential
    Optional SQL credential.

.PARAMETER CheckType
    CHECKDB execution mode:
      PHYSICAL_ONLY (default)
      ESTIMATEONLY
      FULL

.PARAMETER MaxDop
    Optional MAXDOP value for CHECKDB execution.
    Ignored for ESTIMATEONLY.

.PARAMETER Timeout
    Optional query timeout in seconds.

.PARAMETER MaxCheckDbAgeDays
    Number of days after which CHECKDB is considered stale.
    Default is 7 days. Set to 0 to always run.

.PARAMETER IncludeSystemDatabase
    Include master, model, and msdb.
    tempdb is never included.

.PARAMETER EnableException
    Throw exceptions instead of returning failed result objects.

.OUTPUTS
    PSCustomObject per database execution with detailed status, timing,
    and diagnostic information.

.EXAMPLE
    Invoke-MSCheckDb -SqlInstance sql-test01

    Runs DBCC CHECKDB using the default mode PHYSICAL_ONLY against all user databases
    on sql-test01.

    System databases are excluded.
    tempdb is always excluded.
    Databases checked successfully within the last 7 days are skipped.

.EXAMPLE
    Invoke-MSCheckDb -SqlInstance sql-test01 -Verbose

    Runs the default PHYSICAL_ONLY check and displays detailed execution steps.

    Useful for interactive validation or troubleshooting.

.EXAMPLE
    Invoke-MSCheckDb -SqlInstance sql-test01 -Database MyDatabase

    Runs DBCC CHECKDB using PHYSICAL_ONLY only against the MyDatabase database.

.EXAMPLE
    Invoke-MSCheckDb -SqlInstance sql-test01 -Database MyDatabase,ERPProd -MaxDop 2

    Runs DBCC CHECKDB using PHYSICAL_ONLY against MyDatabase and ERPProd with MAXDOP = 2.

    This is useful for reducing CPU pressure during online checks.

.EXAMPLE
    Invoke-MSCheckDb -SqlInstance sql-test01 -MaxCheckDbAgeDays 0

    Forces CHECKDB execution even if the database was checked successfully recently.

    Use this when you intentionally want to ignore the freshness gate.

.EXAMPLE
    Invoke-MSCheckDb -SqlInstance sql-test01 -MaxCheckDbAgeDays 14

    Skips databases that had a successful CHECKDB within the last 14 days.

.EXAMPLE
    Invoke-MSCheckDb -SqlInstance sql-test01 -IncludeSystemDatabase

    Includes master, model, and msdb in the CHECKDB execution.

    tempdb is still excluded by design.

.EXAMPLE
    Invoke-MSCheckDb -SqlInstance sql-test01 -ExcludedDatabase ArchiveDB,OldReports

    Runs CHECKDB against all selected user databases except ArchiveDB and OldReports.

.EXAMPLE
    Invoke-MSCheckDb -SqlInstance sql-test01 -CheckType ESTIMATEONLY

    Runs DBCC CHECKDB WITH ESTIMATEONLY.

    This does not validate database integrity.
    It only estimates the resources required for a CHECKDB operation.

.EXAMPLE
    Invoke-MSCheckDb -SqlInstance sql-test01 -CheckType FULL -Database MyDatabase

    Runs a full DBCC CHECKDB against MyDatabase.

    Use this mode deliberately, normally during a maintenance window or after an incident.

.EXAMPLE
    $cred = Get-Credential
    Invoke-MSCheckDb -SqlInstance sql-test01 -Credential $cred

    Runs CHECKDB using the supplied SQL credential.

.EXAMPLE
    $cred = Get-Credential
    Invoke-MSCheckDb `
        -SqlInstance sql-test01,sql-test02 `
        -Credential $cred `
        -CheckType PHYSICAL_ONLY `
        -MaxDop 2 `
        -Verbose

    Runs PHYSICAL_ONLY CHECKDB against multiple SQL Server instances using the supplied
    credential, limits CHECKDB to MAXDOP = 2, and displays detailed execution logging.

.EXAMPLE
    $result = Invoke-MSCheckDb -SqlInstance sql-test01 -Verbose
    $result | Where-Object Status -ne 'Success'

    Captures the result array and filters databases where CHECKDB did not complete
    successfully.

.EXAMPLE
    $result = Invoke-MSCheckDb -SqlInstance sql-test01
    $result | Export-Csv C:\Temp\checkdb-results.csv -NoTypeInformation

    Runs CHECKDB and exports the structured result objects to a CSV file for later review
    or integration with monitoring controls.

.NOTES
    Author  : Sozezzo Astra
    Purpose : Safe, auditable, automation-friendly CHECKDB execution
    Safety  : No repair, no TABLOCK, no silent behavior
#>

    [CmdletBinding(SupportsShouldProcess)]
    param(
        [Parameter(Mandatory)]
        [string[]]$SqlInstance,

        [string[]]$Database,

        [string[]]$ExcludedDatabase,

        [PSCredential]$Credential,

        [ValidateSet('PHYSICAL_ONLY','ESTIMATEONLY','FULL')]
        [string]$CheckType = 'PHYSICAL_ONLY',

        [ValidateRange(0,[int]::MaxValue)]
        [int]$MaxDop,

        [int]$Timeout = 0,

        [ValidateRange(0,[int]::MaxValue)]
        [int]$MaxCheckDbAgeDays = 7,

        [switch]$IncludeSystemDatabase,

        [switch]$EnableException
    )

    # ------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------
    function New-Result {
        param(
            [string]$Instance,
            [string]$Database,
            [string]$CheckType,
            [datetime]$Start,
            [datetime]$End,
            [int]$MaxDop,
            [int]$Timeout,
            [string]$Status,
            [string]$ErrorMessage,
            [Nullable[bool]]$CorruptionDetected,
            [string]$Notes
        )

        [pscustomobject]@{
            SqlInstance        = $Instance
            Database           = $Database
            CheckType          = $CheckType
            StartTime          = $Start
            EndTime            = $End
            DurationSeconds    = [int]($End - $Start).TotalSeconds
            MaxDop             = $MaxDop
            Timeout            = $Timeout
            Status             = $Status
            ErrorMessage       = $ErrorMessage
            CorruptionDetected = $CorruptionDetected
            Notes              = $Notes
        }
    }

    function Classify-CheckDbError {
        param([string]$Message)

        if ($Message -match 'log is full|LOG_BACKUP|snapshot|exclusive|checkpoint|could not be checked') {
            return 'Blocked'
        }
        return 'Failed'
    }

    function Get-LastCheckDb {
        param(
            $Server,
            [string]$Database
        )

        $dbSafe = $Database.Replace("'", "''")

    $sql = @"
DECLARE @dbinfo TABLE
(
    ParentObject nvarchar(255),
    ObjectName   nvarchar(255),
    Field        nvarchar(255),
    Value        nvarchar(255)
);

INSERT INTO @dbinfo (ParentObject, ObjectName, Field, Value)
EXEC ('DBCC DBINFO (''$dbSafe'') WITH TABLERESULTS, NO_INFOMSGS');

SELECT
    CASE WHEN ISDATE(Value) = 1 THEN CONVERT(datetime, Value) ELSE NULL END AS LastCheckDb
FROM @dbinfo
WHERE Field = 'dbi_dbccLastKnownGood';
"@

        try {
            Invoke-DbaQuery `
                -SqlInstance $Server `
                -Database $Database `
                -Query $sql `
                -EnableException |
            Select-Object -ExpandProperty LastCheckDb
        }
        catch {
            return $null
        }
    }

    function Connect-MSCheckDbInstance {
        param(
            [string]$Instance,
            [PSCredential]$Credential
        )

        # For SQL logins we connect with a CONNECTION STRING using
        # Authentication=SqlPassword instead of -SqlCredential. dbatools treats
        # any -SqlCredential user name containing '@' (e.g. 'DBADash@DBADash') as
        # an Azure AD account and abandons SQL-login auth, which then fails with
        # "The user name or password is incorrect" even when the password is
        # correct. A connection string forces real SQL authentication and is
        # immune to that '@' heuristic. This mirrors Backup-AMPLCDatabase.
        if ($Credential) {
            $csb = New-Object System.Data.SqlClient.SqlConnectionStringBuilder
            $csb['Data Source']            = [string]$Instance
            $csb['Initial Catalog']        = 'master'
            $csb['User ID']                = [string]$Credential.UserName
            $csb['Password']               = [string]$Credential.GetNetworkCredential().Password
            $csb['Encrypt']                = $false
            $csb['TrustServerCertificate'] = $true
            $csb['Application Name']       = 'Invoke-MSCheckDb'
            $connectionString = $csb.ConnectionString + ';Authentication=SqlPassword'
            return Connect-DbaInstance -ConnectionString $connectionString -ErrorAction Stop
        }

        return Connect-DbaInstance -SqlInstance $Instance -TrustServerCertificate -ErrorAction Stop
    }

    # ------------------------------------------------------------
    # Start
    # ------------------------------------------------------------
    $results = @()
    $runId = [guid]::NewGuid().ToString()

    write-host "Invoke-MSCheckDb started (RunId=$runId)"
    write-host "CheckType=$CheckType MaxDop=$MaxDop Timeout=$Timeout MaxCheckDbAgeDays=$MaxCheckDbAgeDays IncludeSystemDatabase=$IncludeSystemDatabase"

    foreach ($instance in $SqlInstance) {

        # --------------------------------------------------------
        # INIT
        # --------------------------------------------------------
        Write-host "[$instance][INIT] Connecting to instance"

        $conn = $null
        try {
            $conn = Connect-MSCheckDbInstance -Instance $instance -Credential $Credential
            Write-Verbose "[$instance][INIT] Connection OK"
        }
        catch {
            $msg = $_.Exception.Message
            Write-Error "[$instance][INIT] Connection failed: $msg"

            $results += New-Result `
                -Instance $instance -Database '' -CheckType $CheckType `
                -Start (Get-Date) -End (Get-Date) `
                -MaxDop $MaxDop -Timeout $Timeout `
                -Status 'Failed' -ErrorMessage $msg `
                -CorruptionDetected $null -Notes 'Connection failure'

            if ($EnableException) { throw }
            continue
        }

        # --------------------------------------------------------
        # RESOLVE DATABASES
        # --------------------------------------------------------
        Write-Verbose "[$instance][RESOLVE] Resolving databases"

        try {
            $dbs = Get-DbaDatabase -SqlInstance $conn -EnableException

            # Always exclude tempdb
            $dbs = $dbs | Where-Object { $_.Name -ne 'tempdb' }

            # Exclude system DBs unless explicitly requested
            if (-not $IncludeSystemDatabase) {
                $dbs = $dbs | Where-Object { $_.IsSystemObject -eq $false }
            }

            if ($Database) {
                $dbs = $dbs | Where-Object { $_.Name -in $Database }
            }

            if ($ExcludedDatabase) {
                $dbs = $dbs | Where-Object { $_.Name -notin $ExcludedDatabase }
            }

            $dbList = $dbs.Name | Sort-Object -Unique

            # Skip - do NOT report as an error - databases that cannot or should
            # not be checked: OFFLINE (or any non-ONLINE state), READ_ONLY, or a
            # database mirroring / Availability Group SECONDARY. DBCC CHECKDB on
            # those either fails or is meaningless, so they are recorded as
            # 'Skipped' with a reason instead of 'Failed'.
            if ($dbList) {
                try {
                    $nameList = ($dbList | ForEach-Object { "'" + ($_ -replace "'", "''") + "'" }) -join ','

                    $stateQuery = @"
SELECT db_name             = d.name,
       status              = CONVERT(sysname, DATABASEPROPERTYEX(d.name, 'Status')),
       updateability       = CONVERT(sysname, DATABASEPROPERTYEX(d.name, 'Updateability')),
       mirroring_role_desc = m.mirroring_role_desc,
       ag_role             = ars.role_desc
FROM sys.databases d
LEFT JOIN sys.database_mirroring m
       ON m.database_id = d.database_id
LEFT JOIN sys.dm_hadr_database_replica_states drs
       ON drs.database_id = d.database_id AND drs.is_local = 1
LEFT JOIN sys.dm_hadr_availability_replica_states ars
       ON ars.replica_id = drs.replica_id AND ars.is_local = 1
WHERE d.name IN ($nameList)
"@

                    $stateRows = Invoke-DbaQuery -SqlInstance $conn -Query $stateQuery -EnableException

                    foreach ($row in $stateRows) {
                        $skipReason = $null
                        if ("$($row.status)" -ne 'ONLINE') {
                            $skipReason = "database is $($row.status) (not ONLINE)"
                        }
                        elseif ("$($row.updateability)" -eq 'READ_ONLY') {
                            $skipReason = 'database is READ_ONLY'
                        }
                        elseif ("$($row.mirroring_role_desc)" -eq 'MIRROR') {
                            $skipReason = 'database mirroring SECONDARY (mirror role)'
                        }
                        elseif ("$($row.ag_role)" -eq 'SECONDARY') {
                            $skipReason = 'Availability Group SECONDARY replica'
                        }

                        if ($skipReason) {
                            $skipName = [string]$row.db_name
                            Write-Warning "[$instance][$skipName][SKIP] $skipReason"

                            $results += New-Result `
                                -Instance $instance -Database $skipName -CheckType $CheckType `
                                -Start (Get-Date) -End (Get-Date) `
                                -MaxDop $MaxDop -Timeout $Timeout `
                                -Status 'Skipped' -ErrorMessage $null `
                                -CorruptionDetected $null -Notes $skipReason

                            $dbList = @($dbList | Where-Object { $_ -ne $skipName })
                        }
                    }
                }
                catch {
                    Write-Warning "[$instance][RESOLVE] Could not evaluate offline/read-only/secondary states; checking all selected databases. $($_.Exception.Message)"
                }
            }

            Write-Verbose "[$instance][RESOLVE] Databases selected: $($dbList -join ', ')"

            if (-not $dbList) {
                Write-Warning "[$instance][RESOLVE] No databases selected"
                continue
            }
        }
        catch {
            $msg = $_.Exception.Message
            Write-Error "[$instance][RESOLVE] Failed: $msg"
            if ($EnableException) { throw }
            continue
        }

        # --------------------------------------------------------
        # Effective MAXDOP
        # --------------------------------------------------------
        $effectiveMaxDop = 0
        if ($CheckType -ne 'ESTIMATEONLY') {
            $effectiveMaxDop = if ($MaxDop -gt 0) { $MaxDop } else { 4 }
        }

        # --------------------------------------------------------
        # EXECUTION PER DATABASE
        # --------------------------------------------------------
        foreach ($db in $dbList) {

            $start = Get-Date
            $status = 'Success'
            $err = $null
            $corrupt = $null
            $notes = $null

            # ----------------------------------------------------
            # PRECHECK – freshness gate
            # ----------------------------------------------------
            if ($CheckType -ne 'ESTIMATEONLY' -and $MaxCheckDbAgeDays -gt 0) {

                Write-Verbose "[$instance][$db][PRECHECK] Reading last successful CHECKDB"

                $lastCheckDb = Get-LastCheckDb -Server $conn -Database $db

                Write-Verbose "[$instance][$db][PRECHECK] lastCheckDb = $lastCheckDb"

                if ($lastCheckDb) {
                    $ageDays = [int]((Get-Date) - $lastCheckDb).TotalDays
                    Write-warning "[$instance][$db][PRECHECK] Last CHECKDB: $lastCheckDb ($ageDays days ago)"

                    if ($ageDays -le $MaxCheckDbAgeDays) {

                        Write-warning "[$instance][$db][SKIP] Skipped (last CHECKDB $ageDays days ago)"

                        $results += New-Result `
                            -Instance $instance -Database $db -CheckType $CheckType `
                            -Start $start -End (Get-Date) `
                            -MaxDop $effectiveMaxDop -Timeout $Timeout `
                            -Status 'Skipped' -ErrorMessage $null `
                            -CorruptionDetected $null `
                            -Notes "Last CHECKDB $ageDays days ago"

                        continue
                    }
                }
            }

            # ----------------------------------------------------
            # BUILD SQL
            # ----------------------------------------------------
            Write-host "[$instance][$db][EXEC] CHECKDB starting ($CheckType)"

            $dbEsc = $db.Replace(']', ']]')

            $sql = switch ($CheckType) {
                'PHYSICAL_ONLY' {
                    "DBCC CHECKDB ([$dbEsc]) WITH PHYSICAL_ONLY, NO_INFOMSGS, ALL_ERRORMSGS, MAXDOP = $effectiveMaxDop;"
                }
                'FULL' {
                    "DBCC CHECKDB ([$dbEsc]) WITH NO_INFOMSGS, ALL_ERRORMSGS, MAXDOP = $effectiveMaxDop;"
                }
                'ESTIMATEONLY' {
                    $notes = 'Estimate only – no validation'
                    "DBCC CHECKDB ([$dbEsc]) WITH ESTIMATEONLY, NO_INFOMSGS;"
                }
            }

            Write-Verbose "[$instance][$db][BUILD] $sql"

            # ----------------------------------------------------
            # EXECUTE
            # ----------------------------------------------------
            try {
                if (-not $PSCmdlet.ShouldProcess("$instance / $db", "DBCC CHECKDB ($CheckType)")) {
                    $status = 'Skipped'
                    $notes  = 'WhatIf/Confirm declined'
                    Write-Verbose "[$instance][$db][EXEC] Skipped by ShouldProcess"
                    $end = Get-Date
                    $results += New-Result `
                        -Instance $instance -Database $db -CheckType $CheckType `
                        -Start $start -End $end `
                        -MaxDop $effectiveMaxDop -Timeout $Timeout `
                        -Status $status -ErrorMessage $null `
                        -CorruptionDetected $null -Notes $notes
                    continue
                }

                Write-Verbose "[$instance][$db][EXEC] Executing"

                Invoke-DbaQuery -SqlInstance $conn `
                    -Database $db -Query $sql -QueryTimeout $Timeout -EnableException | Out-Null

                if ($CheckType -ne 'ESTIMATEONLY') {
                    $corrupt = $false
                }
            }
            catch {
                $err = $_.Exception.Message
                $status = Classify-CheckDbError $err
                $corrupt = $null

                Write-Error "[$instance][$db][ERROR] $err"
                if ($EnableException) { throw }
            }

            $end = Get-Date
            Write-Information "[$instance][$db][RESULT] $status ($([int]($end - $start).TotalSeconds)s)"

            $results += New-Result `
                -Instance $instance -Database $db -CheckType $CheckType `
                -Start $start -End $end `
                -MaxDop $effectiveMaxDop -Timeout $Timeout `
                -Status $status -ErrorMessage $err `
                -CorruptionDetected $corrupt -Notes $notes
        }
    }

    write-host "Invoke-MSCheckDb completed (RunId=$runId)"
    return $results
}