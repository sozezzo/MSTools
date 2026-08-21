function Test-MSSqlBackupReadiness {
<#
.SYNOPSIS
  Deep diagnostic for a single SQL Server instance that explains WHY a backup
  may fail. Intended to be run only against the (few) servers that failed.

.DESCRIPTION
  Runs a battery of independent checks and returns one result object per check
  (Status = Pass / Fail / Warning / Info, plus a Detail message), so the caller
  can log, e-mail or display exactly what is wrong. No single failing check
  stops the others - every check is attempted and reported.

  Checks performed:
    - Host name resolution (DNS)
    - ICMP reachability (informational; ping is often blocked)
    - TCP port / SQL connectivity and version (dbatools Test-DbaConnection)
    - SQL login / authentication (authoritative, via Connect-DbaInstance)
    - Current login's backup rights: sysadmin membership and, per database, the
      BACKUP DATABASE permission (HAS_PERMS_BY_NAME)
    - Always On: is this a SECONDARY and/or a NON-preferred backup replica
    - Every database is listed with its state (accessible?, read-only?, offline/
      suspect/restoring/standby?, snapshot?, recovery model, Always On role and
      backup preference) and whether it is BACKABLE - and if not, exactly why.
      When no user database is backable it explains the "No backup result
      returned" error.
    - Database accessibility / state (offline, suspect, restoring, standby, ...)
    - Backup folder reachable by the SQL service account (server-side, xp_fileexist)
    - Backup folder reachable/writable by the caller (client-side)

.PARAMETER SqlInstance
  Instance to diagnose. Accepts HOST, HOST\INSTANCE or HOST,PORT.

.PARAMETER SqlCredential
  Optional SQL credential. When omitted, Integrated Security is used.

.PARAMETER BackupPath
  Optional root backup folder to validate (server-side and client-side).

.PARAMETER Database
  Optional list of databases to focus the accessibility checks on. When omitted,
  all user databases are considered.

.PARAMETER ConnectTimeout
  Connection timeout in seconds for the SQL connectivity checks. Default 15.

.OUTPUTS
  [pscustomobject] with properties: SqlInstance, Check, Status, Detail.

.EXAMPLE
  Test-MSSqlBackupReadiness -SqlInstance 'SRV01\PROD' -BackupPath '\\nas\backup$' -Verbose

  Runs every check against SRV01\PROD and validates the backup share, printing
  progress to the verbose stream.
#>
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)]
        [string]$SqlInstance,

        [pscredential]$SqlCredential,

        [string]$BackupPath,

        [string[]]$Database,

        [int]$ConnectTimeout = 15
    )

    $checks = New-Object System.Collections.Generic.List[object]

    function Add-Check {
        param(
            [string]$Check,
            [ValidateSet('Pass', 'Fail', 'Warning', 'Info')]
            [string]$Status,
            [string]$Detail
        )
        $checks.Add([pscustomobject]@{
            SqlInstance = $SqlInstance
            Check       = $Check
            Status      = $Status
            Detail      = $Detail
        })
        Write-Verbose ("[{0,-7}] {1} | {2}: {3}" -f $Status, $SqlInstance, $Check, $Detail)
    }

    # --- Derive host / port from a possible HOST\INSTANCE or HOST,PORT -------
    $noProtocol = $SqlInstance -replace '^(tcp|np|lpc):', ''
    $hostPart   = ($noProtocol -split '[\\,]')[0].Trim()

    # 1) DNS resolution ------------------------------------------------------
    try {
        $ips = [System.Net.Dns]::GetHostAddresses($hostPart) |
            Where-Object { $_.AddressFamily -in 'InterNetwork', 'InterNetworkV6' } |
            Select-Object -ExpandProperty IPAddressToString
        if ($ips) {
            Add-Check 'DNS resolution' 'Pass' ("{0} -> {1}" -f $hostPart, ($ips -join ', '))
        }
        else {
            Add-Check 'DNS resolution' 'Fail' ("Host '{0}' did not resolve to any IP address." -f $hostPart)
        }
    }
    catch {
        Add-Check 'DNS resolution' 'Fail' ("Cannot resolve host '{0}': {1}" -f $hostPart, $_.Exception.Message)
    }

    # 2) ICMP ping (informational - ICMP is frequently blocked) --------------
    try {
        if (Test-Connection -ComputerName $hostPart -Count 1 -Quiet -ErrorAction Stop) {
            Add-Check 'ICMP ping' 'Pass' 'Host replied to ping.'
        }
        else {
            Add-Check 'ICMP ping' 'Warning' 'No ICMP reply (may simply be blocked by a firewall).'
        }
    }
    catch {
        Add-Check 'ICMP ping' 'Warning' ("Ping failed (may be blocked): {0}" -f $_.Exception.Message)
    }

    # 3) TCP port / SQL connectivity / version (dbatools) --------------------
    try {
        $tdcParams = @{ SqlInstance = $SqlInstance; ErrorAction = 'Stop' }
        if ($SqlCredential) { $tdcParams['SqlCredential'] = $SqlCredential }
        $conn = Test-DbaConnection @tdcParams

        if ($conn) {
            if ($conn.TcpPort) {
                Add-Check 'TCP port' 'Pass' ("SQL TCP port {0} is reachable." -f $conn.TcpPort)
            }
            else {
                Add-Check 'TCP port' 'Warning' 'TCP port not determined (named instance with SQL Browser / UDP 1434 blocked?).'
            }
            if ($conn.PSObject.Properties['SqlVersion'] -and $conn.SqlVersion) {
                Add-Check 'SQL version' 'Info' ("{0}" -f $conn.SqlVersion)
            }
        }
    }
    catch {
        Add-Check 'TCP port' 'Warning' ("Test-DbaConnection failed: {0}" -f $_.Exception.Message)
    }

    # 4) SQL login / authentication (authoritative) --------------------------
    $server = $null
    try {
        $ciParams = @{
            SqlInstance            = $SqlInstance
            ConnectTimeout         = $ConnectTimeout
            TrustServerCertificate = $true
            ErrorAction            = 'Stop'
        }
        if ($SqlCredential) { $ciParams['SqlCredential'] = $SqlCredential }
        $server = Connect-DbaInstance @ciParams
        Add-Check 'SQL login' 'Pass' ("Connected as '{0}' (v{1})." -f $server.ConnectionContext.TrueLogin, $server.VersionString)
    }
    catch {
        Add-Check 'SQL login' 'Fail' ("Cannot connect / login failed: {0}" -f $_.Exception.Message)
    }

    # The remaining checks need a live connection.
    if ($server) {

        # 5) Always On backup preference per database (feeds the DB report) --
        $agPref = @{}
        try {
            if ($server.IsHadrEnabled) {
                $agQuery = @'
SELECT db_name = d.name,
       is_preferred = sys.fn_hadr_backup_is_preferred_replica(d.name),
       role_desc = ars.role_desc
FROM sys.databases d
JOIN sys.dm_hadr_database_replica_states drs ON drs.database_id = d.database_id AND drs.is_local = 1
JOIN sys.dm_hadr_availability_replica_states ars ON ars.replica_id = drs.replica_id AND ars.is_local = 1
WHERE d.database_id > 4
'@
                foreach ($r in (Invoke-DbaQuery -SqlInstance $server -Query $agQuery -ErrorAction Stop)) {
                    $agPref[[string]$r.db_name] = [pscustomobject]@{
                        Role        = [string]$r.role_desc
                        IsPreferred = [int]$r.is_preferred
                    }
                }
                Add-Check 'Always On' 'Info' ("HADR enabled; {0} local Availability Group database(s)." -f $agPref.Count)
            }
            else {
                Add-Check 'Always On' 'Info' 'Always On (HADR) is not enabled on this instance.'
            }
        }
        catch {
            Add-Check 'Always On' 'Warning' ("Could not evaluate AG backup preference: {0}" -f $_.Exception.Message)
        }

        # 5b) Current login and whether it has rights to back up -------------
        $isSysadmin = $false
        $loginName  = [string]$server.ConnectionContext.TrueLogin
        try {
            $who = Invoke-DbaQuery -SqlInstance $server -ErrorAction Stop -Query @'
SELECT login_name = SUSER_SNAME(),
       is_sysadmin = IS_SRVROLEMEMBER('sysadmin')
'@
            if ($who) {
                $loginName  = [string]$who.login_name
                $isSysadmin = ([int]$who.is_sysadmin -eq 1)
            }
            if ($isSysadmin) {
                Add-Check 'Current login rights' 'Pass' ("'{0}' is a member of sysadmin - it can back up any database." -f $loginName)
            }
            else {
                Add-Check 'Current login rights' 'Info' ("'{0}' is NOT sysadmin - per-database BACKUP permission is verified below." -f $loginName)
            }
        }
        catch {
            Add-Check 'Current login rights' 'Warning' ("Could not determine the current login / server role: {0}" -f $_.Exception.Message)
        }

        # 6) Enumerate EVERY database, list it, and say if / why it is not backable
        try {
            $allDbs = @(Get-DbaDatabase -SqlInstance $server -ErrorAction Stop)
            if ($Database) {
                $allDbs = @($allDbs | Where-Object { $Database -contains $_.Name })
            }

            $systemDbs = @($allDbs | Where-Object { $_.IsSystemObject })
            $userDbs   = @($allDbs | Where-Object { -not $_.IsSystemObject })

            Add-Check 'Databases found' 'Info' `
                ("{0} database(s) total: {1} system, {2} user." -f $allDbs.Count, $systemDbs.Count, $userDbs.Count)

            if ($userDbs.Count -eq 0) {
                Add-Check 'Backable databases' 'Fail' `
                    'No USER databases exist - there is nothing to back up, which is why "No backup result returned" is raised.'
            }
            else {
                $backableCount = 0
                $skippedCount  = 0

                foreach ($db in ($userDbs | Sort-Object Name)) {

                    # Always On role / backup preference for this database (if any).
                    $agRole = $null; $agIsPreferred = $null
                    if ($agPref.ContainsKey([string]$db.Name)) {
                        $agRole        = [string]$agPref[[string]$db.Name].Role
                        $agIsPreferred = [int]$agPref[[string]$db.Name].IsPreferred
                    }

                    # Descriptive flags (shown on every DB line, not blockers).
                    $flags = New-Object System.Collections.Generic.List[string]
                    if ($db.ReadOnly) { $flags.Add('ReadOnly') }
                    $flags.Add("Recovery=$($db.RecoveryModel)")
                    if ($agRole) { $flags.Add("AG:role=$agRole,preferred=$([bool]$agIsPreferred)") }
                    $flagText = if ($flags.Count -gt 0) { " [{0}]" -f ($flags -join '; ') } else { '' }

                    # --- Expected, NON-error states: SKIP and do not test further ---
                    # A backup / CHECKDB legitimately does not run on THIS instance for:
                    #   1. Always On SECONDARY replicas (the primary owns the operation;
                    #      a non-readable secondary also reports IsAccessible = $false)
                    #   2. READ_ONLY databases
                    #   3. OFFLINE databases
                    # These are normal and must NOT be reported as failures.
                    $skipReason = $null
                    if ($agRole -eq 'SECONDARY')              { $skipReason = 'Always On SECONDARY replica' }
                    elseif ($db.ReadOnly)                     { $skipReason = 'database is READ_ONLY' }
                    elseif ("$($db.Status)" -match 'Offline') { $skipReason = "database is OFFLINE (Status='$($db.Status)')" }

                    if ($skipReason) {
                        $skippedCount++
                        Add-Check ("DB '{0}'" -f $db.Name) 'Info' ("skipped - normal, not an error - {0}{1}" -f $skipReason, $flagText)
                        continue
                    }

                    # --- Real blockers that explain an actual backup failure ---
                    # (accessible, ONLINE, not a snapshot, and the login can back it up)
                    $reasons = New-Object System.Collections.Generic.List[string]
                    if ($db.IsDatabaseSnapshot)       { $reasons.Add('is a database snapshot (cannot be backed up)') }
                    if (-not $db.IsAccessible)         { $reasons.Add('is NOT accessible') }
                    if ("$($db.Status)" -ne 'Normal') { $reasons.Add("status is '$($db.Status)'") }

                    # Backup permission - only meaningful when the DB is reachable and
                    # the login is not already sysadmin (sysadmin can back up anything).
                    if (-not $isSysadmin -and $db.IsAccessible -and "$($db.Status)" -eq 'Normal' -and -not $db.IsDatabaseSnapshot) {
                        try {
                            $permRow = Invoke-DbaQuery -SqlInstance $server -Database $db.Name -ErrorAction Stop `
                                -Query "SELECT can_backup = HAS_PERMS_BY_NAME(NULL, NULL, 'BACKUP DATABASE')"
                            if (-not $permRow -or [int]$permRow.can_backup -ne 1) {
                                $reasons.Add("current login '$loginName' lacks BACKUP DATABASE permission (needs sysadmin, db_owner, or db_backupoperator)")
                            }
                        }
                        catch {
                            $reasons.Add("could not verify BACKUP permission: $($_.Exception.Message)")
                        }
                    }

                    $isBackable = ($reasons.Count -eq 0)
                    if ($isBackable) { $backableCount++ }

                    if ($isBackable) {
                        Add-Check ("DB '{0}'" -f $db.Name) 'Pass' ("backable{0}" -f $flagText)
                    }
                    else {
                        Add-Check ("DB '{0}'" -f $db.Name) 'Fail' ("NOT backable - {0}{1}" -f ($reasons -join '; '), $flagText)
                    }
                }

                if ($backableCount -gt 0) {
                    Add-Check 'Backable databases' 'Pass' `
                        ("{0} of {1} user database(s) can be backed up ({2} skipped: SECONDARY / OFFLINE / READ_ONLY)." -f $backableCount, $userDbs.Count, $skippedCount)
                }
                elseif ($skippedCount -eq $userDbs.Count) {
                    Add-Check 'Backable databases' 'Info' `
                        ("All {0} user database(s) are Always On SECONDARY / OFFLINE / READ_ONLY on this instance - nothing to back up or check here (normal, not an error)." -f $userDbs.Count)
                }
                else {
                    Add-Check 'Backable databases' 'Fail' `
                        ("{0} user database(s) exist but NONE are backable - see each DB line above for the reason." -f $userDbs.Count)
                }
            }
        }
        catch {
            Add-Check 'Databases' 'Fail' ("Could not enumerate databases: {0}" -f $_.Exception.Message)
        }
    }

    # 8) Backup folder - server-side (SQL service account) -------------------
    if ($BackupPath) {
        if ($server) {
            try {
                if (Test-DbaPath -SqlInstance $server -Path $BackupPath -ErrorAction Stop) {
                    Add-Check 'Backup path (server-side)' 'Pass' ("SQL service account can access '{0}'." -f $BackupPath)
                }
                else {
                    Add-Check 'Backup path (server-side)' 'Fail' `
                    ("SQL service account CANNOT access '{0}' (share/UNC/NTFS permissions of the SQL service account)." -f $BackupPath)
                }
            }
            catch {
                Add-Check 'Backup path (server-side)' 'Warning' ("Could not test the path from the server: {0}" -f $_.Exception.Message)
            }
        }

        # 9) Backup folder - client-side (this session) ----------------------
        try {
            if (Test-Path -LiteralPath $BackupPath) {
                $probe = Join-Path $BackupPath ("._readiness_{0}.tmp" -f ([guid]::NewGuid().ToString('N').Substring(0, 8)))
                try {
                    'ok' | Out-File -LiteralPath $probe -Encoding ascii -ErrorAction Stop
                    Remove-Item -LiteralPath $probe -Force -ErrorAction SilentlyContinue
                    Add-Check 'Backup path (client-side)' 'Pass' ("Caller can write to '{0}'." -f $BackupPath)
                }
                catch {
                    Add-Check 'Backup path (client-side)' 'Warning' ("Path exists but the caller cannot write: {0}" -f $_.Exception.Message)
                }
            }
            else {
                Add-Check 'Backup path (client-side)' 'Warning' ("Caller cannot see '{0}' (may still be reachable from the server)." -f $BackupPath)
            }
        }
        catch {
            Add-Check 'Backup path (client-side)' 'Warning' ("Client-side path test failed: {0}" -f $_.Exception.Message)
        }
    }

    # --- Overall verdict ----------------------------------------------------
    $failCount = @($checks | Where-Object { $_.Status -eq 'Fail' }).Count
    $warnCount = @($checks | Where-Object { $_.Status -eq 'Warning' }).Count
    $verdict = if ($failCount -gt 0) { 'Fail' } elseif ($warnCount -gt 0) { 'Warning' } else { 'Pass' }
    Add-Check 'SUMMARY' $verdict ("Fails={0}; Warnings={1}." -f $failCount, $warnCount)

    return $checks
}
