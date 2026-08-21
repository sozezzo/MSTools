function Get-MSAlwaysOnHealth {
<#
.SYNOPSIS
    Produces a small, read-only health report for SQL Server Always On
    Availability Groups.

.DESCRIPTION
    Connects to the primary server (node, listener, or alias) and reads the
    Always On (HADR) health information using T-SQL DMVs only.

    The function is strictly READ-ONLY: it only runs SELECT statements against
    the availability group / replica / database DMVs. It does NOT create,
    change, add, fail over, or remove anything.

    It reports, per availability group:
      - the availability group name and current primary replica;
      - each replica: role, availability mode, failover mode, connection state,
        synchronization health, and operational state;
      - each database on each replica: synchronization state / health, whether
        it is suspended (and why), and the log send / redo queue sizes.

    A short summary is written to the verbose stream, and structured objects are
    returned so the result can be piped, formatted, or exported.

.PARAMETER SqlInstance
    The primary server (or any node / listener / alias that belongs to the
    Always On Availability Group). The function normalizes the connection and
    reads the current primary from the DMVs.

.PARAMETER SqlCredential
    Optional PSCredential used to connect. When omitted the current login
    (Integrated Security) is used.

.PARAMETER AvailabilityGroup
    Optional list of availability group names to include (wildcards allowed).
    Empty = all availability groups on the instance.

.PARAMETER ReplicaHealthOnly
    Return only the replica-level health rows (skip the per-database detail).

.PARAMETER UseWmi
    Run additional out-of-band connectivity diagnostics against each replica
    node (requires Windows / WMI-CIM rights on those nodes). When the caller
    has the necessary rights this adds, per replica node:
      - a ping test;
      - a TCP test to the database-mirroring (HADR) endpoint port parsed from
        the replica endpoint URL (typically 5022);
      - the SQL Server service state (via CIM Win32_Service);
      - whether an enabled inbound firewall rule allows the endpoint port.
    Each check is best-effort: failures are reported per node and never stop
    the health report. Results are returned as Level = 'Connectivity' rows.

.PARAMETER WmiCredential
    Optional Windows PSCredential used for the -UseWmi remote CIM/WMI queries.
    When omitted the current Windows identity is used. Ignored unless -UseWmi
    is specified.

.PARAMETER PromptWmiCredential
    Interactively prompt for an Active Directory credential (via
    Get-MSWmiCredential) to use for the -UseWmi diagnostics. Useful when the
    current login lacks WMI rights on the replica nodes but another AD account
    has them. Ignored unless -UseWmi is specified. When both -WmiCredential and
    -PromptWmiCredential are given, the interactive prompt wins.

.PARAMETER IncludeSecondaryDetail
    Connect to each replica server directly (reusing -SqlCredential) and add
    that replica's own local information to the report:
      - whether HADR is enabled on it;
      - its local database-mirroring (HADR) endpoint name, state and port;
      - the endpoint connection auth / encryption settings;
      - recent Always On / mirroring / endpoint lines from its SQL error log.
    This is the piece that cannot be read from the primary and is essential to
    diagnose a DISCONNECTED secondary. Each connection is best-effort: if a
    replica cannot be reached the failure is captured in the row instead of
    stopping the report. Results are returned as Level = 'EndpointDetail' rows.

.PARAMETER CheckServiceAccount
    Verify that the service accounts have the rights required to run Always On.
    Connects to each replica (reusing -SqlCredential) and reads:
      - the SQL Server service startup account (sys.dm_server_services);
      - the database-mirroring (HADR) endpoint state, owner and connection
        auth;
      - the logins that are GRANTed CONNECT on that endpoint.
    It then cross-checks that every replica's endpoint grants CONNECT to the
    OTHER replicas' service accounts - the permission that must exist for the
    replicas to connect to each other. Missing grants are the classic cause of
    a DISCONNECTED / NOT_HEALTHY secondary. It also warns when the HADR endpoint
    or the availability group is not owned by [sa] (a dropped owner login can
    break them) and shows the ALTER AUTHORIZATION fix. Results are returned as
    Level = 'ServiceAccount' rows. Best-effort: unreachable replicas and
    accounts that use a virtual / machine account (which authenticate as
    DOMAIN\host$) are flagged for manual verification rather than reported as
    a failure.

.PARAMETER ConnectTimeoutSeconds
    Caps how long each out-of-band probe waits before giving up on an
    unreachable replica (TCP endpoint test, remote CIM session, direct SQL
    connect). Keeps the report fast when a secondary is down. Default 5.

.PARAMETER SecondaryDetailTimeoutSeconds
    Total time budget (in seconds) for the -IncludeSecondaryDetail phase and
    the maximum time any single query against a replica may run. Once the
    budget is exceeded the remaining replicas are not probed and are returned
    as EndpointDetail rows with EndpointState = 'skipped (timeout)'. Prevents
    the report from hanging for a long time when a replica is slow or
    partially reachable. Ignored unless -IncludeSecondaryDetail is specified.
    Default 30.

.PARAMETER QueryTimeoutSeconds
    Query timeout used for regular SQL health reads. Keeps a busy server from
    hanging an entire category indefinitely. Default 30.

.PARAMETER RetryCount
    Number of attempts for SQL health reads/connect smoke tests before giving
    up. Useful when a replica is briefly busy or the first connection attempt
    fails transiently. Default 2.

.PARAMETER CheckListener
    Read the Availability Group listener configuration (name, port, IP/subnet,
    online state) and verify the listener name resolves in DNS from this host.
    Read-only. Results are returned as Level = 'Listener' rows.

.PARAMETER CheckCluster
    Read the underlying WSFC cluster / quorum state (quorum model & state,
    member nodes and their quorum votes). Read-only. Results are returned as
    Level = 'Cluster' and Level = 'ClusterMember' rows. Clusterless AGs
    (CLUSTER_TYPE = NONE) are reported as informational.

.PARAMETER CheckFailoverReadiness
    Evaluate whether each database is failover-ready and how far each secondary
    lags (RPO). Reads is_database_joined / is_failover_ready and (2016+)
    secondary_lag_seconds. Read-only. Results are returned as
    Level = 'FailoverReadiness' rows.

.PARAMETER CheckBackup
    Read the automated backup preference and per-database log-reuse state. Flags
    log_reuse_wait = AVAILABILITY_REPLICA (a stuck secondary preventing log
    truncation). Read-only. Results are returned as Level = 'Backup' rows.

.PARAMETER IncludeConfiguration
    Emit a read-only snapshot of the Always On / cluster configuration
    (availability & failover mode, seeding mode, session timeout, endpoint URL,
    read-only routing, backup priority, cluster type, DTC/basic/distributed,
    required synchronized secondaries, etc.). Results are returned as
    Level = 'Configuration' rows.

.PARAMETER CheckLoginParity
    Connect to every replica and compare the server logins so a failover does
    not leave a database unusable because a login is missing on the target.
    Only the logins MISSING on one or more replicas are reported (system logins
    like ##*, NT SERVICE\*, NT AUTHORITY\* and BUILTIN\* are excluded); the full
    login list and the logins' rights are NOT reported. Results are returned as
    Level = 'LoginParity' rows, with a Copy-DbaLogin remediation. Skipped
    (Level = 'LoginParity', SKIPPED) on a standalone instance with no Always On
    replicas to compare against.

.PARAMETER CheckVersion
    Connect to every replica and compare the SQL Server build (ProductVersion /
    patch level). Every node in an availability group should run the SAME build;
    a mismatch is a supportability and failover risk. Results are returned as
    Level = 'VersionParity' rows (OK when all builds match, CHECK on a mismatch).

.PARAMETER CheckEventLog
    Read the Windows Failover Clustering events from the System event log of each
    cluster node for the last -EventHoursBack hours and summarise two views:
      - Level = 'ClusterEvent'  : cluster-health failures (quorum / node /
        resource / role).
      - Level = 'ListenerEvent' : events whose message references an AG listener
        name.
    Only the most recent occurrence is shown; the status follows how many
    occurrences fall in the window: none = OK, exactly one = WARN, more than one
    = FAILED. Read-only; remote reads reuse -WmiCredential when supplied.

.PARAMETER EventHoursBack
    How many hours of history the -CheckEventLog queries look back over.
    Default 24, maximum 8760 (one year). Ignored unless -CheckEventLog is set.

.PARAMETER CheckConnection
    Real connection smoke test: open a fresh connection to each target and run
    SELECT 1 (fail = FAILED, slow > 3s = WARN). Level = 'Connection'.

.PARAMETER CheckDatabaseState
    Check sys.databases.state_desc for every database: SUSPECT / RECOVERY_PENDING
    / EMERGENCY are FAILED (red), OFFLINE is WARN (orange). Level = 'DatabaseState'.

.PARAMETER CheckSystemDatabase
    Verify the system databases (master/msdb/model/tempdb) are ONLINE.
    Level = 'SystemDatabase'.

.PARAMETER CheckIoError
    Look for recorded corruption / I/O errors WITHOUT running DBCC CHECKDB:
    msdb.dbo.suspect_pages and 823/824/825 in the current error log.
    Level = 'IoError'.

.PARAMETER CheckClockSkew
    Compare each replica's UTC clock against this host (round-trip adjusted);
    >= 5s = WARN, >= 60s = FAILED. Level = 'ClockSkew'.

.PARAMETER CheckCertificate
    List every SQL certificate (sys.certificates) with its expiry date and flag
    those expiring soon: < 60 days = WARN, < 30 days = FAILED. Level = 'Certificate'.

.PARAMETER CheckPendingReboot
    Check each node for a pending Windows reboot (remote registry / WMI via
    WinRM, reuses -WmiCredential). The Detail lists every condition that triggered
    it (CBS, Windows Update, pending file renames, pending computer rename,
    ConfigMgr) and where it was read from. Pending = WARN. Level = 'PendingReboot'.

.PARAMETER CheckLoginFailed
    Scan the error log for failed logins (error 18456) in the last
    -LoginFailedHoursBack hours and list each distinct login (name, attempt
    count and the client IP of the most recent attempt); any = WARN.
    Level = 'LoginFailed'.

.PARAMETER LoginFailedHoursBack
    Hours of history for -CheckLoginFailed. Default 24, maximum 8760.

.PARAMETER LoginFailedIgnoreIp
    One or more client IP addresses (e.g. security scanners) to exclude when
    deciding whether failed logins are a problem. Attempts from these IPs still
    appear in the full health detail (as INFO, tagged 'ignored IP') but never
    raise a WARN in the status summary or a detected problem.

.OUTPUTS
    [pscustomobject] rows. These kinds are returned:
      - Level = 'Replica'          : one row per replica.
      - Level = 'Database'         : one row per database per replica.
      - Level = 'Connectivity'     : one row per replica node (only with -UseWmi).
      - Level = 'EndpointDetail'   : one row per replica queried directly
                                     (only with -IncludeSecondaryDetail).
      - Level = 'ServiceAccount'   : one row per replica (only with
                                     -CheckServiceAccount); service account +
                                     endpoint CONNECT permission assessment.
      - Level = 'Listener'         : listener config (only with -CheckListener).
      - Level = 'Cluster' /
        'ClusterMember'            : WSFC quorum / members (only -CheckCluster).
      - Level = 'FailoverReadiness': failover-ready / RPO lag per database
                                     (only with -CheckFailoverReadiness).
      - Level = 'Backup'           : backup preference / log-reuse
                                     (only with -CheckBackup).
      - Level = 'Configuration'    : Always On / cluster config snapshot
                                     (only with -IncludeConfiguration).
      - Level = 'LoginParity'      : logins missing on one or more replicas
                                     (only with -CheckLoginParity).
      - Level = 'VersionParity'    : SQL build parity across replicas
                                     (only with -CheckVersion).
      - Level = 'ClusterEvent' /
        'ListenerEvent'            : Windows cluster / listener events
                                     (only with -CheckEventLog).
      - Level = 'Connection'       : connect + SELECT 1 smoke test
                                     (only with -CheckConnection).
      - Level = 'DatabaseState' /
        'SystemDatabase'           : database / system-DB state
                                     (-CheckDatabaseState / -CheckSystemDatabase).
      - Level = 'IoError'          : suspect pages / 823-825 (only -CheckIoError).
      - Level = 'ClockSkew'        : replica clock skew (only -CheckClockSkew).
      - Level = 'Certificate'      : certificate expiry (only -CheckCertificate).
      - Level = 'PendingReboot'    : pending Windows reboot (only -CheckPendingReboot).
      - Level = 'LoginFailed'      : failed-login scan (only -CheckLoginFailed).
      - Level = 'AlwaysOn'         : SKIPPED marker on a standalone instance.
    Every row also carries a Remediation column: when a value looks wrong it
    holds ready-to-run T-SQL / PowerShell; the function itself changes nothing.

.EXAMPLE
    Get-MSAlwaysOnHealth -SqlInstance 'SQLPROD01' -Verbose |
        Format-Table Level, AvailabilityGroup, Replica, Role, SyncHealth, DatabaseName, SyncState -AutoSize

    Reads the Always On health from SQLPROD01 and shows the report as a table.

.EXAMPLE
    $health = Get-MSAlwaysOnHealth -SqlInstance 'SQLLISTENER'
    $health | Where-Object { $_.SyncHealth -ne 'HEALTHY' }

    Returns the health rows and keeps only the ones that are not HEALTHY.

.EXAMPLE
    Get-MSAlwaysOnHealth -SqlInstance 'ISI-M0BDRS32' -UseWmi |
        Where-Object Level -eq 'Connectivity' |
        Format-Table Replica, Node, Ping, Port, PortOpen, ServiceState, FirewallAllowed, Detail -AutoSize

    Adds out-of-band diagnostics for a DISCONNECTED secondary: ping, HADR
    endpoint TCP port test, SQL service state, and firewall rule check.

.NOTES
    Requires Invoke-DbaQuery (dbatools). The function does not import any module
    automatically and performs read-only queries only.
#>
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)]
        [string] $SqlInstance,

        [PSCredential] $SqlCredential,

        [string[]] $AvailabilityGroup,

        [switch] $ReplicaHealthOnly,

        [switch] $UseWmi,

        [PSCredential] $WmiCredential,

        [switch] $PromptWmiCredential,

        [switch] $IncludeSecondaryDetail,

        [switch] $CheckServiceAccount,

        [switch] $CheckListener,

        [switch] $CheckCluster,

        [switch] $CheckFailoverReadiness,

        [switch] $CheckBackup,

        [switch] $IncludeConfiguration,

        [switch] $CheckLoginParity,

        [switch] $CheckVersion,

        [switch] $CheckEventLog,

        [ValidateRange(1, 8760)]
        [int] $EventHoursBack = 24,

        [ValidateRange(1, 120)]
        [int] $ConnectTimeoutSeconds = 5,

        [ValidateRange(1, 3600)]
        [int] $SecondaryDetailTimeoutSeconds = 30,

        [ValidateRange(1, 3600)]
        [int] $QueryTimeoutSeconds = 30,

        [ValidateRange(1, 10)]
        [int] $RetryCount = 2,

        # --- Instance-level readiness checks (run even on a standalone instance) ---
        [switch] $CheckConnection,
        [switch] $CheckDatabaseState,
        [switch] $CheckSystemDatabase,
        [switch] $CheckIoError,
        [switch] $CheckClockSkew,
        [switch] $CheckCertificate,
        [switch] $CheckPendingReboot,
        [switch] $CheckLoginFailed,

        [ValidateRange(1, 8760)]
        [int] $LoginFailedHoursBack = 24,

        [string[]] $LoginFailedIgnoreIp
    )

    if (-not (Get-Command Invoke-DbaQuery -ErrorAction SilentlyContinue)) {
        Write-Warning "Invoke-DbaQuery (dbatools) is not available. Please 'Import-Module dbatools' first."
        return
    }

    $queryParams = @{
        SqlInstance = $SqlInstance
        QueryTimeout = $QueryTimeoutSeconds
        ErrorAction = 'Stop'
    }
    if ($SqlCredential) { $queryParams['SqlCredential'] = $SqlCredential }

    $invokeWithRetry = {
        param(
            [Parameter(Mandatory)] [scriptblock] $ScriptBlock,
            [Parameter(Mandatory)] [string] $Operation
        )

        $lastError = $null
        for ($attempt = 1; $attempt -le $RetryCount; $attempt++) {
            try { return & $ScriptBlock }
            catch {
                $lastError = $_
                if ($attempt -lt $RetryCount) {
                    Write-Verbose ("{0} failed on attempt {1}/{2}: {3}; retrying..." -f $Operation, $attempt, $RetryCount, $_.Exception.Message)
                    Start-Sleep -Milliseconds ([int][math]::Min(2000, 250 * $attempt))
                    continue
                }
            }
        }
        throw $lastError
    }

    # --- Confirm the instance actually has Always On enabled ---------------
    try {
        $hadr = & $invokeWithRetry -Operation "connect/read HADR state from [$SqlInstance]" -ScriptBlock { Invoke-DbaQuery @queryParams -Query @"
SELECT
    ServerName     = CONVERT(sysname, @@SERVERNAME),
    IsHadrEnabled  = CONVERT(tinyint, SERVERPROPERTY('IsHadrEnabled')),
    ProductVersion = CONVERT(sysname, SERVERPROPERTY('ProductVersion'));
"@
        }
    }
    catch {
        Write-Warning "Could not connect to [$SqlInstance] after $RetryCount attempt(s): $($_.Exception.Message)"
        return
    }

    # Always On may be OFF (standalone instance). Instead of bailing out, run the
    # instance-level readiness checks and skip the AG-specific ones.
    $isHadr = ([int]$hadr.IsHadrEnabled -eq 1)
    if (-not $isHadr) {
        Write-Verbose "Always On (HADR) is not enabled on [$($hadr.ServerName)]; AG checks are skipped, instance-level checks still run."
    }

    # Does this instance participate in a WSFC cluster? Used to SKIP the cluster
    # quorum and cluster/listener event checks on a standalone (or clusterless)
    # instance where there is no Failover Cluster to evaluate.
    $hasCluster = $false
    try {
        $clRow = & $invokeWithRetry -Operation "read cluster name from [$($hadr.ServerName)]" -ScriptBlock { Invoke-DbaQuery @queryParams -Query "SELECT TOP (1) cluster_name FROM sys.dm_hadr_cluster WITH (NOLOCK)" }
        $hasCluster = -not [string]::IsNullOrWhiteSpace([string]$clRow.cluster_name)
    }
    catch {}

    # SQL Server major version (11=2012, 12=2014, 13=2016, 14=2017, 15=2019,
    # 16=2022). Used to gate DMV columns that only exist on newer builds.
    $major = 0
    try { $major = [int]((([string]$hadr.ProductVersion) -split '\.')[0]) } catch {}

    Write-Verbose "Reading Always On health from [$($hadr.ServerName)]..."

    # --- Replica-level health ---------------------------------------------
    $replicaQuery = @"
SELECT
    AvailabilityGroup    = ag.name,
    PrimaryReplica       = ags.primary_replica,
    Replica              = ar.replica_server_name,
    EndpointUrl          = ar.endpoint_url,
    Role                 = rs.role_desc,
    AvailabilityMode     = ar.availability_mode_desc,
    FailoverMode         = ar.failover_mode_desc,
    ConnectionState      = rs.connected_state_desc,
    OperationalState     = rs.operational_state_desc,
    RecoveryHealth       = rs.recovery_health_desc,
    SyncHealth           = rs.synchronization_health_desc,
    JoinState            = arcs.join_state_desc,
    LastConnectErrorNo   = rs.last_connect_error_number,
    LastConnectError     = rs.last_connect_error_description,
    LastConnectErrorTime = rs.last_connect_error_timestamp
FROM sys.availability_groups AS ag WITH (NOLOCK)
JOIN sys.availability_replicas AS ar WITH (NOLOCK)
    ON ar.group_id = ag.group_id
JOIN sys.dm_hadr_availability_replica_states AS rs WITH (NOLOCK)
    ON rs.replica_id = ar.replica_id
    AND rs.group_id  = ar.group_id
LEFT JOIN sys.dm_hadr_availability_replica_cluster_states AS arcs WITH (NOLOCK)
    ON arcs.replica_id = ar.replica_id
LEFT JOIN sys.dm_hadr_availability_group_states AS ags WITH (NOLOCK)
    ON ags.group_id = ag.group_id
ORDER BY ag.name, rs.role_desc DESC, ar.replica_server_name;
"@

    $replicaRows = @()
    if ($isHadr) {
        try {
            $replicaRows = @(& $invokeWithRetry -Operation "read replica health from [$($hadr.ServerName)]" -ScriptBlock { Invoke-DbaQuery @queryParams -Query $replicaQuery })
        }
        catch {
            Write-Warning "Could not read replica health after $RetryCount attempt(s) (VIEW SERVER STATE required): $($_.Exception.Message)"
        }
    }

    # --- Database-level health --------------------------------------------
    $databaseRows = @()
    if ($isHadr -and -not $ReplicaHealthOnly) {
        $databaseQuery = @"
SELECT
    AvailabilityGroup = ag.name,
    Replica           = ar.replica_server_name,
    Role              = rs.role_desc,
    DatabaseName      = adc.database_name,
    SyncState         = drs.synchronization_state_desc,
    SyncHealth        = drs.synchronization_health_desc,
    IsSuspended       = drs.is_suspended,
    SuspendReason     = drs.suspend_reason_desc,
    LogSendQueueKB    = drs.log_send_queue_size,
    RedoQueueKB       = drs.redo_queue_size
FROM sys.availability_groups AS ag WITH (NOLOCK)
JOIN sys.availability_replicas AS ar WITH (NOLOCK)
    ON ar.group_id = ag.group_id
JOIN sys.dm_hadr_availability_replica_states AS rs WITH (NOLOCK)
    ON rs.replica_id = ar.replica_id
    AND rs.group_id  = ar.group_id
JOIN sys.dm_hadr_database_replica_states AS drs WITH (NOLOCK)
    ON drs.replica_id = ar.replica_id
    AND drs.group_id  = ar.group_id
JOIN sys.availability_databases_cluster AS adc WITH (NOLOCK)
    ON adc.group_id       = ag.group_id
    AND adc.group_database_id = drs.group_database_id
ORDER BY ag.name, rs.role_desc DESC, ar.replica_server_name, adc.database_name;
"@
        try {
            $databaseRows = @(& $invokeWithRetry -Operation "read database replica health from [$($hadr.ServerName)]" -ScriptBlock { Invoke-DbaQuery @queryParams -Query $databaseQuery })
        }
        catch {
            Write-Warning "Could not read database health after $RetryCount attempt(s): $($_.Exception.Message)"
        }
    }

    # --- Optional availability group filter -------------------------------
    if ($AvailabilityGroup) {
        $agMatch = {
            param($name)
            foreach ($pattern in $AvailabilityGroup) {
                if ($name -like $pattern) { return $true }
            }
            return $false
        }
        $replicaRows  = @($replicaRows  | Where-Object { & $agMatch $_.AvailabilityGroup })
        $databaseRows = @($databaseRows | Where-Object { & $agMatch $_.AvailabilityGroup })
    }

    # --- Build the report objects -----------------------------------------
    $report = New-Object System.Collections.Generic.List[object]

    foreach ($r in $replicaRows) {
        $report.Add([pscustomobject]@{
            Level             = 'Replica'
            AvailabilityGroup = [string]$r.AvailabilityGroup
            PrimaryReplica    = [string]$r.PrimaryReplica
            Replica           = [string]$r.Replica
            Role              = [string]$r.Role
            AvailabilityMode  = [string]$r.AvailabilityMode
            FailoverMode      = [string]$r.FailoverMode
            ConnectionState   = [string]$r.ConnectionState
            OperationalState  = [string]$r.OperationalState
            RecoveryHealth    = [string]$r.RecoveryHealth
            SyncHealth        = [string]$r.SyncHealth
            JoinState         = [string]$r.JoinState
            LastConnectErrorNo   = if ($r.LastConnectErrorNo -is [System.DBNull]) { $null } else { [int]$r.LastConnectErrorNo }
            LastConnectError     = if ($r.LastConnectError -is [System.DBNull]) { '' } else { [string]$r.LastConnectError }
            LastConnectErrorTime = if ($r.LastConnectErrorTime -is [System.DBNull]) { $null } else { $r.LastConnectErrorTime }
            DatabaseName      = ''
            SyncState         = ''
            IsSuspended       = ''
            SuspendReason     = ''
            LogSendQueueKB    = $null
            RedoQueueKB       = $null
            Node              = ''
            Ping              = ''
            Port              = $null
            PortOpen          = ''
            ServiceState      = ''
            FirewallAllowed   = ''
            IsHadrEnabled     = ''
            EndpointName      = ''
            EndpointState     = ''
            EndpointConnAuth  = ''
            RecentErrorLog    = ''
            Detail            = ''
        })
    }

    foreach ($d in $databaseRows) {
        $report.Add([pscustomobject]@{
            Level             = 'Database'
            AvailabilityGroup = [string]$d.AvailabilityGroup
            PrimaryReplica    = ''
            Replica           = [string]$d.Replica
            Role              = [string]$d.Role
            AvailabilityMode  = ''
            FailoverMode      = ''
            ConnectionState   = ''
            OperationalState  = ''
            RecoveryHealth    = ''
            SyncHealth        = [string]$d.SyncHealth
            DatabaseName      = [string]$d.DatabaseName
            SyncState         = [string]$d.SyncState
            IsSuspended       = [bool]$d.IsSuspended
            SuspendReason     = [string]$d.SuspendReason
            LogSendQueueKB    = if ($d.LogSendQueueKB -is [System.DBNull]) { $null } else { [long]$d.LogSendQueueKB }
            RedoQueueKB       = if ($d.RedoQueueKB -is [System.DBNull]) { $null } else { [long]$d.RedoQueueKB }
            JoinState            = ''
            LastConnectErrorNo   = $null
            LastConnectError     = ''
            LastConnectErrorTime = $null
            Node              = ''
            Ping              = ''
            Port              = $null
            PortOpen          = ''
            ServiceState      = ''
            FirewallAllowed   = ''
            IsHadrEnabled     = ''
            EndpointName      = ''
            EndpointState     = ''
            EndpointConnAuth  = ''
            RecentErrorLog    = ''
            Detail            = ''
        })
    }

    # --- Shared connectivity helpers --------------------------------------
    # Raw TCP connect test with a hard timeout (no external module, no OS
    # SYN-retry wait). Used as a fast liveness pre-check so an unreachable
    # replica fails in milliseconds instead of hanging Connect-DbaInstance /
    # Get-DbaErrorLog for minutes and blowing past the time budget.
    $testTcp = {
        param([string] $ComputerName, [int] $Port, [int] $TimeoutMs = 3000)
        $client = [System.Net.Sockets.TcpClient]::new()
        try {
            $async = $client.BeginConnect($ComputerName, $Port, $null, $null)
            if ($async.AsyncWaitHandle.WaitOne($TimeoutMs)) {
                $client.EndConnect($async)
                return $true
            }
            return $false
        }
        catch { return $false }
        finally { $client.Close() }
    }

    # Fast liveness pre-check for a replica row. Returns $true when the host
    # answers on its HADR endpoint port (the replica host is the SQL host).
    # Returns $null when the port cannot be determined (caller then relies on
    # -ConnectTimeoutSeconds instead of skipping).
    $testReplicaAlive = {
        param($replicaRow, [int] $TimeoutMs)
        $node = [string]$replicaRow.Replica
        $port = $null
        if ($replicaRow.EndpointUrl -match '^(?i:TCP)://\[?([^\]:]+)\]?:(\d+)') {
            $node = $matches[1]
            $port = [int]$matches[2]
        }
        if (-not $port) { return $null }
        return (& $testTcp $node $port $TimeoutMs)
    }

    # --- Optional WMI / CIM connectivity diagnostics ----------------------
    if ($UseWmi) {
        Write-Verbose 'Running WMI/CIM connectivity diagnostics (-UseWmi)...'

        # Interactively obtain an AD credential when requested.
        if ($PromptWmiCredential) {
            if (Get-Command Get-MSWmiCredential -ErrorAction SilentlyContinue) {
                $WmiCredential = Get-MSWmiCredential -Message 'Enter the AD credential to use for Always On WMI diagnostics.'
            }
            else {
                Write-Warning 'Get-MSWmiCredential is not available; falling back to Get-Credential.'
                $WmiCredential = Get-Credential -Message 'Enter the AD credential to use for Always On WMI diagnostics.'
            }
        }

        # Best-effort raw TCP port test (no external module required).
        $testTcp = {
            param([string] $ComputerName, [int] $Port, [int] $TimeoutMs = 3000)
            $client = [System.Net.Sockets.TcpClient]::new()
            try {
                $async = $client.BeginConnect($ComputerName, $Port, $null, $null)
                if ($async.AsyncWaitHandle.WaitOne($TimeoutMs)) {
                    $client.EndConnect($async)
                    return $true
                }
                return $false
            }
            catch { return $false }
            finally { $client.Close() }
        }

        # Build the list of nodes to probe. For an Always On AG we probe each
        # replica's HADR endpoint node/port. For a standalone instance (no
        # replicas) we still probe the instance's own host on its live SQL
        # Server TCP port, so connectivity is tested even without a cluster/AG.
        $connTargets = New-Object System.Collections.Generic.List[object]
        if ($replicaRows.Count) {
            foreach ($cr in $replicaRows) {
                $tNode = [string]$cr.Replica
                $tPort = 5022
                if ($cr.EndpointUrl -match '^(?i:TCP)://\[?([^\]:]+)\]?:(\d+)') {
                    $tNode = $matches[1]
                    $tPort = [int]$matches[2]
                }
                $connTargets.Add([pscustomobject]@{ Node = $tNode; Port = $tPort; Row = $cr })
            }
        }
        else {
            # No AG replicas (standalone / clusterless instance): probe the
            # instance host on its live SQL Server TCP port (fallback 1433).
            $saNode = (($SqlInstance -split '\\')[0] -split ',')[0]
            $saPort = 1433
            try {
                $saPortRow = Invoke-DbaQuery @queryParams -Query "SELECT TOP (1) port FROM sys.dm_tcp_listener_states WITH (NOLOCK) WHERE type_desc = 'TSQL' AND state_desc = 'ONLINE' AND is_ipv4 = 1 ORDER BY port"
                if ($saPortRow -and -not ($saPortRow.port -is [System.DBNull]) -and $saPortRow.port) { $saPort = [int]$saPortRow.port }
            }
            catch {}
            $saRow = [pscustomobject]@{
                AvailabilityGroup    = ''
                Replica              = [string]$SqlInstance
                Role                 = 'STANDALONE'
                ConnectionState      = ''
                OperationalState     = ''
                SyncHealth           = ''
                JoinState            = ''
                LastConnectErrorNo   = [System.DBNull]::Value
                LastConnectError     = ''
                LastConnectErrorTime = [System.DBNull]::Value
            }
            $connTargets.Add([pscustomobject]@{ Node = $saNode; Port = $saPort; Row = $saRow })
        }

        # One connectivity row per distinct node.
        $seenNodes = @{}
        foreach ($t in $connTargets) {
            $r    = $t.Row
            $node = [string]$t.Node
            $port = [int]$t.Port
            if ($seenNodes.ContainsKey($node)) { continue }
            $seenNodes[$node] = $true

            $ping            = 'unknown'
            $portOpen        = 'unknown'
            $serviceState    = 'unknown'
            $firewallAllowed = 'unknown'
            $details         = New-Object System.Collections.Generic.List[string]

            # 1) Ping ------------------------------------------------------
            try {
                $ping = if (Test-Connection -ComputerName $node -Count 1 -Quiet -ErrorAction Stop) { 'ok' } else { 'no reply' }
            }
            catch { $ping = 'error'; $details.Add("ping: $($_.Exception.Message)") }

            # 2) TCP endpoint port ----------------------------------------
            try {
                $portOpen = if (& $testTcp $node $port ($ConnectTimeoutSeconds * 1000)) { 'open' } else { 'closed/blocked' }
            }
            catch { $portOpen = 'error'; $details.Add("tcp:$port $($_.Exception.Message)") }

            # 3 & 4) Remote CIM: SQL service state + firewall rule --------
            # Skip CIM entirely when the node did not answer a ping: a remote
            # CIM/WSMan connect to a dead node can otherwise hang for minutes.
            if ($ping -ne 'ok') {
                $serviceState    = 'skipped (no ping)'
                $firewallAllowed = 'skipped (no ping)'
            }
            else {
            $cimSession = $null
            try {
                $cimOption = New-CimSessionOption -Protocol Wsman
                $sessionParams = @{
                    ComputerName       = $node
                    OperationTimeoutSec = $ConnectTimeoutSeconds
                    SessionOption      = $cimOption
                    ErrorAction        = 'Stop'
                }
                if ($WmiCredential) { $sessionParams['Credential'] = $WmiCredential }
                $cimSession = New-CimSession @sessionParams

                # SQL Server service (default or named instance)
                try {
                    $svc = Get-CimInstance -CimSession $cimSession -ClassName Win32_Service `
                        -Filter "Name='MSSQLSERVER' OR Name LIKE 'MSSQL[$]%'" -ErrorAction Stop
                    if ($svc) {
                        $serviceState = (@($svc | ForEach-Object { "$($_.Name)=$($_.State)" }) -join '; ')
                    }
                    else {
                        $serviceState = 'not found'
                    }
                }
                catch { $serviceState = 'error'; $details.Add("service: $($_.Exception.Message)") }

                # Inbound firewall rule allowing the endpoint port
                try {
                    if (Get-Command Get-NetFirewallPortFilter -ErrorAction SilentlyContinue) {
                        $allowed = $false
                        $portFilters = Get-NetFirewallPortFilter -CimSession $cimSession -ErrorAction Stop |
                            Where-Object {
                                $_.Protocol -eq 'TCP' -and
                                ($_.LocalPort -contains "$port" -or $_.LocalPort -eq 'Any')
                            }
                        foreach ($pf in $portFilters) {
                            $rule = $pf | Get-NetFirewallRule -ErrorAction SilentlyContinue
                            if ($rule -and "$($rule.Enabled)" -eq 'True' -and
                                $rule.Direction -eq 'Inbound' -and $rule.Action -eq 'Allow') {
                                $allowed = $true
                                break
                            }
                        }
                        $firewallAllowed = if ($allowed) { 'allowed' } else { 'no allow rule' }
                    }
                    else {
                        $firewallAllowed = 'n/a (NetSecurity module missing)'
                    }
                }
                catch { $firewallAllowed = 'error'; $details.Add("firewall: $($_.Exception.Message)") }
            }
            catch {
                $serviceState    = 'no WMI access'
                $firewallAllowed = 'no WMI access'
                $details.Add("cim: $($_.Exception.Message)")
            }
            finally {
                if ($cimSession) { Remove-CimSession -CimSession $cimSession -ErrorAction SilentlyContinue }
            }
            }

            $report.Add([pscustomobject]@{
                Level             = 'Connectivity'
                AvailabilityGroup = [string]$r.AvailabilityGroup
                PrimaryReplica    = ''
                Replica           = [string]$r.Replica
                Role              = [string]$r.Role
                AvailabilityMode  = ''
                FailoverMode      = ''
                ConnectionState   = [string]$r.ConnectionState
                OperationalState  = [string]$r.OperationalState
                RecoveryHealth    = ''
                SyncHealth        = [string]$r.SyncHealth
                DatabaseName      = ''
                SyncState         = ''
                IsSuspended       = ''
                SuspendReason     = ''
                LogSendQueueKB    = $null
                RedoQueueKB       = $null
                JoinState            = [string]$r.JoinState
                LastConnectErrorNo   = if ($r.LastConnectErrorNo -is [System.DBNull]) { $null } else { [int]$r.LastConnectErrorNo }
                LastConnectError     = if ($r.LastConnectError -is [System.DBNull]) { '' } else { [string]$r.LastConnectError }
                LastConnectErrorTime = if ($r.LastConnectErrorTime -is [System.DBNull]) { $null } else { $r.LastConnectErrorTime }
                Node              = $node
                Ping              = $ping
                Port              = $port
                PortOpen          = $portOpen
                ServiceState      = $serviceState
                FirewallAllowed   = $firewallAllowed
                IsHadrEnabled     = ''
                EndpointName      = ''
                EndpointState     = ''
                EndpointConnAuth  = ''
                RecentErrorLog    = ''
                Detail            = ($details -join ' | ')
            })
        }
    }

    # --- Optional per-replica endpoint / error-log detail -----------------
    # Connects to each replica server directly (this is the information that
    # cannot be read from the primary) and adds its local endpoint state,
    # HADR-enabled flag and recent Always On error-log lines.
    if ($IncludeSecondaryDetail) {
        Write-Verbose ("Collecting per-replica endpoint / error-log detail (-IncludeSecondaryDetail); total time budget {0}s..." -f $SecondaryDetailTimeoutSeconds)

        # Total time budget for this whole phase. Once it is exceeded the
        # remaining replicas are recorded as skipped instead of being probed,
        # so the report never hangs on a slow / partially reachable replica.
        $detailStopwatch = [System.Diagnostics.Stopwatch]::StartNew()

        $endpointQuery = @"
SELECT
    ServerName    = CONVERT(sysname, @@SERVERNAME),
    IsHadrEnabled = CONVERT(tinyint, SERVERPROPERTY('IsHadrEnabled')),
    EndpointName  = e.name,
    EndpointState = e.state_desc,
    EndpointPort  = te.port,
    ConnAuth      = e.connection_auth_desc,
    Encryption    = e.is_encryption_enabled
FROM sys.database_mirroring_endpoints AS e WITH (NOLOCK)
LEFT JOIN sys.tcp_endpoints AS te WITH (NOLOCK)
    ON te.endpoint_id = e.endpoint_id;
"@

        $seenDetail = @{}
        foreach ($r in $replicaRows) {
            $target = [string]$r.Replica
            if ([string]::IsNullOrWhiteSpace($target)) { continue }
            if ($seenDetail.ContainsKey($target)) { continue }
            $seenDetail[$target] = $true

            $isHadr        = ''
            $endpointName  = ''
            $endpointState = ''
            $endpointPort  = $null
            $connAuth      = ''
            $recentLog     = ''
            $detailMsg     = ''

            # Stop probing once the total time budget for this phase is spent.
            # Remaining replicas are still returned, marked as skipped.
            $budgetExceeded = $detailStopwatch.Elapsed.TotalSeconds -ge $SecondaryDetailTimeoutSeconds
            if ($budgetExceeded) {
                $endpointState = 'skipped (timeout)'
                $detailMsg     = "secondary-detail time budget of ${SecondaryDetailTimeoutSeconds}s exceeded"
            }

            # Time left in the budget bounds each individual query so a single
            # slow replica cannot consume the whole allowance.
            $queryTimeout = [int][math]::Max(1, [math]::Ceiling($SecondaryDetailTimeoutSeconds - $detailStopwatch.Elapsed.TotalSeconds))

            # Fast liveness pre-check: if the replica host does not answer on
            # its endpoint port, skip the expensive Connect-DbaInstance /
            # Get-DbaErrorLog calls that would otherwise hang past the budget.
            if (-not $budgetExceeded) {
                $alive = & $testReplicaAlive $r ($ConnectTimeoutSeconds * 1000)
                if ($alive -eq $false) {
                    $budgetExceeded = $true   # reuse the skip path below
                    $endpointState  = 'unreachable'
                    $detailMsg      = "host did not answer on endpoint port within ${ConnectTimeoutSeconds}s (skipped)"
                }
            }

            # Open a single connection with a bounded timeout so a down replica
            # fails fast instead of waiting on the default SQL login timeout.
            $srv = $null
            if (-not $budgetExceeded) {
                try {
                    $connParams = @{
                        SqlInstance    = $target
                        ConnectTimeout = $ConnectTimeoutSeconds
                        ErrorAction    = 'Stop'
                    }
                    if ($SqlCredential) { $connParams['SqlCredential'] = $SqlCredential }
                    $srv = Connect-DbaInstance @connParams
                }
                catch {
                    $endpointState = 'unreachable'
                    $detailMsg     = "connect: $($_.Exception.Message)"
                }
            }

            # Endpoint + HADR state on the replica itself
            if ($srv) {
                try {
                    $ep = Invoke-DbaQuery -SqlInstance $srv -Query $endpointQuery -QueryTimeout $queryTimeout -ErrorAction Stop
                    if ($ep) {
                        $isHadr        = [string]$ep.IsHadrEnabled
                        $endpointName  = [string]$ep.EndpointName
                        $endpointState = [string]$ep.EndpointState
                        $endpointPort  = if ($ep.EndpointPort -is [System.DBNull]) { $null } else { [int]$ep.EndpointPort }
                        $connAuth      = [string]$ep.ConnAuth
                    }
                    else {
                        $endpointState = 'no endpoint'
                    }
                }
                catch {
                    $endpointState = 'query error'
                    $detailMsg     = "endpoint: $($_.Exception.Message)"
                }
            }

            # Recent Always On / mirroring / endpoint error-log lines.
            # Read via xp_readerrorlog with a bounded CommandTimeout (-QueryTimeout)
            # so the SQL driver forcibly aborts a slow read instead of hanging
            # for minutes (Get-DbaErrorLog / SMO ReadErrorLog has no timeout).
            $errorLogBudgetLeft = [int][math]::Floor($SecondaryDetailTimeoutSeconds - $detailStopwatch.Elapsed.TotalSeconds)
            if ($srv -and $errorLogBudgetLeft -gt 2) {
                try {
                    $errorLogQuery = @"
SET NOCOUNT ON;
DECLARE @start DATETIME = DATEADD(DAY, -2, GETDATE());
DECLARE @log TABLE (LogDate DATETIME, ProcessInfo NVARCHAR(100), LogText NVARCHAR(MAX));
INSERT INTO @log (LogDate, ProcessInfo, LogText)
EXEC master.dbo.xp_readerrorlog 0, 1, NULL, NULL, @start, NULL, N'desc';
SELECT TOP (200) LogDate, LogText
FROM @log
WHERE LogText LIKE '%Always On%'
   OR LogText LIKE '%Availability%'
   OR LogText LIKE '%mirroring%'
   OR LogText LIKE '%endpoint%'
   OR LogText LIKE '%Hadr%'
ORDER BY LogDate DESC;
"@
                    $lines = Invoke-DbaQuery -SqlInstance $srv -Query $errorLogQuery -QueryTimeout $errorLogBudgetLeft -ErrorAction Stop |
                        Select-Object -First 8
                    if ($lines) {
                        $recentLog = ($lines | ForEach-Object {
                            '{0:yyyy-MM-dd HH:mm:ss} {1}' -f $_.LogDate, ([string]$_.LogText -replace '\s+', ' ')
                        }) -join ' || '
                    }
                }
                catch {
                    $recentLog = "errorlog: $($_.Exception.Message)"
                }
            }
            elseif ($srv) {
                $recentLog = 'errorlog: skipped (low time budget)'
            }

            $report.Add([pscustomobject]@{
                Level             = 'EndpointDetail'
                AvailabilityGroup = [string]$r.AvailabilityGroup
                PrimaryReplica    = ''
                Replica           = [string]$r.Replica
                Role              = [string]$r.Role
                AvailabilityMode  = ''
                FailoverMode      = ''
                ConnectionState   = [string]$r.ConnectionState
                OperationalState  = ''
                RecoveryHealth    = ''
                SyncHealth        = [string]$r.SyncHealth
                DatabaseName      = ''
                SyncState         = ''
                IsSuspended       = ''
                SuspendReason     = ''
                LogSendQueueKB    = $null
                RedoQueueKB       = $null
                JoinState            = [string]$r.JoinState
                LastConnectErrorNo   = if ($r.LastConnectErrorNo -is [System.DBNull]) { $null } else { [int]$r.LastConnectErrorNo }
                LastConnectError     = if ($r.LastConnectError -is [System.DBNull]) { '' } else { [string]$r.LastConnectError }
                LastConnectErrorTime = if ($r.LastConnectErrorTime -is [System.DBNull]) { $null } else { $r.LastConnectErrorTime }
                Node              = $target
                Ping              = ''
                Port              = $endpointPort
                PortOpen          = ''
                ServiceState      = ''
                FirewallAllowed   = ''
                IsHadrEnabled     = $isHadr
                EndpointName      = $endpointName
                EndpointState     = $endpointState
                EndpointConnAuth  = $connAuth
                RecentErrorLog    = $recentLog
                Detail            = $detailMsg
            })
        }
    }

    # --- Optional service-account / endpoint-permission check -------------
    # Verifies the rights the service accounts need to run Always On: each
    # replica's HADR endpoint must GRANT CONNECT to the OTHER replicas'
    # service accounts. Missing grants are the classic cause of a
    # DISCONNECTED / NOT_HEALTHY secondary.
    if ($CheckServiceAccount) {
        Write-Verbose ("Checking service accounts and endpoint CONNECT permissions required for Always On (-CheckServiceAccount); total time budget {0}s..." -f $SecondaryDetailTimeoutSeconds)

        $svcStopwatch = [System.Diagnostics.Stopwatch]::StartNew()

        $svcAccountQuery = @"
SELECT ServiceAccount = service_account
FROM sys.dm_server_services WITH (NOLOCK)
WHERE servicename LIKE 'SQL Server (%';
"@

        $endpointPermQuery = @"
SELECT
    EndpointName  = e.name,
    EndpointState = e.state_desc,
    EndpointOwner = SUSER_NAME(e.principal_id),
    ConnAuth      = e.connection_auth_desc,
    Grantee       = SUSER_NAME(sp.grantee_principal_id),
    PermState     = sp.state_desc
FROM sys.database_mirroring_endpoints AS e WITH (NOLOCK)
LEFT JOIN sys.server_permissions AS sp WITH (NOLOCK)
    ON sp.class = 105
    AND sp.major_id = e.endpoint_id
    AND sp.permission_name = 'CONNECT';
"@

        # A service account that is really a virtual/built-in account
        # authenticates to the other replicas as the machine account
        # (DOMAIN\host$), so its CONNECT grant cannot be verified by name.
        $isVirtualAccount = {
            param([string] $Account)
            if ([string]::IsNullOrWhiteSpace($Account)) { return $false }
            return ($Account -match '^(?i:NT SERVICE\\|NT AUTHORITY\\|LocalSystem$|\.\\)')
        }

        # 1) Gather per-replica service account + endpoint CONNECT grantees.
        $svcInfo = [ordered]@{}
        $seenSvc = @{}
        foreach ($r in $replicaRows) {
            $target = [string]$r.Replica
            if ([string]::IsNullOrWhiteSpace($target)) { continue }
            if ($seenSvc.ContainsKey($target)) { continue }
            $seenSvc[$target] = $true

            $info = [pscustomobject]@{
                Replica           = $target
                AvailabilityGroup = [string]$r.AvailabilityGroup
                Role              = [string]$r.Role
                ConnectionState   = [string]$r.ConnectionState
                SyncHealth        = [string]$r.SyncHealth
                ServiceAccount    = ''
                EndpointName      = ''
                EndpointState     = ''
                EndpointOwner     = ''
                AgOwner           = ''
                ConnAuth          = ''
                Grantees          = (New-Object System.Collections.Generic.List[string])
                CollectError      = ''
                Reachable         = $false
            }

            $budgetLeft = $SecondaryDetailTimeoutSeconds - $svcStopwatch.Elapsed.TotalSeconds
            if ($budgetLeft -le 0) {
                $info.CollectError = "skipped (timeout): service-account check budget of ${SecondaryDetailTimeoutSeconds}s exceeded"
                $svcInfo[$target] = $info
                continue
            }
            $qTimeout = [int][math]::Max(1, [math]::Ceiling($budgetLeft))

            # Fast liveness pre-check so an unreachable replica fails in
            # milliseconds instead of hanging the bounded connect below.
            $alive = & $testReplicaAlive $r ($ConnectTimeoutSeconds * 1000)
            if ($alive -eq $false) {
                $info.CollectError = "connect: host did not answer on endpoint port within ${ConnectTimeoutSeconds}s"
                $svcInfo[$target] = $info
                continue
            }

            $srv = $null
            try {
                $connParams = @{
                    SqlInstance    = $target
                    ConnectTimeout = $ConnectTimeoutSeconds
                    ErrorAction    = 'Stop'
                }
                if ($SqlCredential) { $connParams['SqlCredential'] = $SqlCredential }
                $srv = Connect-DbaInstance @connParams
            }
            catch {
                $info.CollectError = "connect: $($_.Exception.Message)"
                $svcInfo[$target] = $info
                continue
            }
            $info.Reachable = $true

            try {
                $svc = Invoke-DbaQuery -SqlInstance $srv -Query $svcAccountQuery -QueryTimeout $qTimeout -ErrorAction Stop
                if ($svc) { $info.ServiceAccount = [string]($svc | Select-Object -First 1 -ExpandProperty ServiceAccount) }
            }
            catch { $info.CollectError = "service account: $($_.Exception.Message)" }

            try {
                $perms = Invoke-DbaQuery -SqlInstance $srv -Query $endpointPermQuery -QueryTimeout $qTimeout -ErrorAction Stop
                foreach ($p in $perms) {
                    if (-not $info.EndpointName -and $p.EndpointName -and $p.EndpointName -isnot [System.DBNull]) {
                        $info.EndpointName  = [string]$p.EndpointName
                        $info.EndpointState = [string]$p.EndpointState
                        $info.EndpointOwner = [string]$p.EndpointOwner
                        $info.ConnAuth      = [string]$p.ConnAuth
                    }
                    if ($p.Grantee -isnot [System.DBNull] -and $p.Grantee -and "$($p.PermState)" -eq 'GRANT') {
                        $info.Grantees.Add([string]$p.Grantee)
                    }
                }
                if (-not $info.EndpointName) { $info.EndpointState = 'no endpoint' }
            }
            catch {
                $msg = "endpoint perms: $($_.Exception.Message)"
                $info.CollectError = if ($info.CollectError) { "$($info.CollectError) | $msg" } else { $msg }
            }

            # Availability group owner (should be [sa]; resolved on this replica).
            if ($info.AvailabilityGroup) {
                try {
                    $agName = ([string]$info.AvailabilityGroup) -replace "'", "''"
                    $agOwnerQuery = @"
SELECT AgOwner = sp.name
FROM sys.availability_groups AS ag WITH (NOLOCK)
INNER JOIN sys.availability_replicas AS ar WITH (NOLOCK) ON ag.group_id = ar.group_id
LEFT JOIN sys.server_principals AS sp WITH (NOLOCK) ON ar.owner_sid = sp.sid
WHERE ag.name = N'$agName' AND ar.replica_server_name = @@SERVERNAME;
"@
                    $ago = Invoke-DbaQuery -SqlInstance $srv -Query $agOwnerQuery -QueryTimeout $qTimeout -ErrorAction Stop
                    if ($ago) {
                        $val = $ago | Select-Object -First 1 -ExpandProperty AgOwner
                        $info.AgOwner = if ($null -eq $val -or $val -is [System.DBNull] -or [string]::IsNullOrEmpty([string]$val)) { '(login not found for owner SID)' } else { [string]$val }
                    }
                }
                catch {
                    $msg = "ag owner: $($_.Exception.Message)"
                    $info.CollectError = if ($info.CollectError) { "$($info.CollectError) | $msg" } else { $msg }
                }
            }

            $svcInfo[$target] = $info
        }

        # 2) Cross-check: each replica's endpoint must GRANT CONNECT to the
        #    OTHER replicas' service accounts.
        foreach ($target in @($svcInfo.Keys)) {
            $info    = $svcInfo[$target]
            $issues  = New-Object System.Collections.Generic.List[string]
            $fixes   = New-Object System.Collections.Generic.List[string]
            $status  = 'OK'

            if (-not $info.Reachable) {
                $status = if ($info.CollectError -match 'timeout') { 'SKIPPED' } else { 'ERROR' }
                $issues.Add($info.CollectError)
            }
            else {
                if ($info.EndpointState -and $info.EndpointState -ne 'STARTED') {
                    $status = 'CHECK'
                    $issues.Add("endpoint not started (state=$($info.EndpointState))")
                    if ($info.EndpointName) { $fixes.Add("ALTER ENDPOINT [$($info.EndpointName)] STATE = STARTED;  -- run on $($info.Replica)") }
                }

                # The HADR endpoint should be owned by [sa]: if the owning login
                # is later dropped the endpoint can break. Warn (never fix).
                if ($info.EndpointName -and $info.EndpointOwner -and $info.EndpointOwner -ne 'sa') {
                    if ($status -eq 'OK') { $status = 'CHECK' }
                    $issues.Add("endpoint owner is [$($info.EndpointOwner)], should be [sa] (fix: ALTER AUTHORIZATION ON ENDPOINT::[$($info.EndpointName)] TO [sa];)")
                    $fixes.Add("ALTER AUTHORIZATION ON ENDPOINT::[$($info.EndpointName)] TO [sa];  -- run on $($info.Replica)")
                }

                # The availability group should also be owned by [sa].
                if ($info.AvailabilityGroup -and $info.AgOwner -and $info.AgOwner -ne 'sa') {
                    if ($status -eq 'OK') { $status = 'CHECK' }
                    $issues.Add("availability group owner is [$($info.AgOwner)], should be [sa] (fix: ALTER AUTHORIZATION ON AVAILABILITY GROUP::[$($info.AvailabilityGroup)] TO [sa];)")
                    $fixes.Add("ALTER AUTHORIZATION ON AVAILABILITY GROUP::[$($info.AvailabilityGroup)] TO [sa];  -- run on $($info.Replica)")
                }

                $missing = New-Object System.Collections.Generic.List[string]
                $missingAccounts = New-Object System.Collections.Generic.List[string]
                $manual  = New-Object System.Collections.Generic.List[string]
                $unknown = New-Object System.Collections.Generic.List[string]
                foreach ($peerKey in $svcInfo.Keys) {
                    if ($peerKey -eq $target) { continue }
                    $peer = $svcInfo[$peerKey]
                    if ([string]::IsNullOrWhiteSpace($peer.ServiceAccount)) {
                        $unknown.Add($peerKey)
                    }
                    elseif (& $isVirtualAccount $peer.ServiceAccount) {
                        $manual.Add("$($peer.ServiceAccount) ($peerKey)")
                    }
                    elseif ($info.Grantees -contains $peer.ServiceAccount) {
                        # granted - OK
                    }
                    else {
                        $missing.Add("$($peer.ServiceAccount) ($peerKey)")
                        $missingAccounts.Add([string]$peer.ServiceAccount)
                    }
                }

                if ($missing.Count -gt 0) {
                    $status = 'CHECK'
                    $issues.Add('MISSING endpoint CONNECT for: ' + ($missing -join ', ') + ' (grant with: GRANT CONNECT ON ENDPOINT::[' + $info.EndpointName + '] TO [account])')
                    foreach ($acct in ($missingAccounts | Select-Object -Unique)) {
                        $fixes.Add("GRANT CONNECT ON ENDPOINT::[$($info.EndpointName)] TO [$acct];  -- run on $($info.Replica)")
                    }
                }
                if ($manual.Count -gt 0) {
                    if ($status -eq 'OK') { $status = 'CHECK' }
                    $issues.Add('verify manually (virtual/machine account authenticates as DOMAIN\host$): ' + ($manual -join ', '))
                }
                if ($unknown.Count -gt 0) {
                    if ($status -eq 'OK') { $status = 'CHECK' }
                    $issues.Add('could not read peer service account for: ' + ($unknown -join ', '))
                }
                if ($info.CollectError) {
                    if ($status -eq 'OK') { $status = 'CHECK' }
                    $issues.Add($info.CollectError)
                }
            }

            $detail = if ($issues.Count -gt 0) {
                $issues -join ' | '
            }
            else {
                'OK: endpoint CONNECT granted to all peer service accounts'
            }

            $report.Add([pscustomobject]@{
                Level             = 'ServiceAccount'
                AvailabilityGroup = [string]$info.AvailabilityGroup
                PrimaryReplica    = ''
                Replica           = [string]$info.Replica
                Role              = [string]$info.Role
                AvailabilityMode  = ''
                FailoverMode      = ''
                ConnectionState   = [string]$info.ConnectionState
                OperationalState  = ''
                RecoveryHealth    = ''
                SyncHealth        = [string]$info.SyncHealth
                DatabaseName      = ''
                SyncState         = ''
                IsSuspended       = ''
                SuspendReason     = ''
                LogSendQueueKB    = $null
                RedoQueueKB       = $null
                JoinState            = ''
                LastConnectErrorNo   = $null
                LastConnectError     = ''
                LastConnectErrorTime = $null
                Node              = [string]$info.Replica
                Ping              = ''
                Port              = $null
                PortOpen          = $status
                ServiceState      = [string]$info.ServiceAccount
                FirewallAllowed   = ''
                IsHadrEnabled     = ''
                EndpointName      = [string]$info.EndpointName
                EndpointState     = [string]$info.EndpointState
                EndpointConnAuth  = [string]$info.ConnAuth
                RecentErrorLog    = [string]$info.EndpointOwner
                Detail            = $detail
                Remediation       = ($fixes -join '  ||  ')
            })
        }
    }

    # =====================================================================
    # Optional read-only INFORMATION / CONFIGURATION + extra health checks.
    # Each is behind its own switch and NEVER changes anything: when a value
    # looks wrong it puts ready-to-run T-SQL / PowerShell in the Remediation
    # field instead of acting on it. All are read from the primary, so the
    # listener name alone is enough.
    # =====================================================================
    if ($CheckListener -or $CheckCluster -or $CheckFailoverReadiness -or $CheckBackup -or $IncludeConfiguration -or $CheckLoginParity -or $CheckVersion -or $CheckEventLog -or $CheckConnection -or $CheckDatabaseState -or $CheckSystemDatabase -or $CheckIoError -or $CheckClockSkew -or $CheckCertificate -or $CheckPendingReboot -or $CheckLoginFailed) {

        # Row factory: full column set (incl. Remediation) so the new sections
        # stay consistent with the existing rows.
        $emitRow = {
            param([hashtable] $Fields)
            $base = [ordered]@{
                Level = ''; AvailabilityGroup = ''; PrimaryReplica = ''; Replica = ''; Role = ''
                AvailabilityMode = ''; FailoverMode = ''; ConnectionState = ''; OperationalState = ''
                RecoveryHealth = ''; SyncHealth = ''; DatabaseName = ''; SyncState = ''; IsSuspended = ''
                SuspendReason = ''; LogSendQueueKB = $null; RedoQueueKB = $null; JoinState = ''
                LastConnectErrorNo = $null; LastConnectError = ''; LastConnectErrorTime = $null
                Node = ''; Ping = ''; Port = $null; PortOpen = ''; ServiceState = ''; FirewallAllowed = ''
                IsHadrEnabled = ''; EndpointName = ''; EndpointState = ''; EndpointConnAuth = ''
                RecentErrorLog = ''; Detail = ''; Remediation = ''
            }
            foreach ($k in $Fields.Keys) { $base[$k] = $Fields[$k] }
            $report.Add([pscustomobject]$base)
        }

        $agNamesAll = @($replicaRows | Select-Object -ExpandProperty AvailabilityGroup -Unique)

        # --- Listener configuration (-CheckListener) ----------------------
        if ($CheckListener) {
            Write-Verbose 'Reading Availability Group listener configuration (-CheckListener)...'
            $listenerRows = @()
            try {
                $listenerRows = @(Invoke-DbaQuery @queryParams -Query @"
SELECT
    AvailabilityGroup = ag.name,
    ListenerName      = agl.dns_name,
    Port              = agl.port,
    IpAddress         = lip.ip_address,
    SubnetMask        = lip.ip_subnet_mask,
    IsDhcp            = lip.is_dhcp,
    State             = lip.state_desc
FROM sys.availability_groups AS ag WITH (NOLOCK)
LEFT JOIN sys.availability_group_listeners AS agl WITH (NOLOCK)
    ON agl.group_id = ag.group_id
LEFT JOIN sys.availability_group_listener_ip_addresses AS lip WITH (NOLOCK)
    ON lip.listener_id = agl.listener_id
ORDER BY ag.name, agl.dns_name;
"@)
            }
            catch {
                & $emitRow @{ Level = 'Listener'; PortOpen = 'ERROR'; Detail = "Could not read listener config: $($_.Exception.Message)" }
            }

            $agWithListener = @($listenerRows |
                Where-Object { $_.ListenerName -and $_.ListenerName -isnot [System.DBNull] } |
                Select-Object -ExpandProperty AvailabilityGroup -Unique)

            foreach ($agn in $agNamesAll) {
                if ($agWithListener -notcontains $agn) {
                    & $emitRow @{
                        Level = 'Listener'; AvailabilityGroup = $agn; PortOpen = 'CHECK'
                        Detail = 'No Availability Group listener is configured for this AG.'
                        Remediation = "ALTER AVAILABILITY GROUP [$agn] ADD LISTENER N'<listener-name>' (WITH IP ((N'<ip-address>', N'<subnet-mask>')), PORT = 1433);"
                    }
                }
            }

            foreach ($lr in ($listenerRows | Where-Object { $_.ListenerName -and $_.ListenerName -isnot [System.DBNull] })) {
                $lname  = [string]$lr.ListenerName
                $lstate = [string]$lr.State
                $issues = New-Object System.Collections.Generic.List[string]
                $status = 'OK'
                $remed  = ''

                if ($lstate -and $lstate -ne 'ONLINE') {
                    $status = 'CHECK'
                    $issues.Add("listener IP state = $lstate (expected ONLINE)")
                    $remed  = "Get-ClusterResource | Where-Object Name -like '*$lname*' | Start-ClusterResource   # bring the WSFC listener resource online"
                }

                $dns = ''
                try   { $dns = ([System.Net.Dns]::GetHostAddresses($lname) | ForEach-Object { $_.IPAddressToString }) -join ', ' }
                catch { if ($status -eq 'OK') { $status = 'CHECK' }; $issues.Add("DNS could not resolve '$lname'") }

                $portVal = if ($lr.Port -is [System.DBNull]) { $null } else { [int]$lr.Port }
                $detail  = "Port=$portVal; IP=$($lr.IpAddress); Mask=$($lr.SubnetMask); DHCP=$($lr.IsDhcp); State=$lstate; DNS=$dns"
                if ($issues.Count -gt 0) { $detail = ($issues -join ' | ') + ' | ' + $detail }

                & $emitRow @{
                    Level = 'Listener'; AvailabilityGroup = [string]$lr.AvailabilityGroup
                    EndpointName = $lname; Port = $portVal; PortOpen = $status
                    Detail = $detail; Remediation = $remed
                }
            }
        }

        # --- WSFC cluster / quorum (-CheckCluster) ------------------------
        if ($CheckCluster) {
            Write-Verbose 'Reading WSFC cluster / quorum state (-CheckCluster)...'

            if (-not $hasCluster) {
                & $emitRow @{ Level = 'Cluster'; PortOpen = 'SKIPPED'; Detail = 'No WSFC cluster (standalone / clusterless instance): no quorum to evaluate.' }
            }
            else {
                $clusterType = ''
                if ($major -ge 14) {
                    try {
                        $ct = Invoke-DbaQuery @queryParams -Query "SELECT TOP (1) cluster_type_desc FROM sys.availability_groups WITH (NOLOCK)"
                        $clusterType = [string]$ct.cluster_type_desc
                    } catch {}
                }

                $cl = $null
                try { $cl = Invoke-DbaQuery @queryParams -Query "SELECT cluster_name, quorum_type_desc, quorum_state_desc FROM sys.dm_hadr_cluster WITH (NOLOCK)" } catch {}

                if (-not $cl) {
                    & $emitRow @{ Level = 'Cluster'; PortOpen = 'CHECK'; ServiceState = $clusterType; Detail = 'sys.dm_hadr_cluster returned no rows (insufficient rights?).' }
                }
                else {
                    $qstate  = [string]$cl.quorum_state_desc
                    $qstatus = if ($qstate -match 'NORMAL') { 'OK' } else { 'FAILED' }
                    & $emitRow @{
                        Level = 'Cluster'; PortOpen = $qstatus; ServiceState = [string]$cl.cluster_name
                        Detail = "ClusterType=$clusterType; Cluster=$($cl.cluster_name); Quorum=$qstate; Model=$($cl.quorum_type_desc)"
                        Remediation = $(if ($qstatus -ne 'OK') { 'Get-ClusterQuorum; Get-ClusterNode   # review WSFC quorum / witness configuration' } else { '' })
                    }

                    $members = @()
                    try { $members = @(Invoke-DbaQuery @queryParams -Query "SELECT member_name, member_type_desc, member_state_desc, number_of_quorum_votes FROM sys.dm_hadr_cluster_members WITH (NOLOCK)") } catch {}
                    foreach ($m in $members) {
                        $mstate  = [string]$m.member_state_desc
                        $mstatus = if ($mstate -eq 'UP') { 'OK' } else { 'FAILED' }
                        & $emitRow @{
                            Level = 'ClusterMember'; Node = [string]$m.member_name; Role = [string]$m.member_type_desc
                            PortOpen = $mstatus; Detail = "state=$mstate; quorum_votes=$($m.number_of_quorum_votes)"
                            Remediation = $(if ($mstatus -ne 'OK') { "Get-ClusterNode -Name '$([string]$m.member_name)'   # WSFC member is $mstate - investigate the node/service" } else { '' })
                        }
                    }
                }
            }
        }

        # --- Failover readiness / RPO lag (-CheckFailoverReadiness) -------
        if ($CheckFailoverReadiness) {
            Write-Verbose 'Evaluating failover readiness and secondary lag (-CheckFailoverReadiness)...'
            $lagWarnSeconds = 60
            $lagCol = if ($major -ge 13) { 'drs.secondary_lag_seconds' } else { 'CAST(NULL AS int)' }
            $frRows = @()
            try {
                $frRows = @(Invoke-DbaQuery @queryParams -Query @"
SELECT
    AvailabilityGroup = ag.name,
    Replica           = ar.replica_server_name,
    DatabaseName      = drcs.database_name,
    Role              = rs.role_desc,
    AvailabilityMode  = ar.availability_mode_desc,
    IsJoined          = drcs.is_database_joined,
    IsFailoverReady   = drcs.is_failover_ready,
    IsPendingSuspend  = drcs.is_pending_secondary_suspend,
    LagSeconds        = $lagCol,
    LastCommitTime    = drs.last_commit_time
FROM sys.availability_groups AS ag WITH (NOLOCK)
JOIN sys.availability_replicas AS ar WITH (NOLOCK)
    ON ar.group_id = ag.group_id
JOIN sys.dm_hadr_availability_replica_states AS rs WITH (NOLOCK)
    ON rs.replica_id = ar.replica_id AND rs.group_id = ar.group_id
JOIN sys.dm_hadr_database_replica_cluster_states AS drcs WITH (NOLOCK)
    ON drcs.replica_id = ar.replica_id
LEFT JOIN sys.dm_hadr_database_replica_states AS drs WITH (NOLOCK)
    ON drs.replica_id = ar.replica_id AND drs.group_database_id = drcs.group_database_id
ORDER BY ag.name, ar.replica_server_name, drcs.database_name;
"@)
            }
            catch {
                & $emitRow @{ Level = 'FailoverReadiness'; PortOpen = 'ERROR'; Detail = "Could not read failover-readiness DMVs: $($_.Exception.Message)" }
            }

            foreach ($fr in $frRows) {
                $isJoined = [int]($(if ($fr.IsJoined -is [System.DBNull]) { 1 } else { $fr.IsJoined }))
                $isReady  = $(if ($fr.IsFailoverReady -is [System.DBNull]) { $null } else { [int]$fr.IsFailoverReady })
                $role     = [string]$fr.Role
                $avMode   = [string]$fr.AvailabilityMode
                $lag      = $(if ($fr.LagSeconds -is [System.DBNull]) { $null } else { [int]$fr.LagSeconds })
                $issues   = New-Object System.Collections.Generic.List[string]
                $status   = 'OK'
                $remed    = ''

                if ($isJoined -ne 1) {
                    $status = 'CHECK'
                    $issues.Add('database is NOT joined to the AG on this replica')
                    $remed  = "ALTER DATABASE [$([string]$fr.DatabaseName)] SET HADR AVAILABILITY GROUP = [$([string]$fr.AvailabilityGroup)];   -- run on $([string]$fr.Replica)"
                }
                if ($role -eq 'SECONDARY' -and $avMode -eq 'SYNCHRONOUS_COMMIT' -and $isReady -eq 0) {
                    $status = 'CHECK'
                    $issues.Add('synchronous secondary is NOT failover-ready (data loss on failover)')
                    if (-not $remed) { $remed = "-- Investigate sync on [$([string]$fr.Replica)] (SyncHealth / redo queue). If suspended: ALTER DATABASE [$([string]$fr.DatabaseName)] SET HADR RESUME;" }
                }
                if ($null -ne $lag -and $lag -ge $lagWarnSeconds) {
                    if ($status -eq 'OK') { $status = 'WARN' }
                    $issues.Add("secondary lag = ${lag}s (>= ${lagWarnSeconds}s RPO threshold)")
                }

                $detail = "Role=$role; Mode=$avMode; Joined=$isJoined; FailoverReady=$isReady; Lag=$($lag)s; LastCommit=$($fr.LastCommitTime)"
                if ($issues.Count -gt 0) { $detail = ($issues -join ' | ') + ' | ' + $detail }

                & $emitRow @{
                    Level = 'FailoverReadiness'; AvailabilityGroup = [string]$fr.AvailabilityGroup
                    Replica = [string]$fr.Replica; Node = [string]$fr.Replica; Role = $role
                    AvailabilityMode = $avMode; DatabaseName = [string]$fr.DatabaseName
                    PortOpen = $status; Detail = $detail; Remediation = $remed
                }
            }
        }

        # --- Backup preference / log reuse (-CheckBackup) -----------------
        if ($CheckBackup) {
            Write-Verbose 'Checking backup preference and log-reuse state (-CheckBackup)...'

            $agPref = @()
            try { $agPref = @(Invoke-DbaQuery @queryParams -Query "SELECT AvailabilityGroup = name, Pref = automated_backup_preference_desc FROM sys.availability_groups WITH (NOLOCK)") } catch {}
            foreach ($agb in $agPref) {
                & $emitRow @{ Level = 'Backup'; AvailabilityGroup = [string]$agb.AvailabilityGroup; PortOpen = 'INFO'; Detail = "Automated backup preference = $($agb.Pref)" }
            }

            $lastBk = @{}
            try {
                foreach ($b in @(Invoke-DbaQuery @queryParams -Query @"
SELECT DatabaseName = database_name, BackupType = type, LastBackup = MAX(backup_finish_date)
FROM msdb.dbo.backupset WITH (NOLOCK)
WHERE type IN ('D','L')
GROUP BY database_name, type;
"@)) { $lastBk["$([string]$b.DatabaseName)|$([string]$b.BackupType)"] = $b.LastBackup }
            } catch {}

            $bkDbs = @()
            try {
                $bkDbs = @(Invoke-DbaQuery @queryParams -Query @"
SELECT
    DatabaseName      = d.name,
    LogReuseWait      = d.log_reuse_wait_desc,
    RecoveryModel     = d.recovery_model_desc,
    IsPreferredBackup = sys.fn_hadr_backup_is_preferred_replica(d.name)
FROM sys.databases AS d WITH (NOLOCK)
WHERE d.group_database_id IS NOT NULL
ORDER BY d.name;
"@)
            }
            catch {
                & $emitRow @{ Level = 'Backup'; PortOpen = 'ERROR'; Detail = "Could not read backup / log-reuse state: $($_.Exception.Message)" }
            }

            foreach ($bd in $bkDbs) {
                $dbn      = [string]$bd.DatabaseName
                $logReuse = [string]$bd.LogReuseWait
                $status   = 'OK'
                $remed    = ''
                $issues   = New-Object System.Collections.Generic.List[string]

                if ($logReuse -eq 'AVAILABILITY_REPLICA') {
                    $status = 'WARN'
                    $issues.Add('log_reuse_wait = AVAILABILITY_REPLICA (a secondary has not hardened the log; the log cannot truncate and will grow)')
                    $remed  = "-- A secondary is not hardening the log. Fix the lagging/disconnected secondary (see FailoverReadiness / Database sync). Log backups must run on the preferred replica, e.g.: Backup-DbaDatabase -SqlInstance '<preferred-replica>' -Database '$dbn' -Type Log"
                }

                $lastFull = $lastBk["$dbn|D"]
                $lastLog  = $lastBk["$dbn|L"]
                $detail   = "Recovery=$($bd.RecoveryModel); LogReuse=$logReuse; PreferredBackupHere=$($bd.IsPreferredBackup); LastFull(local msdb)=$lastFull; LastLog(local msdb)=$lastLog"
                if ($issues.Count -gt 0) { $detail = ($issues -join ' | ') + ' | ' + $detail }

                & $emitRow @{ Level = 'Backup'; DatabaseName = $dbn; PortOpen = $status; Detail = $detail; Remediation = $remed }
            }
        }

        # --- AlwaysOn / cluster configuration snapshot (-IncludeConfiguration)
        if ($IncludeConfiguration) {
            Write-Verbose 'Collecting Always On / cluster configuration snapshot (-IncludeConfiguration)...'

            $colClusterType = if ($major -ge 14) { 'ag.cluster_type_desc' }                          else { 'CAST(NULL AS varchar(60))' }
            $colReqSync     = if ($major -ge 14) { 'ag.required_synchronized_secondaries_to_commit' } else { 'CAST(NULL AS int)' }
            $colDbFailover  = if ($major -ge 13) { 'ag.db_failover' }                                 else { 'CAST(NULL AS bit)' }
            $colDtc         = if ($major -ge 13) { 'ag.dtc_support' }                                  else { 'CAST(NULL AS bit)' }
            $colBasic       = if ($major -ge 13) { 'ag.basic_features' }                               else { 'CAST(NULL AS bit)' }
            $colDist        = if ($major -ge 13) { 'ag.is_distributed' }                               else { 'CAST(NULL AS bit)' }

            try {
                foreach ($agc in @(Invoke-DbaQuery @queryParams -Query @"
SELECT
    AvailabilityGroup     = ag.name,
    ClusterType           = $colClusterType,
    BackupPreference      = ag.automated_backup_preference_desc,
    FailureConditionLevel = ag.failure_condition_level,
    HealthCheckTimeout    = ag.health_check_timeout,
    DbLevelFailover       = $colDbFailover,
    DtcSupport            = $colDtc,
    RequiredSyncSecondary = $colReqSync,
    BasicFeatures         = $colBasic,
    IsDistributed         = $colDist
FROM sys.availability_groups AS ag WITH (NOLOCK)
ORDER BY ag.name;
"@)) {
                    & $emitRow @{
                        Level = 'Configuration'; AvailabilityGroup = [string]$agc.AvailabilityGroup; PortOpen = 'INFO'
                        Detail = "ClusterType=$($agc.ClusterType); BackupPref=$($agc.BackupPreference); FailureConditionLevel=$($agc.FailureConditionLevel); HealthCheckTimeout=$($agc.HealthCheckTimeout)ms; DbLevelFailover=$($agc.DbLevelFailover); DTC=$($agc.DtcSupport); RequiredSyncSecondaries=$($agc.RequiredSyncSecondary); Basic=$($agc.BasicFeatures); Distributed=$($agc.IsDistributed)"
                    }
                }
            }
            catch {
                & $emitRow @{ Level = 'Configuration'; PortOpen = 'ERROR'; Detail = "Could not read AG configuration: $($_.Exception.Message)" }
            }

            $colSeed = if ($major -ge 13) { 'ar.seeding_mode_desc' } else { 'CAST(NULL AS varchar(20))' }
            try {
                foreach ($arc in @(Invoke-DbaQuery @queryParams -Query @"
SELECT
    AvailabilityGroup = ag.name,
    Replica           = ar.replica_server_name,
    AvailabilityMode  = ar.availability_mode_desc,
    FailoverMode      = ar.failover_mode_desc,
    SeedingMode       = $colSeed,
    SessionTimeout    = ar.session_timeout,
    EndpointUrl       = ar.endpoint_url,
    PrimaryAllow      = ar.primary_role_allow_connections_desc,
    SecondaryAllow    = ar.secondary_role_allow_connections_desc,
    ReadOnlyRouting   = ar.read_only_routing_url,
    BackupPriority    = ar.backup_priority
FROM sys.availability_replicas AS ar WITH (NOLOCK)
JOIN sys.availability_groups AS ag WITH (NOLOCK) ON ag.group_id = ar.group_id
ORDER BY ag.name, ar.replica_server_name;
"@)) {
                    $avMode = [string]$arc.AvailabilityMode
                    $foMode = [string]$arc.FailoverMode
                    $stO    = $(if ($arc.SessionTimeout -is [System.DBNull]) { $null } else { [int]$arc.SessionTimeout })
                    $status = 'INFO'
                    $issues = New-Object System.Collections.Generic.List[string]
                    $remed  = ''

                    if ($foMode -eq 'AUTOMATIC' -and $avMode -ne 'SYNCHRONOUS_COMMIT') {
                        $status = 'CHECK'
                        $issues.Add("automatic failover requires SYNCHRONOUS_COMMIT (current mode is $avMode)")
                        $remed  = "ALTER AVAILABILITY GROUP [$([string]$arc.AvailabilityGroup)] MODIFY REPLICA ON N'$([string]$arc.Replica)' WITH (AVAILABILITY_MODE = SYNCHRONOUS_COMMIT);"
                    }
                    if ($null -ne $stO -and $stO -lt 10) {
                        if ($status -eq 'INFO') { $status = 'WARN' }
                        $issues.Add("session_timeout = ${stO}s is low (can cause replica flapping)")
                    }

                    $detail = "Mode=$avMode; Failover=$foMode; Seeding=$($arc.SeedingMode); SessionTimeout=$($stO)s; PrimaryAllow=$($arc.PrimaryAllow); SecondaryAllow=$($arc.SecondaryAllow); RO_Routing=$($arc.ReadOnlyRouting); BackupPriority=$($arc.BackupPriority); Endpoint=$($arc.EndpointUrl)"
                    if ($issues.Count -gt 0) { $detail = ($issues -join ' | ') + ' | ' + $detail }

                    & $emitRow @{
                        Level = 'Configuration'; AvailabilityGroup = [string]$arc.AvailabilityGroup
                        Replica = [string]$arc.Replica; Node = [string]$arc.Replica
                        AvailabilityMode = $avMode; FailoverMode = $foMode
                        EndpointName = [string]$arc.EndpointUrl; PortOpen = $status
                        Detail = $detail; Remediation = $remed
                    }
                }
            }
            catch {
                & $emitRow @{ Level = 'Configuration'; PortOpen = 'ERROR'; Detail = "Could not read replica configuration: $($_.Exception.Message)" }
            }
        }

        # --- Login parity across replicas (-CheckLoginParity) -------------
        # For a clean failover every replica must own the same logins. Connect
        # to each replica, list the (non-system) logins, and report ONLY the
        # ones missing on one or more servers - the full login list and the
        # logins' rights are NOT reported.
        if ($CheckLoginParity) {
            if (-not $isHadr) {
                & $emitRow @{ Level = 'LoginParity'; PortOpen = 'SKIPPED'; Detail = 'No Always On (standalone / clusterless instance): no other replicas to compare logins against.' }
            }
            else {
            Write-Verbose 'Comparing server logins across replicas (-CheckLoginParity)...'
            $loginStopwatch = [System.Diagnostics.Stopwatch]::StartNew()

            $loginQuery = @"
SELECT LoginName = name, TypeDesc = type_desc
FROM sys.server_principals WITH (NOLOCK)
WHERE type IN ('S', 'U', 'G')
  AND name NOT LIKE '##%'
  AND name NOT LIKE 'NT SERVICE\%'
  AND name NOT LIKE 'NT AUTHORITY\%'
  AND name NOT LIKE 'BUILTIN\%';
"@

            $loginServers = [ordered]@{}
            $loginType    = @{}
            $reachable    = New-Object System.Collections.Generic.List[string]
            $replicaNames = @($replicaRows | Select-Object -ExpandProperty Replica -Unique | Where-Object { $_ })

            foreach ($srvName in $replicaNames) {
                $budgetLeft = $SecondaryDetailTimeoutSeconds - $loginStopwatch.Elapsed.TotalSeconds
                if ($budgetLeft -le 0) {
                    & $emitRow @{ Level = 'LoginParity'; Node = $srvName; PortOpen = 'SKIPPED'; Detail = "skipped (timeout): login-parity budget of ${SecondaryDetailTimeoutSeconds}s exceeded" }
                    continue
                }
                $qTimeout = [int][math]::Max(1, [math]::Ceiling($budgetLeft))

                $rRow  = $replicaRows | Where-Object { [string]$_.Replica -eq $srvName } | Select-Object -First 1
                $alive = if ($rRow) { & $testReplicaAlive $rRow ($ConnectTimeoutSeconds * 1000) } else { $null }
                if ($alive -eq $false) {
                    & $emitRow @{ Level = 'LoginParity'; Node = $srvName; PortOpen = 'ERROR'; Detail = "connect: host did not answer on endpoint port within ${ConnectTimeoutSeconds}s (logins not compared for this server)" }
                    continue
                }

                $srv = $null
                try {
                    $connParams = @{ SqlInstance = $srvName; ConnectTimeout = $ConnectTimeoutSeconds; ErrorAction = 'Stop' }
                    if ($SqlCredential) { $connParams['SqlCredential'] = $SqlCredential }
                    $srv = Connect-DbaInstance @connParams
                }
                catch {
                    & $emitRow @{ Level = 'LoginParity'; Node = $srvName; PortOpen = 'ERROR'; Detail = "connect: $($_.Exception.Message) (logins not compared for this server)" }
                    continue
                }

                try {
                    $logins = @(Invoke-DbaQuery -SqlInstance $srv -Query $loginQuery -QueryTimeout $qTimeout -ErrorAction Stop)
                    $reachable.Add($srvName)
                    foreach ($lg in $logins) {
                        $ln = [string]$lg.LoginName
                        if (-not $loginServers.Contains($ln)) { $loginServers[$ln] = (New-Object System.Collections.Generic.List[string]) }
                        $loginServers[$ln].Add($srvName)
                        if (-not $loginType.ContainsKey($ln)) { $loginType[$ln] = [string]$lg.TypeDesc }
                    }
                }
                catch {
                    & $emitRow @{ Level = 'LoginParity'; Node = $srvName; PortOpen = 'ERROR'; Detail = "read logins: $($_.Exception.Message)" }
                }
            }

            if ($reachable.Count -lt 2) {
                & $emitRow @{ Level = 'LoginParity'; PortOpen = 'INFO'; Detail = "Login parity needs at least 2 reachable replicas; compared $($reachable.Count)." }
            }
            else {
                $missingAny = $false
                foreach ($ln in @($loginServers.Keys)) {
                    $have    = $loginServers[$ln]
                    $missing = @($reachable | Where-Object { $have -notcontains $_ })
                    if ($missing.Count -gt 0) {
                        $missingAny = $true
                        $src   = $have[0]
                        $remed = ($missing | ForEach-Object { "Copy-DbaLogin -Source '$src' -Destination '$_' -Login '$ln'" }) -join '; '
                        & $emitRow @{
                            Level = 'LoginParity'; ServiceState = $ln; Role = $loginType[$ln]; PortOpen = 'CHECK'
                            Detail = ("type=$($loginType[$ln]); present on: " + ($have -join ', ') + '; MISSING on: ' + ($missing -join ', '))
                            Remediation = $remed
                        }
                    }
                }
                if (-not $missingAny) {
                    & $emitRow @{ Level = 'LoginParity'; PortOpen = 'OK'; Detail = ("All logins present on all replicas (" + ($reachable -join ', ') + ").") }
                }
            }
            }
        }

        # --- SQL build / update parity across replicas (-CheckVersion) ----
        # Every replica should run the SAME SQL Server build (CU / patch level);
        # a build mismatch is a supportability and failover risk. Connect to each
        # replica, read its ProductVersion / patch level, and flag any mismatch.
        if ($CheckVersion) {
            Write-Verbose 'Comparing SQL Server build / update level across replicas (-CheckVersion)...'
            $verStopwatch = [System.Diagnostics.Stopwatch]::StartNew()

            $versionQuery = @"
SELECT
    ProductVersion = CONVERT(sysname, SERVERPROPERTY('ProductVersion')),
    ProductLevel   = CONVERT(sysname, SERVERPROPERTY('ProductLevel')),
    ProductUpdate  = CONVERT(sysname, SERVERPROPERTY('ProductUpdateLevel')),
    Edition        = CONVERT(sysname, SERVERPROPERTY('Edition'))
"@

            $verByServer  = [ordered]@{}
            $replicaNames = @($replicaRows | Select-Object -ExpandProperty Replica -Unique | Where-Object { $_ })

            foreach ($srvName in $replicaNames) {
                $budgetLeft = $SecondaryDetailTimeoutSeconds - $verStopwatch.Elapsed.TotalSeconds
                if ($budgetLeft -le 0) {
                    & $emitRow @{ Level = 'VersionParity'; Replica = $srvName; Node = $srvName; PortOpen = 'SKIPPED'; Detail = "skipped (timeout): version budget of ${SecondaryDetailTimeoutSeconds}s exceeded" }
                    continue
                }
                $qTimeout = [int][math]::Max(1, [math]::Ceiling($budgetLeft))

                $rRow  = $replicaRows | Where-Object { [string]$_.Replica -eq $srvName } | Select-Object -First 1
                $alive = if ($rRow) { & $testReplicaAlive $rRow ($ConnectTimeoutSeconds * 1000) } else { $null }
                if ($alive -eq $false) {
                    & $emitRow @{ Level = 'VersionParity'; Replica = $srvName; Node = $srvName; PortOpen = 'ERROR'; Detail = "connect: host did not answer on endpoint port within ${ConnectTimeoutSeconds}s (version not read)" }
                    continue
                }

                try {
                    $vconn = @{ SqlInstance = $srvName; ConnectTimeout = $ConnectTimeoutSeconds; ErrorAction = 'Stop' }
                    if ($SqlCredential) { $vconn['SqlCredential'] = $SqlCredential }
                    $vsrv = Connect-DbaInstance @vconn
                    $vrow = Invoke-DbaQuery -SqlInstance $vsrv -Query $versionQuery -QueryTimeout $qTimeout -ErrorAction Stop
                    $verByServer[$srvName] = [pscustomobject]@{
                        ProductVersion = [string]$vrow.ProductVersion
                        ProductLevel   = [string]$vrow.ProductLevel
                        ProductUpdate  = [string]$vrow.ProductUpdate
                        Edition        = [string]$vrow.Edition
                    }
                }
                catch {
                    & $emitRow @{ Level = 'VersionParity'; Replica = $srvName; Node = $srvName; PortOpen = 'ERROR'; Detail = "read version: $($_.Exception.Message)" }
                }
            }

            # One INFO row per replica showing its build.
            foreach ($srvName in @($verByServer.Keys)) {
                $vi = $verByServer[$srvName]
                & $emitRow @{
                    Level = 'VersionParity'; Replica = $srvName; Node = $srvName; PortOpen = 'INFO'
                    Detail = "Version=$($vi.ProductVersion); Level=$($vi.ProductLevel); Update=$($vi.ProductUpdate); Edition=$($vi.Edition)"
                }
            }

            $distinctVersions = @($verByServer.Values | ForEach-Object { $_.ProductVersion } | Sort-Object -Unique)
            if ($verByServer.Count -lt 2) {
                & $emitRow @{ Level = 'VersionParity'; PortOpen = 'INFO'; Detail = "Version parity needs at least 2 reachable replicas; compared $($verByServer.Count)." }
            }
            elseif ($distinctVersions.Count -le 1) {
                & $emitRow @{ Level = 'VersionParity'; PortOpen = 'OK'; Detail = "All replicas run the same build ($($distinctVersions -join ', '))." }
            }
            else {
                $pairs = ($verByServer.Keys | ForEach-Object { "$_=$($verByServer[$_].ProductVersion)" }) -join '; '
                & $emitRow @{
                    Level = 'VersionParity'; PortOpen = 'CHECK'
                    Detail = "SQL build MISMATCH across replicas: $pairs"
                    Remediation = 'Patch the lagging replica(s) so every node runs the same SQL Server build/CU. Apply on secondaries first, then fail over and patch the former primary.'
                }
            }
        }

        # --- Windows cluster + listener event log (-CheckEventLog) --------
        # Reads Failover Clustering events from the System log of each cluster
        # node for the last -EventHoursBack hours and produces two views:
        #   * ClusterEvent  - cluster-health failures (quorum / node / resource
        #                     / role), regardless of listener.
        #   * ListenerEvent - events whose message references an AG listener.
        # Only the LAST (most recent) occurrence is shown; the colour follows the
        # number of occurrences in the window: 0 -> OK, 1 -> WARN (orange, one
        # off), >1 -> FAILED (red). Read-only; remote reads reuse -WmiCredential.
        if ($CheckEventLog) {
            Write-Verbose ("Reading Windows Failover Clustering events for the last {0}h (-CheckEventLog)..." -f $EventHoursBack)

            if (-not $hasCluster) {
                & $emitRow @{ Level = 'ClusterEvent';  PortOpen = 'SKIPPED'; Detail = 'No WSFC cluster (standalone instance): no Failover Clustering event log to read.' }
                & $emitRow @{ Level = 'ListenerEvent'; PortOpen = 'SKIPPED'; Detail = 'No WSFC cluster (standalone instance): no listener events to read.' }
            }
            else {

            # Cluster-health event IDs (resource / node / quorum / role failures).
            $clusterHealthIds = @(1069, 1135, 1177, 1205, 1230, 1254)

            # Discover the WSFC node names (fall back to the replica host names).
            $eventNodes = @()
            try {
                $eventNodes = @(Invoke-DbaQuery @queryParams -Query "SELECT member_name FROM sys.dm_hadr_cluster_members WITH (NOLOCK) WHERE member_type_desc LIKE '%node%'" |
                    ForEach-Object { [string]$_.member_name } | Where-Object { $_ })
            } catch {}
            if ($eventNodes.Count -eq 0) {
                $eventNodes = @($replicaRows | ForEach-Object {
                    if ($_.EndpointUrl -match '^(?i:TCP)://\[?([^\]:]+)\]?:\d+') { $matches[1] } else { [string]$_.Replica }
                } | Where-Object { $_ } | Select-Object -Unique)
            }

            # Listener DNS names (self-contained; independent of -CheckListener).
            $listenerNames = @()
            try {
                $listenerNames = @(Invoke-DbaQuery @queryParams -Query "SELECT dns_name FROM sys.availability_group_listeners WITH (NOLOCK)" |
                    ForEach-Object { [string]$_.dns_name } | Where-Object { $_ })
            } catch {}

            $eventStart = (Get-Date).AddHours(-$EventHoursBack)
            $localNames = @($env:COMPUTERNAME, "$env:COMPUTERNAME.$env:USERDNSDOMAIN", 'localhost', '.') | Where-Object { $_ }

            # Pull the raw events from every node (best-effort per node).
            $allEvents       = New-Object System.Collections.Generic.List[object]
            $eventReadErrors = New-Object System.Collections.Generic.List[string]
            foreach ($node in $eventNodes) {
                try {
                    $gweParams = @{
                        ComputerName    = $node
                        FilterHashtable = @{ LogName = 'System'; ProviderName = 'Microsoft-Windows-FailoverClustering'; StartTime = $eventStart }
                        ErrorAction     = 'Stop'
                    }
                    if ($WmiCredential -and ($localNames -notcontains $node)) { $gweParams['Credential'] = $WmiCredential }

                    foreach ($ev in @(Get-WinEvent @gweParams)) {
                        $allEvents.Add([pscustomobject]@{
                            Node        = $node
                            TimeCreated = $ev.TimeCreated
                            Id          = [int]$ev.Id
                            Level       = [string]$ev.LevelDisplayName
                            Message     = ([string]$ev.Message -replace "`r|`n", ' ')
                        })
                    }
                }
                catch {
                    # "No events were found" simply means a clean node - not an error.
                    if ("$($_.Exception.Message)" -notmatch 'No events were found') {
                        $eventReadErrors.Add("${node}: $($_.Exception.Message)")
                    }
                }
            }

            # Drop identical events logged on more than one node so the count
            # reflects distinct occurrences.
            $distinctEvents = @($allEvents |
                Group-Object { "$($_.TimeCreated.Ticks)|$($_.Id)|$($_.Message)" } |
                ForEach-Object { $_.Group | Select-Object -First 1 })

            # Occurrence count -> status token / colour.
            $eventStatus = { param([int] $n) if ($n -le 0) { 'OK' } elseif ($n -eq 1) { 'WARN' } else { 'FAILED' } }

            # Flag any node we could not read (best-effort / informational).
            foreach ($erx in $eventReadErrors) {
                & $emitRow @{ Level = 'ClusterEvent'; PortOpen = 'CHECK'; Detail = "Could not read Windows events from $erx" }
            }

            # -- Cluster-status events -------------------------------------
            $clusterEvents = @($distinctEvents | Where-Object { $clusterHealthIds -contains $_.Id })
            if ($clusterEvents.Count -eq 0) {
                & $emitRow @{ Level = 'ClusterEvent'; PortOpen = 'OK'; Detail = "No cluster-failure events (IDs $($clusterHealthIds -join ', ')) in the last ${EventHoursBack}h on: $($eventNodes -join ', ')." }
            }
            else {
                $lastCe = $clusterEvents | Sort-Object TimeCreated -Descending | Select-Object -First 1
                & $emitRow @{
                    Level = 'ClusterEvent'; Node = $lastCe.Node; PortOpen = (& $eventStatus $clusterEvents.Count)
                    Detail = ("{0} cluster event(s) in last {1}h. LAST: {2} [ID {3}] on {4}: {5}" -f $clusterEvents.Count, $EventHoursBack, $lastCe.TimeCreated, $lastCe.Id, $lastCe.Node, $lastCe.Message)
                    Remediation = "Get-ClusterNode; Get-ClusterResource | Where-Object State -ne 'Online'   # investigate the WSFC failure above"
                }
            }

            # -- Listener error events -------------------------------------
            if ($listenerNames.Count -eq 0) {
                & $emitRow @{ Level = 'ListenerEvent'; PortOpen = 'INFO'; Detail = 'No AG listener is configured, so no listener events were evaluated.' }
            }
            else {
                foreach ($ln in $listenerNames) {
                    $lnEvents = @($distinctEvents | Where-Object { $_.Message -match [regex]::Escape($ln) })
                    if ($lnEvents.Count -eq 0) {
                        & $emitRow @{ Level = 'ListenerEvent'; EndpointName = $ln; PortOpen = 'OK'; Detail = "No events referencing listener '$ln' in the last ${EventHoursBack}h." }
                    }
                    else {
                        $lastLe = $lnEvents | Sort-Object TimeCreated -Descending | Select-Object -First 1
                        & $emitRow @{
                            Level = 'ListenerEvent'; EndpointName = $ln; Node = $lastLe.Node; PortOpen = (& $eventStatus $lnEvents.Count)
                            Detail = ("{0} event(s) referencing '{1}' in last {2}h. LAST: {3} [ID {4}] on {5}: {6}" -f $lnEvents.Count, $ln, $EventHoursBack, $lastLe.TimeCreated, $lastLe.Id, $lastLe.Node, $lastLe.Message)
                            Remediation = "Get-ClusterResource | Where-Object Name -like '*$ln*' | Format-List *; Test-DbaConnection -SqlInstance '$ln'   # investigate the listener / Network Name resource"
                        }
                    }
                }
            }
            }
        }

        # =====================================================================
        # Instance-level readiness checks. These run even on a standalone (non
        # Always On) instance, so they iterate the AG replicas when clustered,
        # otherwise just the local instance.
        # =====================================================================
        $instanceTargets = if ($isHadr -and $replicaRows.Count) {
            @($replicaRows | Select-Object -ExpandProperty Replica -Unique | Where-Object { $_ })
        }
        else {
            @([string]$hadr.ServerName)
        }
        $nodeTargets = @($instanceTargets | ForEach-Object { ($_ -split '\\')[0] } | Where-Object { $_ } | Select-Object -Unique)

        if (-not $isHadr) {
            & $emitRow @{
                Level = 'AlwaysOn'; PortOpen = 'SKIPPED'; ServiceState = [string]$hadr.ServerName
                Detail = 'Standalone instance (Always On not enabled): AG replica / database / listener / cluster / failover checks are skipped; instance-level checks still run.'
            }
        }

        # --- Real connection smoke test (-CheckConnection) ----------------
        if ($CheckConnection) {
            Write-Verbose 'Running connection smoke test (-CheckConnection)...'
            foreach ($srv in $instanceTargets) {
                $t0 = Get-Date
                try {
                    $null = & $invokeWithRetry -Operation "connect smoke test on [$srv]" -ScriptBlock {
                        $cparams = @{ SqlInstance = $srv; ConnectTimeout = $ConnectTimeoutSeconds; ErrorAction = 'Stop' }
                        if ($SqlCredential) { $cparams['SqlCredential'] = $SqlCredential }
                        $conn = Connect-DbaInstance @cparams
                        Invoke-DbaQuery -SqlInstance $conn -Query 'SELECT c = 1' -QueryTimeout $QueryTimeoutSeconds -ErrorAction Stop
                    }
                    $ms = [int]((Get-Date) - $t0).TotalMilliseconds
                    & $emitRow @{ Level = 'Connection'; Replica = $srv; Node = ($srv -split '\\')[0]; PortOpen = $(if ($ms -gt 3000) { 'WARN' } else { 'OK' }); Detail = "connect + SELECT 1 in ${ms}ms" }
                }
                catch {
                    & $emitRow @{ Level = 'Connection'; Replica = $srv; Node = ($srv -split '\\')[0]; PortOpen = 'FAILED'; Detail = "connect failed after $RetryCount attempt(s): $($_.Exception.Message)"; Remediation = "Test-DbaConnection -SqlInstance '$srv'; check the SQL service, TCP port and firewall." }
                }
            }
        }

        # --- Database state (-CheckDatabaseState) -------------------------
        if ($CheckDatabaseState) {
            Write-Verbose 'Checking database states (-CheckDatabaseState)...'
            $dbStateBad = $false
            try {
                foreach ($d in @(& $invokeWithRetry -Operation "read database states from [$($hadr.ServerName)]" -ScriptBlock { Invoke-DbaQuery @queryParams -Query "SELECT name, state_desc, is_read_only FROM sys.databases ORDER BY name" })) {
                    $st = [string]$d.state_desc
                    $status = switch -Regex ($st) { 'SUSPECT|RECOVERY_PENDING|EMERGENCY' { 'FAILED' } 'OFFLINE' { 'WARN' } default { 'OK' } }
                    if ($status -ne 'OK') {
                        $dbStateBad = $true
                        $rem = if ($st -eq 'OFFLINE') { "ALTER DATABASE [$($d.name)] SET ONLINE;" } else { "-- [$($d.name)] is $($st): check the SQL error log / storage; restore from a known-good backup if corrupt." }
                        & $emitRow @{ Level = 'DatabaseState'; DatabaseName = [string]$d.name; PortOpen = $status; Detail = "state=$st; read_only=$($d.is_read_only)"; Remediation = $rem }
                    }
                }
                if (-not $dbStateBad) { & $emitRow @{ Level = 'DatabaseState'; PortOpen = 'OK'; Detail = 'All databases are ONLINE.' } }
            }
            catch { & $emitRow @{ Level = 'DatabaseState'; PortOpen = 'ERROR'; Detail = "Could not read database states: $($_.Exception.Message)" } }
        }

        # --- System database health (-CheckSystemDatabase) ---------------
        if ($CheckSystemDatabase) {
            Write-Verbose 'Checking system databases (-CheckSystemDatabase)...'
            try {
                foreach ($d in @(& $invokeWithRetry -Operation "read system database states from [$($hadr.ServerName)]" -ScriptBlock { Invoke-DbaQuery @queryParams -Query "SELECT name, state_desc FROM sys.databases WHERE database_id IN (1,2,3,4) ORDER BY database_id" })) {
                    $ok = ([string]$d.state_desc -eq 'ONLINE')
                    & $emitRow @{
                        Level = 'SystemDatabase'; DatabaseName = [string]$d.name; PortOpen = $(if ($ok) { 'OK' } else { 'FAILED' }); Detail = "state=$($d.state_desc)"
                        Remediation = $(if (-not $ok) { "System database [$($d.name)] is $($d.state_desc) - the instance is impaired. Check the SQL error log immediately." } else { '' })
                    }
                }
            }
            catch { & $emitRow @{ Level = 'SystemDatabase'; PortOpen = 'ERROR'; Detail = "Could not read system databases: $($_.Exception.Message)" } }
        }

        # --- Corruption / I/O errors, no CHECKDB (-CheckIoError) ---------
        if ($CheckIoError) {
            Write-Verbose 'Checking for suspect pages / I-O errors (-CheckIoError)...'
            $ioFound = $false
            try {
                $suspectPagesQuery = @"
SELECT DatabaseName = DB_NAME(database_id), file_id, page_id, event_type, error_count, last_update_date
FROM msdb.dbo.suspect_pages;
"@
                foreach ($p in @(& $invokeWithRetry -Operation "read suspect pages from [$($hadr.ServerName)]" -ScriptBlock { Invoke-DbaQuery @queryParams -Query $suspectPagesQuery })) {
                    $ioFound = $true
                    & $emitRow @{
                        Level = 'IoError'; DatabaseName = [string]$p.DatabaseName; PortOpen = 'FAILED'
                        Detail = "suspect_pages: page $($p.page_id) file $($p.file_id) event=$($p.event_type) count=$($p.error_count) at $($p.last_update_date)"
                        Remediation = 'Corruption / I/O error recorded. Check the storage subsystem and SQL error log; restore the affected pages / database from a known-good backup.'
                    }
                }
            }
            catch { & $emitRow @{ Level = 'IoError'; PortOpen = 'CHECK'; Detail = "Could not read msdb.dbo.suspect_pages: $($_.Exception.Message)" } }
            try {
                $ioErrorLogQuery = @"
CREATE TABLE #e (LogDate datetime, ProcessInfo nvarchar(100), Text nvarchar(max));
INSERT #e EXEC master.dbo.xp_readerrorlog 0, 1, N'Error: 82';
SELECT Cnt = COUNT(*), LastAt = MAX(LogDate)
FROM #e
WHERE Text LIKE '%Error: 823%' OR Text LIKE '%Error: 824%' OR Text LIKE '%Error: 825%';
DROP TABLE #e;
"@
                $io = @(& $invokeWithRetry -Operation "read I/O errors from [$($hadr.ServerName)]" -ScriptBlock { Invoke-DbaQuery @queryParams -Query $ioErrorLogQuery })
                if ($io -and [int]$io[0].Cnt -gt 0) {
                    $ioFound = $true
                    & $emitRow @{ Level = 'IoError'; PortOpen = 'FAILED'; Detail = "$($io[0].Cnt) hard I/O error(s) (823/824/825) in the current error log; last $($io[0].LastAt)"; Remediation = '823/824/825 = torn / checksum / unreadable pages. Investigate the disk subsystem immediately.' }
                }
            }
            catch {}
            if (-not $ioFound) { & $emitRow @{ Level = 'IoError'; PortOpen = 'OK'; Detail = 'No suspect pages and no 823/824/825 errors in the current log.' } }
        }

        # --- Clock skew between replicas (-CheckClockSkew) ---------------
        if ($CheckClockSkew) {
            Write-Verbose 'Measuring clock skew (-CheckClockSkew)...'
            $skewWarn = 5; $skewFail = 60
            foreach ($srv in $instanceTargets) {
                try {
                    $tp = @{ SqlInstance = $srv; QueryTimeout = $QueryTimeoutSeconds; ErrorAction = 'Stop' }
                    if ($SqlCredential) { $tp['SqlCredential'] = $SqlCredential }
                    $b = [DateTime]::UtcNow
                    $row = & $invokeWithRetry -Operation "read clock from [$srv]" -ScriptBlock { Invoke-DbaQuery @tp -Query 'SELECT utc = SYSUTCDATETIME()' }
                    $a = [DateTime]::UtcNow
                    $mid = $b.AddTicks([long](($a - $b).Ticks / 2))
                    $skew = [math]::Round([math]::Abs(([datetime]$row.utc - $mid).TotalSeconds), 1)
                    $status = if ($skew -ge $skewFail) { 'FAILED' } elseif ($skew -ge $skewWarn) { 'WARN' } else { 'OK' }
                    & $emitRow @{
                        Level = 'ClockSkew'; Replica = $srv; Node = ($srv -split '\\')[0]; PortOpen = $status; Detail = "clock ~${skew}s vs this host (round-trip adjusted)"
                        Remediation = $(if ($status -ne 'OK') { 'Run w32tm /resync; ensure all replicas and DCs share one time source (large skew breaks Kerberos and skews RPO calculations).' } else { '' })
                    }
                }
                catch { & $emitRow @{ Level = 'ClockSkew'; Replica = $srv; Node = ($srv -split '\\')[0]; PortOpen = 'CHECK'; Detail = "could not read clock: $($_.Exception.Message)" } }
            }
        }

        # --- Certificate expiry (-CheckCertificate) ----------------------
        # Covers SQL certificates (endpoint auth / TDE / user certs) and the
        # channel TLS certificate that encrypts client connections
        # (SuperSocketNetLib, read per node). Built-in ##MS_* system signing
        # certificates are excluded (SQL self-manages them). Every certificate
        # is listed with its expiry date; < 60 days = WARN, < 30 days = FAILED.
        if ($CheckCertificate) {
            Write-Verbose 'Checking certificate expiry (-CheckCertificate)...'
            $certBefore = $report.Count

            # 1) SQL certificates (skip the built-in ##MS_* system certs).
            try {
                $certificateQuery = @"
SELECT name, expiry_date, days_left = DATEDIFF(day, SYSUTCDATETIME(), expiry_date)
FROM sys.certificates
WHERE expiry_date IS NOT NULL AND name NOT LIKE '##%'
ORDER BY expiry_date;
"@
                foreach ($c in @(& $invokeWithRetry -Operation "read SQL certificates from [$($hadr.ServerName)]" -ScriptBlock { Invoke-DbaQuery @queryParams -Query $certificateQuery })) {
                    $days = [int]$c.days_left
                    $status = if ($days -lt 30) { 'FAILED' } elseif ($days -lt 60) { 'WARN' } else { 'OK' }
                    & $emitRow @{ Level = 'Certificate'; EndpointName = [string]$c.name; PortOpen = $status; Detail = "SQL cert [$($c.name)] expires $($c.expiry_date) (${days} days)"; Remediation = $(if ($status -ne 'OK') { "Renew / rotate certificate [$($c.name)] before it expires (endpoint / TDE / connection encryption fails on expiry)." } else { '' }) }
                }
            }
            catch { & $emitRow @{ Level = 'Certificate'; PortOpen = 'ERROR'; Detail = "Could not read sys.certificates: $($_.Exception.Message)" } }

            # 2) Channel TLS certificate (SuperSocketNetLib) per node - best-effort WinRM.
            foreach ($node in $nodeTargets) {
                try {
                    $ic = @{
                        ComputerName = $node; ErrorAction = 'Stop'
                        ScriptBlock  = {
                            $out = @()
                            Get-ChildItem 'HKLM:\SOFTWARE\Microsoft\Microsoft SQL Server' -ErrorAction SilentlyContinue |
                                Where-Object { $_.PSChildName -like 'MSSQL*.*' } | ForEach-Object {
                                    $thumb = (Get-ItemProperty (Join-Path $_.PSPath 'MSSQLServer\SuperSocketNetLib') -Name Certificate -ErrorAction SilentlyContinue).Certificate
                                    if ($thumb) {
                                        $cert = Get-ChildItem Cert:\LocalMachine\My -ErrorAction SilentlyContinue | Where-Object { $_.Thumbprint -ieq ($thumb -replace '\s', '') }
                                        if ($cert) { $out += [pscustomobject]@{ Subject = $cert.Subject; NotAfter = $cert.NotAfter } }
                                    }
                                }
                            $out
                        }
                    }
                    if ($WmiCredential) { $ic['Credential'] = $WmiCredential }
                    foreach ($t in @(Invoke-Command @ic)) {
                        $days = [int]([datetime]$t.NotAfter - (Get-Date)).TotalDays
                        $status = if ($days -lt 30) { 'FAILED' } elseif ($days -lt 60) { 'WARN' } else { 'OK' }
                        & $emitRow @{ Level = 'Certificate'; Node = $node; EndpointName = [string]$t.Subject; PortOpen = $status; Detail = "TLS cert expires $($t.NotAfter) (${days} days)"; Remediation = $(if ($status -ne 'OK') { "Renew the SQL TLS certificate on $node before expiry (encrypted client connections will fail)." } else { '' }) }
                    }
                }
                catch { & $emitRow @{ Level = 'Certificate'; Node = $node; PortOpen = 'CHECK'; Detail = "TLS cert read failed on ${node}: $($_.Exception.Message)" } }
            }

            if ($report.Count -eq $certBefore) { & $emitRow @{ Level = 'Certificate'; PortOpen = 'OK'; Detail = 'No user SQL certificates and no TLS channel certificate found (built-in ##MS_* system certs are excluded).' } }
        }

        # --- Pending Windows reboot (-CheckPendingReboot) ----------------
        if ($CheckPendingReboot) {
            Write-Verbose 'Checking for pending Windows reboot (-CheckPendingReboot)...'
            foreach ($node in $nodeTargets) {
                try {
                    $ic = @{
                        ComputerName = $node; ErrorAction = 'Stop'
                        ScriptBlock  = {
                            $reasons = New-Object System.Collections.Generic.List[string]

                            if (Test-Path 'HKLM:\SOFTWARE\Microsoft\Windows\CurrentVersion\Component Based Servicing\RebootPending') {
                                $reasons.Add('Component Based Servicing (RebootPending key) - HKLM:\SOFTWARE\Microsoft\Windows\CurrentVersion\Component Based Servicing\RebootPending')
                            }
                            if (Test-Path 'HKLM:\SOFTWARE\Microsoft\Windows\CurrentVersion\Component Based Servicing\RebootInProgress') {
                                $reasons.Add('Component Based Servicing (RebootInProgress key)')
                            }
                            if (Test-Path 'HKLM:\SOFTWARE\Microsoft\Windows\CurrentVersion\Component Based Servicing\PackagesPending') {
                                $reasons.Add('Component Based Servicing (PackagesPending key)')
                            }
                            if (Test-Path 'HKLM:\SOFTWARE\Microsoft\Windows\CurrentVersion\WindowsUpdate\Auto Update\RebootRequired') {
                                $reasons.Add('Windows Update (RebootRequired key) - HKLM:\SOFTWARE\Microsoft\Windows\CurrentVersion\WindowsUpdate\Auto Update\RebootRequired')
                            }

                            $pfr = Get-ItemProperty 'HKLM:\SYSTEM\CurrentControlSet\Control\Session Manager' -Name PendingFileRenameOperations -ErrorAction SilentlyContinue
                            if ($null -ne $pfr -and $pfr.PendingFileRenameOperations) {
                                $pfrCount = @($pfr.PendingFileRenameOperations | Where-Object { $_ }).Count
                                $reasons.Add("Pending file rename operations ($pfrCount queued) - HKLM:\SYSTEM\CurrentControlSet\Control\Session Manager\PendingFileRenameOperations")
                            }

                            $activeName = (Get-ItemProperty 'HKLM:\SYSTEM\CurrentControlSet\Control\ComputerName\ActiveComputerName' -Name ComputerName -ErrorAction SilentlyContinue).ComputerName
                            $configuredName = (Get-ItemProperty 'HKLM:\SYSTEM\CurrentControlSet\Control\ComputerName\ComputerName' -Name ComputerName -ErrorAction SilentlyContinue).ComputerName
                            if ($activeName -and $configuredName -and $activeName -ne $configuredName) {
                                $reasons.Add("Pending computer rename ('$activeName' -> '$configuredName')")
                            }

                            try {
                                $ccm = Invoke-WmiMethod -Namespace 'ROOT\ccm\ClientSDK' -Class 'CCM_ClientUtilities' -Name 'DetermineIfRebootPending' -ErrorAction Stop
                                if ($ccm -and ($ccm.RebootPending -or $ccm.IsHardRebootPending)) {
                                    $reasons.Add('ConfigMgr / SCCM client reports a reboot pending (ROOT\ccm\ClientSDK CCM_ClientUtilities)')
                                }
                            }
                            catch { }

                            [pscustomobject]@{
                                Pending  = ($reasons.Count -gt 0)
                                Reasons  = @($reasons)
                                Computer = $env:COMPUTERNAME
                            }
                        }
                    }
                    if ($WmiCredential) { $ic['Credential'] = $WmiCredential }
                    $rebootInfo = Invoke-Command @ic
                    $pending = [bool]$rebootInfo.Pending
                    $reasons = @($rebootInfo.Reasons)
                    $sourceComputer = [string]$rebootInfo.Computer

                    if ($pending) {
                        $detail = "Windows reboot pending on [$sourceComputer]. Reason(s): " +
                            ($reasons -join '  ||  ') +
                            '. Read from the remote registry / WMI via WinRM (Invoke-Command).'
                    }
                    else {
                        $detail = "No reboot pending on [$sourceComputer]. Checked CBS (RebootPending/RebootInProgress/PackagesPending), " +
                            'Windows Update (RebootRequired), Session Manager PendingFileRenameOperations, pending computer rename and the ConfigMgr client, ' +
                            'via the remote registry / WMI over WinRM.'
                    }

                    & $emitRow @{
                        Level = 'PendingReboot'; Node = $node; PortOpen = $(if ($pending) { 'WARN' } else { 'OK' }); Detail = $detail
                        Remediation = $(if ($pending) { 'Schedule a reboot (drain / fail the replica over first).' } else { '' })
                    }
                }
                catch { & $emitRow @{ Level = 'PendingReboot'; Node = $node; PortOpen = 'CHECK'; Detail = "WinRM read failed: $($_.Exception.Message)" } }
            }
        }

        # --- Failed logins (-CheckLoginFailed) ---------------------------
        if ($CheckLoginFailed) {
            Write-Verbose ("Scanning error log for failed logins over the last {0}h (-CheckLoginFailed)..." -f $LoginFailedHoursBack)
            try {
                $loginFailedQuery = @"
DECLARE @start datetime = DATEADD(HOUR, -CAST(@h AS int), SYSDATETIME());
CREATE TABLE #e (LogDate datetime, ProcessInfo nvarchar(100), Text nvarchar(max));
INSERT #e EXEC master.dbo.xp_readerrorlog 0, 1, N'Login failed', NULL, @start, NULL, N'desc';
;WITH named AS (
    SELECT
        LogDate,
        LoginName = CASE
            WHEN CHARINDEX(CHAR(39), Text) > 0
             AND CHARINDEX(CHAR(39), Text, CHARINDEX(CHAR(39), Text) + 1) > CHARINDEX(CHAR(39), Text)
            THEN SUBSTRING(Text, CHARINDEX(CHAR(39), Text) + 1,
                     CHARINDEX(CHAR(39), Text, CHARINDEX(CHAR(39), Text) + 1) - CHARINDEX(CHAR(39), Text) - 1)
            ELSE N'(unknown)'
        END,
        ClientIp = CASE
            WHEN CHARINDEX('[CLIENT:', Text) > 0
             AND CHARINDEX(']', Text, CHARINDEX('[CLIENT:', Text)) > CHARINDEX('[CLIENT:', Text)
            THEN LTRIM(RTRIM(SUBSTRING(Text,
                     CHARINDEX('[CLIENT:', Text) + 8,
                     CHARINDEX(']', Text, CHARINDEX('[CLIENT:', Text)) - (CHARINDEX('[CLIENT:', Text) + 8))))
            ELSE NULL
        END
    FROM #e
)
SELECT LoginName, ClientIp, Cnt = COUNT(*), LastAt = MAX(LogDate)
FROM named
GROUP BY LoginName, ClientIp
ORDER BY Cnt DESC, LoginName, ClientIp;
DROP TABLE #e;
"@
                $lf = @(& $invokeWithRetry -Operation "read failed logins from [$($hadr.ServerName)]" -ScriptBlock { Invoke-DbaQuery @queryParams -SqlParameter @{ h = $LoginFailedHoursBack } -Query $loginFailedQuery })
                $rows  = @($lf | Where-Object { $_ -and $null -ne $_.LoginName })

                # Client IPs to exclude from the WARN / problem decision (e.g. scanners).
                $ignoreIpSet = @{}
                foreach ($ip in @($LoginFailedIgnoreIp | Where-Object { $_ })) {
                    $key = ([string]$ip).Trim().ToLowerInvariant()
                    if ($key) { $ignoreIpSet[$key] = $true }
                }
                $isIgnoredIp = {
                    param([string] $ip)
                    if ([string]::IsNullOrWhiteSpace($ip)) { return $false }
                    return $ignoreIpSet.ContainsKey($ip.Trim().ToLowerInvariant())
                }

                $realTotal = 0; $ignoredTotal = 0; $realLogins = @{}
                foreach ($rr in $rows) {
                    $cnt = [int]$rr.Cnt
                    if (& $isIgnoredIp ([string]$rr.ClientIp)) { $ignoredTotal += $cnt }
                    else { $realTotal += $cnt; $realLogins[[string]$rr.LoginName] = $true }
                }

                if ($rows.Count -eq 0) {
                    & $emitRow @{ Level = 'LoginFailed'; PortOpen = 'OK'; Detail = "No failed logins in last ${LoginFailedHoursBack}h." }
                }
                else {
                    if ($realTotal -gt 0) {
                        $ignoredNote = if ($ignoredTotal -gt 0) { " ($ignoredTotal attempt(s) from ignored IP(s) excluded)" } else { '' }
                        & $emitRow @{ Level = 'LoginFailed'; PortOpen = 'WARN'; Detail = "$realTotal failed login attempt(s) from $($realLogins.Count) distinct login(s) in last ${LoginFailedHoursBack}h$ignoredNote."; Remediation = 'Review error 18456 state codes (bad password / disabled / no DB access). Bursts can indicate an app misconfig or a brute-force attempt.' }
                    }
                    else {
                        & $emitRow @{ Level = 'LoginFailed'; PortOpen = 'OK'; Detail = "No unexpected failed logins in last ${LoginFailedHoursBack}h ($ignoredTotal attempt(s) from ignored IP(s))." }
                    }

                    foreach ($rr in $rows) {
                        $ip = [string]$rr.ClientIp
                        if ([string]::IsNullOrWhiteSpace($ip)) { $ip = 'unknown' }
                        $ignoredTag = if (& $isIgnoredIp $ip) { ' (ignored IP)' } else { '' }
                        & $emitRow @{ Level = 'LoginFailed'; EndpointName = [string]$rr.LoginName; PortOpen = 'INFO'; Detail = "login [$([string]$rr.LoginName)]: $([int]$rr.Cnt) failed attempt(s) from IP [$ip]; last $($rr.LastAt)$ignoredTag" }
                    }
                }
            }
            catch { & $emitRow @{ Level = 'LoginFailed'; PortOpen = 'CHECK'; Detail = "Could not read the error log (needs securityadmin): $($_.Exception.Message)" } }
        }
    }

    # --- Short verbose summary --------------------------------------------
    $agNames       = @($report | Select-Object -ExpandProperty AvailabilityGroup -Unique)
    $unhealthyRepl = @($replicaRows  | Where-Object { $_.SyncHealth -and $_.SyncHealth -ne 'HEALTHY' })
    $unhealthyDb   = @($databaseRows | Where-Object { $_.SyncHealth -and $_.SyncHealth -ne 'HEALTHY' })

    Write-Verbose ("Availability group(s): {0}" -f ($agNames -join ', '))
    Write-Verbose ("Replicas: {0} (unhealthy: {1})"  -f $replicaRows.Count,  $unhealthyRepl.Count)
    if (-not $ReplicaHealthOnly) {
        Write-Verbose ("Databases: {0} (unhealthy: {1})" -f $databaseRows.Count, $unhealthyDb.Count)
    }

    # Ensure every row exposes the Remediation column (the original sections
    # do not set it) so callers can rely on it being present.
    foreach ($__row in $report) {
        if (-not $__row.PSObject.Properties['Remediation']) {
            $__row | Add-Member -NotePropertyName Remediation -NotePropertyValue '' -Force
        }
    }

    return $report
}
