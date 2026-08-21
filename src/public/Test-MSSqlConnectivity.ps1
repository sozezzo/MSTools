function Test-MSSqlConnectivity {
<#
.SYNOPSIS
    Verifies everything required to connect to a SQL Server instance from another
    server and returns fix suggestions plus ready-to-use connection strings. When
    run with no target, it discovers the local SQL instances from the registry and
    checks each one.

.DESCRIPTION
    A one-stop pre-flight check for "can another server reach and use this SQL
    Server?". Run it LOCALLY on the SQL host or REMOTELY from the client server.

    With NO -SqlInstance it auto-discovers every SQL Server instance installed on
    THIS machine (from HKLM\...\Microsoft SQL Server\Instance Names\SQL), reads the
    registry configuration (edition, version, enabled protocols, configured TCP
    port - even when the SQL service is stopped) and checks each instance.

    For every instance it checks, and for every problem it returns a concrete fix:

      - Session elevation (is this PowerShell running as Administrator).
      - Whether the current user is a local Administrator on the target host.
      - The SQL Server Windows service state and start mode.
      - The SQL Server Browser service (needed to connect by instance name over
        UDP 1434 when a named instance uses a dynamic port).
      - Network protocols enabled/disabled (TCP/IP, Named Pipes, Shared Memory),
        read from sys.dm_server_registry and, as a fallback, the Windows registry.
      - The configured TCP port (static vs dynamic) and the port actually in use
        / listening (sys.dm_tcp_listener_states + sys.dm_exec_connections + the
        registry).
      - Windows Firewall: whether the profiles are on and whether an inbound rule
        allows the SQL TCP port (and UDP 1434 for the Browser).
      - A raw TCP connect test from THIS host to the SQL port.
      - A real SQL logon, the login used, and whether it is sysadmin
        (IS_SRVROLEMEMBER('sysadmin')).

    OS-level checks (service, firewall, local admin, registry) run directly when
    the target is local, or over WinRM (Invoke-Command) when remote; they are
    best-effort and degrade gracefully. The protocol/port/sysadmin facts also come
    from SQL DMVs, so they work remotely without WinRM as long as a logon succeeds.

    The function is READ-ONLY: it changes nothing, it only reports and suggests.

.PARAMETER SqlInstance
    The SQL Server to test. Accepts HOST, HOST\INSTANCE, HOST,PORT or
    HOST\INSTANCE,PORT (an optional tcp: / np: / lpc: prefix is ignored). When
    OMITTED, the function discovers and checks every local instance instead.

.PARAMETER SqlCredential
    SQL login used for the logon test. When omitted, the current Windows identity
    (Integrated Security) is used - which is what "the current user must be
    sysadmin" refers to.

.PARAMETER Credential
    Windows credential used for the REMOTE OS-level checks (WinRM / Invoke-Command)
    when the target is not the local machine. When omitted, the current Windows
    identity is used for remoting.

.PARAMETER ConnectTimeoutSeconds
    Timeout (seconds) for the SQL logon and the raw TCP connect test. Default 5.

.OUTPUTS
    One [pscustomobject] summary PER instance checked (an array when several local
    instances are discovered). Each summary has the parsed target, the resolved
    port, edition/version, the pass/fail of each area, the ready-to-use connection
    strings (ConnectByInstanceName and ConnectByHostPort), a .Checks array (one row
    per check: Category, Check, Target, Status, Detail, FixSuggestion) and a
    .FixSuggestions array where every fix is its own row with a Description and the
    FixSuggestion to run.

.EXAMPLE
    Test-MSSqlConnectivity

    No target: discovers every local SQL instance from the registry and checks each
    one, printing a colour report, the per-row fix suggestions and the connection
    strings.

.EXAMPLE
    Test-MSSqlConnectivity -SqlInstance 'SQLPROD01\SALES'

    Runs every check against the named instance SALES and returns the summary
    object with the connection strings and per-row fixes.

.EXAMPLE
    $r = Test-MSSqlConnectivity -SqlInstance 'SQLPROD01,1435'
    $r.FixSuggestions | Format-Table Check, Description, FixSuggestion -Wrap

    Tests using an explicit port and lists each fix suggestion with its description.

.NOTES
    Requires .NET System.Data.SqlClient (built into Windows PowerShell 5.1). The
    OS-level checks use the NetSecurity firewall cmdlets and, when remote, WinRM.
    Run elevated ("Run as administrator") so the local OS/registry checks can read
    the firewall, services and group membership.
#>
    [CmdletBinding()]
    param(
        [string] $SqlInstance,

        [System.Management.Automation.PSCredential] $SqlCredential,

        [System.Management.Automation.PSCredential] $Credential,

        [ValidateRange(1, 120)]
        [int] $ConnectTimeoutSeconds = 5
    )

    # --- Status colours + row emitter (uses $report from the caller scope) -
    $statusColor = @{
        OK = 'Green'; WARN = 'Yellow'; FAILED = 'Red'; INFO = 'Gray'; SKIPPED = 'DarkGray'
    }
    $emitRow = {
        param([string] $Category, [string] $Check, [string] $Target, [string] $Status, [string] $Detail, [string] $Fix = '')
        $row = [pscustomobject]@{
            Category      = $Category
            Check         = $Check
            Target        = $Target
            Status        = $Status
            Detail        = $Detail
            FixSuggestion = $Fix
        }
        $report.Add($row)
        $c = if ($statusColor.ContainsKey($Status)) { $statusColor[$Status] } else { 'White' }
        Write-Host ("  [{0,-7}] {1,-22} {2}" -f $Status, $Check, $Detail) -ForegroundColor $c
    }

    # --- Session identity / elevation (per session, not per instance) -----
    $curId = [Security.Principal.WindowsIdentity]::GetCurrent()
    $curUser = $curId.Name
    $isElevated = $false
    try {
        $isElevated = (New-Object Security.Principal.WindowsPrincipal $curId).IsInRole([Security.Principal.WindowsBuiltinRole]::Administrator)
    }
    catch {}

    # --- Small helpers ----------------------------------------------------
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

    $buildConnStr = {
        param([string] $DataSource)
        $b = New-Object System.Data.SqlClient.SqlConnectionStringBuilder
        $b['Data Source']            = $DataSource
        $b['Initial Catalog']        = 'master'
        $b['Connect Timeout']        = $ConnectTimeoutSeconds
        $b['Application Name']        = 'MSTools Test-MSSqlConnectivity'
        $b['TrustServerCertificate'] = $true
        if ($SqlCredential) {
            $b['User ID']  = $SqlCredential.UserName
            $b['Password'] = $SqlCredential.GetNetworkCredential().Password
        }
        else {
            $b['Integrated Security'] = $true
        }
        return $b.ConnectionString
    }

    $sqlQuery = {
        param($Conn, [string] $Query)
        $cmd = $Conn.CreateCommand()
        $cmd.CommandText    = $Query
        $cmd.CommandTimeout = $ConnectTimeoutSeconds
        $adapter = New-Object System.Data.SqlClient.SqlDataAdapter $cmd
        $ds = New-Object System.Data.DataSet
        [void]$adapter.Fill($ds)
        if ($ds.Tables.Count -gt 0) { return $ds.Tables[0] } else { return $null }
    }

    # OS + registry probe. Runs on the SQL host (locally or via WinRM). Reads the
    # service state, firewall, local admins, listening ports AND the Windows
    # registry configuration (instance id, edition/version, enabled protocols and
    # the configured TCP port - available even when the SQL service is stopped).
    $osProbe = {
        param([string] $InstanceName)

        $svcName = if ($InstanceName -and $InstanceName -ne 'MSSQLSERVER') { 'MSSQL$' + $InstanceName } else { 'MSSQLSERVER' }
        $svc = Get-Service -Name $svcName -ErrorAction SilentlyContinue
        $br  = Get-Service -Name 'SQLBrowser' -ErrorAction SilentlyContinue

        $out = [ordered]@{
            ComputerName        = $env:COMPUTERNAME
            SqlServiceName      = $svcName
            SqlServiceStatus    = if ($svc) { "$($svc.Status)" } else { 'NotFound' }
            SqlServiceStart     = if ($svc) { "$($svc.StartType)" } else { '' }
            BrowserStatus       = if ($br)  { "$($br.Status)" }  else { 'NotFound' }
            BrowserStart        = if ($br)  { "$($br.StartType)" } else { '' }
            FirewallOnProfiles  = @()
            FirewallError       = ''
            AllowedTcpPorts     = @()
            UdpBrowserRuleFound = $false
            LocalAdmins         = @()
            LocalAdminsError    = ''
            ListeningPorts      = @()
            ListeningError      = ''
            InstanceId          = ''
            Edition             = ''
            Version             = ''
            PatchLevel          = ''
            RegTcpEnabled       = $null
            RegNpEnabled        = $null
            RegSmEnabled        = $null
            RegStaticPort       = ''
            RegDynamicPort      = ''
            RegError            = ''
        }

        try {
            $profiles = Get-NetFirewallProfile -ErrorAction Stop
            $out.FirewallOnProfiles = @($profiles | Where-Object { $_.Enabled } | ForEach-Object { "$($_.Name)" })
            $allowed = New-Object System.Collections.Generic.List[string]
            Get-NetFirewallRule -Direction Inbound -Action Allow -Enabled True -ErrorAction Stop | ForEach-Object {
                $pf = $_ | Get-NetFirewallPortFilter -ErrorAction SilentlyContinue
                if ($pf) {
                    if ($pf.Protocol -eq 'TCP') { foreach ($x in @($pf.LocalPort)) { [void]$allowed.Add("$x") } }
                    if ($pf.Protocol -eq 'UDP' -and (@($pf.LocalPort) -contains '1434' -or @($pf.LocalPort) -contains 'Any')) { $out.UdpBrowserRuleFound = $true }
                }
            }
            $out.AllowedTcpPorts = $allowed.ToArray()
        }
        catch { $out.FirewallError = $_.Exception.Message }

        try {
            $out.LocalAdmins = @(Get-LocalGroupMember -Group 'Administrators' -ErrorAction Stop | ForEach-Object { "$($_.Name)" })
        }
        catch { $out.LocalAdminsError = $_.Exception.Message }

        try {
            $listen = Get-NetTCPConnection -State Listen -ErrorAction Stop
            $sqlPids = @(Get-Process -Name 'sqlservr' -ErrorAction SilentlyContinue | ForEach-Object { $_.Id })
            $out.ListeningPorts = @(
                $listen | Where-Object { $sqlPids -contains $_.OwningProcess } |
                    ForEach-Object { [int]$_.LocalPort } | Sort-Object -Unique
            )
        }
        catch { $out.ListeningError = $_.Exception.Message }

        # Windows registry: instance id, setup/config, protocols and ports.
        try {
            $rootKey = 'HKLM:\SOFTWARE\Microsoft\Microsoft SQL Server'
            $names   = Get-ItemProperty -Path "$rootKey\Instance Names\SQL" -ErrorAction Stop
            $instId  = $names.$InstanceName
            if ($instId) {
                $out.InstanceId = "$instId"
                $setup = Get-ItemProperty -Path "$rootKey\$instId\Setup" -ErrorAction SilentlyContinue
                if ($setup) {
                    $out.Edition    = "$($setup.Edition)"
                    $out.Version    = "$($setup.Version)"
                    $out.PatchLevel = "$($setup.PatchLevel)"
                }
                $ssnl = "$rootKey\$instId\MSSQLServer\SuperSocketNetLib"
                $tcp = Get-ItemProperty -Path "$ssnl\Tcp" -ErrorAction SilentlyContinue
                $np  = Get-ItemProperty -Path "$ssnl\Np"  -ErrorAction SilentlyContinue
                $sm  = Get-ItemProperty -Path "$ssnl\Sm"  -ErrorAction SilentlyContinue
                if ($tcp -and $null -ne $tcp.Enabled) { $out.RegTcpEnabled = ([int]$tcp.Enabled -eq 1) }
                if ($np  -and $null -ne $np.Enabled)  { $out.RegNpEnabled  = ([int]$np.Enabled  -eq 1) }
                if ($sm  -and $null -ne $sm.Enabled)  { $out.RegSmEnabled  = ([int]$sm.Enabled  -eq 1) }
                $ipall = Get-ItemProperty -Path "$ssnl\Tcp\IPAll" -ErrorAction SilentlyContinue
                if ($ipall) {
                    $out.RegStaticPort  = "$($ipall.TcpPort)"
                    $out.RegDynamicPort = "$($ipall.TcpDynamicPorts)"
                }
            }
            else {
                $out.RegError = "Instance '$InstanceName' was not found under Instance Names\SQL."
            }
        }
        catch { $out.RegError = $_.Exception.Message }

        [pscustomobject]$out
    }

    # Enumerate the SQL instances installed locally (from the registry).
    $discoverLocalInstances = {
        $rootKey = 'HKLM:\SOFTWARE\Microsoft\Microsoft SQL Server'
        $result = @()
        try {
            $names = Get-ItemProperty -Path "$rootKey\Instance Names\SQL" -ErrorAction Stop
            foreach ($prop in $names.PSObject.Properties) {
                if ($prop.Name -like 'PS*') { continue }
                $result += [pscustomobject]@{ InstanceName = $prop.Name; InstanceId = "$($prop.Value)" }
            }
        }
        catch {}
        return $result
    }

    # =====================================================================
    # Per-instance worker: runs every check for one target and returns the
    # summary object.
    # =====================================================================
    $runOne = {
        param([string] $SqlInstance)

        $report = New-Object System.Collections.Generic.List[object]

        # --- Parse HOST\INSTANCE,PORT ------------------------------------
        $raw = ($SqlInstance.Trim() -replace '^(?i)(tcp|np|lpc):', '')
        $portPart = $null
        if ($raw -match ',') {
            $p = $raw -split ',', 2
            $raw = $p[0].Trim()
            $portPart = $p[1].Trim()
        }
        $hostPart = $raw
        $instPart = $null
        if ($raw -match '\\') {
            $p = $raw -split '\\', 2
            $hostPart = $p[0].Trim()
            $instPart = $p[1].Trim()
        }
        $targetHost   = if ($hostPart) { $hostPart } else { $env:COMPUTERNAME }
        $instanceName = if ($instPart) { $instPart } else { 'MSSQLSERVER' }
        $isDefault    = ($instanceName -eq 'MSSQLSERVER')
        $explicitPort = $null
        if ($portPart -and $portPart -match '^\d+$') { $explicitPort = [int]$portPart }

        # --- Local or remote target? -------------------------------------
        $localAliases = @('.', 'localhost', '127.0.0.1', '::1', $env:COMPUTERNAME, [System.Net.Dns]::GetHostName())
        try { $localAliases += ([System.Net.Dns]::GetHostEntry($env:COMPUTERNAME)).HostName } catch {}
        $isLocal = $false
        foreach ($a in $localAliases) { if ($a -and $targetHost -ieq $a) { $isLocal = $true; break } }

        Write-Host ""
        Write-Host ("=== Target [{0}] instance [{1}] ({2}) ===" -f $targetHost, $instanceName, $(if ($isLocal) { 'local' } else { 'remote' })) -ForegroundColor Cyan

        # --- 1) Running as Administrator? --------------------------------
        if ($isElevated) {
            & $emitRow 'Session' 'Run as administrator' $curUser 'OK' 'This PowerShell session is elevated.'
        }
        else {
            & $emitRow 'Session' 'Run as administrator' $curUser 'WARN' 'This session is NOT elevated; local OS/registry checks may be incomplete.' 'Re-launch PowerShell with "Run as administrator" and run this function again.'
        }

        # --- 2) OS + registry probe --------------------------------------
        $osInfo = $null
        $osError = $null
        try {
            if ($isLocal) {
                $osInfo = & $osProbe $instanceName
            }
            else {
                $icArgs = @{ ComputerName = $targetHost; ScriptBlock = $osProbe; ArgumentList = @($instanceName); ErrorAction = 'Stop' }
                if ($Credential) { $icArgs['Credential'] = $Credential }
                $osInfo = Invoke-Command @icArgs
            }
        }
        catch {
            $osError = $_.Exception.Message
        }

        if ($osError) {
            & $emitRow 'OS access' 'Remote OS checks' $targetHost 'WARN' "Could not run OS-level checks: $osError" "Run this function locally and elevated on [$targetHost], or enable WinRM there: Enable-PSRemoting -Force. Protocol/port/sysadmin facts below still come from SQL."
        }

        # Registry configuration snapshot.
        if ($osInfo -and $osInfo.InstanceId) {
            & $emitRow 'Configuration' 'SQL registry config' "$($osInfo.InstanceId)" 'INFO' "Edition '$($osInfo.Edition)'; version $($osInfo.Version); patch $($osInfo.PatchLevel); registry TCP static='$($osInfo.RegStaticPort)' dynamic='$($osInfo.RegDynamicPort)'."
        }
        elseif ($osInfo -and $osInfo.RegError) {
            & $emitRow 'Configuration' 'SQL registry config' $instanceName 'INFO' "Registry config not read: $($osInfo.RegError)"
        }

        # --- Local administrator on the target ---------------------------
        if ($isLocal) {
            $adminSid = New-Object Security.Principal.SecurityIdentifier('S-1-5-32-544')
            $isLocalAdmin = $isElevated -or (@($curId.Groups) -contains $adminSid)
            if ($isLocalAdmin) {
                & $emitRow 'Permissions' 'Local administrator' $curUser 'OK' 'Current user is a local administrator on this host.'
            }
            else {
                & $emitRow 'Permissions' 'Local administrator' $curUser 'WARN' 'Current user does not appear to be a local administrator.' "Add-LocalGroupMember -Group 'Administrators' -Member '$curUser'   # run elevated on [$targetHost]"
            }
        }
        elseif ($osInfo -and -not $osInfo.LocalAdminsError) {
            $probeUser = if ($Credential) { $Credential.UserName } else { $curUser }
            $short = ($probeUser -split '\\')[-1]
            $isLocalAdmin = @($osInfo.LocalAdmins) | Where-Object { $_ -match [regex]::Escape($short) }
            if ($isLocalAdmin) {
                & $emitRow 'Permissions' 'Local administrator' $probeUser 'OK' "Account is in the local Administrators group on [$targetHost]."
            }
            else {
                & $emitRow 'Permissions' 'Local administrator' $probeUser 'WARN' "Account was not found in the local Administrators group on [$targetHost]." "Add-LocalGroupMember -Group 'Administrators' -Member '$probeUser'   # run elevated on [$targetHost]"
            }
        }
        else {
            & $emitRow 'Permissions' 'Local administrator' $targetHost 'SKIPPED' 'Could not read the local Administrators group (needs local run or WinRM).' "Run locally elevated on [$targetHost] to verify local admin membership."
        }

        # --- SQL Server service ------------------------------------------
        if ($osInfo) {
            switch ($osInfo.SqlServiceStatus) {
                'Running'  { & $emitRow 'Service' 'SQL Server service' $osInfo.SqlServiceName 'OK'     "Service is Running (start: $($osInfo.SqlServiceStart))." }
                'NotFound' { & $emitRow 'Service' 'SQL Server service' $osInfo.SqlServiceName 'WARN'   'Service was not found on the target (wrong instance name or host?).' "Verify the instance name; list services: Get-Service *SQL* on [$targetHost]." }
                default    { & $emitRow 'Service' 'SQL Server service' $osInfo.SqlServiceName 'FAILED' "Service is $($osInfo.SqlServiceStatus)." "Start-Service '$($osInfo.SqlServiceName)'   # run elevated on [$targetHost]" }
            }
        }
        else {
            & $emitRow 'Service' 'SQL Server service' $targetHost 'SKIPPED' 'Service state not read (no local/WinRM access).' ''
        }

        # --- 3) SQL logon + protocol/port facts from DMVs ----------------
        $candidates = @()
        if ($explicitPort) { $candidates += "$targetHost,$explicitPort" }
        if (-not $isDefault) { $candidates += "$targetHost\$instanceName" }
        if ($isDefault -and -not $explicitPort) { $candidates += "$targetHost" }
        if ($candidates.Count -eq 0) { $candidates += "$targetHost" }
        $candidates = $candidates | Select-Object -Unique

        $conn = $null
        $connectedDataSource = $null
        $connectError = $null
        foreach ($ds in $candidates) {
            try {
                $c = New-Object System.Data.SqlClient.SqlConnection (& $buildConnStr $ds)
                $c.Open()
                $conn = $c
                $connectedDataSource = $ds
                break
            }
            catch {
                $connectError = $_.Exception.Message
                if ($c) { try { $c.Dispose() } catch {} }
            }
        }

        $loginUsed = if ($SqlCredential) { $SqlCredential.UserName } else { "$curUser (Integrated Security)" }
        $isSysadmin = $null
        $tcpEnabled = $null; $npEnabled = $null; $smEnabled = $null
        $configPort = $null; $portKind = $null
        $listenerPort = $null; $usedPort = $null

        if ($conn) {
            & $emitRow 'SQL logon' 'Connect to SQL' $connectedDataSource 'OK' "Connected as [$loginUsed]."

            try {
                $info = & $sqlQuery $conn @"
SELECT
    ServerName    = CONVERT(sysname, SERVERPROPERTY('ServerName')),
    InstanceName  = ISNULL(CONVERT(sysname, SERVERPROPERTY('InstanceName')), N'MSSQLSERVER'),
    IsClustered   = CONVERT(int, SERVERPROPERTY('IsClustered')),
    Version       = CONVERT(sysname, SERVERPROPERTY('ProductVersion')),
    LoginName     = SUSER_SNAME(),
    IsSysadmin    = IS_SRVROLEMEMBER('sysadmin'),
    SessionPort   = (SELECT TOP (1) local_tcp_port FROM sys.dm_exec_connections WHERE session_id = @@SPID),
    NetTransport  = (SELECT TOP (1) net_transport   FROM sys.dm_exec_connections WHERE session_id = @@SPID);
"@
                if ($info -and $info.Rows.Count -gt 0) {
                    $r0 = $info.Rows[0]
                    $loginUsed = [string]$r0.LoginName
                    $isSysadmin = ([int]$r0.IsSysadmin -eq 1)
                    if ($r0.SessionPort -isnot [System.DBNull] -and [int]$r0.SessionPort -gt 0) { $usedPort = [int]$r0.SessionPort }
                    & $emitRow 'SQL server' 'Instance identity' ([string]$r0.ServerName) 'INFO' "Version $($r0.Version); transport $($r0.NetTransport)."
                }
            }
            catch {
                & $emitRow 'SQL server' 'Instance identity' $connectedDataSource 'WARN' "Could not read server properties: $($_.Exception.Message)"
            }

            try {
                $ls = & $sqlQuery $conn "SELECT port FROM sys.dm_tcp_listener_states WITH (NOLOCK) WHERE type_desc = 'TSQL' AND state_desc = 'ONLINE' AND is_ipv4 = 1 ORDER BY port"
                if ($ls -and $ls.Rows.Count -gt 0) {
                    $ports = @($ls.Rows | ForEach-Object { [int]$_.port } | Sort-Object -Unique)
                    $listenerPort = $ports[0]
                }
            }
            catch {}

            try {
                $reg = & $sqlQuery $conn @"
SELECT
    registry_key = CONVERT(nvarchar(512), registry_key),
    value_name   = CONVERT(nvarchar(256), value_name),
    value_data   = CONVERT(nvarchar(512), value_data)
FROM sys.dm_server_registry
WHERE registry_key LIKE N'%SuperSocketNetLib%';
"@
                if ($reg -and $reg.Rows.Count -gt 0) {
                    $getVal = {
                        param([string] $KeyRegex, [string] $ValueName)
                        $m = $reg.Rows | Where-Object { ([string]$_.registry_key) -match $KeyRegex -and ([string]$_.value_name) -ieq $ValueName }
                        if ($m) { return [string]@($m)[0].value_data } else { return $null }
                    }
                    $t = & $getVal '\\SuperSocketNetLib\\Tcp$' 'Enabled'; if ($null -ne $t) { $tcpEnabled = ([string]$t -eq '1') }
                    $n = & $getVal '\\SuperSocketNetLib\\Np$'  'Enabled'; if ($null -ne $n) { $npEnabled  = ([string]$n -eq '1') }
                    $s = & $getVal '\\SuperSocketNetLib\\Sm$'  'Enabled'; if ($null -ne $s) { $smEnabled  = ([string]$s -eq '1') }

                    $staticPort  = & $getVal '\\Tcp\\IPAll$' 'TcpPort'
                    $dynamicPort = & $getVal '\\Tcp\\IPAll$' 'TcpDynamicPorts'
                    if ($staticPort -and ($staticPort -match '\d')) {
                        $configPort = [int](($staticPort -split ',')[0].Trim())
                        $portKind = 'static'
                    }
                    elseif ($dynamicPort -and ($dynamicPort -match '\d') -and ($dynamicPort.Trim() -ne '0')) {
                        $configPort = [int](($dynamicPort -split ',')[0].Trim())
                        $portKind = 'dynamic'
                    }
                }
            }
            catch {}
        }
        else {
            & $emitRow 'SQL logon' 'Connect to SQL' ($candidates -join ' | ') 'FAILED' "Could not connect: $connectError" "From this host test the port (see below). If the login is wrong, verify the account; if the instance uses a dynamic port, connect by HOST,PORT or start SQL Browser."
        }

        # --- Registry fallback for protocols + port (service may be down) -
        if ($null -eq $tcpEnabled -and $osInfo -and $null -ne $osInfo.RegTcpEnabled) { $tcpEnabled = [bool]$osInfo.RegTcpEnabled }
        if ($null -eq $npEnabled  -and $osInfo -and $null -ne $osInfo.RegNpEnabled)  { $npEnabled  = [bool]$osInfo.RegNpEnabled }
        if ($null -eq $smEnabled  -and $osInfo -and $null -ne $osInfo.RegSmEnabled)  { $smEnabled  = [bool]$osInfo.RegSmEnabled }

        $regPort = $null
        if ($osInfo) {
            if ($osInfo.RegStaticPort -and ($osInfo.RegStaticPort -match '\d')) {
                $regPort = [int](($osInfo.RegStaticPort -split ',')[0].Trim())
            }
            elseif ($osInfo.RegDynamicPort -and ($osInfo.RegDynamicPort -match '\d') -and ($osInfo.RegDynamicPort.Trim() -ne '0')) {
                $regPort = [int](($osInfo.RegDynamicPort -split ',')[0].Trim())
            }
        }
        if (-not $configPort -and $regPort) {
            $configPort = $regPort
            $portKind = if ($osInfo.RegStaticPort -and ($osInfo.RegStaticPort -match '\d')) { 'static' } else { 'dynamic' }
        }

        # --- Resolve the effective port (used > listener > config > explicit > registry) -
        $effectivePort = @($usedPort, $listenerPort, $configPort, $explicitPort, $regPort | Where-Object { $_ } )[0]

        # --- 4) Protocol reporting + fixes -------------------------------
        if ($null -ne $tcpEnabled) {
            if ($tcpEnabled) {
                & $emitRow 'Protocol' 'TCP/IP enabled' $instanceName 'OK' 'TCP/IP is enabled (required for remote connections).'
            }
            else {
                & $emitRow 'Protocol' 'TCP/IP enabled' $instanceName 'FAILED' 'TCP/IP is DISABLED - remote servers cannot connect.' "Enable TCP/IP: SQL Server Configuration Manager > SQL Server Network Configuration > Protocols for $instanceName > TCP/IP = Enabled, then restart the SQL service."
            }
        }
        elseif ($conn) {
            & $emitRow 'Protocol' 'TCP/IP enabled' $instanceName 'INFO' 'Could not read protocol config (sys.dm_server_registry / registry unavailable).'
        }
        if ($null -ne $npEnabled) {
            & $emitRow 'Protocol' 'Named Pipes' $instanceName 'INFO' ("Named Pipes is {0}." -f $(if ($npEnabled) { 'ENABLED' } else { 'disabled' }))
        }
        if ($null -ne $smEnabled) {
            & $emitRow 'Protocol' 'Shared Memory' $instanceName 'INFO' ("Shared Memory is {0} (local connections only)." -f $(if ($smEnabled) { 'ENABLED' } else { 'disabled' }))
        }

        # --- Configured / used port --------------------------------------
        if ($configPort) {
            if ($portKind -eq 'dynamic' -and -not $isDefault) {
                & $emitRow 'Port' 'Configured TCP port' $instanceName 'WARN' "Named instance uses a DYNAMIC port ($configPort); it can change on restart and is awkward to firewall." "Set a STATIC port: SQL Config Manager > TCP/IP > IPAll > TCP Port = $configPort, clear 'TCP Dynamic Ports', restart the service. Then connect with HOST,$configPort."
            }
            else {
                & $emitRow 'Port' 'Configured TCP port' $instanceName 'OK' "SQL is configured for TCP port $configPort ($portKind)."
            }
        }
        if ($effectivePort) {
            & $emitRow 'Port' 'Port in use' $instanceName 'INFO' "Effective TCP port for connections: $effectivePort."
        }
        else {
            & $emitRow 'Port' 'Port in use' $instanceName 'WARN' 'Could not determine the TCP port.' 'Connect once by instance name (with SQL Browser running) or set a static port, then re-run.'
        }

        if ($osInfo -and -not $osInfo.ListeningError -and @($osInfo.ListeningPorts).Count -gt 0) {
            if ($effectivePort -and (@($osInfo.ListeningPorts) -contains [int]$effectivePort)) {
                & $emitRow 'Port' 'Port listening' $targetHost 'OK' "sqlservr is listening on port $effectivePort (ports: $((@($osInfo.ListeningPorts)) -join ', '))."
            }
            else {
                & $emitRow 'Port' 'Port listening' $targetHost 'WARN' "sqlservr listening ports: $((@($osInfo.ListeningPorts)) -join ', ')." 'Confirm which port the client should use; align firewall + connection string to a listening port.'
            }
        }

        # --- 5) SQL Browser ----------------------------------------------
        if ($osInfo) {
            if ($isDefault) {
                & $emitRow 'Service' 'SQL Browser' 'SQLBrowser' 'INFO' 'Default instance - SQL Browser is not required (connect by host or host,1433).'
            }
            elseif ($osInfo.BrowserStatus -eq 'Running') {
                & $emitRow 'Service' 'SQL Browser' 'SQLBrowser' 'OK' 'SQL Browser is Running (connecting by instance name over UDP 1434 will work).'
            }
            else {
                & $emitRow 'Service' 'SQL Browser' 'SQLBrowser' 'WARN' "SQL Browser is $($osInfo.BrowserStatus); connecting by HOST\$instanceName may fail." "Set-Service SQLBrowser -StartupType Automatic; Start-Service SQLBrowser   # on [$targetHost] - or connect by HOST,$($effectivePort) and skip the Browser."
            }
        }

        # --- 6) Windows Firewall -----------------------------------------
        $tcpAllowed = $false
        if ($effectivePort -and $osInfo) {
            $ap = @($osInfo.AllowedTcpPorts)
            if (($ap -contains "$effectivePort") -or ($ap -contains 'Any')) { $tcpAllowed = $true }
        }
        if ($osInfo -and -not $osInfo.FirewallError) {
            $fwOn = @($osInfo.FirewallOnProfiles)
            if ($fwOn.Count -eq 0) {
                & $emitRow 'Firewall' 'Firewall profiles' $targetHost 'INFO' 'All firewall profiles are OFF (nothing blocking the port).'
            }
            else {
                & $emitRow 'Firewall' 'Firewall profiles' $targetHost 'INFO' ("Enabled profiles: {0}." -f ($fwOn -join ', '))
                if ($effectivePort -and $tcpAllowed) {
                    & $emitRow 'Firewall' 'Inbound TCP rule' $targetHost 'OK' "An inbound allow rule covers TCP $effectivePort."
                }
                elseif ($effectivePort) {
                    & $emitRow 'Firewall' 'Inbound TCP rule' $targetHost 'WARN' "No inbound allow rule found for TCP $effectivePort." "New-NetFirewallRule -DisplayName 'SQL Server TCP $effectivePort' -Direction Inbound -Protocol TCP -LocalPort $effectivePort -Action Allow   # on [$targetHost]"
                }
                if (-not $isDefault) {
                    if ($osInfo.UdpBrowserRuleFound) {
                        & $emitRow 'Firewall' 'Inbound UDP 1434' $targetHost 'OK' 'An inbound allow rule covers UDP 1434 (SQL Browser).'
                    }
                    else {
                        & $emitRow 'Firewall' 'Inbound UDP 1434' $targetHost 'WARN' 'No inbound allow rule for UDP 1434 (needed to connect by instance name).' "New-NetFirewallRule -DisplayName 'SQL Browser UDP 1434' -Direction Inbound -Protocol UDP -LocalPort 1434 -Action Allow   # on [$targetHost]"
                    }
                }
            }
        }

        # --- 7) Raw TCP connectivity from THIS host ----------------------
        $tcpConnectOk = $null
        if ($effectivePort) {
            $tcpConnectOk = & $testTcp $targetHost ([int]$effectivePort) ($ConnectTimeoutSeconds * 1000)
            if ($tcpConnectOk) {
                & $emitRow 'Network' 'TCP connect' "$targetHost`:$effectivePort" 'OK' "This host can open a TCP connection to $targetHost on port $effectivePort."
            }
            else {
                & $emitRow 'Network' 'TCP connect' "$targetHost`:$effectivePort" 'FAILED' "This host CANNOT reach $targetHost on TCP $effectivePort." "Check the firewall on [$targetHost] and any network path: Test-NetConnection $targetHost -Port $effectivePort"
            }
        }
        else {
            & $emitRow 'Network' 'TCP connect' $targetHost 'SKIPPED' 'No known port to test.' 'Determine the port first (SQL Browser or a static port), then re-run.'
        }

        # --- 8) Sysadmin --------------------------------------------------
        if ($null -ne $isSysadmin) {
            if ($isSysadmin) {
                & $emitRow 'Permissions' 'Sysadmin (SQL)' $loginUsed 'OK' 'The connecting login is a member of the sysadmin server role.'
            }
            else {
                & $emitRow 'Permissions' 'Sysadmin (SQL)' $loginUsed 'FAILED' 'The connecting login is NOT sysadmin.' "Run as an existing sysadmin: CREATE LOGIN [$loginUsed] FROM WINDOWS;  ALTER SERVER ROLE sysadmin ADD MEMBER [$loginUsed];"
            }
        }
        elseif ($conn) {
            & $emitRow 'Permissions' 'Sysadmin (SQL)' $loginUsed 'WARN' 'Could not evaluate sysadmin membership.'
        }

        if ($conn) { try { $conn.Close(); $conn.Dispose() } catch {} }

        # --- 9) Connection recommendations -------------------------------
        $connectByInstanceName = if ($isDefault) { $targetHost } else { "$targetHost\$instanceName" }
        $connectByHostPort      = if ($effectivePort) { "$targetHost,$effectivePort" } else { $null }

        Write-Host ""
        Write-Host "How to connect from another server:" -ForegroundColor Cyan
        if (-not $isDefault) {
            Write-Host "  By instance name : $connectByInstanceName    (requires SQL Browser + UDP 1434 open)" -ForegroundColor White
        }
        else {
            Write-Host "  By host          : $connectByInstanceName" -ForegroundColor White
        }
        if ($connectByHostPort) {
            Write-Host "  By host,port     : $connectByHostPort    (no SQL Browser needed - recommended)" -ForegroundColor White
        }

        if (-not $isDefault) {
            & $emitRow 'Connection' 'Connect by instance name' $connectByInstanceName 'INFO' "Use HOST\INSTANCE (needs SQL Browser running and UDP 1434 open)."
        }
        if ($connectByHostPort) {
            & $emitRow 'Connection' 'Connect by host,port' $connectByHostPort 'INFO' 'Use HOST,PORT - works without SQL Browser (recommended for firewalled links).'
        }

        # --- Fix suggestions, one per row with a description -------------
        $fixRows = @(
            $report | Where-Object { $_.FixSuggestion } | ForEach-Object {
                [pscustomobject]@{
                    Category      = $_.Category
                    Check         = $_.Check
                    Description   = $_.Detail
                    FixSuggestion = $_.FixSuggestion
                }
            }
        )

        Write-Host ""
        Write-Host "Fix suggestions:" -ForegroundColor Cyan
        if ($fixRows.Count -eq 0) {
            Write-Host "  (none - all checks passed)" -ForegroundColor Green
        }
        else {
            $i = 0
            foreach ($f in $fixRows) {
                $i++
                Write-Host ("  {0}. [{1}] {2}" -f $i, $f.Check, $f.Description) -ForegroundColor Yellow
                Write-Host ("     Fix: {0}" -f $f.FixSuggestion) -ForegroundColor Gray
            }
        }
        Write-Host ""

        # --- Summary object ----------------------------------------------
        $hasFail = @($report | Where-Object { $_.Status -eq 'FAILED' }).Count -gt 0
        $hasWarn = @($report | Where-Object { $_.Status -eq 'WARN' }).Count -gt 0
        $overall = if ($hasFail) { 'FAILED' } elseif ($hasWarn) { 'WARN' } else { 'OK' }

        $sqlSvcStatus  = if ($osInfo) { $osInfo.SqlServiceStatus } else { $null }
        $browserStatus = if ($osInfo) { $osInfo.BrowserStatus } else { $null }
        $fwAllowsPort  = if ($osInfo -and -not $osInfo.FirewallError) { [bool]$tcpAllowed } else { $null }
        $portListening = if ($osInfo -and @($osInfo.ListeningPorts).Count -gt 0 -and $effectivePort) { (@($osInfo.ListeningPorts) -contains [int]$effectivePort) } else { $null }
        $instIdOut     = if ($osInfo) { "$($osInfo.InstanceId)" } else { '' }
        $editionOut    = if ($osInfo) { "$($osInfo.Edition)" } else { '' }
        $versionOut    = if ($osInfo) { "$($osInfo.Version)" } else { '' }

        [pscustomobject]@{
            SqlInstance           = $SqlInstance
            TargetHost            = $targetHost
            InstanceName          = $instanceName
            IsDefaultInstance     = $isDefault
            IsLocal               = $isLocal
            Port                  = $effectivePort
            PortKind              = $portKind
            InstanceId            = $instIdOut
            Edition               = $editionOut
            Version               = $versionOut
            TcpEnabled            = $tcpEnabled
            NamedPipesEnabled     = $npEnabled
            SharedMemoryEnabled   = $smEnabled
            SqlServiceStatus      = $sqlSvcStatus
            BrowserStatus         = $browserStatus
            FirewallAllowsPort    = $fwAllowsPort
            PortListening         = $portListening
            TcpConnectOk          = $tcpConnectOk
            Connected             = [bool]$conn
            LoginUsed             = $loginUsed
            IsSysadmin            = $isSysadmin
            IsRunAsAdmin          = $isElevated
            ConnectByInstanceName = $connectByInstanceName
            ConnectByHostPort     = $connectByHostPort
            OverallStatus         = $overall
            FixSuggestions        = $fixRows
            Checks                = $report.ToArray()
        }
    }

    # =====================================================================
    # Resolve targets: explicit -SqlInstance, or discover local instances.
    # =====================================================================
    if ($SqlInstance) {
        $targets = @($SqlInstance)
    }
    else {
        Write-Host ""
        Write-Host "No -SqlInstance specified: discovering local SQL Server instances from the registry..." -ForegroundColor Cyan
        $localInstances = & $discoverLocalInstances

        if (@($localInstances).Count -eq 0) {
            Write-Host "No SQL Server instance found in the registry on this host." -ForegroundColor Red
            $noneFix = [pscustomobject]@{
                Category      = 'Discovery'
                Check         = 'Find local instance'
                Description   = 'No SQL Server instance is registered under HKLM\SOFTWARE\Microsoft\Microsoft SQL Server\Instance Names\SQL on this host.'
                FixSuggestion = "Install SQL Server on this host, or target a remote server: Test-MSSqlConnectivity -SqlInstance 'HOST\INSTANCE'"
            }
            return [pscustomobject]@{
                SqlInstance           = $null
                TargetHost            = $env:COMPUTERNAME
                InstanceName          = $null
                IsDefaultInstance     = $null
                IsLocal               = $true
                Port                  = $null
                PortKind              = $null
                InstanceId            = ''
                Edition               = ''
                Version               = ''
                TcpEnabled            = $null
                NamedPipesEnabled     = $null
                SharedMemoryEnabled   = $null
                SqlServiceStatus      = $null
                BrowserStatus         = $null
                FirewallAllowsPort    = $null
                PortListening         = $null
                TcpConnectOk          = $null
                Connected             = $false
                LoginUsed             = $null
                IsSysadmin            = $null
                IsRunAsAdmin          = $isElevated
                ConnectByInstanceName = $null
                ConnectByHostPort     = $null
                OverallStatus         = 'FAILED'
                FixSuggestions        = @($noneFix)
                Checks                = @($noneFix)
            }
        }

        Write-Host ("Found {0} local instance(s): {1}" -f @($localInstances).Count, (@($localInstances | ForEach-Object { $_.InstanceName }) -join ', ')) -ForegroundColor Cyan
        $targets = @($localInstances | ForEach-Object {
            if ($_.InstanceName -eq 'MSSQLSERVER') { "$env:COMPUTERNAME" } else { "$env:COMPUTERNAME\$($_.InstanceName)" }
        })
    }

    foreach ($t in $targets) { & $runOne $t }
}
