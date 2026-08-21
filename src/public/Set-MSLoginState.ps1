function Set-MSLoginState {
<#
.SYNOPSIS
    Enables, disables, or drops SQL Server logins - and kills their connections -
    so you can take exclusive control of a server or a single database during an
    intervention (a reliable alternative to SINGLE_USER, which loses the race
    when another session grabs the connection first).

.DESCRIPTION
    Resolves a set of target logins and applies -Action (Enable | Disable | Drop)
    to each. By default DISABLE and DROP also kill the login's active sessions
    (no prompt) so it is evicted immediately and, because it is disabled/denied
    first, cannot reconnect.

    Target selection:
      * -Login accepts one or more names or LIKE patterns ('%' wildcard),
        e.g. 'APP_%'.
      * -Database scopes the operation to one database:
          - Disable / Drop : the logins currently CONNECTED to that database.
          - Enable         : the logins mapped to that database's users (matched
                             by SID), so access can be restored after the work.
      * With no -Database and no -Login, Enable/Disable apply to ALL logins on
        the instance. Drop ALWAYS requires -Login (it never targets every login).

    Always protected (never enabled, disabled, dropped, or killed):
      * the CURRENT login (yourself) and your own session;
      * system logins: 'sa', '##*' certificate logins, 'NT SERVICE\*' and
        'NT AUTHORITY\*';
      * any login listed in -IgnoreLogin (e.g. your co-workers).

    Dropping a login:
      * By default DROP runs a simple DROP LOGIN. If the login cannot be dropped
        because it granted server-level permissions, the operation fails and the
        required REVOKE statements are returned in the RemediationScript property
        so you can review them.
      * -ForceDrop first revokes every permission the login granted (server level,
        plus every permission its database users granted) and then drops it.

    Nothing is changed under -WhatIf. One result object is returned per login the
    function considered and the full list is printed at the end, so you can see
    exactly what happened to every login.

.PARAMETER SqlInstance
    Target SQL Server instance ("Server" or "Server\Instance").

.PARAMETER SqlCredential
    Optional PSCredential used to connect. When omitted, Integrated Security is
    used. Passed through to dbatools.

.PARAMETER Action
    What to do with each target login: Enable, Disable, or Drop.

.PARAMETER Login
    One or more login names or LIKE patterns ('%' wildcard) to target. Required
    when -Action is Drop.

.PARAMETER Database
    Restrict the operation to a single database (see the DESCRIPTION for how each
    action interprets it).

.PARAMETER IgnoreLogin
    One or more login names to leave untouched (exact, case-insensitive), for
    example co-workers who must keep their access during your intervention.

.PARAMETER NoKill
    Do not kill active sessions. By default Disable and Drop kill the target
    login's connections (without prompting). Enable never kills connections.

.PARAMETER ForceDrop
    Only meaningful with -Action Drop. Revoke every permission granted by the
    login (server level) and by its database users (database level), then drop
    the login. Without this switch a blocked drop fails and lists the required
    REVOKE statements in the RemediationScript property.

.PARAMETER Force
    Suppress the confirmation prompt (does not override -WhatIf).

.PARAMETER Timeout
    Query timeout in seconds for Invoke-DbaQuery. Default 0 (provider default).

.EXAMPLE
    Set-MSLoginState -SqlInstance 'SQL01' -Database 'Sales' -Action Disable -Force

    Disables and kicks off every login currently connected to [Sales] (except
    you, system logins, and any -IgnoreLogin), so you can take control.

.EXAMPLE
    Set-MSLoginState -SqlInstance 'SQL01' -Database 'Sales' -Action Enable -Force

    Re-enables the logins mapped to users in [Sales] after the intervention.

.EXAMPLE
    Set-MSLoginState -SqlInstance 'SQL01' -Action Disable -IgnoreLogin 'DOMAIN\alice','DOMAIN\bob' -Force

    Disables all logins on the instance except you, the system logins and your
    two co-workers.

.EXAMPLE
    Set-MSLoginState -SqlInstance 'SQL01' -Action Drop -Login 'TempApp%' -Force

    Kills connections for, and drops, every login whose name starts with
    'TempApp', together with their database users.

.EXAMPLE
    Set-MSLoginState -SqlInstance 'SQL01' -Action Drop -Login 'AMERICAS\jacquep-adm' -ForceDrop -Force

    Revokes every permission the login granted and then drops it, even when a
    plain DROP LOGIN is blocked by "has granted one or more permission(s)".

.NOTES
    Requires dbatools (Connect-DbaInstance, Invoke-DbaQuery, Get-DbaProcess,
    Stop-DbaProcess).
#>
    [CmdletBinding(SupportsShouldProcess, ConfirmImpact = 'High')]
    param(
        [Parameter(Mandatory)]
        [string] $SqlInstance,

        [PSCredential] $SqlCredential,

        [Parameter(Mandatory)]
        [ValidateSet('Enable', 'Disable', 'Drop')]
        [string] $Action,

        [Alias('Name')]
        [string[]] $Login,

        [string] $Database,

        [string[]] $IgnoreLogin,

        [switch] $NoKill,

        [switch] $ForceDrop,

        [switch] $Force,

        [int] $Timeout = 0
    )

    if (-not (Get-Command Connect-DbaInstance -ErrorAction SilentlyContinue)) {
        throw "dbatools is required. Run: Import-Module dbatools"
    }

    # A Drop must always be scoped to an explicit login (never every login).
    $loginPatterns = @($Login | Where-Object { -not [string]::IsNullOrWhiteSpace($_) })
    if ($Action -eq 'Drop' -and $loginPatterns.Count -eq 0) {
        throw "Action 'Drop' requires -Login (a name or '%' pattern). It never targets every login."
    }

    # -Force suppresses confirmation without breaking -WhatIf / explicit -Confirm.
    if ($Force -and -not $PSBoundParameters.ContainsKey('Confirm')) {
        $ConfirmPreference = 'None'
    }

    $connectParams = @{ SqlInstance = $SqlInstance; ErrorAction = 'Stop' }
    if ($SqlCredential) { $connectParams['SqlCredential'] = $SqlCredential }
    try {
        $server = Connect-DbaInstance @connectParams
    }
    catch {
        throw "Could not connect to [$SqlInstance]: $($_.Exception.Message)"
    }

    # --- Small helpers ------------------------------------------------------
    $bracket = { param([string] $n) '[' + ($n -replace '\]', ']]') + ']' }
    $literal = { param([string] $n) $n -replace "'", "''" }

    # LIKE-style match supporting '%' (translated to the PowerShell -like '*').
    $matchAny = {
        param([string] $Name, [string[]] $Patterns)
        foreach ($p in $Patterns) {
            if ([string]::IsNullOrEmpty($p)) { continue }
            if ($Name -like ($p -replace '%', '*')) { return $true }
        }
        return $false
    }

    # System logins that must never be touched (req: 'sa' + 'NT SERVICE\', plus
    # the instance-critical '##*' certificate logins and 'NT AUTHORITY\').
    $systemPatterns = @('sa', '##*', 'NT SERVICE\*', 'NT AUTHORITY\*')
    $isSystem = { param([string] $n) foreach ($p in $systemPatterns) { if ($n -like $p) { return $true } } return $false }

    $ignoreSet = @{}
    foreach ($ig in @($IgnoreLogin | Where-Object { $_ })) { $ignoreSet[$ig.ToLowerInvariant()] = $true }
    $isIgnored = { param([string] $n) $ignoreSet.ContainsKey($n.ToLowerInvariant()) }

    # Kill every session of a login except my own SPID; returns the count killed.
    $killLogin = {
        param([string] $n)
        $procs = @(Get-DbaProcess -SqlInstance $server -Login $n -EnableException |
            Where-Object { [int]$_.Spid -ne $mySpid })
        if ($procs.Count -gt 0) {
            $procs | Stop-DbaProcess -Confirm:$false -EnableException | Out-Null
        }
        return $procs.Count
    }

    # REVOKE statements for every server-level permission this login granted
    # (it is the grantor). Returned as strings so they can be shown or executed.
    $getServerGrantRevokes = {
        param([string] $n)
        $q = @"
SELECT stmt =
    'REVOKE ' + perm.permission_name COLLATE DATABASE_DEFAULT +
    CASE perm.class
        WHEN 101 THEN ' ON LOGIN::' + QUOTENAME(lp.name) COLLATE DATABASE_DEFAULT
        WHEN 105 THEN ' ON ENDPOINT::' + QUOTENAME(ep.name) COLLATE DATABASE_DEFAULT
        ELSE ''
    END +
    ' FROM ' + QUOTENAME(ge.name) COLLATE DATABASE_DEFAULT + ' CASCADE AS ' + QUOTENAME(gr.name) COLLATE DATABASE_DEFAULT + ';'
FROM sys.server_permissions perm
JOIN sys.server_principals gr ON gr.principal_id = perm.grantor_principal_id
JOIN sys.server_principals ge ON ge.principal_id = perm.grantee_principal_id
LEFT JOIN sys.server_principals lp ON perm.class = 101 AND lp.principal_id = perm.major_id
LEFT JOIN sys.endpoints ep ON perm.class = 105 AND ep.endpoint_id = perm.major_id
WHERE perm.grantor_principal_id = SUSER_ID(N'$(& $literal $n)');
"@
        return @(Invoke-DbaQuery -SqlInstance $server -Database master -EnableException -QueryTimeout $Timeout -Query $q |
            ForEach-Object { [string]$_.stmt } | Where-Object { $_ })
    }

    # REVOKE statements for every database-level permission the login's user
    # granted inside $dbName (the user is the grantor).
    $getDbGrantRevokes = {
        param([string] $dbName, [string] $n)
        $q = @"
DECLARE @pid int = (SELECT principal_id FROM sys.database_principals WHERE sid = SUSER_SID(N'$(& $literal $n)'));
SELECT stmt =
    'REVOKE ' + perm.permission_name COLLATE DATABASE_DEFAULT +
    CASE perm.class
        WHEN 1 THEN ' ON OBJECT::' + QUOTENAME(SCHEMA_NAME(o.schema_id)) COLLATE DATABASE_DEFAULT + '.' + QUOTENAME(o.name) COLLATE DATABASE_DEFAULT
        WHEN 3 THEN ' ON SCHEMA::' + QUOTENAME(sch.name) COLLATE DATABASE_DEFAULT
        WHEN 4 THEN ' ON USER::' + QUOTENAME(dpr.name) COLLATE DATABASE_DEFAULT
        WHEN 5 THEN ' ON ASSEMBLY::' + QUOTENAME(asm.name) COLLATE DATABASE_DEFAULT
        WHEN 6 THEN ' ON TYPE::' + QUOTENAME(SCHEMA_NAME(typ.schema_id)) COLLATE DATABASE_DEFAULT + '.' + QUOTENAME(typ.name) COLLATE DATABASE_DEFAULT
        ELSE ''
    END +
    ' FROM ' + QUOTENAME(ge.name) COLLATE DATABASE_DEFAULT + ' CASCADE AS ' + QUOTENAME(gr.name) COLLATE DATABASE_DEFAULT + ';'
FROM sys.database_permissions perm
JOIN sys.database_principals gr ON gr.principal_id = perm.grantor_principal_id
JOIN sys.database_principals ge ON ge.principal_id = perm.grantee_principal_id
LEFT JOIN sys.objects o ON perm.class = 1 AND o.object_id = perm.major_id
LEFT JOIN sys.schemas sch ON perm.class = 3 AND sch.schema_id = perm.major_id
LEFT JOIN sys.database_principals dpr ON perm.class = 4 AND dpr.principal_id = perm.major_id
LEFT JOIN sys.assemblies asm ON perm.class = 5 AND asm.assembly_id = perm.major_id
LEFT JOIN sys.types typ ON perm.class = 6 AND typ.user_type_id = perm.major_id
WHERE perm.grantor_principal_id = @pid AND perm.class IN (0,1,3,4,5,6);
"@
        return @(Invoke-DbaQuery -SqlInstance $server -Database $dbName -EnableException -QueryTimeout $Timeout -Query $q |
            ForEach-Object { [string]$_.stmt } | Where-Object { $_ })
    }

    $results = New-Object System.Collections.Generic.List[object]
    $emit = {
        param([string] $LoginName, [string] $Act, [int] $Killed, [int] $UsersDropped, [bool] $LoginDropped, [string] $Status, [string] $Notes, [string] $Remediation = '')
        $results.Add([pscustomobject]@{
            SqlInstance      = $SqlInstance
            Login            = $LoginName
            Action           = $Act
            SessionsKilled   = $Killed
            UsersDropped     = $UsersDropped
            LoginDropped     = $LoginDropped
            Status           = $Status
            Notes            = $Notes
            RemediationScript = $Remediation
        })
    }

    # --- Current login + session (never act on myself) ----------------------
    $me = Invoke-DbaQuery -SqlInstance $server -Database master -EnableException -QueryTimeout $Timeout `
            -Query "SELECT me = ORIGINAL_LOGIN(), spid = @@SPID;"
    $currentLogin = [string]$me.me
    $mySpid       = [int]$me.spid

    $hasDatabase = $PSBoundParameters.ContainsKey('Database') -and -not [string]::IsNullOrWhiteSpace($Database)

    # --- Validate -Database if supplied -------------------------------------
    if ($hasDatabase) {
        $dbLiteral = & $literal $Database
        $dbCheck = Invoke-DbaQuery -SqlInstance $server -Database master -EnableException -QueryTimeout $Timeout `
                    -Query "SELECT id = DB_ID(N'$dbLiteral');"
        if ($null -eq $dbCheck.id -or $dbCheck.id -is [System.DBNull]) {
            throw "Database [$Database] was not found on [$SqlInstance]."
        }
    }

    # --- Resolve base candidate logins --------------------------------------
    try {
        if ($hasDatabase) {
            if ($Action -eq 'Enable') {
                # Logins mapped to the database's users (matched by SID).
                $baseNames = @(Invoke-DbaQuery -SqlInstance $server -Database $Database -EnableException -QueryTimeout $Timeout -Query @"
SELECT sp.name
FROM sys.database_principals AS dp
JOIN sys.server_principals  AS sp ON sp.sid = dp.sid
WHERE dp.type IN ('S','U','G')
  AND dp.principal_id > 4
  AND sp.type IN ('S','U','G');
"@ | ForEach-Object { [string]$_.name })
            }
            else {
                # Logins currently connected to the database.
                $baseNames = @(Invoke-DbaQuery -SqlInstance $server -Database master -EnableException -QueryTimeout $Timeout -Query @"
SELECT DISTINCT s.login_name AS name
FROM sys.dm_exec_sessions AS s
WHERE s.is_user_process = 1
  AND s.database_id = DB_ID(N'$dbLiteral')
  AND s.login_name IS NOT NULL
  AND s.login_name <> '';
"@ | ForEach-Object { [string]$_.name })
            }
        }
        else {
            # No database: every server login.
            $baseNames = @(Invoke-DbaQuery -SqlInstance $server -Database master -EnableException -QueryTimeout $Timeout -Query @"
SELECT name FROM sys.server_principals
WHERE type IN ('S','U','G')
ORDER BY name;
"@ | ForEach-Object { [string]$_.name })
        }
    }
    catch {
        throw "Could not resolve target logins on [$SqlInstance]: $($_.Exception.Message)"
    }

    $baseNames = @($baseNames | Where-Object { $_ } | Select-Object -Unique)

    if ($loginPatterns.Count -gt 0) {
        $selected = @($baseNames | Where-Object { & $matchAny $_ $loginPatterns })
    }
    else {
        $selected = $baseNames
    }

    $processed = New-Object 'System.Collections.Generic.HashSet[string]' ([System.StringComparer]::OrdinalIgnoreCase)

    # --- Apply the action to each selected login ----------------------------
    foreach ($name in $selected) {

        # System logins are silently excluded (see -IgnoreLogin for the rest).
        if (& $isSystem $name) { continue }
        [void]$processed.Add($name)

        if ($name -ieq $currentLogin) {
            & $emit $name 'Skip' 0 0 $false 'Skipped' 'Current login (self-protected)'
            continue
        }
        if (& $isIgnored $name) {
            & $emit $name 'Skip' 0 0 $false 'Skipped' 'In -IgnoreLogin list'
            continue
        }

        $opText = switch ($Action) {
            'Enable'  { 'ENABLE + GRANT CONNECT SQL' }
            'Disable' { if ($NoKill) { 'DISABLE + DENY CONNECT SQL' } else { 'DISABLE + DENY CONNECT SQL + kill sessions' } }
            'Drop'    { if ($NoKill) { 'DISABLE + DENY + DROP login and its users' } else { 'DISABLE + DENY + kill sessions + DROP login and its users' } }
        }
        if (-not $PSCmdlet.ShouldProcess("[$SqlInstance] login [$name]", $opText)) { continue }

        $lb           = & $bracket $name
        $ll           = & $literal $name
        $killed       = 0
        $usersDropped = 0
        $loginDropped = $false
        $status       = 'Success'
        $note         = ''

        if ($Action -eq 'Enable') {
            try {
                Invoke-DbaQuery -SqlInstance $server -Database master -EnableException -QueryTimeout $Timeout `
                    -Query "ALTER LOGIN $lb ENABLE;"
                try {
                    # Remove any DENY (which overrides GRANT) then restore CONNECT.
                    Invoke-DbaQuery -SqlInstance $server -Database master -EnableException -QueryTimeout $Timeout `
                        -Query "REVOKE CONNECT SQL FROM $lb; GRANT CONNECT SQL TO $lb;"
                }
                catch {
                    $status = 'Partial'
                    $note   = "enabled, but could not restore CONNECT permission: $($_.Exception.Message)"
                }
                if (-not $note) { $note = 'Enabled; CONNECT SQL granted' }
                Write-MSLog -Level Info -Message "Set-MSLoginState: enabled '$name' on [$SqlInstance]."
                & $emit $name 'Enable' 0 0 $false $status $note
            }
            catch {
                Write-MSLog -Level Error -Message "Set-MSLoginState: failed to enable '$name': $($_.Exception.Message)"
                & $emit $name 'Enable' 0 0 $false 'Failed' $_.Exception.Message
            }
        }
        elseif ($Action -eq 'Disable') {
            try {
                Invoke-DbaQuery -SqlInstance $server -Database master -EnableException -QueryTimeout $Timeout `
                    -Query "ALTER LOGIN $lb DISABLE; DENY CONNECT SQL TO $lb;"
                Write-MSLog -Level Info -Message "Set-MSLoginState: disabled '$name' on [$SqlInstance]."
            }
            catch {
                $status = 'Failed'
                $note   = (@($note, "disable/deny: $($_.Exception.Message)") | Where-Object { $_ }) -join ' | '
                Write-MSLog -Level Error -Message "Set-MSLoginState: failed to disable '$name': $($_.Exception.Message)"
            }

            if (-not $NoKill) {
                try { $killed = & $killLogin $name }
                catch {
                    if ($status -eq 'Success') { $status = 'Partial' }
                    $note = (@($note, "kill: $($_.Exception.Message)") | Where-Object { $_ }) -join ' | '
                    Write-MSLog -Level Warning -Message "Set-MSLoginState: error killing sessions for '$name': $($_.Exception.Message)"
                }
                if ($killed -gt 0) { Write-MSLog -Level Info -Message "Set-MSLoginState: killed $killed session(s) for '$name'." }
            }

            & $emit $name 'Disable' $killed 0 $false $status $note
        }
        else {
            # Drop: disable+deny first so it cannot reconnect during the drop.
            try {
                Invoke-DbaQuery -SqlInstance $server -Database master -EnableException -QueryTimeout $Timeout `
                    -Query "ALTER LOGIN $lb DISABLE; DENY CONNECT SQL TO $lb;"
            }
            catch {
                $status = 'Partial'
                $note   = (@($note, "disable/deny: $($_.Exception.Message)") | Where-Object { $_ }) -join ' | '
                Write-MSLog -Level Warning -Message "Set-MSLoginState: disable/deny before drop failed for '$name': $($_.Exception.Message)"
            }

            if (-not $NoKill) {
                try { $killed = & $killLogin $name }
                catch {
                    if ($status -eq 'Success') { $status = 'Partial' }
                    $note = (@($note, "kill: $($_.Exception.Message)") | Where-Object { $_ }) -join ' | '
                    Write-MSLog -Level Warning -Message "Set-MSLoginState: error killing sessions for '$name': $($_.Exception.Message)"
                }
            }

            # Drop every database user mapped to the login (by SID).
            try {
                $dbNames = @(Invoke-DbaQuery -SqlInstance $server -Database master -EnableException -QueryTimeout $Timeout -Query @"
SELECT name FROM sys.databases
WHERE state_desc = 'ONLINE' AND database_id <> 2 AND HAS_DBACCESS(name) = 1
ORDER BY name;
"@ | ForEach-Object { [string]$_.name })
            }
            catch {
                $dbNames = @()
                if ($status -eq 'Success') { $status = 'Partial' }
                $note = (@($note, "list databases: $($_.Exception.Message)") | Where-Object { $_ }) -join ' | '
            }

            foreach ($dbName in $dbNames) {
                $users = @()
                try {
                    $users = @(Invoke-DbaQuery -SqlInstance $server -Database $dbName -EnableException -QueryTimeout $Timeout -Query @"
SELECT name FROM sys.database_principals
WHERE type IN ('S','U','G') AND principal_id > 4 AND sid = SUSER_SID(N'$ll');
"@ | ForEach-Object { [string]$_.name })
                }
                catch {
                    if ($status -eq 'Success') { $status = 'Partial' }
                    $note = (@($note, "list users in [$dbName]: $($_.Exception.Message)") | Where-Object { $_ }) -join ' | '
                    Write-MSLog -Level Warning -Message "Set-MSLoginState: error listing users in [$dbName] for '$name': $($_.Exception.Message)"
                    continue
                }

                foreach ($u in $users) {
                    $ub = & $bracket $u
                    try {
                        Invoke-DbaQuery -SqlInstance $server -Database $dbName -EnableException -QueryTimeout $Timeout -Query "DROP USER $ub;"
                        $usersDropped++
                        Write-MSLog -Level Info -Message "Set-MSLoginState: dropped user [$u] in [$dbName] (mapped to '$name')."
                    }
                    catch {
                        $userErr = $_.Exception.Message

                        # Force: revoke what the user granted in this database, then retry.
                        if ($ForceDrop) {
                            $dbRevokes = @()
                            try { $dbRevokes = @(& $getDbGrantRevokes $dbName $name) } catch { }
                            foreach ($stmt in $dbRevokes) {
                                try { Invoke-DbaQuery -SqlInstance $server -Database $dbName -EnableException -QueryTimeout $Timeout -Query $stmt }
                                catch { Write-MSLog -Level Warning -Message "Set-MSLoginState: failed db revoke ($stmt) in [$dbName]: $($_.Exception.Message)" }
                            }
                            try {
                                Invoke-DbaQuery -SqlInstance $server -Database $dbName -EnableException -QueryTimeout $Timeout -Query "DROP USER $ub;"
                                $usersDropped++
                                Write-MSLog -Level Info -Message "Set-MSLoginState: force-dropped user [$u] in [$dbName] after revoking $($dbRevokes.Count) grant(s)."
                                continue
                            }
                            catch { $userErr = $_.Exception.Message }
                        }

                        if ($status -eq 'Success') { $status = 'Partial' }
                        $note = (@($note, "drop user [$u] in [$dbName]: $userErr") | Where-Object { $_ }) -join ' | '
                        Write-MSLog -Level Warning -Message "Set-MSLoginState: error dropping user [$u] in [$dbName] for '$name': $userErr"
                    }
                }
            }

            # Drop the login itself (a plain DROP LOGIN first).
            $remediationScript = ''
            try {
                Invoke-DbaQuery -SqlInstance $server -Database master -EnableException -QueryTimeout $Timeout -Query "DROP LOGIN $lb;"
                $loginDropped = $true
                Write-MSLog -Level Warning -Message "Set-MSLoginState: dropped login '$name' on [$SqlInstance]."
            }
            catch {
                $dropError = $_.Exception.Message

                # What blocks the drop: permissions this login granted (grantor).
                $revokeStatements = @()
                try { $revokeStatements = @(& $getServerGrantRevokes $name) }
                catch {
                    Write-MSLog -Level Warning -Message "Set-MSLoginState: could not inspect permissions granted by '$name': $($_.Exception.Message)"
                }

                if ($ForceDrop -and $revokeStatements.Count -gt 0) {
                    foreach ($stmt in $revokeStatements) {
                        try {
                            Invoke-DbaQuery -SqlInstance $server -Database master -EnableException -QueryTimeout $Timeout -Query $stmt
                            Write-MSLog -Level Info -Message "Set-MSLoginState: revoked before drop of '$name': $stmt"
                        }
                        catch {
                            Write-MSLog -Level Warning -Message "Set-MSLoginState: failed to revoke ($stmt) for '$name': $($_.Exception.Message)"
                        }
                    }

                    try {
                        Invoke-DbaQuery -SqlInstance $server -Database master -EnableException -QueryTimeout $Timeout -Query "DROP LOGIN $lb;"
                        $loginDropped = $true
                        $note = (@($note, "force-drop: revoked $($revokeStatements.Count) granted permission(s) before dropping") | Where-Object { $_ }) -join ' | '
                        Write-MSLog -Level Warning -Message "Set-MSLoginState: force-dropped login '$name' on [$SqlInstance] after revoking $($revokeStatements.Count) permission(s)."
                    }
                    catch {
                        $status = 'Failed'
                        $remediationScript = ($revokeStatements -join [Environment]::NewLine)
                        $note = (@($note, "drop login (after force revoke): $($_.Exception.Message)") | Where-Object { $_ }) -join ' | '
                        Write-MSLog -Level Error -Message "Set-MSLoginState: failed to force-drop login '$name': $($_.Exception.Message)"
                    }
                }
                else {
                    $status = 'Failed'
                    $remediationScript = ($revokeStatements -join [Environment]::NewLine)
                    if ($revokeStatements.Count -gt 0) {
                        $note = (@($note, "drop login: $dropError", "re-run with -ForceDrop, or revoke first ($($revokeStatements.Count) statement(s) in RemediationScript)") | Where-Object { $_ }) -join ' | '
                    }
                    else {
                        $note = (@($note, "drop login: $dropError") | Where-Object { $_ }) -join ' | '
                    }
                    Write-MSLog -Level Error -Message "Set-MSLoginState: failed to drop login '$name': $dropError"
                }
            }

            & $emit $name 'Drop' $killed $usersDropped $loginDropped $status $note $remediationScript
        }
    }

    # --- Feedback for explicitly named logins that produced no result -------
    # (literal -Login names only; '%' patterns that match nothing are silent.)
    if ($loginPatterns.Count -gt 0) {
        foreach ($p in $loginPatterns) {
            if ($p -like '*%*') { continue }
            if ($processed.Contains($p)) { continue }

            $existsRow = @(Invoke-DbaQuery -SqlInstance $server -Database master -EnableException -QueryTimeout $Timeout `
                -Query "SELECT name FROM sys.server_principals WHERE name = N'$(& $literal $p)';")

            if ($existsRow.Count -eq 0) {
                & $emit $p 'Skip' 0 0 $false 'NotFound' 'Login does not exist'
            }
            elseif (& $isSystem $p) {
                & $emit $p 'Skip' 0 0 $false 'Skipped' 'System login (protected)'
            }
            elseif ($p -ieq $currentLogin) {
                & $emit $p 'Skip' 0 0 $false 'Skipped' 'Current login (self-protected)'
            }
            else {
                $why = if ($hasDatabase) { "not connected/linked to database [$Database]" } else { 'not in scope' }
                & $emit $p 'Skip' 0 0 $false 'Skipped' $why
            }
        }
    }

    Write-MSLog -Level Info -Message ("Set-MSLoginState: Action={0} considered {1} login(s) on [{2}]." -f $Action, $results.Count, $SqlInstance)

    # --- Final listing: every login the function considered -----------------
    $results
}
