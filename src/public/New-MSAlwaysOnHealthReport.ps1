function New-MSAlwaysOnHealthReport {
<#
.SYNOPSIS
    Builds an Always On health report (structured data + HTML) from
    Get-MSAlwaysOnHealth and writes it to an HTML file.

.DESCRIPTION
    Wraps Get-MSAlwaysOnHealth and turns its rows into a ready-to-consume
    report:
      - a status summary (one line per category),
      - the Always On / cluster configuration,
      - the detected problems with ready-to-run fixes,
      - the full health detail,
    each returned as an array AND rendered to HTML (a summary table and a full
    report document). The full report is also written to disk; when -OutputFile
    is omitted (or its folder is not writable) a safe, writable location is
    chosen automatically, so the report is never lost because C:\Temp is missing
    or locked down.

    Every Get-MSAlwaysOnHealth parameter is accepted and forwarded, so the same
    switches (-UseWmi, -CheckCluster, -CheckListener, -CheckVersion, ...) drive
    the report. This function is READ-ONLY against SQL Server; it only writes the
    local HTML file.

.PARAMETER SqlInstance
    Listener, primary server, any node, or alias of the Always On AG.

.PARAMETER OutputFile
    Path of the HTML report to write. May be a full file path or a folder. When
    omitted, a timestamped file is created in the first writable folder among:
    %TEMP%, the system temp folder, LocalAppData, then the current folder. If
    the requested folder is not writable, the same fallback is used.

.PARAMETER Credential
    SQL credential used to connect. When omitted, the current Windows identity
    (Integrated Security) is used.

.PARAMETER ShowReport
    Open the generated HTML report in the default browser.

.PARAMETER HideSkipped
    Hide 'skipped' entries from the HTML output only: skipped categories are
    dropped from the status-summary table and rows whose status is SKIPPED are
    dropped from the full health-detail table. The returned StatusSummary and
    HealthDetail arrays are NOT affected (they still contain every row), so this
    is purely a presentation option for the HTML report / file.

.PARAMETER CheckAgentJob
    Compare SQL Agent jobs between the current primary and every secondary
    replica (uses Compare-MSAgentJob; ignores Enabled; case-insensitive). Does
    nothing on a standalone instance. Any difference is reported as WARN and
    never fails the report. Level = 'AgentJob'.

.PARAMETER CheckMasterProcedure
    Compare the user stored procedures in the [master] database between the
    current primary and every secondary replica. A procedure whose code differs,
    or that is missing on either side, is reported as WARN (never fails the
    report) and named individually. Does nothing on a standalone instance.
    Level = 'MasterProcedure'.

.PARAMETER LoginFailedIgnoreIp
    One or more client IP addresses (e.g. security scanners) whose failed logins
    are still listed in the full health detail but are excluded from the WARN in
    the status summary and from the detected problems. Forwarded to
    Get-MSAlwaysOnHealth.

.PARAMETER AvailabilityGroup
    Forwarded to Get-MSAlwaysOnHealth (filter by AG name; wildcards allowed).
    All other Get-MSAlwaysOnHealth parameters (-ReplicaHealthOnly, -UseWmi,
    -WmiCredential, -PromptWmiCredential, -IncludeSecondaryDetail,
    -CheckServiceAccount, -CheckListener, -CheckCluster, -CheckFailoverReadiness,
    -CheckBackup, -IncludeConfiguration, -CheckLoginParity, -CheckVersion,
    -CheckEventLog, -EventHoursBack, -ConnectTimeoutSeconds,
    -SecondaryDetailTimeoutSeconds, -QueryTimeoutSeconds, -RetryCount) are
    accepted and forwarded the same way.

.OUTPUTS
    [pscustomobject] with:
      PrimaryServer      - the current primary replica name.
      StatusSummary      - array of { Category, Status }.
      Configuration      - Always On / cluster configuration rows.
      Problems           - detected problems with ready-to-run fixes.
      HealthDetail       - the full Get-MSAlwaysOnHealth rows (+ FixSuggestion).
      StatusSummaryHtml  - the status summary as an HTML table.
      FullReportHtml     - the complete HTML document.
      OutputFile         - the path the HTML report was written to.

.EXAMPLE
    $r = New-MSAlwaysOnHealthReport -SqlInstance 'ag-listener' -UseWmi -CheckCluster -CheckListener
    $r.StatusSummary | Format-Table
    $r.OutputFile

.EXAMPLE
    New-MSAlwaysOnHealthReport -SqlInstance 'ag-listener' -CheckCluster -CheckVersion `
        -OutputFile '\\fileshare\reports\' -ShowReport

    Writes the report into the given folder and opens it.

.NOTES
    Requires dbatools (Invoke-DbaQuery) and Get-MSAlwaysOnHealth (MSTools).
#>
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)]
        [string]$SqlInstance,

        [string]$OutputFile,

        [PSCredential]$Credential,

        [switch]$ShowReport,

        # Hide 'skipped' categories/rows from the HTML output only
        # (the returned StatusSummary/HealthDetail arrays are unaffected).
        [switch]$HideSkipped,

        # ---- Get-MSAlwaysOnHealth pass-through parameters ----
        [string[]]$AvailabilityGroup,
        [switch]$ReplicaHealthOnly,
        [switch]$UseWmi,
        [PSCredential]$WmiCredential,
        [switch]$PromptWmiCredential,
        [switch]$IncludeSecondaryDetail,
        [switch]$CheckServiceAccount,
        [switch]$CheckListener,
        [switch]$CheckCluster,
        [switch]$CheckFailoverReadiness,
        [switch]$CheckBackup,
        [switch]$IncludeConfiguration,
        [switch]$CheckLoginParity,
        [switch]$CheckVersion,
        [switch]$CheckEventLog,
        [ValidateRange(1, 8760)]
        [int]$EventHoursBack = 24,
        [ValidateRange(1, 120)]
        [int]$ConnectTimeoutSeconds = 5,
        [ValidateRange(1, 3600)]
        [int]$SecondaryDetailTimeoutSeconds = 30,
        [ValidateRange(1, 3600)]
        [int]$QueryTimeoutSeconds = 30,
        [ValidateRange(1, 10)]
        [int]$RetryCount = 2,

        # ---- instance-level readiness checks (forwarded) ----
        [switch]$CheckConnection,
        [switch]$CheckDatabaseState,
        [switch]$CheckSystemDatabase,
        [switch]$CheckIoError,
        [switch]$CheckClockSkew,
        [switch]$CheckCertificate,
        [switch]$CheckPendingReboot,
        [switch]$CheckLoginFailed,
        [ValidateRange(1, 8760)]
        [int]$LoginFailedHoursBack = 24,

        # Client IPs (e.g. security scanners) whose failed logins are shown but never warn.
        [string[]]$LoginFailedIgnoreIp,

        # Compare SQL Agent jobs between the primary and each secondary (AG only).
        [switch]$CheckAgentJob,

        # Compare [master] stored procedures between the primary and each secondary.
        [switch]$CheckMasterProcedure
    )

    if (-not (Get-Command Get-MSAlwaysOnHealth -ErrorAction SilentlyContinue)) {
        throw "Get-MSAlwaysOnHealth (MSTools) is required."
    }
    if (-not (Get-Command Invoke-DbaQuery -ErrorAction SilentlyContinue)) {
        throw "Invoke-DbaQuery (dbatools) is required. Run: Import-Module dbatools"
    }

    Write-MSLog -Level Info -Message "New-MSAlwaysOnHealthReport: START. Instance=[$SqlInstance]"

    # Identity the report runs under: the SQL credential when supplied, otherwise
    # the current Windows user; plus the WMI identity when -UseWmi is used.
    $runAsUser = if ($Credential) { [string]$Credential.UserName } else { [System.Security.Principal.WindowsIdentity]::GetCurrent().Name }
    $runAsWmi  = if ($UseWmi) { if ($WmiCredential) { [string]$WmiCredential.UserName } else { "$runAsUser (current Windows user)" } } else { '' }
    Write-MSLog -Level Info -Message ("New-MSAlwaysOnHealthReport: running as [{0}]{1}." -f $runAsUser, $(if ($runAsWmi) { "; WMI as [$runAsWmi]" } else { '' }))

    # Base dbatools connection params (credential threaded through when supplied).
    $sqlBase = @{ SqlInstance = $SqlInstance }
    if ($Credential) { $sqlBase['SqlCredential'] = $Credential }

    # --- Run Get-MSAlwaysOnHealth, forwarding every matching bound parameter --
    $healthParams = @{ SqlInstance = $SqlInstance }
    if ($Credential) { $healthParams['SqlCredential'] = $Credential }

    $agCmd = Get-Command Get-MSAlwaysOnHealth
    foreach ($p in $agCmd.Parameters.Keys) {
        if ($p -in @('SqlInstance', 'SqlCredential')) { continue }
        if ($PSBoundParameters.ContainsKey($p)) { $healthParams[$p] = $PSBoundParameters[$p] }
    }

    # Factory for an extra "Full health detail" row that carries a raw diagnostic
    # message (a captured warning/error, or a skipped-check reason) so the raw text
    # is never hidden. Same column set as Get-MSAlwaysOnHealth rows.
    $newDiagRow = {
        param([string]$Detail, [string]$DiagNode = $SqlInstance, [string]$Status = 'ERROR', [string]$Name = '')
        [pscustomobject][ordered]@{
            Level = 'Diagnostic'; AvailabilityGroup = ''; PrimaryReplica = ''; Replica = ''; Role = ''
            AvailabilityMode = ''; FailoverMode = ''; ConnectionState = ''; OperationalState = ''
            RecoveryHealth = ''; SyncHealth = ''; DatabaseName = ''; SyncState = ''; IsSuspended = ''
            SuspendReason = ''; LogSendQueueKB = $null; RedoQueueKB = $null; JoinState = ''
            LastConnectErrorNo = $null; LastConnectError = ''; LastConnectErrorTime = $null
            Node = $DiagNode; Ping = ''; Port = $null; PortOpen = $Status; ServiceState = ''; FirewallAllowed = ''
            IsHadrEnabled = ''; EndpointName = $Name; EndpointState = ''; EndpointConnAuth = ''
            RecentErrorLog = ''; Detail = $Detail; Remediation = ''
        }
    }

    Write-MSLog -Level Info -Message "New-MSAlwaysOnHealthReport: reading Always On health..."
    $healthWarnings = $null
    $healthErrors   = $null
    $health = @(Get-MSAlwaysOnHealth @healthParams -WarningVariable healthWarnings -ErrorVariable healthErrors -ErrorAction SilentlyContinue)

    # Get-MSAlwaysOnHealth reports a failed connection / permission error via
    # Write-Warning (and sometimes the error stream) and then returns nothing, so
    # the raw message would otherwise be lost and the report would only say a check
    # "returned no rows". Capture both streams and surface each raw message as a row
    # in the Full health detail.
    $healthDiagMessages = @(
        @($healthWarnings | ForEach-Object { [string]$_.Message }) +
        @($healthErrors   | ForEach-Object { [string]$_.Exception.Message })
    ) | Where-Object { $_ } | Select-Object -Unique
    if (@($healthDiagMessages).Count -gt 0) {
        $health = @($health) + @($healthDiagMessages | ForEach-Object { & $newDiagRow $_ })
    }

    # --- Current primary replica ---------------------------------------------
    $primaryServer = @(
        $health |
            Where-Object { $_.Level -eq 'Replica' -and $_.PrimaryReplica } |
            Select-Object -First 1 -ExpandProperty PrimaryReplica
    )[0]
    if (-not $primaryServer) {
        if (Get-Command Get-MSPrimaryServerName -ErrorAction SilentlyContinue) {
            try   { $primaryServer = Get-MSPrimaryServerName -SqlInstance $SqlInstance -SqlCredential $Credential }
            catch { $primaryServer = $SqlInstance }
        }
        else { $primaryServer = $SqlInstance }
    }

    # =====================================================================
    # SQL Agent job comparison: primary vs every secondary (AG only).
    # Nothing to compare on a standalone. Differences are WARN, never RED.
    # =====================================================================
    if ($CheckAgentJob) {
        $replicaNames = @(
            $health |
                Where-Object { $_.Level -eq 'Replica' } |
                ForEach-Object { [string]$_.Replica } |
                Where-Object { $_ } |
                Select-Object -Unique
        )
        $jobSecondaries = @($replicaNames | Where-Object { $_ -and ($_ -ine $primaryServer) })
        $jobAgName = @(
            $health | Where-Object { $_.Level -eq 'Replica' } |
                Select-Object -First 1 -ExpandProperty AvailabilityGroup
        )[0]

        $newJobRow = {
            param([string]$Replica, [string]$Status, [string]$Detail, [string]$Remediation)
            [pscustomobject]@{
                Level = 'AgentJob'; AvailabilityGroup = $jobAgName
                Replica = $Replica; Node = $Replica; DatabaseName = ''
                EndpointName = ''; EndpointState = ''; ConnectionState = ''; SyncHealth = ''
                PortOpen = $Status; Detail = $Detail; Remediation = $Remediation
            }
        }

        if (-not (Get-Command Compare-MSAgentJob -ErrorAction SilentlyContinue)) {
            Write-MSLog -Level Warning -Message "New-MSAlwaysOnHealthReport: Compare-MSAgentJob not found; skipping agent job comparison."
        }
        elseif ($jobSecondaries.Count -eq 0) {
            Write-MSLog -Level Info -Message "New-MSAlwaysOnHealthReport: standalone or single replica - skipping agent job comparison."
        }
        else {
            foreach ($secondary in $jobSecondaries) {
                Write-MSLog -Level Info -Message "New-MSAlwaysOnHealthReport: comparing Agent jobs [$primaryServer] -> [$secondary]..."
                try {
                    $jobDiffs = @(Compare-MSAgentJob -Source $primaryServer -Destination $secondary -IgnoreEnabled -CaseInsensitive)
                }
                catch {
                    $health += & $newJobRow $secondary 'WARN' "Agent job comparison failed for [$secondary]: $($_.Exception.Message)" ''
                    continue
                }

                $jobProblems = @($jobDiffs | Where-Object { $_.Status -in @('Different', 'OnlyOnSource', 'OnlyOnDestination') })

                if ($jobProblems.Count -eq 0) {
                    $health += & $newJobRow $secondary 'OK' "All SQL Agent jobs match between [$primaryServer] and [$secondary] (Enabled ignored, case-insensitive)." ''
                    continue
                }

                foreach ($jp in $jobProblems) {
                    $diffText = if ($jp.Differences) { @($jp.Differences) -join ', ' } else { '' }
                    switch ($jp.Status) {
                        'OnlyOnSource' {
                            $jobDetail = "Job [$($jp.JobName)] exists on primary [$primaryServer] but is missing on [$secondary]."
                            $jobFix    = "Copy-DbaAgentJob -Source '$primaryServer' -Destination '$secondary' -Job '$($jp.JobName)'"
                        }
                        'OnlyOnDestination' {
                            $jobDetail = "Job [$($jp.JobName)] exists only on [$secondary] (not on primary [$primaryServer])."
                            $jobFix    = "Review job [$($jp.JobName)] on [$secondary]; remove it there or create it on [$primaryServer]."
                        }
                        default {
                            $jobDetail = "Job [$($jp.JobName)] differs between [$primaryServer] and [$secondary]$(if ($diffText) { ": $diffText" })."
                            $jobFix    = "Copy-DbaAgentJob -Source '$primaryServer' -Destination '$secondary' -Job '$($jp.JobName)' -Force   # review differences first"
                        }
                    }
                    $health += & $newJobRow $secondary 'WARN' $jobDetail $jobFix
                }
            }
        }
    }

    # =====================================================================
    # [master] stored-procedure comparison: primary vs every secondary (AG only).
    # A missing or differing procedure is WARN, never RED, and named.
    # =====================================================================
    if ($CheckMasterProcedure) {
        $procReplicaNames = @(
            $health |
                Where-Object { $_.Level -eq 'Replica' } |
                ForEach-Object { [string]$_.Replica } |
                Where-Object { $_ } |
                Select-Object -Unique
        )
        $procSecondaries = @($procReplicaNames | Where-Object { $_ -and ($_ -ine $primaryServer) })
        $procAgName = @(
            $health | Where-Object { $_.Level -eq 'Replica' } |
                Select-Object -First 1 -ExpandProperty AvailabilityGroup
        )[0]

        $replicaCred = @{}
        if ($Credential) { $replicaCred['SqlCredential'] = $Credential }

        $newProcRow = {
            param([string]$Replica, [string]$Status, [string]$Detail, [string]$Remediation)
            [pscustomobject]@{
                Level = 'MasterProcedure'; AvailabilityGroup = $procAgName
                Replica = $Replica; Node = $Replica; DatabaseName = 'master'
                EndpointName = ''; EndpointState = ''; ConnectionState = ''; SyncHealth = ''
                PortOpen = $Status; Detail = $Detail; Remediation = $Remediation
            }
        }

        # Line-ending / trailing-whitespace normalization so only real code changes count.
        $normalizeProc = {
            param([string]$Text)
            if ([string]::IsNullOrWhiteSpace($Text)) { return '' }
            ((($Text -replace "`r`n", "`n") -replace "`r", "`n")).Trim()
        }

        $procQuery = @"
SELECT ProcName   = QUOTENAME(s.name) + N'.' + QUOTENAME(p.name),
       Definition = OBJECT_DEFINITION(p.object_id)
FROM sys.procedures AS p WITH (NOLOCK)
JOIN sys.schemas AS s WITH (NOLOCK) ON s.schema_id = p.schema_id
WHERE p.is_ms_shipped = 0
ORDER BY ProcName;
"@

        if ($procSecondaries.Count -eq 0) {
            Write-MSLog -Level Info -Message "New-MSAlwaysOnHealthReport: standalone or single replica - skipping master procedure comparison."
        }
        else {
            $primaryProcs = @{}
            $primaryProcOk = $true
            try {
                foreach ($r in @(Invoke-DbaQuery -SqlInstance $primaryServer -Database master -Query $procQuery @replicaCred -ErrorAction Stop)) {
                    $primaryProcs[[string]$r.ProcName] = (& $normalizeProc ([string]$r.Definition))
                }
            }
            catch {
                $primaryProcOk = $false
                $health += & $newProcRow $primaryServer 'WARN' "Could not read [master] stored procedures on primary [$primaryServer]: $($_.Exception.Message)" ''
            }

            if ($primaryProcOk) {
                foreach ($secondary in $procSecondaries) {
                    Write-MSLog -Level Info -Message "New-MSAlwaysOnHealthReport: comparing [master] procedures [$primaryServer] -> [$secondary]..."
                    try {
                        $dstRows = @(Invoke-DbaQuery -SqlInstance $secondary -Database master -Query $procQuery @replicaCred -ErrorAction Stop)
                    }
                    catch {
                        $health += & $newProcRow $secondary 'WARN' "Could not read [master] stored procedures on [$secondary]: $($_.Exception.Message)" ''
                        continue
                    }

                    $secondaryProcs = @{}
                    foreach ($r in $dstRows) { $secondaryProcs[[string]$r.ProcName] = (& $normalizeProc ([string]$r.Definition)) }

                    $allProcNames = @(@($primaryProcs.Keys) + @($secondaryProcs.Keys) | Sort-Object -Unique)
                    $procIssues = 0

                    foreach ($procName in $allProcNames) {
                        $inPrimary = $primaryProcs.ContainsKey($procName)
                        $inSecondary = $secondaryProcs.ContainsKey($procName)

                        if ($inPrimary -and -not $inSecondary) {
                            $procIssues++
                            $health += & $newProcRow $secondary 'WARN' "Stored procedure [$procName] exists in [master] on primary [$primaryServer] but is missing on [$secondary]." "Script [$procName] from [$primaryServer].[master] and create it on [$secondary].[master]."
                        }
                        elseif ($inSecondary -and -not $inPrimary) {
                            $procIssues++
                            $health += & $newProcRow $secondary 'WARN' "Stored procedure [$procName] exists in [master] on [$secondary] but not on primary [$primaryServer]." "Review [$procName] on [$secondary].[master]; drop it there or create it on [$primaryServer]."
                        }
                        elseif ($primaryProcs[$procName] -ine $secondaryProcs[$procName]) {
                            $procIssues++
                            $health += & $newProcRow $secondary 'WARN' "Stored procedure [$procName] in [master] differs between [$primaryServer] and [$secondary]." "Re-deploy [$procName] on [$secondary].[master] from [$primaryServer].[master]."
                        }
                    }

                    if ($procIssues -eq 0) {
                        $health += & $newProcRow $secondary 'OK' "All [master] stored procedures match between [$primaryServer] and [$secondary]." ''
                    }
                }
            }
        }
    }

    # =====================================================================
    # Extra SPACE data (drives / files near their limits) for the summary
    # and the detected-problems section.
    # =====================================================================
    $NearLimitPct    = 80    # warn when a file is >= this % of its MaxSize limit
    $LowDriveFreePct = 15    # warn when the volume has < this % free
    $LowDriveFreeGB  = 10    # ...or less than this many GB free

    $spaceQuery = @"
SELECT
    DatabaseName  = DB_NAME(mf.database_id),
    FileType      = mf.type_desc,
    LogicalName   = mf.name,
    Drive         = LEFT(vs.volume_mount_point, 3),
    FileSizeMB    = CAST(mf.size / 128.0 AS DECIMAL(18,1)),
    MaxSizeMB     = CASE
                        WHEN mf.max_size IN (-1, 268435456) THEN NULL      -- unlimited
                        WHEN mf.max_size = 0 THEN 0                        -- no growth
                        ELSE CAST(mf.max_size / 128.0 AS DECIMAL(18,1))
                    END,
    PctOfMaxSize  = CASE
                        WHEN mf.max_size > 0 AND mf.max_size <> 268435456
                        THEN CAST(100.0 * mf.size / mf.max_size AS DECIMAL(5,1))
                        ELSE NULL
                    END,
    Growth        = CASE
                        WHEN mf.growth = 0 THEN 'NO GROWTH'
                        WHEN mf.is_percent_growth = 1 THEN CONVERT(varchar(10), mf.growth) + '%'
                        ELSE CONVERT(varchar(20), CAST(mf.growth / 128.0 AS DECIMAL(18,1))) + ' MB'
                    END,
    DriveFreeGB   = CAST(vs.available_bytes / 1073741824.0 AS DECIMAL(18,1)),
    DriveTotalGB  = CAST(vs.total_bytes / 1073741824.0 AS DECIMAL(18,1)),
    DriveFreePct  = CAST(100.0 * vs.available_bytes / NULLIF(vs.total_bytes, 0) AS DECIMAL(5,1))
FROM sys.master_files AS mf WITH (NOLOCK)
CROSS APPLY sys.dm_os_volume_stats(mf.database_id, mf.file_id) AS vs
WHERE mf.database_id > 4          -- skip system databases (optional)
ORDER BY DatabaseName, FileType;
"@

    try {
        $space = @(Invoke-DbaQuery @sqlBase -Query $spaceQuery -ErrorAction Stop)
    }
    catch {
        Write-MSLog -Level Warning -Message "New-MSAlwaysOnHealthReport: could not read space information - $($_.Exception.Message)"
        $space = @()
    }

    # Result sets reused by both the summary and the detected problems.
    $notHealthy = $health | Where-Object { $_.SyncHealth -and $_.SyncHealth -ne 'HEALTHY' }
    $nearLimit  = $space  | Where-Object { ($_.PctOfMaxSize -as [double]) -ge $NearLimitPct }
    $lowDrives  = $space  |
        Where-Object {
            ($_.DriveFreePct -as [double]) -lt $LowDriveFreePct -or
            ($_.DriveFreeGB  -as [double]) -lt $LowDriveFreeGB
        } |
        Sort-Object Drive -Unique

    # --- Row groups by Level --------------------------------------------------
    $replicaRows = @($health | Where-Object Level -eq 'Replica')
    $dbRows      = @($health | Where-Object Level -eq 'Database')
    $connRows    = @($health | Where-Object Level -eq 'Connectivity')
    $epRows      = @($health | Where-Object Level -eq 'EndpointDetail')
    $secRows     = @($health | Where-Object Level -eq 'ServiceAccount')
    $listRows    = @($health | Where-Object Level -eq 'Listener')
    $cluRows     = @($health | Where-Object { $_.Level -eq 'Cluster' -or $_.Level -eq 'ClusterMember' })
    $froRows     = @($health | Where-Object Level -eq 'FailoverReadiness')
    $bkpRows     = @($health | Where-Object Level -eq 'Backup')
    $cfgRows     = @($health | Where-Object Level -eq 'Configuration')
    $lgnRows     = @($health | Where-Object Level -eq 'LoginParity')
    $jobRows     = @($health | Where-Object Level -eq 'AgentJob')
    $procRows    = @($health | Where-Object Level -eq 'MasterProcedure')
    $verRows     = @($health | Where-Object Level -eq 'VersionParity')
    $cluEvtRows  = @($health | Where-Object Level -eq 'ClusterEvent')
    $lsnEvtRows  = @($health | Where-Object Level -eq 'ListenerEvent')
    $connSmokeRows = @($health | Where-Object Level -eq 'Connection')
    $dbStateRows   = @($health | Where-Object Level -eq 'DatabaseState')
    $sysDbRows     = @($health | Where-Object Level -eq 'SystemDatabase')
    $ioRows        = @($health | Where-Object Level -eq 'IoError')
    $skewRows      = @($health | Where-Object Level -eq 'ClockSkew')
    $certRows      = @($health | Where-Object Level -eq 'Certificate')
    $rebootRows    = @($health | Where-Object Level -eq 'PendingReboot')
    $loginFailRows = @($health | Where-Object Level -eq 'LoginFailed')

    $resolveStatus = {
        param($rows, $failed, $warn)
        if (-not $rows -or @($rows).Count -eq 0)   { return @{ Text = 'skipped'; Color = 'DarkGray' } }
        if ($failed -and @($failed).Count -gt 0)   { return @{ Text = 'FAILED';  Color = 'Red' } }
        if ($warn   -and @($warn).Count   -gt 0)   { return @{ Text = 'WARN';    Color = 'Yellow' } }
        if (@($rows | Where-Object { "$($_.PortOpen)" -ne 'SKIPPED' }).Count -eq 0) { return @{ Text = 'skipped'; Color = 'DarkGray' } }
        return @{ Text = 'ok'; Color = 'Green' }
    }

    # For the switch-driven sections the row status token lives in PortOpen.
    $statusFailed = { param($rows) @($rows | Where-Object { "$($_.PortOpen)" -in @('ERROR', 'FAILED') }) }
    $statusWarn   = { param($rows) @($rows | Where-Object { "$($_.PortOpen)" -in @('CHECK', 'WARN') }) }

    $summary = [ordered]@{
        'Server (replicas)'      = & $resolveStatus $replicaRows ($replicaRows | Where-Object { $_.ConnectionState -ne 'CONNECTED' -or $_.SyncHealth -ne 'HEALTHY' }) $null
        'Database sync'          = & $resolveStatus $dbRows      $notHealthy $null
        'Connectivity (WMI)'     = & $resolveStatus $connRows    ($connRows | Where-Object { $_.Ping -ne 'ok' -or $_.PortOpen -notmatch 'open' }) $null
        'Endpoints'              = & $resolveStatus $epRows      ($epRows | Where-Object { $_.EndpointState -and $_.EndpointState -notmatch 'STARTED' }) $null
        'ServiceAccount'         = & $resolveStatus $secRows     ($secRows | Where-Object { $_.PortOpen -eq 'ERROR' }) ($secRows | Where-Object { $_.PortOpen -eq 'CHECK' })
        'Listener'               = & $resolveStatus $listRows    (& $statusFailed $listRows) (& $statusWarn $listRows)
        'Cluster / quorum'       = & $resolveStatus $cluRows     (& $statusFailed $cluRows)  (& $statusWarn $cluRows)
        'Failover readiness'     = & $resolveStatus $froRows     (& $statusFailed $froRows)  (& $statusWarn $froRows)
        'Backups / log-reuse'    = & $resolveStatus $bkpRows     (& $statusFailed $bkpRows)  (& $statusWarn $bkpRows)
        'Configuration'          = & $resolveStatus $cfgRows     (& $statusFailed $cfgRows)  (& $statusWarn $cfgRows)
        'Logins (parity)'        = & $resolveStatus $lgnRows     (& $statusFailed $lgnRows)  (& $statusWarn $lgnRows)
        'Agent jobs (AG)'        = & $resolveStatus $jobRows     $null                       (& $statusWarn $jobRows)
        'Master procedures'      = & $resolveStatus $procRows    $null                       (& $statusWarn $procRows)
        'SQL build (version)'    = & $resolveStatus $verRows     (& $statusFailed $verRows)    (& $statusWarn $verRows)
        'Cluster events (Win)'   = & $resolveStatus $cluEvtRows  (& $statusFailed $cluEvtRows) (& $statusWarn $cluEvtRows)
        'Listener events (Win)'  = & $resolveStatus $lsnEvtRows  (& $statusFailed $lsnEvtRows) (& $statusWarn $lsnEvtRows)
        'Connection (smoke)'     = & $resolveStatus $connSmokeRows (& $statusFailed $connSmokeRows) (& $statusWarn $connSmokeRows)
        'Database state'         = & $resolveStatus $dbStateRows   (& $statusFailed $dbStateRows)   (& $statusWarn $dbStateRows)
        'System databases'       = & $resolveStatus $sysDbRows     (& $statusFailed $sysDbRows)     (& $statusWarn $sysDbRows)
        'Corruption / I/O'       = & $resolveStatus $ioRows        (& $statusFailed $ioRows)        (& $statusWarn $ioRows)
        'Clock skew'             = & $resolveStatus $skewRows      (& $statusFailed $skewRows)      (& $statusWarn $skewRows)
        'Certificates'           = & $resolveStatus $certRows      (& $statusFailed $certRows)      (& $statusWarn $certRows)
        'Pending reboot'         = & $resolveStatus $rebootRows    (& $statusFailed $rebootRows)    (& $statusWarn $rebootRows)
        'Failed logins'          = & $resolveStatus $loginFailRows (& $statusFailed $loginFailRows) (& $statusWarn $loginFailRows)
        'Space (data/log)'       = & $resolveStatus $space       $lowDrives  $nearLimit
    }

    # =====================================================================
    # Some checks must not be silently 'skipped'. When they are, flag them as a
    # problem (WARN + a Detected-problems entry with the reason and a fix) but
    # do NOT add a new status-summary category.
    # =====================================================================
    $mustNotSkip = [ordered]@{
        'Connection (smoke)'  = @{ Rows = $connSmokeRows; Switch = 'CheckConnection' }
        'Database state'      = @{ Rows = $dbStateRows;   Switch = 'CheckDatabaseState' }
        'System databases'    = @{ Rows = $sysDbRows;     Switch = 'CheckSystemDatabase' }
        'Corruption / I/O'    = @{ Rows = $ioRows;        Switch = 'CheckIoError' }
        'Clock skew'          = @{ Rows = $skewRows;      Switch = 'CheckClockSkew' }
        'Certificates'        = @{ Rows = $certRows;      Switch = 'CheckCertificate' }
        'Pending reboot'      = @{ Rows = $rebootRows;    Switch = 'CheckPendingReboot' }
        'Failed logins'       = @{ Rows = $loginFailRows; Switch = 'CheckLoginFailed' }
        'Space (data/log)'    = @{ Rows = $space;         Switch = $null }
        'Backups / log-reuse' = @{ Rows = $bkpRows;       Switch = 'CheckBackup' }
    }

    $skipProblems = @()
    $skipDetailRows = @()
    foreach ($cat in $mustNotSkip.Keys) {
        if (-not $summary.Contains($cat)) { continue }
        if ([string]$summary[$cat].Text -ne 'skipped') { continue }

        $info = $mustNotSkip[$cat]
        $catRows = @($info.Rows)

        $rawErrorText = ''
        if ($catRows.Count -eq 0) {
            if ($info.Switch -and -not $PSBoundParameters.ContainsKey($info.Switch)) {
                $reason = "the -$($info.Switch) check was not requested, so it did not run."
                $fix    = "Add -$($info.Switch) to the New-MSAlwaysOnHealthReport call."
            }
            else {
                $reason = 'the check ran but returned no rows (a node may be unreachable or the account lacks permission).'
                if (@($healthDiagMessages).Count -gt 0) {
                    $rawErrorText = ($healthDiagMessages -join [Environment]::NewLine)
                    $reason += [Environment]::NewLine + 'Raw error(s):' + [Environment]::NewLine + $rawErrorText
                }
                $fix    = 'Confirm every replica/node is reachable and the account has the required permission (e.g. VIEW SERVER STATE / securityadmin).'

                # Guarantee the raw reason (and any captured error) shows up in the
                # Full health detail, tied to this specific category.
                $detailText = if ($rawErrorText) { "$cat skipped - returned no rows. Raw error(s): $rawErrorText" }
                              else { "$cat skipped - the check ran but returned no rows (node unreachable or missing permission such as VIEW SERVER STATE / securityadmin)." }
                $skipDetailRows += & $newDiagRow $detailText $SqlInstance 'ERROR' $cat
            }
        }
        else {
            # Unfiltered: every row for this check with node, status and the FULL
            # Detail (including any raw error message) - nothing deduped or dropped.
            $raw = @($catRows | ForEach-Object {
                $node       = @($_.Node, $_.Replica | Where-Object { $_ })[0]
                $prefix     = if ($node) { "[$node] " } else { '' }
                $detailText = [string]$_.Detail
                if ([string]::IsNullOrWhiteSpace($detailText)) { $detailText = '(no detail)' }
                $rem = if ($_.PSObject.Properties['Remediation']) { [string]$_.Remediation } else { '' }
                if ($rem) { $detailText = "$detailText  [Remediation: $rem]" }
                "$prefix($($_.PortOpen)) $detailText"
            }) -join [Environment]::NewLine
            $reason = $raw
            $fix = switch -Regex ($raw) {
                'timeout'                                  { 'Increase -ConnectTimeoutSeconds and/or -SecondaryDetailTimeoutSeconds; a slow or busy replica is timing out.'; break }
                'unreachable|could not connect|connect:'   { 'Verify network / WinRM / SQL connectivity to the node and that the account can connect.'; break }
                'permission|securityadmin|sysadmin|denied|VIEW SERVER STATE' { 'Grant the required permission (e.g. securityadmin / VIEW SERVER STATE) to the account running the report.'; break }
                default                                    { 'Review the reason(s) above; ensure the check ran on a reachable node with sufficient permissions.' }
            }
        }

        # Surface it as a problem in the summary without creating a new category.
        $summary[$cat].Text  = 'WARN'
        $summary[$cat].Color = 'Yellow'

        $skipProblems += [pscustomobject]@{
            Problem       = "Skipped check: $cat"
            Object        = $cat
            Issue         = "Expected to run but was skipped:" + [Environment]::NewLine + $reason
            FixSuggestion = $fix
        }
    }

    # Add the skipped-check diagnostic rows to the health rows so each one appears
    # in the Full health detail (never dropped by -HideSkipped: they are ERROR).
    if (@($skipDetailRows).Count -gt 0) {
        $health = @($health) + @($skipDetailRows)
    }

    # --- Status summary as an array of objects -------------------------------
    $statusSummaryArray = foreach ($item in $summary.GetEnumerator()) {
        [pscustomobject]@{
            Category = [string]$item.Key
            Status   = [string]$item.Value.Text
        }
    }

    # =====================================================================
    # HTML building
    # =====================================================================

    # --- Ready-to-use fix for a single health row (blank when healthy) -------
    $getFix = {
        param($r)
        switch ($r.Level) {
            'Database' {
                if ($r.IsSuspended -eq $true -or ($r.SyncHealth -and $r.SyncHealth -ne 'HEALTHY')) {
                    return "ALTER DATABASE [$($r.DatabaseName)] SET HADR RESUME;  -- run on replica $($r.Replica)"
                }
            }
            'Replica' {
                if ($r.ConnectionState -ne 'CONNECTED' -or ($r.SyncHealth -and $r.SyncHealth -ne 'HEALTHY')) {
                    return "Test-DbaConnection -SqlInstance '$($r.Replica)';  -- then on each affected DB: ALTER DATABASE [<db>] SET HADR RESUME;"
                }
            }
            'Connectivity' {
                $fixes = @()
                if ($r.Ping -ne 'ok')             { $fixes += "Test-Connection -ComputerName '$($r.Node)' -Count 2   # host down / DNS?" }
                if ($r.PortOpen -notmatch 'open') { $fixes += "New-NetFirewallRule -DisplayName 'SQL HADR $($r.Port)' -Direction Inbound -Protocol TCP -LocalPort $($r.Port) -Action Allow   # run on $($r.Node)" }
                if ($r.ServiceState -match 'Stop'){ $fixes += "Get-Service -ComputerName '$($r.Node)' -Name 'MSSQL*' | Start-Service" }
                if ($fixes) { return ($fixes -join '  ||  ') }
            }
            'EndpointDetail' {
                if ("$($r.IsHadrEnabled)" -eq '0') {
                    return "Enable-DbaAgHadr -SqlInstance '$($r.Replica)' -Force   # enables HADR (restarts the SQL service)"
                }
                if ($r.EndpointState -and $r.EndpointState -notmatch 'STARTED' -and $r.EndpointName) {
                    return "ALTER ENDPOINT [$($r.EndpointName)] STATE = STARTED;  -- run on $($r.Replica)"
                }
            }
            'ServiceAccount' {
                # Exact per-row fixes are provided by Get-MSAlwaysOnHealth in Remediation.
                if ($r.PSObject.Properties['Remediation'] -and $r.Remediation) {
                    return [string]$r.Remediation
                }
            }
        }
        return ''
    }

    # --- Short help shown next to each status-summary category ---------------
    $categoryHelp = @{
        'Server (replicas)'      = 'Each AG replica is CONNECTED and its synchronization health is HEALTHY.'
        'Database sync'          = 'Per-database Always On data movement (not suspended, sync health HEALTHY).'
        'Connectivity (WMI)'     = 'Ping, endpoint / SQL TCP port, SQL service state and firewall on each node.'
        'Endpoints'              = 'The Always On (database mirroring) endpoint exists and is STARTED.'
        'ServiceAccount'         = 'Replica service accounts have CONNECT on each peer''s endpoint; the endpoint and availability group are owned by [sa].'
        'Listener'               = 'The AG listener exists, is online and answers on its TCP port.'
        'Cluster / quorum'       = 'WSFC cluster is up and quorum / node votes are configured correctly.'
        'Failover readiness'     = 'A safe automatic failover is possible (SYNCHRONIZED + AUTOMATIC).'
        'Backups / log-reuse'    = 'Recent full / log backups exist and nothing blocks transaction-log reuse.'
        'Configuration'          = 'Snapshot of AG / replica settings (availability mode, failover mode).'
        'Logins (parity)'        = 'Logins exist on every replica with matching SIDs (no orphans after failover).'
        'Agent jobs (AG)'        = 'SQL Agent jobs match between the primary and each secondary (Enabled ignored, case-insensitive).'
        'Master procedures'      = '[master] user stored procedures match (same code, none missing) between the primary and each secondary.'
        'SQL build (version)'    = 'All replicas run the same SQL Server build / patch level.'
        'Cluster events (Win)'   = 'Recent Failover Clustering errors in the Windows event log.'
        'Listener events (Win)'  = 'Recent listener / network-name / DNS errors in the Windows event log.'
        'Connection (smoke)'     = 'A basic login and test query succeed against the instance.'
        'Database state'         = 'User databases are ONLINE (not SUSPECT / RECOVERY_PENDING / EMERGENCY / OFFLINE).'
        'System databases'       = 'master, model, msdb and tempdb are online and healthy.'
        'Corruption / I/O'       = 'suspect_pages and 823 / 824 / 825 I/O errors in the error log.'
        'Clock skew'             = 'Time difference between nodes stays small (Kerberos / AG heartbeat).'
        'Certificates'           = 'Endpoint / TLS certificates are present and not expiring soon.'
        'Pending reboot'         = 'Windows is not waiting on a pending reboot after patching.'
        'Failed logins'          = 'Recent failed login attempts recorded in the SQL error log.'
        'Space (data/log)'       = 'Data / log files are not near MaxSize and volumes have free space.'
    }

    # --- Status summary rows (colored) ---------------------------------------
    $statusColor = @{ 'ok' = '#2e7d32'; 'WARN' = '#f9a825'; 'FAILED' = '#c62828'; 'skipped' = '#9e9e9e' }
    $summaryRowsHtml = foreach ($item in $summary.GetEnumerator()) {
        $t = [string]$item.Value.Text
        if ($HideSkipped -and $t -eq 'skipped') { continue }
        $c = $statusColor[$t]; if (-not $c) { $c = '#9e9e9e' }
        $help = [string]$categoryHelp[[string]$item.Key]
        "<tr><td>$($item.Key)</td><td style=`"color:#fff;background:$c;font-weight:bold;text-align:center`">$t</td><td style=`"color:#555`">$help</td></tr>"
    }
    $summaryTable = "<table><tr><th>Category</th><th>Status</th><th>What it checks</th></tr>$(($summaryRowsHtml) -join '')</table>"

    # --- Detected problems + ready-to-use fixes (health + space) -------------
    $healthProblems = $health | ForEach-Object {
        $fix = if ($_.PSObject.Properties['Remediation'] -and $_.Remediation) { [string]$_.Remediation } else { & $getFix $_ }
        $bad = "$($_.PortOpen)" -in @('CHECK', 'WARN', 'FAILED', 'ERROR')
        if ($fix -or $bad) {
            [pscustomobject]@{
                Problem       = $_.Level
                Object        = @($_.Replica, $_.Node, $_.DatabaseName, $_.EndpointName | Where-Object { $_ })[0]
                Issue         = (@($_.Detail, $_.ConnectionState, $_.SyncHealth, $_.EndpointState) | Where-Object { $_ }) -join ' / '
                FixSuggestion = $fix
            }
        }
    }

    $spaceProblems = @()
    $spaceProblems += $nearLimit | ForEach-Object {
        $newMaxMB = [int]([math]::Ceiling(([double]$_.MaxSizeMB) * 1.5 / 1024.0) * 1024)
        [pscustomobject]@{
            Problem       = 'File near MaxSize limit'
            Object        = "$($_.DatabaseName) / $($_.LogicalName) ($($_.FileType))"
            Issue         = "$($_.FileSizeMB) MB of $($_.MaxSizeMB) MB ($($_.PctOfMaxSize)%)"
            FixSuggestion = "ALTER DATABASE [$($_.DatabaseName)] MODIFY FILE (NAME = N'$($_.LogicalName)', MAXSIZE = ${newMaxMB}MB); -- or MAXSIZE = UNLIMITED"
        }
    }
    $spaceProblems += $lowDrives | ForEach-Object {
        [pscustomobject]@{
            Problem       = 'Low disk space'
            Object        = "Drive $($_.Drive)"
            Issue         = "$($_.DriveFreeGB) GB free of $($_.DriveTotalGB) GB ($($_.DriveFreePct)%)"
            FixSuggestion = "Get-DbaDbFile -SqlInstance '$SqlInstance' | Where-Object PhysicalName -like '$($_.Drive)*' | Sort-Object Size -Descending   # then: expand disk / move a file / shrink log after a LOG backup"
        }
    }

    $allProblems = @($healthProblems) + @($spaceProblems) + @($skipProblems)
    $problemsHtml = if ($allProblems.Count -gt 0) {
        $allProblems | ConvertTo-Html -As Table -Fragment -Property Problem, Object, Issue, FixSuggestion
    }
    else {
        '<p style="color:#2e7d32"><b>No problems detected - nothing to fix.</b></p>'
    }

    # --- Full health detail, with the FixSuggestion column -------------------
    $healthDetail = $health | Select-Object *, @{ Name = 'FixSuggestion'; Expression = { & $getFix $_ } }
    $detailForHtml = if ($HideSkipped) { $healthDetail | Where-Object { "$($_.PortOpen)" -ne 'SKIPPED' } } else { $healthDetail }
    $detailHtml   = $detailForHtml | ConvertTo-Html -As Table -Fragment

    # --- Configuration snapshot ----------------------------------------------
    $configHtml = if ($cfgRows.Count -gt 0) {
        $cfgRows | ConvertTo-Html -As Table -Fragment -Property AvailabilityGroup, Replica, AvailabilityMode, FailoverMode, PortOpen, Detail
    }
    else {
        '<p><i>Configuration snapshot not collected (run with -IncludeConfiguration).</i></p>'
    }

    # --- Assemble the document -----------------------------------------------
    $css = @"
<style>
body { font-family: 'Segoe UI', Arial, sans-serif; font-size: 13px; }
h1 { font-size: 20px; }
h2 { font-size: 16px; margin-top: 24px; }
table { border-collapse: collapse; width: 100%; margin-bottom: 12px; }
th, td { border: 1px solid #ccc; padding: 6px; vertical-align: top; text-align: left; }
th { background-color: #f2f2f2; }
tr:nth-child(even) { background-color: #fafafa; }
</style>
"@

    $doc = @"
<html>
<head>$css</head>
<body>
<h1>Always On health report</h1>
<p>Generated $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss') &nbsp;|&nbsp; Instance: $SqlInstance &nbsp;|&nbsp; Primary: $primaryServer</p>
<p>Run as: $runAsUser$(if ($runAsWmi) { " &nbsp;|&nbsp; WMI as: $runAsWmi" })</p>

<h2>Status summary</h2>
$summaryTable

<h2>Always On &amp; cluster configuration</h2>
$configHtml

<h2>Detected problems &amp; ready-to-use fixes</h2>
$problemsHtml

<h2>Full health detail</h2>
$detailHtml
</body>
</html>
"@

    # =====================================================================
    # Resolve a safe, writable output path and write the report.
    # =====================================================================
    $testWritable = {
        param([string]$Dir)
        if ([string]::IsNullOrWhiteSpace($Dir)) { return $false }
        try {
            if (-not (Test-Path -LiteralPath $Dir)) {
                New-Item -ItemType Directory -Path $Dir -Force -ErrorAction Stop | Out-Null
            }
            $probe = Join-Path $Dir ([System.IO.Path]::GetRandomFileName())
            Set-Content -LiteralPath $probe -Value 'x' -ErrorAction Stop
            Remove-Item -LiteralPath $probe -Force -ErrorAction SilentlyContinue
            return $true
        }
        catch { return $false }
    }

    $safeName        = ($SqlInstance -replace '[\\/:*?"<>|]', '_')
    $defaultFileName = "AlwaysOnHealth_${safeName}_$(Get-Date -Format 'yyyyMMdd_HHmmss').html"

    $targetPath = $null
    if (-not [string]::IsNullOrWhiteSpace($OutputFile)) {
        $isDir = (Test-Path -LiteralPath $OutputFile -PathType Container) -or
                 $OutputFile.EndsWith('\') -or $OutputFile.EndsWith('/')
        if ($isDir) {
            $reqDir = $OutputFile.TrimEnd('\', '/')
            if (& $testWritable $reqDir) { $targetPath = Join-Path $reqDir $defaultFileName }
        }
        else {
            $reqDir = Split-Path -Path $OutputFile -Parent
            if ([string]::IsNullOrWhiteSpace($reqDir)) { $reqDir = (Get-Location).Path }
            if (& $testWritable $reqDir) { $targetPath = Join-Path $reqDir (Split-Path -Path $OutputFile -Leaf) }
        }
        if (-not $targetPath) {
            Write-MSLog -Level Warning -Message "New-MSAlwaysOnHealthReport: -OutputFile location is not writable; using a safe fallback folder."
        }
    }

    if (-not $targetPath) {
        foreach ($d in @($env:TEMP, [System.IO.Path]::GetTempPath(), [Environment]::GetFolderPath('LocalApplicationData'), (Get-Location).Path)) {
            if (& $testWritable $d) { $targetPath = Join-Path $d $defaultFileName; break }
        }
    }
    if (-not $targetPath) {
        $targetPath = Join-Path (Get-Location).Path $defaultFileName
    }

    try {
        $doc | Out-File -LiteralPath $targetPath -Encoding utf8 -ErrorAction Stop
        Write-MSLog -Level Info -Message "New-MSAlwaysOnHealthReport: report written to [$targetPath]."
    }
    catch {
        Write-MSLog -Level Error -Message "New-MSAlwaysOnHealthReport: failed to write report to [$targetPath] - $($_.Exception.Message)"
    }

    if ($ShowReport -and (Test-Path -LiteralPath $targetPath)) {
        try { Start-Process $targetPath }
        catch { Write-MSLog -Level Warning -Message "New-MSAlwaysOnHealthReport: could not open the report - $($_.Exception.Message)" }
    }

    Write-MSLog -Level Info -Message "New-MSAlwaysOnHealthReport: DONE. Primary=[$primaryServer], Problems=$($allProblems.Count)."

    # =====================================================================
    # Return the structured report.
    # =====================================================================
    [pscustomobject]@{
        PrimaryServer     = [string]$primaryServer
        RunAsUser         = [string]$runAsUser
        RunAsWmi          = [string]$runAsWmi
        StatusSummary     = @($statusSummaryArray)
        Configuration     = @($cfgRows)
        Problems          = @($allProblems)
        HealthDetail      = @($healthDetail)
        StatusSummaryHtml = $summaryTable
        FullReportHtml    = $doc
        OutputFile        = $targetPath
    }
}
