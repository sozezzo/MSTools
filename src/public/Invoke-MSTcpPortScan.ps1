function Invoke-MSTcpPortScan {
<#
.SYNOPSIS
    Parallel TCP port scanner using runspaces (PowerShell 5.1 compatible).

.DESCRIPTION
    Performs a TCP "connect scan" over a range of ports against a target host/IP,
    using a RunspacePool to scan ports in parallel.

    This is a pure PowerShell implementation, ideal for environments where tools
    like nmap cannot be installed or executed.

    It approximates the behavior of:
        nmap -p 1-65535 -T4 -v <ip>

    Limitations compared to nmap:
        - Only TCP connect scan (no raw SYN, no UDP)
        - No OS detection (-O / -A)
        - No service fingerprinting (-sV)
        - No NSE scripts
        - No traceroute

    Still, it is very useful to quickly discover which TCP ports are open.

.PARAMETER Target
    Hostname or IP address to scan (e.g. "192.0.2.10").

.PARAMETER StartPort
    First port in the range to scan. Default = 1.

.PARAMETER EndPort
    Last port in the range to scan. Default = 65535 (full TCP range).

.PARAMETER TimeoutMs
    Base connection timeout in milliseconds per port.
    Lower = faster, but may miss slow responses.
    Typical values: 50–250ms. Default = 150.

.PARAMETER MaxThreads
    Base maximum number of parallel runspaces (degree of parallelism).
    Increase to use more CPU and network; decrease if the machine is weak.
    Typical values: 100–500. Default = 200.

.PARAMETER ScanMode
    Behavior profile that adjusts Timeout and MaxThreads (unless you override them):

        Custom    : Use TimeoutMs and MaxThreads exactly as provided (default).
        Safe      : Lower concurrency for fragile/unknown targets.
        Stealth   : Very low concurrency + small random delays (minimal noise).
        Aggressive: High concurrency, low timeout (fast but noisy).
        Mixed     : Aggressive when scanning few ports, safer when scanning many.

.OUTPUTS
    [pscustomobject] with:
        - Target : string
        - Port   : int
        - State  : 'Open'

    The function writes "[OPEN] <target>:<port>" to the console as it finds open ports,
    and returns a collection of objects that you can pipe to Export-Csv, Where-Object, etc.

.EXAMPLE
    # Quick scan of "top" ports (1–1024), Safe mode
    Invoke-MSTcpPortScan -Target "192.0.2.10" -StartPort 1 -EndPort 1024 -ScanMode Safe

.EXAMPLE
    # Approximate nmap: nmap -p 1-65535 -T4 -v 192.0.2.10
    Invoke-MSTcpPortScan -Target "192.0.2.10" -StartPort 1 -EndPort 65535 -TimeoutMs 100 -MaxThreads 300 -ScanMode Custom

.EXAMPLE
    # Fast internal scan using Aggressive mode (you manage the servers)
    Invoke-MSTcpPortScan -Target "192.0.2.10" -ScanMode Aggressive

.EXAMPLE
    # Stealthy scan with minimal footprint
    Invoke-MSTcpPortScan -Target "192.0.2.10" -ScanMode Stealth

.EXAMPLE
    # Save results to CSV
    $open = Invoke-MSTcpPortScan -Target "192.0.2.10" -ScanMode Aggressive
    $open | Export-Csv "C:\Temp\192.0.2.10-open-ports.csv" -NoTypeInformation

.NOTES
    Author  : Sozezzo Astra
    Purpose : Lightweight nmap-like TCP scanner when only PowerShell 5.1 is available.
#>
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)]
        [string]$Target,

        [int]$StartPort = 1,
        [int]$EndPort   = 65535,   # Full range by default

        [int]$TimeoutMs  = 150,    # Base connection timeout per port
        [int]$MaxThreads = 200,    # Base degree of parallelism

        [ValidateSet('Custom','Safe','Stealth','Aggressive','Mixed')]
        [string]$ScanMode = 'Custom'
    )

    if ($StartPort -lt 1 -or $EndPort -gt 65535 -or $StartPort -gt $EndPort) {
        throw "Invalid port range. Use 1-65535 and ensure StartPort <= EndPort."
    }

    $ports      = $StartPort..$EndPort
    $totalPorts = $ports.Count

    # Calculate progress batch: ~15 messages, rounded to nearest 500 when large.
    $QueueBatchSize = [math]::Max(1, [math]::Round($totalPorts / 15.0))
    if ($QueueBatchSize -ge 500) {
        $QueueBatchSize = [math]::Round($QueueBatchSize / 500.0) * 500
        if ($QueueBatchSize -lt 1) { $QueueBatchSize = 1 }
    }

    # Effective values that may be adjusted by ScanMode
    $effectiveTimeoutMs  = $TimeoutMs
    $effectiveMaxThreads = $MaxThreads
    $queueDelayBaseMs    = 0   # for Stealth mode

    switch ($ScanMode) {
        'Safe' {
            if ($TimeoutMs  -eq 150) { $effectiveTimeoutMs  = 300 }
            if ($MaxThreads -eq 200) { $effectiveMaxThreads = 50  }
        }
        'Stealth' {
            if ($TimeoutMs  -eq 150) { $effectiveTimeoutMs  = 500 }
            if ($MaxThreads -eq 200) { $effectiveMaxThreads = 20  }
            $queueDelayBaseMs = 50   # small random delay per queued port
        }
        'Aggressive' {
            if ($TimeoutMs  -eq 150) { $effectiveTimeoutMs  = 75  }
            if ($MaxThreads -eq 200) { $effectiveMaxThreads = 500 }
        }
        'Mixed' {
            if ($totalPorts -le 5000) {
                # Few ports: be aggressive
                if ($TimeoutMs  -eq 150) { $effectiveTimeoutMs  = 75  }
                if ($MaxThreads -eq 200) { $effectiveMaxThreads = 400 }
            }
            else {
                # Many ports: safer defaults
                if ($TimeoutMs  -eq 150) { $effectiveTimeoutMs  = 250 }
                if ($MaxThreads -eq 200) { $effectiveMaxThreads = 120 }
            }
        }
        'Custom' {
            # Use provided TimeoutMs and MaxThreads as-is
        }
    }

    Write-Host ("Parallel scan on {0} ports {1}-{2} (Timeout={3}ms, MaxThreads={4}, Mode={5}) ..." -f $Target, $StartPort, $EndPort, $effectiveTimeoutMs, $effectiveMaxThreads, $ScanMode) 
     # -ForegroundColor Cyan

    # Script block executed in each runspace
    $scanScript = {
        param($Target, $Port, $TimeoutMs)

        $tcpClient = $null
        try {
            $tcpClient = New-Object System.Net.Sockets.TcpClient
            $async = $tcpClient.BeginConnect($Target, $Port, $null, $null)

            if ($async.AsyncWaitHandle.WaitOne($TimeoutMs, $false) -and $tcpClient.Connected) {
                $tcpClient.EndConnect($async)
                [pscustomobject]@{
                    Target = $Target
                    Port   = $Port
                    State  = 'Open'
                }
            }
        }
        catch {
            # Ignore individual port errors
        }
        finally {
            if ($tcpClient -ne $null) {
                $tcpClient.Close()
                $tcpClient.Dispose()
            }
        }
    }

    # Create runspace pool (PowerShell 5.1 style)
    $initialState = [System.Management.Automation.Runspaces.InitialSessionState]::CreateDefault()
    $runspacePool = [System.Management.Automation.Runspaces.RunspaceFactory]::CreateRunspacePool(
        1,                      # Min runspaces
        $effectiveMaxThreads,   # Max runspaces
        $initialState,
        $Host
    )
    $runspacePool.Open()

    $jobs    = New-Object System.Collections.Generic.List[object]
    $counter = 0

    # Measure total duration (queue + scan)
    $overallSw = [System.Diagnostics.Stopwatch]::StartNew()

    foreach ($port in $ports) {
        $counter++

        $ps = [System.Management.Automation.PowerShell]::Create()
        $ps.RunspacePool = $runspacePool
        $null = $ps.AddScript($scanScript).AddArgument($Target).AddArgument($port).AddArgument($effectiveTimeoutMs)

        $handle = $ps.BeginInvoke()

        $jobs.Add([pscustomobject]@{
            Port       = $port
            PSInstance = $ps
            Handle     = $handle
        }) | Out-Null

        if (($counter % $QueueBatchSize) -eq 0) {
            Write-Host ("Queued {0}/{1} ports..." -f $counter, $totalPorts)
        }

        # Stealth mode: add small random delays while queuing
        if ($queueDelayBaseMs -gt 0) {
            $delay = Get-Random -Minimum $queueDelayBaseMs -Maximum ($queueDelayBaseMs * 3)
            Start-Sleep -Milliseconds $delay
        }
    }

    $openPorts = New-Object System.Collections.Generic.List[object]

    Write-Host "Waiting for results..." 
    # -ForegroundColor Yellow

    foreach ($job in $jobs) {
        $result = $job.PSInstance.EndInvoke($job.Handle)

        if ($result) {
            foreach ($r in $result) {
                if ($r.State -eq 'Open') {
                    $openPorts.Add($r) | Out-Null
                    Write-Host ("[OPEN] {0}:{1}" -f $r.Target, $r.Port) -ForegroundColor Green
                }
            }
        }

        $job.PSInstance.Dispose()
    }

    $runspacePool.Close()
    $runspacePool.Dispose()

    $overallSw.Stop()
    $duration = $overallSw.Elapsed

    $openPorts = $openPorts | Sort-Object Port

    Write-Host ""
    Write-Host ("Scan complete. Found {0} open port(s). Duration {1}" -f $openPorts.Count, $duration) 
    # -ForegroundColor Green

    return $openPorts
}