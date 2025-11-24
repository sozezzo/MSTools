function Write-Log {
<#
.SYNOPSIS
    Writes timestamped log messages to a primary log file and (optionally) to an extra,
    extension-suffixed side log. Supports numeric level aliases for Grafana and external parsers.

.DESCRIPTION
    Behavior overview:
      - If no LogFileName is provided (and no global default is set), messages are NOT written to disk;
        they are printed to the console (as if -Verbose were used).
      - Supports global defaults:
          $Global:WriteLog_LogFileName       [string] -> default main log file path
          $Global:WriteLog_Verbose           [bool]   -> force console output
          $Global:WriteLog_MaximumSizeLogKB  [int]    -> default rotation size in KB
      - Supports global level suppression (string[] of levels: Debug, Info, Warning, Error, Critical):
          $Global:WriteLog_DisabledConsoleLevels -> levels NOT printed to console
          $Global:WriteLog_DisabledFileLevels    -> levels NOT written to files
        If unset or empty, no suppression occurs.
      - Debug gating:
          $Global:WriteLog_DebugEnabled [bool] -> when $true, Debug messages are allowed;
                                                 when $false or unset, Debug messages are dropped entirely.
      - Trace switch:
          Use -Trace on any log call to append " | duration=..." since the previous traced call.
          Duration format: hh:mm:ss (<24h) or dd hh:mm:ss (≥24h).
          The stored timestamp is updated ONLY when -Trace is used, and is kept in UTC at
          $Global:WriteLog_LastTraceAt.
      - Extra side log with -Save:
          Pass -Save 'xyz' to ALSO write the entry to "<LogFileName>.xyz.log".
          -Save does NOT change severity; it only adds a suffix to create an extra log file.
          The side log is written only when a main log file is active (console-only mode ignores -Save).
          The -Save value must be an extension-like token (no path separators or wildcards).

    Numeric level aliases (for Grafana / parsers):
        0 = Debug
        1 = Info
        2 = Warning
        3 = Error
        4 = Critical

.PARAMETER Message
    The log message to write.

.PARAMETER Level
    Severity level, string or numeric (0–4). Valid names: Debug, Info, Warning, Error, Critical.
    The level name is included in the log entry, e.g. "[Info(1)]".

.PARAMETER LogFileName
    The path to the main log file. If omitted and no global default exists, logs only to console.

.PARAMETER MaximumSizeLogKB
    Maximum size of a log file in KB before rotation. Default 50,000 KB (50 MB),
    unless overridden by $Global:WriteLog_MaximumSizeLogKB.

.PARAMETER Save
    Optional suffix (extension-only) to ALSO save to a side log:
      "<LogFileName>.<Save>.log"
    Example: -Save 'sql' -> writes to main log AND "<main>.sql.log".
    Invalid tokens (containing path/wildcard characters) will throw.

.PARAMETER Trace
    When present, appends duration since the previous traced call. Updates the trace timestamp.

.EXAMPLE
    # Info-level message (string)
    Write-Log -Message "Starting sync" -Level Info

.EXAMPLE
    # Equivalent using numeric level
    Write-Log -Message "Starting sync" -Level 1

.EXAMPLE
    # Trace duration between steps
    Write-Log -Message "Step 1 begin" -Level Info -Trace
    Start-Sleep -Seconds 2
    Write-Log -Message "Step 1 end" -Level Info -Trace   # | duration=00:00:02

.EXAMPLE
    # Write to an extra side log "<main>.sql.log"
    Write-Log -Message "Executed batch 42" -Level Info -Save 'sql'

.EXAMPLE
    # Reset trace baseline if needed
    Remove-Variable -Name WriteLog_LastTraceAt -Scope Global -ErrorAction SilentlyContinue

.NOTES
    Author: Sozezzo Astra
    License: MIT
    Version: 2.3.0
#>

    [CmdletBinding()]
    param(
        [string] $Message = "",
        [Alias("Severity")]
        [object] $Level = "Info",  # can be string or numeric (0–4)
        [string] $LogFileName = "",
        [int]    $MaximumSizeLogKB = 50000,
        [string] $Save = "",
        [switch] $Trace
    )
    
    # ---------- Early gating for Debug ----------
    $debugEnabled = [bool]$Global:WriteLog_DebugEnabled
    if ($LevelName -eq 'Debug' -and -not $debugEnabled) { return }

    # ---------- Map numeric aliases ----------
    $levelMap = @{
        0 = 'Debug'
        1 = 'Info'
        2 = 'Warning'
        3 = 'Error'
        4 = 'Critical'
    }

    # ---------- Normalize Level -> $LevelName (TitleCase), $LevelNum (0..4) ----------
    try {
        $toTitle = { param($s) (Get-Culture).TextInfo.ToTitleCase(($s -as [string]).ToLower()) }

        if ($Level -is [string]) {
            $raw = $Level.Trim()
            if ($raw -match '^\d+$') {
                $n = [int]$raw
            } else {
                $name = ($raw -replace '\s','').ToLower()
                switch ($name) {
                    'debug'    { $n = 0; break }
                    'info'     { $n = 1; break }
                    'warning'  { $n = 2; break }
                    'error'    { $n = 3; break }
                    'critical' { $n = 4; break }
                    default    { throw "name" }
                }
            }
        }
        elseif ($Level -is [ValueType]) {
            $n = [int]$Level  # accept int64/byte/etc
        }
        else {
            throw "type"
        }

        if (-not $levelMap.ContainsKey($n)) { throw "range" }

        $LevelNum  = $n
        $LevelName = & $toTitle $levelMap[$n]
    }
    catch {
        throw "Invalid Level: $($Level) - Must be one of Debug, Info, Warning, Error, Critical or 0 to 4"
    }

    # ---------- Validate/Sanitize Save (extension token only) ----------
    if ($Save) {
        $Save = $Save.Trim()
        if ($Save.StartsWith('.')) { $Save = $Save.Substring(1) }
        if ($Save -match '[\\/:\*\?"<>\|]') {
            throw "Invalid -Save value '$Save'. Use an extension-like token (no path characters)."
        }
    }
    
    # ---------- Resolve effective settings ----------
    $effectiveLogFile = if ($PSBoundParameters.ContainsKey('LogFileName') -and -not [string]::IsNullOrWhiteSpace($LogFileName)) {
        $LogFileName
    } elseif ($Global:WriteLog_LogFileName -and -not [string]::IsNullOrWhiteSpace($Global:WriteLog_LogFileName)) {
        [string]$Global:WriteLog_LogFileName
    } else {
        ""  # console-only mode
    }

    if (-not $PSBoundParameters.ContainsKey('MaximumSizeLogKB') -and $Global:WriteLog_MaximumSizeLogKB) {
        $MaximumSizeLogKB = [int]$Global:WriteLog_MaximumSizeLogKB
    }

    $paramVerbose = $PSBoundParameters.ContainsKey('Verbose')
    $effectiveVerbose =
        $paramVerbose -or
        ([bool]($Global:WriteLog_Verbose) -eq $true) -or
        ([string]::IsNullOrWhiteSpace($effectiveLogFile))

    # ---------- Global suppression ----------
    $disabledConsole = @()
    if ($Global:WriteLog_DisabledConsoleLevels -is [System.Collections.IEnumerable]) {
        $disabledConsole = [string[]]$Global:WriteLog_DisabledConsoleLevels
    }
    $disabledFiles = @()
    if ($Global:WriteLog_DisabledFileLevels -is [System.Collections.IEnumerable]) {
        $disabledFiles = [string[]]$Global:WriteLog_DisabledFileLevels
    }

    $suppressConsole = ($disabledConsole -and ($disabledConsole -contains $LevelName))
    $suppressFiles   = ($disabledFiles   -and ($disabledFiles   -contains $LevelName))

    # ---------- Compose entry (with optional Trace duration) ----------
    $nowLocal = Get-Date
    $ts = $nowLocal.ToString('yyyy-MM-dd HH:mm:ss')

    $durationSuffix = ''
    if ($Trace.IsPresent) {
        $nowUtc  = [DateTime]::UtcNow
        $lastUtc = $Global:WriteLog_LastTraceAt
        if ($lastUtc -is [DateTime]) {
            $delta = $nowUtc - $lastUtc
            if ($delta.TotalSeconds -ge 0) {
                if ($delta.TotalDays -ge 1) {
                    $durationSuffix = " | duration={0:%d} {0:hh\:mm\:ss}" -f $delta
                } else {
                    $durationSuffix = " | duration={0:hh\:mm\:ss}" -f $delta
                }
            }
        }
        $Global:WriteLog_LastTraceAt = $nowUtc
        $Global:WriteLog_LastTraceStampString = $nowLocal.ToString('yyyy-MM-dd HH:mm:ss')
    }

    $logEntry = "$ts|[$LevelName($LevelNum)]|$Message$durationSuffix"

    # ---------- Write to files ----------
    if (-not [string]::IsNullOrWhiteSpace($effectiveLogFile) -and -not $suppressFiles) {
        if (Test-Path $effectiveLogFile) {
            $SizeLogKB = (Get-Item $effectiveLogFile).Length / 1KB
            if ($SizeLogKB -gt $MaximumSizeLogKB) {
                $LogFileBak = "$effectiveLogFile.bak"
                if (Test-Path $LogFileBak) { Remove-Item -Force $LogFileBak }
                Rename-Item -Path $effectiveLogFile -NewName $LogFileBak
            }
        } else {
            $dir = Split-Path -Path $effectiveLogFile -Parent
            if ($dir -and -not (Test-Path $dir)) { New-Item -ItemType Directory -Path $dir -Force | Out-Null }
        }

        Add-Content -Path $effectiveLogFile -Value $logEntry

        if ($Save) {
            $levelFile = "$effectiveLogFile.$Save.log"
            $levelDir  = Split-Path -Path $levelFile -Parent
            if ($levelDir -and -not (Test-Path $levelDir)) { New-Item -ItemType Directory -Path $levelDir -Force | Out-Null }

            if (Test-Path $levelFile) {
                $SizeLogKB = (Get-Item $levelFile).Length / 1KB
                if ($SizeLogKB -gt $MaximumSizeLogKB) {
                    $levelFileBak = "$levelFile.bak"
                    if (Test-Path $levelFileBak) { Remove-Item -Force $levelFileBak }
                    Rename-Item -Path $levelFile -NewName $levelFileBak
                }
            }
            Add-Content -Path $levelFile -Value $logEntry
        }
    }

    # ---------- Console output ----------
    if ($effectiveVerbose -and -not $suppressConsole) {
        $color = switch ($LevelName) {
            "Critical" { "Red" }
            "Error"    { "Red" }
            "Warning"  { "Yellow" }
            "Info"     { "Gray" }
            "Debug"    { "DarkGray" }
            default    { "White" }
        }
        Write-Host $logEntry -ForegroundColor $color
    }
}
 