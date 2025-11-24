<#
.SYNOPSIS
    Writes timestamped log messages to a primary log file and optional level-specific log files.

.DESCRIPTION
    Behavior overview:
      - If no LogFileName is provided (and no global default is set), messages are NOT written to disk;
        they are printed to the console (as if -Verbose were used).
      - Supports global defaults:
          $Global:WriteLog_LogFileName       [string]   -> default log file path
          $Global:WriteLog_Verbose           [bool]     -> force console output
          $Global:WriteLog_MaximumSizeLogKB  [int]      -> default rotation size in KB
      - Supports global level suppression (string[] of levels: Info, Warning, Error, Alert, Important):
          $Global:WriteLog_DisabledConsoleLevels -> levels NOT printed to console
          $Global:WriteLog_DisabledFileLevels    -> levels NOT written to files
        If unset or empty, no suppression occurs.

.PARAMETER Message
    The log message to write.

.PARAMETER LogFileName
    The path to the main log file. If omitted and no global default exists, logs only to console.

.PARAMETER MaximumSizeLogKB
    Maximum size of a log file in KB before rotation. Default 50,000 KB (50 MB),
    unless overridden by $Global:WriteLog_MaximumSizeLogKB.

.PARAMETER Level
    Optional severity level (Info, Warning, Error, Alert, Important).
    If specified AND -SaveSeparately is used, also writes to <LogFileName>.<Level>.log.

.PARAMETER SaveSeparately
    When present and Level is set, writes to a per-level side log in addition to main log.

.PARAMETER Verbose
    When present, prints the log message to the console (color-coded).
    Console output also occurs when no effective log file is set OR $Global:WriteLog_Verbose is $true.

.EXAMPLE
    # Console only (no file)
    Write-Log -Message "Backup completed" -Level Info -Verbose

.EXAMPLE
    # Use global defaults for path/verbosity/size
    $Global:WriteLog_LogFileName       = 'C:\Logs\backup.log'
    $Global:WriteLog_Verbose           = $true
    $Global:WriteLog_MaximumSizeLogKB  = 20000
    Write-Log -Message "Backup completed" -Level Info

.EXAMPLE
    # Suppress DEBUG-like chatter without touching call sites
    $Global:WriteLog_DisabledConsoleLevels = @('Info')       # don't print Info to console
    $Global:WriteLog_DisabledFileLevels    = @('Info')       # don't write Info to files

.NOTES
    Author: Milton Sozezzo
    License: MIT
    Version: 1.2.0
#>
function Write-Log {
    [CmdletBinding()]
    param(
        [string] $Message = "",
        [string] $LogFileName = "",
        [int]    $MaximumSizeLogKB = 50000,
        [ValidateSet("Info", "Warning", "Error", "Alert", "Important")]
        [string] $Level,
        [switch] $SaveSeparately,
        [switch] $Verbose
    )

    # ---------- Resolve effective settings (Params > Globals > Defaults) ----------
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

    $effectiveVerbose =
        ($Verbose.IsPresent) -or
        ([bool]($Global:WriteLog_Verbose) -eq $true) -or
        ([string]::IsNullOrWhiteSpace($effectiveLogFile))  # console-only if no file name

    # ---------- Global suppression lists ----------

    
    $disabledConsole = @()
    if ($Global:WriteLog_DisabledConsoleLevels -is [System.Collections.IEnumerable]) {
        $disabledConsole = [string[]]$Global:WriteLog_DisabledConsoleLevels
    }
    $disabledFiles = @()
    if ($Global:WriteLog_DisabledFileLevels -is [System.Collections.IEnumerable]) {
        $disabledFiles = [string[]]$Global:WriteLog_DisabledFileLevels
    }

    $suppressConsole = $false
    $suppressFiles   = $false
    if ($Level) {
        if ($disabledConsole -and ($disabledConsole -contains $Level)) { $suppressConsole = $true }
        if ($disabledFiles   -and ($disabledFiles   -contains $Level)) { $suppressFiles   = $true }
    }

    # ---------- Compose entry ----------
    $ts       = Get-Date -Format 'yyyy-MM-dd HH:mm:ss'
    $logEntry = "$ts|$Message"

    # ---------- Write to files (unless suppressed) ----------
    if (-not [string]::IsNullOrWhiteSpace($effectiveLogFile) -and -not $suppressFiles) {

        # Rotate base log file if needed
        if (Test-Path $effectiveLogFile) {
            $SizeLogKB = (Get-Item $effectiveLogFile).Length / 1KB
            if ($SizeLogKB -gt $MaximumSizeLogKB) {
                $LogFileBak = "$effectiveLogFile.bak"
                if (Test-Path $LogFileBak) { Remove-Item -Force $LogFileBak }
                Rename-Item -Path $effectiveLogFile -NewName $LogFileBak
            }
        } else {
            # Ensure the folder exists if a path was provided
            $dir = Split-Path -Path $effectiveLogFile -Parent
            if ($dir -and -not (Test-Path $dir)) { New-Item -ItemType Directory -Path $dir -Force | Out-Null }
        }

        # Write to main log file
        $logEntry | Tee-Object -FilePath $effectiveLogFile -Append | Out-Null

        # Level-specific side log if requested
        if ($Level -and $SaveSeparately.IsPresent) {
            $levelFile = "$effectiveLogFile.$Level.log"
            if (Test-Path $levelFile) {
                $SizeLogKB = (Get-Item $levelFile).Length / 1KB
                if ($SizeLogKB -gt $MaximumSizeLogKB) {
                    $levelFileBak = "$levelFile.bak"
                    if (Test-Path $levelFileBak) { Remove-Item -Force $levelFileBak }
                    Rename-Item -Path $levelFile -NewName $levelFileBak
                }
            }
            $logEntry | Tee-Object -FilePath $levelFile -Append | Out-Null
        }
    }

    # ---------- Console output (unless suppressed) ----------
    if ($effectiveVerbose -and -not $suppressConsole) {
        $color = switch ($Level) {
            "Info"      { "Gray" }
            "Warning"   { "Yellow" }
            "Error"     { "Red" }
            "Alert"     { "Magenta" }
            "Important" { "Cyan" }
            default     { "White" }
        }
        Write-Host $logEntry -ForegroundColor $color
    }
}
