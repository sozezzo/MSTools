function Write-LogInternal {
<#
.SYNOPSIS
    Lightweight internal debug helper that emits messages only when enabled.

.DESCRIPTION
    Write-LogInternal is a thin shim around your public Write-Log function.
    It short-circuits when internal debug is disabled to keep runtime overhead minimal.
    If Write-Log is temporarily unavailable (e.g., during module bootstrap),
    it falls back to Write-Host so you never lose crucial diagnostics.

    Enable/disable in two ways (both supported):
      1) Single switch (global boolean):
           $Global:WriteLog_DebugInternalEnabled = $true | $false

      2) Category-based hashtable (preferred for future-proofing):
           $Global:WriteLog_Debug = @{
               Internal = $true
               SQL      = $false
               Network  = $false
           }

    The function checks the hashtable first (if present). If the requested
    -Category key is found, that value controls gating. If not found but an
    'Internal' key exists, it uses that. Otherwise it falls back to the legacy
    $Global:WriteLog_DebugInternalEnabled boolean.

.PARAMETER Message
    The debug text to log.

.PARAMETER Trace
    When present, passes -Trace:$true through to Write-Log so you can opt into
    deeper tracing from call sites without changing your public logger’s defaults.

.PARAMETER Category
    A simple tag that prefixes the message and can be used with
    $Global:WriteLog_Debug hashtable gating. Defaults to 'Internal'.

.EXAMPLE
    $Global:WriteLog_DebugInternalEnabled = $true
    Write-LogInternal -Message 'Warm-up complete.'

.EXAMPLE
    $Global:WriteLog_Debug = @{ Internal = $true; SQL = $false }
    Write-LogInternal -Message 'Connected to server.' -Category SQL   # will be skipped (false)
    Write-LogInternal -Message 'Bootstrap done.'                       # will run (true)

.EXAMPLE
    Write-LogInternal -Message 'Query plan captured.' -Trace

.NOTES
    Visibility:
      - Place this file under:  src\private
      - Do NOT export it in your module manifest/exports. Private functions
        remain callable by other functions in the module but hidden from end users.

    Fallback behavior:
      - If Write-Log is not available yet, prints via Write-Host with a clear
        [DEBUG/<Category>] prefix, then returns.

    Author:  Sozezzo Astra
    Version: 2025-10-28
#>
    [CmdletBinding()]
    param(
        [Parameter(Mandatory, Position=0)]
        [string] $Message,

        [switch] $Trace,

        [string] $Category = 'Internal'
    )

    # ---------- Gate: category-aware first, legacy bool second ----------
    $enabled = $false

    if ($Global:WriteLog_Debug -is [hashtable]) {
        if ($Global:WriteLog_Debug.ContainsKey($Category)) {
            $enabled = [bool]$Global:WriteLog_Debug[$Category]
        }
        elseif ($Global:WriteLog_Debug.ContainsKey('Internal')) {
            $enabled = [bool]$Global:WriteLog_Debug['Internal']
        }
    }
    elseif ($PSBoundParameters.ContainsKey('Category')) {
        # No hashtable present; if a custom category was asked for, honor legacy flag only.
        $enabled = [bool]$Global:WriteLog_DebugInternalEnabled
    }
    else {
        $enabled = [bool]$Global:WriteLog_DebugInternalEnabled
    }

    if (-not $enabled) { return }

    # ---------- Fallback if public logger isn't available yet ----------
    $writeLogCmd = Get-Command -Name Write-Log -ErrorAction SilentlyContinue
    if (-not $writeLogCmd) {
        Write-Host ("[DEBUG/{0}] {1}" -f $Category, $Message)
        return
    }

    # ---------- Forward to the public logger ----------
    # -Level is pinned to Debug; -Trace flows through as a switch
    Write-Log -Message ("[{0}] {1}" -f $Category, $Message) -Level Debug -Trace:$Trace
}
