function Invoke-MSRestartLocalServer {
<#
.SYNOPSIS
  Restarts the local server immediately.

.DESCRIPTION
  Issues a forced restart of the local machine using Restart-Computer.
  An optional comment/reason can be supplied for logging purposes.

.PARAMETER Comment
  A message describing the reason for the restart.
  Defaults to "Reboot initiated by [PS] Invoke-MSRestartLocalServer function".

.EXAMPLE
  Invoke-MSRestartLocalServer

.EXAMPLE
  Invoke-MSRestartLocalServer -Comment "Applying Windows updates"

.NOTES
  Author  : Sozezzo Astra
  Requires: Local administrator privileges.
#>
    Param(
        [string]$Comment = "Reboot initiated by [PS] Invoke-MSRestartLocalServer function"  # Default restart comment
    )

    try {
        # Initiates a reboot of the local server with a custom comment
        Restart-Computer -ArgumentList "/r /c `"$Comment`"" -Force

        Write-Host "Restart command issued successfully. Reason: $Comment"
    }
    catch {
        Write-Error "Failed to initiate restart: $_"
    }
}