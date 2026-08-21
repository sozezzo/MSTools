function Set-MSLockScreenTimeout {
<#
.SYNOPSIS
Sets the Windows Console Lock Display Off Timeout.

.DESCRIPTION
Configures the amount of time Windows waits before turning off the display
after the workstation has been locked.

A value of 0 disables the timeout and keeps the display active indefinitely
while the workstation remains locked.

The function verifies the current setting before applying any changes and
reports whether a modification was required.

.PARAMETER Minutes
Lock screen timeout in minutes.

Valid values:
    0      = Never turn off display while locked
    1-1440 = Timeout in minutes

.EXAMPLE
Set-MSLockScreenTimeout -Minutes 0

Disables the lock screen display timeout.

.EXAMPLE
Set-MSLockScreenTimeout -Minutes 15

Sets the lock screen display timeout to 15 minutes.

.NOTES
Author  : Sozezzo
Requires: Administrator privileges

Power Setting:
    Console lock display off timeout

Subgroup GUID:
    7516b95f-f776-4464-8c53-06167f40cc99

Setting GUID:
    8ec4b3a5-6868-48c2-be75-4f3044be88a7
#>    
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)]
        [ValidateRange(0, 1440)]
        [int]$Minutes
    )

    # GUIDs
    $SubGroupGuid = "7516b95f-f776-4464-8c53-06167f40cc99"
    $SettingGuid  = "8ec4b3a5-6868-48c2-be75-4f3044be88a7"

    try {
        Write-Host "Reading current lock screen timeout value..."

        # Get active scheme GUID
        $schemeOutput = powercfg /getactivescheme
        if ($schemeOutput -notmatch 'GUID:\s+([a-f0-9-]+)') {
            throw "Unable to determine active power scheme."
        }

        $activeScheme = $matches[1]

        # Query setting
        $queryOutput = powercfg /query $activeScheme $SubGroupGuid $SettingGuid

        $line = $queryOutput | Select-String "Current AC Power Setting Index"

        if (-not $line) {
            throw "Could not read current timeout value. The setting may be hidden."
        }

        $hexValue = ($line -split '\s+')[-1]
        $currentSeconds = [Convert]::ToInt32($hexValue,16)
        $currentMinutes = [math]::Round($currentSeconds / 60)

        Write-Host "Current timeout: $currentMinutes minute(s)"

        if ($currentMinutes -eq $Minutes) {
            Write-Host "No change required. Value already set to $Minutes minute(s)." -ForegroundColor Yellow
            return
        }

        $newSeconds = $Minutes * 60

        Write-Host "Changing timeout to $Minutes minute(s)..."

        powercfg /setacvalueindex $activeScheme $SubGroupGuid $SettingGuid $newSeconds | Out-Null
        powercfg /setactive $activeScheme | Out-Null

        Write-Host "Timeout changed successfully from $currentMinutes to $Minutes minute(s)." -ForegroundColor Green
    }
    catch {
        Write-Error "Failed to update lock screen timeout. $($_.Exception.Message)"
    }
}
