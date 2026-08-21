function Set-MSAutoShrink {
<#
.SYNOPSIS
    Enable or disable AUTO_SHRINK on one or more databases across one or more SQL instances.

.DESCRIPTION
    If -Database is provided, applies only to those databases.
    If omitted, applies to all user databases (database_id > 4).

    The function:
        - Supports multiple SQL instances
        - Logs every action
        - Detects current AUTO_SHRINK state
        - Skips READ_ONLY or non-ONLINE databases
        - Reports if no change is needed
        - Uses direct T-SQL (no SMO overhead)
        - Continues execution even if one instance fails

.PARAMETER SqlInstance
    One or more SQL instances.

.PARAMETER Database
    Optional list of databases.

.PARAMETER Enable
    Enable AUTO_SHRINK.

.PARAMETER Disable
    Disable AUTO_SHRINK.
#>

    [CmdletBinding(SupportsShouldProcess = $true)]
    param(
        [Parameter(Mandatory)]
        [string[]] $SqlInstance,

        [string[]] $Database,

        [switch] $Enable,
        [switch] $Disable
    )

    if (-not $Enable -and -not $Disable) {
        throw "You must specify either -Enable or -Disable."
    }

    if ($Enable -and $Disable) {
        throw "You cannot use -Enable and -Disable together."
    }

    $targetValue = if ($Enable) { 1 } else { 0 }
    $targetState = if ($Enable) { "ON" } else { "OFF" }

    $globalChanged = 0
    $globalSkipped = 0
    $globalNoChange = 0
    $globalErrors = 0

    foreach ($instance in $SqlInstance) {

        Write-Host ""
        Write-Host "=================================================="
        Write-Host "INSTANCE: $instance"
        Write-Host "Target AUTO_SHRINK: $targetState"
        Write-Host "=================================================="

        $changed = 0
        $skipped = 0
        $noChange = 0
        $errors = 0

        # Build filter
        if ($Database) {
            $quoted = $Database | ForEach-Object { "'$_'" }
            $filter = "name IN ($($quoted -join ","))"
        }
        else {
            $filter = "database_id > 4"
        }

        $query = @"
SELECT
    name,
    state_desc,
    is_read_only,
    is_auto_shrink_on
FROM sys.databases
WHERE $filter
ORDER BY name
"@

        try {
            $dbs = Invoke-DbaQuery -SqlInstance $instance -Query $query -EnableException
        }
        catch {
            Write-Host "ERROR: Unable to connect or query instance $instance"
            Write-Host $_
            $globalErrors++
            continue
        }

        foreach ($db in $dbs) {

            Write-Host ""
            Write-Host "Instance: $instance"
            Write-Host "Database: $($db.name)"
            Write-Host "State   : $($db.state_desc)"
            Write-Host "ReadOnly: $($db.is_read_only)"
            Write-Host "Current AUTO_SHRINK: $(if($db.is_auto_shrink_on){'ON'}else{'OFF'})"

            if ($db.state_desc -ne "ONLINE") {
                Write-Host "SKIPPED: Database not ONLINE."
                $skipped++
                continue
            }

            if ($db.is_read_only -eq 1) {
                Write-Host "SKIPPED: Database is READ_ONLY."
                $skipped++
                continue
            }

            if ($db.is_auto_shrink_on -eq $targetValue) {
                Write-Host "NO CHANGE: Already set to $targetState."
                $noChange++
                continue
            }

            $alter = "ALTER DATABASE [$($db.name)] SET AUTO_SHRINK $targetState;"

            if ($PSCmdlet.ShouldProcess("$instance - $($db.name)", "Set AUTO_SHRINK $targetState")) {

                try {
                    Invoke-DbaQuery -SqlInstance $instance -Query $alter -EnableException
                    Write-Host "CHANGED: AUTO_SHRINK set to $targetState."
                    $changed++
                }
                catch {
                    Write-Host "ERROR: Failed to change AUTO_SHRINK."
                    Write-Host $_
                    $errors++
                }
            }
        }

        Write-Host ""
        Write-Host "---- Instance Summary ----"
        Write-Host "Changed   : $changed"
        Write-Host "No Change : $noChange"
        Write-Host "Skipped   : $skipped"
        Write-Host "Errors    : $errors"

        $globalChanged += $changed
        $globalSkipped += $skipped
        $globalNoChange += $noChange
        $globalErrors += $errors
    }

    Write-Host ""
    Write-Host "=================================================="
    Write-Host "GLOBAL SUMMARY"
    Write-Host "Changed   : $globalChanged"
    Write-Host "No Change : $globalNoChange"
    Write-Host "Skipped   : $globalSkipped"
    Write-Host "Errors    : $globalErrors"
    Write-Host "=================================================="
}