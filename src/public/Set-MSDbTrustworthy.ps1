function Set-MSDbTrustworthy {
<#
.SYNOPSIS
    Sets SQL Server database TRUSTWORTHY option.

.DESCRIPTION
    Uses dbatools to set TRUSTWORTHY ON or OFF for one or more databases.

    If -Database is omitted, all user databases are processed.
    By default, TRUSTWORTHY is set to ON.

    Use -Force for automation to avoid confirmation.

    Use -KillConnections to kill active user sessions before changing TRUSTWORTHY.
    This does not use SINGLE_USER, so it is compatible with AlwaysOn / mirroring restrictions.

.PARAMETER SqlInstance
    SQL Server instance name.

.PARAMETER Database
    One or more database names.
    If omitted, all user databases are used.

.PARAMETER Trustworthy
    Desired TRUSTWORTHY value.
    Defaults to $true.

.PARAMETER Force
    Avoids confirmation prompts.

.PARAMETER KillConnections
    Kills active user sessions connected to the target database before changing TRUSTWORTHY.

.EXAMPLE
    Set-MSDbTrustworthy -SqlInstance "SQL01"

.EXAMPLE
    Set-MSDbTrustworthy -SqlInstance "SQL01" -Database "MyDB" -Trustworthy $false -Force

.EXAMPLE
    Set-MSDbTrustworthy -SqlInstance "SQL01" -Database "MyDB" -KillConnections -Force

.NOTES
    Requires dbatools.
#>
    [CmdletBinding(SupportsShouldProcess, ConfirmImpact = 'Medium')]
    param(
        [Parameter(Mandatory)]
        [string]$SqlInstance,

        [string[]]$Database,

        [bool]$Trustworthy = $true,

        [switch]$Force,

        [switch]$KillConnections
    )

    $state = if ($Trustworthy) { 'ON' } else { 'OFF' }
    $targetValue = if ($Trustworthy) { 1 } else { 0 }

    if (-not $Database -or $Database.Count -eq 0) {
        $Database = Get-DbaDatabase -SqlInstance $SqlInstance -ExcludeSystem |
            Where-Object { $_.Status -eq 'Normal' } |
            Select-Object -ExpandProperty Name
    }

    foreach ($db in $Database) {

        $safeDb = $db.Replace(']', ']]')
        $safeDbLiteral = $db.Replace("'", "''")

        $query = @"
IF EXISTS (
    SELECT 1
    FROM sys.databases
    WHERE name = N'$safeDbLiteral'
      AND is_trustworthy_on <> $targetValue
)
BEGIN
    ALTER DATABASE [$safeDb] SET TRUSTWORTHY $state;
END
"@

        Write-Host "Processing database: $db"

        $continue = $true

        if (-not $Force) {
            $continue = $PSCmdlet.ShouldContinue(
                "Do you want to set TRUSTWORTHY $state on database [$db] on [$SqlInstance]?`r`n`r`nTo avoid this pop-up add the parameter -Force",
                "Confirm TRUSTWORTHY change"
            )
        }

        if ($continue -and $PSCmdlet.ShouldProcess("$SqlInstance.$db", "SET TRUSTWORTHY $state")) {

            if ($KillConnections) {
                Get-DbaProcess `
                    -SqlInstance $SqlInstance `
                    -Database $db |
                    Where-Object {
                        $_.IsSystem -eq $false -and
                        $_.Login -notin @('sa') -and
                        $_.Program -notlike '*dbatools*'
                    } |
                    Stop-DbaProcess -Confirm:$false
            }

            Invoke-DbaQuery `
                -SqlInstance $SqlInstance `
                -Database master `
                -Query $query `
                -EnableException
        }
    }
}
