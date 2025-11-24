function Set-DbaDbTrustworthy {
    <#
    .SYNOPSIS
        Sets the TRUSTWORTHY property of a SQL Server database.

    .DESCRIPTION
        Uses dbatools to ALTER DATABASE and set the TRUSTWORTHY option
        to ON or OFF. Compatible with Write-Log conventions.

    .PARAMETER SqlInstance
        The SQL Server instance name.

    .PARAMETER Database
        The database name to modify.

    .PARAMETER Enable
        Switch to enable TRUSTWORTHY (ON). If omitted, TRUSTWORTHY is set to OFF.

    .PARAMETER WhatIf
        Shows what would happen if the command runs.

    .EXAMPLE
        Set-DbaDbTrustworthy -SqlInstance "SQL-MERTIOS" -Database "MyApp" -Enable

    .EXAMPLE
        Set-DbaDbTrustworthy -SqlInstance "SQL-MERTIOS" -Database "MyApp" -Enable:$false

    .NOTES
        Author : Astra for Milton Soz
        Requires: dbatools module
    #>
    [CmdletBinding(SupportsShouldProcess, ConfirmImpact = 'Medium')]
    param(
        [Parameter(Mandatory)][string]$SqlInstance,
        [Parameter(Mandatory)][string]$Database,
        [switch]$Enable
    )

    $state = if ($Enable) { 'ON' } else { 'OFF' }
    $msg = "Setting TRUSTWORTHY = $state for [$SqlInstance].[$Database]"

    if ($PSCmdlet.ShouldProcess("$Database on $SqlInstance", $msg)) {
        try {
            Write-Log -Message $msg -LogFileName $LogFileName -Level Info
            Invoke-DbaQuery -SqlInstance $SqlInstance -Database master -Query "ALTER DATABASE [$Database] SET TRUSTWORTHY $state;"
            Write-Log -Message "TRUSTWORTHY successfully set to $state for [$SqlInstance].[$Database]" -Level Info
        }
        catch {
            Write-Log -Message "Failed to set TRUSTWORTHY for [$SqlInstance].[$Database]: $_"  -Level Error
            throw
        }
    }
}