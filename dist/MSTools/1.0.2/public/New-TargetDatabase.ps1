function New-TargetDatabase {
<#
.SYNOPSIS
Ensures a clean target database exists on a destination SQL Server.

.DESCRIPTION
Creates a database named <NewDatabase> on <DestInstance> using the instance defaults.
If a database with the same name already exists:
- With -DropIfExists, it will be dropped and recreated (requires confirmation unless -Confirm:$false).
- Without -DropIfExists, the function throws.

Writes progress to an optional log file via your Write-Log function.

.PARAMETER DestInstance
SQL Server instance where the database should exist (e.g., "ServerA\SQL2022").

.PARAMETER NewDatabase
Name of the database to create.

.PARAMETER LogFileName
Path to a log file used by your Write-Log function (optional).

.PARAMETER DropIfExists
If supplied and the database exists, drop it first, then recreate it.

.PARAMETER PassThru
If supplied, returns the dbatools database object at the end.

.EXAMPLE
New-TargetDatabase -DestInstance "ISI-M0BDRS31" -NewDatabase "MyAppDB" -DropIfExists -Confirm:$false

Drops and recreates [ISI-M0BDRS31].[MyAppDB] without prompting (because -Confirm:$false).

.EXAMPLE
New-TargetDatabase -DestInstance "ISI-M0BDRS31" -NewDatabase "MyAppDB" -PassThru

Ensures the database exists (throws if it already exists) and returns the database object.

.NOTES
- Uses dbatools commands: Get-DbaDatabase, Remove-DbaDatabase, New-DbaDatabase.
- Honor -WhatIf / -Confirm via SupportsShouldProcess.
- Default collation/owner/recovery model are whatever the instance would normally use.
#>
    [CmdletBinding(SupportsShouldProcess, ConfirmImpact = 'High')]
    param(
        [Parameter(Mandatory)][string] $DestInstance,
        [Parameter(Mandatory)][string] $NewDatabase,

        [switch] $DropIfExists,
        [switch] $PassThru
    )

    Write-Log -Message "New-TargetDatabase started for [$DestInstance].[$NewDatabase]." -Level Info  

    try {
        $existing = Get-DbaDatabase -SqlInstance $DestInstance -Database $NewDatabase -ErrorAction SilentlyContinue

        if ($existing) {
            if ($DropIfExists) {
                if ($PSCmdlet.ShouldProcess("$DestInstance / $NewDatabase", "Drop existing database")) {
                    Write-Log -Message "Dropping existing database '$NewDatabase' on '$DestInstance'." -Level Warning 
                    Remove-DbaDatabase -SqlInstance $DestInstance -Database $NewDatabase -Confirm:$false
                }
            }
            else {
                throw "Database '$NewDatabase' already exists on '$DestInstance'. Use -DropIfExists to replace it."
            }
        }

        if ($PSCmdlet.ShouldProcess("$DestInstance / $NewDatabase", "Create database (instance defaults)")) {
            Write-Log -Message "Creating database '$NewDatabase' using instance defaults." -Level Info  
            New-DbaDatabase -SqlInstance $DestInstance -Name $NewDatabase -ErrorAction Stop -Confirm:$false | Out-Null
        }

        Write-Log -Message "Target database '$NewDatabase' is ready on '$DestInstance'." -Level Warning  

        if ($PassThru) {
            return Get-DbaDatabase -SqlInstance $DestInstance -Database $NewDatabase
        }
    }
    catch {
        Write-Log -Message ("ERROR: " + $_.Exception.Message) -Level Error 
        throw
    }
}
