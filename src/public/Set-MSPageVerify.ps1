function Set-MSPageVerify {
<#
.SYNOPSIS
    Sets PAGE_VERIFY option for databases.

.DESCRIPTION
    If -PageVerify is not specified, CHECKSUM is used (recommended best practice).

    - Supports multiple SQL instances
    - Skips read-only databases
    - Skips tempdb
    - Logs change vs no-change
    - Supports WhatIf / Confirm

.PARAMETER SqlInstance
    One or more SQL Server instances.

.PARAMETER Database
    Optional list of databases to include.

.PARAMETER ExcludedDatabase
    Optional list of databases to exclude.

.PARAMETER Credential
    SQL Credential if needed.

.PARAMETER PageVerify
    CHECKSUM (default), TORN_PAGE_DETECTION, or NONE.

.PARAMETER IncludeSystemDatabase
    Include master, model, msdb (tempdb always excluded).

.EXAMPLE
    Set-MSPageVerify -SqlInstance SQL01

.EXAMPLE
    Set-MSPageVerify -SqlInstance SQL01,SQL02 -PageVerify CHECKSUM -WhatIf

.EXAMPLE
    Set-MSPageVerify -SqlInstance SQL01 -Database MyDB
#>

    [CmdletBinding(SupportsShouldProcess = $true)]
    param (
        [Parameter(Mandatory)]
        [string[]]$SqlInstance,

        [string[]]$Database,

        [string[]]$ExcludedDatabase,

        [PSCredential]$Credential,

        [ValidateSet('CHECKSUM','TORN_PAGE_DETECTION','NONE')]
        [string]$PageVerify = 'CHECKSUM',

        [switch]$IncludeSystemDatabase
    )

    foreach ($instance in $SqlInstance) {

        Write-Host "INSTANCE [$instance] - starting"

        try {
            $databases = Get-DbaDatabase `
                -SqlInstance $instance `
                -SqlCredential $Credential
        }
        catch {
            Write-Warning "Failed to connect to [$instance] : $_"
            continue
        }

        # Exclude tempdb always
        $databases = $databases | Where-Object { $_.Name -ne 'tempdb' }

        if (-not $IncludeSystemDatabase) {
            $databases = $databases | Where-Object { -not $_.IsSystemObject }
        }

        if ($Database) {
            $databases = $databases | Where-Object { $_.Name -in $Database }
        }

        if ($ExcludedDatabase) {
            $databases = $databases | Where-Object { $_.Name -notin $ExcludedDatabase }
        }

        foreach ($db in $databases) {

            Write-Host "DB [$($db.Name)] - evaluating"

            if ($db.IsReadOnly) {
                Write-Warning "DB [$($db.Name)] is READONLY - skipping"
                continue
            }

            $current = $db.PageVerify

            if ($current -eq $PageVerify) {
                Write-Host "DB [$($db.Name)] already [$current] - no change"
                continue
            }

            $query = "ALTER DATABASE [$($db.Name)] SET PAGE_VERIFY $PageVerify;"

            if ($PSCmdlet.ShouldProcess("$instance - $($db.Name)", "Set PAGE_VERIFY to $PageVerify")) {

                try {
                    Invoke-DbaQuery `
                        -SqlInstance $instance `
                        -SqlCredential $Credential `
                        -Database master `
                        -Query $query

                    Write-Host "DB [$($db.Name)] changed from [$current] to [$PageVerify]"
                }
                catch {
                    Write-Error "Failed to change [$($db.Name)] on [$instance] : $_"
                }
            }
        }

        Write-Host "INSTANCE [$instance] - completed"
    }
}