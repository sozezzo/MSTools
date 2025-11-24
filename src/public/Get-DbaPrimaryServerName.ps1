function Get-DbaPrimaryServerName {
<#
.SYNOPSIS
    Returns the current primary replica (server\instance) for an Always On AG target.
    If the target is not in an AG (or access is limited), returns the target's @@SERVERNAME.

.DESCRIPTION
    - Pass a node, listener, or alias in -SqlInstance.
    - Uses T-SQL only (via Invoke-DbaQuery) and does NOT import any modules.
    - If AG DMVs are not accessible (e.g., missing VIEW SERVER STATE), it still returns @@SERVERNAME.
    - Avoids ever returning the input alias; always normalizes to real server name(s).

.PARAMETER SqlInstance
    SQL instance / listener / alias name to connect to.

.PARAMETER SqlCredential
    Optional PSCredential.

.OUTPUTS
    [string]  The primary replica name (server\instance) if in AG; otherwise @@SERVERNAME.

.EXAMPLE
    Get-DbaPrimaryServerName -SqlInstance "MESLISTENER"
    # -> e.g. "SQL-MERTIOS1\INST1"

.EXAMPLE
    Get-DbaPrimaryServerName -SqlInstance "sql-alias"
    # -> e.g. "SQL-DEV01" (standalone)
#>
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)]
        [string] $SqlInstance,

        [PSCredential] $SqlCredential
    )

    # Require Invoke-DbaQuery but do NOT import anything automatically
    if (-not (Get-Command Invoke-DbaQuery -ErrorAction SilentlyContinue)) {
        Write-Warning "Invoke-DbaQuery (dbatools) is not available. Please 'Import-Module dbatools' first."
        return $SqlInstance
    }

    # First: normalize to the actual connected server name (not the alias/listener)
    $serverName = $null
    try {
        $row = Invoke-DbaQuery -SqlInstance $SqlInstance -SqlCredential $SqlCredential `
            -Query "SELECT CONVERT(sysname, @@SERVERNAME) AS ServerName,
                           CONVERT(tinyint, SERVERPROPERTY('IsHadrEnabled')) AS IsHadrEnabled;"
        $serverName = $row.ServerName
        $isHadr     = [int]$row.IsHadrEnabled
    }
    catch {
        # If even this fails, last resort is to return the original input
        return $SqlInstance
    }

    if (-not $serverName) {
        return $SqlInstance
    }

    # If AG is enabled, try to get the current primary from DMV
    if ($isHadr -eq 1) {
        try {
            # Works on primary and secondary: everyone knows who the current primary is
            $p = Invoke-DbaQuery -SqlInstance $SqlInstance -SqlCredential $SqlCredential `
                -Query "SELECT TOP(1) CONVERT(sysname, primary_replica) AS PrimaryReplica
                        FROM sys.dm_hadr_availability_group_states WITH (NOLOCK);"
            $primaryReplica = $p.PrimaryReplica

            if ($primaryReplica) {
                return [string]$primaryReplica
            }
        }
        catch {
            # No VIEW SERVER STATE or other DMV access issue → fall through to @@SERVERNAME
        }
    }

    # Not in AG or couldn’t read AG DMVs → return the real connected server name
    return [string]$serverName
}

