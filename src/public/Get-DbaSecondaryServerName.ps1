function Get-DbaSecondaryServerName {
<#
.SYNOPSIS
    Returns the secondary replica name(s) for a SQL Server AlwaysOn AG.

.DESCRIPTION
    DMV-based (alias-proof). If the local instance isn’t in an AG or lacks visibility, returns $null.
    - Default: all secondary names as a comma-separated string.
    - -AsArray : all secondary names as string[].
    - -MainOnly: one “main” secondary (prefers SYNCHRONOUS_COMMIT + AUTOMATIC + HEALTHY).
                 With -AsArray, still returns a single-element array.

.PARAMETER SqlInstance
    Server\Instance, hostname, FQDN, or client alias.

.PARAMETER MainOnly
    Return only the best candidate secondary (see selection rules).

.PARAMETER AsArray
    Return string[] instead of a comma-separated string.

.EXAMPLE
    Get-DbaSecondaryServerName -SqlInstance SQL-ALIAS

.EXAMPLE
    Get-DbaSecondaryServerName -SqlInstance SQL-ALIAS -AsArray

.EXAMPLE
    Get-DbaSecondaryServerName -SqlInstance SQL-ALIAS -MainOnly

.NOTES
    Author  : Sozezzo Astra
    Version : 1.4 (DMV-based; PS 5.1 compatible)
#>
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)]
        [string] $SqlInstance,

        [switch] $MainOnly,
        [switch] $AsArray
    )

    try {
        $smo = Connect-DbaInstance -SqlInstance $SqlInstance -ErrorAction Stop

        # Pull per-replica metadata we need to rank secondaries
        $q = @"
SELECT 
    ag.name                                   AS ag_name,
    r.replica_server_name                     AS replica_server_name,
    ars.is_local                              AS is_local,
    ars.role_desc                             AS role_desc,
    r.availability_mode_desc                  AS availability_mode_desc,
    r.failover_mode_desc                      AS failover_mode_desc,
    ars.synchronization_health_desc           AS synchronization_health_desc,
    r.backup_priority                         AS backup_priority
FROM sys.dm_hadr_availability_replica_states AS ars
JOIN sys.availability_replicas               AS r   ON r.replica_id = ars.replica_id
JOIN sys.availability_groups                 AS ag  ON ag.group_id   = r.group_id
"@

        $rows = Invoke-DbaQuery -SqlInstance $smo -Query $q -ErrorAction Stop
        if (-not $rows -or $rows.Count -eq 0) { return $null }

        # Identify local AGs (is_local = 1)
        $locals = $rows | Where-Object { $_.is_local -eq 1 }
        if (-not $locals -or $locals.Count -eq 0) { return $null }

        $allSecondaries = @()

        foreach ($local in $locals) {
            $agName = $local.ag_name
            $role   = [string]$local.role_desc
            $thisAg = $rows | Where-Object { $_.ag_name -eq $agName }

            # Collect the secondaries for this AG as objects we can rank
            $secs = $thisAg | Where-Object { $_.role_desc -eq 'SECONDARY' } | ForEach-Object {
                [pscustomobject]@{
                    ag_name                     = $_.ag_name
                    replica_server_name         = [string]$_.replica_server_name
                    availability_mode_desc      = [string]$_.availability_mode_desc      # SYNCHRONOUS_COMMIT | ASYNCHRONOUS_COMMIT
                    failover_mode_desc          = [string]$_.failover_mode_desc          # AUTOMATIC | MANUAL
                    synchronization_health_desc = [string]$_.synchronization_health_desc # HEALTHY | PARTIALLY_HEALTHY | NOT_HEALTHY
                    backup_priority             = [int]$_.backup_priority                # 0..100 (higher is more preferred for backups)
                }
            }

            if ($role -eq 'SECONDARY') {
                # If I'm secondary, per earlier spec, include myself
                $self = [pscustomobject]@{
                    ag_name                     = $local.ag_name
                    replica_server_name         = [string]$local.replica_server_name
                    availability_mode_desc      = [string]$local.availability_mode_desc
                    failover_mode_desc          = [string]$local.failover_mode_desc
                    synchronization_health_desc = [string]$local.synchronization_health_desc
                    backup_priority             = [int]$local.backup_priority
                }
                if ($self.replica_server_name) { $secs = @($self) + $secs | Select-Object -Unique -Property replica_server_name, * }
            }

            if ($secs) { $allSecondaries += $secs }
        }

        if (-not $allSecondaries -or $allSecondaries.Count -eq 0) { return $null }

        # Deduplicate across AGs (same name possibly appears multiple times)
        $allSecondaries = $allSecondaries | Sort-Object replica_server_name -Unique

        if ($MainOnly) {
            # Rank secondaries (best failover partner first)
            # Priority rules:
            #   1) SYNCHRONOUS_COMMIT over ASYNCHRONOUS_COMMIT
            #   2) AUTOMATIC over MANUAL
            #   3) HEALTHY over others (PARTIALLY_HEALTHY, NOT_HEALTHY)
            #   4) Higher backup_priority preferred (tiebreaker)
            #   5) Name (stable tiebreaker)
            $ranked = $allSecondaries | Sort-Object `
                @{e={ if ($_.availability_mode_desc -eq 'SYNCHRONOUS_COMMIT') {0} else {1} }}, `
                @{e={ if ($_.failover_mode_desc      -eq 'AUTOMATIC')        {0} else {1} }}, `
                @{e={ switch ($_.synchronization_health_desc) { 'HEALTHY' {0}; 'PARTIALLY_HEALTHY' {1}; default {2} } }}, `
                @{e={ -1 * $_.backup_priority }}, `
                @{e={$_.replica_server_name}}

            $best = $ranked | Select-Object -First 1
            if (-not $best) { return $null }

            if ($AsArray) { return @($best.replica_server_name) }
            return $best.replica_server_name
        }
        else {
            $names = $allSecondaries | Select-Object -ExpandProperty replica_server_name | Where-Object { $_ } | Select-Object -Unique
            if (-not $names -or $names.Count -eq 0) { return $null }
            if ($AsArray) { return @($names) }
            return ($names -join ',')
        }
    }
    catch {
        Write-Warning "Failed to determine secondary for [$SqlInstance]: $($_.Exception.Message)"
        return $null
    }
}
