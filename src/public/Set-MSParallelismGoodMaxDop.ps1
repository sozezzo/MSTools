function Set-MSParallelismGoodMaxDop
{
<#
.SYNOPSIS
Sets or recommends a safe MAXDOP (Max Degree of Parallelism) value for a SQL Server instance based on NUMA topology.

.DESCRIPTION
This function evaluates the SQL Server CPU topology using sys.dm_os_schedulers
and calculates a recommended MAXDOP value using the Microsoft baseline guideline:

    MAXDOP = MIN(8, schedulers per NUMA node)

The function:
- Retrieves the current MAXDOP configuration.
- Determines schedulers per NUMA node.
- Calculates a topology-based recommendation.
- Optionally applies the new value using sp_configure.
- Supports -WhatIf and -Confirm.

The recommendation is based strictly on hardware topology and does NOT
analyze workload type, query patterns, wait statistics, or concurrency.

.PARAMETER SqlInstance
Target SQL Server instance name.

.PARAMETER SqlCredential
Optional SQL credential for authentication.

.PARAMETER MaxDop
Optional explicit MAXDOP value. If provided, the automatic calculation
is bypassed and this value will be used instead.

.EXAMPLE
Set-MSParallelismGoodMaxDop -SqlInstance "SQLPROD01"

Calculates recommended MAXDOP based on NUMA topology and applies it
if different from the current value.

.EXAMPLE
Set-MSParallelismGoodMaxDop -SqlInstance "SQLPROD01" -MaxDop 4

Forces MAXDOP to 4 regardless of topology calculation.

.EXAMPLE
Set-MSParallelismGoodMaxDop -SqlInstance "SQLPROD01" -WhatIf

Shows what would change without applying it.

.WHY_USE
Improper MAXDOP configuration can cause:

- Worker thread starvation
- Excessive CXPACKET / CXCONSUMER waits
- Poor concurrency under OLTP workloads
- CPU skew across NUMA nodes

This function provides a safe baseline aligned with Microsoft
recommendations and is useful for:

- Standardizing new SQL Server deployments
- Correcting legacy configurations (e.g., MAXDOP = 0 or very high values)
- Validating topology alignment across environments

It is especially useful in environments with multiple NUMA nodes
where MAXDOP should not exceed schedulers per NUMA node.

.WARNINGS
1) This recommendation is topology-based ONLY.
   It does NOT evaluate workload type.

2) OLTP systems often perform better with lower MAXDOP values
   (e.g., 1–4), even if topology allows 8.

3) Data warehouse or reporting systems may benefit from higher
   values than 8 in controlled environments.

4) Never apply changes blindly across production servers without
   validating:
      - Wait statistics
      - Query Store behavior
      - CPU utilization
      - Parallel worker usage

5) Changing MAXDOP can significantly impact performance and
   concurrency. Always test in lower environments first.

.NOTES
Author: Sozezzo Astra
Version: 1.0
Compatibility: SQL Server 2012+
Requires: dbatools module
#>

    [CmdletBinding(SupportsShouldProcess = $true)]
    param (
        [Parameter(Mandatory)]
        [string] $SqlInstance,

        [PSCredential] $SqlCredential,

        [int] $MaxDop 
    )

    # --- Get current MAXDOP ---
    $current = Get-DbaSpConfigure -SqlInstance $SqlInstance `
                                  -SqlCredential $SqlCredential `
                                  -Name 'max degree of parallelism'

    $currentMaxDop = $current.RunningValue

    # --- Calculate schedulers per NUMA node ---
    $schedulerQuery = @"
WITH VisibleSchedulers AS (
    SELECT parent_node_id
    FROM sys.dm_os_schedulers
    WHERE status = 'VISIBLE ONLINE'
      AND scheduler_id < 255
),
SchedulersPerNuma AS (
    SELECT parent_node_id, COUNT(*) AS schedulers
    FROM VisibleSchedulers
    GROUP BY parent_node_id
)
SELECT
    MAX(schedulers) AS schedulers_per_numa,
    COUNT(*) AS numa_nodes
FROM SchedulersPerNuma;
"@

    $topology = Invoke-DbaQuery -SqlInstance $SqlInstance `
                                -SqlCredential $SqlCredential `
                                -Query $schedulerQuery

    $schedulersPerNuma = [int]$topology.schedulers_per_numa
    $numaNodes = [int]$topology.numa_nodes

    if ($schedulersPerNuma -lt 1) {
        throw "Unable to determine schedulers per NUMA node."
    }

    # --- Determine recommended MAXDOP ---
    if ($PSBoundParameters.ContainsKey('MaxDop')) {
        $targetMaxDop = $MaxDop
        $decision = "Explicit MAXDOP value provided by parameter."
    }
    else {
        $targetMaxDop = [Math]::Min(8, $schedulersPerNuma)
        $decision = "Calculated baseline MAXDOP = MIN(8, schedulers per NUMA node)."
    }

    # --- Explanation output ---
    Write-Host ""
    Write-Host "SQL Instance               : $SqlInstance"
    Write-Host "Current MAXDOP             : $currentMaxDop"
    Write-Host "NUMA nodes                 : $numaNodes"
    Write-Host "Schedulers per NUMA node   : $schedulersPerNuma"
    Write-Host "Recommended MAXDOP         : $targetMaxDop"
    Write-Host "Decision                   : $decision"
    Write-Host ""

    # --- Apply change ---
    if ($currentMaxDop -eq $targetMaxDop) {
        Write-Host "MAXDOP already set to the desired value. No change required."
        return
    }

    if ($PSCmdlet.ShouldProcess(
        $SqlInstance,
        "Set MAXDOP from $currentMaxDop to $targetMaxDop"
    )) {
        Set-DbaSpConfigure -SqlInstance $SqlInstance `
                           -SqlCredential $SqlCredential `
                           -Name 'max degree of parallelism' `
                           -Value $targetMaxDop

        Write-Host "MAXDOP updated successfully."
    }
}