function Compare-MSAlwaysOnAgentJob
{
<#
.SYNOPSIS
Compare SQL Server Agent jobs between the primary and secondary replicas of an AlwaysOn Availability Group.

.DESCRIPTION
This function retrieves the primary and secondary replicas of a SQL Server AlwaysOn Availability Group
and compares SQL Server Agent jobs between the primary replica and each secondary replica.

The comparison is performed using the function Compare-MSAgentJob. The function can optionally:

- Show only differences between jobs
- Filter jobs by name or category
- Exclude specific jobs or categories
- Ignore the Enabled status of jobs
- Perform case-insensitive comparisons
- Display results as objects or formatted tables

This is useful to validate that SQL Agent jobs are consistent across all replicas of an AlwaysOn cluster.

.PARAMETER SqlInstance
SQL Server instance name that belongs to the AlwaysOn Availability Group.

.PARAMETER Job
Optional list of job names to include in the comparison.

.PARAMETER Category
Optional list of job categories to include in the comparison.

.PARAMETER ExcludeJob
List of job names to exclude from the result.

.PARAMETER ExcludeCategory
List of job categories to exclude from the result.

.PARAMETER IgnoreEnabled
Ignore differences in job enabled status.

.PARAMETER CaseInsensitive
Perform comparisons ignoring character case differences.

.PARAMETER OnlyDifference
Return only jobs that are different between replicas.

.PARAMETER ShowTable
Display results using Format-Table instead of returning objects.

.EXAMPLE
Compare-MSAlwaysOnAgentJob -SqlInstance "SQLPROD01"

Compares SQL Agent jobs between the primary replica and all secondary replicas
and returns only the differences.

.EXAMPLE
Compare-MSAlwaysOnAgentJob `
    -SqlInstance "SQLPROD01" `
    -ExcludeCategory "Database Maintenance"

Compares jobs but excludes jobs belonging to the category "Database Maintenance".

.EXAMPLE
Compare-MSAlwaysOnAgentJob `
    -SqlInstance "SQLPROD01" `
    -ShowTable

Displays the comparison results as a formatted table.

.NOTES
Requires the following helper functions to exist in the environment:

- Get-MSPrimaryServerName
- Get-MSSecondaryServerName
- Compare-MSAgentJob

Typically used in AlwaysOn environments to ensure SQL Agent job consistency
across replicas.
#>

    [CmdletBinding()]
    param(
        [Parameter(Mandatory)]
        [string] $SqlInstance,

        [string[]] $Job,
        [string[]] $Category,

        [string[]] $ExcludeJob,
        [string[]] $ExcludeCategory,

        [switch]  $IgnoreEnabled = $true,
        [switch]  $CaseInsensitive = $true,
        [switch]  $OnlyDifference = $true,
        [switch]  $ShowTable
    )

    function Write-LogSafe {
        param([string]$Message, [string]$Level = 'Info')

        if (Get-Command -Name Write-MSLog -ErrorAction SilentlyContinue) {
            try { Write-MSLog -Message $Message -Level $Level }
            catch { Write-Verbose $Message }
        }
        else {
            Write-Verbose $Message
        }
    }

    $primaryServer   = Get-MSPrimaryServerName   -SqlInstance $SqlInstance
    $SecondaryServer = Get-MSSecondaryServerName -SqlInstance $SqlInstance -AsArray

    Write-LogSafe -Message "Primary server   : $primaryServer"
    Write-LogSafe -Message "Secondary server : $($SecondaryServer -join ', ')"

    $Compare = foreach ($Secondary in $SecondaryServer)
    {
        Compare-MSAgentJob `
            -Source $primaryServer `
            -Destination $Secondary `
            -IgnoreEnabled:$IgnoreEnabled `
            -CaseInsensitive:$CaseInsensitive
    }

    # Only difference
    if ($OnlyDifference)
    {
        $Compare = $Compare | Where-Object Status -ne 'Same'
    }

    # Apply filters
    if ($PSBoundParameters.ContainsKey('Job'))
    {
        Write-LogSafe -Message "Filter by job name: $($Job -join ', ')"
        $Compare = $Compare | Where-Object { $_.JobName -in $Job }
    }
    
    if ($PSBoundParameters.ContainsKey('Category'))
    {
        Write-LogSafe -Message "Filter by category name"
        $Compare = $Compare | Where-Object { $_.Category -in $Category }
    }
    
    # Apply exclusion filters
    if ($ExcludeJob)
    {
        Write-LogSafe -Message "Exclude by job name"
        $Compare = $Compare | Where-Object { $_.JobName -notin $ExcludeJob }
    }

    if ($ExcludeCategory)
    {
        Write-LogSafe -Message "Exclude by category name"
        $Compare = $Compare | Where-Object { $_.Category -notin $ExcludeCategory }
    }

    # Output
    if ($ShowTable.IsPresent)
    {
        $Compare | Format-Table InstanceSource,InstanceDestination,JobName,Status,Differences,IsEnabled,Category -Auto
    }
    else
    {
        $Compare
    }
}