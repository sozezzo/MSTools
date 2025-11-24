function Update-DbaJobCluster {
<#
.SYNOPSIS
  Update SQL Agent jobs from a source to a destination using Compare-DbaAgentJob.

.DESCRIPTION
  Compares jobs between Source and Destination using Compare-DbaAgentJob with no pre-filters.
  Inside the loop, applies source-side eligibility checks:
    - Category filter on source only (supports multiple categories).
    - Only jobs that are IsEnabled on the source are eligible.
  Copy-DbaAgentJob is called only when:
    1) the job is MissingOnDestination, or
    2) the job is Different and -Force is specified.
  Every job not copied is logged as skip with the reason.
  Supports -WhatIf and -Verbose.

.PARAMETER Source
  Source SQL Server instance (primary).

.PARAMETER Destination
  Destination SQL Server instance (secondary).

.PARAMETER Category
  Optional source-only category filter. Accepts one or more category names.

.PARAMETER IncludeSystemJobs
  Include common system jobs such as syspolicy_purge_history.

.PARAMETER Force
  Copy jobs reported as Different.

.PARAMETER DisableOnDestination
  Switch. If present, copied jobs are disabled on the destination after copy (passed through to Copy-DbaAgentJob).

.EXAMPLE
  Update-DbaJobCluster -Source SQLPRI -Destination SQLSEC

.EXAMPLE
  Update-DbaJobCluster -Source SQLPRI -Destination SQLSEC -Category "Maintenance","Backup"

.EXAMPLE
  Update-DbaJobCluster -Source SQLPRI -Destination SQLSEC -Force -DisableOnDestination

.EXAMPLE
  Update-DbaJobCluster -Source SQLPRI -Destination SQLSEC -Category "Maintenance" -WhatIf -Verbose

.NOTES
  Author  : Sozezzo Astra
  Version : 1.3.0
  Date    : 2025-11-04
#>
  [CmdletBinding(SupportsShouldProcess = $true, ConfirmImpact = 'Medium')]
  param(
    [Parameter(Mandatory)][string]$Source,
    [Parameter(Mandatory)][string]$Destination,
    [string[]]$Category,
    [switch]$IncludeSystemJobs,
    [switch]$Force,
    [switch]$DisableOnDestination
  )

  try {
    Write-Log -Level Info -Message "Starting Update-DbaJobCluster"
    Write-Log -Level Info -Message "Source: $Source"
    Write-Log -Level Info -Message "Destination: $Destination"
    if ($Category) { Write-Log -Level Info -Message "Source category filter inside loop: $($Category -join ', ')" }
    Write-Log -Level Info -Message "Only jobs IsEnabled on source are eligible"
    if ($Force)   { Write-Log -Level Info -Message "Force enabled for Different jobs" }
    Write-Log -Level Info -Message "DisableOnDestination: $DisableOnDestination"

    # System jobs to skip unless explicitly included
    $systemSkip = @('syspolicy_purge_history','sp_delete_backuphistory','sp_purge_jobhistory')

    # Compare without pre-filtering: we want to log all outcomes
    Write-Log -Level Info -Message "Comparing jobs between [$Source] and [$Destination]..."
    $compare = Compare-DbaAgentJob -Source $Source -Destination $Destination
    if (-not $compare) {
      Write-Log -Level Info -Message "Compare-DbaAgentJob returned no results."
      return
    }

    $copied  = 0
    $skipped = 0

    foreach ($row in $compare) {
      $name      = $row.JobName
      $cat       = $row.Category
      $isEnabled = $row.IsEnabled
      $status    = $row.Status

      # Skip system jobs unless requested
      if (-not $IncludeSystemJobs -and ($systemSkip -contains $name)) {
        Write-Log -Level Info -Message "skip: $name is a system job"
        $skipped++; continue
      }

      # Source-only category filter (supports multiple)
      if ($Category -and ($Category -notcontains $cat)) {
        Write-Log -Level Info -Message "skip: $name category [$cat] not in selected list ($($Category -join ', '))"
        $skipped++; continue
      }

      # Source-only enabled filter
      if (-not $isEnabled) {
        Write-Log -Level Info -Message "skip: $name is disabled on source"
        $skipped++; continue
      }

      switch ($status) {
        'MissingOnDestination' {
          $target = "$Destination job $name"
          $action = "Copy from $Source"
          if ($PSCmdlet.ShouldProcess($target, $action)) {
            $params = @{
              Source      = $Source
              Destination = $Destination
              Job         = $name
              Force       = $true
              ErrorAction = 'Stop'
              Verbose     = $VerbosePreference
              WhatIf      = $WhatIfPreference
            }
            if ($DisableOnDestination.IsPresent) { $params.DisableOnDestination = $true }

            try {
              Write-Log -Level Info -Message "copy: $name reason MissingOnDestination"
              Copy-DbaAgentJob @params
              $copied++
            }
            catch {
              Write-Log -Level Error -Message "Copy failed for $name : $($_.Exception.Message)"
            }
          }
          else {
            if ($WhatIfPreference) {
              Write-Log -Level Info -Message "whatif: would copy $name reason MissingOnDestination to $Destination from $Source"
            }
          }
          break
        }

        'Different' {
          if ($Force) {
            $target = "$Destination job $name"
            $action = "Copy from $Source"
            if ($PSCmdlet.ShouldProcess($target, $action)) {
              $params = @{
                Source      = $Source
                Destination = $Destination
                Job         = $name
                Force       = $true
                ErrorAction = 'Stop'
                Verbose     = $VerbosePreference
                WhatIf      = $WhatIfPreference
              }
              if ($DisableOnDestination.IsPresent) { $params.DisableOnDestination = $true }

              try {
                Write-Log -Level Info -Message "copy: $name reason Different and Force specified"
                Copy-DbaAgentJob @params
                $copied++
              }
              catch {
                Write-Log -Level Error -Message "Copy failed for $name : $($_.Exception.Message)"
              }
            }
            else {
              if ($WhatIfPreference) {
                Write-Log -Level Info -Message "whatif: would copy $name reason Different and Force specified to $Destination from $Source"
              }
            }
          }
          else {
            Write-Log -Level Info -Message "skip: $name is Different but Force not specified"
            $skipped++
          }
          break
        }

        'Same' {
          Write-Log -Level Info -Message "skip: $name is Same"
          $skipped++
          break
        }

        'MissingOnSource' {
          Write-Log -Level Info -Message "skip: $name exists only on destination (MissingOnSource)"
          $skipped++
          break
        }

        default {
          Write-Log -Level Info -Message "skip: $name status $status"
          $skipped++
        }
      }
    }

    Write-Log -Level Info -Message "Update-DbaJobCluster completed. Copied: $copied. Skipped: $skipped."
  }
  catch {
    Write-Log -Level Error -Message "Unexpected error: $($_.Exception.Message)"
  }
}

