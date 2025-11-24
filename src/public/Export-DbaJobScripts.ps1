function Export-DbaJobScripts {
<#
.SYNOPSIS
    Exports SQL Agent Job scripts to individual .job.sql files.

.DESCRIPTION
    Uses Get-DbaAgentJob and Export-DbaScript (dbatools) to export each job definition
    without altering its content. Invalid filename characters are replaced with underscores.
    If duplicate sanitized names occur, numeric suffixes (e.g., (1), (2)) are appended.
    Supports filtering by Job name and Category.

.PARAMETER SqlInstance
    SQL Server instance name or alias.

.PARAMETER Path
    Destination folder where the .job.sql files will be saved.

.PARAMETER Job
    One or more job names to include (exact match, case-insensitive).

.PARAMETER Category
    One or more job categories to include (exact match, case-insensitive).

.EXAMPLE
    Export-DbaJobScript -SqlInstance SQLPROD01 -Path C:\Backups\Jobs

.EXAMPLE
    Export-DbaJobScript -SqlInstance SQLPROD01 -Path C:\Backups\Jobs -Job "Test Base","Test\Base"

.EXAMPLE
    Export-DbaJobScript -SqlInstance SQLPROD01 -Path C:\Backups\Jobs -Category "Database Maintenance"

.NOTES
    Author: Sozezzo Astra
    Version: 1.4
#>
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][string]$SqlInstance,
        [Parameter(Mandatory)][string]$Path,
        [string[]]$Job,
        [string[]]$Category
    )

    $formattedDateTime = (Get-Date).ToString("yyyy-MM-dd_HHmmss")

    try {
        if (-not (Test-Path -LiteralPath $Path)) {
            New-Item -ItemType Directory -Force -Path $Path | Out-Null
            Write-Log -Message "Created destination folder: $Path" -Level Info
        }

        $jobs = Get-DbaAgentJob -SqlInstance $SqlInstance -ErrorAction Stop
        $jobs = $jobs | Where-Object { $_ -ne $null -and $_.PSObject -and $_.Name }

        Write-Log -Message "Retrieved $($jobs.Count) jobs from instance [$SqlInstance]" -Level Info

        # Apply filters
        if ($Job -and $Job.Count -gt 0) {
            $nameSet = [System.Collections.Generic.HashSet[string]]::new([System.StringComparer]::OrdinalIgnoreCase)
            $Job | ForEach-Object { [void]$nameSet.Add($_) }
            $jobs = $jobs | Where-Object { $nameSet.Contains($_.Name) }
            Write-Log -Message "Applied job filter: $($Job -join ', ')" -Level Info
        }

        if ($Category -and $Category.Count -gt 0) {
            $catSet = [System.Collections.Generic.HashSet[string]]::new([System.StringComparer]::OrdinalIgnoreCase)
            $Category | ForEach-Object { [void]$catSet.Add($_) }
            $jobs = $jobs | Where-Object { $_.Category -and $catSet.Contains($_.Category) }
            Write-Log -Message "Applied category filter: $($Category -join ', ')" -Level Info
        }

        if (-not $jobs -or $jobs.Count -eq 0) {
            Write-Log -Message "No jobs matched the specified filters on instance [$SqlInstance]" -Level Warn
            return
        }

        $usedNames = @{}

        foreach ($jobObj in $jobs) {
            if ($jobObj -is [string]) { continue }

            $jobName = $jobObj.Name

            # Sanitize file name
            $safeName = [Regex]::Replace($jobName, '[\\\/:\*\?"<>\|]', '_')
            $safeName = $safeName -replace '\s+', '_'
            $safeName = $safeName.Trim('.')
            if ([string]::IsNullOrWhiteSpace($safeName)) { $safeName = 'Job' }

            if ($usedNames.ContainsKey($safeName)) {
                $usedNames[$safeName]++
                $finalName = "$safeName($($usedNames[$safeName]))"
            } else {
                $usedNames[$safeName] = 0
                $finalName = $safeName
            }

            $filePath = Join-Path $Path "job.$finalName.$formattedDateTime.sql"

            try {
                # Export as text and write directly
                $scriptText = Export-DbaScript -InputObject $jobObj -Passthru -ErrorAction Stop | Out-String
                Set-Content -LiteralPath $filePath -Value $scriptText -Encoding UTF8
                Write-Log -Message "Exported job [$jobName] to $filePath" -Level Info
            }
            catch {
                Write-Log -Message "Failed to export job [$jobName]: $($_.Exception.Message)" -Level Error
            }
        }

        Write-Log -Message "Job export completed for instance [$SqlInstance]" -Level Info
    }
    catch {
        Write-Log -Message "Unhandled error in Export-DbaJobScript: $($_.Exception.Message)" -Level Error
    }
}

