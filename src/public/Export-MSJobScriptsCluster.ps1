function Export-MSJobScriptsCluster {
<#
.SYNOPSIS
    Exports SQL Agent job scripts from the primary and secondary servers of a cluster.

.DESCRIPTION
    Detects the primary and secondary nodes of a SQL Server instance and exports
    SQL Agent job scripts into the specified path. You can filter by category or job name.

.PARAMETER SqlInstance
    The SQL Server instance name or alias.

.PARAMETER Path
    The directory where the exported scripts will be stored.

.PARAMETER Category
    Optional. Export only jobs in this category.

.PARAMETER Job
    Optional. Export only the job with this name.

.EXAMPLE
    Export-MSJobScriptsCluster -SqlInstance "sql-cluster" -Path "C:\temp\sql-cluster"

.EXAMPLE
    Export-MSJobScriptsCluster -SqlInstance "sql-cluster" -Path "C:\temp" -Category "Maintenance"

.EXAMPLE
    Export-MSJobScriptsCluster -SqlInstance "sql-cluster" -Path "C:\temp" -Job "Index Optimize"

.NOTES
    Author: Sozezzo Astra
    Version: 1.2
#>
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][string]$SqlInstance,
        [Parameter(Mandatory)][string]$Path,
        [string]$Category,
        [string]$Job
    )

    try {
        Write-MSLog -Message "Starting job export for instance [$SqlInstance]" -Level Info

        # --- Validate and prepare output path ---
        if (-not (Test-Path -LiteralPath $Path)) {
            Write-MSLog -Message "Creating folder: $Path" -Level Info
            New-Item -ItemType Directory -Force -Path $Path | Out-Null
        }

        # --- Identify primary and secondary nodes ---
        $PrimaryServerName   = Get-MSPrimaryServerName   -SqlInstance $SqlInstance
        $SecondaryServerName = Get-MSSecondaryServerName -SqlInstance $SqlInstance

        if ([string]::IsNullOrWhiteSpace($PrimaryServerName)) {
            Write-MSLog -Message "Unable to determine primary server for [$SqlInstance]" -Level Warning
            return
        }

        if ([string]::IsNullOrWhiteSpace($SecondaryServerName)) {
            Write-MSLog -Message "No secondary detected. Exporting from primary [$PrimaryServerName]." -Level Info
            $ExportTargets = @(@{ Server = $PrimaryServerName; Suffix = '_P' })
        }
        else {
            Write-MSLog -Message "Primary: [$PrimaryServerName], Secondary: [$SecondaryServerName]" -Level Info
            $ExportTargets = @(
                @{ Server = $PrimaryServerName;   Suffix = '_P' },
                @{ Server = $SecondaryServerName; Suffix = '_S' }
            )
        }

        # --- Export logic ---
        foreach ($target in $ExportTargets) {
            $serverName = $target.Server
            $suffix     = $target.Suffix
            $serverPath = Join-Path $Path "$($serverName)$suffix"

            Write-MSLog -Message "Processing server [$serverName]" -Level Info
            New-Item -ItemType Directory -Force -Path $serverPath | Out-Null

            $jobs = Get-DbaAgentJob -SqlInstance $serverName -ErrorAction Stop

            # Apply filters
            if ($Category) {
                $jobs = $jobs | Where-Object { $_.Category -eq $Category }
                Write-MSLog -Message "Filtered by category [$Category]" -Level Debug
            }

            if ($Job) {
                $jobs = $jobs | Where-Object { $_.Name -eq $Job }
                Write-MSLog -Message "Filtered by job [$Job]" -Level Debug
            }

            if (-not $jobs) {
                Write-MSLog -Message "No jobs found for [$serverName] with specified filters." -Level Warning
                continue
            }

            foreach ($j in $jobs) {
                # Clean job name
                $cleanName = ($j.Name -replace '[^a-zA-Z0-9_\- ]', '_').Trim()
                $cleanName = ($cleanName -replace '\s+', '_')

                $fileName = "$cleanName.job.sql"
                $filePath = Join-Path $serverPath $fileName

                $n = 1
                while (Test-Path -LiteralPath $filePath) {
                    $filePath = Join-Path $serverPath "$cleanName($n).job.sql"
                    $n++
                }

                Write-MSLog -Message "Exporting job [$($j.Name)] to [$filePath]" -Level Info
                Export-DbaScript -InputObject $j -FilePath $filePath -ErrorAction Stop
            }
        }

        Write-MSLog -Message "* All jobs exported successfully to: $Path" -Level Info
    }
    catch {
        Write-MSLog -Message "* Error exporting jobs for [$SqlInstance]: $($_.Exception.Message)" -Level Error
    }
}