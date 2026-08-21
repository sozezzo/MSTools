function Export-MSJobScripts {
<#
.SYNOPSIS
    Exports SQL Agent Jobs as .sql scripts from one or more SQL Server instances.

.DESCRIPTION
    Export-MSJobScripts extracts SQL Agent Jobs using dbatools and saves
    them as individual .sql files.

    The function supports:
      - Multiple SQL instances
      - Optional job filtering and exclusion
      - Optional category filtering (exact, case-insensitive)
      - Optional manifest-based incremental exports
      - Timestamped or stable filenames
      - Version-safe dbatools parameter usage

    When -UseManifest is enabled, only new or modified jobs are exported.
    Job renames are also detected and re-exported.

.PARAMETER SqlInstance
    One or more SQL Server instances.

.PARAMETER Path
    Root output folder for exported job scripts.

.PARAMETER UseManifest
    Enables manifest-based incremental export.

.PARAMETER NoTimestamp
    Disables timestamp in filenames.
    Existing files are overwritten.

.PARAMETER Job
    Optional list of job names to include.

.PARAMETER ExcludeJob
    Optional list of job names to exclude.

.PARAMETER Category
    Optional list of SQL Agent job categories to include.
    Matching is exact and case-insensitive.

.PARAMETER Encoding
    Output file encoding (default UTF8).

.PARAMETER NoPrefix
    Removes dbatools script prefix if supported.

.PARAMETER NoInstancePrefix
    Omits the instance/server name prefix from the exported .sql file names.
    Useful when each instance already has its own output folder, so the file
    names do not repeat the server name. The internal manifest file name is
    not affected.

.EXAMPLE
    Export-MSJobScripts `
        -SqlInstance SQL01 `
        -Path D:\Backups\Jobs

.EXAMPLE
    Export-MSJobScripts `
        -SqlInstance SQL01,SQL02 `
        -Path D:\Backups\Jobs `
        -UseManifest

.EXAMPLE
    Export-MSJobScripts `
        -SqlInstance SQL01 `
        -Path D:\Backups\Jobs `
        -Category Maintenance `
        -NoTimestamp

.EXAMPLE
    Export-MSJobScripts `
        -SqlInstance SQL01 `
        -Path D:\Backups\Jobs `
        -ExcludeJob 'syspolicy_purge_history'

.NOTES
    Author  : Sozezzo Astra
    Module  : MSTools
    Version : 1.0
#>

    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]
        [string[]]$SqlInstance,

        [Parameter(Mandatory = $true)]
        [string]$Path,

        [Parameter(Mandatory = $false)]
        [switch]$UseManifest,

        [Parameter(Mandatory = $false)]
        [switch]$NoTimestamp,

        [Parameter(Mandatory = $false)]
        [string[]]$Job,

        [Parameter(Mandatory = $false)]
        [string[]]$ExcludeJob,

        [Parameter(Mandatory = $false)]
        [string[]]$Category,

        [Parameter(Mandatory = $false)]
        [ValidateSet('ASCII','BigEndianUnicode','Byte','String','Unicode','UTF7','UTF8','Unknown')]
        [string]$Encoding = 'UTF8',

        [Parameter(Mandatory = $false)]
        [switch]$NoPrefix,

        [Parameter(Mandatory = $false)]
        [switch]$NoInstancePrefix

    )

    Write-MSLog -Level Info -Message 'Export-MSJobScripts: START'

    # ---------------------------------------------------------------------
    # Helper: convert arbitrary text to filesystem-safe token
    # ---------------------------------------------------------------------
    function ConvertTo-SafeFileToken {
        param([Parameter(Mandatory)][string]$Text)

        $safe = $Text -replace '[\\\/:\*\?"<>\|\[\]]', '_'
        #$safe = $Text -replace '[\\\/:\*\?"<>\|]', '_'
        $safe = $safe -replace '[\x00-\x1F]', ''
        $safe = $safe.Trim().TrimEnd('.')
        $safe = $safe -replace '_{2,}', '_'

        if ([string]::IsNullOrWhiteSpace($safe)) {
            $safe = '_'
        }

        return $safe
    }

    # ---------------------------------------------------------------------
    # Helper: add parameter only if supported by installed dbatools version
    # ---------------------------------------------------------------------
    function Add-IfSupported {
        param(
            [Parameter(Mandatory)][string]$CommandName,
            [Parameter(Mandatory)][hashtable]$Splat,
            [Parameter(Mandatory)][string]$ParamName,
            [Parameter(Mandatory)]$Value
        )

        $cmd = Get-Command -Name $CommandName -ErrorAction Stop
        if ($cmd.Parameters.ContainsKey($ParamName)) {
            $Splat[$ParamName] = $Value
        }
    }

    # ---------------------------------------------------------------------
    # Preconditions
    # ---------------------------------------------------------------------
    if (-not (Get-Module -ListAvailable -Name dbatools)) {
        throw 'dbatools module is required.'
    }

    if (-not (Test-Path -LiteralPath $Path)) {
        Write-MSLog -Level Info -Message "Creating root folder [$Path]"
        $null = New-Item -Path $Path -ItemType Directory -Force -ErrorAction Stop
    }

    $runAt = Get-Date
    $multi = ($SqlInstance.Count -gt 1)

    $allResults = New-Object System.Collections.Generic.List[object]

    foreach ($instance in $SqlInstance) {

        Write-MSLog -Level Info -Message "Processing instance [$instance]"

        $instanceToken  = ConvertTo-SafeFileToken -Text $instance
        $instanceFolder = if ($multi) {
            Join-Path -Path $Path -ChildPath $instanceToken
        } else {
            $Path
        }

        if (-not (Test-Path -LiteralPath $instanceFolder)) {
            Write-MSLog -Level Debug -Message "Creating instance folder [$instanceFolder]"
            $null = New-Item -Path $instanceFolder -ItemType Directory -Force -ErrorAction Stop
        }

        # -----------------------------------------------------------------
        # Connect to SQL Server
        # -----------------------------------------------------------------
        $connectParams = @{
            SqlInstance = $instance
            ErrorAction = 'Continue'
        }
        Add-IfSupported -CommandName 'Connect-DbaInstance' -Splat $connectParams -ParamName 'EnableException' -Value $true

        try {
            Write-MSLog -Level Debug -Message "Connecting to [$instance]"
            $srv = Connect-DbaInstance @connectParams
        }
        catch {
            throw "Connect-DbaInstance failed for [$instance]. $($_.Exception.Message)"
        }

        # -----------------------------------------------------------------
        # Manifest handling
        # -----------------------------------------------------------------
        $manifest     = $null
        $manifestPath = Join-Path -Path $instanceFolder -ChildPath ("{0}.jobs.manifest.clixml" -f $instanceToken)

        if ($UseManifest) {

            Write-MSLog -Level Debug -Message "Manifest enabled for [$instance]"

            if (Test-Path -LiteralPath $manifestPath) {
                try {
                    $manifest = Import-Clixml -LiteralPath $manifestPath -ErrorAction Stop
                }
                catch {
                    Write-MSLog -Level Warning -Message "Manifest corrupted, recreating [$manifestPath]"
                    $stamp = Get-Date -Format 'yyyyMMdd-HHmmss'
                    Move-Item -Path $manifestPath -Destination "$manifestPath.corrupt.$stamp" -Force -ErrorAction SilentlyContinue
                }
            }

            if (-not $manifest) {
                $manifest = [pscustomobject]@{
                    ManifestVersion = 1
                    Server          = $instance
                    SqlInstance     = $instance
                    BackupFolder    = $instanceFolder
                    LastRunAt       = $null
                    LastBackupAt    = $null
                    Jobs            = @{}
                }
            }

            $manifest.LastRunAt = $runAt
        }

        # -----------------------------------------------------------------
        # Retrieve jobs
        # -----------------------------------------------------------------
        Write-MSLog -Level Debug -Message "Retrieving SQL Agent jobs"

        $getParams = @{
            SqlInstance = $srv
            ErrorAction = 'Stop'
        }

        if ($Job) {
            Add-IfSupported -CommandName 'Get-DbaAgentJob' -Splat $getParams -ParamName 'Job' -Value $Job
        }
        Add-IfSupported -CommandName 'Get-DbaAgentJob' -Splat $getParams -ParamName 'EnableException' -Value $true

        $jobs = Get-DbaAgentJob @getParams

        if ($ExcludeJob) {
            Write-MSLog -Level Debug -Message "Applying ExcludeJob filter"
            $excludeSet = [System.Collections.Generic.HashSet[string]]::new(
                [System.Collections.Generic.IEnumerable[string]]$ExcludeJob
            )
            $jobs = $jobs | Where-Object { -not $excludeSet.Contains($_.Name) }
        }

        if ($Category) {
            Write-MSLog -Level Debug -Message "Applying Category filter"
            $categorySet = [System.Collections.Generic.HashSet[string]]::new(
                $Category,
                [System.StringComparer]::OrdinalIgnoreCase
            )
            $jobs = $jobs | Where-Object { $_.Category -and $categorySet.Contains($_.Category) }
        }

        # -----------------------------------------------------------------
        # Export loop
        # -----------------------------------------------------------------
        $exportedCount = 0

        foreach ($j in $jobs) {

            $jobId   = if ($j.JobId) { $j.JobId.ToString() } elseif ($j.Id) { $j.Id.ToString() } else { [guid]::NewGuid().ToString() }
            $jobName = $j.Name

            Write-MSLog -Level Debug -Message "Evaluating job [$jobName]"

            $dateCreated  = $j.DateCreated
            $dateModified = $j.DateLastModified
            if (-not $dateModified -and $j.DateModified) {
                $dateModified = $j.DateModified
            }

            $dateToken = ([datetime]$dateModified).ToString('yyyyMMdd-HHmmss-fff')

            $exportNeeded = $true
            $prevFilePath = $null

            if ($UseManifest -and $manifest.Jobs.ContainsKey($jobId)) {

                $prev = $manifest.Jobs[$jobId]

                if ($prev.LastBackupFile) {
                    $prevFilePath = Join-Path -Path $manifest.BackupFolder -ChildPath $prev.LastBackupFile
                }

                if ($prevFilePath -and (Test-Path -LiteralPath $prevFilePath)) {

                    if ($prev.DateModified -and $dateModified) {
                        $exportNeeded = ([datetime]$dateModified -gt [datetime]$prev.DateModified)
                    }

                    if (-not $exportNeeded -and $prev.JobName -ne $jobName) {
                        $exportNeeded = $true
                    }
                }
            }

            if (-not $exportNeeded) {
                Write-MSLog -Level Debug -Message "Skipping job [$jobName] (no changes)"
                $allResults.Add([pscustomobject]@{
                    SqlInstance  = $instance
                    JobName      = $jobName
                    Status       = 'Skipped'
                    FilePath     = $prevFilePath
                    DateCreated  = $dateCreated
                    DateModified = $dateModified
                })
                continue
            }

            # -------------------------------------------------------------
            # Export job
            # -------------------------------------------------------------
            $jobToken = ConvertTo-SafeFileToken -Text $jobName

            if ($NoInstancePrefix) {
                if ($NoTimestamp) {
                    $fileName = 'job_{0}.sql' -f $jobToken
                }
                else {
                    $fileName = 'job_{0}.{1}.sql' -f $jobToken, $dateToken
                }
            }
            else {
                if ($NoTimestamp) {
                    $fileName = '{0}_job_{1}.sql' -f $instanceToken, $jobToken
                }
                else {
                    $fileName = '{0}_job_{1}.{2}.sql' -f $instanceToken, $jobToken, $dateToken
                }
            }

            $filePath = Join-Path -Path $instanceFolder -ChildPath $fileName

            Write-MSLog -Level Info -Message "Exporting job [$jobName] -> [$filePath]"
             
            if (Test-Path -LiteralPath $filePath) {
                Remove-Item -Path $filePath -Force -ErrorAction SilentlyContinue
            }

            $exportParams = @{
                InputObject = $j
                FilePath    = $filePath
                Encoding    = $Encoding
                Confirm     = $false
                ErrorAction = 'Stop'
            }
             
            if ($NoPrefix) {
                Add-IfSupported -CommandName 'Export-DbaScript' -Splat $exportParams -ParamName 'NoPrefix' -Value $true
            }
            Add-IfSupported -CommandName 'Export-DbaScript' -Splat $exportParams -ParamName 'EnableException' -Value $true
             
            Export-DbaScript @exportParams | Out-Null

            $exportedCount++
             
            if ($UseManifest) {
                $manifest.Jobs[$jobId] = [pscustomobject]@{
                    JobId          = $jobId
                    JobName        = $jobName
                    DateCreated    = $dateCreated
                    DateModified   = $dateModified
                    LastBackupAt   = $runAt
                    LastBackupFile = $fileName
                }
            }
             
            $allResults.Add([pscustomobject]@{
                SqlInstance  = $instance
                JobName      = $jobName
                Status       = 'Exported'
                FilePath     = $filePath
                DateCreated  = $dateCreated
                DateModified = $dateModified
            })
        }
         
        # -----------------------------------------------------------------
        # Save manifest
        # -----------------------------------------------------------------
        if ($UseManifest) {
            Write-MSLog -Level Debug -Message "Saving manifest for [$instance]"
            $tmp = "$manifestPath.tmp"
            Export-Clixml -Path $tmp -InputObject $manifest -Force
            Move-Item -Path $tmp -Destination $manifestPath -Force
        }
    }
    
    Write-MSLog -Level Info -Message 'Export-MSJobScripts: END'
    return $allResults
}