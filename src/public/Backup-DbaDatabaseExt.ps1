function Backup-DbaDatabaseExt {
<#
.SYNOPSIS
    Performs filtered SQL Server backups ( Full, Diff, or Log ) using dbatools,
    with optional per-database cleanup and fixed filenames.

.DESCRIPTION
    Backup-DbaDatabaseExt allows you to back up only a subset of databases 
    based on wildcard patterns (e.g., "App*", "Sales*", "Reporting*").
    It applies dbatools best practices and adds:

        • Backup type: Full, Diff, Log
        • Compression ON by default (unless -NoCompress is used)
        • Checksum ON by default (unless -NoChecksum is used)
        • Skips SIMPLE recovery databases when Type = Log
        • Uses BuildPath or FilePath depending on -NoTimestamp
        • Per-database cleanup after a successful backup
        • Clear summary result object with Status per database
        • Optional on-screen summary (NoShowSummary to hide)

    Path behavior:
        • If a single SqlInstance is specified, backups go directly under -Path.
        • If multiple SqlInstance values are specified, each instance uses a subfolder:
              <Path>\<InstanceNameSanitized>\
          where invalid path characters (\, /, :, *, ?, ", <, >, |) are replaced by "_".

    NoTimestamp behavior:
        • Default: dbatools generates timestamped filenames.
        • With -NoTimestamp:
              <DatabaseName>_<Type>.bak  (Full/Diff)
              <DatabaseName>_<Type>.trn  (Log)
          Each backup overwrites the previous one for that DB+Type.

    Cleanup behavior (per database):
        • If -CleanupOlderThanHours > 0:
            After a successful backup, the function deletes old *.bak / *.trn
            for that database only (BaseName -like "<DBName>*") under the
            instance folder (effectivePath).
        • Cleanup is skipped when -OutputScriptOnly is used.

    Compression behavior:
        • Compression is ON by default (unless -NoCompress is used).
        • We do NOT pre-test compression support.
        • We call Backup-DbaDatabase with -EnableException so failures throw.
        • If the backup fails with a compression-related error
          (e.g., "Compression is not supported", "The backup cannot be performed because 'COMPRESSION' was requested...",
           or matches "530.*backup"), we log a warning and retry once without -CompressBackup.
        • If the retry succeeds, the backup is marked as Success with
          UsedCompression = $false and FallbackNoCompression = $true.

.PARAMETER SqlInstance
    One or more SQL Server instance names.
    Example: "SQL01","SQL02\Instance","AGListener01"

.PARAMETER Path
    Backup root path (UNC or local).
    Single instance: backups under this path.
    Multiple instances: per-instance subfolders under this path.

.PARAMETER Database
    Database name filter(s). Accepts wildcards (e.g., "App*", "Sales*", "*").
    Default = "*".

.PARAMETER Type
    Backup type: Full, Diff, or Log.
    Default = Full.

.PARAMETER CompressBackup
    Enables backup compression (default: ON unless -NoCompress is used).

.PARAMETER NoCompress
    Disables compression entirely.

.PARAMETER Checksum
    Enables checksum (default: ON unless -NoChecksum is used).

.PARAMETER NoChecksum
    Disables checksum entirely.

.PARAMETER CopyOnly
    Performs a copy-only backup without affecting the backup chain.

.PARAMETER Verify
    Verifies the generated backup files after creation.

.PARAMETER OutputScriptOnly
    Returns the T-SQL commands instead of executing the backup.
    Cleanup is not performed in this mode.

.PARAMETER Description
    Adds a description to the backup metadata.

.PARAMETER CleanupOlderThanHours
    If > 0, then after each successful backup of a database, the function
    deletes old backup files (*.bak / *.trn) for that database whose
    LastWriteTime is older than the given number of hours.

.PARAMETER NoTimestamp
    Uses fixed filenames instead of timestamped ones:
        <DatabaseName>_<Type>.bak  (Full/Diff)
        <DatabaseName>_<Type>.trn  (Log)
    Each new backup overwrites the previous one for that DB+Type.

.PARAMETER NoShowSummary
    If specified, the function does not print the on-screen summary table
    (but still returns the result objects to the pipeline).

.OUTPUTS
    PSCustomObject with:
        Instance, Database, Type, Status (Success/Failed/Offline),
        UsedCompression, FallbackNoCompression, Path,
        NoTimestamp, CleanupHours, Error, Timestamp

.NOTES
    Author: Sozezzo Astra
    Depends on: dbatools (Get-DbaDatabase, Backup-DbaDatabase)
#>

    [CmdletBinding(SupportsShouldProcess = $true)]
    param(
        [Parameter(Mandatory = $true)]
        [string[]]
        $SqlInstance,

        [Parameter(Mandatory = $true)]
        [string]
        $Path,

        [Parameter(Mandatory = $false)]
        [string[]]
        $Database = '*',

        [Parameter(Mandatory = $false)]
        [ValidateSet('Full','Diff','Log')]
        [string]
        $Type = 'Full',

        [Parameter(Mandatory = $false)]
        [switch]
        $CompressBackup,

        [Parameter(Mandatory = $false)]
        [switch]
        $NoCompress,

        [Parameter(Mandatory = $false)]
        [switch]
        $Checksum,

        [Parameter(Mandatory = $false)]
        [switch]
        $NoChecksum,

        [Parameter(Mandatory = $false)]
        [switch]
        $CopyOnly,

        [Parameter(Mandatory = $false)]
        [switch]
        $Verify,

        [Parameter(Mandatory = $false)]
        [switch]
        $OutputScriptOnly,

        [Parameter(Mandatory = $false)]
        [string]
        $Description,

        [Parameter(Mandatory = $false)]
        [int]
        $CleanupOlderThanHours,

        [Parameter(Mandatory = $false)]
        [switch]
        $NoTimestamp,

        [Parameter(Mandatory = $false)]
        [switch]
        $NoShowSummary
    )

    begin {
        # Best practice defaults: compression & checksum ON unless explicitly disabled
        if (-not $PSBoundParameters.ContainsKey('CompressBackup') -and -not $NoCompress) {
            $CompressBackup = $true
            Write-Log -Level Debug -Message "Backup-DbaDatabaseExt: CompressBackup default → TRUE."
        }

        if (-not $PSBoundParameters.ContainsKey('Checksum') -and -not $NoChecksum) {
            $Checksum = $true
            Write-Log -Level Debug -Message "Backup-DbaDatabaseExt: Checksum default → TRUE."
        }

        # Collect per-database results
        $BackupResults = @()

        Write-Log -Level Info -Message "Backup-DbaDatabaseExt: START. Instances=[$($SqlInstance -join ', ')], RootPath=[$Path], Type=[$Type], NoTimestamp=[$NoTimestamp], CleanupOlderThanHours=[$CleanupOlderThanHours]"
    }

    process {
        $multiInstance = $SqlInstance.Count -gt 1

        foreach ($instanceName in $SqlInstance) {

            $instance = [DbaInstanceParameter]$instanceName

            # Determine effective path for this instance
            if ($multiInstance) {
                $sanitizedInstanceName = $instanceName -replace '[\\/:*?"<>|]', '_'
                $effectivePath = Join-Path -Path $Path -ChildPath $sanitizedInstanceName
            }
            else {
                $effectivePath = $Path
            }

            Write-Log -Level Info -Message "Processing instance [$instanceName] → EffectivePath=[$effectivePath]"

            # Ensure the effective path exists
            try {
                if (-not (Test-Path -LiteralPath $effectivePath)) {
                    Write-Log -Level Debug -Message "Creating directory [$effectivePath]."
                    New-Item -ItemType Directory -Path $effectivePath -Force | Out-Null
                }
            }
            catch {
                Write-Log -Level Error -Message "[$instanceName] → Failed to create/access directory [$effectivePath]. Error: $($_.Exception.Message)"
                continue
            }

            # Step 1 — Get all non-system DBs
            try {
                $allDbs = Get-DbaDatabase -SqlInstance $instance -ExcludeSystem
                Write-Log -Level Debug -Message "[$instanceName] → [$($allDbs.Count)] user DB(s) found."
            }
            catch {
                Write-Log -Level Error -Message "[$instanceName] → Get-DbaDatabase FAILED. Error: $($_.Exception.Message)"
                continue
            }

            if (-not $allDbs) {
                Write-Log -Level Warning -Message "[$instanceName] → No user databases found."
                continue
            }

            # Step 2 — Skip SIMPLE when Type = Log
            if ($Type -eq 'Log') {
                $before = $allDbs.Count
                $allDbs = $allDbs | Where-Object { $_.RecoveryModel -ne 'Simple' }
                $after = $allDbs.Count

                if ($after -lt $before) {
                    Write-Log -Level Info -Message "[$instanceName] → Excluded [$($before - $after)] SIMPLE recovery DB(s) from log backup."
                }

                if (-not $allDbs) {
                    Write-Log -Level Warning -Message "[$instanceName] → No eligible databases (non-SIMPLE) remain for log backup."
                    continue
                }
            }

            # Step 3 — Apply name filtering
            if ($Database.Count -eq 1 -and $Database[0] -eq '*') {
                $dbs = $allDbs
                Write-Log -Level Info -Message "[$instanceName] → Using ALL eligible user DBs (no name filter)."
            }
            else {
                $dbs = $allDbs | Where-Object {
                    $dbName = $_.Name
                    foreach ($pattern in $Database) {
                        if ($dbName -like $pattern) { return $true }
                    }
                    return $false
                }

                if (-not $dbs) {
                    Write-Log -Level Warning -Message "[$instanceName] → No databases match filter(s): $($Database -join ', ')"
                    continue
                }

                Write-Log -Level Info -Message "[$instanceName] → Matching DBs: $($dbs.Name -join ', ')"
            }

            # Step 4 — Run backup per database (and cleanup immediately after)
            foreach ($db in $dbs) {

                $dbName = $db.Name

                # Check if database is offline (based on data we already have from Get-DbaDatabase)
                $isOffline = $false

                if ($db.PSObject.Properties.Name -contains 'Status' -and $db.Status -eq 'Offline') {
                    $isOffline = $true
                }
                elseif ($db.PSObject.Properties.Name -contains 'IsAccessible' -and -not $db.IsAccessible) {
                    # Some versions expose IsAccessible instead
                    $isOffline = $true
                }

                if ($isOffline) {
                    Write-Log -Level Warning -Message "[$instanceName] → Database [$dbName] is OFFLINE. Backup will be skipped and reported as Offline."

                    $result = [pscustomobject]@{
                        Instance              = $instanceName
                        Database              = $dbName
                        Type                  = $Type
                        Status                = 'Offline'
                        UsedCompression       = $false
                        FallbackNoCompression = $false
                        Path                  = $effectivePath
                        NoTimestamp           = [bool]$NoTimestamp
                        CleanupHours          = $CleanupOlderThanHours
                        Error                 = 'Database is offline – backup skipped.'
                        Timestamp             = Get-Date
                    }

                    $BackupResults += $result
                    continue
                }

                $targetText = "$Type backup for [$dbName] to [$effectivePath]"

                if (-not $PSCmdlet.ShouldProcess($instanceName, $targetText)) {
                    Write-Log -Level Info -Message "[$instanceName] → ShouldProcess declined for DB [$dbName] (WhatIf?)."
                    continue
                }

                # Build parameters for Backup-DbaDatabase
                $params = @{
                    SqlInstance      = $instance
                    Database         = $dbName
                    Type             = $Type
                    EnableException  = $true
                }

                if ($NoTimestamp) {
                    # Fixed filename mode: <DBName>_<Type>.bak / .trn
                    $fileExt = if ($Type -eq 'Log') { 'trn' } else { 'bak' }

                    $fixedName = ('{0}_{1}.{2}' -f $dbName, $Type, $fileExt)
                    $filePath  = Join-Path -Path $effectivePath -ChildPath $fixedName

                    $params.FilePath   = $filePath
                    $params.Initialize = $true   # overwrite file on each run

                    Write-Log -Level Info -Message "[$instanceName] → NoTimestamp=TRUE, DB=[$dbName], FilePath=[$filePath]"
                }
                else {
                    # Default dbatools behavior: timestamped filenames with BuildPath
                    $params.Path      = $effectivePath
                    $params.BuildPath = $true
                }

                $initialHadCompression = $false
                if ($CompressBackup -and -not $NoCompress) {
                    $params.CompressBackup = $true
                    $initialHadCompression = $true
                }
                if ($Checksum       -and -not $NoChecksum)  { $params.Checksum       = $true }
                if ($CopyOnly)                              { $params.CopyOnly       = $true }
                if ($Verify)                                { $params.Verify         = $true }
                if ($OutputScriptOnly)                      { $params.OutputScriptOnly = $true }
                if ($Description)                           { $params.Description    = $Description }
                if ($NoTimestamp)                           { $params.WithFormat = $true }


                $paramText = ($params.GetEnumerator() | ForEach-Object { "$($_.Key)=$($_.Value)" }) -join '; '
                Write-Log -Level Debug -Message "[$instanceName] → Backup-DbaDatabase params for DB [$dbName]: $paramText"

                $backupSucceeded       = $false
                $errorMessage          = $null
                $usedCompression       = $false
                $fallbackNoCompression = $false

                # First attempt (possibly with compression)
                try {
                    Write-Log -Level Debug -Message "[$instanceName] → @@ Before run Backup-DbaDatabase for DB [$dbName]."
                    $null = Backup-DbaDatabase @params
                    $backupSucceeded = $true
                    $usedCompression = $initialHadCompression
                    Write-Log -Level Info -Message "[$instanceName] → Backup completed for DB [$dbName]."
                }
                catch {
                    $errorMessage = $_.Exception.Message
                    Write-Log -Level Error -Message "[$instanceName] → Backup FAILED for DB [$dbName]. Error: $errorMessage"

                    # Check if the error looks like a compression/media problem:
                    #  - "Compression is not supported"
                    #  - 530.*backup
                    #  - "The backup cannot be performed because 'COMPRESSION' was requested..."
                    $isCompressionError = $false

                    Write-Log -Level Debug -Message "[$instanceName] → Compression error check for DB [$dbName]."
                    Write-Log -Level Debug -Message "--------"
                    Write-Log -Level Debug -Message $errorMessage
                    Write-Log -Level Debug -Message "--------"

                    if ($initialHadCompression) {
                        if ($errorMessage -match 'Compression is not supported' -or
                            $errorMessage -match '530.*backup' -or
                            $errorMessage -match "The backup cannot be performed because 'COMPRESSION' was requested" -or
                            ($errorMessage -match 'COMPRESSION' -and $errorMessage -match 'backup cannot be performed')) {
                            $isCompressionError = $true
                        }
                    }

                    if ($isCompressionError -and -not $OutputScriptOnly) {
                        Write-Log -Level Warning -Message "[$instanceName] → Compression/media mismatch for DB [$dbName]. Retrying backup WITHOUT compression."

                        # Remove compression and retry
                        if ($params.ContainsKey('CompressBackup')) {
                            $params.Remove('CompressBackup')
                        }
                        $initialHadCompression = $false  # second attempt has no compression

                        try {
                            $null = Backup-DbaDatabase @params
                            $backupSucceeded       = $true
                            $usedCompression       = $false
                            $fallbackNoCompression = $true
                            $errorMessage          = $null
                            Write-Log -Level Info -Message "[$instanceName] → Backup completed for DB [$dbName] on retry WITHOUT compression."
                        }
                        catch {
                            $errorMessage = $_.Exception.Message
                            Write-Log -Level Error -Message "[$instanceName] → Retry WITHOUT compression FAILED for DB [$dbName]. Error: $errorMessage"
                        }
                    }
                }

                # Register per-database result
                $result = [pscustomobject]@{
                    Instance              = $instanceName
                    Database              = $dbName
                    Type                  = $Type
                    Status                = if ($backupSucceeded) { 'Success' } else { 'Failed' }
                    UsedCompression       = $usedCompression
                    FallbackNoCompression = $fallbackNoCompression
                    Path                  = $effectivePath
                    NoTimestamp           = [bool]$NoTimestamp
                    CleanupHours          = $CleanupOlderThanHours
                    Error                 = $errorMessage
                    Timestamp             = Get-Date
                }

                $BackupResults += $result

                # Per-database cleanup immediately after successful backup
                if ($backupSucceeded -and $CleanupOlderThanHours -gt 0 -and -not $OutputScriptOnly) {
                    try {
                        $threshold = (Get-Date).AddHours(-$CleanupOlderThanHours)

                        Write-Log -Level Info -Message "[$instanceName] → Cleanup for DB [$dbName]: deleting *.bak/*.trn older than [$CleanupOlderThanHours] hour(s) under [$effectivePath]."

                        # Find old backup files for THIS database only (BaseName starts with DB name)
                        $allOldFiles = Get-ChildItem -Path $effectivePath -Recurse -Include *.bak, *.trn -ErrorAction SilentlyContinue |
                                       Where-Object { $_.LastWriteTime -lt $threshold }

                        $files = $allOldFiles | Where-Object {
                            $_.BaseName -like ("{0}*" -f $dbName)
                        }

                        if ($files -and $files.Count -gt 0) {
                            Write-Log -Level Info -Message "[$instanceName] → Cleanup for DB [$dbName]: found [$($files.Count)] file(s) to delete."

                            foreach ($file in $files) {
                                Write-Log -Level Debug -Message "[$instanceName] → Cleanup for DB [$dbName]: deleting [$($file.FullName)] (LastWriteTime=$($file.LastWriteTime))"
                                Remove-Item -LiteralPath $file.FullName -Force
                            }

                            Write-Log -Level Info -Message "[$instanceName] → Cleanup for DB [$dbName] completed under [$effectivePath]."
                        }
                        else {
                            Write-Log -Level Info -Message "[$instanceName] → Cleanup for DB [$dbName]: no files older than [$CleanupOlderThanHours] hour(s) under [$effectivePath]."
                        }
                    }
                    catch {
                        Write-Log -Level Error -Message "[$instanceName] → Cleanup for DB [$dbName] FAILED under [$effectivePath]. Error: $($_.Exception.Message)"
                    }
                }
                elseif ($CleanupOlderThanHours -gt 0 -and $OutputScriptOnly) {
                    Write-Log -Level Debug -Message "[$instanceName] → Cleanup for DB [$dbName] skipped (OutputScriptOnly is set)."
                }
            }
        }
    }

    end {
        $results = $BackupResults

        $total   = $results.Count
        $success = ($results | Where-Object Status -eq 'Success').Count
        $failed  = ($results | Where-Object Status -eq 'Failed').Count

        Write-Log -Level Info -Message "Backup-DbaDatabaseExt: Summary → Total=[$total], Success=[$success], Failed=[$failed]."

        # Detect if we are the last command in the pipeline
        $invocation      = $PSCmdlet.MyInvocation
        $atEndOfPipeline = ($invocation.PipelinePosition -eq $invocation.PipelineLength)

        if (-not $NoShowSummary.IsPresent) {
            if ($atEndOfPipeline -and $results.Count -gt 0) {
                $results |
                    Select-Object Instance, Database, Type, Status, UsedCompression, FallbackNoCompression, Path |
                    Format-Table -AutoSize |
                    Out-Host
            }
        }

        # Emit results to the pipeline so callers can process them
        $results
    }
}