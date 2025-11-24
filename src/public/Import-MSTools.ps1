function Import-MSTools {
<#
.SYNOPSIS
    Installs or updates the MSTools module from a local/network path or (by default) from GitHub.

.DESCRIPTION
    - No parameters: downloads MSTools from GitHub (branch 'main') and installs it.
    - -SourcePath: copies from local/UNC folder.
    - Installs to Program Files by default; use -UserScope for the user's Modules.
    - Backs up any existing MSTools folder to a timestamped backup.
    - Uses 'git' if requested/available; otherwise downloads a ZIP.
    - PowerShell 5.1 compatible.

.PARAMETER SourcePath
    Local or UNC folder containing MSTools to copy from.

.PARAMETER UserScope
    Install under Documents\WindowsPowerShell\Modules instead of Program Files.

.PARAMETER Branch
    Git branch to fetch when downloading from GitHub (default: 'main').

.PARAMETER UseGit
    Prefer cloning with 'git' when pulling from GitHub. Falls back to ZIP if git not found.

.PARAMETER RepoUrl
    GitHub repository URL. Default: https://github.com/sozezzo/MSTools.git

.PARAMETER ModuleName
    Target module folder name. Default: MSTools

.PARAMETER Force
    Continue and warn on errors inside try/catch (still respects -WhatIf/-Confirm).

.OUTPUTS
    PSCustomObject summary.

.NOTES
    Author : Sozezzo Astra
    Version: 1.0.3 (PS 5.1 safe, conservative quoting)
#>
    [CmdletBinding(SupportsShouldProcess = $true, ConfirmImpact = 'Medium')]
    param(
        [Parameter(Position=0)]
        [string]$SourcePath,

        [switch]$UserScope,

        [string]$Branch = 'main',

        [switch]$UseGit,

        [string]$RepoUrl = 'https://github.com/sozezzo/MSTools.git',

        [string]$ModuleName = 'MSTools',

        [switch]$Force
    )

    begin {
        function Test-Admin {
            try {
                $id = [Security.Principal.WindowsIdentity]::GetCurrent()
                $p  = New-Object Security.Principal.WindowsPrincipal $id
                return $p.IsInRole([Security.Principal.WindowsBuiltinRole]::Administrator)
            } catch {
                return $false
            }
        }

        function Get-UserModulesPath {
            $doc = [Environment]::GetFolderPath('MyDocuments')
            Join-Path $doc 'WindowsPowerShell\Modules'
        }

        function Get-SystemModulesPath {
            Join-Path $env:ProgramFiles 'WindowsPowerShell\Modules'
        }

        function Get-TempWorkDir {
            $base = Join-Path ([IO.Path]::GetTempPath()) ("MSTools_Install_{0:yyyyMMdd_HHmmss}" -f (Get-Date))
            New-Item -Type Directory -Path $base -Force | Out-Null
            return $base
        }

        function Ensure-ZipAssemblies {
            if (-not ([AppDomain]::CurrentDomain.GetAssemblies() | Where-Object { $_.GetName().Name -eq 'System.IO.Compression.FileSystem' })) {
                Add-Type -AssemblyName 'System.IO.Compression.FileSystem' -ErrorAction SilentlyContinue
            }
        }

        function Expand-ZipFallback {
            param(
                [Parameter(Mandatory)][string]$ZipPath,
                [Parameter(Mandatory)][string]$Destination
            )
            Ensure-ZipAssemblies
            [System.IO.Compression.ZipFile]::ExtractToDirectory($ZipPath, $Destination)
        }

        function Download-FromGitHubZip {
            param(
                [Parameter(Mandatory)][string]$RepoOwnerRepo,
                [Parameter(Mandatory)][string]$Branch,
                [Parameter(Mandatory)][string]$OutDir
            )
            $zipUrl  = "https://codeload.github.com/$RepoOwnerRepo/zip/refs/heads/$Branch"
            $zipPath = Join-Path $OutDir "repo.zip"
            Invoke-WebRequest -Uri $zipUrl -OutFile $zipPath -UseBasicParsing -ErrorAction Stop

            $expanded = Join-Path $OutDir "expanded"
            New-Item -ItemType Directory -Path $expanded -Force | Out-Null
            $expandArchive = Get-Command Expand-Archive -ErrorAction SilentlyContinue
            if ($expandArchive) {
                Expand-Archive -Path $zipPath -DestinationPath $expanded -Force
            } else {
                Expand-ZipFallback -ZipPath $zipPath -Destination $expanded
            }
            Remove-Item $zipPath -Force

            $repoName = $RepoOwnerRepo.Split('/')[1]
            $folder   = Join-Path $expanded ("{0}-{1}" -f $repoName, $Branch)
            if (-not (Test-Path $folder)) {
                throw "Unexpected ZIP layout. Could not find extracted folder for $RepoOwnerRepo / $Branch."
            }
            return $folder
        }

        function Git-Clone {
            param(
                [Parameter(Mandatory)][string]$RepoUrl,
                [Parameter(Mandatory)][string]$Branch,
                [Parameter(Mandatory)][string]$OutDir
            )
            $git = Get-Command git -ErrorAction SilentlyContinue
            if (-not $git) {
                throw "git not found on PATH."
            }
            & $git.Source clone --depth 1 --branch $Branch --single-branch $RepoUrl $OutDir | Out-Null
            return $OutDir
        }

        function Read-ModuleVersion {
            param([string]$ModuleFolder, [string]$ModuleName)
            $psd1 = Get-ChildItem -LiteralPath $ModuleFolder -Filter "$ModuleName.psd1" -Recurse -ErrorAction SilentlyContinue | Select-Object -First 1
            if ($psd1) {
                try {
                    $data = Import-PowerShellDataFile -Path $psd1.FullName
                    return $data.ModuleVersion
                } catch {
                    return $null
                }
            }
            return $null
        }

        function Copy-ModuleContent {
            param(
                [Parameter(Mandatory)][string]$SourceDir,
                [Parameter(Mandatory)][string]$DestDir
            )
            $robo = Get-Command robocopy.exe -ErrorAction SilentlyContinue
            $excludeDirs  = @('.git','.github','.vs','bin','obj')
            $excludeFiles = @('.gitattributes','.gitignore','.editorconfig')
            if ($robo) {
                $args = @($SourceDir, $DestDir, '*', '/E', '/NFL', '/NDL', '/NJH', '/NJS', '/NP', '/R:1', '/W:1')
                if ($excludeDirs.Count)  { $args += '/XD'; $args += $excludeDirs }
                if ($excludeFiles.Count) { $args += '/XF'; $args += $excludeFiles }
                & $robo @args | Out-Null
            } else {
                $items = Get-ChildItem -LiteralPath $SourceDir -Force
                foreach ($i in $items) {
                    if ($excludeDirs -contains $i.Name -or $excludeFiles -contains $i.Name) { continue }
                    Copy-Item -LiteralPath $i.FullName -Destination $DestDir -Recurse -Force
                }
            }
        }
    } # end begin

    process {
        $destRoot = if ($UserScope) { Get-UserModulesPath } else { Get-SystemModulesPath }

        if (-not $UserScope -and -not (Test-Admin)) {
            throw "Administrator privileges are required to install into '$destRoot'. Re-run PowerShell as Administrator or use -UserScope."
        }

        $destPath       = Join-Path $destRoot $ModuleName
        $backupPath     = $null
        $workDir        = Get-TempWorkDir
        $sourceResolved = $null
        $method         = $null

        try {
            if ($PSCmdlet.ShouldProcess($destPath, "Install/Update $ModuleName")) {

                if ([string]::IsNullOrWhiteSpace($SourcePath)) {
                    $repoOwnerRepo = ($RepoUrl -replace '^https://github\.com/','') -replace '\.git$',''
                    $gitAvailable  = [bool](Get-Command git -ErrorAction SilentlyContinue)
                    if ($UseGit -and $gitAvailable) {
                        $method   = 'Git'
                        $cloneDir = Join-Path $workDir 'gitclone'
                        New-Item -Type Directory -Path $cloneDir -Force | Out-Null
                        Write-Verbose ("Cloning " + $RepoUrl + " (branch '" + $Branch + "')...")
                        $sourceResolved = Git-Clone -RepoUrl $RepoUrl -Branch $Branch -OutDir $cloneDir
                    } else {
                        $method = 'Zip'
                        Write-Verbose ("Downloading ZIP from " + $repoOwnerRepo + " (branch '" + $Branch + "')...")
                        $sourceResolved = Download-FromGitHubZip -RepoOwnerRepo $repoOwnerRepo -Branch $Branch -OutDir $workDir
                    }
                } else {
                    $method = 'LocalPath'
                    $sourceResolved = (Resolve-Path -LiteralPath $SourcePath).ProviderPath
                    if (-not (Test-Path $sourceResolved)) {
                        throw ("SourcePath not found: " + $SourcePath)
                    }
                }

                # Identify module root to copy
                $candidate = @(
                    (Join-Path $sourceResolved $ModuleName),
                    $sourceResolved
                ) | Where-Object {
                    (Test-Path (Join-Path $_ ($ModuleName + '.psd1'))) -or
                    (Test-Path (Join-Path $_ ($ModuleName + '.psm1')))
                } | Select-Object -First 1

                if (-not $candidate) {
                    $candidate = $sourceResolved
                    Write-Verbose ("Could not locate '" + $ModuleName + ".psd1' or '" + $ModuleName + ".psm1' - copying entire source folder.")
                }

                # Backup current install if present
                if (Test-Path $destPath) {
                    $timestamp  = Get-Date -Format 'yyyyMMdd_HHmmss'
                    $backupPath = Join-Path $destRoot ($ModuleName + '._backup_' + $timestamp)
                    if ($PSCmdlet.ShouldProcess($destPath, ("Backup to " + $backupPath))) {
                        Move-Item -LiteralPath $destPath -Destination $backupPath -Force
                    }
                }

                if (-not (Test-Path $destRoot)) {
                    New-Item -ItemType Directory -Path $destRoot -Force | Out-Null
                }
                New-Item -ItemType Directory -Path $destPath -Force | Out-Null

                Write-Verbose ("Copying from '" + $candidate + "' to '" + $destPath + "'...")
                Copy-ModuleContent -SourceDir $candidate -DestDir $destPath

                $version = Read-ModuleVersion -ModuleFolder $destPath -ModuleName $ModuleName

                [pscustomobject]@{
                    ModuleName  = $ModuleName
                    Destination = $destPath
                    Source      = (if ($SourcePath) { $SourcePath } else { $RepoUrl + '@' + $Branch + ' (' + $method + ')' })
                    Method      = $method
                    Branch      = $Branch
                    Version     = $version
                    BackupPath  = $backupPath
                    Scope       = (if ($UserScope) { 'User' } else { 'System' })
                }
            }
        } catch {
            if (-not $Force) {
                throw
            } else {
                $msg = "Import-MSTools encountered an error but -Force was specified:" + [Environment]::NewLine + $_.Exception.Message
                Write-Warning $msg
            }
        } finally {
            try {
                if (Test-Path $workDir) {
                    Remove-Item -LiteralPath $workDir -Recurse -Force -ErrorAction SilentlyContinue
                }
            } catch {
                # ignore cleanup errors
            }
        }
    } # end process
} # end function

Set-Alias -Name import-mstools -Value Import-MSTools -Scope Global
