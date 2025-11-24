function Compress-Folder {
<#
.SYNOPSIS
    Compresses a folder using .NET built-in compression (ZIP).

.DESCRIPTION
    Creates a ZIP file from a folder using System.IO.Compression.ZipFile.
    Supports recursion, filters by file name or extension, deletion of
    compressed files, optional removal of the source folder, and automatic
    numbering when the output file already exists. Missing paths trigger
    warnings instead of errors.

.AUTHOR
    Sozezzo Astra
.VERSION
    1.4
#>

    [CmdletBinding(SupportsShouldProcess = $true)]
    param(
        [string] $Path,
        [string] $OutputFile,
        [string] $FileName,
        [string] $Extension,
        [switch] $Recurse,
        [switch] $DeleteOriginals,
        [switch] $RemoveSourceFolder,
        [ValidateSet('Zip')]
        [string] $ArchiveType = 'Zip',
        [ValidateSet('Optimal','Fastest','NoCompression','SmallestSize')]
        [string] $CompressionLevel = 'Optimal'
    )

    begin {
        Add-Type -AssemblyName System.IO.Compression.FileSystem

        function Get-CompressionLevelEnum {
            param([string] $Name)
            try {
                return [System.Enum]::Parse([System.IO.Compression.CompressionLevel], $Name, $true)
            }
            catch {
                if ($Name -eq 'SmallestSize') {
                    Write-Warning 'Compression level SmallestSize not supported on this runtime. Falling back to Optimal.'
                }
                return [System.IO.Compression.CompressionLevel]::Optimal
            }
        }

        function Get-UniqueFileName {
            param([string] $BasePath)
            $dir  = Split-Path $BasePath -Parent
            $name = [System.IO.Path]::GetFileNameWithoutExtension($BasePath)
            $ext  = [System.IO.Path]::GetExtension($BasePath)
            $counter = 0
            $newFile = $BasePath
            while (Test-Path $newFile) {
                $counter++
                $newFile = Join-Path $dir ("{0}({1}){2}" -f $name, $counter, $ext)
            }
            return $newFile
        }
    }

    process {
        try {
            if (-not $Path -and -not $OutputFile) {
                Write-Warning 'No path or output file specified. Nothing to compress.'
                return
            }

            # If Path not provided, infer from OutputFile folder
            if (-not $Path -and (Split-Path $OutputFile -Parent)) {
                $Path = Split-Path $OutputFile -Parent
            }

            if (-not (Test-Path $Path -PathType Container)) {
                Write-Warning "Folder not found: $Path"
                return
            }

            $sourcePath = (Resolve-Path $Path).Path

            # Determine OutputFile
            if (-not $OutputFile) {
                $parentDir  = Split-Path $sourcePath -Parent
                $folderName = Split-Path $sourcePath -Leaf
                $OutputFile = Join-Path $parentDir "$folderName.zip"
            }
            elseif (-not (Split-Path $OutputFile -Parent)) {
                $OutputFile = Join-Path $sourcePath $OutputFile
            }

            if ($ArchiveType -ne 'Zip') {
                Write-Warning "Archive type '$ArchiveType' is not supported by built-in .NET compression."
                return
            }

            $OutputFile = Get-UniqueFileName -BasePath $OutputFile
            $levelEnum  = Get-CompressionLevelEnum -Name $CompressionLevel

            # Gather files
            $gciParams = @{
                Path        = $sourcePath
                File        = $true
                ErrorAction = 'SilentlyContinue'
            }
            if ($Recurse) { $gciParams['Recurse'] = $true }

            $files = Get-ChildItem @gciParams
            if (-not $files) {
                Write-Warning "No files found in: $sourcePath"
                return
            }

            if ($FileName) {
                $files = $files | Where-Object { $_.BaseName -eq $FileName }
                if (-not $files) {
                    Write-Warning "File name not found: $FileName"
                    return
                }
            }
            elseif ($Extension) {
                $ext = '.' + $Extension.TrimStart('.')
                $files = $files | Where-Object { $_.Extension -eq $ext }
                if (-not $files) {
                    Write-Warning "No files found with extension: $Extension"
                    return
                }
            }

            # Temporary folder
            $tempFolder = Join-Path ([System.IO.Path]::GetTempPath()) ("CompressTemp_" + [guid]::NewGuid().ToString())
            New-Item -ItemType Directory -Path $tempFolder | Out-Null

            foreach ($file in $files) {
                $relative = $file.FullName.Substring($sourcePath.Length).TrimStart('\','/')
                $dest     = Join-Path $tempFolder $relative
                $destDir  = Split-Path $dest -Parent
                New-Item -ItemType Directory -Force -Path $destDir | Out-Null
                Copy-Item -LiteralPath $file.FullName -Destination $dest -Force
            }

            if ($PSCmdlet.ShouldProcess($OutputFile, "Create ZIP from $sourcePath")) {
                [System.IO.Compression.ZipFile]::CreateFromDirectory($tempFolder, $OutputFile, $levelEnum, $false)
            }

            # Delete originals if requested
            if ($DeleteOriginals) {
                foreach ($file in $files) {
                    if (Test-Path $file.FullName) {
                        if ($PSCmdlet.ShouldProcess($file.FullName, 'Delete original file')) {
                            Remove-Item -LiteralPath $file.FullName -Force -ErrorAction SilentlyContinue
                        }
                    }
                }
            }

            # Remove source folder if requested
            if ($RemoveSourceFolder) {
                $safe = ($sourcePath -match '^[A-Za-z]:\\.+')
                if (-not $safe) {
                    Write-Warning "Refusing to remove unsafe path: $sourcePath"
                }
                else {
                    $remaining = Get-ChildItem -LiteralPath $sourcePath -Recurse -Force -ErrorAction SilentlyContinue
                    if (-not $remaining -or $remaining.Count -eq 0) {
                        if ($PSCmdlet.ShouldProcess($sourcePath, 'Remove source folder')) {
                            Remove-Item -LiteralPath $sourcePath -Recurse -Force -ErrorAction SilentlyContinue
                        }
                    }
                    else {
                        Write-Warning 'Source folder not empty. Skipping removal.'
                    }
                }
            }

            Write-Host 'Folder compressed successfully.'
            Write-Host "Source : $sourcePath"
            Write-Host "Output : $OutputFile"
            Write-Host "Files  : $($files.Count)"
            Write-Host "Level  : $($levelEnum.ToString())"
        }
        catch {
            Write-Warning ("Unexpected error: " + $_.Exception.Message)
        }
        finally {
            if ($tempFolder -and (Test-Path $tempFolder)) {
                Remove-Item -Recurse -Force $tempFolder
            }
        }
    }
}
 