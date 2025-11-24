function Get-LastFileModifiedDate {
    <#
    .SYNOPSIS
        Gets the most recent file modification date from a folder.

    .DESCRIPTION
        Scans the specified folder (and optionally its subfolders)
        and returns either the latest LastWriteTime (DateTime)
        or a structured file object with file name, folder path, and last modified date.
        Supports case-insensitive filtering by file extension.

    .PARAMETER Path
        Folder path to scan.

    .PARAMETER Recurse
        Include subfolders in the search.

    .PARAMETER ReturnObject
        If specified, returns a custom object instead of just the datetime.

    .PARAMETER Extension
        One or more file extensions to include (e.g. ".sql", ".txt").
        If omitted, all files are included.

    .NOTES
        Author : Sozezzo Astra  
        Version: 1.5
    #>

    [CmdletBinding()]
    param(
        [Parameter(Mandatory)]
        [string] $Path,

        [switch] $Recurse,

        [switch] $ReturnObject,

        [string[]] $Extension
    )

    if (-not (Test-Path -LiteralPath $Path)) {
        return $null
    }

    $files = Get-ChildItem -Path $Path -File -ErrorAction SilentlyContinue -Recurse:$Recurse

    if ($Extension) {
        $extensionsNormalized = $Extension | ForEach-Object {
            if ($_ -notmatch '^\.') { ".$_" } else { $_ }
        }
        $files = $files | Where-Object { $extensionsNormalized -contains $_.Extension.ToLower() }
    }

    if (-not $files) {
        return $null
    }

    $latestFile = $files | Sort-Object LastWriteTime -Descending | Select-Object -First 1

    if ($ReturnObject) {
        return [PSCustomObject]@{
            FileName      = $latestFile.Name
            FilePath      = $latestFile.DirectoryName
            LastWriteTime = $latestFile.LastWriteTime
        }
    }
    else {
        return $latestFile.LastWriteTime
    }
}
