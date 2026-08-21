function Compare-MSFolder {
<#
.SYNOPSIS
Compares the content of two folders.

.DESCRIPTION
Compare-MSFolder compares files between two folders, commonly network folders
or cluster node folders that are expected to contain the same files.

It reports:
- Matching files
- Files missing in Path1
- Files missing in Path2
- Size differences
- Date/time differences
- Folder-level summary

.PARAMETER Path1
First folder path.

.PARAMETER Path2
Second folder path.

.PARAMETER Recurse
Search subfolders recursively.

.PARAMETER IgnoreDate
Ignore file date/time differences.

.PARAMETER IgnoreSize
Ignore file size differences.

.PARAMETER SizeTolerancePercent
Allows size difference tolerance in percent.

Example:
2 means a size difference of 2 percent or less is considered acceptable.

.PARAMETER IgnoreExtensions
List of file extensions to ignore.

Examples:
".log", ".xel", "tmp"

.PARAMETER Show
Controls the detail output.

Valid values:
- Default
- All
- Match
- Diff

.EXAMPLE
$r = Compare-MSFolder `
    -Path1 "\\isi-m0bdrs31\D$\MSSQL" `
    -Path2 "\\isi-m0bdrs32\D$\MSSQL" `
    -Recurse `
    -IgnoreDate `
    -SizeTolerancePercent 2 `
    -IgnoreExtensions ".xel", ".log" `
    -Show Diff

$r.Details | Format-Table -AutoSize
$r.Summary | Format-Table -AutoSize

Compares two SQL Server folders recursively, ignores date differences,
allows 2 percent size tolerance, ignores .xel and .log files, and shows only differences.

.EXAMPLE
$totalPath1 = ($r.Details | Measure-Object -Property Path1Size -Sum).Sum
$totalPath2 = ($r.Details | Measure-Object -Property Path2Size -Sum).Sum

[PSCustomObject]@{
    Path1TotalGB = [math]::Round($totalPath1 / 1GB, 2)
    Path2TotalGB = [math]::Round($totalPath2 / 1GB, 2)
}

Calculates total file size for both paths from a previous comparison result.
#>

    [CmdletBinding()]
    param (
        [Parameter(Mandatory)]
        [string]$Path1,

        [Parameter(Mandatory)]
        [string]$Path2,

        [switch]$Recurse,

        [switch]$IgnoreDate,

        [switch]$IgnoreSize,

        [double]$SizeTolerancePercent = 0,

        [string[]]$IgnoreExtensions = @(),

        [ValidateSet("Default", "All", "Match", "Diff")]
        [string]$Show = "Default"
    )

    $Path1 = $Path1.TrimEnd('\')
    $Path2 = $Path2.TrimEnd('\')

    $IgnoreExtensions = $IgnoreExtensions | ForEach-Object {
        if ($_ -notmatch "^\.") { ".$($_.ToLowerInvariant())" } else { $_.ToLowerInvariant() }
    }

    function Get-FileMap {
        param (
            [string]$BasePath
        )

        $items = Get-ChildItem -Path $BasePath -File -Recurse:$Recurse -ErrorAction SilentlyContinue

        $map = New-Object 'System.Collections.Generic.Dictionary[string,object]' ([System.StringComparer]::OrdinalIgnoreCase)

        foreach ($item in $items) {

            if ($IgnoreExtensions -contains $item.Extension.ToLowerInvariant()) {
                continue
            }

            if ($item.FullName.Length -gt $BasePath.Length) {
                $relativePath = $item.FullName.Substring($BasePath.Length).TrimStart('\', '/')
            }
            else {
                $relativePath = $item.Name
            }

            $relativePath = ($relativePath -replace '\\', '/').Trim()

            $map[$relativePath] = [PSCustomObject]@{
                FullName      = $item.FullName
                Length        = $item.Length
                LastWriteTime = $item.LastWriteTimeUtc
            }
        }

        return $map
    }

    Write-Verbose "Scanning $Path1"
    $map1 = Get-FileMap -BasePath $Path1

    Write-Verbose "Scanning $Path2"
    $map2 = Get-FileMap -BasePath $Path2

    $allKeys = ($map1.Keys + $map2.Keys) | Sort-Object -Unique

    $results = New-Object System.Collections.Generic.List[object]

    foreach ($key in $allKeys) {

        $file1 = if ($map1.ContainsKey($key)) { $map1[$key] } else { $null }
        $file2 = if ($map2.ContainsKey($key)) { $map2[$key] } else { $null }

        $status = "Match"
        $sizeDiff = $false
        $dateDiff = $false

        if (-not $file1) {
            $status = "MissingInPath1"
        }
        elseif (-not $file2) {
            $status = "MissingInPath2"
        }
        else {
            if (-not $IgnoreSize) {
                if ($file1.Length -ne $file2.Length) {

                    if ($SizeTolerancePercent -gt 0) {
                        $max = [math]::Max($file1.Length, $file2.Length)
                        $diff = [math]::Abs($file1.Length - $file2.Length)

                        if ($max -ne 0) {
                            $percent = ($diff / $max) * 100

                            if ($percent -gt $SizeTolerancePercent) {
                                $sizeDiff = $true
                            }
                        }
                    }
                    else {
                        $sizeDiff = $true
                    }
                }
            }

            if (-not $IgnoreDate) {
                if ($file1.LastWriteTime -ne $file2.LastWriteTime) {
                    $dateDiff = $true
                }
            }

            if ($sizeDiff -or $dateDiff) {
                $status = "Different"
            }
        }

        $results.Add([PSCustomObject]@{
            RelativePath = $key
            Status       = $status
            SizeDiff     = $sizeDiff
            DateDiff     = $dateDiff
            Path1Size    = if ($file1) { $file1.Length } else { $null }
            Path2Size    = if ($file2) { $file2.Length } else { $null }
            Path1DateUtc = if ($file1) { $file1.LastWriteTime } else { $null }
            Path2DateUtc = if ($file2) { $file2.LastWriteTime } else { $null }
        })
    }

    $folderSummary = $results |
        Group-Object {
            if ([string]::IsNullOrWhiteSpace($_.RelativePath)) {
                "."
            }
            else {
                $folder = Split-Path -Path $_.RelativePath -Parent
                if ([string]::IsNullOrWhiteSpace($folder)) { "." } else { $folder }
            }
        } |
        ForEach-Object {
            [PSCustomObject]@{
                Folder     = $_.Name
                HasDiff    = ($_.Group.Status -ne "Match") -contains $true
                TotalFiles = $_.Count
                DiffCount  = ($_.Group | Where-Object { $_.Status -ne "Match" }).Count
            }
        }

    switch ($Show) {
        "All"     { $filtered = $results }
        "Diff"    { $filtered = $results | Where-Object Status -ne "Match" }
        "Match"   { $filtered = $results | Where-Object Status -eq "Match" }
        "Default" { $filtered = $results }
    }

    return [PSCustomObject]@{
        Summary = $folderSummary
        Details = $filtered
    }
}
