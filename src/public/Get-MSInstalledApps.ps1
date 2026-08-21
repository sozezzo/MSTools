function Get-MSInstalledApps {
<#
.SYNOPSIS
Retrieves installed applications while allowing fine‑grained filtering.

.DESCRIPTION
Get-MSInstalledApps queries standard Windows uninstall registry paths
(HKLM, WOW6432Node, HKCU) and returns installed applications with optional
filtering rules:

- Include specific Microsoft apps even though Microsoft‑published apps are excluded by default.
- Exclude apps by DisplayName.
- Exclude apps by Publisher.
- Output results as JSON if desired.

.PARAMETER Exceptions
Microsoft applications you explicitly WANT to include even though Microsoft
publishers are normally filtered out.

.PARAMETER AppExclusions
Application DisplayNames you want removed from the results.

.PARAMETER PublisherExclusions
Publishers you want removed from the results.

.PARAMETER AsJson
Switch to output results as JSON instead of PowerShell objects.

.EXAMPLE
Get-MSInstalledApps -AppExclusions "Visual Studio Code","Git" -AsJson

Returns all installed apps except VS Code and Git, formatted as JSON.

.EXAMPLE
Get-MSInstalledApps -Exceptions "Microsoft SQL Server Management Studio"

Returns all non-Microsoft apps plus SSMS even though it is published by Microsoft.


$AppExclusions =  @(
    "Visual Studio Code",
    "Microsoft Edge", 
    "Veritas NetBackup Client",
    "Notepad++ (64-bit x64)",
    "Python 2.7.16 (64-bit)",
    "Sentinel Agent",
    "Veritas NetBackup Client",
    "Node.js",
    "entinel Agent",
    "IIS Express Application Compatibility Database for x64",                                                                                               
    "IIS Express Application Compatibility Database for x86",
    "Notepad++ (64-bit x64)",
    "Microsoft SQL Server 2017",
    "Git version 2.31.1" 
)
Get-MSInstalledApps -AppExclusions $AppExclusions -AsJson

Returns all non-Microsoft apps and exclude some application.


.NOTES
Author: Sozezzo

#>
    param(
        [string[]]$Exceptions = @(),          # Microsoft apps you WANT to include
        [string[]]$AppExclusions = @(),       # Apps you want to REMOVE by DisplayName
        [string[]]$PublisherExclusions = @(), # Publishers you want to REMOVE
        [switch]$AsJson                       # Output JSON instead of objects
    )

    $paths = @(
        "HKLM:\Software\Microsoft\Windows\CurrentVersion\Uninstall\*",
        "HKLM:\Software\WOW6432Node\Microsoft\Windows\CurrentVersion\Uninstall\*",
        "HKCU:\Software\Microsoft\Windows\CurrentVersion\Uninstall\*"
    )

    $apps = foreach ($path in $paths) {
        Get-ItemProperty -Path $path -ErrorAction SilentlyContinue |
            Where-Object { $_.DisplayName -and $_.DisplayName.Trim() -ne "" } |
            Select-Object DisplayName, Publisher, DisplayVersion, InstallDate
    }

    $filtered = $apps |
        Where-Object {
            (
                $_.Publisher -notmatch "Microsoft" -or
                $Exceptions -contains $_.DisplayName
            ) -and
            ($AppExclusions -notcontains $_.DisplayName) -and
            ($PublisherExclusions -notcontains $_.Publisher)
        } |
        Sort-Object DisplayName

    if ($AsJson) {
        return $filtered | ConvertTo-Json -Depth 5
    }

    return $filtered
}
