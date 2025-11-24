
param(
  [ValidateSet('major','minor','patch','set')]
  [string]$Mode = 'patch',
  [string]$ModuleName = 'MSTools',
  [string]$Version    # required for Mode 'set'
)

$ErrorActionPreference = 'Stop'

$RepoRoot = Split-Path -Parent $PSScriptRoot
$Psd1     = Get-ChildItem -Path (Join-Path $RepoRoot 'src') -Filter "$ModuleName.psd1" | Select-Object -First 1
if (-not $Psd1) { throw "Manifest '$ModuleName.psd1' not found in src/." }

$Hash = Import-PowerShellDataFile $Psd1.FullName
[version]$v = $Hash.ModuleVersion
if (-not $v) { throw "ModuleVersion missing in $($Psd1.FullName)." }

switch ($Mode) {
  'major' { $v = [version]"$($v.Major+1).0.0" }
  'minor' { $v = [version]"$($v.Major).$($v.Minor+1).0" }
  'patch' { $v = [version]"$($v.Major).$($v.Minor).$($v.Build+1)" }
  'set'   { if (-not $Version) { throw "Provide -Version for Mode 'set'." }; $v = [version]$Version }
}

# Update only the version field; preserve other metadata
$Hash.ModuleVersion = $v.ToString()

New-ModuleManifest -Path $Psd1.FullName `
  -RootModule        $Hash.RootModule `
  -ModuleVersion     $Hash.ModuleVersion `
  -Author            $Hash.Author `
  -CompanyName       $Hash.CompanyName `
  -Description       $Hash.Description `
  -PowerShellVersion $Hash.PowerShellVersion `
  -FunctionsToExport $Hash.FunctionsToExport `
  -CmdletsToExport   $Hash.CmdletsToExport `
  -AliasesToExport   $Hash.AliasesToExport `
  -VariablesToExport $Hash.VariablesToExport `
  -Guid              $Hash.Guid `
  -PrivateData       $Hash.PrivateData | Out-Null

Write-Host "* Manifest version updated to $($Hash.ModuleVersion)"

