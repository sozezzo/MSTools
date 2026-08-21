# Minimal autoloader for development (source)
if (Test-Path "$PSScriptRoot\Classes") {
  Get-ChildItem -LiteralPath "$PSScriptRoot\Classes" -Filter *.ps1 -Recurse -ErrorAction SilentlyContinue |
    ForEach-Object { . $_.FullName }
}
if (Test-Path "$PSScriptRoot\Private") {
  Get-ChildItem -LiteralPath "$PSScriptRoot\Private" -Filter *.ps1 -Recurse -ErrorAction SilentlyContinue |
    ForEach-Object { . $_.FullName }
}
if (Test-Path "$PSScriptRoot\Public") {
  Get-ChildItem -LiteralPath "$PSScriptRoot\Public" -Filter *.ps1 -Recurse -ErrorAction SilentlyContinue |
    ForEach-Object { . $_.FullName }
}
# Exports are explicit in the dist manifest/psm1 during packaging.
