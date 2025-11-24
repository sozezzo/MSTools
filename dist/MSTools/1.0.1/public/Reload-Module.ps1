function Reload-Module {
<#
.SYNOPSIS
    Forces a PowerShell module to reload by removing and re-importing it.

.DESCRIPTION
    Reload-Module removes an already loaded module from the current PowerShell session
    and re-imports it from disk using Import-Module -Force.
    This is useful during development or debugging when you’ve made changes
    to a module and need to refresh it without restarting PowerShell.

.PARAMETER ModuleName
    The name of the module to reload. Must match an existing module
    available in the current session or in any module path.

.EXAMPLES
    # 1) Reload a module named "MyCustomTools"
    Reload-Module -ModuleName "MyCustomTools"

    # 2) Reload dbatools after making edits
    Reload-Module dbatools

    # 3) Use inside your development session to refresh a local module
    Reload-Module .\MyModule.psm1

.OUTPUTS
    Displays module name, version, and path in a formatted table.

.NOTES
    - Uses Write-Host for feedback.
    - Safe to run even if the module is not loaded (fails silently on Remove-Module).
    - Helpful when iterating on PowerShell module code.

    Equivalent to:
        Remove-Module <Name> -Force
        Import-Module  <Name> -Force
        Get-Module     <Name>

#>
    param([string]$ModuleName)
    Write-Host "Re-importing module $ModuleName ..."
    Remove-Module -Name $ModuleName -Force -ErrorAction SilentlyContinue
    Import-Module -Name $ModuleName -Force -ErrorAction Stop
    (Get-Module $ModuleName) | Format-Table Name, Version, Path -AutoSize
}