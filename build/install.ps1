<# ======================================================================
 install.ps1
 - Copies latest build from .\dist\MSTools\<version>\ to:
     C:\Program Files\WindowsPowerShell\Modules\MSTools\<version>\
 - Re-imports the module
 - Applies basic security hygiene:
     * Unblocks downloaded files (Zone.Identifier)
     * (Optional) Hardens ACLs on the target folder
     * (Optional) Assists with ExecutionPolicy for loading unsigned scripts
     * Reports Authenticode signature status of module files
 Requirements: Run as Administrator
====================================================================== #>

[CmdletBinding()]
param(
    [string]$ModuleName = 'MSTools',

    # If you want to install a specific version from dist\..., pass it here; otherwise we pick the highest version
    [string]$Version = $null,

    # Also install for PowerShell 7+ (C:\Program Files\PowerShell\Modules)
    [switch]$AlsoInstallForPwsh7,

    # Apply secure default ACLs on the installed folder
    [switch]$HardenAcl = $true,

    # If execution policy blocks loading unsigned modules, set CurrentUser to RemoteSigned (safer than changing LocalMachine)
    [switch]$FixExecutionPolicy
)


# --- Auto-elevate if not running as Administrator --------------------------
if (-not ([Security.Principal.WindowsPrincipal] [Security.Principal.WindowsIdentity]::GetCurrent()
).IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)) {

    Write-Host "** Elevating privileges (Administrator required)..." -ForegroundColor Yellow

    $psi = New-Object System.Diagnostics.ProcessStartInfo
    $psi.FileName  = (Get-Process -Id $PID).Path
    $psi.Arguments = '"' + $MyInvocation.MyCommand.Definition + '" ' + ($args -join ' ')
    $psi.Verb      = 'runas'   # triggers UAC prompt
    try {
        [System.Diagnostics.Process]::Start($psi) | Out-Null
    } catch {
        Write-Warning "Elevation cancelled by user."
    }
    exit
}
# --------------------------------------------------------------------------



$ErrorActionPreference = 'Stop'

function Assert-Admin {
    $id = [Security.Principal.WindowsIdentity]::GetCurrent()
    $p  = New-Object Security.Principal.WindowsPrincipal($id)
    if (-not $p.IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)) {
        throw "Please run this script in an elevated (Administrator) PowerShell."
    }
}

function Get-RepoRoot {
    # repo root assumed = parent of this script's folder
    return (Split-Path -Parent $PSScriptRoot)
}

function Get-LatestVersionFolder {
    param([string]$BasePath)

    $candidates = Get-ChildItem -LiteralPath $BasePath -Directory -ErrorAction Stop |
                  Where-Object { $_.Name -match '^\d+(\.\d+){1,3}$' } |
                  Sort-Object { [version]$_.Name } -Descending
    if (-not $candidates) { return $null }
    return $candidates[0]
}

function Get-SourceFolder {
    param([string]$RepoRoot, [string]$ModuleName, [string]$Version)

    $distModule = Join-Path $RepoRoot "dist\$ModuleName"
    if (-not (Test-Path $distModule)) {
        throw "Build output not found: $distModule. Run your build script first."
    }

    if ($Version) {
        $src = Join-Path $distModule $Version
        if (-not (Test-Path $src)) { throw "Specified version not found in dist: $src" }
        return (Get-Item $src)
    }

    $latest = Get-LatestVersionFolder -BasePath $distModule
    if (-not $latest) { throw "No versioned folders found under $distModule." }
    return $latest
}

function Install-ToPath {
    param(
        [Parameter(Mandatory)][string]$SourcePath,   # e.g. D:\repo\dist\MSTools\1.0.1
        [Parameter(Mandatory)][string]$DestBase,     # e.g. C:\Program Files\WindowsPowerShell\Modules
        [Parameter(Mandatory)][string]$ModuleName
    )

    $versionName     = Split-Path $SourcePath -Leaf
    $destModuleRoot  = Join-Path $DestBase $ModuleName
    $dest            = Join-Path $destModuleRoot $versionName

    # Clean any prior install of this version
    if (Test-Path -LiteralPath $dest) {
        Write-Verbose "Removing existing destination: $dest"
        Remove-Item -LiteralPath $dest -Recurse -Force
    }

    # Ensure module root and version folder are directories
    if (-not (Test-Path -LiteralPath $destModuleRoot)) {
        New-Item -ItemType Directory -Path $destModuleRoot -Force | Out-Null
    }
    New-Item -ItemType Directory -Path $dest -Force | Out-Null  # <-- key line

    Write-Host "Copying $ModuleName $versionName -> $dest"

    # Copy items one by one to avoid wildcard/directory semantics issues
    Get-ChildItem -LiteralPath $SourcePath | ForEach-Object {
        Copy-Item -LiteralPath $_.FullName -Destination $dest -Recurse -Force
    }

    return $dest
}

function Unblock-Tree {
    param([string]$Path)
    Write-Verbose "Unblocking files in $Path"
    Get-ChildItem -LiteralPath $Path -Recurse -File | ForEach-Object {
        try { Unblock-File -LiteralPath $_.FullName -ErrorAction Stop } catch { }
    }
}

function Harden-ModuleAcl {
    param([Parameter(Mandatory)][string]$TargetPath)

    Write-Host "Hardening ACL on: $TargetPath"

    $acl = Get-Acl -LiteralPath $TargetPath

    # Disable inheritance and do NOT copy existing ACEs (start clean)
    $acl.SetAccessRuleProtection($true, $false)

    # Enum shortcuts
    $FSR = [System.Security.AccessControl.FileSystemRights]
    $INF = [System.Security.AccessControl.InheritanceFlags]
    $PRF = [System.Security.AccessControl.PropagationFlags]
    $ACT = [System.Security.AccessControl.AccessControlType]

    $inherit = $INF::ContainerInherit -bor $INF::ObjectInherit
    $prop    = $PRF::None

    $rules = @(
        (New-Object System.Security.AccessControl.FileSystemAccessRule -ArgumentList @(
            'NT AUTHORITY\SYSTEM', $FSR::FullControl, $inherit, $prop, $ACT::Allow
        )),
        (New-Object System.Security.AccessControl.FileSystemAccessRule -ArgumentList @(
            'BUILTIN\Administrators', $FSR::FullControl, $inherit, $prop, $ACT::Allow
        )),
        (New-Object System.Security.AccessControl.FileSystemAccessRule -ArgumentList @(
            'BUILTIN\Users', $FSR::ReadAndExecute, $inherit, $prop, $ACT::Allow
        ))
    )

    # Add the rules
    foreach ($r in $rules) { $acl.AddAccessRule($r) }

    # Apply to folder (will inherit to children)
    Set-Acl -LiteralPath $TargetPath -AclObject $acl
}


function Check-ExecutionPolicy {
    if ($FixExecutionPolicy) {
        $cur = Get-ExecutionPolicy -Scope CurrentUser
        if ($cur -in @('Undefined','Restricted','AllSigned','Default')) {
            Write-Host "Setting ExecutionPolicy for CurrentUser to RemoteSigned..."
            Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser -Force
        } else {
            Write-Verbose "CurrentUser ExecutionPolicy already: $cur"
        }
    } else {
        $lm = Get-ExecutionPolicy -Scope LocalMachine
        if ($lm -in @('Restricted','AllSigned')) {
            Write-Warning "LocalMachine ExecutionPolicy is $lm. If module loading fails due to signatures, re-run with -FixExecutionPolicy or sign the module."
        }
    }
}

function Report-SignatureStatus {
    param([string]$Folder)
    Write-Host "Signature status for module files:"
    Get-ChildItem -LiteralPath $Folder -Recurse -File -Include *.ps1,*.psm1,*.psd1 |
      ForEach-Object {
        $sig = Get-AuthenticodeSignature -LiteralPath $_.FullName
        [PSCustomObject]@{
            File = $_.FullName
            Status = $sig.Status
            Signer = $sig.SignerCertificate.Subject -as [string]
        }
      } | Format-Table -AutoSize
}

function Reload-Module {
    param([string]$ModuleName)
    Write-Host "Re-importing module $ModuleName ..."
    Remove-Module -Name $ModuleName -Force -ErrorAction SilentlyContinue
    Import-Module -Name $ModuleName -Force -ErrorAction Stop
    (Get-Module $ModuleName) | Format-Table Name, Version, Path -AutoSize
}

# ------------------- main -------------------
Assert-Admin

$repo = Get-RepoRoot
$srcFolder = Get-SourceFolder -RepoRoot $repo -ModuleName $ModuleName -Version $Version
$srcVersion = $srcFolder.Name
Write-Host "Installing $ModuleName version $srcVersion from:`n$($srcFolder.FullName)"

# Windows PowerShell target (v5.1)
$destWinPS = "C:\Program Files\WindowsPowerShell\Modules"
$installedPath1 = Install-ToPath -SourcePath $srcFolder.FullName -DestBase $destWinPS -ModuleName $ModuleName

# PowerShell 7+ target (optional)
if ($AlsoInstallForPwsh7) {
    $destPwsh7 = "C:\Program Files\PowerShell\Modules"
    $null = Install-ToPath -SourcePath $srcFolder.FullName -DestBase $destPwsh7 -ModuleName $ModuleName
}

# Security hygiene
Unblock-Tree -Path $installedPath1
if ($HardenAcl) { Harden-ModuleAcl -TargetPath (Split-Path -Parent $installedPath1) }  # set ACLs at module root so they inherit
Check-ExecutionPolicy
Report-SignatureStatus -Folder $installedPath1

# Reload module (current session)
Reload-Module -ModuleName $ModuleName

Write-Host "** Done."
