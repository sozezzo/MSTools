function Test-MSFolderOrStop {
<#
.SYNOPSIS
  Ensures a folder exists; tries to create it if missing.
  Returns:
    $true  - if folder existed or was created
    $false - if folder does not exist and cannot be created
#>
  [CmdletBinding()]
  param(
    [Parameter(Mandatory = $true)]
    [string]$Path,

    [Parameter(Mandatory = $false)]
    [string]$Name = 'Folder'   # logical identifier
  )

  try {
    # Folder already exists → OK
    if (Test-Path -LiteralPath $Path) {
      return $true
    }

    # Folder missing → try to create
    New-Item -ItemType Directory -Path $Path -Force | Out-Null
    return $true
  }
  catch {
    # Do NOT throw — return false
    $msg = "[{0}] Unable to create folder '{1}' : {2}" -f $Name, $Path, $_.Exception.Message
    Write-Error $msg
    return $false
  }
}