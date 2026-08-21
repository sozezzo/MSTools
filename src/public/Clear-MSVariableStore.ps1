function Clear-MSVariableStore {
<#
.SYNOPSIS
  Deletes the entire MS variable store.
#>
    [CmdletBinding(SupportsShouldProcess)]
    param(
        [string]$Path,
        [string]$StoreName
    )

    $storePath = Get-MSVariableStorePath -Path $Path -StoreName $StoreName

    if (-not (Test-Path $storePath)) {
        return
    }

    if ($PSCmdlet.ShouldProcess($storePath, "Clear MS variable store")) {
        Remove-Item -Path $storePath -Force
    }
}