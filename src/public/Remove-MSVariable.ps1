function Remove-MSVariable {
<#
.SYNOPSIS
  Removes a variable from the MS store.
#>
    [CmdletBinding(SupportsShouldProcess)]
    param(
        [Parameter(Mandatory)][Alias('Name')]
        [string]$VariableName,

        [string]$Path,
        [string]$StoreName,

        [switch]$PassThru
    )

    $storePath = Get-MSVariableStorePath -Path $Path -StoreName $StoreName
    $store     = Get-MSVariableStore -Path $Path -StoreName $StoreName

    if (-not $store.ContainsKey($VariableName)) {
        if ($PassThru) { return $false }
        return
    }

    if ($PSCmdlet.ShouldProcess($storePath, "Remove variable '$VariableName'")) {
        [void]$store.Remove($VariableName)

        if ($store.Count -eq 0) {
            if (Test-Path $storePath) {
                Remove-Item -Path $storePath -Force
            }
        }
        else {
            Save-MSVariableStore -Store $store -Path $Path -StoreName $StoreName | Out-Null
        }
    }

    if ($PassThru) { return $true }
}