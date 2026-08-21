function Get-MSVariable {
<#
.SYNOPSIS
  Retrieves a variable value from the MS store.
#>
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][Alias('Name')]
        [string]$VariableName,

        [string]$Path,
        [string]$StoreName,

        $DefaultValue = $null,

        [switch]$ThrowIfMissing
    )

    $store = Get-MSVariableStore -Path $Path -StoreName $StoreName

    if ($store.ContainsKey($VariableName)) {
        return $store[$VariableName]
    }

    if ($ThrowIfMissing) {
        $storePath = Get-MSVariableStorePath -Path $Path -StoreName $StoreName
        throw "MS variable '$VariableName' not found in store: $storePath"
    }

    return $DefaultValue
}