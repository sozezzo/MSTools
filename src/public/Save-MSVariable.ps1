function Save-MSVariable {
<#
.SYNOPSIS
  Saves or updates a variable in the MS store.

.DESCRIPTION
  Persists a key/value pair into a CLIXML-backed store.
  Supports most common PowerShell types including arrays, hashtables, DateTime, and PSCustomObject.
  Limitations: objects that are not serializable (e.g., scriptblocks, live connections/handles, COM objects)
  cannot be saved meaningfully. Some complex .NET objects may deserialize as "deserialized.*" data objects.

.PARAMETER VariableName
  The key name used to store the value.

.PARAMETER Value
  The value to store (arrays and nested objects are supported when serializable).

.PARAMETER Path
  Optional explicit store file path.

.PARAMETER StoreName
  Optional store name used for default AppData-backed path (interactive mode).
#>
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][Alias('Name')]
        [string]$VariableName,

        [Parameter(Mandatory)] 
        $Value,

        [string]$Path,
        [string]$StoreName
    )

    $store = Get-MSVariableStore -Path $Path -StoreName $StoreName
    $store[$VariableName] = $Value
    Save-MSVariableStore -Store $store -Path $Path -StoreName $StoreName | Out-Null
}