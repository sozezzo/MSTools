function Add-MSConnectionStringPart {
<#
.SYNOPSIS
Appends one segment to a connection string builder.

.DESCRIPTION
Appends a connection string segment to an existing StringBuilder.
If the builder already contains text, a semicolon is added before the new part.

This is an internal helper function.

.PARAMETER Builder
The StringBuilder used to build the final connection string.

.PARAMETER Part
The connection string segment to append.

.EXAMPLE
$builder = New-Object System.Text.StringBuilder

Add-MSConnectionStringPart `
    -Builder $builder `
    -Part "Data Source=MyServer"

Add-MSConnectionStringPart `
    -Builder $builder `
    -Part "Encrypt=False"

$builder.ToString()

Returns:
Data Source=MyServer;Encrypt=False
#>
    [CmdletBinding()]
    param (
        [Parameter(Mandatory = $true)]
        [System.Text.StringBuilder]$Builder,

        [Parameter(Mandatory = $true)]
        [string]$Part
    )

    if ($Builder.Length -gt 0) {
        [void]$Builder.Append(';')
    }

    [void]$Builder.Append($Part)
}
