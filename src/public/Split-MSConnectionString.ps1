function Split-MSConnectionString {
<#
.SYNOPSIS
Splits a connection string into individual tag/value segments.

.DESCRIPTION
Splits a connection string by semicolon while respecting quoted values and
brace-delimited values.

This avoids incorrectly splitting passwords or values that contain semicolons.

.PARAMETER ConnectionString
The connection string to split.

.EXAMPLE
Split-MSConnectionString `
    -ConnectionString "Data Source=MyServer;Password=`"abc;def`";Encrypt=False"

Returns segments similar to:
Data Source=MyServer
Password="abc;def"
Encrypt=False
#>
    [CmdletBinding()]
    param (
        [Parameter(Mandatory = $true)]
        [string]$ConnectionString
    )

    $parts = New-Object System.Collections.Generic.List[string]
    $current = New-Object System.Text.StringBuilder

    $insideSingleQuote = $false
    $insideDoubleQuote = $false
    $insideBrace = $false

    for ($i = 0; $i -lt $ConnectionString.Length; $i++) {
        $c = $ConnectionString[$i]

        if ($c -eq "'" -and -not $insideDoubleQuote -and -not $insideBrace) {
            $insideSingleQuote = -not $insideSingleQuote
            [void]$current.Append($c)
            continue
        }

        if ($c -eq '"' -and -not $insideSingleQuote -and -not $insideBrace) {
            $insideDoubleQuote = -not $insideDoubleQuote
            [void]$current.Append($c)
            continue
        }

        if ($c -eq '{' -and -not $insideSingleQuote -and -not $insideDoubleQuote) {
            $insideBrace = $true
            [void]$current.Append($c)
            continue
        }

        if ($c -eq '}' -and $insideBrace -and -not $insideSingleQuote -and -not $insideDoubleQuote) {
            $insideBrace = $false
            [void]$current.Append($c)
            continue
        }

        if ($c -eq ';' -and -not $insideSingleQuote -and -not $insideDoubleQuote -and -not $insideBrace) {
            $parts.Add($current.ToString())
            [void]$current.Clear()
            continue
        }

        [void]$current.Append($c)
    }

    if ($current.Length -gt 0) {
        $parts.Add($current.ToString())
    }

    return $parts
}
