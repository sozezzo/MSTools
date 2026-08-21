function Find-MSNextConnectionStringSeparator {
<#
.SYNOPSIS
Finds the next semicolon separator in a connection string.

.DESCRIPTION
Finds the next semicolon that separates connection string segments.
Semicolons inside quoted values are ignored.

.PARAMETER Text
The full connection string text.

.PARAMETER StartIndex
The index where the search starts.

.EXAMPLE
Find-MSNextConnectionStringSeparator `
    -Text "Data Source=MyServer;Password=`"abc;def`";Encrypt=False" `
    -StartIndex 0

Returns the index of the next valid connection string separator.
#>
    [CmdletBinding()]
    param (
        [Parameter(Mandatory = $true)]
        [string]$Text,

        [Parameter(Mandatory = $true)]
        [int]$StartIndex
    )

    $insideQuote = $false
    $quoteChar = [char]0

    for ($i = $StartIndex; $i -lt $Text.Length; $i++) {
        $c = $Text[$i]

        if ($c -eq '"' -or $c -eq "'") {
            if (-not $insideQuote) {
                $insideQuote = $true
                $quoteChar = $c
            }
            elseif ($quoteChar -eq $c) {
                if ($i + 1 -lt $Text.Length -and $Text[$i + 1] -eq $c) {
                    $i++
                    continue
                }

                $insideQuote = $false
                $quoteChar = [char]0
            }
        }
        elseif ($c -eq ';' -and -not $insideQuote) {
            return $i
        }
    }

    return $Text.Length
}
