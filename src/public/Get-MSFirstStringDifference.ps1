
function Get-MSFirstStringDifference {
<#
.SYNOPSIS
Returns the first difference between two strings.

.DESCRIPTION
Compares two strings character by character and returns detailed
information about the first difference found.

The function can perform case-sensitive or case-insensitive comparisons
and provides the following information:

- Absolute character position of the difference
- Line number and column number
- Differing characters from each string
- Complete line contents from both strings
- Context preview around the difference
- End-of-string (EOS) detection when strings have different lengths

This function is useful for troubleshooting configuration files,
PowerShell scripts, SQL scripts, JSON, XML, CSV files, generated text,
and other text-based content.

.PARAMETER String1
The first string to compare.

.PARAMETER String2
The second string to compare.

.PARAMETER IgnoreCase
Performs a case-insensitive comparison.

.PARAMETER ContextRadius
Number of characters displayed before and after the differing
character in the context preview.

Default: 25

.EXAMPLE
Get-MSFirstStringDifference `
    -String1 "Hello World" `
    -String2 "Hello Word"

Returns information about the first differing character.

.EXAMPLE
Get-MSFirstStringDifference `
    -String1 (Get-Content .\File1.txt -Raw) `
    -String2 (Get-Content .\File2.txt -Raw)

Compares the contents of two text files.

.EXAMPLE
Get-MSFirstStringDifference `
    -String1 $Json1 `
    -String2 $Json2 `
    -IgnoreCase

Performs a case-insensitive comparison of two JSON strings.

.EXAMPLE
$result = Get-MSFirstStringDifference `
    -String1 $ScriptA `
    -String2 $ScriptB

if (-not $result.AreEqual) {
    $result | Format-List *
}

Displays detailed information about the first difference found.

.OUTPUTS
System.Management.Automation.PSCustomObject

Returns an object containing comparison results, including:
- AreEqual
- AbsoluteIndex
- LineNumber
- ColumnNumber
- Char1
- Char2
- Line1
- Line2
- Context1
- Context1Mark
- Context2
- Context2Mark
- Note

.NOTES
Author  : Milton Sozezzo
Version : 1.0

Line and column numbers are 1-based.

When one string ends before the other, the function reports
the difference using the special value '<EOS>' (End Of String).

Context previews may span multiple lines and display newline
characters using visible symbols for readability.
#>    
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)] [string]$String1,
        [Parameter(Mandatory)] [string]$String2,
        [switch]$IgnoreCase,
        [int]$ContextRadius = 25   # chars to show on each side in context preview
    )

    function Get-LineColumn {
        param([string]$s, [int]$index)

        if ($index -lt 0) { return [pscustomobject]@{ Line=0; Column=0; LineText='' } }

        # Handle empty/newline-only safely
        $n = $s.Length
        if ($n -eq 0) { return [pscustomobject]@{ Line=1; Column=1; LineText='' } }

        # Count lines using `n as the line separator (works for `r`n too)
        $line = (($s.Substring(0, [Math]::Min($index, $n))) -split "`n").Count
        if ($line -lt 1) { $line = 1 }

        # Find start of this line (last `n before index)
        $lastNl = $s.LastIndexOf("`n", [Math]::Min($index, $n - 1))
        $lineStart = if ($lastNl -ge 0) { $lastNl + 1 } else { 0 }

        # Column is 1-based; account for `r that may precede `n
        $col = $index - $lineStart + 1
        if ($col -lt 1) { $col = 1 }

        # Extract the entire line text (until next `n or end)
        $nextNl = $s.IndexOf("`n", $lineStart)
        $lineEnd = if ($nextNl -ge 0) { $nextNl } else { $n }
        $lineText = $s.Substring($lineStart, $lineEnd - $lineStart).TrimEnd("`r")

        [pscustomobject]@{
            Line     = $line
            Column   = $col
            LineText = $lineText
        }
    }

    function Get-Context {
        param([string]$s, [int]$index, [int]$radius, [int]$col1Based)

        if ($s.Length -eq 0) {
            return [pscustomobject]@{ Text=''; Marker='' }
        }

        # Window around the absolute index (not line-bound)
        $start = [Math]::Max(0, $index - $radius)
        $len   = [Math]::Min($s.Length - $start, 2*$radius + 1)
        $text  = $s.Substring($start, $len)

        # Compute marker position within this window
        $markerPos = $index - $start
        if ($markerPos -lt 0) { $markerPos = 0 }
        $marker = (' ' * $markerPos) + '^'

        # For readability, replace newlines in context preview
        $text = $text -replace "`r", '␍' -replace "`n", '␊'

        [pscustomobject]@{
            Text   = $text
            Marker = $marker
        }
    }

    # Choose equality comparison
    $comparison = if ($IgnoreCase) { [System.StringComparison]::OrdinalIgnoreCase }
                  else              { [System.StringComparison]::Ordinal }

    $minLen = [Math]::Min($String1.Length, $String2.Length)

    # Scan for first differing char
    $diffIndex = -1
    for ($i = 0; $i -lt $minLen; $i++) {
        if (-not [System.String]::Equals($String1[$i], $String2[$i], $comparison)) {
            $diffIndex = $i
            break
        }
    }

    if ($diffIndex -eq -1) {
        if ($String1.Length -ne $String2.Length) {
            # First difference is exactly at the end of the shorter string
            $diffIndex = $minLen
        } else {
            return [pscustomobject]@{
                AreEqual       = $true
                AbsoluteIndex  = -1
                LineNumber     = 0
                ColumnNumber   = 0
                Char1          = $null
                Char2          = $null
                Line1          = $null
                Line2          = $null
                Context1       = $null
                Context2       = $null
                Note           = 'Strings are identical.'
            }
        }
    }

    # Gather per-string position info (use index but cap to length-1 for EOS cases)
    $pos1 = Get-LineColumn -s $String1 -index ([Math]::Min($diffIndex, [Math]::Max(0, $String1.Length - 1)))
    $pos2 = Get-LineColumn -s $String2 -index ([Math]::Min($diffIndex, [Math]::Max(0, $String2.Length - 1)))

    $char1 = if ($diffIndex -lt $String1.Length) { $String1[$diffIndex] } else { '<EOS>' }
    $char2 = if ($diffIndex -lt $String2.Length) { $String2[$diffIndex] } else { '<EOS>' }

    $ctx1 = Get-Context -s $String1 -index ([Math]::Min($diffIndex, [Math]::Max(0, $String1.Length - 1))) -radius $ContextRadius -col1Based $pos1.Column
    $ctx2 = Get-Context -s $String2 -index ([Math]::Min($diffIndex, [Math]::Max(0, $String2.Length - 1))) -radius $ContextRadius -col1Based $pos2.Column

    [pscustomobject]@{
        AreEqual      = $false
        AbsoluteIndex = $diffIndex                 # 0-based absolute char index
        LineNumber    = $pos1.Line                 # 1-based
        ColumnNumber  = $pos1.Column               # 1-based (for String1 position)
        Char1         = $char1.ToString()
        Char2         = $char2.ToString()
        Line1         = $pos1.LineText
        Line2         = $pos2.LineText
        Context1      = $ctx1.Text
        Context1Mark  = $ctx1.Marker
        Context2      = $ctx2.Text
        Context2Mark  = $ctx2.Marker
        Note          = if ($char1 -eq '<EOS>' -or $char2 -eq '<EOS>') { 'Difference is at end of one string.' } else { $null }
    }
}

