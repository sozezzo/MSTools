function Convert-MSArrayToHtml {
<#
.SYNOPSIS
  Converts an array of objects into an HTML table, optionally as a full HTML document.

.DESCRIPTION
  Takes an array of PowerShell objects and renders them as an HTML table with basic
  inline CSS styling (suitable for emails or reports). Supports column filtering and
  two output modes: a full HTML document or a table fragment for embedding.

.PARAMETER InputObject
  The array of objects to convert. Each object becomes a table row.

.PARAMETER Title
  Title displayed as an <h2> heading above the table (default: "Report").

.PARAMETER Columns
  Optional list of property names to include as columns. If omitted, all properties are used.

.PARAMETER NoFragment
  When specified, outputs a full HTML document (with <html>, <head>, <body> tags).
  By default, outputs only the CSS + <h2> + <table> fragment (safe for embedding in email).

.OUTPUTS
  System.String — HTML content as a string.

.EXAMPLE
  $data | Convert-MSArrayToHtml -Title "Server Report"

.EXAMPLE
  $data | Convert-MSArrayToHtml -Title "Jobs" -Columns "Name","Status" -NoFragment

.NOTES
  Author : Sozezzo Astra
#>
    [CmdletBinding()]
    Param(
        [Parameter(Mandatory)]
        [array]$InputObject,

        [string]$Title = "Report",

        [string[]]$Columns,

        [switch]$NoFragment  # <-- NEW
    )

    # CSS (email-safe)
    $css = @"
<style>
table {
    border-collapse: collapse;
    width: 100%;
    font-family: Arial;
    font-size: 13px;
}
th, td {
    border: 1px solid #ccc;
    padding: 6px;
}
th {
    background-color: #f2f2f2;
    font-weight: bold;
}
tr:nth-child(even) {
    background-color: #fafafa;
}
</style>
"@

    # Filter columns (if requested)
    if ($Columns) {
        $data = $InputObject | Select-Object -Property $Columns
    }
    else {
        $data = $InputObject
    }

    if ($NoFragment) {
        # Full HTML document
        return $data | ConvertTo-Html -As Table -Title $Title -Head $css
    }
    else {
        # Only the table fragment (safe for embedding)
        $table = $data | ConvertTo-Html -As Table -Fragment
        return "$css`n<h2>$Title</h2>`n$table"
    }
}