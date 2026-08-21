function Remove-MSConnectionStringTag {
<#
.SYNOPSIS
Removes a tag from a connection string.

.DESCRIPTION
Removes a tag/value pair from a connection string by tag name.
The tag comparison is case-insensitive.

The function uses Split-MSConnectionString, so semicolons inside quoted values
or braces are handled correctly.

.PARAMETER ConnectionString
The source connection string.

.PARAMETER Tag
The tag to remove.

.EXAMPLE
Remove-MSConnectionStringTag `
    -ConnectionString "Data Source=MyServer;Encrypt=False;Trust Server Certificate=True" `
    -Tag "Trust Server Certificate"

Returns:
Data Source=MyServer;Encrypt=False
#>
    [CmdletBinding()]
    param (
        [Parameter(Mandatory = $true)]
        [string]$ConnectionString,

        [Parameter(Mandatory = $true)]
        [string]$Tag
    )

    if ([string]::IsNullOrWhiteSpace($ConnectionString)) {
        return $ConnectionString
    }

    if ([string]::IsNullOrWhiteSpace($Tag)) {
        return $ConnectionString
    }

    $tagToRemove = $Tag.Trim()
    $parts = Split-MSConnectionString -ConnectionString $ConnectionString

    $result = New-Object System.Text.StringBuilder

    foreach ($part in $parts) {
        $trimmedPart = $part.Trim()

        if ($trimmedPart.Length -eq 0) {
            continue
        }

        $equalIndex = $trimmedPart.IndexOf('=')

        if ($equalIndex -le 0) {
            Add-MSConnectionStringPart -Builder $result -Part $trimmedPart
            continue
        }

        $currentTag = $trimmedPart.Substring(0, $equalIndex).Trim()

        if ($currentTag -ieq $tagToRemove) {
            continue
        }

        Add-MSConnectionStringPart -Builder $result -Part $trimmedPart
    }

    return $result.ToString()
}
