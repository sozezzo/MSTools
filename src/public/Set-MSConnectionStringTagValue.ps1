function Set-MSConnectionStringTagValue {
<#
.SYNOPSIS
Sets or adds a tag/value pair inside a connection string.

.DESCRIPTION
Searches a connection string for a specific tag name. If the tag already exists,
its value is replaced. If the tag does not exist, the tag/value pair is added
to the end of the connection string.

The function uses Split-MSConnectionString, so semicolons inside quoted values
or braces are handled correctly.

.PARAMETER ConnectionString
The source connection string.

.PARAMETER Tag
The connection string tag to set, for example "Application Name".

.PARAMETER Value
The new value for the tag.

.EXAMPLE
Set-MSConnectionStringTagValue `
    -ConnectionString "Data Source=MyServer;Integrated Security=True" `
    -Tag "Application Name" `
    -Value "MyApplication-DbaDash"

Returns:
Data Source=MyServer;Integrated Security=True;Application Name=MyApplication-DbaDash

.EXAMPLE
Set-MSConnectionStringTagValue `
    -ConnectionString "Data Source=MyServer;Application Name=OldApp" `
    -Tag "Application Name" `
    -Value "MyApplication-DbaDash"

Returns:
Data Source=MyServer;Application Name=MyApplication-DbaDash
#>
    [CmdletBinding()]
    param (
        [Parameter(Mandatory = $true)]
        [string]$ConnectionString,

        [Parameter(Mandatory = $true)]
        [string]$Tag,

        [Parameter(Mandatory = $true)]
        [string]$Value
    )

    if ([string]::IsNullOrWhiteSpace($ConnectionString)) {
        return $ConnectionString
    }

    if ([string]::IsNullOrWhiteSpace($Tag)) {
        return $ConnectionString
    }

    $tagToSet = $Tag.Trim()
    $parts = Split-MSConnectionString -ConnectionString $ConnectionString

    $result = New-Object System.Text.StringBuilder
    $tagWasFound = $false

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

        if ($currentTag -ieq $tagToSet) {
            Add-MSConnectionStringPart -Builder $result -Part "$tagToSet=$Value"
            $tagWasFound = $true
            continue
        }

        Add-MSConnectionStringPart -Builder $result -Part $trimmedPart
    }

    if (-not $tagWasFound) {
        Add-MSConnectionStringPart -Builder $result -Part "$tagToSet=$Value"
    }

    return $result.ToString()
}
