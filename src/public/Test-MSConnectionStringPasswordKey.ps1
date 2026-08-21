function Test-MSConnectionStringPasswordKey {
    <#
    .SYNOPSIS
    Tests whether a connection-string key represents a password field.

    .DESCRIPTION
    Returns $true when the supplied key, after trimming, equals 'Password' or
    'Pwd' (case-insensitive). Used when parsing connection strings to locate the
    password tag.

    .PARAMETER Key
    The connection-string key name to test.

    .EXAMPLE
    Test-MSConnectionStringPasswordKey -Key 'Password'

    Returns $true.

    .EXAMPLE
    Test-MSConnectionStringPasswordKey -Key 'Data Source'

    Returns $false.
    #>
    [CmdletBinding()]
    param (
        [Parameter(Mandatory = $true)]
        [string]$Key
    )

    $normalizedKey = $Key.Trim()

    return (
        $normalizedKey -ieq "Password" -or
        $normalizedKey -ieq "Pwd"
    )
}
