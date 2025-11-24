function Compare-DbaCredential {
<#
.SYNOPSIS
  Compares SQL Server credentials (metadata only) between two instances.

.DESCRIPTION
  Uses dbatools Get-DbaCredential to load credentials from Source and Destination.
  Compares by Name and selected properties (default: Identity).
  Password/secret cannot be compared (SQL Server never exposes it).

.PARAMETER Source
  Source SQL instance (Server\Instance, hostname, FQDN, alias).

.PARAMETER Destination
  Destination SQL instance.

.PARAMETER Credential
  Optional filter (name or wildcard) to limit which credentials are compared.

.PARAMETER Properties
  Which properties to compare (default: 'Identity').

.PARAMETER CaseInsensitive
  Treat name comparisons as case-insensitive (default: $true).

.PARAMETER ShowEqual
  Include rows that are identical (default: $false).

.NOTES
  Works in Windows PowerShell 5.1 and PowerShell 7+.
#>
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)] [string] $Source,
        [Parameter(Mandatory)] [string] $Destination,
        [string] $Credential,
        [string[]] $Properties = @('Identity'),
        [switch] $CaseInsensitive,
        [switch] $ShowEqual
    )

    # 1) Load credentials
    $src = if ($Credential) {
        Get-DbaCredential -SqlInstance $Source -Credential $Credential -ErrorAction Stop
    } else {
        Get-DbaCredential -SqlInstance $Source -ErrorAction Stop
    }

    $dst = if ($Credential) {
        Get-DbaCredential -SqlInstance $Destination -Credential $Credential -ErrorAction Stop
    } else {
        Get-DbaCredential -SqlInstance $Destination -ErrorAction Stop
    }

    # 2) Build lookup by Name (case sensitivity handling)
    if ($CaseInsensitive) {
        $cmp = 'InvariantCultureIgnoreCase'
    } else {
        $cmp = 'InvariantCulture'
    }

    $dstMap = @{}
    foreach ($d in $dst) { $dstMap[$d.Name] = $d }

    $results = New-Object System.Collections.Generic.List[object]

    # 3) Walk source set and compare
    foreach ($s in $src) {
        $hasDst = $dstMap.ContainsKey($s.Name)
        if (-not $hasDst) {
            $results.Add([pscustomobject]@{
                Name                    = $s.Name
                SourceIdentity          = $s.Identity
                DestinationIdentity     = $null
                Status                  = 'MissingOnDestination'
                CanPasswordsBeCompared  = $false
            })
            continue
        }

        $d = $dstMap[$s.Name]
        $diffs = @()

        foreach ($p in $Properties) {
            $sv = $s.$p
            $dv = $d.$p
            $equal = $true
            if ($sv -is [string] -and $dv -is [string]) {
                $equal = [string]::Compare($sv, $dv, $CaseInsensitive) -eq 0
            } else {
                $equal = ($sv -eq $dv)
            }
            if (-not $equal) { $diffs += $p }
        }

        if ($diffs.Count -gt 0) {
            $results.Add([pscustomobject]@{
                Name                    = $s.Name
                SourceIdentity          = $s.Identity
                DestinationIdentity     = $d.Identity
                Status                  = 'PropertyMismatch'
                DifferentProperties     = ($diffs -join ',')
                CanPasswordsBeCompared  = $false
            })
        } elseif ($ShowEqual) {
            $results.Add([pscustomobject]@{
                Name                    = $s.Name
                SourceIdentity          = $s.Identity
                DestinationIdentity     = $d.Identity
                Status                  = 'Equal'
                DifferentProperties     = ''
                CanPasswordsBeCompared  = $false
            })
        }
    }

    # 4) Anything present only on destination?
    $srcNames = [System.Collections.Generic.HashSet[string]]::new([StringComparer]::$cmp)
    $src | ForEach-Object { [void]$srcNames.Add($_.Name) }
    foreach ($d in $dst) {
        if (-not $srcNames.Contains($d.Name)) {
            $results.Add([pscustomobject]@{
                Name                    = $d.Name
                SourceIdentity          = $null
                DestinationIdentity     = $d.Identity
                Status                  = 'MissingOnSource'
                CanPasswordsBeCompared  = $false
            })
        }
    }

    $results
}
