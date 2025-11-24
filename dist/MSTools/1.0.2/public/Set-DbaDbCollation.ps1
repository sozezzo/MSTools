function Set-DbaDbCollation {
    <#
    .SYNOPSIS
        Changes the default collation of one or more SQL Server databases.

    .DESCRIPTION
        Sets the database-level default collation using:
            ALTER DATABASE [db] COLLATE <CollationName>

        Notes:
          - Does NOT change existing column collations (only DB default).
          - May need exclusive access; function will retry under SINGLE_USER.

    .PARAMETER SqlInstance
        One or more targets (string or DbaInstanceParameter).

    .PARAMETER SqlCredential
        PSCredential (SQL or AD). Omit for integrated auth.

    .PARAMETER Database
        One or more DB names (strings, wildcards, or Get-DbaDatabase objects).

    .PARAMETER Collation
        Destination collation (e.g. Latin1_General_100_CI_AS_SC).

    .EXAMPLE
        Set-DbaDbCollation -SqlInstance SQL01 -Database MyDb -Collation Latin1_General_CI_AS

    .NOTES
        Compatible with Windows PowerShell 5.1 and PowerShell 7+.
        Auto-detects if dbatools cmdlets support -EnableException.
    #>
    [CmdletBinding(SupportsShouldProcess, ConfirmImpact='High')]
    param(
        [Parameter(Mandatory, Position=0)]
        [DbaInstanceParameter[]] $SqlInstance,

        [Parameter()]
        [PSCredential] $SqlCredential,

        [Parameter(Mandatory)]
        [Object[]] $Database,

        [Parameter(Mandatory)]
        [ValidateNotNullOrEmpty()]
        [string] $Collation
    )

    begin {
        # Utility: check if a command has a given parameter
        function __Has-Param {
            param([string]$Command,[string]$Param)
            try { return (Get-Command -Name $Command -ErrorAction Stop).Parameters.ContainsKey($Param) }
            catch { return $false }
        }

        $HasEnable_Connect = __Has-Param 'Connect-DbaInstance' 'EnableException'
        $HasEnable_Invoke  = __Has-Param 'Invoke-DbaQuery'      'EnableException'
        $HasEnable_GetDb   = __Has-Param 'Get-DbaDatabase'      'EnableException'

        function __Connect {
            param($Instance, $Cred)
            $splat = @{
                SqlInstance = $Instance
                ErrorAction = 'Stop'
            }
            if ($Cred) { $splat.SqlCredential = $Cred }
            if ($HasEnable_Connect) { $splat.EnableException = $true }
            Connect-DbaInstance @splat
        }

        function __InvokeQ {
            param($Server,[string]$Query,[int]$Timeout = 0)
            $splat = @{
                SqlInstance  = $Server
                Query        = $Query
                ErrorAction  = 'Stop'
            }
            if ($Timeout -gt 0) { $splat.QueryTimeout = $Timeout }
            if ($HasEnable_Invoke) { $splat.EnableException = $true }
            Invoke-DbaQuery @splat
        }

        function __GetDb {
            param($Server,[string]$Name)
            $splat = @{
                SqlInstance = $Server
                Database    = $Name
                ErrorAction = 'Stop'
            }
            if ($HasEnable_GetDb) { $splat.EnableException = $true }
            Get-DbaDatabase @splat
        }
    }

    process {
        foreach ($instance in $SqlInstance) {
            # Connect
            try {
                $server = __Connect -Instance $instance -Cred $SqlCredential
            } catch {
                Write-Error "Failed to connect to [$instance]: $($_.Exception.Message)"
                continue
            }

            # Validate collation exists on this instance
            try {
                $collEsc = $Collation.Replace("'", "''")
                $supported = __InvokeQ $server "SELECT name FROM sys.fn_helpcollations() WHERE name = N'$collEsc';"
                if (-not $supported -or -not $supported.name) {
                    Write-Error "Collation '$Collation' is not recognized by instance [$($server.DomainInstanceName)]."
                    continue
                }
            } catch {
                Write-Error "Could not validate collation on [$($server.DomainInstanceName)]: $($_.Exception.Message)"
                continue
            }

            # Resolve DBs
            $dbs = @()
            foreach ($item in $Database) {
                if ($item -is [Microsoft.SqlServer.Management.Smo.Database]) {
                    $dbs += $item
                    continue
                }
                elseif ($item -is [string]) {
                    try { $dbs += __GetDb $server $item } catch { Write-Error $_; continue }
                }
                else {
                    # PS 5.1-safe: avoid ?. operator
                    if ($item.PSObject.Properties.Match('Name')) {
                        $name = $item.PSObject.Properties['Name'].Value
                        if ($null -ne $name) {
                            try { $dbs += __GetDb $server $name } catch { Write-Error $_; continue }
                        }
                    }
                }
            }
            if (-not $dbs) {
                Write-Verbose "No databases matched on [$($server.DomainInstanceName)] for input '$Database'."
                continue
            }

            # Apply
            foreach ($db in $dbs) {
                $old = $db.Collation
                $target = $Collation
                $insName  = $server.DomainInstanceName
                $nameSafe = $db.Name.Replace(']', ']]')

                if ($old -eq $target) {
                    [PSCustomObject]@{
                        SqlInstance  = $insName
                        Database     = $db.Name
                        OldCollation = $old
                        NewCollation = $target
                        Status       = 'Skipped'
                        Notes        = 'Already at desired collation'
                    }
                    continue
                }

                $what = "Set collation of [$insName].[$($db.Name)] from '$old' to '$target'"
                if ($PSCmdlet.ShouldProcess($what, 'ALTER DATABASE COLLATE')) {
                    $alterStmt = "ALTER DATABASE [$nameSafe] COLLATE $target;"
                    $applied = $false
                    $notes   = ''

                    # Try direct alter
                    try {
                        __InvokeQ $server $alterStmt
                        $applied = $true
                        $notes   = 'Altered without forcing SINGLE_USER'
                    } catch {
                        $err = $_.Exception.Message
                        $needsSingle = ($err -match 'exclusively locked' -or
                                        $err -match 'being accessed by other users' -or
                                        $err -match '\b5030\b' -or
                                        $err -match 'multi-user')
                        if ($needsSingle) {
                            try {
                                __InvokeQ $server "ALTER DATABASE [$nameSafe] SET SINGLE_USER WITH ROLLBACK IMMEDIATE;"
                                __InvokeQ $server $alterStmt
                                __InvokeQ $server "ALTER DATABASE [$nameSafe] SET MULTI_USER;"
                                $applied = $true
                                $notes   = 'Connections killed; changed under SINGLE_USER then restored MULTI_USER'
                            } catch {
                                try { __InvokeQ $server "ALTER DATABASE [$nameSafe] SET MULTI_USER;" } catch {}
                                Write-Error "Failed to change collation on [$insName].[$($db.Name)]: $($_.Exception.Message)"
                            }
                        }
                        else {
                            Write-Error "Failed to change collation on [$insName].[$($db.Name)]: $err"
                        }
                    }

                    try { $db.Refresh() } catch {}
                    [PSCustomObject]@{
                        SqlInstance  = $insName
                        Database     = $db.Name
                        OldCollation = $old
                        NewCollation = if ($applied) { $target } else { $db.Collation }
                        Status       = if ($applied) { 'Changed' } else { 'Failed' }
                        Notes        = if ($applied) { $notes } else { 'See errors above' }
                    }
                }
            }
        }
    }
}
