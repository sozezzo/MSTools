function Set-DbaDbServiceBroker {
<#
.SYNOPSIS
    Enables, disables, or resets (NEW_BROKER) the Service Broker state for one or more SQL Server databases.

.DESCRIPTION
    dbatools currently lacks a native Set-DbaDbServiceBroker cmdlet. This function wraps the T-SQL
    ALTER DATABASE ... SET { ENABLE_BROKER | DISABLE_BROKER | NEW_BROKER } using dbatools helpers.
    It supports multiple databases (explicit names, wildcards, or -AllUser), exclusions, WITH ROLLBACK IMMEDIATE,
    WhatIf/Confirm, and returns pre/post state.

.PARAMETER SqlInstance
    Target SQL Server instance (anything accepted by DbaInstanceParameter).

.PARAMETER SqlCredential
    PSCredential to connect (SQL or Windows). Passed to dbatools commands.

.PARAMETER Database
    One or more database names. Wildcards supported. You can also pipe plain strings to this parameter.

.PARAMETER AllUser
    Operate on all user databases (excludes master, model, msdb, tempdb).

.PARAMETER ExcludeDatabase
    One or more names (supports wildcards) to exclude.

.PARAMETER EnableBroker
    Sets ENABLE_BROKER.

.PARAMETER DisableBroker
    Sets DISABLE_BROKER.

.PARAMETER NewBroker
    Sets NEW_BROKER (generates a new service_broker_guid).

.PARAMETER WithRollbackImmediate
    Appends WITH ROLLBACK IMMEDIATE to the ALTER DATABASE statement.

.PARAMETER Timeout
    Query timeout (seconds) for Invoke-DbaQuery. Default 0 (provider default).

.PARAMETER PassThru
    If set, writes results to pipeline. (Note: this function always writes results.)

.EXAMPLE
    Set-DbaDbServiceBroker -SqlInstance SQL01 -Database MyDb -EnableBroker -WithRollbackImmediate

.EXAMPLE
    Set-DbaDbServiceBroker -SqlInstance SQL01 -AllUser -ExcludeDatabase "ReportServer%" -DisableBroker -WhatIf

.EXAMPLE
    "DbA","DbB" | Set-DbaDbServiceBroker -SqlInstance SQL01 -NewBroker -WithRollbackImmediate -Confirm

.NOTES
    Author: Sozezzo
    Requires: dbatools (Invoke-DbaQuery, Get-DbaDatabase)
    Caution: NEW_BROKER invalidates existing dialogs; plan drains accordingly.
#>
    [CmdletBinding(SupportsShouldProcess = $true, ConfirmImpact = 'High')]
    param(
        [Parameter(Mandatory)]
        [DbaInstanceParameter] $SqlInstance,

        [PSCredential] $SqlCredential,

        [Parameter(ValueFromPipeline, ValueFromPipelineByPropertyName)]
        [string[]] $Database,

        [switch] $AllUser,
        [string[]] $ExcludeDatabase,

        [switch] $EnableBroker,
        [switch] $DisableBroker,
        [switch] $NewBroker,

        [switch] $WithRollbackImmediate,

        [int] $Timeout = 0,

        [switch] $PassThru
    )

    begin {
        # Validate mutually exclusive action switches
        $actions = @()
        if ($EnableBroker)  { $actions += 'ENABLE_BROKER' }
        if ($DisableBroker) { $actions += 'DISABLE_BROKER' }
        if ($NewBroker)     { $actions += 'NEW_BROKER' }

        if ($actions.Count -eq 0) {
            throw "You must specify one of: -EnableBroker, -DisableBroker, or -NewBroker."
        }
        if ($actions.Count -gt 1) {
            throw "Specify only one action: -EnableBroker OR -DisableBroker OR -NewBroker."
        }

        # Helper: expand DB list using Get-DbaDatabase with wildcards/exclusions
        function Resolve-DbList {
            param(
                [DbaInstanceParameter] $Instance,
                [PSCredential] $Cred,
                [string[]] $Names,
                [switch] $AllUser,
                [string[]] $Exclude
            )

            # Get baseline set (all user DBs or the union of wildcards)
            if ($AllUser -or -not $Names) {
                $all = Get-DbaDatabase -SqlInstance $Instance -SqlCredential $Cred -ExcludeSystem
                if ($Names) {
                    $dbs = foreach ($n in $Names) { $all | Where-Object { $_.Name -like $n } }
                    $dbs = $dbs | Select-Object -Unique
                } else {
                    $dbs = $all
                }
            } else {
                $dbs = foreach ($n in $Names) {
                    Get-DbaDatabase -SqlInstance $Instance -SqlCredential $Cred -Database $n -EnableException:$false
                }
                $dbs = $dbs | Where-Object { $_ } | Select-Object -Unique
            }

            # Apply exclusions
            if ($Exclude) {
                foreach ($ex in $Exclude) {
                    $dbs = $dbs | Where-Object { $_.Name -notlike $ex }
                }
            }

            if (-not $dbs) {
                throw "No target databases resolved. Check -Database/-AllUser and -ExcludeDatabase filters."
            }

            return $dbs
        }

        $action = $actions[0]  # exactly one
        $withClause = if ($WithRollbackImmediate) { " WITH ROLLBACK IMMEDIATE" } else { "" }

        $prePostSql = @"
SELECT name,
       is_broker_enabled,
       service_broker_guid
FROM sys.databases
WHERE name = @db;
"@

        $results = New-Object System.Collections.Generic.List[object]
        $targetsBuffer = @()
    }

    process {
        # Accumulate pipeline inputs (so exclusions/wildcards apply consistently once)
        if ($Database) { $targetsBuffer += $Database }
    }

    end {
        try {
            $targets = Resolve-DbList -Instance $SqlInstance -Cred $SqlCredential -Names $targetsBuffer -AllUser:$AllUser -Exclude $ExcludeDatabase
        } catch {
            throw $_
        }

        foreach ($t in $targets) {
            $dbName = $t.Name

            # Pre-state
            $pre = Invoke-DbaQuery -SqlInstance $SqlInstance -SqlCredential $SqlCredential -Database master -Query $prePostSql -SqlParameter @{ db = $dbName } -QueryTimeout $Timeout -EnableException:$false
            $preEnabled = $null
            $preGuid    = $null
            if ($pre -and $pre.Count -gt 0) {
                $preEnabled = [bool]$pre[0].is_broker_enabled
                $preGuid    = [string]$pre[0].service_broker_guid
            }

            # Build ALTER DATABASE and ShouldProcess message (PS5.1-safe)
            $alter  = "ALTER DATABASE [$dbName] SET $action$withClause;"
            $should = "$action on [$dbName]$withClause"

            if ($PSCmdlet.ShouldProcess("$($SqlInstance) / $dbName", $should)) {
                $err = $null
                try {
                    Invoke-DbaQuery -SqlInstance $SqlInstance -SqlCredential $SqlCredential -Database master -Query $alter -QueryTimeout $Timeout -EnableException
                } catch {
                    $err = $_
                }

                # Post-state
                $post = Invoke-DbaQuery -SqlInstance $SqlInstance -SqlCredential $SqlCredential -Database master -Query $prePostSql -SqlParameter @{ db = $dbName } -QueryTimeout $Timeout -EnableException:$false
                $postEnabled = $null
                $postGuid    = $null
                if ($post -and $post.Count -gt 0) {
                    $postEnabled = [bool]$post[0].is_broker_enabled
                    $postGuid    = [string]$post[0].service_broker_guid
                }

                $status = if ($err) { "Failed" } else { "Success" }
                $note   = if ($err) { ($err.Exception.Message -replace '\s+', ' ') } else { "" }

                $results.Add([pscustomobject]@{
                    SqlInstance            = [string]$SqlInstance
                    Database               = $dbName
                    Action                 = $action
                    WithRollbackImmediate  = [bool]$WithRollbackImmediate
                    Pre_IsEnabled          = $preEnabled
                    Pre_Guid               = $preGuid
                    Post_IsEnabled         = $postEnabled
                    Post_Guid              = $postGuid
                    Status                 = $status
                    Notes                  = $note
                })
            }
        }

        # Always emit results (dbatools style usually uses -PassThru; we emit for convenience)
        $results
    }
}
