function Set-DbaDbSnapshotIsolation {
    <#
    .SYNOPSIS
        Enables or disables ALLOW_SNAPSHOT_ISOLATION on a database.

    .DESCRIPTION
        This function uses Invoke-DbaQuery to run ALTER DATABASE statements
        for one or more databases.

    .PARAMETER SqlInstance
        The target SQL Server instance.

    .PARAMETER Database
        One or more database names.

    .PARAMETER Enable
        Switch to enable snapshot isolation. If omitted, snapshot isolation will be disabled.

    .EXAMPLE
        Set-DbaDbSnapshotIsolation -SqlInstance "MyServer\Instance" -Database "MyDb" -Enable

    .EXAMPLE
        Set-DbaDbSnapshotIsolation -SqlInstance "MyServer\Instance" -Database "MyDb"
    #>
    [CmdletBinding(SupportsShouldProcess)]
    param(
        [Parameter(Mandatory)]
        [string]$SqlInstance,

        [Parameter(Mandatory)]
        [string[]]$Database,

        [switch]$Enable
    )

    foreach ($db in $Database) {
        $action = if ($Enable) { "ON" } else { "OFF" }
        $query = "ALTER DATABASE [$db] SET ALLOW_SNAPSHOT_ISOLATION $action;"

        if ($PSCmdlet.ShouldProcess($db, "Set ALLOW_SNAPSHOT_ISOLATION $action")) {
            Invoke-DbaQuery -SqlInstance $SqlInstance -Database master -Query $query
        }
    }
}
