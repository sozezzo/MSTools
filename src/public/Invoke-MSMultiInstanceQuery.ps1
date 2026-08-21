function Invoke-MSMultiInstanceQuery {
    <#
    .SYNOPSIS
        Executes the same SQL query against multiple SQL Server instances and optionally all databases.

    .DESCRIPTION
        Executes a query against one or more SQL Server instances.

        Execution engine:
        - Uses Invoke-Sqlcmd if available.
        - Falls back to Invoke-DbaQuery if Invoke-Sqlcmd is not available.

        Supports:
        - One or many SQL instances.
        - One database or all online databases.
        - Optional system database inclusion.
        - Optional database exclusions.
        - Optional SQL message capture.
        - Echoing SQL PRINT / RAISERROR informational messages.

    .PARAMETER SqlInstance
        One or more SQL Server instances.

    .PARAMETER Database
        Database name to connect to. Defaults to master.
        Ignored when -AllDatabases is used.

    .PARAMETER AllDatabases
        Executes the query against all online databases on each instance.

    .PARAMETER IncludeSystemDatabases
        Includes master, model, msdb, and tempdb when -AllDatabases is used.

    .PARAMETER ExcludeDatabase
        One or more databases to exclude when -AllDatabases is used.

    .PARAMETER Query
        T-SQL query to execute.

    .PARAMETER InputFile
        SQL script file to execute instead of Query.

    .PARAMETER Credential
        Optional credential.

    .PARAMETER QueryTimeout
        Query timeout in seconds. Default: 0.

    .PARAMETER ConnectionTimeout
        Connection timeout in seconds. Used only if supported by the selected command.

    .PARAMETER TrustServerCertificate
        Uses TrustServerCertificate when supported by the selected command.

    .PARAMETER IncludeMessages
        Returns SQL informational messages as output rows.

    .PARAMETER EchoMessages
        Writes SQL informational messages to the host.
        This automatically enables message capture.

    .PARAMETER EnableException
        Throws errors instead of returning error objects.

    .EXAMPLE
        Invoke-MSMultiInstanceQuery `
            -SqlInstance 'SQL01','SQL02' `
            -AllDatabases `
            -Query 'SELECT DB_NAME() AS DatabaseName, COUNT(*) AS TableCount FROM sys.tables'

    .EXAMPLE
        Invoke-MSMultiInstanceQuery `
            -SqlInstance 'SQL01' `
            -AllDatabases `
            -EchoMessages `
            -Query "SELECT DB_NAME() AS db; PRINT DB_NAME(); PRINT '=======================';"
    #>

    [CmdletBinding(DefaultParameterSetName = 'Query')]
    param (
        [Parameter(Mandatory)]
        [Alias('ServerInstance')]
        [string[]]$SqlInstance,

        [Parameter()]
        [string]$Database = 'master',

        [Parameter()]
        [switch]$AllDatabases,

        [Parameter()]
        [switch]$IncludeSystemDatabases,

        [Parameter()]
        [string[]]$ExcludeDatabase,

        [Parameter(Mandatory, ParameterSetName = 'Query')]
        [string]$Query,

        [Parameter(Mandatory, ParameterSetName = 'InputFile')]
        [ValidateScript({
            if (-not (Test-Path $_)) {
                throw "Input file does not exist: $_"
            }

            $true
        })]
        [string]$InputFile,

        [Parameter()]
        [System.Management.Automation.PSCredential]$Credential,

        [Parameter()]
        [int]$QueryTimeout = 0,

        [Parameter()]
        [int]$ConnectionTimeout = 15,

        [Parameter()]
        [switch]$TrustServerCertificate,

        [Parameter()]
        [switch]$IncludeMessages,

        [Parameter()]
        [switch]$EchoMessages,

        [Parameter()]
        [switch]$EnableException
    )

    begin {
        $hasInvokeSqlcmd = [bool](Get-Command Invoke-Sqlcmd -ErrorAction SilentlyContinue)
        $hasInvokeDbaQuery = [bool](Get-Command Invoke-DbaQuery -ErrorAction SilentlyContinue)

        if (-not $hasInvokeSqlcmd -and -not $hasInvokeDbaQuery) {
            throw "Neither Invoke-Sqlcmd nor Invoke-DbaQuery was found. Install the SqlServer module or dbatools."
        }

        if ($hasInvokeSqlcmd) {
            $script:MSQueryEngine = 'Invoke-Sqlcmd'
        }
        else {
            $script:MSQueryEngine = 'Invoke-DbaQuery'
        }

        # EchoMessages must request SQL messages even if IncludeMessages was not specified.
        $script:MSWantMessages = [bool]($IncludeMessages -or $EchoMessages)

        Write-Verbose "Using query engine: $script:MSQueryEngine"

        $executionId = [guid]::NewGuid()

        function Get-MSMessageText {
            param (
                [Parameter()]
                [object]$InputObject
            )

            if ($null -eq $InputObject) {
                return $null
            }

            # dbatools -MessagesToOutput may return PRINT/RAISERROR messages as plain strings.
            if ($InputObject -is [string]) {
                if (-not [string]::IsNullOrWhiteSpace($InputObject)) {
                    return $InputObject
                }

                return $null
            }

            if ($InputObject -is [System.Management.Automation.VerboseRecord]) {
                if (-not [string]::IsNullOrWhiteSpace($InputObject.Message)) {
                    return $InputObject.Message
                }

                return $null
            }

            $propertyNames = @($InputObject.PSObject.Properties.Name)

            foreach ($propertyName in @('Message', 'Text', 'MessageText')) {
                if ($propertyNames -contains $propertyName) {
                    $value = $InputObject.$propertyName

                    if (-not [string]::IsNullOrWhiteSpace([string]$value)) {
                        return [string]$value
                    }
                }
            }

            if ($propertyNames -contains 'Errors') {
                try {
                    if ($InputObject.Errors.Count -gt 0) {
                        $errorMessages = $InputObject.Errors | ForEach-Object { $_.Message }
                        return ($errorMessages -join [Environment]::NewLine)
                    }
                }
                catch {
                    return $null
                }
            }

            return $null
        }

        function Convert-MSDataRowToHashtable {
            param (
                [Parameter(Mandatory)]
                [object]$Row
            )

            $result = [ordered]@{}

            if ($Row -is [System.Data.DataRow]) {
                foreach ($column in $Row.Table.Columns) {
                    $result[$column.ColumnName] = $Row[$column.ColumnName]
                }

                return $result
            }

            foreach ($property in $Row.PSObject.Properties) {
                # Avoid polluting the result with DataRow technical properties.
                if ($property.Name -in @(
                    'RowError',
                    'RowState',
                    'Table',
                    'ItemArray',
                    'HasErrors'
                )) {
                    continue
                }

                $result[$property.Name] = $property.Value
            }

            return $result
        }

        function Invoke-MSInternalQuery {
            param (
                [Parameter(Mandatory)]
                [string]$Instance,

                [Parameter(Mandatory)]
                [string]$DatabaseName,

                [Parameter()]
                [string]$SqlQuery,

                [Parameter()]
                [string]$SqlInputFile
            )

            if ($script:MSQueryEngine -eq 'Invoke-Sqlcmd') {
                $command = Get-Command Invoke-Sqlcmd

                $params = @{
                    ServerInstance = $Instance
                    Database       = $DatabaseName
                    ErrorAction    = 'Stop'
                }

                if ($command.Parameters.ContainsKey('QueryTimeout')) {
                    $params.QueryTimeout = $QueryTimeout
                }

                if ($command.Parameters.ContainsKey('ConnectionTimeout')) {
                    $params.ConnectionTimeout = $ConnectionTimeout
                }

                if ($SqlQuery) {
                    $params.Query = $SqlQuery
                }
                elseif ($SqlInputFile) {
                    if ($command.Parameters.ContainsKey('InputFile')) {
                        $params.InputFile = $SqlInputFile
                    }
                    else {
                        throw "Invoke-Sqlcmd does not support -InputFile in this installed version."
                    }
                }
                else {
                    throw "Either SqlQuery or SqlInputFile must be provided."
                }

                if ($Credential) {
                    if ($command.Parameters.ContainsKey('Credential')) {
                        $params.Credential = $Credential
                    }
                    else {
                        throw "Invoke-Sqlcmd does not support -Credential in this installed version."
                    }
                }

                if ($TrustServerCertificate -and $command.Parameters.ContainsKey('TrustServerCertificate')) {
                    $params.TrustServerCertificate = $true
                }

                if ($script:MSWantMessages) {
                    # PRINT output from Invoke-Sqlcmd is normally emitted through the verbose stream.
                    return & { Invoke-Sqlcmd @params -Verbose } 4>&1
                }

                return Invoke-Sqlcmd @params
            }

            if ($script:MSQueryEngine -eq 'Invoke-DbaQuery') {
                $command = Get-Command Invoke-DbaQuery

                $params = @{
                    SqlInstance = $Instance
                    Database    = $DatabaseName
                    ErrorAction = 'Stop'
                }

                if ($SqlQuery) {
                    $params.Query = $SqlQuery
                }
                elseif ($SqlInputFile) {
                    if ($command.Parameters.ContainsKey('File')) {
                        $params.File = $SqlInputFile
                    }
                    elseif ($command.Parameters.ContainsKey('InputFile')) {
                        $params.InputFile = $SqlInputFile
                    }
                    else {
                        throw "Invoke-DbaQuery does not support -File or -InputFile in this installed version."
                    }
                }
                else {
                    throw "Either SqlQuery or SqlInputFile must be provided."
                }

                if ($Credential) {
                    if ($command.Parameters.ContainsKey('SqlCredential')) {
                        $params.SqlCredential = $Credential
                    }
                    elseif ($command.Parameters.ContainsKey('Credential')) {
                        $params.Credential = $Credential
                    }
                    else {
                        throw "Invoke-DbaQuery does not support -SqlCredential or -Credential in this installed version."
                    }
                }

                if ($QueryTimeout -gt 0 -and $command.Parameters.ContainsKey('QueryTimeout')) {
                    $params.QueryTimeout = $QueryTimeout
                }

                if ($ConnectionTimeout -gt 0 -and $command.Parameters.ContainsKey('ConnectionTimeout')) {
                    $params.ConnectionTimeout = $ConnectionTimeout
                }

                if ($TrustServerCertificate -and $command.Parameters.ContainsKey('TrustServerCertificate')) {
                    $params.TrustServerCertificate = $true
                }

                if ($script:MSWantMessages) {
                    if ($command.Parameters.ContainsKey('MessagesToOutput')) {
                        $params.MessagesToOutput = $true
                    }
                    else {
                        Write-Warning "Invoke-DbaQuery does not support -MessagesToOutput in this installed version. SQL PRINT messages cannot be captured."
                    }
                }

                return Invoke-DbaQuery @params
            }

            throw "Unknown query engine: $script:MSQueryEngine"
        }

        function Get-TargetDatabases {
            param (
                [Parameter(Mandatory)]
                [string]$Instance
            )

            if (-not $AllDatabases) {
                return @($Database)
            }

            $databaseQuery = @"
SELECT name
FROM sys.databases
WHERE state_desc = 'ONLINE'
  AND source_database_id IS NULL
  AND is_distributor = 0
ORDER BY name;
"@

            try {
                $rawDatabaseRows = Invoke-MSInternalQuery `
                    -Instance $Instance `
                    -DatabaseName 'master' `
                    -SqlQuery $databaseQuery

                $databases = foreach ($row in @($rawDatabaseRows)) {
                    if ($null -ne (Get-MSMessageText -InputObject $row)) {
                        continue
                    }

                    if ($row -is [System.Data.DataRow]) {
                        $row['name']
                    }
                    elseif ($row.PSObject.Properties.Name -contains 'name') {
                        $row.name
                    }
                }

                if (-not $IncludeSystemDatabases) {
                    $systemDatabases = @('master', 'model', 'msdb', 'tempdb')
                    $databases = $databases | Where-Object { $_ -notin $systemDatabases }
                }

                if ($ExcludeDatabase) {
                    $databases = $databases | Where-Object { $_ -notin $ExcludeDatabase }
                }

                return @($databases)
            }
            catch {
                if ($EnableException) {
                    throw
                }

                [pscustomobject]@{
                    _ExecutionId  = $executionId
                    _QueryEngine  = $script:MSQueryEngine
                    _SqlInstance  = $Instance
                    _Database     = 'master'
                    _Status       = 'Failed'
                    _RowType      = 'DatabaseDiscoveryError'
                    _StartTime    = Get-Date
                    _EndTime      = Get-Date
                    _DurationMs   = 0
                    _ErrorMessage = $_.Exception.Message
                }

                return @()
            }
        }
    }

    process {
        foreach ($instance in $SqlInstance) {
            Write-Verbose "Preparing target databases for [$instance]."

            $targetDatabases = Get-TargetDatabases -Instance $instance

            foreach ($targetDatabase in $targetDatabases) {
                $startTime = Get-Date

                Write-Verbose "Executing query on [$instance], database [$targetDatabase] using [$script:MSQueryEngine]."

                try {
                    if ($PSCmdlet.ParameterSetName -eq 'Query') {
                        $rawRows = Invoke-MSInternalQuery `
                            -Instance $instance `
                            -DatabaseName $targetDatabase `
                            -SqlQuery $Query
                    }
                    else {
                        $rawRows = Invoke-MSInternalQuery `
                            -Instance $instance `
                            -DatabaseName $targetDatabase `
                            -SqlInputFile $InputFile
                    }

                    $endTime = Get-Date
                    $durationMs = [math]::Round(($endTime - $startTime).TotalMilliseconds, 0)

                    $hasDataRows = $false
                    $hasMessageRows = $false

                    foreach ($rawRow in @($rawRows)) {
                        $messageText = Get-MSMessageText -InputObject $rawRow

                        if ($messageText) {
                            $hasMessageRows = $true

                            if ($EchoMessages) {
                                Write-Host "[$instance][$targetDatabase] $messageText"
                            }

                            if ($IncludeMessages) {
                                [pscustomobject]@{
                                    _ExecutionId  = $executionId
                                    _QueryEngine  = $script:MSQueryEngine
                                    _SqlInstance  = $instance
                                    _Database     = $targetDatabase
                                    _Status       = 'Success'
                                    _RowType      = 'Message'
                                    _StartTime    = $startTime
                                    _EndTime      = $endTime
                                    _DurationMs   = $durationMs
                                    _ErrorMessage = $null
                                    MessageText   = $messageText
                                }
                            }

                            continue
                        }

                        if ($null -eq $rawRow) {
                            continue
                        }

                        $hasDataRows = $true

                        $rowValues = Convert-MSDataRowToHashtable -Row $rawRow

                        $output = [ordered]@{
                            _ExecutionId  = $executionId
                            _QueryEngine  = $script:MSQueryEngine
                            _SqlInstance  = $instance
                            _Database     = $targetDatabase
                            _Status       = 'Success'
                            _RowType      = 'Data'
                            _StartTime    = $startTime
                            _EndTime      = $endTime
                            _DurationMs   = $durationMs
                            _ErrorMessage = $null
                        }

                        foreach ($key in $rowValues.Keys) {
                            if ($output.Contains($key)) {
                                $output["Query_$key"] = $rowValues[$key]
                            }
                            else {
                                $output[$key] = $rowValues[$key]
                            }
                        }

                        [pscustomobject]$output
                    }

                    if (-not $hasDataRows -and -not $hasMessageRows) {
                        [pscustomobject]@{
                            _ExecutionId  = $executionId
                            _QueryEngine  = $script:MSQueryEngine
                            _SqlInstance  = $instance
                            _Database     = $targetDatabase
                            _Status       = 'Success'
                            _RowType      = 'NoRows'
                            _StartTime    = $startTime
                            _EndTime      = $endTime
                            _DurationMs   = $durationMs
                            _ErrorMessage = $null
                        }
                    }
                }
                catch {
                    $endTime = Get-Date
                    $durationMs = [math]::Round(($endTime - $startTime).TotalMilliseconds, 0)

                    if ($EnableException) {
                        throw
                    }

                    [pscustomobject]@{
                        _ExecutionId  = $executionId
                        _QueryEngine  = $script:MSQueryEngine
                        _SqlInstance  = $instance
                        _Database     = $targetDatabase
                        _Status       = 'Failed'
                        _RowType      = 'Error'
                        _StartTime    = $startTime
                        _EndTime      = $endTime
                        _DurationMs   = $durationMs
                        _ErrorMessage = $_.Exception.Message
                    }
                }
            }
        }
    }
}
