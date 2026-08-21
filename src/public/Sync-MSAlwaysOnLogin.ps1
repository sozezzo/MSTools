function Sync-MSAlwaysOnLogin {
    <#
    .SYNOPSIS
        Compares and synchronizes SQL Server logins between AlwaysOn replicas.

    .DESCRIPTION
        Resolves the replicas of one AlwaysOn Availability Group and compares the
        primary replica with every secondary replica. By default, the primary is
        the source. Use -SourceSecondary to use one secondary as the source
        and synchronize the primary from it.

        The comparison checks:
            - Login name and type
            - SID
            - Enabled/disabled and access status
            - Default database and language
            - SQL password hash and password-policy settings
            - Credential association
            - Server-role memberships
            - Explicit server-level permissions

        System-generated logins are excluded. Database users and database-level
        permissions are not compared because they are stored inside an Availability
        Group database and move with that database.

        Synchronization modes:
            Show   Reports differences without changing anything.
            Copy   Copies logins missing on the destination. It never modifies a
                   login that already exists (a differing login is skipped).
            Delete Deletes logins found only on the destination.
            Full   Performs Copy and Delete, and with -Force also replaces logins
                   that already exist on the destination but differ.

        The function supports -WhatIf and -Confirm. Inner dbatools confirmation is
        suppressed only after this function's ShouldProcess confirmation succeeds.

        WARNING: Replacing a login uses Copy-DbaLogin -Force. dbatools drops and
        recreates the destination login and can transfer database or SQL Agent job
        ownership to sa. Review the Show results before using Copy -Force or Full
        -Force.

    .PARAMETER SqlInstance
        Listener name or any replica that belongs to the Availability Group.

    .PARAMETER AvailabilityGroup
        Availability Group name. Required when SqlInstance hosts more than one
        Availability Group.

    .PARAMETER Login
        Optional exact list of login names to compare and synchronize.

    .PARAMETER Synchronize
        Show, Copy, Delete, or Full. The default is Show.

    .PARAMETER Force
        Allows Full to drop and recreate an existing destination login when its
        properties differ from the source, and suppresses the per-login
        confirmation prompt (unless you pass an explicit -Confirm). It does not
        terminate active connections.

    .PARAMETER KillActiveConnection
        Allows dbatools to terminate active sessions when replacing or deleting a
        login. Use only during an approved maintenance window.

    .PARAMETER AllowEmptySource
        Allows an unfiltered Delete or Full operation when the source contains
        no eligible user logins. Without this switch, the function stops instead of
        treating every destination login as an extra login.

    .PARAMETER SourceSecondary
        Uses a secondary replica as the source and the primary as the destination.

    .PARAMETER SourceSecondaryReplica
        Name of the secondary replica to use with -SourceSecondary. It is required
        when the Availability Group contains more than one secondary.

    .PARAMETER Report
        Opens the generated HTML report in the default handler (usually a browser).
        The function always returns the report in the ReportHtml property; this
        switch only forces the report to be displayed and records the saved file
        path in the ReportPath property.

    .PARAMETER SqlCredential
        Optional credential used for every replica connection.

    .EXAMPLE
        Sync-MSAlwaysOnLogin -SqlInstance 'AGLISTENER'

        Reports login differences without changing anything.

    .EXAMPLE
        Sync-MSAlwaysOnLogin -SqlInstance 'AGLISTENER' -AvailabilityGroup 'AG01' `
            -Synchronize Copy -WhatIf

        Previews the missing-login copies. Existing logins that differ are reported
        and skipped (Copy never replaces them).

    .EXAMPLE
        Sync-MSAlwaysOnLogin -SqlInstance 'AGLISTENER' -AvailabilityGroup 'AG01' `
            -Synchronize Full -Force -Confirm:$false

        Copies missing logins, deletes destination-only logins, and replaces
        different destination logins without an interactive prompt. Existing
        sessions are not terminated.

    .EXAMPLE
        Sync-MSAlwaysOnLogin -SqlInstance 'SQLNODE2' -AvailabilityGroup 'AG01' `
            -SourceSecondary -SourceSecondaryReplica 'SQLNODE2' -Synchronize Full `
            -Force -Report

        Uses SQLNODE2 as the source, synchronizes the primary, and returns an
        HTML report together with the structured results.
    #>

    [CmdletBinding(SupportsShouldProcess = $true, ConfirmImpact = 'High')]
    param(
        [Parameter(Mandatory)]
        [ValidateNotNullOrEmpty()]
        [string]$SqlInstance,

        [ValidateNotNullOrEmpty()]
        [string]$AvailabilityGroup,

        [string[]]$Login,

        [Alias('Synchonize', 'Synchronise', 'Sync')]
        [ValidateSet('Show', 'Copy', 'Delete', 'Full')]
        [string]$Synchronize = 'Show',

        [switch]$Force,
        [switch]$KillActiveConnection,
        [Alias('AllowEmptyReference')]
        [switch]$AllowEmptySource,
        [Alias('RefSecondary')]
        [switch]$SourceSecondary,

        [Alias('ReferenceSecondary')]
        [ValidateNotNullOrEmpty()]
        [string]$SourceSecondaryReplica,

        [switch]$Report,

        [System.Management.Automation.PSCredential]$SqlCredential
    )

    # -Force suppresses the confirmation prompt (an explicit -Confirm still wins).
    if ($Force -and -not $PSBoundParameters.ContainsKey('Confirm')) {
        $ConfirmPreference = 'None'
    }

    function Write-MSLogSafe {
        param(
            [Parameter(Mandatory)]
            [string]$Message,

            [ValidateSet('Info', 'Warning', 'Error', 'Debug')]
            [string]$Level = 'Info'
        )

        $command = Get-Command -Name Write-MSLog -ErrorAction SilentlyContinue
        if ($command) {
            try {
                Write-MSLog -Message $Message -Level $Level | Out-Null
                return
            }
            catch {
                Write-Verbose $Message
                return
            }
        }

        Write-Verbose $Message
    }

    function Get-PropertyValue {
        param(
            [AllowNull()]
            [object]$InputObject,

            [Parameter(Mandatory)]
            [string]$Name
        )

        if ($null -eq $InputObject) {
            return $null
        }

        $property = $InputObject.PSObject.Properties[$Name]
        if ($null -eq $property) {
            return $null
        }

        return $property.Value
    }

    function ConvertTo-ComparableValue {
        param(
            [AllowNull()]
            [object]$Value
        )

        if ($null -eq $Value) {
            return '<NULL>'
        }

        if ($Value -is [byte[]]) {
            return ([System.BitConverter]::ToString($Value)).Replace('-', '')
        }

        if ($Value -is [bool]) {
            return $Value.ToString().ToUpperInvariant()
        }

        if ($Value -is [System.Array] -and -not ($Value -is [string])) {
            $items = @(
                foreach ($item in $Value) {
                    ConvertTo-ComparableValue -Value $item
                }
            )
            return (($items | Sort-Object -Unique) -join '|')
        }

        return ([string]$Value).Trim()
    }

    function Test-IsSystemLogin {
        param(
            [Parameter(Mandatory)]
            [object]$LoginObject
        )

        $name = [string](Get-PropertyValue -InputObject $LoginObject -Name 'Name')
        if ([string]::IsNullOrWhiteSpace($name)) {
            return $true
        }

        $principalId = Get-PropertyValue -InputObject $LoginObject -Name 'ID'
        if ($null -eq $principalId) {
            $principalId = Get-PropertyValue -InputObject $LoginObject -Name 'PrincipalId'
        }

        if ($null -ne $principalId -and [int]$principalId -eq 1) {
            return $true
        }

        $isSystemObject = Get-PropertyValue -InputObject $LoginObject -Name 'IsSystemObject'
        if ($isSystemObject -eq $true) {
            return $true
        }

        $trimmedName = $name.Trim()
        if ($trimmedName -ieq 'sa')                 { return $true }
        if ($trimmedName -match '^##')              { return $true }
        if ($trimmedName -match '^NT\s+SERVICE\\')   { return $true }
        if ($trimmedName -match '^NT\s+AUTHORITY\\') { return $true }
        if ($trimmedName -match '^BUILTIN\\')        { return $true }

        return $false
    }

    function Get-LoginInventory {
        param(
            [Parameter(Mandatory)]
            [string]$Server,

            [string[]]$Names
        )

        $loginParameters = @{
            SqlInstance     = $Server
            Detailed        = $true
            EnableException = $true
        }

        if ($SqlCredential) {
            $loginParameters.SqlCredential = $SqlCredential
        }

        if ($Names -and $Names.Count -gt 0) {
            $loginParameters.Login = $Names
        }

        $loginRows = @(
            Get-DbaLogin @loginParameters |
                Where-Object { -not (Test-IsSystemLogin -LoginObject $_) }
        )

        if (-not $loginRows -or $loginRows.Count -eq 0) {
            return @()
        }

        $inventoryNames = @($loginRows | ForEach-Object { [string]$_.Name })

        $roleParameters = @{
            SqlInstance     = $Server
            Login           = $inventoryNames
            EnableException = $true
        }

        if ($SqlCredential) {
            $roleParameters.SqlCredential = $SqlCredential
        }

        $roleRows = @(Get-DbaServerRoleMember @roleParameters)
        $rolesByLogin = @{}

        foreach ($roleRow in $roleRows) {
            $memberName = [string](Get-PropertyValue -InputObject $roleRow -Name 'Name')
            $roleName = [string](Get-PropertyValue -InputObject $roleRow -Name 'Role')

            if ([string]::IsNullOrWhiteSpace($memberName) -or
                [string]::IsNullOrWhiteSpace($roleName)) {
                continue
            }

            $rolesByLogin[$memberName] = @($rolesByLogin[$memberName]) + $roleName
        }

        $permissionParameters = @{
            SqlInstance        = $Server
            Database           = 'master'
            IncludeServerLevel = $true
            EnableException    = $true
        }

        if ($SqlCredential) {
            $permissionParameters.SqlCredential = $SqlCredential
        }

        $permissionRows = @(
            Get-DbaPermission @permissionParameters |
                Where-Object {
                    [string]::IsNullOrWhiteSpace([string]$_.Database) -and
                    $_.Grantee -in $inventoryNames
                }
        )

        $permissionsByLogin = @{}

        foreach ($permissionRow in $permissionRows) {
            $grantee = [string](Get-PropertyValue -InputObject $permissionRow -Name 'Grantee')
            if ([string]::IsNullOrWhiteSpace($grantee)) {
                continue
            }

            $securableType = ConvertTo-ComparableValue (
                Get-PropertyValue $permissionRow 'SecurableType'
            )
            $securable = ConvertTo-ComparableValue (
                Get-PropertyValue $permissionRow 'Securable'
            )

            # The SERVER securable is returned as the local instance name. Normalize
            # it so identical permissions on different replicas compare correctly.
            if ($securableType -ieq 'SERVER') {
                $securable = '<SERVER>'
            }

            $permissionSignature = '{0}|{1}|{2}|{3}' -f
                (ConvertTo-ComparableValue (Get-PropertyValue $permissionRow 'PermState')),
                (ConvertTo-ComparableValue (Get-PropertyValue $permissionRow 'PermissionName')),
                $securableType,
                $securable

            $permissionsByLogin[$grantee] = @($permissionsByLogin[$grantee]) + $permissionSignature
        }

        $inventory = @()

        foreach ($loginRow in $loginRows) {
            $name = [string](Get-PropertyValue -InputObject $loginRow -Name 'Name')

            $sid = Get-PropertyValue -InputObject $loginRow -Name 'SidString'
            if ([string]::IsNullOrWhiteSpace([string]$sid)) {
                $sid = Get-PropertyValue -InputObject $loginRow -Name 'Sid'
            }

            $passwordHash = Get-PropertyValue -InputObject $loginRow -Name 'PasswordHash'

            $inventory += [pscustomobject][ordered]@{
                Name                      = $name
                LoginType                 = ConvertTo-ComparableValue (Get-PropertyValue $loginRow 'LoginType')
                Sid                       = ConvertTo-ComparableValue $sid
                IsDisabled                = ConvertTo-ComparableValue (Get-PropertyValue $loginRow 'IsDisabled')
                HasAccess                 = ConvertTo-ComparableValue (Get-PropertyValue $loginRow 'HasAccess')
                DefaultDatabase           = ConvertTo-ComparableValue (Get-PropertyValue $loginRow 'DefaultDatabase')
                Language                  = ConvertTo-ComparableValue (Get-PropertyValue $loginRow 'Language')
                PasswordPolicyEnforced    = ConvertTo-ComparableValue (Get-PropertyValue $loginRow 'PasswordPolicyEnforced')
                PasswordExpirationEnabled = ConvertTo-ComparableValue (Get-PropertyValue $loginRow 'PasswordExpirationEnabled')
                MustChangePassword        = ConvertTo-ComparableValue (Get-PropertyValue $loginRow 'MustChangePassword')
                Credential                = ConvertTo-ComparableValue (Get-PropertyValue $loginRow 'Credential')
                PasswordHash              = ConvertTo-ComparableValue $passwordHash
                ServerRoles               = ConvertTo-ComparableValue @($rolesByLogin[$name] | Sort-Object -Unique)
                ServerPermissions         = ConvertTo-ComparableValue @($permissionsByLogin[$name] | Sort-Object -Unique)
            }
        }

        return $inventory
    }

    function ConvertTo-LoginMap {
        param(
            [object[]]$Inventory
        )

        $map = @{}
        foreach ($item in @($Inventory)) {
            if (-not [string]::IsNullOrWhiteSpace([string]$item.Name)) {
                $map[[string]$item.Name] = $item
            }
        }

        return $map
    }

    function Compare-LoginSnapshot {
        param(
            [Parameter(Mandatory)]
            [object]$SourceLogin,

            [Parameter(Mandatory)]
            [object]$DestinationLogin
        )

        $properties = @(
            'LoginType',
            'Sid',
            'IsDisabled',
            'HasAccess',
            'DefaultDatabase',
            'Language',
            'PasswordPolicyEnforced',
            'PasswordExpirationEnabled',
            'MustChangePassword',
            'Credential',
            'PasswordHash',
            'ServerRoles',
            'ServerPermissions'
        )

        $differences = @()

        foreach ($propertyName in $properties) {
            $sourceValue = ConvertTo-ComparableValue (
                Get-PropertyValue -InputObject $SourceLogin -Name $propertyName
            )
            $destinationValue = ConvertTo-ComparableValue (
                Get-PropertyValue -InputObject $DestinationLogin -Name $propertyName
            )

            if ($sourceValue -ine $destinationValue) {
                $differences += $propertyName
            }
        }

        return $differences
    }

    function Get-LoginPropertyComparison {
        param(
            [Parameter(Mandatory)]
            [object]$Source,

            [Parameter(Mandatory)]
            [object]$Destination
        )

        $properties = @(
            'LoginType',
            'Sid',
            'IsDisabled',
            'HasAccess',
            'DefaultDatabase',
            'Language',
            'PasswordPolicyEnforced',
            'PasswordExpirationEnabled',
            'MustChangePassword',
            'Credential',
            'PasswordHash',
            'ServerRoles',
            'ServerPermissions'
        )

        foreach ($propertyName in $properties) {
            $sourceValue = [string](Get-PropertyValue -InputObject $Source -Name $propertyName)
            $destinationValue = [string](Get-PropertyValue -InputObject $Destination -Name $propertyName)

            [pscustomobject]@{
                Property    = $propertyName
                Source      = $sourceValue
                Destination = $destinationValue
                Differs     = ($sourceValue -ine $destinationValue)
            }
        }
    }

    function Get-DbaResultText {
        param(
            [object[]]$Rows
        )

        $messages = @()
        foreach ($row in @($Rows)) {
            $status = [string](Get-PropertyValue -InputObject $row -Name 'Status')
            $notes = [string](Get-PropertyValue -InputObject $row -Name 'Notes')

            if (-not [string]::IsNullOrWhiteSpace($status)) {
                if (-not [string]::IsNullOrWhiteSpace($notes)) {
                    $messages += "$status : $notes"
                }
                else {
                    $messages += $status
                }
            }
        }

        return (($messages | Select-Object -Unique) -join '; ')
    }

    function Test-CopyResult {
        param([object[]]$Rows)

        return @(
            $Rows | Where-Object {
                [string](Get-PropertyValue -InputObject $_ -Name 'Status') -ieq 'Successful'
            }
        ).Count -gt 0
    }

    function Test-RemoveResult {
        param([object[]]$Rows)

        return @(
            $Rows | Where-Object {
                [string](Get-PropertyValue -InputObject $_ -Name 'Status') -ieq 'Dropped'
            }
        ).Count -gt 0
    }

    function ConvertTo-HtmlEncodedText {
        param([AllowNull()][object]$Value)

        return [System.Net.WebUtility]::HtmlEncode([string]$Value)
    }

    function Get-LoginReportHtml {
        param(
            [object[]]$Rows,
            [string]$GroupName,
            [string]$SourceServer,
            [string[]]$DestinationServers,
            [string]$PrimaryServer,
            [string[]]$SecondaryServers
        )

        $head = @"
<style>
body { font-family: Arial, sans-serif; font-size: 12px; color: #222; }
table { border-collapse: collapse; width: 100%; margin-top: 12px; }
th, td { border: 1px solid #cfcfcf; padding: 6px 8px; text-align: left; vertical-align: top; }
th { background: #ececec; }
tr:nth-child(even) { background: #fafafa; }
tr.diff td { background: #fde0e0; }
.summary { margin: 4px 0; }
</style>
"@

        $groupText = ConvertTo-HtmlEncodedText $GroupName
        $primaryText = ConvertTo-HtmlEncodedText $PrimaryServer
        $secondaryText = ConvertTo-HtmlEncodedText ($SecondaryServers -join ', ')
        $sourceText = ConvertTo-HtmlEncodedText $SourceServer
        $destinationText = ConvertTo-HtmlEncodedText ($DestinationServers -join ', ')

        $body = @(
            '<h2>AlwaysOn Login Synchronization</h2>',
            "<p class='summary'><strong>Availability Group:</strong> $groupText</p>",
            "<p class='summary'><strong>Primary server:</strong> $primaryText</p>",
            "<p class='summary'><strong>Secondary server(s):</strong> $secondaryText</p>",
            "<p class='summary'><strong>Source:</strong> $sourceText</p>",
            "<p class='summary'><strong>Destinations:</strong> $destinationText</p>"
        )

        if (-not $Rows -or $Rows.Count -eq 0) {
            $body += '<p>No differences were found.</p>'
        }
        else {
            $body += $Rows |
                Select-Object Timestamp, SourceServer, DestinationServer, LoginName,
                    Status, Differences, Action, Succeeded, Detail |
                ConvertTo-Html -Fragment -As Table
        }

        # Side-by-side property comparison for each differing login that carries it
        # (populated when a specific -Login is requested).
        $comparisonRows = @($Rows | Where-Object { $_.PSObject.Properties['Comparison'] -and $_.Comparison })
        foreach ($comparisonRow in $comparisonRows) {
            $loginText = ConvertTo-HtmlEncodedText $comparisonRow.LoginName
            $sourceHead = ConvertTo-HtmlEncodedText $comparisonRow.SourceServer
            $destinationHead = ConvertTo-HtmlEncodedText $comparisonRow.DestinationServer

            $detailRows = foreach ($item in @($comparisonRow.Comparison)) {
                $rowClass = if ($item.Differs) { " class='diff'" } else { '' }
                '<tr{0}><td>{1}</td><td>{2}</td><td>{3}</td></tr>' -f
                    $rowClass,
                    (ConvertTo-HtmlEncodedText $item.Property),
                    (ConvertTo-HtmlEncodedText $item.Source),
                    (ConvertTo-HtmlEncodedText $item.Destination)
            }

            $body += "<h3>Difference detail for login [$loginText]</h3>"
            $body += "<table><tr><th>Property</th><th>Source ($sourceHead)</th><th>Destination ($destinationHead)</th></tr>$(($detailRows) -join '')</table>"
        }

        return ConvertTo-Html -Title 'AlwaysOn Login Synchronization' -Head $head -Body $body
    }

    $requiredCommands = @(
        'Get-DbaAgReplica',
        'Get-DbaLogin',
        'Get-DbaServerRoleMember',
        'Get-DbaPermission'
    )

    if ($Synchronize -in @('Copy', 'Full')) {
        $requiredCommands += 'Copy-DbaLogin'
    }

    if ($Synchronize -in @('Delete', 'Full')) {
        $requiredCommands += 'Remove-DbaLogin'
    }

    $missingCommands = @(
        $requiredCommands |
            Select-Object -Unique |
            Where-Object { -not (Get-Command -Name $_ -ErrorAction SilentlyContinue) }
    )

    if ($missingCommands.Count -gt 0) {
        throw "Required dbatools commands were not found: $($missingCommands -join ', ')."
    }

    if ($SourceSecondaryReplica -and -not $SourceSecondary) {
        throw '-SourceSecondaryReplica requires -SourceSecondary.'
    }

    if ($KillActiveConnection -and $Synchronize -eq 'Show') {
        Write-Warning '-KillActiveConnection has no effect when -Synchronize is Show.'
    }

    $replicaParameters = @{
        SqlInstance     = $SqlInstance
        EnableException = $true
    }

    if ($SqlCredential) {
        $replicaParameters.SqlCredential = $SqlCredential
    }

    if ($AvailabilityGroup) {
        $replicaParameters.AvailabilityGroup = $AvailabilityGroup
    }

    $replicas = @(Get-DbaAgReplica @replicaParameters)
    if (-not $replicas -or $replicas.Count -eq 0) {
        throw "No AlwaysOn replicas were found through [$SqlInstance]."
    }

    $groupNames = @(
        $replicas |
            ForEach-Object { [string]$_.AvailabilityGroup } |
            Where-Object { -not [string]::IsNullOrWhiteSpace($_) } |
            Sort-Object -Unique
    )

    if (-not $AvailabilityGroup -and $groupNames.Count -gt 1) {
        throw "[$SqlInstance] returned multiple Availability Groups: $($groupNames -join ', '). Specify -AvailabilityGroup."
    }

    if ($AvailabilityGroup) {
        $selectedGroup = $AvailabilityGroup
        $replicas = @(
            $replicas | Where-Object { [string]$_.AvailabilityGroup -ieq $selectedGroup }
        )
    }
    else {
        $selectedGroup = $groupNames | Select-Object -First 1
    }

    if (-not $replicas -or $replicas.Count -eq 0) {
        throw "Availability Group [$selectedGroup] was not found through [$SqlInstance]."
    }

    $primaryServers = @(
        $replicas |
            Where-Object { [string]$_.Role -ieq 'Primary' } |
            ForEach-Object { [string]$_.Name } |
            Where-Object { -not [string]::IsNullOrWhiteSpace($_) } |
            Sort-Object -Unique
    )

    if ($primaryServers.Count -ne 1) {
        throw "Expected one primary replica for [$selectedGroup], but found [$($primaryServers.Count)]."
    }

    $primaryServer = $primaryServers[0]

    $secondaryServers = @(
        $replicas |
            Where-Object { [string]$_.Role -ieq 'Secondary' } |
            ForEach-Object { [string]$_.Name } |
            Where-Object { -not [string]::IsNullOrWhiteSpace($_) } |
            Sort-Object -Unique
    )

    if ($secondaryServers.Count -eq 0) {
        throw "No secondary replicas were found for [$selectedGroup]."
    }

    if ($SourceSecondary) {
        if ($SourceSecondaryReplica) {
            $matchedSecondary = @(
                $secondaryServers | Where-Object { $_ -ieq $SourceSecondaryReplica }
            )

            if ($matchedSecondary.Count -ne 1) {
                throw (
                    "Source secondary [$SourceSecondaryReplica] is not a secondary replica " +
                    "of [$selectedGroup]. Available secondaries: $($secondaryServers -join ', ')."
                )
            }

            $sourceServer = $matchedSecondary[0]
        }
        elseif ($secondaryServers.Count -eq 1) {
            $sourceServer = $secondaryServers[0]
        }
        else {
            throw (
                "[$selectedGroup] has multiple secondary replicas. Specify " +
                "-SourceSecondaryReplica. Available secondaries: $($secondaryServers -join ', ')."
            )
        }

        $destinationServers = @($primaryServer)
    }
    else {
        $sourceServer = $primaryServer
        $destinationServers = @($secondaryServers)
    }

    Write-MSLogSafe -Message (
        "Sync-MSAlwaysOnLogin: START. AG=[$selectedGroup], Source=[$sourceServer], " +
        "Destinations=[$($destinationServers -join ', ')], Synchronize=[$Synchronize], Force=[$Force], " +
        "KillActiveConnection=[$KillActiveConnection]"
    )

    $sourceInventory = @(
        Get-LoginInventory -Server $sourceServer -Names $Login
    )

    if ($sourceInventory.Count -eq 0 -and
        (-not $Login -or $Login.Count -eq 0) -and
        $Synchronize -in @('Delete', 'Full') -and
        -not $AllowEmptySource) {
        throw (
            "The source [$sourceServer] contains no eligible user logins. " +
            'Use -AllowEmptySource only if deleting every eligible destination login is intentional.'
        )
    }

    $sourceMap = ConvertTo-LoginMap -Inventory $sourceInventory

    $results = @()

    foreach ($destinationServer in $destinationServers) {
        try {
            $destinationInventory = @(Get-LoginInventory -Server $destinationServer -Names $Login)
        }
        catch {
            $results += [pscustomobject][ordered]@{
                Timestamp         = Get-Date
                AvailabilityGroup = $selectedGroup
                SourceServer      = $sourceServer
                DestinationServer = $destinationServer
                LoginName         = $null
                Status            = 'DestinationInventoryFailed'
                Differences       = 'UnableToCompare'
                Action            = 'Failed'
                Succeeded         = $false
                Detail            = $_.Exception.Message
            }

            Write-MSLogSafe -Message (
                "Unable to inventory destination [$destinationServer]: $($_.Exception.Message)"
            ) -Level Error
            continue
        }

        $destinationMap = ConvertTo-LoginMap -Inventory $destinationInventory

        $allNames = @(
            @($sourceMap.Keys) + @($destinationMap.Keys) |
                Sort-Object -Unique
        )

        $destinationResults = @()
        $changeAttempted = $false

        foreach ($name in $allNames) {
            $inSource = $sourceMap.ContainsKey($name)
            $inDestination = $destinationMap.ContainsKey($name)

            if ($inSource -and $inDestination) {
                $differenceProperties = @(
                    Compare-LoginSnapshot `
                        -SourceLogin $sourceMap[$name] `
                        -DestinationLogin $destinationMap[$name]
                )

                if ($differenceProperties.Count -eq 0) {
                    continue
                }

                $row = [pscustomobject][ordered]@{
                    Timestamp       = Get-Date
                    AvailabilityGroup = $selectedGroup
                    SourceServer      = $sourceServer
                    DestinationServer = $destinationServer
                    LoginName       = $name
                    Status          = 'Different'
                    Differences     = $differenceProperties -join ', '
                    Action          = 'NoAction'
                    Succeeded       = $null
                    Detail          = 'The login exists on both replicas but its properties differ.'
                }

                # When a specific login is requested, keep the full side-by-side
                # property comparison for the report's detail table.
                if ($Login -and $Login.Count -gt 0) {
                    $row | Add-Member -NotePropertyName 'Comparison' -NotePropertyValue (
                        @(Get-LoginPropertyComparison -Source $sourceMap[$name] -Destination $destinationMap[$name])
                    )
                }

                if ($Synchronize -eq 'Show') {
                    $row.Action = 'Show'
                }
                elseif ($Synchronize -eq 'Copy') {
                    $row.Action = 'Skipped'
                    $row.Detail = 'Copy only copies logins missing on the destination; an existing login that differs is skipped. Use Full -Force to replace it.'
                }
                elseif ($Synchronize -eq 'Delete') {
                    $row.Action = 'Skipped'
                    $row.Detail = 'Delete mode does not remove a login that exists on the source.'
                }
                elseif (-not $Force) {
                    $row.Action = 'RequiresForce'
                    $row.Detail = 'Use -Force with Full to replace this different destination login.'
                }
                elseif ($PSCmdlet.ShouldProcess(
                    "[$destinationServer].[$name]",
                    "Drop and recreate the login from source [$sourceServer]"
                )) {
                    $changeAttempted = $true

                    try {
                        $copyParameters = @{
                            Source          = $sourceServer
                            Destination     = $destinationServer
                            Login           = $name
                            Force           = $true
                            EnableException = $true
                            Confirm         = $false
                        }

                        if ($KillActiveConnection) {
                            $copyParameters.KillActiveConnection = $true
                        }

                        if ($SqlCredential) {
                            $copyParameters.SourceSqlCredential = $SqlCredential
                            $copyParameters.DestinationSqlCredential = $SqlCredential
                        }

                        $copyResult = @(Copy-DbaLogin @copyParameters)

                        if (Test-CopyResult -Rows $copyResult) {
                            $row.Action = 'Replaced'
                            $row.Succeeded = $true
                            $row.Detail = 'dbatools reported success; final verification is pending.'
                        }
                        else {
                            $row.Action = 'ReplaceFailed'
                            $row.Succeeded = $false
                            $row.Detail = "dbatools did not report a successful replacement. $(Get-DbaResultText $copyResult)"
                        }
                    }
                    catch {
                        $row.Action = 'ReplaceFailed'
                        $row.Succeeded = $false
                        $row.Detail = $_.Exception.Message
                    }
                }
                else {
                    $row.Action = if ($WhatIfPreference) { 'WhatIf' } else { 'Declined' }
                    $row.Detail = 'The replacement was not executed.'
                }

                $destinationResults += $row
                continue
            }

            if ($inSource) {
                $row = [pscustomobject][ordered]@{
                    Timestamp       = Get-Date
                    AvailabilityGroup = $selectedGroup
                    SourceServer      = $sourceServer
                    DestinationServer = $destinationServer
                    LoginName       = $name
                    Status          = 'OnlyOnSource'
                    Differences     = 'MissingOnDestination'
                    Action          = 'NoAction'
                    Succeeded       = $null
                    Detail          = "The login exists on [$sourceServer] and is missing on [$destinationServer]."
                }

                if ($Synchronize -eq 'Show') {
                    $row.Action = 'Show'
                }
                elseif ($Synchronize -eq 'Delete') {
                    $row.Action = 'Skipped'
                    $row.Detail = 'Delete mode cannot remove a login that is already missing on the destination.'
                }
                elseif ($PSCmdlet.ShouldProcess(
                    "[$destinationServer].[$name]",
                    "Copy the missing login from source [$sourceServer]"
                )) {
                    $changeAttempted = $true

                    try {
                        $copyParameters = @{
                            Source          = $sourceServer
                            Destination     = $destinationServer
                            Login           = $name
                            EnableException = $true
                            Confirm         = $false
                        }

                        if ($SqlCredential) {
                            $copyParameters.SourceSqlCredential = $SqlCredential
                            $copyParameters.DestinationSqlCredential = $SqlCredential
                        }

                        $copyResult = @(Copy-DbaLogin @copyParameters)

                        if (Test-CopyResult -Rows $copyResult) {
                            $row.Action = 'Copied'
                            $row.Succeeded = $true
                            $row.Detail = 'dbatools reported success; final verification is pending.'
                        }
                        else {
                            $row.Action = 'CopyFailed'
                            $row.Succeeded = $false
                            $row.Detail = "dbatools did not report a successful copy. $(Get-DbaResultText $copyResult)"
                        }
                    }
                    catch {
                        $row.Action = 'CopyFailed'
                        $row.Succeeded = $false
                        $row.Detail = $_.Exception.Message
                    }
                }
                else {
                    $row.Action = if ($WhatIfPreference) { 'WhatIf' } else { 'Declined' }
                    $row.Detail = 'The copy was not executed.'
                }

                $destinationResults += $row
                continue
            }

            if ($inDestination) {
                $row = [pscustomobject][ordered]@{
                    Timestamp       = Get-Date
                    AvailabilityGroup = $selectedGroup
                    SourceServer      = $sourceServer
                    DestinationServer = $destinationServer
                    LoginName       = $name
                    Status          = 'OnlyOnDestination'
                    Differences     = 'MissingOnSource'
                    Action          = 'NoAction'
                    Succeeded       = $null
                    Detail          = "The login exists on [$destinationServer] and is missing on [$sourceServer]."
                }

                if ($Synchronize -eq 'Show') {
                    $row.Action = 'Show'
                }
                elseif ($Synchronize -eq 'Copy') {
                    $row.Action = 'Skipped'
                    $row.Detail = 'Copy mode does not remove destination-only logins.'
                }
                elseif ($PSCmdlet.ShouldProcess(
                    "[$destinationServer].[$name]",
                    "Delete the destination-only login"
                )) {
                    $changeAttempted = $true

                    try {
                        $removeParameters = @{
                            SqlInstance    = $destinationServer
                            Login          = $name
                            EnableException = $true
                            Confirm        = $false
                        }

                        if ($KillActiveConnection) {
                            $removeParameters.Force = $true
                        }

                        if ($SqlCredential) {
                            $removeParameters.SqlCredential = $SqlCredential
                        }

                        $removeResult = @(Remove-DbaLogin @removeParameters)

                        if (Test-RemoveResult -Rows $removeResult) {
                            $row.Action = 'Deleted'
                            $row.Succeeded = $true
                            $row.Detail = 'dbatools reported success; final verification is pending.'
                        }
                        else {
                            $row.Action = 'DeleteFailed'
                            $row.Succeeded = $false
                            $row.Detail = "dbatools did not report a successful deletion. $(Get-DbaResultText $removeResult)"
                        }
                    }
                    catch {
                        $row.Action = 'DeleteFailed'
                        $row.Succeeded = $false
                        $row.Detail = $_.Exception.Message
                    }
                }
                else {
                    $row.Action = if ($WhatIfPreference) { 'WhatIf' } else { 'Declined' }
                    $row.Detail = 'The deletion was not executed.'
                }

                $destinationResults += $row
            }
        }

        if ($changeAttempted) {
            try {
                $finalInventory = @(Get-LoginInventory -Server $destinationServer -Names $Login)
                $finalMap = ConvertTo-LoginMap -Inventory $finalInventory

                foreach ($resultRow in $destinationResults) {
                    if ($resultRow.Succeeded -ne $true) {
                        continue
                    }

                    if ($resultRow.Action -in @('Copied', 'Replaced')) {
                        if (-not $finalMap.ContainsKey($resultRow.LoginName)) {
                            $resultRow.Action = 'VerificationFailed'
                            $resultRow.Succeeded = $false
                            $resultRow.Detail = 'The login was not found on the destination after the copy operation.'
                            continue
                        }

                        $remainingDifferences = @(
                            Compare-LoginSnapshot `
                                -SourceLogin $sourceMap[$resultRow.LoginName] `
                                -DestinationLogin $finalMap[$resultRow.LoginName]
                        )

                        if ($remainingDifferences.Count -gt 0) {
                            $resultRow.Action = 'VerificationFailed'
                            $resultRow.Succeeded = $false
                            $resultRow.Differences = $remainingDifferences -join ', '
                            $resultRow.Detail = 'The login exists after the copy, but differences remain.'
                        }
                        else {
                            $resultRow.Detail = 'The login was verified against the source after the copy.'
                        }
                    }
                    elseif ($resultRow.Action -eq 'Deleted') {
                        if ($finalMap.ContainsKey($resultRow.LoginName)) {
                            $resultRow.Action = 'VerificationFailed'
                            $resultRow.Succeeded = $false
                            $resultRow.Detail = 'The login still exists on the destination after the delete operation.'
                        }
                        else {
                            $resultRow.Detail = 'The login deletion was verified on the destination.'
                        }
                    }
                }
            }
            catch {
                $verificationError = $_.Exception.Message

                foreach ($resultRow in $destinationResults) {
                    if ($resultRow.Succeeded -eq $true) {
                        $resultRow.Action = 'VerificationFailed'
                        $resultRow.Succeeded = $false
                        $resultRow.Detail = "Unable to verify the destination after synchronization: $verificationError"
                    }
                }
            }
        }

        $results += $destinationResults
    }

    $rows = @($results)

    Write-MSLogSafe -Message (
        "Sync-MSAlwaysOnLogin: END. Differences=[$($rows.Count)], " +
        "Copied=[$(@($rows | Where-Object Action -eq 'Copied').Count)], " +
        "Replaced=[$(@($rows | Where-Object Action -eq 'Replaced').Count)], " +
        "Deleted=[$(@($rows | Where-Object Action -eq 'Deleted').Count)], " +
        "Failed=[$(@($rows | Where-Object Succeeded -eq $false).Count)]"
    )

    $reportHtml = Get-LoginReportHtml `
        -Rows $rows `
        -GroupName $selectedGroup `
        -SourceServer $sourceServer `
        -DestinationServers $destinationServers `
        -PrimaryServer $primaryServer `
        -SecondaryServers $secondaryServers

    $reportPath = $null

    if ($Report) {
        $fileName = 'MSAlwaysOnLoginSync_{0:yyyyMMdd_HHmmss}_{1}.html' -f (Get-Date), ([guid]::NewGuid().ToString('N'))
        $reportPath = Join-Path -Path ([System.IO.Path]::GetTempPath()) -ChildPath $fileName

        try {
            Set-Content -LiteralPath $reportPath -Value $reportHtml -Encoding UTF8
            Write-MSLogSafe -Message "Sync-MSAlwaysOnLogin: report saved to [$reportPath]."
            Invoke-Item -LiteralPath $reportPath
        }
        catch {
            Write-MSLogSafe -Message (
                "Unable to display the report [$reportPath]: $($_.Exception.Message)"
            ) -Level Warning
        }
    }

    return [pscustomobject][ordered]@{
        AvailabilityGroup = $selectedGroup
        PrimaryServer     = $primaryServer
        SecondaryServers  = $secondaryServers
        SourceServer      = $sourceServer
        DestinationServers = $destinationServers
        Synchronize       = $Synchronize
        Differences       = $rows
        DifferenceCount   = $rows.Count
        CopiedCount       = @($rows | Where-Object Action -eq 'Copied').Count
        ReplacedCount     = @($rows | Where-Object Action -eq 'Replaced').Count
        DeletedCount      = @($rows | Where-Object Action -eq 'Deleted').Count
        FailedCount       = @($rows | Where-Object Succeeded -eq $false).Count
        ReportHtml        = $reportHtml
        ReportPath        = $reportPath
    }
}
