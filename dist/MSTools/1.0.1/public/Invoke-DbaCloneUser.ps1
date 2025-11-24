function Invoke-DbaCloneUser {
<#
.SYNOPSIS
    Clones database users from a source database to a destination database, with include/exclude filters
    and safety skips for system users. Supports -WhatIf/-Confirm and optional summary output.

.DESCRIPTION
    Invoke-DbaCloneUser enumerates users from the source database and attempts to create corresponding
    users in the destination database using New-DbaDbUser. It skips known system users (dbo, guest,
    INFORMATION_SCHEMA, sys, ##MS_*), respects -IncludeUser / -ExcludeUser filters, and avoids creating
    users that already exist in the destination. If a source user is mapped to a login, that login is
    passed through to New-DbaDbUser (assuming the login already exists on the destination instance).
    Default schema is preserved when set.

    Key behaviors:
      - Skips system users and user patterns (##MS_*).
      - Skips users not in -IncludeUser (if provided) and those in -ExcludeUser.
      - Skips users that already exist on destination (by name).
      - Preserves Login and DefaultSchema when available.
      - Supports -WhatIf/-Confirm via SupportsShouldProcess.
      - Writes progress and summary via Write-Log.
      - Use -PassThru to receive a structured result (Created/Skipped/Failed + per-user details).

.PARAMETER SourceInstance
    SQL Server instance hosting the source database (e.g., 'SQL01' or 'SQL01\INST1').

.PARAMETER SourceDatabase
    Name of the source database to read users from.

.PARAMETER DestInstance
    SQL Server instance hosting the destination database.

.PARAMETER DestDatabase
    Name of the destination database where users will be created.

.PARAMETER IncludeUser
    Optional exact-match list of user names to include (case-insensitive).
    Examples: 'appuser', 'DOMAIN\svc_app'
    If omitted, all non-system users are eligible (subject to -ExcludeUser).

.PARAMETER ExcludeUser
    Optional exact-match list of user names to exclude (case-insensitive).
    Examples: 'guest','dbo'

.PARAMETER LogFileName
    Optional log file path. Verbose streams and Write-Log messages are appended here.

.PARAMETER PassThru
    When specified, returns a [pscustomobject] summary with counts and a per-user detail list.

.INPUTS
    None. All inputs are provided via parameters.

.OUTPUTS
    If -PassThru is provided, outputs a PSCustomObject:
        Source       (string) -> "SourceInstance.SourceDatabase"
        Destination  (string) -> "DestInstance.DestDatabase"
        Created      (int)
        Skipped      (int)
        Failed       (int)
        Details      (IEnumerable<PSCustomObject> with: User, Action, Detail)

    Otherwise, no output on success.

.EXAMPLES
    # 1) Clone all non-system users; assume matching logins already exist on destination
    Invoke-DbaCloneUser `
        -SourceInstance "SQL01" -SourceDatabase "AppDB" `
        -DestInstance   "SQL02" -DestDatabase   "AppDB_Clone" `
        -LogFileName "C:\logs\clone-users.log"

    # 2) Only clone specific users; return a summary object
    Invoke-DbaCloneUser `
        -SourceInstance "SQL01" -SourceDatabase "Sales" `
        -DestInstance   "SQL02" -DestDatabase   "Sales_Stage" `
        -IncludeUser 'report_user','DOMAIN\svc_sales' `
        -PassThru

    # 3) Exclude known accounts and run as a dry run
    Invoke-DbaCloneUser `
        -SourceInstance "Prod\OLTP" -SourceDatabase "HR" `
        -DestInstance   "Stage\OLTP" -DestDatabase   "HR_Test" `
        -ExcludeUser 'guest','dbo','DOMAIN\temp_user' `
        -WhatIf

    # 4) Capture details for auditing
    $res = Invoke-DbaCloneUser `
        -SourceInstance "SQL01" -SourceDatabase "Ops" `
        -DestInstance   "SQL02" -DestDatabase   "Ops_QA" `
        -PassThru
    $res.Details | Format-Table

.NOTES
    System-user detection:
      - Names: 'dbo','guest','INFORMATION_SCHEMA','sys'
      - Patterns: '##MS_*'
      - Additionally respects the SMO IsSystemObject flag.

    Filters:
      - -IncludeUser: exact, case-insensitive match list (if supplied, only these are considered).
      - -ExcludeUser: exact, case-insensitive match list (always excluded).

    Existence checks:
      - Destination users are fetched up-front; name matches are skipped (no change).

    Mappings:
      - If the source user has a Login, it is passed to New-DbaDbUser (login must already exist).
      - DefaultSchema is preserved when present.
      - Contained users (without logins) will be created as users without a login mapping.

    Permissions:
      - Source: metadata read on users.
      - Destination: CREATE USER, ALTER AUTHORIZATION (as applicable), and permission to set default schema.

    Logging:
      - Write-Log is expected to accept: -Message, -Level, -LogFileName, -Verbose.

    Error handling:
      - Per-user creation is wrapped in try/catch; failures are logged and recorded in Details.
      - Fatal errors bubble to the outer catch, are logged, and rethrown.

.DEPENDENCIES
    dbatools:
      Get-DbaDbUser
      New-DbaDbUser

.LINK
    https://dbatools.io/
#>
	
    [CmdletBinding(SupportsShouldProcess)]
    param(
        [Parameter(Mandatory)][string]$SourceInstance,
        [Parameter(Mandatory)][string]$SourceDatabase,
        [Parameter(Mandatory)][string]$DestInstance,
        [Parameter(Mandatory)][string]$DestDatabase,

        # Optional filters
        [string[]]$IncludeUser,     # e.g. 'appuser','DOMAIN\svc_app'
        [string[]]$ExcludeUser,     # e.g. 'guest','dbo'

        # Logging
        [string]$LogFileName,

        # Summary object output
        [switch]$PassThru
    )

    # Helper: system users to skip
    $systemUserNames = @('dbo','guest','INFORMATION_SCHEMA','sys')
    $systemUserPatterns = @('##MS_*')  # e.g. ##MS_PolicyTsqlExecutionLogin##

    function _isSystemUser([string]$name, [bool]$isSystemObject) {
        if ($isSystemObject) { return $true }
        if ($systemUserNames -contains $name) { return $true }
        foreach ($p in $systemUserPatterns) { if ($name -like $p) { return $true } }
        return $false
    }

    function _includeName([string]$name, [string[]]$incl) {
        if (-not $incl -or $incl.Count -eq 0) { return $true }
        foreach ($i in $incl) { if ($name -ieq $i) { return $true } }
        return $false
    }
    function _excludeName([string]$name, [string[]]$excl) {
        if (-not $excl -or $excl.Count -eq 0) { return $false }
        foreach ($e in $excl) { if ($name -ieq $e) { return $true } }
        return $false
    }

    try {
        Write-Log -Message "Copying database users: [$SourceInstance].[$SourceDatabase] → [$DestInstance].[$DestDatabase]" -Level Info -LogFileName $LogFileName -Verbose

        # Source users
        $srcUsers = Get-DbaDbUser -SqlInstance $SourceInstance -Database $SourceDatabase

        # Destination existing users (for skip-if-exists)
        $dstUsers = Get-DbaDbUser -SqlInstance $DestInstance -Database $DestDatabase
        $dstUserNames = @{}
        foreach ($du in $dstUsers) { $dstUserNames[$du.Name.ToLowerInvariant()] = $true }

        $created = 0; $skipped = 0; $failed = 0
        $results = @()

        foreach ($u in $srcUsers) {
            $name = $u.Name

            # Skip system users
            if (_isSystemUser $name $u.IsSystemObject) {
                Write-Log -Message "Skip system user: $name" -Level Info -LogFileName $LogFileName
                $skipped++
                $results += [pscustomobject]@{ User=$name; Action='Skip-System'; Detail='' }
                continue
            }

            # Filters
            if (-not (_includeName $name $IncludeUser)) {
                Write-Log -Message "Skip (not in IncludeUser): $name" -Level Info -LogFileName $LogFileName
                $skipped++
                $results += [pscustomobject]@{ User=$name; Action='Skip-Include'; Detail='' }
                continue
            }
            if (_excludeName $name $ExcludeUser) {
                Write-Log -Message "Skip (in ExcludeUser): $name" -Level Info -LogFileName $LogFileName
                $skipped++
                $results += [pscustomobject]@{ User=$name; Action='Skip-Exclude'; Detail='' }
                continue
            }

            # Already exists on destination?
            if ($dstUserNames.ContainsKey($name.ToLowerInvariant())) {
                Write-Log -Message "Skip (already exists on destination): $name" -Level Info -LogFileName $LogFileName
                $skipped++
                $results += [pscustomobject]@{ User=$name; Action='Skip-Exists'; Detail='' }
                continue
            }

            # Build New-DbaDbUser params
            $params = @{
                SqlInstance     = $DestInstance
                Database        = $DestDatabase
                Username        = $u.Name
                Confirm         = $false
                EnableException = $true
            }

            # If a Login is associated, use it (typical case).
            # You said logins already exist on the new instance.
            if ($u.Login) { $params.Login = $u.Login }

            # Preserve DefaultSchema if set
            if ($u.DefaultSchema) { $params.DefaultSchema = $u.DefaultSchema }

            try {
                if ($PSCmdlet.ShouldProcess("[$DestInstance].[$DestDatabase]", "Create user '$($u.Name)' (Login='$($u.Login)'; DefaultSchema='$($u.DefaultSchema)')")) {
                    New-DbaDbUser @params *>> $LogFileName
                    Write-Log -Message "Created user: $name (Login='$($u.Login)'; DefaultSchema='$($u.DefaultSchema)')" -Level Important -LogFileName $LogFileName
                    $created++
                    $results += [pscustomobject]@{ User=$name; Action='Created'; Detail=("Login=$($u.Login); DefaultSchema=$($u.DefaultSchema)") }
                }
            }
            catch {
                $failed++
                $msg = $_.Exception.Message
                Write-Log -Message "FAILED to create user: $name — $msg" -Level Error -LogFileName $LogFileName
                $results += [pscustomobject]@{ User=$name; Action='Failed'; Detail=$msg }
                continue
            }
        }

        Write-Log -Message ("Summary: Created={0}  Skipped={1}  Failed={2}" -f $created,$skipped,$failed) -Level Alert -LogFileName $LogFileName -Verbose

        if ($PassThru) {
            [pscustomobject]@{
                Source        = "$SourceInstance.$SourceDatabase"
                Destination   = "$DestInstance.$DestDatabase"
                Created       = $created
                Skipped       = $skipped
                Failed        = $failed
                Details       = $results
            }
        }
    }
    catch {
        Write-Log -Message ("ERROR (Invoke-DbaCloneUser): " + $_.Exception.Message) -Level Error -LogFileName $LogFileName
        throw
    }
}
