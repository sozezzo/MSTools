function New-MSLinkedServerLogin {
    <#
    .SYNOPSIS
        Creates/configures a SQL Login intended for Linked Server access.

    .DESCRIPTION
        Creates a SQL Authentication login and grants access to one or more
        databases using a shared, level-based database role.

        Levels:

            Read
                SELECT

            Read/Update
                SELECT
                INSERT
                UPDATE
                DELETE

            Read/Update/Execute
                SELECT
                INSERT
                UPDATE
                DELETE
                EXECUTE

        If -Database is omitted or empty, all ONLINE, READ_WRITE user databases
        are configured. System databases and database snapshots are excluded.

        The database role is SHARED per permission level (one role per level,
        reused by every login of that level) - it is NOT tied to the login.

        Role names:

            role_LinkedServer_Read
            role_LinkedServer_ReadUpdate
            role_LinkedServer_ReadUpdateExecute

        -Force:
            - Recreates the SQL Login (kills its sessions first).
            - Preserves the original LOGIN SID so existing database users
              do not become orphaned.

        The shared level role is never dropped. Regardless of -Force, the login
        is removed from the other level roles and the role's permissions are
        reset deterministically.

    .PARAMETER SqlInstance
        Target SQL Server instance.

    .PARAMETER SqlCredential
        Optional PSCredential used to connect. When omitted, the current
        Windows identity (Integrated Security) is used. Passed through to
        dbatools.

    .PARAMETER Database
        One or more database names.

        If omitted or empty:
            all ONLINE, READ_WRITE user databases are used.

    .PARAMETER Level
        Permission level:

            Read
            Read/Update
            Read/Update/Execute

    .PARAMETER Login
        SQL Login name.

    .PARAMETER Password
        Password for the SQL Login, supplied as a SecureString (never plain text).

    .PARAMETER Force
        Recreates the SQL login (drops and recreates it, preserving its SID)
        if it already exists. Also passed to Copy-DbaLogin when
        -CopyToSecondary is used, so the secondary copy is refreshed too.

    .PARAMETER CopyToSecondary
        After configuring the primary, copy the login to the Always On secondary
        replica(s) with Copy-DbaLogin, which preserves the SID and password
        hash. This keeps the login identical on every replica so the
        AG-replicated database users are not orphaned. Secondaries are
        auto-discovered from the AG unless -SecondaryServer is given.

    .PARAMETER SecondaryServer
        Explicit secondary replica name(s) to copy the login to, overriding the
        automatic AG discovery. Only used with -CopyToSecondary.

    .EXAMPLE
        $Password = Read-Host -AsSecureString 'SQL login password'

        New-MSLinkedServerLogin `
            -SqlInstance SQLSERVER01 `
            -Database Database1, Database2 `
            -Level "Read/Update/Execute" `
            -Login LS_MES `
            -Password $Password

    .EXAMPLE
        New-MSLinkedServerLogin `
            -SqlInstance SQLSERVER01 `
            -Level "Read/Update" `
            -Login LS_Reporting `
            -Password $Password

        Configures all user databases.

    .EXAMPLE
        New-MSLinkedServerLogin `
            -SqlInstance SQLSERVER01 `
            -Database MesProd, MesLims `
            -Level "Read/Update/Execute" `
            -Login LS_MES `
            -Password $Password `
            -Force

    .EXAMPLE
        New-MSLinkedServerLogin `
            -SqlInstance SQLLISTENER `
            -Database MesProd `
            -Level "Read/Update/Execute" `
            -Login LS_MES `
            -Password $Password `
            -CopyToSecondary

        Configures the login on the primary and copies it (same SID) to every
        Always On secondary replica so its database users are not orphaned.
    #>

    [CmdletBinding(SupportsShouldProcess = $true, ConfirmImpact = 'Medium')]
    param (
        [Parameter(Mandatory)]
        [string]$SqlInstance,

        [PSCredential]$SqlCredential,

        [string[]]$Database,

        [Parameter(Mandatory)]
        [ValidateSet(
            'Read',
            'Read/Update',
            'Read/Update/Execute'
        )]
        [string]$Level,

        [Parameter(Mandatory)]
        [ValidateNotNullOrEmpty()]
        [string]$Login,

        [Parameter(Mandatory)]
        [ValidateNotNull()]
        [SecureString]$Password,

        [switch]$Force,

        [switch]$CopyToSecondary,

        [string[]]$SecondaryServer
    )


    #------------------------------------------------------------
    # Verify dbatools
    #------------------------------------------------------------

    if (-not (Get-Command Invoke-DbaQuery -ErrorAction SilentlyContinue)) {
        throw "Invoke-DbaQuery was not found. Install/import the dbatools module."
    }

    Write-MSLog -Level Info -Message "New-MSLinkedServerLogin: START. Instance=[$SqlInstance], Login=[$Login], Level=[$Level], Force=[$Force]"

    # Base Invoke-DbaQuery parameters (SqlCredential threaded through when supplied).
    $sqlBase = @{
        SqlInstance     = $SqlInstance
        EnableException = $true
    }
    if ($SqlCredential) { $sqlBase['SqlCredential'] = $SqlCredential }

    # Convert the SecureString password to plain text only where the T-SQL
    # parameter needs it; it is never logged or returned.
    $PlainPassword = [System.Net.NetworkCredential]::new('', $Password).Password


    #------------------------------------------------------------
    # Normalize permission level
    #------------------------------------------------------------

    $LevelToken = switch ($Level) {
        'Read'                { 'Read' }
        'Read/Update'         { 'ReadUpdate' }
        'Read/Update/Execute' { 'ReadUpdateExecute' }
    }


    #------------------------------------------------------------
    # Role names
    #
    # Shared, level-based roles (NOT tied to the login):
    #   role_LinkedServer_Read
    #   role_LinkedServer_ReadUpdate
    #   role_LinkedServer_ReadUpdateExecute
    #------------------------------------------------------------

    $RoleName = "role_LinkedServer_${LevelToken}"

    # All level roles, so a login can be dropped from the other levels
    # when its access level changes.
    $RoleRead              = 'role_LinkedServer_Read'
    $RoleReadUpdate        = 'role_LinkedServer_ReadUpdate'
    $RoleReadUpdateExecute = 'role_LinkedServer_ReadUpdateExecute'


    Write-Verbose "SQL Instance : $SqlInstance"
    Write-Verbose "Login        : $Login"
    Write-Verbose "Level        : $Level"
    Write-Verbose "Role         : $RoleName"
    Write-Verbose "Force        : $Force"


    #============================================================
    # Get database information
    #============================================================

    $DatabaseQuery = @'
SELECT
    name AS DatabaseName,
    database_id AS DatabaseId,
    state_desc AS State,
    is_read_only AS IsReadOnly,
    CASE WHEN source_database_id IS NOT NULL THEN 1 ELSE 0 END AS IsSnapshot
FROM sys.databases
ORDER BY name;
'@


    $AllDatabases = Invoke-DbaQuery @sqlBase `
        -Database master `
        -Query $DatabaseQuery


    #------------------------------------------------------------
    # No database specified = all user databases
    #------------------------------------------------------------

    if (-not $Database -or $Database.Count -eq 0) {
        $TargetDatabases = @(
            $AllDatabases |
                Where-Object {
                    $_.DatabaseId -gt 4 -and
                    $_.State -eq 'ONLINE' -and
                    -not $_.IsReadOnly -and
                    $_.IsSnapshot -eq 0
                } |
                Select-Object -ExpandProperty DatabaseName
        )
    }
    else {
        $TargetDatabases = @()

        foreach ($DatabaseName in ($Database | Select-Object -Unique)) {
            $DbInfo = $AllDatabases |
                Where-Object {
                    $_.DatabaseName -eq $DatabaseName
                }

            if (-not $DbInfo) {
                throw "Database [$DatabaseName] does not exist on [$SqlInstance]."
            }

            if ($DbInfo.State -ne 'ONLINE') {
                throw "Database [$DatabaseName] is not ONLINE. Current state: $($DbInfo.State)"
            }

            if ($DbInfo.IsReadOnly) {
                throw "Database [$DatabaseName] is READ_ONLY."
            }

            if ($DbInfo.IsSnapshot -eq 1) {
                throw "Database [$DatabaseName] is a database snapshot."
            }

            $TargetDatabases += $DbInfo.DatabaseName
        }
    }


    if ($TargetDatabases.Count -eq 0) {
        throw "No databases were selected."
    }


    Write-Verbose "Databases: $($TargetDatabases -join ', ')"


    #============================================================
    # Create / recreate SQL Login
    #============================================================

    $LoginQuery = @'
SET NOCOUNT ON;

DECLARE
    @ExistingSid  varbinary(85),
    @ExistingType nvarchar(60),
    @SQL          nvarchar(max),
    @KillSQL      nvarchar(max);

SELECT
    @ExistingSid  = sid,
    @ExistingType = type_desc
FROM sys.server_principals
WHERE name = @LoginName;


------------------------------------------------------------
-- Name exists but is not a SQL Login
------------------------------------------------------------

IF @ExistingType IS NOT NULL
AND @ExistingType <> N'SQL_LOGIN'
BEGIN
    THROW 50000,
          'A server principal with this name already exists but it is not a SQL Login.',
          1;
END;


------------------------------------------------------------
-- FORCE
--
-- Kill existing sessions and drop the SQL Login.
--
-- Important:
-- Keep the SID so database users mapped to this login
-- do not become orphaned.
------------------------------------------------------------

IF @Force = 1
AND @ExistingType = N'SQL_LOGIN'
BEGIN

    SET @KillSQL = N'';

    SELECT
        @KillSQL = @KillSQL +
                   N'KILL ' +
                   CONVERT(nvarchar(20), session_id) +
                   N';'
    FROM sys.dm_exec_sessions
    WHERE login_name = @LoginName
      AND session_id <> @@SPID;


    IF LEN(@KillSQL) > 0
    BEGIN
        EXEC sys.sp_executesql @KillSQL;
    END;


    SET @SQL =
        N'DROP LOGIN ' +
        QUOTENAME(@LoginName) +
        N';';

    EXEC sys.sp_executesql @SQL;

    SET @ExistingType = NULL;
END;


------------------------------------------------------------
-- Create SQL Login
------------------------------------------------------------

IF @ExistingType IS NULL
BEGIN

    SET @SQL =
        N'CREATE LOGIN ' +
        QUOTENAME(@LoginName) +
        N'
        WITH
            PASSWORD = N''' +
            REPLACE(@Password, N'''', N'''''') +
            N''',
            CHECK_POLICY = ON,
            CHECK_EXPIRATION = OFF,
            DEFAULT_DATABASE = [master]';


    ------------------------------------------------------------
    -- If FORCE recreated an existing login, preserve its SID
    ------------------------------------------------------------

    IF @ExistingSid IS NOT NULL
    BEGIN
        SET @SQL =
            @SQL +
            N',
            SID = ' +
            CONVERT(nvarchar(200), @ExistingSid, 1);
    END;


    SET @SQL = @SQL + N';';

    EXEC sys.sp_executesql @SQL;


    IF @Force = 1
       AND @ExistingSid IS NOT NULL
    BEGIN
        SELECT N'Recreated' AS LoginAction;
    END
    ELSE
    BEGIN
        SELECT N'Created' AS LoginAction;
    END;

END
ELSE
BEGIN

    SELECT N'Existing' AS LoginAction;

END;
'@


    $LoginParameters = @{
        LoginName = $Login
        Password  = $PlainPassword
        Force     = [bool]$Force
    }

    if ($PSCmdlet.ShouldProcess("[$SqlInstance]", "Create/recreate SQL login [$Login] (Force=$Force)")) {
        try {
            $LoginResult = Invoke-DbaQuery @sqlBase `
                -Database master `
                -Query $LoginQuery `
                -SqlParameter $LoginParameters

            $LoginAction = $LoginResult.LoginAction
        }
        catch {
            throw "Failed configuring SQL Login [$Login] on [$SqlInstance]. $($_.Exception.Message)"
        }

        Write-MSLog -Level Info -Message "New-MSLinkedServerLogin: login [$Login] action = $LoginAction."
    }
    else {
        $LoginAction = 'Skipped (WhatIf)'
    }


    #============================================================
    # Configure every target database
    #============================================================

    $Results = @()


    foreach ($DatabaseName in $TargetDatabases) {

        if (-not $PSCmdlet.ShouldProcess("[$SqlInstance].[$DatabaseName]", "Configure user + role [$RoleName] (level $Level)")) {
            $Results += [PSCustomObject]@{
                SqlInstance = $SqlInstance
                Database    = $DatabaseName
                Login       = $Login
                LoginAction = $LoginAction
                Level       = $Level
                Role        = $RoleName
                Status      = 'Skipped (WhatIf)'
            }
            continue
        }

        Write-MSLog -Level Info -Message "New-MSLinkedServerLogin: configuring database [$DatabaseName]."


        $DatabasePermissionQuery = @'
SET NOCOUNT ON;

DECLARE
    @SQL      nvarchar(max),
    @UserType char(1);


------------------------------------------------------------
-- Check existing database principal
------------------------------------------------------------

SELECT
    @UserType = type
FROM sys.database_principals
WHERE name = @LoginName;


------------------------------------------------------------
-- Same name is being used by a database role
------------------------------------------------------------

IF @UserType = 'R'
BEGIN
    THROW 50001,
          'A database role already exists with the same name as the login.',
          1;
END;


------------------------------------------------------------
-- Create database user
------------------------------------------------------------

IF @UserType IS NULL
BEGIN

    SET @SQL =
        N'CREATE USER ' +
        QUOTENAME(@LoginName) +
        N' FOR LOGIN ' +
        QUOTENAME(@LoginName) +
        N';';

    EXEC sys.sp_executesql @SQL;

END
ELSE
BEGIN

    ------------------------------------------------------------
    -- Repair/remap user if necessary
    ------------------------------------------------------------

    SET @SQL =
        N'ALTER USER ' +
        QUOTENAME(@LoginName) +
        N' WITH LOGIN = ' +
        QUOTENAME(@LoginName) +
        N';';

    EXEC sys.sp_executesql @SQL;

END;


------------------------------------------------------------
-- Remove membership from the other possible roles
------------------------------------------------------------

DECLARE @PossibleRoles TABLE
(
    RoleName sysname
);

INSERT INTO @PossibleRoles
(
    RoleName
)
VALUES
    (@RoleRead),
    (@RoleReadUpdate),
    (@RoleReadUpdateExecute);


DECLARE
    @OtherRole sysname;


DECLARE RoleCursor CURSOR LOCAL FAST_FORWARD
FOR
    SELECT RoleName
    FROM @PossibleRoles
    WHERE RoleName <> @RoleName;


OPEN RoleCursor;

FETCH NEXT FROM RoleCursor
INTO @OtherRole;


WHILE @@FETCH_STATUS = 0
BEGIN

    IF EXISTS
    (
        SELECT 1
        FROM sys.database_role_members drm
        INNER JOIN sys.database_principals r
            ON r.principal_id = drm.role_principal_id
        INNER JOIN sys.database_principals u
            ON u.principal_id = drm.member_principal_id
        WHERE r.name = @OtherRole
          AND u.name = @LoginName
    )
    BEGIN

        SET @SQL =
            N'ALTER ROLE ' +
            QUOTENAME(@OtherRole) +
            N' DROP MEMBER ' +
            QUOTENAME(@LoginName) +
            N';';

        EXEC sys.sp_executesql @SQL;

    END;


    FETCH NEXT FROM RoleCursor
    INTO @OtherRole;

END;


CLOSE RoleCursor;
DEALLOCATE RoleCursor;


------------------------------------------------------------
-- Create target role (shared, level-based; created once and
-- reused by every login of this level)
------------------------------------------------------------

IF DATABASE_PRINCIPAL_ID(@RoleName) IS NULL
BEGIN

    SET @SQL =
        N'CREATE ROLE ' +
        QUOTENAME(@RoleName) +
        N' AUTHORIZATION [dbo];';

    EXEC sys.sp_executesql @SQL;

END;


------------------------------------------------------------
-- Remove the permissions managed by this function
--
-- This makes the permission level deterministic.
------------------------------------------------------------

SET @SQL =
    N'REVOKE SELECT, INSERT, UPDATE, DELETE, EXECUTE
      FROM ' +
    QUOTENAME(@RoleName) +
    N';';

EXEC sys.sp_executesql @SQL;


------------------------------------------------------------
-- Apply requested permissions
------------------------------------------------------------

IF @Level = N'Read'
BEGIN

    SET @SQL =
        N'GRANT SELECT
          TO ' +
        QUOTENAME(@RoleName) +
        N';';

END;


IF @Level = N'Read/Update'
BEGIN

    SET @SQL =
        N'GRANT
            SELECT,
            INSERT,
            UPDATE,
            DELETE
          TO ' +
        QUOTENAME(@RoleName) +
        N';';

END;


IF @Level = N'Read/Update/Execute'
BEGIN

    SET @SQL =
        N'GRANT
            SELECT,
            INSERT,
            UPDATE,
            DELETE,
            EXECUTE
          TO ' +
        QUOTENAME(@RoleName) +
        N';';

END;


EXEC sys.sp_executesql @SQL;


------------------------------------------------------------
-- Add user to role
------------------------------------------------------------

IF NOT EXISTS
(
    SELECT 1
    FROM sys.database_role_members drm
    INNER JOIN sys.database_principals r
        ON r.principal_id = drm.role_principal_id
    INNER JOIN sys.database_principals u
        ON u.principal_id = drm.member_principal_id
    WHERE r.name = @RoleName
      AND u.name = @LoginName
)
BEGIN

    SET @SQL =
        N'ALTER ROLE ' +
        QUOTENAME(@RoleName) +
        N' ADD MEMBER ' +
        QUOTENAME(@LoginName) +
        N';';

    EXEC sys.sp_executesql @SQL;

END;


SELECT
    DB_NAME()      AS DatabaseName,
    @LoginName     AS LoginName,
    @RoleName      AS RoleName,
    @Level         AS PermissionLevel;
'@


        $DatabaseParameters = @{
            LoginName             = $Login
            RoleName              = $RoleName
            RoleRead              = $RoleRead
            RoleReadUpdate        = $RoleReadUpdate
            RoleReadUpdateExecute = $RoleReadUpdateExecute
            Level                 = $Level
        }


        try {
            $null = Invoke-DbaQuery @sqlBase `
                -Database $DatabaseName `
                -Query $DatabasePermissionQuery `
                -SqlParameter $DatabaseParameters

            $Results += [PSCustomObject]@{
                SqlInstance = $SqlInstance
                Database    = $DatabaseName
                Login       = $Login
                LoginAction = $LoginAction
                Level       = $Level
                Role        = $RoleName
                Status      = 'OK'
            }
        }
        catch {
            Write-MSLog -Level Error -Message "New-MSLinkedServerLogin: [$DatabaseName] failed - $($_.Exception.Message)"

            $Results += [PSCustomObject]@{
                SqlInstance = $SqlInstance
                Database    = $DatabaseName
                Login       = $Login
                LoginAction = $LoginAction
                Level       = $Level
                Role        = $RoleName
                Status      = "ERROR: $($_.Exception.Message)"
            }
        }
    }

    #============================================================
    # Copy the login to the Always On secondary replica(s)
    #
    # The login must exist on every replica with the SAME SID so the
    # AG-replicated database users map correctly instead of becoming
    # orphaned. Copy-DbaLogin preserves the SID and password hash; creating
    # a fresh login on the secondary would generate a new SID and orphan them.
    #============================================================

    if ($CopyToSecondary -and $LoginAction -ne 'Skipped (WhatIf)') {

        # Resolve the secondary replica(s): explicit list, else auto-discover.
        if ($SecondaryServer) {
            $secondaries = @($SecondaryServer | Where-Object { $_ } | Select-Object -Unique)
        }
        elseif (Get-Command Get-MSSecondaryServerName -ErrorAction SilentlyContinue) {
            $secondaries = @(Get-MSSecondaryServerName -SqlInstance $SqlInstance -AsArray) |
                Where-Object { $_ } | Select-Object -Unique
        }
        else {
            $secondaries = @()
            Write-MSLog -Level Warning -Message "New-MSLinkedServerLogin: Get-MSSecondaryServerName not available; pass -SecondaryServer to copy the login."
        }

        if (-not (Get-Command Copy-DbaLogin -ErrorAction SilentlyContinue)) {
            Write-MSLog -Level Warning -Message "New-MSLinkedServerLogin: Copy-DbaLogin (dbatools) not available; skipping copy to secondary."
        }
        elseif (-not $secondaries -or $secondaries.Count -eq 0) {
            Write-MSLog -Level Info -Message "New-MSLinkedServerLogin: -CopyToSecondary set but no secondary replica was found for [$SqlInstance]."
        }
        else {
            foreach ($secondary in $secondaries) {

                if (-not $PSCmdlet.ShouldProcess("[$secondary]", "Copy login [$Login] from [$SqlInstance] (preserve SID)")) {
                    continue
                }

                try {
                    $copyParams = @{
                        Source          = $SqlInstance
                        Destination     = $secondary
                        Login           = $Login
                        EnableException = $true
                    }
                    if ($SqlCredential) {
                        $copyParams['SourceSqlCredential']      = $SqlCredential
                        $copyParams['DestinationSqlCredential'] = $SqlCredential
                    }
                    if ($Force) { $copyParams['Force'] = $true }

                    $null = Copy-DbaLogin @copyParams

                    Write-MSLog -Level Info -Message "New-MSLinkedServerLogin: copied login [$Login] to secondary [$secondary]."

                    $Results += [PSCustomObject]@{
                        SqlInstance = $secondary
                        Database    = '(secondary replica)'
                        Login       = $Login
                        LoginAction = 'CopiedToSecondary'
                        Level       = $Level
                        Role        = $RoleName
                        Status      = 'OK'
                    }
                }
                catch {
                    Write-MSLog -Level Error -Message "New-MSLinkedServerLogin: failed to copy login [$Login] to secondary [$secondary] - $($_.Exception.Message)"

                    $Results += [PSCustomObject]@{
                        SqlInstance = $secondary
                        Database    = '(secondary replica)'
                        Login       = $Login
                        LoginAction = 'CopyToSecondaryFailed'
                        Level       = $Level
                        Role        = $RoleName
                        Status      = "ERROR: $($_.Exception.Message)"
                    }
                }
            }
        }
    }

    Write-MSLog -Level Info -Message "New-MSLinkedServerLogin: DONE. Login=[$Login], Databases=$($TargetDatabases.Count)."

    return $Results
}