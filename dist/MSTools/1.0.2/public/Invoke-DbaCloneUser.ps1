function Invoke-DbaCloneUser {

    <#
    .SYNOPSIS
        Clones database users from a source database to a destination database, optionally forcing recreation and auto-adding missing logins.

    .DESCRIPTION
        - Skips system users (dbo, guest, INFORMATION_SCHEMA, sys, ##MS_*).
        - Respects -IncludeUser / -ExcludeUser.
        - Auto-creates missing logins on the destination using Copy-DbaLogin from the source.
        - Preserves DefaultSchema when present.
        - If -Force is specified and the user already exists on the destination, it will be dropped and recreated.
        - Supports -WhatIf/-Confirm via SupportsShouldProcess.
        - Uses Write-Log (expects -Message/-Level).

    .PARAMETER SourceInstance
        Source SQL Server instance.

    .PARAMETER SourceDatabase
        Source database name.

    .PARAMETER DestInstance
        Destination SQL Server instance.

    .PARAMETER DestDatabase
        Destination database name.

    .PARAMETER IncludeUser
        Exact-match list of users to include (case-insensitive).

    .PARAMETER ExcludeUser
        Exact-match list of users to exclude (case-insensitive).

    .PARAMETER Force
        If set: drop/recreate users that already exist; pass through to Copy-DbaLogin to overwrite/refresh login as needed.

    .PARAMETER PassThru
        Returns a summary object with details.

    .OUTPUTS
        PSCustomObject when -PassThru is provided.

    .DEPENDENCIES
        dbatools: Get-DbaDbUser, New-DbaDbUser, Remove-DbaDbUser, Get-DbaLogin, Copy-DbaLogin

    .AUTHOR
        Sozezzo Astra

    #>    

    [CmdletBinding(SupportsShouldProcess)]
    param(
        [Parameter(Mandatory)][string]$SourceInstance,
        [Parameter(Mandatory)][string]$SourceDatabase,
        [Parameter(Mandatory)][string]$DestInstance,
        [Parameter(Mandatory)][string]$DestDatabase,
        [string[]]$IncludeUser,
        [string[]]$ExcludeUser,
        [switch]$Force,
        [switch]$PassThru
    )

    $systemUserNames    = @('dbo','guest','INFORMATION_SCHEMA','sys')
    $systemUserPatterns = @('##MS_*')

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

    Write-Log -Message "Cloning users from [$SourceInstance].[$SourceDatabase] to [$DestInstance].[$DestDatabase] (Force=$Force)" -Level Info

    $srcUsers = Get-DbaDbUser -SqlInstance $SourceInstance -Database $SourceDatabase -EnableException
    $dstUsers = Get-DbaDbUser -SqlInstance $DestInstance -Database $DestDatabase   -EnableException

    $dstUserNames = @{}
    foreach ($du in $dstUsers) { $dstUserNames[$du.Name.ToLowerInvariant()] = $true }

    $dstLogins = Get-DbaLogin -SqlInstance $DestInstance -EnableException
    $dstLoginNames = @{}
    foreach ($dl in $dstLogins) { $dstLoginNames[$dl.Name.ToLowerInvariant()] = $true }

    $created = 0; $skipped = 0; $failed = 0
    $results = @()

    foreach ($u in $srcUsers) {
        $name = $u.Name

        if (_isSystemUser $name $u.IsSystemObject) {
            Write-Log -Message "Skip system user: $name" -Level Info
            $skipped++; $results += [pscustomobject]@{ User=$name; Action='Skip-System'; Detail='' }
            continue
        }

        if (-not (_includeName $name $IncludeUser)) {
            Write-Log -Message "Skip (not in IncludeUser): $name" -Level Info
            $skipped++; $results += [pscustomobject]@{ User=$name; Action='Skip-Include'; Detail='' }
            continue
        }
        if (_excludeName $name $ExcludeUser) {
            Write-Log -Message "Skip (in ExcludeUser): $name" -Level Info
            $skipped++; $results += [pscustomobject]@{ User=$name; Action='Skip-Exclude'; Detail='' }
            continue
        }

        $existsOnDest = $dstUserNames.ContainsKey($name.ToLowerInvariant())

        if ($existsOnDest -and -not $Force) {
            Write-Log -Message "Skip (already exists on destination): $name" -Level Info
            $skipped++; $results += [pscustomobject]@{ User=$name; Action='Skip-Exists'; Detail='' }
            continue
        } elseif ($existsOnDest -and $Force) {
            if ($PSCmdlet.ShouldProcess("[$DestInstance].[$DestDatabase]", "Drop user '$name' before recreate")) {
                Remove-DbaDbUser -SqlInstance $DestInstance -Database $DestDatabase -User $name -Confirm:$false -EnableException
                $dstUserNames.Remove($name.ToLowerInvariant()) | Out-Null
                Write-Log -Message "Dropped existing user: $name (Force)" -Level Warning
            }
        }

        # Ensure/refresh login when mapped
        if ($u.Login) {
            $loginName = $u.Login
            $loginKey  = $loginName.ToLowerInvariant()

            if ($Force) {
                if ($PSCmdlet.ShouldProcess($DestInstance, "Copy/refresh login '$loginName' from $SourceInstance (Force)")) {
                    Copy-DbaLogin -Source $SourceInstance -Destination $DestInstance -Login $loginName -Force:$true -EnableException
                    $dstLoginNames[$loginKey] = $true
                    Write-Log -Message "Ensured/updated login on destination: $loginName" -Level Warning
                }
            } elseif (-not $dstLoginNames.ContainsKey($loginKey)) {
                if ($PSCmdlet.ShouldProcess($DestInstance, "Copy missing login '$loginName' from $SourceInstance")) {
                    Copy-DbaLogin -Source $SourceInstance -Destination $DestInstance -Login $loginName -EnableException
                    $dstLoginNames[$loginKey] = $true
                    Write-Log -Message "Added missing login on destination: $loginName" -Level Warning
                }
            }
        }

        # >>> NEW: re-check user existence right before creation <<<
        $userNow = Get-DbaDbUser -SqlInstance $DestInstance -Database $DestDatabase -User $name -EnableException -ErrorAction SilentlyContinue
        if ($null -ne $userNow) {
            if (-not $Force) {
                Write-Log -Message "Skip (detected user now exists after login copy): $name" -Level Info
                $skipped++; $results += [pscustomobject]@{ User=$name; Action='Skip-Exists-PostCheck'; Detail='' }
                continue
            } else {
                if ($PSCmdlet.ShouldProcess("[$DestInstance].[$DestDatabase]", "Drop user '$name' (post-check) before recreate")) {
                    Remove-DbaDbUser -SqlInstance $DestInstance -Database $DestDatabase -User $name -Confirm:$false -EnableException
                    Write-Log -Message "Dropped existing user (post-check): $name (Force)" -Level Warning
                }
            }
        }

        # Create (or recreate) user
        $params = @{
            SqlInstance     = $DestInstance
            Database        = $DestDatabase
            Username        = $u.Name
            Confirm         = $false
            EnableException = $true
            Force           = $Force
        }
        if ($u.Login)         { $params.Login = $u.Login }
        if ($u.DefaultSchema) { $params.DefaultSchema = $u.DefaultSchema }

        if ($PSCmdlet.ShouldProcess("[$DestInstance].[$DestDatabase]", "Create user '$($u.Name)' (Login='$($u.Login)'; DefaultSchema='$($u.DefaultSchema)'; Force=$Force)")) {
            New-DbaDbUser @params

            $action = 'Created'
            if ($existsOnDest) { $action = 'Recreated' }
            Write-Log -Message ("{0} user: {1} (Login='{2}'; DefaultSchema='{3}')" -f $action, $name, $u.Login, $u.DefaultSchema) -Level Warning
            $created++
            $results += [pscustomobject]@{
                User   = $name
                Action = $action
                Detail = ("Login={0}; DefaultSchema={1}" -f $u.Login, $u.DefaultSchema)
            }
        }
    }

    Write-Log -Message ("Summary: Created={0}  Skipped={1}  Failed={2}" -f $created,$skipped,$failed) -Level Warning
    if ($PassThru) {
        [pscustomobject]@{
            Source      = "$SourceInstance.$SourceDatabase"
            Destination = "$DestInstance.$DestDatabase"
            Created     = $created
            Skipped     = $skipped
            Failed      = $failed
            Details     = $results
        }
    }
}
     