function Grant-MSWmiAccess {
<#
.SYNOPSIS
  Grants a service account the required permissions to use WMI on the local
  or remote Windows server (groups + optional Administrators + firewall).

.DESCRIPTION
  - If -ComputerName is omitted, applies on the LOCAL server.
  - Resolves the account to a SID on the target server (fails fast if not resolvable).
  - Adds the account to local groups:
      * Performance Monitor Users
      * Performance Log Users
      * Distributed COM Users
      * (Optional) Administrators
    Uses LocalAccounts cmdlets when available; otherwise uses ADSI (WinNT provider) fallback.
  - Enables "Windows Management Instrumentation (WMI)" firewall rule group.
  - -GrantRootCimv2RemoteAccess is kept for backward compatibility (no-op).

.PARAMETER ComputerName
  Optional. Target server. Defaults to local computer.

.PARAMETER ServiceAccount
  Account to grant (DOMAIN\User or MACHINE\User).

.PARAMETER AddToAdministrators
  Add to local Administrators.

.PARAMETER GrantRootCimv2RemoteAccess
  Deprecated/no-op (kept so old scripts don’t break).

.PARAMETER Credential
  Optional. Credential used to connect to a remote -ComputerName (passed to
  Invoke-Command). Ignored when running against the local computer.

.EXAMPLE
  Grant-MSWmiAccess -ServiceAccount "MYDOMAIN\svc_Monitor" -AddToAdministrators
#>

    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $false)]
        [string]$ComputerName = $env:COMPUTERNAME,

        [Parameter(Mandatory = $true)]
        [string]$ServiceAccount,

        [switch]$AddToAdministrators,

        # Backward compatibility only – ignored
        [switch]$GrantRootCimv2RemoteAccess,

        [Parameter(Mandatory = $false)]
        [System.Management.Automation.PSCredential]$Credential
    )

    # WMI / remoting target the Windows HOST, not a SQL Server named instance.
    # Strip any "\INSTANCE" and ",PORT" so "HOST\INSTANCE" targets "HOST".
    $targetHost = (($ComputerName -replace '^(tcp|np|lpc):', '') -split '[\\,]')[0].Trim()

    Write-Host "Granting WMI-related access for [$ServiceAccount] on [$targetHost]..." -ForegroundColor Cyan

    $scriptBlock = {
        param($ServiceAccount, $AddToAdministrators, $GrantRootCimv2RemoteAccess)

        Write-Host "Running on $env:COMPUTERNAME as $(whoami)" -ForegroundColor Cyan
        Write-Host "Target account: [$ServiceAccount]" -ForegroundColor Cyan

        if ($GrantRootCimv2RemoteAccess) {
            Write-Host "Note: -GrantRootCimv2RemoteAccess is deprecated/no-op in this version." -ForegroundColor DarkGray
        }

        # 0) Resolve account -> SID
        try {
            $ntAccount = New-Object System.Security.Principal.NTAccount($ServiceAccount)
            $sid       = $ntAccount.Translate([System.Security.Principal.SecurityIdentifier])
            Write-Host "Account resolved successfully. SID = $sid" -ForegroundColor Green
        }
        catch {
            Write-Error "Cannot resolve account [$ServiceAccount] on [$env:COMPUTERNAME]. Error: $($_.Exception.Message)"
            return
        }

        $hasLocalAccounts = [bool](Get-Command Get-LocalGroup -ErrorAction SilentlyContinue)

        # --- ADSI helpers (fallback when LocalAccounts module is missing) ---
        function Get-AdsiGroup {
            param([string]$GroupName)
            $path = "WinNT://$env:COMPUTERNAME/$GroupName,group"
            return [ADSI]$path  # returns DirectoryEntry
        }

        function Test-AdsiIsMember {
            param(
                [object]$Group,     # DirectoryEntry
                [string]$Account
            )

            $members = @($Group.psbase.Invoke("Members")) | ForEach-Object {
                $_.GetType().InvokeMember("Name", 'GetProperty', $null, $_, $null)
            }

            if ($Account -like "*\*") {
                $short = $Account.Split('\',2)[1]
                return ($members -contains $short)
            }

            return ($members -contains $Account)
        }

        function Add-AdsiMember {
            param(
                [object]$Group,     # DirectoryEntry
                [string]$Account
            )

            $winntPath = if ($Account -like "*\*") {
                $d,$u = $Account.Split('\',2)
                "WinNT://$d/$u"
            } else {
                "WinNT://$env:COMPUTERNAME/$Account"
            }

            $Group.Add($winntPath)
        }

        # 1) Add to groups
        $groups = @(
            'Performance Monitor Users',
            'Performance Log Users',
            'Distributed COM Users'
        )

        if ($AddToAdministrators) {
            $groups += 'Administrators'
        }

        foreach ($g in $groups) {
            try {
                if ($hasLocalAccounts) {
                    # Modern path
                    $null = Get-LocalGroup -Name $g -ErrorAction Stop

                    $already = Get-LocalGroupMember -Group $g -ErrorAction SilentlyContinue |
                               Where-Object { $_.Name -ieq $ServiceAccount }

                    if ($already) {
                        Write-Host "[$ServiceAccount] is already a member of [$g]. Skipping." -ForegroundColor DarkGray
                    } else {
                        Write-Host "Adding [$ServiceAccount] to group [$g]..." -ForegroundColor Yellow
                        Add-LocalGroupMember -Group $g -Member $ServiceAccount -ErrorAction Stop
                    }
                }
                else {
                    # ADSI fallback
                    $grpAdsi = Get-AdsiGroup $g

                    if (Test-AdsiIsMember $grpAdsi $ServiceAccount) {
                        Write-Host "[$ServiceAccount] is already a member of [$g]. Skipping." -ForegroundColor DarkGray
                    } else {
                        Write-Host "Adding [$ServiceAccount] to group [$g] (ADSI fallback)..." -ForegroundColor Yellow
                        Add-AdsiMember $grpAdsi $ServiceAccount
                    }
                }
            }
            catch {
                Write-Warning "Failed to process group [$g] for [$ServiceAccount]: $($_.Exception.Message)"
            }
        }

        # 2) Enable WMI firewall rules
        try {
            Write-Host "Enabling WMI Firewall Rules..." -ForegroundColor Yellow
            Enable-NetFirewallRule -DisplayGroup 'Windows Management Instrumentation (WMI)' -ErrorAction Stop
        }
        catch {
            Write-Warning "Failed to enable WMI firewall rules: $($_.Exception.Message)"
        }

        Write-Host "WMI-related configuration completed on $env:COMPUTERNAME." -ForegroundColor Green
    }

    if ($targetHost -ieq $env:COMPUTERNAME) {
        & $scriptBlock $ServiceAccount $AddToAdministrators $GrantRootCimv2RemoteAccess
        Write-Host "Finished." -ForegroundColor Green
    }
    else {
        $invokeParams = @{
            ComputerName = $targetHost
            ScriptBlock  = $scriptBlock
            ArgumentList = $ServiceAccount, $AddToAdministrators, $GrantRootCimv2RemoteAccess
        }
        if ($Credential) { $invokeParams.Credential = $Credential }

        Invoke-Command @invokeParams
        Write-Host "Finished remote execution." -ForegroundColor Green
    }
}