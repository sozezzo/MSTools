function Test-MSWmiAccess {
<#
.SYNOPSIS
  Tests whether a user account can access WMI on a remote server.

.DESCRIPTION
  Attempts to query basic WMI classes to confirm:
    - Remote connection works
    - Firewall permits WMI
    - User has permission for root\cimv2
    - DCOM/WMI provider responds correctly

  Uses Get-CimInstance first (via a DCOM CimSession).
  Falls back to Get-WmiObject for legacy testing.

.PARAMETER ComputerName
  Target server to test, e.g. "MyServer01".

.PARAMETER Credential
  Credential object for the account to test
  (e.g., from Get-Credential).

.EXAMPLE
  $cred = Get-Credential "MYDOMAIN\svc_Monitor"
  Test-WmiAccess -ComputerName "MyServer01" -Credential $cred
#>

    [CmdletBinding()]
    param(
        [Parameter(Mandatory)]
        [string]$ComputerName,

        [Parameter(Mandatory)]
        [System.Management.Automation.PSCredential]$Credential
    )

    # WMI / DCOM connect to a Windows HOST, not a SQL Server named instance.
    # Strip any "\INSTANCE" and ",PORT" so "HOST\INSTANCE" targets "HOST"
    # (a named instance would otherwise cause "Invalid namespace root\cimv2").
    $targetHost = (($ComputerName -replace '^(tcp|np|lpc):', '') -split '[\\,]')[0].Trim()

    $result = [ordered]@{
        ComputerName = $ComputerName
        UserName     = $Credential.UserName
        Cim_Ok       = $false
        Wmi_Ok       = $false
        ErrorMessage = $null
        Timestamp    = (Get-Date)
    }

    Write-Host "`nTesting WMI connectivity to [$ComputerName] using [$($Credential.UserName)]..." -ForegroundColor Cyan

    # -----------------------------
    # CIM test via DCOM CimSession
    # -----------------------------
    # On PowerShell 7 the CIM auto-loader can resolve the Windows PowerShell
    # (Desktop) copy of CimCmdlets from System32 - which is not compatible with
    # the Core edition - and fail with:
    #   "Cannot find the built-in module 'CimCmdlets' ... 'Core' edition".
    # Importing it by its full path under $PSHOME forces the correct copy.
    if (-not (Get-Module CimCmdlets)) {
        Import-Module (Join-Path $PSHOME 'Modules\CimCmdlets') -ErrorAction SilentlyContinue
    }

    if (-not (Get-Module CimCmdlets)) {
        $msg = "CIM SKIPPED: CimCmdlets module could not be loaded in this PowerShell session."
        $result.ErrorMessage = $msg
        Write-Host $msg -ForegroundColor DarkYellow
    }
    else {
        $cimSession = $null
        try {
            # Force DCOM so the test really validates classic WMI/DCOM path,
            # and avoid using -Credential directly on Get-CimInstance.
            $sessionOption = New-CimSessionOption -Protocol Dcom

            $cimSession = New-CimSession -ComputerName $targetHost `
                                         -Credential $Credential `
                                         -SessionOption $sessionOption `
                                         -ErrorAction Stop

            $null = Get-CimInstance -ClassName Win32_OperatingSystem `
                                    -CimSession $cimSession `
                                    -ErrorAction Stop

            $result.Cim_Ok = $true
            Write-Host "CIM test (DCOM): OK" -ForegroundColor Green
        }
        catch {
            $msg = "CIM FAILED: $($_.Exception.Message)"
            $result.ErrorMessage = $msg
            Write-Warning $msg
        }
        finally {
            if ($cimSession) {
                $cimSession | Remove-CimSession
            }
        }
    }

    # -----------------------------
    # Legacy WMI test (Get-WmiObject)
    # -----------------------------
    try {
        $null = Get-WmiObject -Class Win32_ComputerSystem `
                              -ComputerName $targetHost `
                              -Credential $Credential `
                              -ErrorAction Stop

        $result.Wmi_Ok = $true
        Write-Host "WMI (legacy) test: OK" -ForegroundColor Green
    }
    catch {
        $msg = "WMI FAILED: $($_.Exception.Message)"

        if ($result.ErrorMessage) {
            $result.ErrorMessage += "`n$msg"
        }
        else {
            $result.ErrorMessage = $msg
        }

        Write-Warning "WMI legacy test failed: $($_.Exception.Message)"
    }

    [pscustomobject]$result
}
 