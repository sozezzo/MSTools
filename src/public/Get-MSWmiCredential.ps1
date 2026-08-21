function Get-MSWmiCredential {
<#
.SYNOPSIS
    Prompts for (and optionally validates) an Active Directory credential to be
    used for remote WMI / CIM queries.

.DESCRIPTION
    Interactively asks the operator for a Windows / Active Directory credential
    using the standard secure Get-Credential dialog, so an elevated or
    different AD account can be used for out-of-band WMI/CIM diagnostics
    without hard-coding a password.

    When -TestComputer is supplied the function validates the credential by
    opening a short-lived CIM session against that computer. If the session
    cannot be opened the credential is rejected (an error is thrown unless
    -Quiet is used, in which case $null is returned).

    The function never writes the password anywhere; it only returns a live
    [PSCredential] object to the caller.

.PARAMETER UserName
    Optional default user name to pre-fill in the prompt (e.g. 'CONTOSO\dba').

.PARAMETER Message
    Optional message shown in the credential dialog.

.PARAMETER TestComputer
    Optional computer name to validate the credential against by opening a
    temporary CIM session. Useful to confirm the account really has WMI rights
    on a replica node before running diagnostics.

.PARAMETER Quiet
    Return $null instead of throwing when the prompt is cancelled or the
    validation against -TestComputer fails.

.OUTPUTS
    [System.Management.Automation.PSCredential] or $null (with -Quiet).

.EXAMPLE
    $cred = Get-MSWmiCredential -UserName 'CONTOSO\dba'

    Prompts for a password for CONTOSO\dba and returns the credential.

.EXAMPLE
    $cred = Get-MSWmiCredential -TestComputer 'ISI-M0BDRS31'
    Get-MSAlwaysOnHealth -SqlInstance 'ISI-M0BDRS32' -UseWmi -WmiCredential $cred

    Prompts for a credential, verifies it can open a CIM session on the
    secondary node, then uses it for the Always On WMI diagnostics.

.NOTES
    Read-only: the function only prompts and, optionally, opens and immediately
    closes a validation CIM session. It changes nothing on the target computer.
#>
    [CmdletBinding()]
    [OutputType([System.Management.Automation.PSCredential])]
    param(
        [string] $UserName,

        [string] $Message = 'Enter the Active Directory credential to use for WMI/CIM queries.',

        [string] $TestComputer,

        [switch] $Quiet
    )

    # --- Prompt for the credential ----------------------------------------
    try {
        if ($UserName) {
            $credential = Get-Credential -UserName $UserName -Message $Message -ErrorAction Stop
        }
        else {
            $credential = Get-Credential -Message $Message -ErrorAction Stop
        }
    }
    catch {
        $msg = "Credential prompt was cancelled or failed: $($_.Exception.Message)"
        if ($Quiet) { Write-Verbose $msg; return $null }
        throw $msg
    }

    if (-not $credential) {
        $msg = 'No credential was provided.'
        if ($Quiet) { Write-Verbose $msg; return $null }
        throw $msg
    }

    # --- Optional validation against a computer via CIM -------------------
    if ($TestComputer) {
        $cimSession = $null
        try {
            $cimSession = New-CimSession -ComputerName $TestComputer -Credential $credential -ErrorAction Stop
            # Touch a trivial class to confirm the account really has WMI rights.
            $null = Get-CimInstance -CimSession $cimSession -ClassName Win32_ComputerSystem -ErrorAction Stop
            Write-Verbose "Credential '$($credential.UserName)' validated against [$TestComputer]."
        }
        catch {
            $msg = "Credential '$($credential.UserName)' failed WMI validation against [$TestComputer]: $($_.Exception.Message)"
            if ($Quiet) { Write-Verbose $msg; return $null }
            throw $msg
        }
        finally {
            if ($cimSession) { Remove-CimSession -CimSession $cimSession -ErrorAction SilentlyContinue }
        }
    }

    return $credential
}
