function Test-MSIsAdmin {
    <#
    .SYNOPSIS
        Checks if the current PowerShell session is running as Administrator.
    .OUTPUTS
        [bool] $true if running elevated, otherwise $false.
    #>
    try {
        $id = [Security.Principal.WindowsIdentity]::GetCurrent()
        $principal = New-Object Security.Principal.WindowsPrincipal($id)
        return $principal.IsInRole([Security.Principal.WindowsBuiltinRole]::Administrator)  
    }
    catch {
        Write-Verbose "Admin check failed: $($_.Exception.Message)"
        return $false
    }
}