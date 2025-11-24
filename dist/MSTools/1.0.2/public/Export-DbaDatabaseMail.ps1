function Export-DbaDatabaseMail {
<#
.SYNOPSIS
  Export Database Mail (accounts, profiles, mappings, principal grants, global config) to a single .sql file.

.DESCRIPTION
  Reads Database Mail metadata from msdb and emits an idempotent T-SQL script using sysmail_* procedures.
  SMTP passwords cannot be retrieved; a placeholder is emitted and must be filled manually.

.PARAMETER SqlInstance
  Target SQL Server instance (name, listener, or connection string).

.PARAMETER Path
  Directory where the file will be written when -Filename is not supplied (uses DatabaseMail.sql).

.PARAMETER Filename
  Full output path including file name. Overrides -Path.

.EXAMPLE
  Export-DbaDatabaseMail -SqlInstance "MyServer" -Path "C:\temp\sql"

.EXAMPLE
  Export-DbaDatabaseMail -SqlInstance "ProdListener,1433" -Filename "D:\exports\dbmail_Prod.sql"

.NOTES
  Author : Sozezzo Astra
  Version: 1.1.0
#>
  [CmdletBinding()]
  param(
    [Parameter(Mandatory)][string]$SqlInstance,
    [string]$Path,
    [string]$Filename
  )

  begin {
    function Write-LogSafe {
      param(
        [ValidateSet('Debug','Info','Warning','Error','Critical')]
        [string]$Level = 'Info',
        [string]$Message = ''
      )
      if (Get-Command Write-Log -ErrorAction SilentlyContinue) {
        Write-Log -Level $Level -Message $Message
      } else {
        switch ($Level) {
          'Debug'    { Write-Verbose $Message }
          'Info'     { Write-Host $Message }
          'Warning'  { Write-Warning $Message }
          'Error'    { Write-Error $Message }
          'Critical' { Write-Error $Message }
        }
      }
    }

    $SqlQuote = {
      param([string]$s)
      if ($null -eq $s) { return $null }
      return ($s -replace '''','''''')
    }

    if (-not (Get-Command Invoke-DbaQuery -ErrorAction SilentlyContinue)) {
      Write-LogSafe -Level Error -Message "dbatools is required (Invoke-DbaQuery not found). Import or install dbatools and try again."
      return
    }
  }

  process {
    try {
      Write-LogSafe -Level Info -Message "Starting Export-DbaDatabaseMail for instance [$SqlInstance]"

      if ([string]::IsNullOrWhiteSpace($Filename)) {
        if ([string]::IsNullOrWhiteSpace($Path)) { throw "Either -Path or -Filename must be provided." }
        $Filename = Join-Path -Path $Path -ChildPath 'DatabaseMail.sql'
      }

      $outDir = Split-Path -Path $Filename -Parent
      if (-not (Test-Path -LiteralPath $outDir)) {
        Write-LogSafe -Level Info -Message "Creating directory [$outDir]"
        New-Item -Path $outDir -ItemType Directory -Force | Out-Null
      }

      # Queries (tables for static objects; helper SPs for version-safe metadata)
      $qAccounts = @"
SELECT a.account_id, a.name AS account_name, a.description, a.email_address, a.display_name, a.replyto_address,
       s.servername AS mailserver_name, s.port, s.enable_ssl, s.username
FROM msdb.dbo.sysmail_account a
LEFT JOIN msdb.dbo.sysmail_server s ON a.account_id = s.account_id
ORDER BY a.account_id;
"@

      $qProfiles = @"
SELECT profile_id, name AS profile_name, description
FROM msdb.dbo.sysmail_profile
ORDER BY profile_id;
"@

      $qProfileAccounts = @"
SELECT pa.profile_id, p.name AS profile_name, pa.account_id, a.name AS account_name, pa.sequence_number
FROM msdb.dbo.sysmail_profileaccount pa
JOIN msdb.dbo.sysmail_profile p ON pa.profile_id = p.profile_id
JOIN msdb.dbo.sysmail_account a ON pa.account_id = a.account_id
ORDER BY p.name, pa.sequence_number;
"@

      # Use helper SPs to avoid schema drift
      $qPrincipalProfile = "EXEC msdb.dbo.sysmail_help_principalprofile_sp;"   # returns profile_id, profile_name, principal_id, is_default
      $qConfiguration    = "EXEC msdb.dbo.sysmail_help_configure_sp;"          # returns parameter_name, parameter_value, description

      # Execute
      $accounts         = Invoke-DbaQuery -SqlInstance $SqlInstance -Database msdb -Query $qAccounts
      $profiles         = Invoke-DbaQuery -SqlInstance $SqlInstance -Database msdb -Query $qProfiles
      $profileAccounts  = Invoke-DbaQuery -SqlInstance $SqlInstance -Database msdb -Query $qProfileAccounts
      $principalProfile = Invoke-DbaQuery -SqlInstance $SqlInstance -Database msdb -Query $qPrincipalProfile
      $configuration    = Invoke-DbaQuery -SqlInstance $SqlInstance -Database msdb -Query $qConfiguration

      # Build script
      $sb = New-Object System.Text.StringBuilder
      [void]$sb.AppendLine("-- Database Mail export generated on $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')")
      [void]$sb.AppendLine("-- Instance: $SqlInstance")
      [void]$sb.AppendLine("USE [msdb];")
      [void]$sb.AppendLine("SET NOCOUNT ON;")
      [void]$sb.AppendLine("")

      # Global configuration: set values via sysmail_configure_sp (safe to re-run)
      if ($configuration -and $configuration.Count -gt 0) {
        [void]$sb.AppendLine("-- Configuration")
        foreach ($row in $configuration) {
          $pname  = & $SqlQuote $row.parameter_name
          $pvalue = & $SqlQuote $row.parameter_value
          [void]$sb.AppendLine("EXEC msdb.dbo.sysmail_configure_sp @parameter_name = N'$pname', @parameter_value = N'$pvalue';")
        }
        [void]$sb.AppendLine("")
      }

      # Accounts
      if ($accounts -and $accounts.Count -gt 0) {
        [void]$sb.AppendLine("-- Accounts")
        foreach ($a in $accounts) {
          $an   = & $SqlQuote $a.account_name
          $desc = if ($a.description)     { " , @description = N'$(& $SqlQuote $a.description)'" } else { "" }
          $email= if ($a.email_address)   { " , @email_address = N'$(& $SqlQuote $a.email_address)'" } else { "" }
          $dname= if ($a.display_name)    { " , @display_name  = N'$(& $SqlQuote $a.display_name)'" } else { "" }
          $reply= if ($a.replyto_address) { " , @replyto_address = N'$(& $SqlQuote $a.replyto_address)'" } else { "" }

          $msvr = if ($a.mailserver_name) { " , @mailserver_name = N'$(& $SqlQuote $a.mailserver_name)'" } else { "" }
          $port = if ($a.port -is [int] -and $a.port -gt 0) { " , @port = $($a.port)" } else { "" }
          $ssl  = if ($null -ne $a.enable_ssl) { " , @enable_ssl = " + ($(if($a.enable_ssl){1}else{0})) } else { "" }
          $user = if ($a.username) { " , @username = N'$(& $SqlQuote $a.username)'" } else { "" }

          [void]$sb.AppendLine("IF NOT EXISTS (SELECT 1 FROM msdb.dbo.sysmail_account WHERE name = N'$an')")
          [void]$sb.AppendLine("BEGIN")
          [void]$sb.AppendLine("  EXEC msdb.dbo.sysmail_add_account_sp @account_name = N'$an'$desc$email$dname$reply$msvr$port$ssl$user")
          [void]$sb.AppendLine("    /* , @password = N'<<FILL_PASSWORD>>' */ ;")
          [void]$sb.AppendLine("END")
          [void]$sb.AppendLine("")
        }
      }

      # Profiles
      if ($profiles -and $profiles.Count -gt 0) {
        [void]$sb.AppendLine("-- Profiles")
        foreach ($p in $profiles) {
          $pn = & $SqlQuote $p.profile_name
          $pd = if ($p.description) { " , @description = N'$(& $SqlQuote $p.description)'" } else { "" }
          [void]$sb.AppendLine("IF NOT EXISTS (SELECT 1 FROM msdb.dbo.sysmail_profile WHERE name = N'$pn')")
          [void]$sb.AppendLine("  EXEC msdb.dbo.sysmail_add_profile_sp @profile_name = N'$pn'$pd;")
        }
        [void]$sb.AppendLine("")
      }

      # Profile-Account mapping
      if ($profileAccounts -and $profileAccounts.Count -gt 0) {
        [void]$sb.AppendLine("-- Profile-Account Mapping")
        foreach ($pa in $profileAccounts) {
          $pn  = & $SqlQuote $pa.profile_name
          $an  = & $SqlQuote $pa.account_name
          $seq = [int]$pa.sequence_number
          [void]$sb.AppendLine("IF NOT EXISTS (SELECT 1 FROM msdb.dbo.sysmail_profileaccount pa
                               JOIN msdb.dbo.sysmail_profile p ON pa.profile_id = p.profile_id
                               JOIN msdb.dbo.sysmail_account a ON pa.account_id = a.account_id
                               WHERE p.name = N'$pn' AND a.name = N'$an')")
          [void]$sb.AppendLine("  EXEC msdb.dbo.sysmail_add_profileaccount_sp @profile_name = N'$pn', @account_name = N'$an', @sequence_number = $seq;")
        }
        [void]$sb.AppendLine("")
      }

      # Principal grants / defaults (from helper SP output)
      if ($principalProfile -and $principalProfile.Count -gt 0) {
        [void]$sb.AppendLine("-- Principal Grants / Defaults")
        foreach ($gp in $principalProfile) {
          $pn  = & $SqlQuote $gp.profile_name
          $pid = [int]$gp.principal_id
          $def = if ($gp.is_default) { 1 } else { 0 }
          [void]$sb.AppendLine("IF NOT EXISTS (SELECT 1 FROM msdb.dbo.sysmail_principalprofile pp
                               JOIN msdb.dbo.sysmail_profile p ON pp.profile_id = p.profile_id
                               WHERE p.name = N'$pn' AND pp.principal_id = $pid)")
          [void]$sb.AppendLine("  EXEC msdb.dbo.sysmail_add_principalprofile_sp @profile_name = N'$pn', @principal_id = $pid, @is_default = $def;")
        }
        [void]$sb.AppendLine("")
      }

      [void]$sb.AppendLine("-- End of Database Mail export")

      # Write file (UTF-8 BOM)
      $content = $sb.ToString()
      if ($PSVersionTable.PSVersion.Major -ge 6) {
        Set-Content -LiteralPath $Filename -Value $content -Encoding utf8BOM
      } else {
        Set-Content -LiteralPath $Filename -Value $content -Encoding UTF8
      }

      Write-LogSafe -Level Info -Message "Exported Database Mail to [$Filename]"
    }
    catch {
      Write-LogSafe -Level Error -Message ("Error exporting Database Mail from [{0}]: {1}" -f $SqlInstance, $_.Exception.Message)
      if ($_.ScriptStackTrace) { Write-LogSafe -Level Debug -Message ("Stack: {0}" -f $_.ScriptStackTrace) }
    }
    finally {
      Write-LogSafe -Level Info -Message "Finished Export-DbaDatabaseMail for instance [$SqlInstance]"
    }
  }
}
