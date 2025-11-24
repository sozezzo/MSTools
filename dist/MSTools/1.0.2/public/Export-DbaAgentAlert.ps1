function Export-DbaAgentAlert {
<#
.SYNOPSIS
    Exports SQL Server Agent Alerts from an instance as CREATE scripts.

.DESCRIPTION
    Retrieves Agent Alerts via dbatools (Get-DbaAgentAlert) and generates T-SQL scripts
    using Export-DbaScript. Output can be:
      - a single combined file (when -FileName is provided),
      - one file per alert in a folder (when -Path is provided),
      - or streamed to the pipeline (no -Path/-FileName).

.PARAMETER SqlInstance
    SQL Server instance (Server\Instance, hostname, FQDN, alias).

.PARAMETER Path
    Destination folder for per-alert files. Created if it does not exist.
    Ignored if -FileName is used.

.PARAMETER FileName
    Full path to a single output file that will contain all alerts' scripts concatenated.

.PARAMETER AgentAlert
    One or more alert names (supports wildcards) to filter which alerts are exported.

.EXAMPLE
    Export-DbaAgentAlert -SqlInstance "prod-sql01" -Path "C:\Backups\AgentAlerts"

.EXAMPLE
    Export-DbaAgentAlert -SqlInstance "prod-sql01" -FileName "C:\Backups\AgentAlerts\alerts.sql"

.EXAMPLE
    Export-DbaAgentAlert -SqlInstance "prod-sql01" -AgentAlert "Severity*", "Error*"

.NOTES
    Author  : Sozezzo Astra
    Version : 1.1
    Requires: dbatools module
#>
    [CmdletBinding(SupportsShouldProcess = $true, ConfirmImpact = 'Low')]
    param(
        [Parameter(Mandatory)]
        [string] $SqlInstance,

        [string] $Path,

        [string] $FileName,

        [string[]] $AgentAlert
    )

    begin {
        function _Sanitize-FileName {
            param([string] $Name)
            $invalid = [System.IO.Path]::GetInvalidFileNameChars() + [char]'/'
            $sb = New-Object System.Text.StringBuilder
            foreach ($ch in $Name.ToCharArray()) {
                if ($invalid -contains $ch) { [void]$sb.Append('_') } else { [void]$sb.Append($ch) }
            }
            $sb.ToString()
        }

        function _Ensure-Directory {
            param([string] $Dir)
            if ([string]::IsNullOrWhiteSpace($Dir)) { return }
            if (-not (Test-Path -LiteralPath $Dir -PathType Container)) {
                if ($PSCmdlet.ShouldProcess($Dir, "Create directory")) {
                    New-Item -ItemType Directory -Path $Dir -Force | Out-Null
                }
            }
        }

        function _WriteUtf8NoBom {
            param([string] $Path, [string[]] $Content)
            # Works on both Windows PowerShell and PowerShell 7
            $enc = New-Object System.Text.UTF8Encoding($false)
            [System.IO.File]::WriteAllLines($Path, $Content, $enc)
        }

        $hasWriteLog = Get-Command -Name Write-Log -ErrorAction SilentlyContinue
        function _logInfo { param($m) if ($hasWriteLog) { Write-Log -Message $m -Level Info } else { Write-Verbose $m } }
        function _logWarn { param($m) if ($hasWriteLog) { Write-Log -Message $m -Level Warning } else { Write-Warning $m } }
        function _logErr  { param($m) if ($hasWriteLog) { Write-Log -Message $m -Level Error } else { Write-Error $m } }

        _logInfo "Starting Export-DbaAgentAlert for instance [$SqlInstance]"

        if ($FileName) {
            $targetDir = [System.IO.Path]::GetDirectoryName($FileName)
            if (-not $targetDir) {
                throw "Invalid -FileName. Provide a full path including the file name."
            }
            _Ensure-Directory -Dir $targetDir
        } elseif ($Path) {
            _Ensure-Directory -Dir $Path
        }
    }

    process {
        try {
            _logInfo "Retrieving Agent Alerts from [$SqlInstance]"

            $getParams = @{
                SqlInstance = $SqlInstance
                ErrorAction = 'Stop'
            }
            if ($AgentAlert -and $AgentAlert.Count -gt 0) {
                $getParams['Alert'] = $AgentAlert
                _logInfo "Filter: alerts = $($AgentAlert -join ', ')"
            }

            $alerts = Get-DbaAgentAlert @getParams | Sort-Object Name

            if (-not $alerts) {
                _logWarn "No Agent Alerts found on [$SqlInstance] for the given filters."
                return
            }

            _logInfo ("Found {0} alert(s) on [{1}]" -f $alerts.Count, $SqlInstance)

            if ($FileName) {
                if ($PSCmdlet.ShouldProcess($FileName, "Export {0} alert(s) to single file" -f $alerts.Count)) {
                    $combined = foreach ($a in $alerts) {
                        "/* ===== Agent Alert: {0} (Instance: {1}) ===== */" -f $a.Name, $SqlInstance
                        Export-DbaScript -InputObject $a -Passthru -NoPrefix
                        ""
                    }
                    _WriteUtf8NoBom -Path $FileName -Content $combined
                    _logInfo "Exported alerts to [$FileName]"
                }
            } elseif ($Path) {
                $nameCount = @{}
                foreach ($a in $alerts) {
                    $base = _Sanitize-FileName $a.Name
                    if (-not $nameCount.ContainsKey($base)) { $nameCount[$base] = 0 } else { $nameCount[$base]++ }
                    $suffix = if ($nameCount[$base] -gt 0) { "({0})" -f $nameCount[$base] } else { "" }
                    $datetime = Get-Date -Format 'yyyy-MM-dd_HHmmss'
                    $outFile = Join-Path -Path $Path -ChildPath ("{3}.{0}{1}.{2}.agentalert.sql" -f $base, $suffix, $datetime, $SqlInstance)

                    if ($PSCmdlet.ShouldProcess($outFile, "Export alert [$($a.Name)]")) {
                        $content = @(
                            "/* ===== Agent Alert: {0} (Instance: {1}) ===== */" -f $a.Name, $SqlInstance
                            Export-DbaScript -InputObject $a -Passthru -NoPrefix
                            ""
                        )
                        _WriteUtf8NoBom -Path $outFile -Content $content
                        _logInfo "Exported [$($a.Name)] to [$outFile]"
                    }
                }
            } else {
                foreach ($a in $alerts) {
                    "/* ===== Agent Alert: {0} (Instance: {1}) ===== */" -f $a.Name, $SqlInstance
                    Export-DbaScript -InputObject $a -Passthru -NoPrefix
                    ""
                }
            }
        }
        catch {
            _logErr ("Error exporting Agent Alerts from [{0}]: {1}" -f $SqlInstance, $_.Exception.Message)
            throw
        }
        finally {
            _logInfo "Finished Export-DbaAgentAlert for instance [$SqlInstance]"
        }
    }
}