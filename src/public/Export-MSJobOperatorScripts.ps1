function Export-MSJobOperatorScripts {
<#
.SYNOPSIS
    Export SQL Agent Job Operators from an instance to a .sql file.

.DESCRIPTION
    Exports one or all SQL Agent Operators from the specified SQL Server instance into a SQL script file.
    Uses dbatools cmdlets internally.

.PARAMETER SqlInstance
    SQL Server instance name or connection string.

.PARAMETER Path
    Folder where the script file will be saved.
    If -FileName is not provided, defaults to "$Path\operators.sql".

.PARAMETER FileName
    Full path and filename for the exported script.
    If provided, overrides -Path.

.PARAMETER Operator
    Optional filter to export only the specified Operator name(s).
    Supports exact names or wildcards (e.g., 'DBA*').

.EXAMPLE
    Export-MSJobOperatorScripts -SqlInstance "SQL01" -Path "C:\Backup\Jobs"

.EXAMPLE
    Export-MSJobOperatorScripts -SqlInstance "SQL01" -FileName "C:\Backup\operators.sql" -Operator "DBA_Alert","Ops_*"

.NOTES
    Author: Sozezzo Astra
    Version: 1.1
#>
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][string] $SqlInstance,
        [string] $Path,
        [string] $FileName,
        [string[]] $Operator
    )

    try {
        Write-MSLog -Message "Starting Export-MSJobOperatorScripts for instance [$SqlInstance]" -Level Info

        # Resolve output target
        if (-not $FileName) {
            if (-not $Path) {
                throw "You must specify either -Path or -FileName."
            }
            if (-not (Test-Path -LiteralPath $Path)) {
                Write-MSLog -Message "Path not found, creating: $Path" -Level Info
                New-Item -ItemType Directory -Path $Path -Force | Out-Null
            }
            $FileName = Join-Path -Path $Path -ChildPath 'operators.sql'
        }

        Write-MSLog -Message "Retrieving operators from [$SqlInstance]" -Level Debug
        $operators = Get-DbaAgentOperator -SqlInstance $SqlInstance -ErrorAction Stop

        if ($Operator) {
            # Allow exact or wildcard matches, case-insensitive
            $patterns = @($Operator) | Where-Object { $_ -ne $null -and $_ -ne '' }
            if ($patterns.Count -gt 0) {
                $operators = $operators | Where-Object {
                    $name = $_.Name
                    foreach ($pat in $patterns) {
                        if ($name -like $pat) { return $true }
                    }
                    return $false
                }
                Write-MSLog -Message "Filtered operators: $($operators.Name -join ', ')" -Level Debug
            }
        }

        if (-not $operators -or $operators.Count -eq 0) {
            Write-MSLog -Message "No operators found to export on [$SqlInstance]." -Level Warning
            return
        }

        Write-MSLog -Message "Exporting operators to $FileName" -Level Info
        $script = Export-DbaScript -InputObject $operators -Passthru -ErrorAction Stop

        # Ensure we always write UTF-8 (PS5 uses BOM)
        Set-Content -Path $FileName -Value $script -Encoding UTF8

        Write-MSLog -Message "Export completed successfully: $FileName" -Level Info
    }
    catch {
        Write-MSLog -Message "Error exporting job operators from [$SqlInstance]: $($_.Exception.Message)" -Level Error
    }
}