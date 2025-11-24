function Export-DbaJobCategoryScripts {
<#
.SYNOPSIS
    Exports T-SQL to (re)create SQL Agent categories (Jobs, Alerts, Operators) into a single .sql file.

.DESCRIPTION
    Reads msdb.dbo.syscategories on the target instance and writes idempotent
    sp_add_category commands to create each category if missing.

.PARAMETER SqlInstance
    Target SQL Server instance (name, hostname, or connection string).

.PARAMETER FileName
    Full path to the output .sql file. If provided, this is used as-is.

.PARAMETER Path
    Folder where the script will be saved as 'category.sql' when -FileName is not provided.

.PARAMETER Category
    Optional list of category names to include (exact match). If omitted, exports all.

.EXAMPLE
    Export-DbaJobCategoryScripts -SqlInstance "SQL01" -Path "C:\out"

.EXAMPLE
    Export-DbaJobCategoryScripts -SqlInstance "SQL01" -FileName "C:\out\category.ALL.sql"

.EXAMPLE
    Export-DbaJobCategoryScripts -SqlInstance "SQL01" -Path "C:\out" -Category "Database Maintenance","Backups"

.NOTES
    Author: Sozezzo Astra
    Version: 1.1
#>
    [CmdletBinding(SupportsShouldProcess)]
    param(
        [Parameter(Mandatory)][string] $SqlInstance,
        [string] $FileName,
        [string] $Path,
        [string[]] $Category
    )

    try {
        # -------- Resolve output target (FileName vs Path) --------
        if ([string]::IsNullOrWhiteSpace($FileName)) {
            if ([string]::IsNullOrWhiteSpace($Path)) {
                $msg = "You must provide either -FileName (full path to file) OR -Path (directory to save 'category.sql')."
                Write-Log -Message $msg -Level Error
                throw $msg
            }
            # Ensure directory exists for Path and compose default filename
            if (-not (Test-Path -LiteralPath $Path)) {
                New-Item -ItemType Directory -Path $Path -Force | Out-Null
            }
            $FileName = Join-Path $Path 'category.sql'
        } else {
            # Ensure directory exists for FileName's parent
            $dir = Split-Path -Path $FileName -Parent
            if ($dir -and -not (Test-Path -LiteralPath $dir)) {
                New-Item -ItemType Directory -Path $dir -Force | Out-Null
            }
        }

        Write-Log -Message "Reading categories from [$SqlInstance]..." -Level Info

        # Pull raw categories directly from msdb for reliable class/type
        $tsql = @"
SELECT name,
       category_class,      -- 1=JOB, 2=ALERT, 3=OPERATOR
       category_type        -- jobs only: 1=LOCAL, 2=MULTI-SERVER
FROM   msdb.dbo.syscategories
WHERE  category_class IN (1,2,3)
ORDER BY category_class, name;
"@

        $rows = Invoke-DbaQuery -SqlInstance $SqlInstance -Database msdb -Query $tsql -EnableException:$true

        if ($Category) {
            $rows = $rows | Where-Object { $_.name -in $Category }
        }

        if (-not $rows -or $rows.Count -eq 0) {
            Write-Log -Message "No categories matched filters. Nothing to export." -Level Warning
            return
        }

        $clsMap  = @{ 1 = 'JOB'; 2 = 'ALERT'; 3 = 'OPERATOR' }
        $typeMap = @{ 1 = 'LOCAL'; 2 = 'MULTI-SERVER' }  # only for JOB

        $header = @"
-- ===========================================
-- Category export for: $SqlInstance
-- Generated: $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')
-- Note: Idempotent creation using msdb.dbo.sp_add_category
-- ===========================================
USE [msdb];
GO

"@

        $chunks = New-Object System.Collections.Generic.List[string]
        $chunks.Add($header) | Out-Null

        foreach ($r in $rows) {
            $name     = [string]$r.name
            $clsNum   = [int]$r.category_class
            $typeNum  = if ($r.category_type -ne $null) { [int]$r.category_type } else { $null }

            $class = $clsMap[$clsNum]
            if (-not $class) { continue }

            # Escape single quotes for T-SQL
            $nameSql = $name -replace '''', ''''''

            # Idempotent check
            $check = "IF NOT EXISTS (SELECT 1 FROM msdb.dbo.syscategories WHERE name = N'$nameSql' AND category_class = $clsNum)"
            if ($class -eq 'JOB') {
                $typeLabel = $typeMap[$typeNum]
                if (-not $typeLabel) { $typeLabel = 'LOCAL' } # default sane fallback
                $cmd = "    EXEC msdb.dbo.sp_add_category @class = N'JOB', @type = N'$typeLabel', @name = N'$nameSql';"
            } elseif ($class -eq 'ALERT') {
                $cmd = "    EXEC msdb.dbo.sp_add_category @class = N'ALERT', @name = N'$nameSql';"
            } else { # OPERATOR
                $cmd = "    EXEC msdb.dbo.sp_add_category @class = N'OPERATOR', @name = N'$nameSql';"
            }

            $block = @"
-- Category: $name  (Class=$class)
$check
BEGIN
$cmd
END
GO

"@
            $chunks.Add($block) | Out-Null
            Write-Log -Message "Prepared category [$name] (Class=$class)" -Level Debug
        }

        $final = $chunks -join ""

        if ($PSCmdlet.ShouldProcess($FileName, "Write category script")) {
            Set-Content -LiteralPath $FileName -Value $final -Encoding UTF8
            Write-Log -Message "Saved category script to: $FileName" -Level Info
        }
    }
    catch {
        Write-Log -Message "Error in Export-DbaJobCategoryScripts: $($_.Exception.Message)" -Level Error
    }
}
