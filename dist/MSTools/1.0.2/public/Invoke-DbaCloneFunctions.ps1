function Invoke-DbaCloneFunctions {
<#
.SYNOPSIS
    Scripts and deploys user-defined functions (UDFs) from a source database to a destination database.

.DESCRIPTION
    Exports UDFs from the source DB (optionally filtered by a provided list) and deploys them to the
    destination DB. You can prepend native `DROP FUNCTION IF EXISTS` for SQL Server 2016+ to simplify redeploys,
    or fall back to SMO's IncludeIfNotExists wrapper.

.PARAMETER SourceInstance
    Source SQL Server instance (e.g. 'SQL01' or 'SQL01\INST1').

.PARAMETER SourceDatabase
    Source database name.

.PARAMETER DestInstance
    Destination SQL Server instance.

.PARAMETER DestDatabase
    Destination database name.

.PARAMETER ScriptPath
    Output .sql file for the scripted functions. Overwritten if exists.

.PARAMETER Functions
    Optional list of functions to copy. Accepts two-part names (schema.object) or bare names,
    supports wildcards (e.g., 'dbo.ufn_*', 'fn_*'). If omitted, all user UDFs are processed.

.PARAMETER UseDropIfExists
    When set, generates `DROP FUNCTION IF EXISTS [schema].[name];` before each CREATE FUNCTION.
    Requires destination version >= SQL Server 2016 (13.x). Otherwise falls back to IncludeIfNotExists.

.PARAMETER MaxPasses
    Maximum retry passes for Invoke-DbaExecuteBatchWithRetry. Default: 10.

.PARAMETER LogFileName
    Optional log file path; if omitted, uses $Global:WriteLog_LogFileName when present.

.EXAMPLE
    Invoke-DbaCloneFunctions -SourceInstance SQL01 -SourceDatabase AppDB `
                             -DestInstance SQL02 -DestDatabase AppDB_Stage `
                             -ScriptPath 'C:\out\AppDB-functions.sql'

.EXAMPLE
    Invoke-DbaCloneFunctions -SourceInstance SQL01 -SourceDatabase Sales `
                             -DestInstance SQL02 -DestDatabase Sales_QA `
                             -ScriptPath 'D:\drop\Sales-functions.sql' `
                             -Functions 'dbo.ufn_*','reporting.ufnTotal*' `
                             -UseDropIfExists
#>
    [CmdletBinding(SupportsShouldProcess, ConfirmImpact='Medium')]
    param(
        [Parameter(Mandatory)][string]$SourceInstance,
        [Parameter(Mandatory)][string]$SourceDatabase,
        [Parameter(Mandatory)][string]$DestInstance,
        [Parameter(Mandatory)][string]$DestDatabase,

        [Parameter(Mandatory)][string]$ScriptPath,

        [string[]]$Functions,
        [switch]$UseDropIfExists,
        [int]$MaxPasses = 10,
        [string]$LogFileName
    )

    begin {
        if (-not $LogFileName) { $LogFileName = $Global:WriteLog_LogFileName }

        # Ensure output folder exists
        $dir = Split-Path -Path $ScriptPath -Parent
        if ($dir -and -not (Test-Path -LiteralPath $dir)) {
            New-Item -ItemType Directory -Force -Path $dir | Out-Null
        }

        # Destination version (for DROP IF EXISTS)
        $dropIfExistsSupported = $false
        try {
            $destServer = Connect-DbaInstance -SqlInstance $DestInstance -EnableException
            $dropIfExistsSupported = ($destServer.Information.Version.Major -ge 13) # SQL 2016+
        } catch {
            Write-Log -Message "WARN: Could not determine destination version: $($_.Exception.Message). Assuming no DROP IF EXISTS." -Level Warning
        }

        if ($UseDropIfExists -and -not $dropIfExistsSupported) {
            Write-Log -Message "Destination does not support DROP IF EXISTS (requires SQL 2016+). Falling back to IncludeIfNotExists." -Level Warning
            $UseDropIfExists = $false
        }

        # Scripting options for UDFs
        $opt = New-DbaScriptingOption
        $opt.NoCollation        = $true
        $opt.IncludeIfNotExists = -not $UseDropIfExists
        $opt.AnsiFile           = $true
        $opt.ScriptSchema       = $true
        $opt.DriAll             = $false
        $opt.Indexes            = $false
        $opt.Triggers           = $false
    }

    process {
        try {
            Write-Log -Message "Collecting UDFs from [$SourceInstance].[$SourceDatabase]." -Level Info

            $allUdfs = Get-DbaDbUdf -SqlInstance $SourceInstance -Database $SourceDatabase -ExcludeSystemUdf -EnableException

            # Normalize: ensure two-part matching with wildcard support
            function _tp($n) {
                if ($n -match '^[\[\]A-Za-z0-9_]+\.[\[\]A-Za-z0-9_]+$') { $n } else { "*.$n" }
            }

            $targetUdfs = $allUdfs
            if ($Functions -and $Functions.Count) {
                $patterns = $Functions | ForEach-Object { _tp $_ }
                $targetUdfs = $allUdfs | Where-Object {
                    $two = "$($_.Schema).$($_.Name)"
                    foreach ($p in $patterns) { if ($two -like $p) { return $true } }
                    return $false
                }
            }

            if (-not $targetUdfs -or $targetUdfs.Count -eq 0) {
                Write-Log -Message "No UDFs matched the provided filter." -Level Warning
                return
            }

            Write-Log -Message ("UDFs selected: " + ($targetUdfs | ForEach-Object { "$($_.Schema).$($_.Name)" } | Sort-Object | Out-String).Trim()) -Level Info

            # Start fresh output file
            if (Test-Path $ScriptPath) { Remove-Item -LiteralPath $ScriptPath -Force }
            New-Item -ItemType File -Path $ScriptPath -Force | Out-Null

            if ($UseDropIfExists) {
                Write-Log -Message "Scripting with DROP FUNCTION IF EXISTS prelude per object." -Level Info
                foreach ($f in $targetUdfs) {
                    $two = "[{0}].[{1}]" -f $f.Schema, $f.Name
                    $header = "-- Function: $two" + [Environment]::NewLine
                    $drop   = "DROP FUNCTION IF EXISTS $two;" + [Environment]::NewLine

                    $body = ($f | Export-DbaScript -ScriptingOptionsObject $opt -Passthru -EnableException -Confirm:$false) -join [Environment]::NewLine
                    Add-Content -LiteralPath $ScriptPath -Value ($header + $drop + $body + [Environment]::NewLine + "GO" + [Environment]::NewLine)
                }
            } else {
                Write-Log -Message "Scripting using IncludeIfNotExists wrapper (SMO)." -Level Info
                $targetUdfs | Export-DbaScript -FilePath $ScriptPath -Append:$true -ScriptingOptionsObject $opt -EnableException -Confirm:$false *>> $LogFileName
            }

            if ($PSCmdlet.ShouldProcess("$DestInstance / $DestDatabase", "Deploy functions")) {
                Write-Log -Message "Deploying functions into [$DestInstance].[$DestDatabase]." -Level Warning
                [void](Invoke-DbaExecuteBatchWithRetry -SqlInstance $DestInstance -Database $DestDatabase -FilePath $ScriptPath -MaxPasses $MaxPasses)
                Write-Log -Message "Functions deployment completed." -Level Warning
            } else {
                Write-Log -Message "Scripting completed (no deployment due to -WhatIf / -Confirm choice)." -Level Info
            }
        }
        catch {
            Write-Log -Message ("ERROR (Invoke-DbaCloneFunctions): " + $_.Exception.Message) -Level Error
            throw
        }
    }
}
