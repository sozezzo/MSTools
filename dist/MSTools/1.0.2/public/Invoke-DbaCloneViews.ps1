function Invoke-DbaCloneViews {
<#
.SYNOPSIS
    Scripts and deploys Views from a source database to a destination database.

.DESCRIPTION
    Exports views from the source DB (optionally filtered by a provided list)
    and deploys them to the destination DB. By default uses SMO's IncludeIfNotExists
    wrapper; optionally emits SQL Server 2016+ syntax `DROP VIEW IF EXISTS` before CREATE
    to avoid long IF/OBJECT_ID guards.

.PARAMETER SourceInstance
    Source SQL Server instance (e.g. 'SQL01' or 'SQL01\INST1').

.PARAMETER SourceDatabase
    Source database name.

.PARAMETER DestInstance
    Destination SQL Server instance.

.PARAMETER DestDatabase
    Destination database name.

.PARAMETER ScriptPath
    Output .sql file for the scripted views. Overwritten if exists.

.PARAMETER Views
    Optional list of views to copy. Accepts two-part names (schema.object) or bare names,
    supports wildcards (e.g., 'dbo.Sales*', 'vw_*'). If omitted, all user views are processed.

.PARAMETER UseDropIfExists
    When set, generates `DROP VIEW IF EXISTS [schema].[name];` before each CREATE VIEW.
    Requires destination version >= SQL Server 2016 (13.x). Otherwise falls back to
    SMO IncludeIfNotExists wrapper.

.PARAMETER MaxPasses
    Maximum retry passes for Invoke-DbaExecuteBatchWithRetry. Default: 10.

.PARAMETER LogFileName
    Optional log file path; if omitted, uses $Global:WriteLog_LogFileName when present.

.EXAMPLE
    Invoke-DbaCloneViews -SourceInstance SQL01 -SourceDatabase AppDB `
                         -DestInstance SQL02 -DestDatabase AppDB_Stage `
                         -ScriptPath 'C:\out\AppDB-views.sql'

.EXAMPLE
    # Only some views, dropping if exist on destination
    Invoke-DbaCloneViews -SourceInstance SQL01 -SourceDatabase Sales `
                         -DestInstance SQL02 -DestDatabase Sales_QA `
                         -ScriptPath 'D:\drop\Sales-views.sql' `
                         -Views 'dbo.vw_*','reporting.SalesBy*' `
                         -UseDropIfExists

.NOTES
    - Requires dbatools.
    - Respects -WhatIf / -Confirm.
#>
    [CmdletBinding(SupportsShouldProcess, ConfirmImpact='Medium')]
    param(
        [Parameter(Mandatory)][string]$SourceInstance,
        [Parameter(Mandatory)][string]$SourceDatabase,
        [Parameter(Mandatory)][string]$DestInstance,
        [Parameter(Mandatory)][string]$DestDatabase,

        [Parameter(Mandatory)][string]$ScriptPath,

        [string[]]$Views,
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

        # Resolve destination version for DROP IF EXISTS support
        $destServer = $null
        try {
            $destServer = Connect-DbaInstance -SqlInstance $DestInstance -EnableException
            $destMajor  = $destServer.Information.Version.Major
            $dropIfExistsSupported = ($destMajor -ge 13) # SQL Server 2016+
        } catch {
            Write-Log -Message "WARN: Could not determine destination version: $($_.Exception.Message). Assuming no DROP IF EXISTS." -Level Warning
            $dropIfExistsSupported = $false
        }

        if ($UseDropIfExists -and -not $dropIfExistsSupported) {
            Write-Log -Message "Destination does not support DROP IF EXISTS (requires SQL 2016+). Falling back to IncludeIfNotExists." -Level Warning
            $UseDropIfExists = $false
        }

        # Scripting options
        $opt = New-DbaScriptingOption
        $opt.NoCollation        = $true
        $opt.IncludeIfNotExists = -not $UseDropIfExists
        $opt.AnsiFile           = $true
        $opt.ScriptSchema       = $true
        $opt.DriAll             = $false
        $opt.Indexes            = $false
        $opt.Triggers           = $true
    }

    process {
        try {
            Write-Log -Message "Collecting views from [$SourceInstance].[$SourceDatabase]." -Level Info

            $allViews = Get-DbaDbView -SqlInstance $SourceInstance -Database $SourceDatabase -ExcludeSystemView -EnableException

            # Normalize a function to two-part name
            function _twopart($v) {
                if ($v -match '^[\[\]A-Za-z0-9_]+\.[\[\]A-Za-z0-9_]+$') { return $v }
                else { return "*.$v" } # treat bare names as wildcard across schemas
            }

            $targetViews = $allViews
            if ($Views -and $Views.Count) {
                $patterns = $Views | ForEach-Object { _twopart $_ }
                $targetViews = $allViews | Where-Object {
                    $two = "$($_.Schema).$($_.Name)"
                    # Allow wildcard matching on two-part patterns
                    foreach ($p in $patterns) {
                        if ($two -like $p) { return $true }
                    }
                    return $false
                }
            }

            if (-not $targetViews -or $targetViews.Count -eq 0) {
                Write-Log -Message "No views matched the provided filter." -Level Warning
                return
            }

            Write-Log -Message ("Views selected: " + ($targetViews | ForEach-Object { "$($_.Schema).$($_.Name)" } | Sort-Object | Out-String).Trim()) -Level Info

            # Start fresh output file
            if (Test-Path $ScriptPath) { Remove-Item -LiteralPath $ScriptPath -Force }
            New-Item -ItemType File -Path $ScriptPath -Force | Out-Null

            if ($UseDropIfExists) {
                Write-Log -Message "Scripting with DROP VIEW IF EXISTS prelude per object." -Level Info
                # Script each view -> prepend DROP IF EXISTS -> append GO
                foreach ($v in $targetViews) {
                    $two = "[{0}].[{1}]" -f $v.Schema, $v.Name
                    $drop = "IF EXISTS (SELECT 1 FROM sys.views WHERE object_id = OBJECT_ID(N'$two')) DROP VIEW $two; -- safety on old compat" + [Environment]::NewLine
                    $drop2 = "DROP VIEW IF EXISTS $two;" + [Environment]::NewLine
                    # Favor native syntax; keep classic guard as comment/safety for older compat levels
                    $header = "-- View: $two" + [Environment]::NewLine
                    $pre    = $drop2

                    $body = ($v | Export-DbaScript -ScriptingOptionsObject $opt -Passthru -EnableException -Confirm:$false) -join [Environment]::NewLine
                    Add-Content -LiteralPath $ScriptPath -Value ($header + $pre + $body + [Environment]::NewLine + "GO" + [Environment]::NewLine)
                }
            } else {
                Write-Log -Message "Scripting using IncludeIfNotExists wrapper (SMO)." -Level Info
                $targetViews | Export-DbaScript -FilePath $ScriptPath -Append:$true -ScriptingOptionsObject $opt -EnableException -Confirm:$false *>> $LogFileName
            }

            if ($PSCmdlet.ShouldProcess("$DestInstance / $DestDatabase", "Deploy views")) {
                Write-Log -Message "Deploying views into [$DestInstance].[$DestDatabase]." -Level Warning
                [void](Invoke-DbaExecuteBatchWithRetry -SqlInstance $DestInstance -Database $DestDatabase -FilePath $ScriptPath -MaxPasses $MaxPasses)
                Write-Log -Message "Views deployment completed." -Level Warning
            } else {
                Write-Log -Message "Scripting completed (no deployment due to -WhatIf / -Confirm choice)." -Level Info
            }
        }
        catch {
            Write-Log -Message ("ERROR (Invoke-DbaCloneViews): " + $_.Exception.Message) -Level Error
            throw
        }
    }
}
