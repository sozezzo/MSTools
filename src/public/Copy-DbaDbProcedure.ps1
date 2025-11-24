function Copy-DbaDbProcedure {
    <#
    .SYNOPSIS
        Copies user stored procedures between databases/servers.

    .DESCRIPTION
        Reads procedure definitions from the source database (default master)
        and creates them on the destination database.

        - Without -Force: only procedures that do not exist on Destination are created.
        - With -Force: existing procedures on Destination are dropped and recreated.
        - With -Force -CompareFirst: existing procedures are only replaced if the
          source and destination definitions are different.

    .PARAMETER Source
        Source SQL instance (Server\Instance, hostname, FQDN, alias).

    .PARAMETER Destination
        Destination SQL instance.

    .PARAMETER Database
        Database name to copy procedures from/to. Defaults to 'master'.

    .PARAMETER Procedure
        One or more procedure names to copy. Supports PowerShell wildcards.
        If omitted, all user procedures (non-MS shipped) are copied.

    .PARAMETER Force
        Overwrite existing procedures on the destination.

    .PARAMETER CompareFirst
        When used with -Force, compares source and destination definitions first.
        If definitions are equal, the procedure is not copied.

    .EXAMPLE
        Copy-DbaDbProcedure -Source "PROD-SQL01" -Destination "TEST-SQL01"

        Copies all user procedures from master on PROD-SQL01 to master on TEST-SQL01.
        Existing procedures on TEST-SQL01 are left untouched (no overwrite).

    .EXAMPLE
        Copy-DbaDbProcedure -Source "PROD-SQL01" -Destination "TEST-SQL01" -Database "MyDB" -Procedure "usp_*" -Force

        Copies/overwrites all procedures whose name matches 'usp_*' from MyDB on PROD-SQL01
        to MyDB on TEST-SQL01.

    .EXAMPLE
        Copy-DbaDbProcedure -Source "PROD-SQL01" -Destination "TEST-SQL01" -Database "MyDB" -Procedure "usp_*" -Force -CompareFirst

        Same as above, but existing procedures on TEST-SQL01 are only replaced if the
        definitions differ between source and destination.
    #>
    [CmdletBinding(SupportsShouldProcess = $true, ConfirmImpact = 'Medium')]
    param(
        [Parameter(Mandatory = $true)]
        [string]$Source,

        [Parameter(Mandatory = $true)]
        [string]$Destination,

        [string]$Database = 'master',

        [string[]]$Procedure,

        [switch]$Force,

        [switch]$CompareFirst
    )

    Write-Log -Level Info  -Message "Copy-DbaDbProcedure started. Source=[$Source], Destination=[$Destination], Database=[$Database], Force=[$Force], CompareFirst=[$CompareFirst], Procedures=[${($Procedure -join ', ')}]"
    Write-Log -Level Debug -Message "Copy-DbaDbProcedure parameters: $(($PSBoundParameters.GetEnumerator() | ForEach-Object { "$($_.Key)=$($_.Value)" }) -join '; ')"

    # Get source procedures + definitions
    $srcQuery = @"
SELECT
    p.name              AS ProcName,
    s.name              AS SchemaName,
    m.definition        AS Definition
FROM sys.procedures AS p
JOIN sys.schemas   AS s ON p.schema_id = s.schema_id
JOIN sys.sql_modules AS m ON p.object_id = m.object_id
WHERE
    ISNULL(OBJECTPROPERTY(p.object_id, 'IsMsShipped'), 0) = 0;
"@

    Write-Log -Level Debug -Message "Querying source procedures from [$Source]. Database=[$Database]."
    $srcProcs = Invoke-DbaQuery -SqlInstance $Source -Database $Database -Query $srcQuery

    if (-not $srcProcs) {
        Write-Log -Level Warning -Message "No user procedures found on source [$Source] in database [$Database]. Nothing to copy."
        return
    }

    # Optional filter by -Procedure (PowerShell wildcards on ProcName)
    if ($Procedure) {
        Write-Log -Level Debug -Message "Filtering source procedures by patterns: $($Procedure -join ', ')"

        $patterns = $Procedure
        $srcProcs = $srcProcs | Where-Object {
            $procName = $_.ProcName
            $patterns | Where-Object { $procName -like $_ } | ForEach-Object { $true } | Measure-Object | Select-Object -ExpandProperty Count
        } | Select-Object -Unique
    }

    if (-not $srcProcs) {
        Write-Log -Level Warning -Message "After filtering by -Procedure, no source procedures remain. Nothing to copy."
        return
    }

    Write-Log -Level Info -Message ("Found {0} source procedures to consider for copy." -f $srcProcs.Count)

    # Get destination existing procedures + definitions
    $destQuery = @"
SELECT
    p.name              AS ProcName,
    s.name              AS SchemaName,
    m.definition        AS Definition
FROM sys.procedures AS p
JOIN sys.schemas   AS s ON p.schema_id = s.schema_id
JOIN sys.sql_modules AS m ON p.object_id = m.object_id
WHERE
    ISNULL(OBJECTPROPERTY(p.object_id, 'IsMsShipped'), 0) = 0;
"@

    Write-Log -Level Debug -Message "Querying destination procedures from [$Destination]. Database=[$Database]."
    $destProcs = Invoke-DbaQuery -SqlInstance $Destination -Database $Database -Query $destQuery

    # Build lookups: name → definition
    $destLookup = @{}
    $destDefLookup = @{}
    foreach ($d in $destProcs) {
        $key = ($d.SchemaName + '.' + $d.ProcName).ToLowerInvariant()
        $destLookup[$key] = $true
        $destDefLookup[$key] = $d.Definition
    }

    $results = @()
    $created = 0
    $replaced = 0
    $skippedExisting = 0
    $skippedSame = 0

    foreach ($p in $srcProcs) {
        $fullName = "$($p.SchemaName).$($p.ProcName)"
        $key = $fullName.ToLowerInvariant()
        $existsOnDest = $destLookup.ContainsKey($key)

        Write-Log -Level Debug -Message "Processing procedure [$fullName]. ExistsOnDestination=[$existsOnDest]."

        if ($existsOnDest -and -not $Force) {
            # Only copy "new" procedures when Force is not specified
            Write-Log -Level Info -Message "Skipping existing procedure [$fullName] on destination [$Destination] (Force not specified)."
            $skippedExisting++
            $results += [pscustomobject]@{
                Source       = $Source
                Destination  = $Destination
                Database     = $Database
                Procedure    = $fullName
                Action       = 'SkippedExisting'
                ForceApplied = $false
                Reason       = 'ExistsOnDestination_NoForce'
            }
            continue
        }

        # If exists and CompareFirst+Force: compare definitions, skip if identical
        if ($existsOnDest -and $Force -and $CompareFirst) {
            $srcDef  = ($p.Definition  -as [string])
            $destDef = ($destDefLookup[$key] -as [string])

            # Simple normalization: trim and normalize line endings
            $normalize = {
                param($text)
                if ($null -eq $text) { return $null }
                return ($text -replace "`r`n", "`n").Trim()
            }

            $srcNorm  = & $normalize $srcDef
            $destNorm = & $normalize $destDef

            if ($srcNorm -eq $destNorm) {
                Write-Log -Level Info -Message "Skipping procedure [$fullName] on destination [$Destination]; definitions are identical (CompareFirst)."
                $skippedSame++
                $results += [pscustomobject]@{
                    Source       = $Source
                    Destination  = $Destination
                    Database     = $Database
                    Procedure    = $fullName
                    Action       = 'SkippedSameDefinition'
                    ForceApplied = $true
                    Reason       = 'CompareFirst_DefinitionsEqual'
                }
                continue
            }
            else {
                Write-Log -Level Debug -Message "Definitions differ for [$fullName]; will replace (Force + CompareFirst)."
            }
        }

        # Build T-SQL batch
        $batch = if ($existsOnDest -and $Force) {
            @"
IF OBJECT_ID(N'[$($p.SchemaName)].[$($p.ProcName)]', 'P') IS NOT NULL
    DROP PROCEDURE [$($p.SchemaName)].[$($p.ProcName)];
$($p.Definition)
"@
        }
        else {
            # New proc only
            $p.Definition
        }

        $action = if ($existsOnDest) { if ($Force) { 'Replaced' } else { 'Created' } } else { 'Created' }

        if ($PSCmdlet.ShouldProcess("$Destination / $Database", "Create or replace procedure [$fullName] ($action)")) {
            Write-Log -Level Info -Message "Executing T-SQL batch for procedure [$fullName] on destination [$Destination] (Action=$action)."

            Invoke-DbaQuery -SqlInstance $Destination -Database $Database -Query $batch

            if ($action -eq 'Created') { $created++ } else { $replaced++ }

            $results += [pscustomobject]@{
                Source       = $Source
                Destination  = $Destination
                Database     = $Database
                Procedure    = $fullName
                Action       = $action
                ForceApplied = [bool]$Force
                Reason       = if ($existsOnDest) { 'ExistsOnDestination_Overwritten' } else { 'NewOnDestination' }
            }
        }
        else {
            Write-Log -Level Info -Message "WhatIf/Confirm prevented execution for procedure [$fullName]."
            $results += [pscustomobject]@{
                Source       = $Source
                Destination  = $Destination
                Database     = $Database
                Procedure    = $fullName
                Action       = 'NotExecuted'
                ForceApplied = [bool]$Force
                Reason       = 'ShouldProcessDenied'
            }
        }
    }

    Write-Log -Level Info -Message ("Copy-DbaDbProcedure finished. Created={0}, Replaced={1}, SkippedExisting={2}, SkippedSameDefinition={3}" -f $created, $replaced, $skippedExisting, $skippedSame)

    # Output a small summary per procedure
    $results
}

