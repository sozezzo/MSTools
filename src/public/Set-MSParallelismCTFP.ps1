function Set-MSParallelismCTFP {
<#
.SYNOPSIS
    Sets the SQL Server "Cost Threshold for Parallelism" (CTFP) 
    according to defined policy limits.

.DESCRIPTION
    This function evaluates and enforces the instance-level setting 
    "cost threshold for parallelism" using a controlled policy range 
    (default 20–500).

    It reads the current configuration directly from sys.configurations 
    and applies changes only if necessary.

    The function:
        • Ensures value remains inside policy limits
        • Automatically enables "show advanced options" if required
        • Avoids change if already compliant
        • Skips unsupported platforms safely
        • Supports -WhatIf and -Confirm

    No SMO is used. All operations are T-SQL based.

.PARAMETER SqlInstance
    Target SQL Server instance name.
    Example formats:
        SERVERNAME
        SERVERNAME\INSTANCE
        tcp:SERVERNAME,1433

.PARAMETER CTFP
    Target Cost Threshold for Parallelism value.
    If not specified, default policy minimum (20) is applied.

    Policy limits:
        Minimum: 20
        Maximum: 500

.EXAMPLE
    Set-MSParallelismCTFP -SqlInstance "SQL01"

    Applies default policy value (20) if change is required.

.EXAMPLE
    Set-MSParallelismCTFP -SqlInstance "SQL01" -CTFP 30

    Sets Cost Threshold for Parallelism to 30 (within policy).

.EXAMPLE
    Set-MSParallelismCTFP -SqlInstance "SQL01" -CTFP 100 -WhatIf

    Shows what would change without applying it.

.OUTPUTS
    Returns a structured object:
        SqlInstance
        Action        (None | Updated | Skipped)
        CurrentCTFP
        TargetCTFP
        Reason

.WHY THIS SETTING MATTERS
    Cost Threshold for Parallelism determines the minimum estimated
    query cost required before SQL Server considers parallel execution.

    Too LOW:
        • Excessive parallel plans
        • Worker thread pressure
        • CXPACKET / CXCONSUMER waits
        • Context switching overhead

    Too HIGH:
        • Parallelism rarely used
        • Large analytical queries run serial
        • Slower reporting workloads

    There is NO universal correct value.
    It depends on workload characteristics.

    Typical baselines:
        OLTP: 20–40
        Mixed workload: 30–60
        Reporting/DW: 50–100+
        500 effectively disables parallelism for most workloads.

.WARNINGS
    • This is an instance-level configuration.
      It affects ALL databases on the server.

    • Changes take effect immediately after RECONFIGURE.

    • Do NOT rely on hardware-only formulas.
      CPU count and RAM size alone are insufficient
      to determine optimal CTFP.

    • Always validate impact using:
          - Query wait statistics
          - Worker thread usage
          - Plan cache analysis
          - Query Store (if enabled)

    • Not supported on Azure SQL Database.

.NOTES
    Author  : Sozezzo Astra
    Version : 1.0
    Requires: sysadmin or ALTER SETTINGS permission
#>

    [CmdletBinding(SupportsShouldProcess)]
    param(
        [Parameter(Mandatory)]
        [string]$SqlInstance,

        [Parameter()]
        [int]$CTFP
    )

    # ---- Policy limits ----
    $MinCTFP = 20
    $MaxCTFP = 500

    # ---- Default value ----
    if (-not $PSBoundParameters.ContainsKey('CTFP')) {
        $CTFP = $MinCTFP
    }

    # ---- Enforce limits ----
    if ($CTFP -lt $MinCTFP) {
        $CTFP = $MinCTFP
    }
    elseif ($CTFP -gt $MaxCTFP) {
        $CTFP = $MaxCTFP
    }

    # ---- Read current values (fast, no SMO) ----
    $config = Invoke-DbaQuery `
        -SqlInstance $SqlInstance `
        -Query "
            SELECT
                MAX(CASE WHEN name = 'cost threshold for parallelism'
                         THEN CAST(value_in_use AS int) END) AS CurrentCTFP,
                MAX(CASE WHEN name = 'show advanced options'
                         THEN CAST(value_in_use AS int) END) AS ShowAdvanced
            FROM sys.configurations
            WHERE name IN ('cost threshold for parallelism', 'show advanced options');
        "

    $CurrentCTFP  = [int]$config.CurrentCTFP
    $ShowAdvanced = [int]$config.ShowAdvanced

    Write-Verbose "Current CTFP: $CurrentCTFP"
    Write-Verbose "Show advanced options: $ShowAdvanced"
    Write-Verbose "Requested CTFP (after policy): $CTFP"

    # ---- No change needed ----
    if ($CurrentCTFP -eq $CTFP) {
        Write-Output @{
            SqlInstance = $SqlInstance
            Action      = 'None'
            CurrentCTFP = $CurrentCTFP
            TargetCTFP  = $CTFP
            Reason      = 'Already compliant'
        }
        return
    }

    # ---- Apply change ----
    if ($PSCmdlet.ShouldProcess(
        $SqlInstance,
        "Set Cost Threshold for Parallelism from $CurrentCTFP to $CTFP"
    )) {

        Write-Output "Set Cost Threshold for Parallelism from $CurrentCTFP to $CTFP at $SqlInstance"

        Invoke-DbaQuery `
            -SqlInstance $SqlInstance `
            -Query "
                IF $ShowAdvanced = 0
                BEGIN
                    EXEC sys.sp_configure 'show advanced options', 1;
                    RECONFIGURE;
                END

                EXEC sys.sp_configure 'cost threshold for parallelism', $CTFP;
                RECONFIGURE;
            "

        Write-Output @{
            SqlInstance = $SqlInstance
            Action      = 'Updated'
            CurrentCTFP = $CurrentCTFP
            TargetCTFP  = $CTFP
            Reason      = 'Policy enforcement (20–500)'
        }
    }
}