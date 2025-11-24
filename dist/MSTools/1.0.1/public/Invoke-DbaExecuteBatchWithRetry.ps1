function Invoke-DbaExecuteBatchWithRetry {
<#
.SYNOPSIS
    Executes a .sql script file in discrete batches split on GO, retrying failed batches
    across multiple passes until success or the maximum number of passes is reached.

.DESCRIPTION
    Invoke-DbaExecuteBatchWithRetry reads a .sql file, splits it into batches on standalone
    GO lines (case-insensitive), prepends a small set of safe SET options to each batch, and
    executes the batches in order using Invoke-DbaQuery. Any failed batches are retried in
    subsequent passes, up to -MaxPasses. If an entire pass makes no progress (all pending
    batches fail again), the run is stopped to avoid an infinite loop. Execution details,
    failures, and the failed SQL are optionally logged via Write-Log.

    Batch prefix applied before each execution:
        SET XACT_ABORT OFF;
        SET QUOTED_IDENTIFIER ON;
        SET ANSI_NULLS ON;

.PARAMETER SqlInstance
    Target SQL Server instance name or connection string (e.g. 'SQL01', 'SQL01\INST1').

.PARAMETER Database
    Database context in which to execute the batches.

.PARAMETER FilePath
    Full path to the .sql file to execute. Throws if the file does not exist.

.PARAMETER MaxPasses
    Maximum number of retry passes. Failed batches are re-attempted each pass.
    Note: With the current loop, a value of 0 still results in one execution pass.
    Default: 5.

.PARAMETER CommandTimeout
    Timeout (seconds) for execution. 0 = unlimited.
    (Pass this through to Invoke-DbaQuery if you need to enforce a timeout.)
    Default: 0.

.PARAMETER LogFile
    Optional log file path. When supplied, Write-Log messages and verbose streams
    from Invoke-DbaQuery are appended to this file.

.INPUTS
    None. All inputs are provided via parameters.

.OUTPUTS
    System.Boolean
      - $true  when all batches succeed within the pass limit.
      - $false when one or more batches still fail after the final pass.

.EXAMPLES
    # 1) Run a script with defaults (5 passes, unlimited timeout), log to file
    Invoke-DbaExecuteBatchWithRetry `
        -SqlInstance "SQL01" -Database "AppDB" `
        -FilePath "C:\deploy\appdb_indexes.sql" `
        -LogFile  "C:\deploy\logs\indexes.log"

    # 2) Limit to 2 retry passes and enforce a 10-minute timeout
    Invoke-DbaExecuteBatchWithRetry `
        -SqlInstance "Prod\OLTP" -Database "HR" `
        -FilePath "E:\releases\hr_constraints.sql" `
        -MaxPasses 2 -CommandTimeout 600 `
        -LogFile "E:\releases\logs\hr_constraints.log"

    # 3) Use in a guarded deployment step
    if (-not (Invoke-DbaExecuteBatchWithRetry -SqlInstance "SQL02" -Database "Ops_QA" `
           -FilePath "D:\drops\ops_programmables.sql" -LogFile "D:\drops\deploy.log")) {
        throw "Programmables deployment failed. Check log."
    }

.NOTES
    Behavior:
      - Batches are split using regex: ^\s*GO\s*;?\s*$ (Multiline, IgnoreCase).
      - Empty/whitespace batches are skipped.
      - A pass that makes no progress (all pending batches fail) triggers an early stop.
      - Each failed batch is logged with its pass number and batch index; the failed SQL text
        is also dumped to the log for troubleshooting.

    Logging:
      - Uses Write-Log for messages. In this function, calls include positional -Message usage
        and the parameter name -LogFilePath/-LogFile interchangeably; ensure your Write-Log
        supports the signatures you use (e.g., -Message and -LogFilePath/-LogFile).
      - Invoke-DbaQuery verbose output is appended to -LogFile via stream redirection.

    Dependencies:
      - dbatools: Invoke-DbaQuery
      - Write-Log utility capable of arguments like:
            Write-Log -Message <string> [-Level <Enum/String>] [-Verbose] [-LogFilePath <path>]
        (Adjust if your implementation uses -LogFile instead of -LogFilePath.)

    Timeouts:
      - The -CommandTimeout parameter is declared but not passed to Invoke-DbaQuery in this
        function as written. If you need enforcement, add: -CommandTimeout $CommandTimeout.

    Return semantics:
      - Returns $true on complete success; $false if any batches still fail at the end.
      - Throws immediately only for missing -FilePath; other errors are captured per-batch.

.LINK
    https://dbatools.io/
#>
	
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)] [string]$SqlInstance,
        [Parameter(Mandatory)] [string]$Database,
        [Parameter(Mandatory)] [string]$FilePath,
        [int]$MaxPasses = 5,
        [int]$CommandTimeout = 0,   # 0 = unlimited
        [string]$LogFile = $null
    )

    if (-not (Test-Path $FilePath)) { throw "File not found: $FilePath" }
    $text = Get-Content -LiteralPath $FilePath -Raw

    # Split on standalone GO lines (case-insensitive)
    $batches = [regex]::Split($text, '^\s*GO\s*;?\s*$', 'Multiline,IgnoreCase') |
               Where-Object { $_.Trim().Length -gt 0 }

    # Prepend a few safe SETs per batch
    $prefix = @"
SET XACT_ABORT OFF;
SET QUOTED_IDENTIFIER ON;
SET ANSI_NULLS ON;
"@

    $failed = @()
    $pending = for ($i=0; $i -lt $batches.Count; $i++) {
        [pscustomobject]@{ Index=$i; Sql=($prefix + $batches[$i]) }
    }

    $pass = 0
    while ($pending.Count -gt 0 -and $pass -le $MaxPasses) {

        $pass++
        Write-Log "Executing $($pending.Count) batch(es)..." -verbose -LogFilePath $LogFile
    
        $failed = @()
        foreach ($b in $pending) {
            try {
                Invoke-DbaQuery -SqlInstance $SqlInstance -Database $Database -Query $b.Sql *>> $LogFile
                Write-Log "\r\nGO\\r\n$($b.Sql)\r\nGO"  -LogFilePath $LogFile
            }
            catch {
                $msg = $_.Exception.Message
                Write-Log -message "FAILED [Pass $pass, Batch $($b.Index)]: $msg" -LogFilePath $LogFile 
                $failed += $b
            }
        }

        # No progress; stop to avoid infinite loop
        if ($failed.Count -eq $pending.Count) {
            Write-Log "No progress in pass $pass; stopping. Remaining: $($failed.Count) batch(es)." -LogFilePath $LogFile -Verbose
            break
        }
        
        if ($failed.Count -eq 0)
        {
             Write-Log -message "All script was executed" -Level Important -LogFilePath $LogFile -Verbose
        }

        $pending = $failed
       
    }

    if ($pending.Count -gt 0) {

        Write-Log "Some batches failed after $($pass) passes: $($pending.Count). See log for details." -LogFilePath $LogFile -Verbose -Level Error

        foreach ($b in $failed) {
            Write-Log -message "/* FAILED SQL Script */ \r\n\r\nGO\r\n$($b.Sql)\r\nGO\r\n\r\n$($b.Sql)" -LogFilePath $LogFile
        }

        return $false

    } else {

        Write-Log "Programmables deployed after $($pass) pass(es)." -LogFilePath $LogFile -Verbose
        return $true

    }

}