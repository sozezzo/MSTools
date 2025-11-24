function Invoke-DbaCloneProgrammables {
<#
.SYNOPSIS
    Scripts and deploys programmable objects (UDFs, Views, Stored Procedures, Synonyms, DB-level DDL triggers)
    and table/view DML triggers from a source database to a destination database.

.DESCRIPTION
    Invoke-DbaCloneProgrammables generates two script files:
      1) -ScriptPath  ? Functions, Views, Stored Procedures, Synonyms, and database-level DDL triggers.
      2) -ScriptPath    ? Table and View DML triggers.

    Objects are exported with dbatools in a dependency-friendly order (Functions ? Views ? Procedures ? Synonyms ? DB DDL triggers),
    using scripting options that omit collation, constraints, and indexes while allowing triggers where applicable.
    The function then (optionally) deploys each script into the destination database using
    Invoke-DbaExecuteBatchWithRetry to handle inter-batch dependencies and transient failures.

    Key behaviors:
      - Creates parent directories for the provided output paths when missing.
      - Overwrites the target files.
      - Separates DML triggers into their own script file.
      - Supports -WhatIf / -Confirm via SupportsShouldProcess.
      - Writes progress and results via Write-Log.

.PARAMETER SourceInstance
    SQL Server instance hosting the source database (e.g., 'SQL01' or 'SQL01\INST1').

.PARAMETER SourceDatabase
    Name of the source database from which programmables and triggers will be scripted.

.PARAMETER DestInstance
    SQL Server instance hosting the destination database.

.PARAMETER DestDatabase
    Name of the destination database where the generated scripts will be deployed.

.PARAMETER ScriptPath
    Full path to the output .sql file for programmables (functions, views, procedures, synonyms, DB-level DDL triggers).
    The directory is created if needed; existing file is overwritten.

.PARAMETER ScriptPath
    Full path to the output .sql file for DML triggers on tables and views.
    The directory is created if needed; existing file is overwritten.

.PARAMETER MaxPasses
    Maximum number of retry passes used by Invoke-DbaExecuteBatchWithRetry during deployment.
    Default: 10.

.PARAMETER LogFileName
    Optional log file path. Verbose streams and Write-Log messages are appended here.

.INPUTS
    None. All inputs are parameters.

.OUTPUTS
    None. Writes logs; throws on fatal errors.

.EXAMPLES
    # 1) Script and deploy all programmables (two files) with default retry passes
    Invoke-DbaCloneProgrammables `
        -SourceInstance "SQL01" -SourceDatabase "AppDB" `
        -DestInstance   "SQL02" -DestDatabase   "AppDB_Clone" `
        -ScriptPath "C:\out\AppDB-programmables.sql" `
        -ScriptPath   "C:\out\AppDB-dml-triggers.sql" `
        -LogFileName       "C:\out\AppDB-programmables.log"

    # 2) Generate scripts only (no deployment)
    Invoke-DbaCloneProgrammables `
        -SourceInstance "SQL01" -SourceDatabase "Sales" `
        -DestInstance   "SQL02" -DestDatabase   "Sales_Stage" `
        -ScriptPath "D:\drop\Sales-programmables.sql" `
        -ScriptPath   "D:\drop\Sales-dml-triggers.sql" `
        -WhatIf

    # 3) Increase retries for fragile environments
    Invoke-DbaCloneProgrammables `
        -SourceInstance "Prod\OLTP" -SourceDatabase "HR" `
        -DestInstance   "Stage\OLTP" -DestDatabase   "HR_Test" `
        -ScriptPath "E:\deploy\HR-programmables.sql" `
        -ScriptPath   "E:\deploy\HR-dml-triggers.sql" `
        -MaxPasses 20 `
        -LogFileName "E:\deploy\logs\HR-deploy.log"

 
#>	
    [CmdletBinding(SupportsShouldProcess)]
    param(
        [Parameter(Mandatory)][string]$SourceInstance,
        [Parameter(Mandatory)][string]$SourceDatabase,
        [Parameter(Mandatory)][string]$DestInstance,
        [Parameter(Mandatory)][string]$DestDatabase,

        [Parameter(Mandatory)][string]$ScriptPath,  # output script 

        [int]$MaxPasses = 10
    )

    $LogFileName = if ($Global:WriteLog_LogFileName) { $Global:WriteLog_LogFileName } else { $null }

    # Ensure directories exist
    foreach ($p in @($ScriptPath, $ScriptPath)) {
        $dir = Split-Path -Path $p -Parent
        if ($dir -and -not (Test-Path -LiteralPath $dir)) {
            New-Item -ItemType Directory -Force -Path $dir | Out-Null
        }
    }

    try {

        Write-Log -Message "Copy programmable objects (views, functions, procedures, synonyms, DDL triggers) into new database" -Level Info

        # Common scripting options for programmables (no collations, no indexes/constraints)
        $optionsProg = New-DbaScriptingOption
        $optionsProg.NoCollation         = $true
        $optionsProg.IncludeIfNotExists  = $false
        $optionsProg.AnsiFile            = $true
        $optionsProg.ScriptSchema        = $true
        $optionsProg.DriAll              = $false
        $optionsProg.Indexes             = $false
        $optionsProg.Triggers            = $true  # allow scripting of object-level triggers when applicable

        # --- Export programmables in dependency-friendly order ---
        # UDFs
        Write-Log -Message "Exporting Functions" -Level Info 
        Get-DbaDbUdf -SqlInstance $SourceInstance -Database $SourceDatabase -ExcludeSystemUdf |
            Export-DbaScript -FilePath $ScriptPath -Append:$false -ScriptingOptionsObject $optionsProg -EnableException -Confirm:$false *>> $LogFileName

        # Views
        Write-Log -Message "Exporting Views" -Level Info
        Get-DbaDbView -SqlInstance $SourceInstance -Database $SourceDatabase -ExcludeSystemView |
            Export-DbaScript -FilePath $ScriptPath -Append:$true  -ScriptingOptionsObject $optionsProg -EnableException -Confirm:$false *>> $LogFileName

        # Procedures
        Write-Log -Message "Exporting Procedures" -Level Info 
        Get-DbaDbStoredProcedure -SqlInstance $SourceInstance -Database $SourceDatabase -ExcludeSystemSp |
            Export-DbaScript -FilePath $ScriptPath -Append:$true  -ScriptingOptionsObject $optionsProg -EnableException -Confirm:$false *>> $LogFileName

        # Synonyms
        Write-Log -Message "Exporting Synonyms" -Level Info
        Get-DbaDbSynonym -SqlInstance $SourceInstance -Database $SourceDatabase |
            Export-DbaScript -FilePath $ScriptPath -Append:$true  -ScriptingOptionsObject $optionsProg -EnableException -Confirm:$false *>> $LogFileName

        # DDL triggers (database-level)
        Write-Log -Message "Exporting DDL triggers" -Level Info 
        Get-DbaDbTrigger -SqlInstance $SourceInstance -Database $SourceDatabase -EnableException |
            Export-DbaScript -FilePath $ScriptPath -Append:$true  -ScriptingOptionsObject $optionsProg -EnableException -Confirm:$false *>> $LogFileName

        

        # --- Export DML triggers (table + view) into separate file ---
        $optionsTrig = New-DbaScriptingOption
        $optionsTrig.NoCollation         = $true
        $optionsTrig.IncludeIfNotExists  = $false
        $optionsTrig.AnsiFile            = $true
        $optionsTrig.ScriptSchema        = $true
        $optionsTrig.DriAll              = $false
        $optionsTrig.Indexes             = $false
        $optionsTrig.Triggers            = $true

        # TABLE triggers
        Write-Log -Message "Exporting DML triggers (table triggers)" -Level Info 
        Get-DbaDbTable -SqlInstance $SourceInstance -Database $SourceDatabase |
            Export-DbaScript -FilePath $ScriptPath -Append:$true -ScriptingOptionsObject $optionsTrig -EnableException -Confirm:$false *>> $LogFileName

        # VIEW triggers
        Write-Log -Message "Exporting DML triggers (VIEW triggers)" -Level Info 
        Get-DbaDbView -SqlInstance $SourceInstance -Database $SourceDatabase -ExcludeSystemView |
            Export-DbaScript -FilePath $ScriptPath -Append:$true  -ScriptingOptionsObject $optionsTrig -EnableException -Confirm:$false *>> $LogFileName

        # --- Deploy with retries (handles inter-batch deps safely) ---
        if ($PSCmdlet.ShouldProcess("$DestInstance / $DestDatabase", "Deploy programmables")) {
            Write-Log -Message "Deploying programmables into $DestDatabase on $DestInstance" -Level Warning 
            [void](Invoke-DbaExecuteBatchWithRetry -SqlInstance $DestInstance -Database $DestDatabase -FilePath $ScriptPath -MaxPasses $MaxPasses)
        }

        Write-Log -Message "Programmables deployment completed." -Level Warning 

    }
    catch {
        Write-Log -Message ("ERROR (Invoke-DbaCloneProgrammables): " + $_.Exception.Message) -Level Error 
        throw
    }
}
