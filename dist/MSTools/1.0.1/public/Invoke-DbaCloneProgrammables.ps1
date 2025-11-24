function Invoke-DbaCloneProgrammables {
<#
.SYNOPSIS
    Scripts and deploys programmable objects (UDFs, Views, Stored Procedures, Synonyms, DB-level DDL triggers)
    and table/view DML triggers from a source database to a destination database.

.DESCRIPTION
    Invoke-DbaCloneProgrammables generates two script files:
      1) -ProgrammablesPath  → Functions, Views, Stored Procedures, Synonyms, and database-level DDL triggers.
      2) -DmlTriggersPath    → Table and View DML triggers.

    Objects are exported with dbatools in a dependency-friendly order (Functions → Views → Procedures → Synonyms → DB DDL triggers),
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

.PARAMETER ProgrammablesPath
    Full path to the output .sql file for programmables (functions, views, procedures, synonyms, DB-level DDL triggers).
    The directory is created if needed; existing file is overwritten.

.PARAMETER DmlTriggersPath
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
        -ProgrammablesPath "C:\out\AppDB-programmables.sql" `
        -DmlTriggersPath   "C:\out\AppDB-dml-triggers.sql" `
        -LogFileName       "C:\out\AppDB-programmables.log"

    # 2) Generate scripts only (no deployment)
    Invoke-DbaCloneProgrammables `
        -SourceInstance "SQL01" -SourceDatabase "Sales" `
        -DestInstance   "SQL02" -DestDatabase   "Sales_Stage" `
        -ProgrammablesPath "D:\drop\Sales-programmables.sql" `
        -DmlTriggersPath   "D:\drop\Sales-dml-triggers.sql" `
        -WhatIf

    # 3) Increase retries for fragile environments
    Invoke-DbaCloneProgrammables `
        -SourceInstance "Prod\OLTP" -SourceDatabase "HR" `
        -DestInstance   "Stage\OLTP" -DestDatabase   "HR_Test" `
        -ProgrammablesPath "E:\deploy\HR-programmables.sql" `
        -DmlTriggersPath   "E:\deploy\HR-dml-triggers.sql" `
        -MaxPasses 20 `
        -LogFileName "E:\deploy\logs\HR-deploy.log"

.NOTES
    Export order & contents:
      - ProgrammablesPath:
            Get-DbaDbUdf            (ExcludeSystemUdf)       → Export-DbaScript -Append:
	
    [CmdletBinding(SupportsShouldProcess)]
    param(
        [Parameter(Mandatory)][string]$SourceInstance,
        [Parameter(Mandatory)][string]$SourceDatabase,
        [Parameter(Mandatory)][string]$DestInstance,
        [Parameter(Mandatory)][string]$DestDatabase,

        [Parameter(Mandatory)][string]$ProgrammablesPath,  # views, UDFs, procs, synonyms, DDL triggers
        [Parameter(Mandatory)][string]$DmlTriggersPath,    # table/view triggers

        [int]$MaxPasses = 10,
        [string]$LogFileName
    )

    # Ensure directories exist
    foreach ($p in @($ProgrammablesPath, $DmlTriggersPath)) {
        $dir = Split-Path -Path $p -Parent
        if ($dir -and -not (Test-Path -LiteralPath $dir)) {
            New-Item -ItemType Directory -Force -Path $dir | Out-Null
        }
    }

    try {

        Write-Log -Message "Copy programmable objects (views, functions, procedures, synonyms, DDL triggers) into new database" -Level Info -LogFileName $LogFileName -verbose

        # Common scripting options for programmables (no collations, no indexes/constraints)
        $optionsProg = New-DbaScriptingOption
        $optionsProg.NoCollation         = $true
        $optionsProg.IncludeIfNotExists  = $true
        $optionsProg.AnsiFile            = $true
        $optionsProg.ScriptSchema        = $true
        $optionsProg.DriAll              = $false
        $optionsProg.Indexes             = $false
        $optionsProg.Triggers            = $true  # allow scripting of object-level triggers when applicable

        # --- Export programmables in dependency-friendly order ---
        # UDFs
        Write-Log -Message "Exporting Functions" -Level Info -LogFileName $LogFileName -verbose
        Get-DbaDbUdf -SqlInstance $SourceInstance -Database $SourceDatabase -ExcludeSystemUdf |
            Export-DbaScript -FilePath $ProgrammablesPath -Append:$false -ScriptingOptionsObject $optionsProg -EnableException -Confirm:$false *>> $LogFileName

        # Views
        Write-Log -Message "Exporting Views" -Level Info -LogFileName $LogFileName -verbose
        Get-DbaDbView -SqlInstance $SourceInstance -Database $SourceDatabase -ExcludeSystemView |
            Export-DbaScript -FilePath $ProgrammablesPath -Append:$true  -ScriptingOptionsObject $optionsProg -EnableException -Confirm:$false *>> $LogFileName

        # Procedures
        Write-Log -Message "Exporting Procedures" -Level Info -LogFileName $LogFileName -verbose
        Get-DbaDbStoredProcedure -SqlInstance $SourceInstance -Database $SourceDatabase -ExcludeSystemSp |
            Export-DbaScript -FilePath $ProgrammablesPath -Append:$true  -ScriptingOptionsObject $optionsProg -EnableException -Confirm:$false *>> $LogFileName

        # Synonyms
        Write-Log -Message "Exporting Synonyms" -Level Info -LogFileName $LogFileName -verbose
        Get-DbaDbSynonym -SqlInstance $SourceInstance -Database $SourceDatabase |
            Export-DbaScript -FilePath $ProgrammablesPath -Append:$true  -ScriptingOptionsObject $optionsProg -EnableException -Confirm:$false *>> $LogFileName

        # DDL triggers (database-level)
        Write-Log -Message "Exporting DDL triggers" -Level Info -LogFileName $LogFileName -verbose
        Get-DbaDbTrigger -SqlInstance $SourceInstance -Database $SourceDatabase -EnableException |
            Export-DbaScript -FilePath $ProgrammablesPath -Append:$true  -ScriptingOptionsObject $optionsProg -EnableException -Confirm:$false *>> $LogFileName

        Write-Log -Message "Programmables script: $ProgrammablesPath" -Level Info -LogFileName $LogFileName

        # --- Export DML triggers (table + view) into separate file ---
        Write-Log -Message "Exporting DML triggers (table/view triggers)" -Level Info -LogFileName $LogFileName

        $optionsTrig = New-DbaScriptingOption
        $optionsTrig.NoCollation         = $true
        $optionsTrig.IncludeIfNotExists  = $true
        $optionsTrig.AnsiFile            = $true
        $optionsTrig.ScriptSchema        = $true
        $optionsTrig.DriAll              = $false
        $optionsTrig.Indexes             = $false
        $optionsTrig.Triggers            = $true

        # TABLE triggers
        Get-DbaDbTable -SqlInstance $SourceInstance -Database $SourceDatabase |
            Export-DbaScript -FilePath $DmlTriggersPath -Append:$false -ScriptingOptionsObject $optionsTrig -EnableException -Confirm:$false *>> $LogFileName

        # VIEW triggers
        Get-DbaDbView -SqlInstance $SourceInstance -Database $SourceDatabase -ExcludeSystemView |
            Export-DbaScript -FilePath $DmlTriggersPath -Append:$true  -ScriptingOptionsObject $optionsTrig -EnableException -Confirm:$false *>> $LogFileName

        Write-Log -Message "DML trigger script: $DmlTriggersPath" -Level Info -LogFileName $LogFileName

        # --- Deploy with retries (handles inter-batch deps safely) ---
        if ($PSCmdlet.ShouldProcess("$DestInstance / $DestDatabase", "Deploy programmables")) {
            Write-Log -Message "Deploying programmables into $DestDatabase on $DestInstance" -Level Important -LogFileName $LogFileName
            [void](Invoke-DbaExecuteBatchWithRetry -SqlInstance $DestInstance -Database $DestDatabase -FilePath $ProgrammablesPath -MaxPasses $MaxPasses -LogFile $LogFileName)

            Write-Log -Message "Deploying DML triggers into $DestDatabase on $DestInstance" -Level Important -LogFileName $LogFileName
            [void](Invoke-DbaExecuteBatchWithRetry -SqlInstance $DestInstance -Database $DestDatabase -FilePath $DmlTriggersPath -MaxPasses $MaxPasses -LogFile $LogFileName)
        }

        Write-Log -Message "Programmables deployment completed." -Level Alert -LogFileName $LogFileName
    }
    catch {
        Write-Log -Message ("ERROR (Invoke-DbaCloneProgrammables): " + $_.Exception.Message) -Level Error -LogFileName $LogFileName
        throw
    }
}
