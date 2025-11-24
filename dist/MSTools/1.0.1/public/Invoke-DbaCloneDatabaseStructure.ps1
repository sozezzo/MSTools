function Invoke-DbaCloneDatabaseStructure {
<#
.SYNOPSIS
    Scripts the *base schema* (schemas, user-defined table types, sequences, tables) from a source
    database and optionally deploys it to a destination database.

.DESCRIPTION
    Invoke-DbaCloneDatabase generates a single .sql file containing foundational database objects:
      - Schemas
      - User-Defined Table Types (UDTTs)
      - Sequences
      - Tables

    Scripting options are set to:
      - IncludeIfNotExists ($false)
      - ScriptSchema ($true)
      - AnsiFile ($true)
      - NoCollation ($true)  → removes COLLATE clauses
      - Indexes ($false)
      - DriAll ($false)      → excludes PK/FK/UNIQUE/CHECK/DEFAULT
      - Triggers ($false)

    This produces a *clean base* without constraints, indexes, or triggers—intended to be followed
    by subsequent phases (e.g., PKs, constraints, indexes, programmables, triggers).
    When -WhatIf/-Confirm is used (SupportsShouldProcess), deployment can be previewed without executing.

.PARAMETER SourceInstance
    SQL Server instance hosting the source database (e.g., 'SQL01' or 'SQL01\INST1').

.PARAMETER SourceDatabase
    Name of the source database to script.

.PARAMETER DestInstance
    SQL Server instance hosting the destination database.

.PARAMETER DestDatabase
    Name of the destination database where the base schema will be deployed.

.PARAMETER BaseScriptPath
    Full path to the output .sql file that will contain the base schema.
    The parent directory is created if it does not exist. Existing file is overwritten.

.PARAMETER LogFileName
    Optional log file path. Verbose output from Export-DbaScript and Invoke-DbaQuery,
    plus Write-Log messages, are appended here.

.PARAMETER PassThru
    When specified, returns a [pscustomobject] with BaseScriptPath, counts of scripted objects,
    and the formatted destination.

.INPUTS
    None. All inputs are provided via parameters.

.OUTPUTS
    If -PassThru is provided, outputs a PSCustomObject:
        BaseScriptPath (string)
        ScriptedCounts (hashtable: Schemas, Types, Sequences, Tables)
        Destination    (string)  -> "DestInstance.DestDatabase"

    Otherwise, no output on success.

.EXAMPLES
    # 1) Script base schema and deploy to destination
    Invoke-DbaCloneDatabase `
        -SourceInstance "SQL01" -SourceDatabase "AppDB" `
        -DestInstance   "SQL02" -DestDatabase   "AppDB_Base" `
        -BaseScriptPath "C:\out\AppDB-base.sql" `
        -LogFileName    "C:\out\AppDB-base.log"

    # 2) Script only (no deployment) and return counts
    Invoke-DbaCloneDatabase `
        -SourceInstance "SQL01" -SourceDatabase "Sales" `
        -DestInstance   "SQL02" -DestDatabase   "Sales_Stage" `
        -BaseScriptPath "D:\drop\Sales-base.sql" `
        -PassThru -WhatIf

    # 3) Use in a pipeline with checks
    $result = Invoke-DbaCloneDatabase `
        -SourceInstance "Prod\OLTP" -SourceDatabase "HR" `
        -DestInstance   "Stage\OLTP" -DestDatabase "HR_Stage" `
        -BaseScriptPath "E:\deploy\HR-base.sql" `
        -LogFileName    "E:\deploy\logs\HR-base.log" `
        -PassThru
    $result.ScriptedCounts

.NOTES
    Export order (to honor dependencies):
      1) Schemas
      2) User-Defined Table Types
      3) Sequences
      4) Tables

    Scripting options (New-DbaScriptingOption):
        ScriptSchema       = $true
        IncludeIfNotExists = $false
        AnsiFile           = $true
        NoCollation        = $true
        Indexes            = $false
        DriAll             = $false
        Triggers           = $false

    Deployment:
      - If ShouldProcess approves, the generated script is executed with:
            Invoke-DbaQuery -SqlInstance <DestInstance> -Database <DestDatabase> -File <BaseScriptPath>
      - Intended to be followed by separate steps for PKs, constraints, indexes, programmables, and triggers.

    Permissions:
      - Source: metadata read on the scripted object types.
      - Destination: CREATE SCHEMA / CREATE TYPE / CREATE SEQUENCE / CREATE TABLE and ALTER as required.

    Logging:
      - Write-Log should support: -Message, -Level, -LogFileName, -Verbose.
      - Verbose output from dbatools commands is redirected and appended to -LogFileName.

    Error Handling:
      - Failures are logged via Write-Log and rethrown for upstream handling.

.DEPENDENCIES
    dbatools:
      Get-DbaDbSchema
      Get-DbaDbUserDefinedTableType
      Get-DbaDbSequence
      Get-DbaDbTable
      New-DbaScriptingOption
      Export-DbaScript
      Invoke-DbaQuery

.LINK
    https://dbatools.io/
#>
	
    [CmdletBinding(SupportsShouldProcess)]
    param(
        [Parameter(Mandatory)][string]$SourceInstance,
        [Parameter(Mandatory)][string]$SourceDatabase,

        [Parameter(Mandatory)][string]$DestInstance,
        [Parameter(Mandatory)][string]$DestDatabase,

        # Where to save the generated base schema script (.sql)
        [Parameter(Mandatory)][string]$BaseScriptPath,

        # Your logger file (optional)
        [string]$LogFileName,

        # If set, return the script path (and the list of scripted objects)
        [switch]$PassThru
    )

    try {
        # Ensure folder exists
        $dir = Split-Path -Path $BaseScriptPath -Parent
        if (-not (Test-Path -LiteralPath $dir)) {
            New-Item -ItemType Directory -Force -Path $dir | Out-Null
        }

        Write-Log -Message "Script base objects (no collations in the script)" -Level Info -LogFileName $LogFileName

        # Build scripting options (NO COLLATE, NO indexes/constraints/triggers)
        $optionsBase = New-DbaScriptingOption
        $optionsBase.ScriptSchema       = $true
        $optionsBase.IncludeIfNotExists = $false
        $optionsBase.AnsiFile           = $true
        $optionsBase.NoCollation        = $true
        $optionsBase.Indexes            = $false
        $optionsBase.DriAll             = $false
        $optionsBase.Triggers           = $false

        # Order matters: schemas/types/sequences before tables
        Write-Log -Message "Exporting base schema to '$BaseScriptPath'." -Level Info -LogFileName $LogFileName
        @(
            Get-DbaDbSchema               -SqlInstance $SourceInstance -Database $SourceDatabase
            Get-DbaDbUserDefinedTableType -SqlInstance $SourceInstance -Database $SourceDatabase
            Get-DbaDbSequence             -SqlInstance $SourceInstance -Database $SourceDatabase
            Get-DbaDbTable                -SqlInstance $SourceInstance -Database $SourceDatabase
        ) | Export-DbaScript `
                -FilePath $BaseScriptPath `
                -Append:$false `
                -ScriptingOptionsObject $optionsBase `
                -EnableException `
                -Confirm:$false *>> $LogFileName

        # Deploy the generated script
        if ($PSCmdlet.ShouldProcess("$DestInstance / $DestDatabase", "Deploy base schema from $BaseScriptPath")) {
            Write-Log -Message "Deploy base schema into [$DestDatabase] at [$DestInstance]." -Level Important -LogFileName $LogFileName
            Invoke-DbaQuery -SqlInstance $DestInstance -Database $DestDatabase -File $BaseScriptPath -EnableException -Verbose *>> $LogFileName
        }

        Write-Log -Message "Base schema deployment completed for [$DestInstance].[$DestDatabase]." -Level Alert -LogFileName $LogFileName

        if ($PassThru) {
            # Return both the script path and a quick count of scripted objects
            $counts = @{
                Schemas  = (Get-DbaDbSchema               -SqlInstance $SourceInstance -Database $SourceDatabase).Count
                Types    = (Get-DbaDbUserDefinedTableType -SqlInstance $SourceInstance -Database $SourceDatabase).Count
                Sequences= (Get-DbaDbSequence             -SqlInstance $SourceInstance -Database $SourceDatabase).Count
                Tables   = (Get-DbaDbTable                -SqlInstance $SourceInstance -Database $SourceDatabase).Count
            }
            [pscustomobject]@{
                BaseScriptPath = $BaseScriptPath
                ScriptedCounts = $counts
                Destination    = "$DestInstance.$DestDatabase"
            }
        }
    }
    catch {
        Write-Log -Message ("ERROR (Invoke-DbaCloneDatabase): " + $_.Exception.Message) -Level Error -LogFileName $LogFileName
        throw
    }
}
