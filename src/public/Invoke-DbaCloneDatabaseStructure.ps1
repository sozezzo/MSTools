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
      - IncludeIfNotExists ($true)
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

.PARAMETER ScriptPath
    Full path to the output .sql file that will contain the base schema.
    The parent directory is created if it does not exist. Existing file is overwritten.

.PARAMETER PassThru
    When specified, returns a [pscustomobject] with ScriptPath, counts of scripted objects,
    and the formatted destination.

.INPUTS
    None. All inputs are provided via parameters.

.OUTPUTS
    If -PassThru is provided, outputs a PSCustomObject:
        ScriptPath (string)
        ScriptedCounts (hashtable: Schemas, Types, Sequences, Tables)
        Destination    (string)  -> "DestInstance.DestDatabase"

    Otherwise, no output on success.

.EXAMPLES
    # 1) Script base schema and deploy to destination
    Invoke-DbaCloneDatabase `
        -SourceInstance "SQL01" -SourceDatabase "AppDB" `
        -DestInstance   "SQL02" -DestDatabase   "AppDB_Base" `
        -ScriptPath "C:\out\AppDB-base.sql" `

    # 2) Script only (no deployment) and return counts
    Invoke-DbaCloneDatabase `
        -SourceInstance "SQL01" -SourceDatabase "Sales" `
        -DestInstance   "SQL02" -DestDatabase   "Sales_Stage" `
        -ScriptPath "D:\drop\Sales-base.sql" `
        -PassThru -WhatIf

    # 3) Use in a pipeline with checks
    $result = Invoke-DbaCloneDatabase `
        -SourceInstance "Prod\OLTP" -SourceDatabase "HR" `
        -DestInstance   "Stage\OLTP" -DestDatabase "HR_Stage" `
        -ScriptPath "E:\deploy\HR-base.sql" `
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
        IncludeIfNotExists = $true
        AnsiFile           = $true
        NoCollation        = $true
        Indexes            = $false
        DriAll             = $false
        Triggers           = $false

    Deployment:
      - If ShouldProcess approves, the generated script is executed with:
            Invoke-DbaQuery -SqlInstance <DestInstance> -Database <DestDatabase> -File <ScriptPath>
      - Intended to be followed by separate steps for PKs, constraints, indexes, programmables, and triggers.

    Permissions:
      - Source: metadata read on the scripted object types.
      - Destination: CREATE SCHEMA / CREATE TYPE / CREATE SEQUENCE / CREATE TABLE and ALTER as required.

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
        [Parameter(Mandatory)][string]$ScriptPath,

        # If set, return the script path (and the list of scripted objects)
        [switch]$PassThru
    )

    $LogFileName = if ($Global:WriteLog_LogFileName) { $Global:WriteLog_LogFileName } else { $null }

    try {
        # Ensure folder exists
        $dir = Split-Path -Path $ScriptPath -Parent
        if (-not (Test-Path -LiteralPath $dir)) {
            New-Item -ItemType Directory -Force -Path $dir | Out-Null
        }

        Write-Log -Message "Script base objects (no collations in the script)" -Level Info  

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
        Write-Log -Message "Exporting base schema to '$ScriptPath'." -Level Info  
        @(
            Get-DbaDbSchema               -SqlInstance $SourceInstance -Database $SourceDatabase
            Get-DbaDbUserDefinedTableType -SqlInstance $SourceInstance -Database $SourceDatabase
            Get-DbaDbSequence             -SqlInstance $SourceInstance -Database $SourceDatabase
            Get-DbaDbTable                -SqlInstance $SourceInstance -Database $SourceDatabase
        ) | Export-DbaScript `
                -FilePath $ScriptPath `
                -Append:$false `
                -ScriptingOptionsObject $optionsBase `
                -EnableException `
                -Confirm:$false *>> $LogFileName

        # Deploy the generated script
        if ($PSCmdlet.ShouldProcess("$DestInstance / $DestDatabase", "Deploy base schema from $ScriptPath")) {
            Write-Log -Message "Deploy base schema into [$DestDatabase] at [$DestInstance]." -Level Warning  
            Invoke-DbaQuery -SqlInstance $DestInstance -Database $DestDatabase -File $ScriptPath -EnableException -Verbose *>> $LogFileName
        }

        Write-Log -Message "Base schema deployment completed for [$DestInstance].[$DestDatabase]." -Level Warning 

        if ($PassThru) {
            # Return both the script path and a quick count of scripted objects
            $counts = @{
                Schemas  = (Get-DbaDbSchema               -SqlInstance $SourceInstance -Database $SourceDatabase).Count
                Types    = (Get-DbaDbUserDefinedTableType -SqlInstance $SourceInstance -Database $SourceDatabase).Count
                Sequences= (Get-DbaDbSequence             -SqlInstance $SourceInstance -Database $SourceDatabase).Count
                Tables   = (Get-DbaDbTable                -SqlInstance $SourceInstance -Database $SourceDatabase).Count
            }
            [pscustomobject]@{
                ScriptPath = $ScriptPath
                ScriptedCounts = $counts
                Destination    = "$DestInstance.$DestDatabase"
            }
        }
    }
    catch {
        Write-Log -Message ("ERROR (Invoke-DbaCloneDatabase): " + $_.Exception.Message) -Level Error 
        throw
    }
}
