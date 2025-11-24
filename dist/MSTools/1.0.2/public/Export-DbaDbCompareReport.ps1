<#
    Export-DbaDbCompareReport.ps1

    Generates an HTML report comparing structure and selected metadata of two SQL Server databases.
    Uses dbatools for connectivity and querying.

    Optional comparisons (opt-in):
      - Table DML triggers
      - Database DDL triggers
      - Primary keys & Foreign keys
      - Index definitions (excluding PK/Unique-constraint indexes)

    IMPORTANT (fixes in this version)
      - Collation conflicts eliminated by using COLLATE DATABASE_DEFAULT on all string concatenations and definitions.
      - Empty/null safety: all collections coerced to @() before comparison.

    Examples
    --------
    Export-DbaDbCompareReport -SourceInstance SQL01 -SourceDatabase DB1 `
        -TargetInstance SQL02 -TargetDatabase DB1 -OutputPath C:\Reports\

    Export-DbaDbCompareReport -SourceInstance SQL01 -SourceDatabase DB1 `
        -TargetInstance SQL02 -TargetDatabase DB1 -CompareKeys -CompareIndexes

    Export-DbaDbCompareReport -SourceInstance SQL01 -SourceDatabase DB1 `
        -TargetInstance SQL02 -TargetDatabase DB1 -CompareAll
#>

function Export-DbaDbCompareReport {
[CmdletBinding()]
param (
    [Parameter(Mandatory = $true)] [string]$SourceInstance,
    [Parameter(Mandatory = $true)] [string]$SourceDatabase,
    [Parameter(Mandatory = $true)] [string]$TargetInstance,
    [Parameter(Mandatory = $true)] [string]$TargetDatabase,

    [string]$OutputPath = (Join-Path -Path $PWD -ChildPath "DatabaseComparisonReport.html"),

    # ---- optional comparisons (opt-in) ----
    [switch]$CompareTableTriggers,
    [switch]$CompareDbTriggers,
    [switch]$CompareKeys,
    [switch]$CompareIndexes,
    [switch]$CompareAll
)

#region 00. Normalize options and defaults
if ($CompareAll) {
    $CompareTableTriggers = $true
    $CompareDbTriggers    = $true
    $CompareKeys          = $true
    $CompareIndexes       = $true
}

$CharOK      = [char]0x2705   # ✅
$CharWarn    = [char]0x26A0   # ⚠
$CharFail    = [char]0x274C   # ❌
$CharDefault = [char]0x2753   # ❓

$OutputPath = $OutputPath.Trim()
if ($OutputPath.EndsWith('\') -or $OutputPath.EndsWith('/')) {
    $timestamp = Get-Date -Format 'yyyyMMdd-HHmmss'
    $OutputPath = Join-Path -Path $OutputPath -ChildPath "DatabaseComparisonReport-$timestamp.html"
}
#endregion

#region 01. Dependencies
try {
    if (-not (Get-Module -ListAvailable -Name dbatools)) {
        throw "The dbatools module is required. Install with 'Install-Module dbatools'."
    }
    Import-Module dbatools -ErrorAction Stop
}
catch {
    throw "Failed to load the dbatools module. $_"
}
#endregion

#region 02. Helpers (queries, diffs, normalize, html)
function Invoke-DbMetadataQuery {
    <#
      Executes a query via dbatools and returns @() on failure for safe downstream handling.
    #>
    param ([string]$Instance,[string]$Database,[string]$Query)
    try {
        $result = Invoke-DbaQuery -SqlInstance $Instance -Database $Database -Query $Query -As PSObject -EnableException
        if ($null -eq $result) { return @() }
        return $result
    }
    catch {
        Write-Warning "Query failed on $Instance/$Database. $_"
        return @()
    }
}

function Get-FirstStringDifference {
    [CmdletBinding()] param(
        [Parameter(Mandatory)] [string]$String1,
        [Parameter(Mandatory)] [string]$String2,
        [switch]$IgnoreCase,
        [int]$ContextRadius = 25
    )
    function Get-LineColumn {
        param([string]$s, [int]$index)
        if ($index -lt 0) { return [pscustomobject]@{ Line=0; Column=0; LineText='' } }
        $n = $s.Length
        if ($n -eq 0) { return [pscustomobject]@{ Line=1; Column=1; LineText='' } }
        $line = (($s.Substring(0, [Math]::Min($index, $n))) -split "`n").Count
        if ($line -lt 1) { $line = 1 }
        $lastNl = $s.LastIndexOf("`n", [Math]::Min($index, $n - 1))
        $lineStart = if ($lastNl -ge 0) { $lastNl + 1 } else { 0 }
        $col = $index - $lineStart + 1
        if ($col -lt 1) { $col = 1 }
        $nextNl = $s.IndexOf("`n", $lineStart)
        $lineEnd = if ($nextNl -ge 0) { $nextNl } else { $n }
        $lineText = $s.Substring($lineStart, $lineEnd - $lineStart).TrimEnd("`r")
        [pscustomobject]@{ Line=$line; Column=$col; LineText=$lineText }
    }
    function Get-Context {
        param([string]$s, [int]$index, [int]$radius, [int]$col1Based)
        if ($s.Length -eq 0) { return [pscustomobject]@{ Text=''; Marker='' } }
        $start  = [Math]::Max(0, $index - $radius)
        $len    = [Math]::Min($s.Length - $start, 2*$radius + 1)
        $text   = $s.Substring($start, $len)
        $marker = (' ' * ($index - $start)) + '^'
        $text = $text -replace "`r", '␍' -replace "`n", '␊'
        [pscustomobject]@{ Text=$text; Marker=$marker }
    }
    $cmp = if ($IgnoreCase) {[System.StringComparison]::OrdinalIgnoreCase} else {[System.StringComparison]::Ordinal}
    $minLen = [Math]::Min($String1.Length, $String2.Length)
    $diffIndex = -1
    for ($i=0; $i -lt $minLen; $i++) {
        if (-not [System.String]::Equals($String1[$i], $String2[$i], $cmp)) { $diffIndex = $i; break }
    }
    if ($diffIndex -eq -1) {
        if ($String1.Length -ne $String2.Length) { $diffIndex = $minLen } else {
            return [pscustomobject]@{ AreEqual=$true; AbsoluteIndex=-1; LineNumber=0; ColumnNumber=0; Char1=$null; Char2=$null; Line1=$null; Line2=$null; Context1=$null; Context2=$null; Note='Strings are identical.' }
        }
    }
    $pos1 = Get-LineColumn -s $String1 -index ([Math]::Min($diffIndex, [Math]::Max(0, $String1.Length - 1)))
    $pos2 = Get-LineColumn -s $String2 -index ([Math]::Min($diffIndex, [Math]::Max(0, $String2.Length - 1)))
    $char1 = if ($diffIndex -lt $String1.Length) { $String1[$diffIndex] } else { '<EOS>' }
    $char2 = if ($diffIndex -lt $String2.Length) { $String2[$diffIndex] } else { '<EOS>' }
    $ctx1 = Get-Context -s $String1 -index ([Math]::Min($diffIndex, [Math]::Max(0, $String1.Length - 1))) -radius $ContextRadius -col1Based $pos1.Column
    $ctx2 = Get-Context -s $String2 -index ([Math]::Min($diffIndex, [Math]::Max(0, $String2.Length - 1))) -radius $ContextRadius -col1Based $pos2.Column
    [pscustomobject]@{
        AreEqual      = $false
        AbsoluteIndex = $diffIndex
        LineNumber    = $pos1.Line
        ColumnNumber  = $pos1.Column
        Char1         = $char1.ToString()
        Char2         = $char2.ToString()
        Line1         = $pos1.LineText
        Line2         = $pos2.LineText
        Context1      = $ctx1.Text; Context1Mark = $ctx1.Marker
        Context2      = $ctx2.Text; Context2Mark = $ctx2.Marker
        Note          = if ($char1 -eq '<EOS>' -or $char2 -eq '<EOS>') { 'Difference is at end of one string.' } else { $null }
        ToString      = "Different was found`r`nAbsolute Position : $diffIndex`r`nLine : $($pos1.Line) - Column Number : $($pos1.Column)`r`nText 1 : $($ctx1.Text)`r`n        $($ctx1.Marker)`r`nText 2 : $($ctx2.Text)`r`n        $($ctx2.Marker)`r`n"
    }
}

function Normalize-Definition {
    param([string]$Text)
    if ([string]::IsNullOrEmpty($Text)) { return '' }
    $t = $Text -replace '\r\n', "`n"
    $t = $t -replace '[\t ]+(?=`n)', ''
    $t = $t.ToLower().Trim()
    return $t
}

function New-ObjectComparisonResult {
    <#
      Input : collections with properties { ObjectName, Definition }
      Output: { ObjectType, ObjectName, Result }
      Safe with empty inputs.
    #>
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][string]$ObjectType,

        # allow empty arrays to bind
        [Parameter(Mandatory)][AllowEmptyCollection()][object[]]$Source,
        [Parameter(Mandatory)][AllowEmptyCollection()][object[]]$Target,

        [switch]$Strict
    )

    # Coerce nulls to empty arrays (prevents binding errors)
    if ($null -eq $Source) { $Source = @() }
    if ($null -eq $Target) { $Target = @() }

    if (-not $script:CharOK)      { $script:CharOK      = '✅' }
    if (-not $script:CharWarn)    { $script:CharWarn    = '⚠️' }
    if (-not $script:CharFail)    { $script:CharFail    = '❌' }
    if (-not $script:CharDefault) { $script:CharDefault = '❓' }

    $src = @{}; foreach ($s in $Source) { if ($s.ObjectName) { $src[$s.ObjectName] = $s } }
    $dst = @{}; foreach ($t in $Target) { if ($t.ObjectName) { $dst[$t.ObjectName] = $t } }

    foreach ($name in @($src.Keys + $dst.Keys) | Sort-Object -Unique) {
        $a = $src[$name]; $b = $dst[$name]

        if (-not $a -and $b) { [pscustomobject]@{ ObjectType=$ObjectType; ObjectName=$name; Result="$CharWarn Missing in source" }; continue }
        if ($a -and -not $b) { [pscustomobject]@{ ObjectType=$ObjectType; ObjectName=$name; Result="$CharWarn Missing in target" }; continue }

        $defA = "$($a.Definition)"; $defB = "$($b.Definition)"
        if (-not $Strict) { $defA = Normalize-Definition $defA; $defB = Normalize-Definition $defB }

        if ($defA -ceq $defB) {
            [pscustomobject]@{ ObjectType=$ObjectType; ObjectName=$name; Result="$CharOK Match" }
        } else {
            try { $r = Get-FirstStringDifference -String1 $defA -String2 $defB; Write-Log -Message "Object $ObjectType : $name `r`n$($r.ToString)" } catch {}
            [pscustomobject]@{ ObjectType=$ObjectType; ObjectName=$name; Result="$CharFail Different definition" }
        }
    }
}

function Get-DbaObjectComparison {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][string]$ObjectType,
        [Parameter(Mandatory)][string]$SourceInstance,
        [Parameter(Mandatory)][string]$SourceDatabase,
        [Parameter(Mandatory)][string]$TargetInstance,
        [Parameter(Mandatory)][string]$TargetDatabase,
        [Parameter(Mandatory)][string]$Query
    )

    $src = @(Invoke-DbMetadataQuery -Instance $SourceInstance -Database $SourceDatabase -Query $Query | Select-Object ObjectName, Definition)
    $dst = @(Invoke-DbMetadataQuery -Instance $TargetInstance -Database $TargetDatabase -Query $Query | Select-Object ObjectName, Definition)

    # If both sides are empty, nothing to compare—return an empty result set
    if ($src.Count -eq 0 -and $dst.Count -eq 0) { return @() }

    New-ObjectComparisonResult -ObjectType $ObjectType -Source $src -Target $dst
}


function New-NameComparisonResult {
    param (
        [string]$ObjectType,
        [AllowEmptyCollection()][string[]]$SourceNames,
        [AllowEmptyCollection()][string[]]$TargetNames
    )
    $SourceNames = @($SourceNames) # null -> @()
    $TargetNames = @($TargetNames)

    $sourceSet = @($SourceNames | Where-Object { -not [string]::IsNullOrWhiteSpace($_) } | Sort-Object -Unique)
    $targetSet = @($TargetNames | Where-Object { -not [string]::IsNullOrWhiteSpace($_) } | Sort-Object -Unique)
    $allNames  = @($sourceSet + $targetSet) | Sort-Object -Unique

    foreach ($name in $allNames) {
        $inSource = $sourceSet -contains $name
        $inTarget = $targetSet -contains $name
        if ($inSource -and $inTarget)      { $result = "$CharOK Match" }
        elseif ($inSource -and -not $inTarget) { $result = "$CharWarn Missing in target" }
        elseif (-not $inSource -and $inTarget) { $result = "$CharWarn Missing in source" }
        else { continue }
        [PSCustomObject]@{ ObjectType=$ObjectType; ObjectName=$name; Result=$result }
    }
}

function New-HtmlSection {
    <#
        Renders one section as:
          <h2>Title</h2>
          <table>...</table>
        IMPORTANT: Use -PreContent so we don't duplicate <h2> before every row.
    #>
    param (
        [string]$Title,
        [object[]]$Data
    )

    if (-not $Data -or $Data.Count -eq 0) {
        return "<h2>$Title</h2><p>No differences found.</p>"
    }

    # ConvertTo-Html returns a string[] when -Fragment is used; join for safety.
    $fragment = $Data | ConvertTo-Html -Fragment -PreContent "<h2>$Title</h2>"
    return ($fragment -join "`n")
}

function Format-DatabaseOptionName {
    param ([string]$OptionName)
    if ([string]::IsNullOrWhiteSpace($OptionName)) { return $OptionName }
    $prefix=''; $nameBody=$OptionName
    if ($nameBody.StartsWith('Scoped.')) { $prefix='Scoped: '; $nameBody=$nameBody.Substring(7) }
    $nameBody = $nameBody -replace '[._]', ' '
    $ti = [System.Globalization.CultureInfo]::InvariantCulture.TextInfo
    $title = $ti.ToTitleCase($nameBody.ToLowerInvariant())
    $title = $title -replace '\bAnsi\b','ANSI' -replace '\bSql\b','SQL'
    "$prefix$title"
}
#endregion

#region 03. Queries (collation-safe)
# Row counts
$rowCountQuery = @"
SELECT
    s.name COLLATE DATABASE_DEFAULT AS SchemaName,
    t.name COLLATE DATABASE_DEFAULT AS TableName,
    SUM(ps.row_count) AS RowCountValue
FROM sys.tables AS t
JOIN sys.schemas AS s ON t.schema_id = s.schema_id
JOIN sys.dm_db_partition_stats AS ps ON t.object_id = ps.object_id AND ps.index_id IN (0, 1)
GROUP BY s.name, t.name;
"@

# Columns
$columnQuery = @"
SELECT
    TABLE_SCHEMA COLLATE DATABASE_DEFAULT AS TABLE_SCHEMA,
    TABLE_NAME   COLLATE DATABASE_DEFAULT AS TABLE_NAME,
    COLUMN_NAME  COLLATE DATABASE_DEFAULT AS COLUMN_NAME
FROM INFORMATION_SCHEMA.COLUMNS;
"@

# Synonyms (name compare list)
$synonymQuery = @"
SELECT
    (s.name COLLATE DATABASE_DEFAULT) + N'.' + (sn.name COLLATE DATABASE_DEFAULT) AS ObjectName,
    CONVERT(nvarchar(max), OBJECT_DEFINITION(sn.object_id)) COLLATE DATABASE_DEFAULT AS Definition
FROM sys.synonyms AS sn
JOIN sys.schemas AS s ON sn.schema_id = s.schema_id;
"@

# Schemas
$schemaQuery = "SELECT name COLLATE DATABASE_DEFAULT AS ObjectName FROM sys.schemas WHERE schema_id < 16384;"

# Views
$viewQuery = @"
SELECT
    (s.name COLLATE DATABASE_DEFAULT) + N'.' + (v.name COLLATE DATABASE_DEFAULT) AS ObjectName,
    CONVERT(nvarchar(max), OBJECT_DEFINITION(v.object_id)) COLLATE DATABASE_DEFAULT AS Definition
FROM sys.views AS v
JOIN sys.schemas AS s ON v.schema_id = s.schema_id;
"@

# Procedures
$procedureQuery = @"
SELECT
    (s.name COLLATE DATABASE_DEFAULT) + N'.' + (p.name COLLATE DATABASE_DEFAULT) AS ObjectName,
    CONVERT(nvarchar(max), OBJECT_DEFINITION(p.object_id)) COLLATE DATABASE_DEFAULT AS Definition
FROM sys.procedures AS p
JOIN sys.schemas AS s ON p.schema_id = s.schema_id;
"@

# Functions
$functionQuery = @"
SELECT
    (s.name COLLATE DATABASE_DEFAULT) + N'.' + (o.name COLLATE DATABASE_DEFAULT) AS ObjectName,
    CONVERT(nvarchar(max), OBJECT_DEFINITION(o.object_id)) COLLATE DATABASE_DEFAULT AS Definition
FROM sys.objects AS o
JOIN sys.schemas AS s ON o.schema_id = s.schema_id
WHERE o.[type] IN ('FN', 'IF', 'TF', 'FS', 'FT');
"@

# Table triggers
$tableTriggerQuery = @"
SELECT
    (sch.name COLLATE DATABASE_DEFAULT) + N'.' + (tr.name COLLATE DATABASE_DEFAULT) AS ObjectName,
    CONVERT(nvarchar(max), OBJECT_DEFINITION(tr.object_id)) COLLATE DATABASE_DEFAULT AS Definition
FROM sys.triggers AS tr
JOIN sys.objects AS parentObj ON tr.parent_id = parentObj.object_id
JOIN sys.schemas AS sch ON parentObj.schema_id = sch.schema_id
WHERE tr.parent_class_desc = 'OBJECT_OR_COLUMN';
"@

# DB triggers
$dbTriggerQuery = @"
SELECT
    name COLLATE DATABASE_DEFAULT AS ObjectName,
    CONVERT(nvarchar(max), OBJECT_DEFINITION(object_id)) COLLATE DATABASE_DEFAULT AS Definition
FROM sys.triggers
WHERE parent_class_desc = 'DATABASE';
"@

# Primary keys (deterministic definition)
$primaryKeyQuery = @"
;WITH pk AS (
  SELECT
    (QUOTENAME(s.name) + '.' + QUOTENAME(t.name) + '.' + QUOTENAME(kc.name)) COLLATE DATABASE_DEFAULT AS ObjectName,
    kc.name COLLATE DATABASE_DEFAULT AS ConstraintName,
    s.name  COLLATE DATABASE_DEFAULT AS SchemaName,
    t.name  COLLATE DATABASE_DEFAULT AS TableName,
    i.type_desc,
    i.is_padded, i.ignore_dup_key, i.allow_row_locks, i.allow_page_locks,
    i.fill_factor,
    i.object_id, i.index_id
  FROM sys.key_constraints kc
  JOIN sys.tables t  ON t.object_id = kc.parent_object_id
  JOIN sys.schemas s ON s.schema_id = t.schema_id
  JOIN sys.indexes i ON i.object_id = t.object_id AND i.index_id = kc.unique_index_id
  WHERE kc.type = 'PK'
),
keys AS (
  SELECT
    p.ObjectName,
    STUFF((
      SELECT ',' + QUOTENAME(c2.name) + CASE WHEN ic2.is_descending_key=1 THEN ' DESC' ELSE ' ASC' END
      FROM sys.index_columns ic2
      JOIN sys.columns c2 ON c2.object_id = ic2.object_id AND c2.column_id = ic2.column_id
      WHERE ic2.object_id = p.object_id AND ic2.index_id = p.index_id AND ic2.is_included_column = 0
      ORDER BY ic2.key_ordinal
      FOR XML PATH(''), TYPE).value('.','nvarchar(max)'),1,1,'') AS KeyCols
  FROM pk p
)
SELECT
  p.ObjectName,
  'CONSTRAINT ' + QUOTENAME(p.ConstraintName) + ' PRIMARY KEY ' + p.type_desc +
  ' (' + k.KeyCols + ')' +
  ' WITH (PAD_INDEX=' + CASE WHEN p.is_padded=1 THEN 'ON' ELSE 'OFF' END +
  ', IGNORE_DUP_KEY=' + CASE WHEN p.ignore_dup_key=1 THEN 'ON' ELSE 'OFF' END +
  ', ALLOW_ROW_LOCKS=' + CASE WHEN p.allow_row_locks=1 THEN 'ON' ELSE 'OFF' END +
  ', ALLOW_PAGE_LOCKS=' + CASE WHEN p.allow_page_locks=1 THEN 'ON' ELSE 'OFF' END +
  CASE WHEN p.fill_factor>0 THEN ', FILLFACTOR='+CONVERT(varchar(3),p.fill_factor) ELSE '' END +
  ')' AS Definition
FROM pk p
JOIN keys k ON k.ObjectName = p.ObjectName
ORDER BY p.ObjectName;
"@

# Foreign keys (deterministic definition)
$foreignKeyQuery = @"
WITH fk AS (
  SELECT
    (QUOTENAME(s.name)+'.'+QUOTENAME(t.name)+'.'+QUOTENAME(f.name)) COLLATE DATABASE_DEFAULT AS ObjectName,
    f.name COLLATE DATABASE_DEFAULT AS ConstraintName,
    s.name COLLATE DATABASE_DEFAULT AS SchemaName, t.name COLLATE DATABASE_DEFAULT AS TableName,
    sr.name COLLATE DATABASE_DEFAULT AS RefSchemaName, tr.name COLLATE DATABASE_DEFAULT AS RefTableName,
    f.object_id, f.delete_referential_action_desc, f.update_referential_action_desc,
    f.is_not_trusted, f.is_disabled
  FROM sys.foreign_keys f
  JOIN sys.tables t  ON t.object_id = f.parent_object_id
  JOIN sys.schemas s ON s.schema_id = t.schema_id
  JOIN sys.tables tr ON tr.object_id = f.referenced_object_id
  JOIN sys.schemas sr ON sr.schema_id = tr.schema_id
),
parent_cols AS (
  SELECT
    f.object_id,
    STUFF((
      SELECT ',' + QUOTENAME(cp.name)
      FROM sys.foreign_key_columns fkc2
      JOIN sys.columns cp ON cp.object_id = fkc2.parent_object_id AND cp.column_id = fkc2.parent_column_id
      WHERE fkc2.constraint_object_id = f.object_id
      ORDER BY fkc2.constraint_column_id
      FOR XML PATH(''), TYPE).value('.','nvarchar(max)'),1,1,'') AS ParentCols
  FROM fk f
),
ref_cols AS (
  SELECT
    f.object_id,
    STUFF((
      SELECT ',' + QUOTENAME(cr.name)
      FROM sys.foreign_key_columns fkc2
      JOIN sys.columns cr ON cr.object_id = fkc2.referenced_object_id AND cr.column_id = fkc2.referenced_column_id
      WHERE fkc2.constraint_object_id = f.object_id
      ORDER BY fkc2.constraint_column_id
      FOR XML PATH(''), TYPE).value('.','nvarchar(max)'),1,1,'') AS RefCols
  FROM fk f
)
SELECT
  f.ObjectName,
  'CONSTRAINT ' + QUOTENAME(f.ConstraintName) +
  ' FOREIGN KEY (' + pc.ParentCols + ')' +
  ' REFERENCES ' + QUOTENAME(f.RefSchemaName)+'.'+QUOTENAME(f.RefTableName) +
  ' (' + rc.RefCols + ')' +
  ' ON DELETE ' + f.delete_referential_action_desc +
  ' ON UPDATE ' + f.update_referential_action_desc +
  CASE WHEN f.is_not_trusted=1 THEN ' NOT TRUSTED' ELSE '' END +
  CASE WHEN f.is_disabled=1 THEN ' DISABLED' ELSE '' END AS Definition
FROM fk f
JOIN parent_cols pc ON pc.object_id = f.object_id
JOIN ref_cols    rc ON rc.object_id = f.object_id
ORDER BY f.ObjectName;
"@

# Non-constraint indexes
$indexQuery = @"
WITH ix AS (
  SELECT
    (QUOTENAME(s.name)+'.'+QUOTENAME(t.name)+'.'+QUOTENAME(i.name)) COLLATE DATABASE_DEFAULT AS ObjectName,
    s.name COLLATE DATABASE_DEFAULT AS SchemaName, t.name COLLATE DATABASE_DEFAULT AS TableName, i.name COLLATE DATABASE_DEFAULT AS IndexName,
    i.object_id, i.index_id,
    i.type_desc, i.is_unique, i.is_padded, i.ignore_dup_key,
    i.allow_row_locks, i.allow_page_locks,
    i.fill_factor, i.has_filter, i.filter_definition,
    i.is_hypothetical, i.is_disabled, i.is_primary_key, i.is_unique_constraint
  FROM sys.indexes i
  JOIN sys.tables t  ON t.object_id = i.object_id
  JOIN sys.schemas s ON s.schema_id = t.schema_id
  WHERE i.index_id > 0
    AND i.is_hypothetical = 0
    AND i.is_primary_key = 0
    AND i.is_unique_constraint = 0
),
keycols AS (
  SELECT
    i.object_id, i.index_id,
    STUFF((
      SELECT ',' + QUOTENAME(c2.name) + CASE WHEN ic2.is_descending_key=1 THEN ' DESC' ELSE ' ASC' END
      FROM sys.index_columns ic2
      JOIN sys.columns c2 ON c2.object_id = ic2.object_id AND c2.column_id = ic2.column_id
      WHERE ic2.object_id = i.object_id AND ic2.index_id = i.index_id AND ic2.is_included_column = 0
      ORDER BY ic2.key_ordinal
      FOR XML PATH(''), TYPE).value('.','nvarchar(max)'),1,1,'') AS KeyCols
  FROM ix i
),
inclcols AS (
  SELECT
    i.object_id, i.index_id,
    STUFF((
      SELECT ',' + QUOTENAME(c2.name)
      FROM sys.index_columns ic2
      JOIN sys.columns c2 ON c2.object_id = ic2.object_id AND c2.column_id = ic2.column_id
      WHERE ic2.object_id = i.object_id AND ic2.index_id = i.index_id AND ic2.is_included_column = 1
      ORDER BY ic2.index_column_id
      FOR XML PATH(''), TYPE).value('.','nvarchar(max)'),1,1,'') AS IncludedCols
  FROM ix i
)
SELECT
  i.ObjectName,
  'INDEX ' + QUOTENAME(i.IndexName) +
  ' ON ' + QUOTENAME(i.SchemaName)+'.'+QUOTENAME(i.TableName) +
  ' (' + kc.KeyCols + ')' +
  CASE WHEN ic.IncludedCols IS NOT NULL AND ic.IncludedCols <> '' THEN ' INCLUDE (' + ic.IncludedCols + ')' ELSE '' END +
  ' ' + i.type_desc +
  CASE WHEN i.is_unique=1 THEN ' UNIQUE' ELSE '' END +
  CASE WHEN i.has_filter=1 THEN ' WHERE ' + i.filter_definition ELSE '' END +
  ' WITH (PAD_INDEX=' + CASE WHEN i.is_padded=1 THEN 'ON' ELSE 'OFF' END +
  ', IGNORE_DUP_KEY=' + CASE WHEN i.ignore_dup_key=1 THEN 'ON' ELSE 'OFF' END +
  ', ALLOW_ROW_LOCKS=' + CASE WHEN i.allow_row_locks=1 THEN 'ON' ELSE 'OFF' END +
  ', ALLOW_PAGE_LOCKS=' + CASE WHEN i.allow_page_locks=1 THEN 'ON' ELSE 'OFF' END +
  CASE WHEN i.fill_factor>0 THEN ', FILLFACTOR='+CONVERT(varchar(3),i.fill_factor) ELSE '' END +
  ')' +
  CASE WHEN i.is_disabled=1 THEN ' DISABLED' ELSE '' END AS Definition
FROM ix i
JOIN keycols kc ON kc.object_id = i.object_id AND kc.index_id = i.index_id
LEFT JOIN inclcols ic ON ic.object_id = i.object_id AND ic.index_id = i.index_id
ORDER BY i.ObjectName;
"@

# DB options
$metadataOptionsQuery = @"
DECLARE @options TABLE ( OptionName NVARCHAR(256), OptionValue NVARCHAR(4000) );
INSERT INTO @options (OptionName, OptionValue)
SELECT 'collation_name', CAST(collation_name AS NVARCHAR(4000)) FROM sys.databases WHERE name = DB_NAME();
INSERT INTO @options (OptionName, OptionValue)
SELECT 'compatibility_level', CAST(compatibility_level AS NVARCHAR(4000)) FROM sys.databases WHERE name = DB_NAME();
INSERT INTO @options (OptionName, OptionValue)
SELECT 'containment_desc', CAST(containment_desc AS NVARCHAR(4000)) FROM sys.databases WHERE name = DB_NAME();
INSERT INTO @options (OptionName, OptionValue)
SELECT 'recovery_model_desc', CAST(recovery_model_desc AS NVARCHAR(4000)) FROM sys.databases WHERE name = DB_NAME();
INSERT INTO @options (OptionName, OptionValue)
SELECT 'page_verify_option_desc', CAST(page_verify_option_desc AS NVARCHAR(4000)) FROM sys.databases WHERE name = DB_NAME();
INSERT INTO @options (OptionName, OptionValue)
SELECT 'target_recovery_time_in_seconds', CAST(target_recovery_time_in_seconds AS NVARCHAR(4000)) FROM sys.databases WHERE name = DB_NAME();
INSERT INTO @options (OptionName, OptionValue)
SELECT 'is_auto_close_on', CAST(is_auto_close_on AS NVARCHAR(4000)) FROM sys.databases WHERE name = DB_NAME();
INSERT INTO @options (OptionName, OptionValue)
SELECT 'is_auto_shrink_on', CAST(is_auto_shrink_on AS NVARCHAR(4000)) FROM sys.databases WHERE name = DB_NAME();
INSERT INTO @options (OptionName, OptionValue)
SELECT 'is_auto_create_stats_on', CAST(is_auto_create_stats_on AS NVARCHAR(4000)) FROM sys.databases WHERE name = DB_NAME();
INSERT INTO @options (OptionName, OptionValue)
SELECT 'is_auto_update_stats_on', CAST(is_auto_update_stats_on AS NVARCHAR(4000)) FROM sys.databases WHERE name = DB_NAME();
INSERT INTO @options (OptionName, OptionValue)
SELECT 'is_auto_update_stats_async_on', CAST(is_auto_update_stats_async_on AS NVARCHAR(4000)) FROM sys.databases WHERE name = DB_NAME();
INSERT INTO @options (OptionName, OptionValue)
SELECT 'is_read_committed_snapshot_on', CAST(is_read_committed_snapshot_on AS NVARCHAR(4000)) FROM sys.databases WHERE name = DB_NAME();
INSERT INTO @options (OptionName, OptionValue)
SELECT 'snapshot_isolation_state_desc', CAST(snapshot_isolation_state_desc AS NVARCHAR(4000)) FROM sys.databases WHERE name = DB_NAME();
INSERT INTO @options (OptionName, OptionValue)
SELECT 'is_ansi_null_default_on', CAST(is_ansi_null_default_on AS NVARCHAR(4000)) FROM sys.databases WHERE name = DB_NAME();
INSERT INTO @options (OptionName, OptionValue)
SELECT 'is_ansi_nulls_on', CAST(is_ansi_nulls_on AS NVARCHAR(4000)) FROM sys.databases WHERE name = DB_NAME();
INSERT INTO @options (OptionName, OptionValue)
SELECT 'is_ansi_padding_on', CAST(is_ansi_padding_on AS NVARCHAR(4000)) FROM sys.databases WHERE name = DB_NAME();
INSERT INTO @options (OptionName, OptionValue)
SELECT 'is_ansi_warnings_on', CAST(is_ansi_warnings_on AS NVARCHAR(4000)) FROM sys.databases WHERE name = DB_NAME();
INSERT INTO @options (OptionName, OptionValue)
SELECT 'is_arithabort_on', CAST(is_arithabort_on AS NVARCHAR(4000)) FROM sys.databases WHERE name = DB_NAME();
INSERT INTO @options (OptionName, OptionValue)
SELECT 'is_concat_null_yields_null_on', CAST(is_concat_null_yields_null_on AS NVARCHAR(4000)) FROM sys.databases WHERE name = DB_NAME();
INSERT INTO @options (OptionName, OptionValue)
SELECT 'is_numeric_roundabort_on', CAST(is_numeric_roundabort_on AS NVARCHAR(4000)) FROM sys.databases WHERE name = DB_NAME();
INSERT INTO @options (OptionName, OptionValue)
SELECT 'is_quoted_identifier_on', CAST(is_quoted_identifier_on AS NVARCHAR(4000)) FROM sys.databases WHERE name = DB_NAME();
INSERT INTO @options (OptionName, OptionValue)
SELECT 'is_trustworthy_on', CAST(is_trustworthy_on AS NVARCHAR(4000)) FROM sys.databases WHERE name = DB_NAME();
INSERT INTO @options (OptionName, OptionValue)
SELECT 'is_cursor_close_on_commit_on', CAST(is_cursor_close_on_commit_on AS NVARCHAR(4000)) FROM sys.databases WHERE name = DB_NAME();
INSERT INTO @options (OptionName, OptionValue)
SELECT 'Scoped.' + name, CAST(value AS NVARCHAR(4000)) FROM sys.database_scoped_configurations;

IF EXISTS (SELECT 1 FROM sys.all_objects WHERE object_id = OBJECT_ID('sys.database_options'))
BEGIN
    INSERT INTO @options (OptionName, OptionValue)
    SELECT option_name, option_state_desc FROM sys.database_options;
END;

SELECT OptionName COLLATE DATABASE_DEFAULT AS OptionName,
       OptionValue COLLATE DATABASE_DEFAULT AS OptionValue
FROM @options;
"@

# Users
$userQuery = @"
SELECT name COLLATE DATABASE_DEFAULT AS ObjectName
FROM sys.database_principals
WHERE type IN ('S', 'U', 'G') AND principal_id > 4;
"@
#endregion

#region 04. Row counts
$sourceRowCounts = Invoke-DbMetadataQuery -Instance $SourceInstance -Database $SourceDatabase -Query $rowCountQuery
$targetRowCounts = Invoke-DbMetadataQuery -Instance $TargetInstance -Database $TargetDatabase -Query $rowCountQuery

$rowCountLookupSource = @{}
foreach ($row in $sourceRowCounts) { $rowCountLookupSource["$($row.SchemaName).$($row.TableName)"] = $row }
$rowCountLookupTarget = @{}
foreach ($row in $targetRowCounts) { $rowCountLookupTarget["$($row.SchemaName).$($row.TableName)"] = $row }

$tableRowResults = @()
$allTableKeys = @($rowCountLookupSource.Keys + $rowCountLookupTarget.Keys) | Sort-Object -Unique
foreach ($key in $allTableKeys) {
    $schema, $table = $key.Split('.', 2)
    $sourceValue = $rowCountLookupSource[$key]
    $targetValue = $rowCountLookupTarget[$key]
    if ($null -eq $sourceValue) { $result = $CharWarn + ' Missing in source' }
    elseif ($null -eq $targetValue) { $result = $CharWarn + ' Missing in target' }
    elseif ($sourceValue.RowCountValue -eq $targetValue.RowCountValue) { $result = $CharOK + ' Match' }
    else { $result = $CharFail + ' Different row count' }
    $tableRowResults += [PSCustomObject]@{
        Schema=$schema; Table=$table
        SourceRows = if ($sourceValue) { $sourceValue.RowCountValue } else { $null }
        TargetRows = if ($targetValue) { $targetValue.RowCountValue } else { $null }
        Result     = $result
    }
}
#endregion

#region 05. Column sets
$sourceColumns = Invoke-DbMetadataQuery -Instance $SourceInstance -Database $SourceDatabase -Query $columnQuery
$targetColumns = Invoke-DbMetadataQuery -Instance $TargetInstance -Database $TargetDatabase -Query $columnQuery

$columnLookupSource = @{}
foreach ($row in $sourceColumns) {
    $key = "$($row.TABLE_SCHEMA).$($row.TABLE_NAME)"
    if (-not $columnLookupSource.ContainsKey($key)) { $columnLookupSource[$key] = @() }
    $columnLookupSource[$key] += $row.COLUMN_NAME
}

$columnLookupTarget = @{}
foreach ($row in $targetColumns) {
    $key = "$($row.TABLE_SCHEMA).$($row.TABLE_NAME)"
    if (-not $columnLookupTarget.ContainsKey($key)) { $columnLookupTarget[$key] = @() }
    $columnLookupTarget[$key] += $row.COLUMN_NAME
}

$tableColumnResults = @()
$allColumnTableKeys = @($columnLookupSource.Keys + $columnLookupTarget.Keys) | Sort-Object -Unique
foreach ($key in $allColumnTableKeys) {
    $schema, $table = $key.Split('.', 2)
    $sourceList = if ($columnLookupSource[$key]) { @($columnLookupSource[$key] | Sort-Object) } else { @() }
    $targetList = if ($columnLookupTarget[$key]) { @($columnLookupTarget[$key] | Sort-Object) } else { @() }

    if     ($sourceList.Count -eq 0) { $result = $CharWarn + ' Missing table in source' }
    elseif ($targetList.Count -eq 0) { $result = $CharWarn + ' Missing table in target' }
    elseif ($sourceList.Count -eq $targetList.Count -and (@(Compare-Object -ReferenceObject $sourceList -DifferenceObject $targetList).Count -eq 0)) { $result = $CharOK + ' Columns match' }
    else { $result = $CharFail + ' Column mismatch' }

    $sourceOnly = @($sourceList | Where-Object { $targetList -notcontains $_ })
    $targetOnly = @($targetList | Where-Object { $sourceList -notcontains $_ })

    $tableColumnResults += [PSCustomObject]@{
        Schema=$schema; Table=$table
        SourceColumnCount = $sourceList.Count
        TargetColumnCount = $targetList.Count
        SourceOnlyColumns = if ($sourceOnly.Count) { ($sourceOnly -join ', ') } else { '' }
        TargetOnlyColumns = if ($targetOnly.Count) { ($targetOnly -join ', ') } else { '' }
        Result            = $result
    }
}
#endregion

#region 06. Object & definition comparisons (core + optional)
$objectComparisons = @()

# Synonym name existence
$objectComparisons += New-NameComparisonResult -ObjectType 'Synonym' `
    -SourceNames (Invoke-DbMetadataQuery -Instance $SourceInstance -Database $SourceDatabase -Query $synonymQuery | Select-Object -ExpandProperty ObjectName) `
    -TargetNames (Invoke-DbMetadataQuery -Instance $TargetInstance -Database $TargetDatabase -Query $synonymQuery | Select-Object -ExpandProperty ObjectName)

# Schemas / Views / Procedures / Functions
$objectComparisons += Get-DbaObjectComparison -ObjectType "Schema"    -Query $schemaQuery    -SourceInstance $SourceInstance -SourceDatabase $SourceDatabase -TargetInstance $TargetInstance -TargetDatabase $TargetDatabase
$objectComparisons += Get-DbaObjectComparison -ObjectType "View"      -Query $viewQuery      -SourceInstance $SourceInstance -SourceDatabase $SourceDatabase -TargetInstance $TargetInstance -TargetDatabase $TargetDatabase
$objectComparisons += Get-DbaObjectComparison -ObjectType "Procedure" -Query $procedureQuery -SourceInstance $SourceInstance -SourceDatabase $SourceDatabase -TargetInstance $TargetInstance -TargetDatabase $TargetDatabase
$objectComparisons += Get-DbaObjectComparison -ObjectType "Function"  -Query $functionQuery  -SourceInstance $SourceInstance -SourceDatabase $SourceDatabase -TargetInstance $TargetInstance -TargetDatabase $TargetDatabase

# Optional: triggers
if ($CompareTableTriggers) {
    $objectComparisons += Get-DbaObjectComparison -ObjectType "Trigger (Table)" -Query $tableTriggerQuery `
        -SourceInstance $SourceInstance -SourceDatabase $SourceDatabase -TargetInstance $TargetInstance -TargetDatabase $TargetDatabase
}
if ($CompareDbTriggers) {
    $objectComparisons += Get-DbaObjectComparison -ObjectType "Trigger (Database)" -Query $dbTriggerQuery `
        -SourceInstance $SourceInstance -SourceDatabase $SourceDatabase -TargetInstance $TargetInstance -TargetDatabase $TargetDatabase
}

# Optional: keys (PK + FK)
if ($CompareKeys) {
    $objectComparisons += Get-DbaObjectComparison -ObjectType "Primary Key" -Query $primaryKeyQuery `
        -SourceInstance $SourceInstance -SourceDatabase $SourceDatabase -TargetInstance $TargetInstance -TargetDatabase $TargetDatabase
    $objectComparisons += Get-DbaObjectComparison -ObjectType "Foreign Key" -Query $foreignKeyQuery `
        -SourceInstance $SourceInstance -SourceDatabase $SourceDatabase -TargetInstance $TargetInstance -TargetDatabase $TargetDatabase
}

# Optional: indexes
if ($CompareIndexes) {
    $objectComparisons += Get-DbaObjectComparison -ObjectType "Index" -Query $indexQuery `
        -SourceInstance $SourceInstance -SourceDatabase $SourceDatabase -TargetInstance $TargetInstance -TargetDatabase $TargetDatabase
}
#endregion

#region 07. Users and DB options
$userComparison = New-NameComparisonResult -ObjectType 'Database User' `
    -SourceNames (Invoke-DbMetadataQuery -Instance $SourceInstance -Database $SourceDatabase -Query $userQuery | Select-Object -ExpandProperty ObjectName) `
    -TargetNames (Invoke-DbMetadataQuery -Instance $TargetInstance -Database $TargetDatabase -Query $userQuery | Select-Object -ExpandProperty ObjectName)

$sourceOptions = Invoke-DbMetadataQuery -Instance $SourceInstance -Database $SourceDatabase -Query $metadataOptionsQuery
$targetOptions = Invoke-DbMetadataQuery -Instance $TargetInstance -Database $TargetDatabase -Query $metadataOptionsQuery

$optionLookupSource = @{}
foreach ($row in $sourceOptions) { if ($row.OptionName -and -not $optionLookupSource.ContainsKey($row.OptionName)) { $optionLookupSource[$row.OptionName] = $row.OptionValue } }
$optionLookupTarget = @{}
foreach ($row in $targetOptions) { if ($row.OptionName -and -not $optionLookupTarget.ContainsKey($row.OptionName)) { $optionLookupTarget[$row.OptionName] = $row.OptionValue } }

$optionComparison = @()
$allOptionNames = @($optionLookupSource.Keys + $optionLookupTarget.Keys) | Sort-Object -Unique
foreach ($optionName in $allOptionNames) {
    $sourceValue = if ($optionLookupSource.ContainsKey($optionName)) { $optionLookupSource[$optionName] } else { $null }
    $targetValue = if ($optionLookupTarget.ContainsKey($optionName)) { $optionLookupTarget[$optionName] } else { $null }

    if     ($null -eq $sourceValue -and $null -ne $targetValue) { $result = $CharDefault + ' Missing in source' }
    elseif ($null -ne $sourceValue -and $null -eq $targetValue) { $result = $CharWarn    + ' Missing in target' }
    elseif (("$sourceValue") -eq ("$targetValue"))              { $result = $CharOK      + ' Match' }
    else                                                        { $result = $CharFail    + ' Different value' }

    $displayName = Format-DatabaseOptionName -OptionName $optionName
    $optionComparison += [PSCustomObject]@{
        OptionName  = $displayName
        SourceValue = $sourceValue
        TargetValue = $targetValue
        Result      = $result
    }
}
#endregion

#region 08. HTML output
$styleBlock = @"
<style>
body { font-family: 'Segoe UI', Arial, sans-serif; margin: 20px; }
h1 { color: #2c3e50; }
h2 { color: #34495e; border-bottom: 1px solid #ccc; padding-bottom: 4px; }
table { border-collapse: collapse; width: 100%; margin-bottom: 20px; }
th, td { border: 1px solid #ddd; padding: 6px 8px; text-align: left; }
th { background-color: #f2f2f2; }
.toggle-container { margin: 16px 0; }
.toggle-link { display: inline-block; padding: 6px 14px; background-color: #3498db; color: #fff; text-decoration: none; border-radius: 4px; }
.toggle-link:hover, .toggle-link:focus { background-color: #2d83bf; color: #fff; }
body.hide-ok table tbody tr.status-ok { display: none; }
.ok { color: #16a34a; } .warn { color: #f59e0b; } .fail { color: #dc2626; } .def { color: #6b7280; }
</style>
"@

$scriptBlock = @"
<script>
document.addEventListener('DOMContentLoaded', function () {
  const toggleLink = document.getElementById('toggle-differences'); if (!toggleLink) return;
  const tables = Array.from(document.querySelectorAll('table'));
  tables.forEach(table => {
    const rows = Array.from(table.querySelectorAll('tr'));
    const head = rows.find(r => r.querySelectorAll('th').length > 0); if (!head) return;
    const headers = Array.from(head.querySelectorAll('th')).map(th => th.textContent.trim().toLowerCase());
    const resultIx = headers.indexOf('result'); if (resultIx === -1) return;
    rows.forEach(r => {
      if (r === head) return;
      const tds = r.querySelectorAll('td, th'); const cell = tds[resultIx]; if (!cell) return;
      const txt = cell.textContent.trim();
      if (/match/i.test(txt)) r.classList.add('status-ok'); else r.classList.add('status-diff');
    });
  });
  let showAll = false;
  const apply = () => { document.body.classList.toggle('hide-ok', !showAll); toggleLink.textContent = showAll ? 'Show differences' : 'Show all comparation'; };
  toggleLink.addEventListener('click', e => { e.preventDefault(); showAll = !showAll; apply(); });
  apply();
});
</script>
"@

$reportGeneratedOn = Get-Date
$toggleControl = @'
<div class="toggle-container">
  <a href="#" id="toggle-differences" class="toggle-link">Show all comparation</a>
</div>
'@

$reportSections = @()
$reportSections += $toggleControl
$reportSections += "<h1>Database Comparison Report</h1>"
$reportSections += "<p><strong>Source:</strong> $SourceInstance/$SourceDatabase</p>"
$reportSections += "<p><strong>Target:</strong> $TargetInstance/$TargetDatabase</p>"
$reportSections += "<p><strong>Generated:</strong> $($reportGeneratedOn.ToString('yyyy-MM-dd HH:mm:ss'))</p>"
$reportSections += New-HtmlSection -Title 'Table row counts' -Data $tableRowResults
$reportSections += New-HtmlSection -Title 'Table column definitions' -Data $tableColumnResults
$reportSections += New-HtmlSection -Title 'Object & Definition Comparison' -Data $objectComparisons
$reportSections += New-HtmlSection -Title 'Database users' -Data $userComparison
$reportSections += New-HtmlSection -Title 'Database configuration options' -Data $optionComparison

$headContent = $styleBlock + "`n" + $scriptBlock
$fullHtml = ConvertTo-Html -Title 'Database Comparison Report' -Head $headContent -Body ($reportSections -join "`n")
Set-Content -Path $OutputPath -Value $fullHtml -Encoding UTF8

Write-Host "Report generated at $OutputPath"
Start-Process $OutputPath
#endregion

}
