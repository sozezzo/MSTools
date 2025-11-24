function Compare-DbaDbObject {
<#
.SYNOPSIS
    Compares object presence and key metadata between two SQL Server databases and returns a list of differences.

.DESCRIPTION
    Compare-DbaDbObject inspects a broad set of database objects on a source and a destination:
      - Tables (presence + rowcount via sys.dm_db_partition_stats for heap/clustered rows)
      - Views, Functions, Stored Procedures, Synonyms, Triggers (DML and DB-level DDL)
      - Users (non-system principals by name)
      - Non-constraint indexes (excludes PK/UQ constraints)
      - Primary Keys, UNIQUE constraints, Foreign Keys
      - CHECK constraints and DEFAULT constraints

    It queries both databases, builds fast lookup maps (hash sets / dictionaries), and emits a
    collection of issue objects for anything missing or materially different (e.g., row count mismatch,
    index key/include list differences, constraint key differences, FK mappings, normalized definitions for
    CHECK/DEFAULT). The output is suitable for reporting or feeding follow-up remediation steps.

.PARAMETER SourceInstance
    SQL Server instance name (or connection string) for the source, e.g. 'SQL01' or 'SQL01\INST1'.

.PARAMETER SourceDatabase
    Source database name.

.PARAMETER DestInstance
    SQL Server instance name (or connection string) for the destination.

.PARAMETER DestDatabase
    Destination database name.

.INPUTS
    None. All inputs are provided via parameters.

.OUTPUTS
    PSCustomObject[]  (one object per detected issue)
      Properties (vary by ObjectType):
        ObjectType  : 'Table' | 'View' | 'Function' | 'Procedure' | 'Trigger' | 'Synonym' | 'User'
                      | 'Index' | 'PrimaryKey' | 'UniqueConstraint' | 'ForeignKey'
                      | 'CheckConstraint' | 'DefaultConstraint'
        Name        : Object name (schema-qualified when applicable; special 'DATABASE::...' for DB DDL triggers)
        Issue       : 'Missing' | 'RowCountMismatch' | 'MissingOrDifferent'
        Detail      : Additional text describing the difference (varies by type)
        SourceRows  : (Tables only) source rowcount (bigint)
        DestRows    : (Tables only) destination rowcount (bigint)

.EXAMPLES
    # 1) Basic comparison between two DBs on the same instance
    $issues = Compare-DbaDbObject `
        -SourceInstance "SQL01" -SourceDatabase "AppDB" `
        -DestInstance   "SQL01" -DestDatabase   "AppDB_Clone"
    $issues | Format-Table ObjectType, Name, Issue, Detail -Auto

    # 2) Cross-instance comparison, export to CSV
    Compare-DbaDbObject `
        -SourceInstance "Prod\OLTP" -SourceDatabase "HR" `
        -DestInstance   "Stage\OLTP" -DestDatabase   "HR_Stage" |
    Export-Csv "E:\reports\hr-db-diff.csv" -NoTypeInformation

    # 3) Filter only missing constraints
    $diff = Compare-DbaDbObject -SourceInstance "SQL01" -SourceDatabase "Sales" `
                                -DestInstance   "SQL02" -DestDatabase   "Sales_QA"
    $diff | Where-Object { $_.ObjectType -in 'PrimaryKey','UniqueConstraint','ForeignKey','CheckConstraint','DefaultConstraint' -and $_.Issue -ne 'RowCountMismatch' }

.NOTES
    Comparison details:
      - Tables: Presence + row counts (heap & clustered via sys.dm_db_partition_stats where index_id in (0,1)).
      - Views/Functions/Procedures/Synonyms/Triggers/Users: Presence by name (non-system only for Users).
      - Non-constraint Indexes: Compares table, index name, type, uniqueness, key columns (order), and included columns.
      - PK/UQ: Compares constraint name and ordered key columns.
      - Foreign Keys: Compares constraint name, from/to tables, and ordered column mappings.
      - CHECK/DEFAULT: Compares normalized definitions (CR/LF/TAB stripped and trimmed).

    Normalization:
      - CHECK/DEFAULT definitions are normalized by removing CR/LF/TAB and trimming to reduce false positives
        due to formatting.

    Dependencies:
      - dbatools: Invoke-DbaQuery
      - STRING_AGG is used in queries; requires SQL Server 2017 (compatibility level 140) or later.
        If targeting older versions, replace STRING_AGG logic with FOR XML PATH aggregation or STRING_ESCAPE alternatives.

    Performance:
      - Uses hash sets/dictionaries for O(1) lookups on destination objects.
      - Rowcount queries are lightweight but reflect committed rows only (not including disabled/nonclustered index partitions).

    Limitations:
      - Does not compare object definitions (T-SQL text) for programmables—only presence.
      - Does not compare permissions, ownership, extended properties, partition schemes, filegroups, or filtered index predicates.
      - Synonyms are compared by presence (name) only, not by target.

    Return semantics:
      - Always returns an array (possibly empty). To check for any differences:
            if ((Compare-DbaDbObject ...).Count) { 'Differences found' } else { 'No differences' }

.DEPENDENCIES
    dbatools module (https://dbatools.io/)
      - Invoke-DbaQuery

.LINK
    https://dbatools.io/
#>
	
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][string]$SourceInstance,
        [Parameter(Mandatory)][string]$SourceDatabase,
        [Parameter(Mandatory)][string]$DestInstance,
        [Parameter(Mandatory)][string]$DestDatabase
    )

    function Invoke-Q {
        param([string]$Instance,[string]$Database,[string]$Query)
        Invoke-DbaQuery -SqlInstance $Instance -Database $Database -Query $Query -EnableException
    }

    # ---------------- Existing object queries (yours) ----------------
    $qTables = @"
SELECT QUOTENAME(s.name)+'.'+QUOTENAME(t.name) AS [Name],
       SUM(p.row_count) AS [Rows]
FROM sys.dm_db_partition_stats p
JOIN sys.tables t  ON t.object_id = p.object_id
JOIN sys.schemas s ON s.schema_id = t.schema_id
WHERE p.index_id IN (0,1)
GROUP BY s.name, t.name;
"@

    $qViews = @"
SELECT QUOTENAME(s.name)+'.'+QUOTENAME(v.name) AS [Name]
FROM sys.views v
JOIN sys.schemas s ON s.schema_id = v.schema_id
WHERE v.is_ms_shipped = 0;
"@

    $qFunctions = @"
SELECT QUOTENAME(s.name)+'.'+QUOTENAME(o.name) AS [Name]
FROM sys.objects o
JOIN sys.schemas s ON s.schema_id = o.schema_id
WHERE o.type IN ('FN','IF','TF','FS','FT')
  AND o.is_ms_shipped = 0;
"@

    $qProcedures = @"
SELECT QUOTENAME(s.name)+'.'+QUOTENAME(p.name) AS [Name]
FROM sys.procedures p
JOIN sys.schemas s ON s.schema_id = p.schema_id
WHERE p.is_ms_shipped = 0;
"@

    $qTriggers = @"
-- DML triggers (table-level)
SELECT QUOTENAME(OBJECT_SCHEMA_NAME(tr.object_id)) + '.' + QUOTENAME(tr.name) AS [Name]
FROM sys.triggers tr
JOIN sys.tables   t  ON tr.parent_id = t.object_id
WHERE tr.is_ms_shipped = 0
UNION ALL
-- DDL (database) triggers
SELECT 'DATABASE::' + QUOTENAME(tr.name) AS [Name]
FROM sys.triggers tr
WHERE tr.parent_id = 0 AND tr.is_ms_shipped = 0;
"@

    $qSynonyms = @"
SELECT QUOTENAME(s.name)+'.'+QUOTENAME(sn.name) AS [Name]
FROM sys.synonyms sn
JOIN sys.schemas s ON s.schema_id = sn.schema_id;
"@

    # ---------------- New queries: users & relational objects ----------------

    # Non-system users (assumes you only care about presence by name)
    $qUsers = @"
SELECT dp.name AS [Name]
FROM sys.database_principals dp
WHERE dp.type IN ('S','U','G','E')         -- SQL user, Windows user, Windows group, External user
  AND dp.name NOT IN ('dbo','guest','INFORMATION_SCHEMA','sys')
  AND dp.name NOT LIKE '##MS_%' ESCAPE '\'
"@

    # Non-constraint indexes only (exclude PK & unique constraints)
    $qIndexes = @"
WITH idx AS (
  SELECT
    QUOTENAME(s.name)+'.'+QUOTENAME(t.name) AS TableName,
    i.name AS IndexName,
    i.index_id,
    i.type_desc AS IndexType,
    i.is_unique AS IsUnique,
    i.is_unique_constraint AS IsUniqueConstraint,
    i.is_primary_key AS IsPrimaryKey
  FROM sys.indexes i
  JOIN sys.tables t  ON t.object_id = i.object_id
  JOIN sys.schemas s ON s.schema_id = t.schema_id
  WHERE i.is_hypothetical = 0
    AND i.index_id > 0
    AND i.is_primary_key = 0
    AND i.is_unique_constraint = 0
)
SELECT
  TableName,
  IndexName,
  IndexType,
  IsUnique,
  (
    SELECT STRING_AGG(QUOTENAME(c.name),'|') WITHIN GROUP (ORDER BY ic.key_ordinal)
    FROM sys.index_columns ic
    JOIN sys.columns c
      ON c.object_id = ic.object_id AND c.column_id = ic.column_id
    WHERE ic.object_id = OBJECT_ID(TableName)
      AND ic.index_id  = (SELECT TOP 1 index_id FROM sys.indexes WHERE name = idx.IndexName AND object_id = OBJECT_ID(TableName))
      AND ic.is_included_column = 0
  ) AS KeyCols,
  (
    SELECT STRING_AGG(QUOTENAME(c.name),'|') WITHIN GROUP (ORDER BY c.column_id)
    FROM sys.index_columns ic
    JOIN sys.columns c
      ON c.object_id = ic.object_id AND c.column_id = ic.column_id
    WHERE ic.object_id = OBJECT_ID(TableName)
      AND ic.index_id  = (SELECT TOP 1 index_id FROM sys.indexes WHERE name = idx.IndexName AND object_id = OBJECT_ID(TableName))
      AND ic.is_included_column = 1
  ) AS IncludeCols
FROM idx
ORDER BY TableName, IndexName;
"@

    # Primary keys (by constraint)
    $qPK = @"
SELECT
  QUOTENAME(s.name)+'.'+QUOTENAME(t.name) AS TableName,
  kc.name AS ConstraintName,
  STRING_AGG(QUOTENAME(c.name),'|') WITHIN GROUP (ORDER BY ic.key_ordinal) AS KeyCols
FROM sys.key_constraints kc
JOIN sys.tables t  ON t.object_id = kc.parent_object_id
JOIN sys.schemas s ON s.schema_id = t.schema_id
JOIN sys.index_columns ic ON ic.object_id = t.object_id AND ic.index_id = kc.unique_index_id
JOIN sys.columns c ON c.object_id = ic.object_id AND c.column_id = ic.column_id
WHERE kc.type = 'PK'
GROUP BY s.name,t.name,kc.name
ORDER BY TableName, ConstraintName;
"@

    # UNIQUE constraints (not plain unique indexes)
    $qUQ = @"
SELECT
  QUOTENAME(s.name)+'.'+QUOTENAME(t.name) AS TableName,
  kc.name AS ConstraintName,
  STRING_AGG(QUOTENAME(c.name),'|') WITHIN GROUP (ORDER BY ic.key_ordinal) AS KeyCols
FROM sys.key_constraints kc
JOIN sys.tables t  ON t.object_id = kc.parent_object_id
JOIN sys.schemas s ON s.schema_id = t.schema_id
JOIN sys.index_columns ic ON ic.object_id = t.object_id AND ic.index_id = kc.unique_index_id
JOIN sys.columns c ON c.object_id = ic.object_id AND c.column_id = ic.column_id
WHERE kc.type = 'UQ'
GROUP BY s.name,t.name,kc.name
ORDER BY TableName, ConstraintName;
"@

    # Foreign keys (with full mapping and ordinals)
    $qFK = @"
SELECT
  fk.name AS ConstraintName,
  QUOTENAME(sSrc.name)+'.'+QUOTENAME(tSrc.name) AS FromTable,
  QUOTENAME(sRef.name)+'.'+QUOTENAME(tRef.name) AS ToTable,
  STRING_AGG(QUOTENAME(cSrc.name),'|') WITHIN GROUP (ORDER BY fkc.constraint_column_id) AS FromCols,
  STRING_AGG(QUOTENAME(cRef.name),'|') WITHIN GROUP (ORDER BY fkc.constraint_column_id) AS ToCols
FROM sys.foreign_keys fk
JOIN sys.foreign_key_columns fkc ON fkc.constraint_object_id = fk.object_id
JOIN sys.tables tSrc  ON tSrc.object_id = fk.parent_object_id
JOIN sys.schemas sSrc ON sSrc.schema_id = tSrc.schema_id
JOIN sys.columns cSrc ON cSrc.object_id = fkc.parent_object_id AND cSrc.column_id = fkc.parent_column_id
JOIN sys.tables tRef  ON tRef.object_id = fk.referenced_object_id
JOIN sys.schemas sRef ON sRef.schema_id = tRef.schema_id
JOIN sys.columns cRef ON cRef.object_id = fkc.referenced_object_id AND cRef.column_id = fkc.referenced_column_id
GROUP BY fk.name, sSrc.name, tSrc.name, sRef.name, tRef.name
ORDER BY FromTable, ConstraintName;
"@

    # CHECK constraints
    $qCheck = @"
SELECT
  QUOTENAME(s.name)+'.'+QUOTENAME(t.name) AS TableName,
  cc.name AS ConstraintName,
  LTRIM(RTRIM(REPLACE(REPLACE(REPLACE(cc.definition,CHAR(13),''),CHAR(10),''),CHAR(9),' '))) AS DefinitionNorm
FROM sys.check_constraints cc
JOIN sys.tables t  ON t.object_id = cc.parent_object_id
JOIN sys.schemas s ON s.schema_id = t.schema_id
ORDER BY TableName, ConstraintName;
"@

    # DEFAULT constraints
    $qDefault = @"
SELECT
  QUOTENAME(s.name)+'.'+QUOTENAME(t.name) AS TableName,
  dc.name AS ConstraintName,
  QUOTENAME(c.name) AS ColumnName,
  LTRIM(RTRIM(REPLACE(REPLACE(REPLACE(dc.definition,CHAR(13),''),CHAR(10),''),CHAR(9),' '))) AS DefinitionNorm
FROM sys.default_constraints dc
JOIN sys.tables t  ON t.object_id = dc.parent_object_id
JOIN sys.schemas s ON s.schema_id = t.schema_id
JOIN sys.columns c ON c.object_id = t.object_id AND c.column_id = dc.parent_column_id
ORDER BY TableName, ConstraintName;
"@

    # ---------------- pull source/destination data ----------------
    $srcTables   = Invoke-Q $SourceInstance $SourceDatabase $qTables
    $dstTables   = Invoke-Q $DestInstance   $DestDatabase   $qTables
    $srcViews    = Invoke-Q $SourceInstance $SourceDatabase $qViews
    $dstViews    = Invoke-Q $DestInstance   $DestDatabase   $qViews
    $srcFuncs    = Invoke-Q $SourceInstance $SourceDatabase $qFunctions
    $dstFuncs    = Invoke-Q $DestInstance   $DestDatabase   $qFunctions
    $srcProcs    = Invoke-Q $SourceInstance $SourceDatabase $qProcedures
    $dstProcs    = Invoke-Q $DestInstance   $DestDatabase   $qProcedures
    $srcTriggers = Invoke-Q $SourceInstance $SourceDatabase $qTriggers
    $dstTriggers = Invoke-Q $DestInstance   $DestDatabase   $qTriggers
    $srcSynonyms = Invoke-Q $SourceInstance $SourceDatabase $qSynonyms
    $dstSynonyms = Invoke-Q $DestInstance   $DestDatabase   $qSynonyms

    $srcUsers    = Invoke-Q $SourceInstance $SourceDatabase $qUsers
    $dstUsers    = Invoke-Q $DestInstance   $DestDatabase   $qUsers

    $srcIdx      = Invoke-Q $SourceInstance $SourceDatabase $qIndexes
    $dstIdx      = Invoke-Q $DestInstance   $DestDatabase   $qIndexes

    $srcPK       = Invoke-Q $SourceInstance $SourceDatabase $qPK
    $dstPK       = Invoke-Q $DestInstance   $DestDatabase   $qPK

    $srcUQ       = Invoke-Q $SourceInstance $SourceDatabase $qUQ
    $dstUQ       = Invoke-Q $DestInstance   $DestDatabase   $qUQ

    $srcFK       = Invoke-Q $SourceInstance $SourceDatabase $qFK
    $dstFK       = Invoke-Q $DestInstance   $DestDatabase   $qFK

    $srcCheck    = Invoke-Q $SourceInstance $SourceDatabase $qCheck
    $dstCheck    = Invoke-Q $DestInstance   $DestDatabase   $qCheck

    $srcDefault  = Invoke-Q $SourceInstance $SourceDatabase $qDefault
    $dstDefault  = Invoke-Q $DestInstance   $DestDatabase   $qDefault

    # ---------------- build fast lookup sets/maps ----------------
    $dstTableMap = @{}
    foreach ($r in $dstTables) { $dstTableMap[$r.Name] = [int64]$r.Rows }

    $dstSet = @{
        View      = [System.Collections.Generic.HashSet[string]]::new()
        Function  = [System.Collections.Generic.HashSet[string]]::new()
        Procedure = [System.Collections.Generic.HashSet[string]]::new()
        Trigger   = [System.Collections.Generic.HashSet[string]]::new()
        Synonym   = [System.Collections.Generic.HashSet[string]]::new()
        User      = [System.Collections.Generic.HashSet[string]]::new()
    }
    foreach ($r in $dstViews)    { $null = $dstSet.View.Add($r.Name) }
    foreach ($r in $dstFuncs)    { $null = $dstSet.Function.Add($r.Name) }
    foreach ($r in $dstProcs)    { $null = $dstSet.Procedure.Add($r.Name) }
    foreach ($r in $dstTriggers) { $null = $dstSet.Trigger.Add($r.Name) }
    foreach ($r in $dstSynonyms) { $null = $dstSet.Synonym.Add($r.Name) }
    foreach ($r in $dstUsers)    { $null = $dstSet.User.Add($r.Name) }

    # Dictionaries for relational objects (build canonical keys)
    function mk($args) { ($args -join '||').ToLowerInvariant() }

    $dstIdxMap = @{}   # key: Table|Index|Type|KeyCols|IncludeCols|IsUnique
    foreach ($r in $dstIdx) {
        $k = mk @($r.TableName, $r.IndexName, $r.IndexType, $r.KeyCols, $r.IncludeCols, [int]$r.IsUnique)
        $dstIdxMap[$k] = $true
    }

    $dstPKMap = @{}   # key: Table|Constraint|KeyCols
    foreach ($r in $dstPK) {
        $k = mk @($r.TableName, $r.ConstraintName, $r.KeyCols)
        $dstPKMap[$k] = $true
    }

    $dstUQMap = @{}   # key: Table|Constraint|KeyCols
    foreach ($r in $dstUQ) {
        $k = mk @($r.TableName, $r.ConstraintName, $r.KeyCols)
        $dstUQMap[$k] = $true
    }

    $dstFKMap = @{}   # key: Constraint|FromTable|ToTable|FromCols|ToCols
    foreach ($r in $dstFK) {
        $k = mk @($r.ConstraintName, $r.FromTable, $r.ToTable, $r.FromCols, $r.ToCols)
        $dstFKMap[$k] = $true
    }

    $dstCheckMap = @{}   # key: Table|Constraint|Def
    foreach ($r in $dstCheck) {
        $k = mk @($r.TableName, $r.ConstraintName, $r.DefinitionNorm)
        $dstCheckMap[$k] = $true
    }

    $dstDefaultMap = @{} # key: Table|Constraint|Column|Def
    foreach ($r in $dstDefault) {
        $k = mk @($r.TableName, $r.ConstraintName, $r.ColumnName, $r.DefinitionNorm)
        $dstDefaultMap[$k] = $true
    }

    # ---------------- compare & collect issues ----------------
    $issues = New-Object System.Collections.Generic.List[object]

    # Tables + rowcount
    foreach ($t in $srcTables) {
        $name = $t.Name
        $srcRows = [int64]$t.Rows
        if (-not $dstTableMap.ContainsKey($name)) {
            $issues.Add([pscustomobject]@{ ObjectType='Table'; Name=$name; Issue='Missing'; SourceRows=$srcRows; DestRows=$null })
        } else {
            $dstRows = [int64]$dstTableMap[$name]
            if ($srcRows -ne $dstRows) {
                $issues.Add([pscustomobject]@{ ObjectType='Table'; Name=$name; Issue='RowCountMismatch'; SourceRows=$srcRows; DestRows=$dstRows })
            }
        }
    }

    foreach ($v in $srcViews)     { if (-not $dstSet.View.Contains($v.Name))       { $issues.Add([pscustomobject]@{ ObjectType='View';      Name=$v.Name; Issue='Missing' }) } }
    foreach ($f in $srcFuncs)     { if (-not $dstSet.Function.Contains($f.Name))   { $issues.Add([pscustomobject]@{ ObjectType='Function';  Name=$f.Name; Issue='Missing' }) } }
    foreach ($p in $srcProcs)     { if (-not $dstSet.Procedure.Contains($p.Name))  { $issues.Add([pscustomobject]@{ ObjectType='Procedure'; Name=$p.Name; Issue='Missing' }) } }
    foreach ($tr in $srcTriggers) { if (-not $dstSet.Trigger.Contains($tr.Name))   { $issues.Add([pscustomobject]@{ ObjectType='Trigger';   Name=$tr.Name; Issue='Missing' }) } }
    foreach ($sn in $srcSynonyms) { if (-not $dstSet.Synonym.Contains($sn.Name))   { $issues.Add([pscustomobject]@{ ObjectType='Synonym';   Name=$sn.Name; Issue='Missing' }) } }

    # Users
    foreach ($u in $srcUsers)     { if (-not $dstSet.User.Contains($u.Name))       { $issues.Add([pscustomobject]@{ ObjectType='User';      Name=$u.Name; Issue='Missing' }) } }

    # Indexes (non-constraint)
    foreach ($r in $srcIdx) {
        $k = mk @($r.TableName, $r.IndexName, $r.IndexType, $r.KeyCols, $r.IncludeCols, [int]$r.IsUnique)
        if (-not $dstIdxMap.ContainsKey($k)) {
            $issues.Add([pscustomobject]@{
                ObjectType='Index'; Name="$($r.TableName).$($r.IndexName)"; Issue='MissingOrDifferent';
                Detail=("Type={0}; Key=({1}); Include=({2}); Unique={3}" -f $r.IndexType,$r.KeyCols,$r.IncludeCols,$r.IsUnique)
            })
        }
    }

    # PK
    foreach ($r in $srcPK) {
        $k = mk @($r.TableName, $r.ConstraintName, $r.KeyCols)
        if (-not $dstPKMap.ContainsKey($k)) {
            $issues.Add([pscustomobject]@{
                ObjectType='PrimaryKey'; Name="$($r.TableName).$($r.ConstraintName)"; Issue='MissingOrDifferent';
                Detail=("Key=({0})" -f $r.KeyCols)
            })
        }
    }

    # UNIQUE constraints
    foreach ($r in $srcUQ) {
        $k = mk @($r.TableName, $r.ConstraintName, $r.KeyCols)
        if (-not $dstUQMap.ContainsKey($k)) {
            $issues.Add([pscustomobject]@{
                ObjectType='UniqueConstraint'; Name="$($r.TableName).$($r.ConstraintName)"; Issue='MissingOrDifferent';
                Detail=("Key=({0})" -f $r.KeyCols)
            })
        }
    }

    # FKs
    foreach ($r in $srcFK) {
        $k = mk @($r.ConstraintName, $r.FromTable, $r.ToTable, $r.FromCols, $r.ToCols)
        if (-not $dstFKMap.ContainsKey($k)) {
            $issues.Add([pscustomobject]@{
                ObjectType='ForeignKey'; Name=$r.ConstraintName; Issue='MissingOrDifferent';
                Detail=("From {0} ({1}) -> {2} ({3})" -f $r.FromTable,$r.FromCols,$r.ToTable,$r.ToCols)
            })
        }
    }

    # CHECK
    foreach ($r in $srcCheck) {
        $k = mk @($r.TableName, $r.ConstraintName, $r.DefinitionNorm)
        if (-not $dstCheckMap.ContainsKey($k)) {
            $issues.Add([pscustomobject]@{
                ObjectType='CheckConstraint'; Name="$($r.TableName).$($r.ConstraintName)"; Issue='MissingOrDifferent';
                Detail=("Def={0}" -f $r.DefinitionNorm)
            })
        }
    }

    # DEFAULT
    foreach ($r in $srcDefault) {
        $k = mk @($r.TableName, $r.ConstraintName, $r.ColumnName, $r.DefinitionNorm)
        if (-not $dstDefaultMap.ContainsKey($k)) {
            $issues.Add([pscustomobject]@{
                ObjectType='DefaultConstraint'; Name="$($r.TableName).$($r.ConstraintName)"; Issue='MissingOrDifferent';
                Detail=("Col={0}; Def={1}" -f $r.ColumnName,$r.DefinitionNorm)
            })
        }
    }

    return ,$issues.ToArray()
}