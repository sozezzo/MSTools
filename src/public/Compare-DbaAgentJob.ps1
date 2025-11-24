function Compare-DbaAgentJob {
<#
.SYNOPSIS
  Compares SQL Agent jobs between two instances (ignoring Enabled and other volatile noise).
  Returns:
    - All jobs that are Missing or Different (always),
    - Plus jobs that are Same only when disabled on Source or Destination.

.DESCRIPTION
  - Loads jobs from Source/Destination using dbatools (Get-DbaAgentJob).
  - Builds a normalized model (job properties, steps, schedules) excluding VersionNumber.
  - Ignores Enabled in the comparison by default.
  - Case-insensitive normalization by default.
  - Output columns include IsEnabled and Category from the **Source only**.

.PARAMETER Source
  Source SQL instance.

.PARAMETER Destination
  Destination SQL instance.

.PARAMETER Job
  Optional filter by job name(s).

.PARAMETER Category
  Optional filter by category name(s).

.PARAMETER IgnoreEnabled
  Ignore the Enabled flag in comparison (default: $true).

.PARAMETER CaseInsensitive
  Compare identifiers case-insensitively (default: $true).

.OUTPUTS
  PSCustomObject:
    InstanceSource, InstanceDestination, JobName, Status, Differences,
    SourceHash, DestinationHash, IsEnabled, Category
  NOTE: IsEnabled and Category are taken **only from Source** (null if job is missing on Source).

.NOTES
  Author : Sozezzo Astra
  Version: 1.3.3
#>
  [CmdletBinding()]
  param(
    [Parameter(Mandatory)] [object] $Source,
    [Parameter(Mandatory)] [object] $Destination,
    [string[]] $Job,
    [string[]] $Category,
    [switch]  $IgnoreEnabled = $true,
    [switch]  $CaseInsensitive = $true
  )

  function Write-LogSafe {
    param([string]$Message, [string]$Level = 'Info')
    if (Get-Command -Name Write-Log -ErrorAction SilentlyContinue) {
      try { Write-Log -Message $Message -Level $Level } catch { Write-Verbose $Message }
    } else { Write-Verbose $Message }
  }

  function Get-StringHash {
    param([Parameter(Mandatory)][string]$Text)
    $bytes = [System.Text.Encoding]::UTF8.GetBytes($Text)
    $sha   = [System.Security.Cryptography.SHA256]::Create()
    ($sha.ComputeHash($bytes) | ForEach-Object { $_.ToString('x2') }) -join ''
  }

  function Normalize-Whitespace {
    param([string]$Text)
    if ([string]::IsNullOrWhiteSpace($Text)) { return '' }
    (($Text -replace "`r`n","`n") -replace "`r","`n") -replace '\s+',' ' | ForEach-Object { $_.Trim() }
  }

  function NormId {
    param([string]$Text, [switch]$CaseInsensitive)
    $t = Normalize-Whitespace $Text
    if ($CaseInsensitive) { return $t.ToLowerInvariant() }
    return $t
  }

  function Build-JobModelObject {
    param(
      [Parameter(Mandatory)] $JobObject,
      [switch]               $IncludeEnabled,
      [switch]               $CaseInsensitive
    )
    $job = $JobObject

    # Exclude VersionNumber (volatile). Enabled can be included conditionally.
    $model = [ordered]@{
      Name                 = NormId $job.Name -CaseInsensitive:$CaseInsensitive
      Category             = NormId $job.Category -CaseInsensitive:$CaseInsensitive
      Description          = Normalize-Whitespace $job.Description
      OwnerLoginName       = NormId $job.OwnerLoginName -CaseInsensitive:$CaseInsensitive
      EmailLevel           = $job.EmailLevel
      NetSendLevel         = $job.NetSendLevel
      PageLevel            = $job.PageLevel
      DeleteLevel          = $job.DeleteLevel
      NotifyEmailOperator  = NormId $job.OperatorToEmail -CaseInsensitive:$CaseInsensitive
      NotifyNetSendOperator= NormId $job.OperatorToNetSend -CaseInsensitive:$CaseInsensitive
      NotifyPageOperator   = NormId $job.OperatorToPage -CaseInsensitive:$CaseInsensitive
      StartStepName        = NormId $job.StartStepName -CaseInsensitive:$CaseInsensitive
      Steps                = @()
      Schedules            = @()
    }
    if ($IncludeEnabled) { $model.Enabled = [bool]$job.IsEnabled }

    foreach ($s in ($job.JobSteps | Sort-Object Id)) {
      $model.Steps += [ordered]@{
        Id                = $s.Id
        Name              = NormId $s.Name -CaseInsensitive:$CaseInsensitive
        SubSystem         = $s.SubSystem
        DatabaseName      = NormId $s.DatabaseName -CaseInsensitive:$CaseInsensitive
        Command           = Normalize-Whitespace $s.Command
        OnSuccessAction   = $s.OnSuccessAction
        OnSuccessStep     = $s.OnSuccessStep
        OnFailAction      = $s.OnFailAction
        OnFailStep        = $s.OnFailStep
        RetryAttempts     = $s.RetryAttempts
        RetryInterval     = $s.RetryInterval
        OutputFileName    = Normalize-Whitespace $s.OutputFileName
        ProxyName         = NormId $s.ProxyName -CaseInsensitive:$CaseInsensitive
      }
    }

    foreach ($sch in ($job.JobSchedules | Sort-Object Name)) {
      $js = $sch.JobSchedule
      $model.Schedules += [ordered]@{
        Name                        = NormId $js.Name -CaseInsensitive:$CaseInsensitive
        FrequencyTypes              = $js.FrequencyTypes
        FrequencyInterval           = $js.FrequencyInterval
        FrequencySubDayTypes        = $js.FrequencySubDayTypes
        FrequencySubDayInterval     = $js.FrequencySubDayInterval
        FrequencyRelativeIntervals  = $js.FrequencyRelativeIntervals
        FrequencyRecurrenceFactor   = $js.FrequencyRecurrenceFactor
        ActiveStartDate             = $js.ActiveStartDate
        ActiveEndDate               = $js.ActiveEndDate
        ActiveStartTimeOfDay        = $js.ActiveStartTimeOfDay
        ActiveEndTimeOfDay          = $js.ActiveEndTimeOfDay
      }
    }

    [pscustomobject]$model
  }

  function Get-JobModelJson {
    param(
      [Parameter(Mandatory)] $JobObject,
      [switch] $IncludeEnabled,
      [switch] $CaseInsensitive
    )
    (Build-JobModelObject -JobObject $JobObject -IncludeEnabled:$IncludeEnabled -CaseInsensitive:$CaseInsensitive) |
      ConvertTo-Json -Depth 10
  }

  # Source-only output helpers
  function Out-SourceIsEnabled { param($s) if ($s) { return [bool]$s.IsEnabled } else { return $null } }
  function Out-SourceCategory { param($s) if ($s) { return $s.Category } else { return $null } }

  function IsDisabledAnywhere { param($s,$d)
    if ($s -and -not $s.IsEnabled) { return $true }
    if ($d -and -not $d.IsEnabled) { return $true }
    return $false
  }

  if (-not (Get-Command Get-DbaAgentJob -ErrorAction SilentlyContinue)) {
    throw "dbatools is required (Get-DbaAgentJob). Please ensure dbatools is loaded."
  }

  Write-LogSafe "Loading jobs from [$Source] and [$Destination]..."

  $srcJobs = Get-DbaAgentJob -SqlInstance $Source -EnableException:$false
  $dstJobs = Get-DbaAgentJob -SqlInstance $Destination -EnableException:$false

  if ($Job) {
    $srcJobs = $srcJobs | Where-Object { $_.Name -in $Job }
    $dstJobs = $dstJobs | Where-Object { $_.Name -in $Job }
  }
  if ($Category) {
    $srcJobs = $srcJobs | Where-Object { $_.Category -in $Category }
    $dstJobs = $dstJobs | Where-Object { $_.Category -in $Category }
  }

  # Case-insensitive name index
  $srcMap = @{}
  foreach ($j in $srcJobs) { $srcMap[$j.Name.ToLower()] = $j }
  $dstMap = @{}
  foreach ($j in $dstJobs) { $dstMap[$j.Name.ToLower()] = $j }

  $allNames = @($srcMap.Keys + $dstMap.Keys) | Sort-Object -Unique
  $results  = New-Object System.Collections.Generic.List[object]

  foreach ($nameKey in $allNames) {
    $src = $srcMap[$nameKey]
    $dst = $dstMap[$nameKey]

    if     ($src) { $jobName = $src.Name }
    elseif ($dst) { $jobName = $dst.Name }
    else          { $jobName = $nameKey }

    # Missing on one side — always include
    if (-not $src) {
      $results.Add([pscustomobject]@{
        InstanceSource      = "$Source"
        InstanceDestination = "$Destination"
        JobName             = $jobName
        Status              = 'OnlyOnDestination'
        Differences         = @('Job missing on Source')
        SourceHash          = $null
        DestinationHash     = Get-StringHash (Get-JobModelJson -JobObject $dst -IncludeEnabled:(!$IgnoreEnabled) -CaseInsensitive:$CaseInsensitive)
        IsEnabled           = (Out-SourceIsEnabled $src)   # null when missing on Source
        Category            = (Out-SourceCategory $src)    # null when missing on Source
      })
      continue
    }
    if (-not $dst) {
      $results.Add([pscustomobject]@{
        InstanceSource      = "$Source"
        InstanceDestination = "$Destination"
        JobName             = $jobName
        Status              = 'OnlyOnSource'
        Differences         = @('Job missing on Destination')
        SourceHash          = Get-StringHash (Get-JobModelJson -JobObject $src -IncludeEnabled:(!$IgnoreEnabled) -CaseInsensitive:$CaseInsensitive)
        DestinationHash     = $null
        IsEnabled           = (Out-SourceIsEnabled $src)
        Category            = (Out-SourceCategory $src)
      })
      continue
    }

    # Compare normalized models (Enabled excluded by default)
    $srcJson = Get-JobModelJson -JobObject $src -IncludeEnabled:(!$IgnoreEnabled) -CaseInsensitive:$CaseInsensitive
    $dstJson = Get-JobModelJson -JobObject $dst -IncludeEnabled:(!$IgnoreEnabled) -CaseInsensitive:$CaseInsensitive

    $srcHash = Get-StringHash $srcJson
    $dstHash = Get-StringHash $dstJson

    if ($srcHash -eq $dstHash) {
      # Include SAME only if disabled somewhere (source or destination)
      if (IsDisabledAnywhere $src $dst) {
        $results.Add([pscustomobject]@{
          InstanceSource      = "$Source"
          InstanceDestination = "$Destination"
          JobName             = $jobName
          Status              = 'Same'
          Differences         = @()
          SourceHash          = $srcHash
          DestinationHash     = $dstHash
          IsEnabled           = (Out-SourceIsEnabled $src)
          Category            = (Out-SourceCategory $src)
        })
      }
      continue
    }

    # Different — always include
    $diffs = New-Object System.Collections.Generic.List[string]
    if ($src.OwnerLoginName -ne $dst.OwnerLoginName) { $null = $diffs.Add('OwnerLoginName') }
    if ($src.Category       -ne $dst.Category)       { $null = $diffs.Add('Category') }
    if ($src.JobSteps.Count -ne $dst.JobSteps.Count) { $null = $diffs.Add('StepsCount') }
    if ($src.JobSchedules.Count -ne $dst.JobSchedules.Count) { $null = $diffs.Add('SchedulesCount') }

    if ($src.JobSteps.Count -eq $dst.JobSteps.Count) {
      $srcSteps = $src.JobSteps | Sort-Object Id
      $dstSteps = $dst.JobSteps | Sort-Object Id
      for ($i=0; $i -lt $srcSteps.Count; $i++) {
        $a = $srcSteps[$i]; $b = $dstSteps[$i]
        if ((NormId $a.Name -CaseInsensitive:$CaseInsensitive) -ne (NormId $b.Name -CaseInsensitive:$CaseInsensitive)) { $null = $diffs.Add("StepId[$($a.Id)].Name") }
        if ($a.SubSystem -ne $b.SubSystem)                                           { $null = $diffs.Add("StepId[$($a.Id)].SubSystem") }
        if ((NormId $a.DatabaseName -CaseInsensitive:$CaseInsensitive) -ne (NormId $b.DatabaseName -CaseInsensitive:$CaseInsensitive)) { $null = $diffs.Add("StepId[$($a.Id)].DatabaseName") }
        if ((Normalize-Whitespace $a.Command) -ne (Normalize-Whitespace $b.Command)) { $null = $diffs.Add("StepId[$($a.Id)].Command") }
        if ($a.OnSuccessAction -ne $b.OnSuccessAction)                               { $null = $diffs.Add("StepId[$($a.Id)].OnSuccessAction") }
        if ($a.OnFailAction -ne $b.OnFailAction)                                     { $null = $diffs.Add("StepId[$($a.Id)].OnFailAction") }
        if ($a.RetryAttempts -ne $b.RetryAttempts)                                   { $null = $diffs.Add("StepId[$($a.Id)].RetryAttempts") }
        if ($a.RetryInterval -ne $b.RetryInterval)                                   { $null = $diffs.Add("StepId[$($a.Id)].RetryInterval") }
        if ((NormId $a.ProxyName -CaseInsensitive:$CaseInsensitive) -ne (NormId $b.ProxyName -CaseInsensitive:$CaseInsensitive)) { $null = $diffs.Add("StepId[$($a.Id)].ProxyName") }
      }
    }

    if ($src.JobSchedules.Count -eq $dst.JobSchedules.Count) {
      $srcSch = $src.JobSchedules | Sort-Object Name
      $dstSch = $dst.JobSchedules | Sort-Object Name
      for ($j=0; $j -lt $srcSch.Count; $j++) {
        $sa = $srcSch[$j].JobSchedule; $sb = $dstSch[$j].JobSchedule
        if ((NormId $sa.Name -CaseInsensitive:$CaseInsensitive) -ne (NormId $sb.Name -CaseInsensitive:$CaseInsensitive)) { $null = $diffs.Add("Schedule[$($sa.Name)].Name") }
        if ($sa.FrequencyTypes -ne $sb.FrequencyTypes)                               { $null = $diffs.Add("Schedule[$($sa.Name)].FrequencyTypes") }
        if ($sa.FrequencyInterval -ne $sb.FrequencyInterval)                         { $null = $diffs.Add("Schedule[$($sa.Name)].FrequencyInterval") }
        if ($sa.FrequencySubDayTypes -ne $sb.FrequencySubDayTypes)                   { $null = $diffs.Add("Schedule[$($sa.Name)].FrequencySubDayTypes") }
        if ($sa.FrequencySubDayInterval -ne $sb.FrequencySubDayInterval)             { $null = $diffs.Add("Schedule[$($sa.Name)].FrequencySubDayInterval") }
        if ($sa.FrequencyRelativeIntervals -ne $sb.FrequencyRelativeIntervals)       { $null = $diffs.Add("Schedule[$($sa.Name)].FrequencyRelativeIntervals") }
        if ($sa.FrequencyRecurrenceFactor -ne $sb.FrequencyRecurrenceFactor)         { $null = $diffs.Add("Schedule[$($sa.Name)].FrequencyRecurrenceFactor") }
        if ($sa.ActiveStartDate -ne $sb.ActiveStartDate)                             { $null = $diffs.Add("Schedule[$($sa.Name)].ActiveStartDate") }
        if ($sa.ActiveEndDate -ne $sb.ActiveEndDate)                                 { $null = $diffs.Add("Schedule[$($sa.Name)].ActiveEndDate") }
        if ($sa.ActiveStartTimeOfDay -ne $sb.ActiveStartTimeOfDay)                   { $null = $diffs.Add("Schedule[$($sa.Name)].ActiveStartTimeOfDay") }
        if ($sa.ActiveEndTimeOfDay -ne $sb.ActiveEndTimeOfDay)                       { $null = $diffs.Add("Schedule[$($sa.Name)].ActiveEndTimeOfDay") }
      }
    }

    if ($diffs.Count -eq 0) { $null = $diffs.Add('JobDefinitionDiff') }

    $results.Add([pscustomobject]@{
      InstanceSource      = "$Source"
      InstanceDestination = "$Destination"
      JobName             = $jobName
      Status              = 'Different'
      Differences         = [string[]]$diffs
      SourceHash          = $srcHash
      DestinationHash     = $dstHash
      IsEnabled           = (Out-SourceIsEnabled $src)
      Category            = (Out-SourceCategory $src)
    })
  }

  return $results
}