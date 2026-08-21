function Set-MSAgentJobOwner {
<#
.SYNOPSIS
    Sets the owner of SQL Agent jobs to a target login - the jobs equivalent of
    Set-DbaDbOwner.

.DESCRIPTION
    Connects to each instance and sets the OwnerLoginName of every SQL Agent job
    to -TargetLogin (default 'sa'), skipping any job whose owner already matches.
    Jobs and categories can be excluded by name.

    The target login must already exist on the instance; if it does not, the
    instance is skipped. One result object is returned per job the function
    considered, and the full list is printed at the end.

    Nothing is changed under -WhatIf. This function requires dbatools.

.PARAMETER SqlInstance
    One or more target SQL Server instances ("Server" or "Server\Instance").

.PARAMETER SqlCredential
    Optional PSCredential used to connect. When omitted, Integrated Security is
    used. Passed through to dbatools.

.PARAMETER TargetLogin
    The login to set as the job owner. Default 'sa'.

.PARAMETER ExcludeJob
    One or more job names to leave untouched (exact, case-insensitive).

.PARAMETER ExcludeCategory
    One or more job categories to leave untouched (exact, case-insensitive).

.EXAMPLE
    Set-MSAgentJobOwner -SqlInstance 'SQL01'

    Sets every SQL Agent job on SQL01 to be owned by [sa].

.EXAMPLE
    Set-MSAgentJobOwner -SqlInstance 'SQL01' -TargetLogin 'DOMAIN\sqladmin' -ExcludeCategory 'Report Server' -WhatIf

    Previews changing every job owner to DOMAIN\sqladmin except jobs in the
    'Report Server' category.

.NOTES
    Requires dbatools (Connect-DbaInstance, Get-DbaAgentJob, Get-DbaLogin).
#>
    [CmdletBinding(SupportsShouldProcess, ConfirmImpact = 'Medium')]
    param(
        [Parameter(Mandatory, ValueFromPipeline)]
        [string[]] $SqlInstance,

        [PSCredential] $SqlCredential,

        [string] $TargetLogin = 'sa',

        [string[]] $ExcludeJob,

        [string[]] $ExcludeCategory
    )

    begin {
        if (-not (Get-Command Connect-DbaInstance -ErrorAction SilentlyContinue)) {
            throw "dbatools is required. Run: Import-Module dbatools"
        }

        # Log through Write-MSLog when available; otherwise fall back to -Verbose.
        $log = {
            param([string] $Message, [string] $Level = 'Info')
            if (Get-Command Write-MSLog -ErrorAction SilentlyContinue) {
                try { Write-MSLog -Level $Level -Message $Message } catch { Write-Verbose $Message }
            }
            else { Write-Verbose $Message }
        }

        $excludeJobSet = New-Object 'System.Collections.Generic.HashSet[string]' ([System.StringComparer]::OrdinalIgnoreCase)
        foreach ($j in @($ExcludeJob | Where-Object { -not [string]::IsNullOrWhiteSpace($_) })) { [void]$excludeJobSet.Add($j) }

        $excludeCategorySet = New-Object 'System.Collections.Generic.HashSet[string]' ([System.StringComparer]::OrdinalIgnoreCase)
        foreach ($c in @($ExcludeCategory | Where-Object { -not [string]::IsNullOrWhiteSpace($_) })) { [void]$excludeCategorySet.Add($c) }

        $results = New-Object System.Collections.Generic.List[object]
        $emit = {
            param([string] $Instance, [string] $Job, [string] $Category, [string] $PreviousOwner, [string] $NewOwner, [string] $Status, [string] $Notes)
            $results.Add([pscustomobject]@{
                SqlInstance   = $Instance
                Job           = $Job
                Category      = $Category
                PreviousOwner = $PreviousOwner
                NewOwner      = $NewOwner
                Status        = $Status
                Notes         = $Notes
            })
        }
    }

    process {
        foreach ($instance in $SqlInstance) {

            $connectParams = @{ SqlInstance = $instance; ErrorAction = 'Stop' }
            if ($SqlCredential) { $connectParams['SqlCredential'] = $SqlCredential }
            try {
                $server = Connect-DbaInstance @connectParams
            }
            catch {
                & $log "Set-MSAgentJobOwner: could not connect to [$instance]: $($_.Exception.Message)" 'Error'
                & $emit $instance '' '' '' $TargetLogin 'Failed' "Connect: $($_.Exception.Message)"
                continue
            }

            # The target login must exist before it can own anything.
            $targetFound = @(Get-DbaLogin -SqlInstance $server -Login $TargetLogin -EnableException:$false)
            if ($targetFound.Count -eq 0) {
                & $log "Set-MSAgentJobOwner: target login [$TargetLogin] does not exist on [$instance]; skipping." 'Warning'
                & $emit $instance '' '' '' $TargetLogin 'Skipped' "Target login [$TargetLogin] not found on [$instance]"
                continue
            }

            $jobs = @(Get-DbaAgentJob -SqlInstance $server -EnableException:$false)
            foreach ($job in $jobs) {
                $jobName      = [string]$job.Name
                $category     = [string]$job.Category
                $currentOwner = [string]$job.OwnerLoginName

                if ($excludeJobSet.Contains($jobName)) {
                    & $emit $instance $jobName $category $currentOwner $currentOwner 'Skipped' 'In -ExcludeJob list'
                    continue
                }
                if ($excludeCategorySet.Contains($category)) {
                    & $emit $instance $jobName $category $currentOwner $currentOwner 'Skipped' 'In -ExcludeCategory list'
                    continue
                }
                if ($currentOwner -ieq $TargetLogin) {
                    & $emit $instance $jobName $category $currentOwner $currentOwner 'AlreadySet' 'Owner already matches target'
                    continue
                }

                if (-not $PSCmdlet.ShouldProcess("[$instance] job [$jobName]", "Set owner to [$TargetLogin] (was [$currentOwner])")) {
                    & $emit $instance $jobName $category $currentOwner $TargetLogin 'WhatIf' 'Not executed'
                    continue
                }

                try {
                    $job.OwnerLoginName = $TargetLogin
                    $job.Alter()
                    & $log "Set-MSAgentJobOwner: [$instance] job [$jobName] owner [$currentOwner] -> [$TargetLogin]." 'Info'
                    & $emit $instance $jobName $category $currentOwner $TargetLogin 'Changed' ''
                }
                catch {
                    & $log "Set-MSAgentJobOwner: failed to set owner for [$instance] job [$jobName]: $($_.Exception.Message)" 'Error'
                    & $emit $instance $jobName $category $currentOwner $TargetLogin 'Failed' $_.Exception.Message
                }
            }
        }
    }

    end {
        $results
    }
}
