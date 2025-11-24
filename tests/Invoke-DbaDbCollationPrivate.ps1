

 
function Initialize-DbaCloneTargetDatabase {
    [CmdletBinding(SupportsShouldProcess, ConfirmImpact='High')]
    param(
        [Parameter(Mandatory)] [string] $DestInstance,
        [Parameter(Mandatory)] [string] $NewDatabase,
        [Parameter(Mandatory)] [string] $TargetCollation,

        [ValidateSet('Simple','Full','BulkLogged')]
        [string] $RecoveryModel = 'Simple',

        # If omitted, Set-DbaDbOwner keeps default owner (usually 'sa' if you specify it)
        [string] $Owner = 'sa',

        # Your logger
        [string] $LogFileName,

        # Behavior
        [switch] $DropIfExists,   # drop and recreate if DB already exists
        [switch] $PassThru        # return the DB object at the end
    )

    Write-Log -Message "Initialize-DbaCloneTargetDatabase started for [$DestInstance].[$NewDatabase] (collation: $TargetCollation)" -Level Info -LogFileName $LogFileName

    try {
        $exists = Get-DbaDatabase -SqlInstance $DestInstance -Database $NewDatabase -ErrorAction SilentlyContinue

        if ($exists -and $DropIfExists) {
            if ($PSCmdlet.ShouldProcess("$DestInstance / $NewDatabase", "Drop database")) {
                Write-Log -Message "Dropping existing database '$NewDatabase' on '$DestInstance'." -Level Important -LogFileName $LogFileName
                Remove-DbaDatabase -SqlInstance $DestInstance -Database $NewDatabase -Confirm:$false
            }
        } elseif ($exists) {
            throw "Database '$NewDatabase' already exists on '$DestInstance'. Use -DropIfExists to replace it."
        }

        if ($PSCmdlet.ShouldProcess("$DestInstance / $NewDatabase", "Create database with collation $TargetCollation")) {
            Write-Log -Message "Creating database '$NewDatabase' with collation '$TargetCollation'." -Level Info -LogFileName $LogFileName
            New-DbaDatabase -SqlInstance $DestInstance -Name $NewDatabase -Collation $TargetCollation -ErrorAction Stop -Confirm:$false
        }

        if ($PSCmdlet.ShouldProcess("$DestInstance / $NewDatabase", "Set owner and recovery model")) {
            if ($Owner) {
                Write-Log -Message "Setting owner to '$Owner'." -Level Info -LogFileName $LogFileName
                Set-DbaDbOwner -SqlInstance $DestInstance -Database $NewDatabase -TargetLogin $Owner -Confirm:$false
            }
            Write-Log -Message "Setting recovery model to '$RecoveryModel'." -Level Info -LogFileName $LogFileName
            Set-DbaDbRecoveryModel -SqlInstance $DestInstance -Database $NewDatabase -RecoveryModel $RecoveryModel -Confirm:$false
        }

        Write-Log -Message "Target database '$NewDatabase' ready on '$DestInstance' (collation: $TargetCollation)." -Level Alert -LogFileName $LogFileName

        if ($PassThru) {
            return Get-DbaDatabase -SqlInstance $DestInstance -Database $NewDatabase
        }
    }
    catch {
        Write-Log -Message ("ERROR: " + $_.Exception.Message) -Level Error -LogFileName $LogFileName
        throw
    }
}









 

