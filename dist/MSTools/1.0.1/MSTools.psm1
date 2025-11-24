# Load the private implementation (your real code)
. $PSScriptRoot\Public\Export-DbaDbCompareReport.ps1

# Export the public function so users can call it
Export-ModuleMember -Function Export-DbaDbCompareReport

