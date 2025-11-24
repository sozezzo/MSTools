# bump patch (1.2.3 -> 1.2.4)
pwsh .\build\Bump-ModuleVersion.ps1 -Mode patch

# bump minor (1.2.3 -> 1.3.0)
pwsh .\build\Bump-ModuleVersion.ps1 -Mode minor

# set exact version
pwsh .\build\Bump-ModuleVersion.ps1 -Mode set -Version 2.0.0

# To build
pwsh .\build\build.ps1
