cls

rem powershell -f Bump-ModuleVersion.ps1
cls
powershell -f build.ps1 

powershell -f install.ps1

timeout 10
 



