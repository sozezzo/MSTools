cls

rem powershell -f Bump-ModuleVersion.ps1
cls
powershell -f build.ps1 

powershell -f install.ps1

rem If first argument is "exit", leave immediately
if "%~1"=="exit" exit

timeout 10
 



