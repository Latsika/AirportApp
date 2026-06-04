@echo off
setlocal

set "QUIET=0"
if /i "%~1"=="/quiet" set "QUIET=1"
set "LOG=dist\fix_dist_airportapp_acl.log"

cd /d "%~dp0\.."

net session >nul 2>&1
if errorlevel 1 (
  echo Requesting administrator rights...
  powershell -NoProfile -ExecutionPolicy Bypass -Command "Start-Process -FilePath '%~f0' -ArgumentList '/quiet' -Verb RunAs"
  exit /b 0
)

echo [%DATE% %TIME%] Starting dist AirportApp.exe ACL repair > "%LOG%"

if not exist "dist\AirportApp_updated.exe" (
  echo [ERROR] Missing dist\AirportApp_updated.exe
  echo [ERROR] Missing dist\AirportApp_updated.exe >> "%LOG%"
  echo Build the updated EXE first.
  if "%QUIET%"=="0" pause
  exit /b 1
)

echo [1/4] Taking ownership of old dist\AirportApp.exe...
echo [1/4] Taking ownership of old dist\AirportApp.exe... >> "%LOG%"
if exist "dist\AirportApp.exe" (
  takeown /f "dist\AirportApp.exe" /a >> "%LOG%" 2>&1
  icacls "dist\AirportApp.exe" /inheritance:e >> "%LOG%" 2>&1
  icacls "dist\AirportApp.exe" /grant "%USERDOMAIN%\%USERNAME%:F" >> "%LOG%" 2>&1
)

echo [2/4] Removing old dist\AirportApp.exe...
echo [2/4] Removing old dist\AirportApp.exe... >> "%LOG%"
if exist "dist\AirportApp.exe" (
  del /f /q "dist\AirportApp.exe" >> "%LOG%" 2>&1
)
if exist "dist\AirportApp.exe" (
  echo [ERROR] Could not remove old dist\AirportApp.exe
  echo [ERROR] Could not remove old dist\AirportApp.exe >> "%LOG%"
  if "%QUIET%"=="0" pause
  exit /b 1
)

echo [3/4] Installing updated EXE...
echo [3/4] Installing updated EXE... >> "%LOG%"
ren "dist\AirportApp_updated.exe" "AirportApp.exe" >> "%LOG%" 2>&1

echo [4/4] Restoring standard permissions...
echo [4/4] Restoring standard permissions... >> "%LOG%"
icacls "dist" /inheritance:e >> "%LOG%" 2>&1
icacls "dist" /reset /T /C >> "%LOG%" 2>&1
icacls "dist\AirportApp.exe" >> "%LOG%" 2>&1

echo.
echo Done. dist\AirportApp.exe has been replaced.
echo [%DATE% %TIME%] Done. dist\AirportApp.exe has been replaced. >> "%LOG%"
if "%QUIET%"=="0" pause
