@echo off
setlocal

cd /d "%~dp0\.."

if not exist ".venv\Scripts\python.exe" (
  echo [ERROR] Missing .venv\Scripts\python.exe
  echo Create virtualenv first, then rerun this script.
  exit /b 1
)

set "PY=.venv\Scripts\python.exe"

for /f %%I in ('powershell -NoProfile -Command "Get-Date -Format yyyy-MM-dd_HHmmss"') do set "BUILD_TS=%%I"
set "PRESERVED_DB=backups\dist_airport_app_before_portable_build_%BUILD_TS%.db"
set "PRESERVED_SECRET=backups\dist_airport_app_secret_before_portable_build_%BUILD_TS%.secret"

echo [1/4] Installing build dependencies...
"%PY%" -m pip install --upgrade pip >nul
if errorlevel 1 exit /b 1
"%PY%" -m pip install -r requirements.txt pyinstaller >nul
if errorlevel 1 exit /b 1

echo [2/4] Cleaning old build artifacts...
if not exist "backups" mkdir "backups"
if exist "dist\airport_app.db" (
  echo Preserving existing dist\airport_app.db...
  copy /y "dist\airport_app.db" "%PRESERVED_DB%" >nul
)
if exist "dist\airport_app.secret" (
  echo Preserving existing dist\airport_app.secret...
  copy /y "dist\airport_app.secret" "%PRESERVED_SECRET%" >nul
)
if exist "build" rmdir /s /q "build"
if exist "dist" rmdir /s /q "dist"
if exist "dist" (
  echo [ERROR] Could not clean dist. Close AirportApp and retry.
  if not exist "dist" mkdir "dist"
  if exist "%PRESERVED_DB%" copy /y "%PRESERVED_DB%" "dist\airport_app.db" >nul
  if exist "%PRESERVED_SECRET%" copy /y "%PRESERVED_SECRET%" "dist\airport_app.secret" >nul
  exit /b 1
)

echo [3/4] Building portable EXE...
"%PY%" -m PyInstaller --noconfirm --clean "installer\airport_app_portable.spec"
if errorlevel 1 (
  echo [ERROR] Portable build failed.
  if not exist "dist" mkdir "dist"
  if exist "%PRESERVED_DB%" copy /y "%PRESERVED_DB%" "dist\airport_app.db" >nul
  if exist "%PRESERVED_SECRET%" copy /y "%PRESERVED_SECRET%" "dist\airport_app.secret" >nul
  exit /b 1
)

echo [4/4] Preparing runtime folders...
if not exist "dist" mkdir "dist"
if not exist "dist\logs" mkdir "dist\logs"
if not exist "dist\backups" mkdir "dist\backups"
if exist "%PRESERVED_DB%" (
  echo Restoring preserved dist\airport_app.db...
  copy /y "%PRESERVED_DB%" "dist\airport_app.db" >nul
)
if exist "%PRESERVED_SECRET%" (
  echo Restoring preserved dist\airport_app.secret...
  copy /y "%PRESERVED_SECRET%" "dist\airport_app.secret" >nul
)

echo Writing release metadata...
"%PY%" "installer\write_release_info.py"
if errorlevel 1 exit /b 1

echo.
echo Build complete:
echo   dist\AirportApp.exe
echo   dist\RELEASE_INFO.md
echo.
echo For a fresh install, copy AirportApp.exe to the target folder and start it.
echo For an existing customer, use install_update.exe or copy the whole app folder with airport_app.db.
echo Python is NOT required on that PC.

endlocal
