@echo off
setlocal

cd /d "%~dp0\.."

if not exist ".venv\Scripts\python.exe" (
  echo [ERROR] Missing .venv\Scripts\python.exe
  echo Create virtualenv first, then rerun this script.
  exit /b 1
)

set "PY=.venv\Scripts\python.exe"

echo [1/4] Installing build dependencies...
"%PY%" -m pip install --upgrade pip >nul
if errorlevel 1 exit /b 1
"%PY%" -m pip install -r requirements.txt pyinstaller >nul
if errorlevel 1 exit /b 1

echo [2/4] Cleaning old build artifacts...
if exist "build" rmdir /s /q "build"
if exist "dist" rmdir /s /q "dist"

echo [3/4] Building portable EXE...
"%PY%" -m PyInstaller --noconfirm --clean "installer\airport_app_portable.spec"
if errorlevel 1 exit /b 1

echo [4/4] Preparing runtime folders...
if not exist "dist" mkdir "dist"
if not exist "dist\logs" mkdir "dist\logs"
if not exist "dist\backups" mkdir "dist\backups"

echo.
echo Build complete:
echo   dist\AirportApp.exe
echo.
echo Copy AirportApp.exe to another Windows PC and double-click to start.
echo Python is NOT required on that PC.

endlocal
