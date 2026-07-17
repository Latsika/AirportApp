@echo off
setlocal

cd /d "%~dp0\.."

if not exist ".venv\Scripts\python.exe" (
  echo [ERROR] Missing .venv\Scripts\python.exe
  exit /b 1
)

set "PY=.venv\Scripts\python.exe"

echo [1/3] Building current AirportApp.exe and release metadata...
call installer\build_portable.bat
if errorlevel 1 exit /b 1

echo [2/3] Building install_update.exe...
"%PY%" -m pip install pyinstaller >nul
if errorlevel 1 exit /b 1
"%PY%" -m PyInstaller --noconfirm --clean installer\install_update.spec
if errorlevel 1 exit /b 1

echo [3/3] Done.
echo.
echo Generated:
echo   dist\install_update.exe
echo.
echo Usage:
echo   Run install_update.exe and select target folder with AirportApp.exe
echo   The updater preserves airport_app.db and replaces AirportApp.exe plus RELEASE_INFO.md.
echo.

endlocal
