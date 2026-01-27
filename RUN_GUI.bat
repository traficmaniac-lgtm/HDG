@echo off
setlocal enabledelayedexpansion

cd /d "%~dp0"

set "PY_CMD="
py -3 -c "import sys" >nul 2>&1
if %errorlevel%==0 (
  set "PY_CMD=py -3"
) else (
  python -c "import sys" >nul 2>&1
  if %errorlevel%==0 (
    set "PY_CMD=python"
  )
)

if "%PY_CMD%"=="" (
  echo [ERROR] Python 3 was not found. Please install Python 3 and try again.
  pause
  exit /b 1
)

echo [INFO] Using Python: %PY_CMD%

if not exist ".venv\Scripts\python.exe" (
  echo [INFO] Creating virtual environment in .venv...
  %PY_CMD% -m venv .venv
  if %errorlevel% neq 0 (
    echo [ERROR] Failed to create virtual environment.
    pause
    exit /b 1
  )
)

echo [INFO] Installing dependencies from requirements.txt...
.venv\Scripts\python.exe -m pip install --upgrade pip
.venv\Scripts\python.exe -m pip install -r requirements.txt
if %errorlevel% neq 0 (
  echo [ERROR] Failed to install dependencies.
  pause
  exit /b 1
)

echo [INFO] Starting GUI...
.venv\Scripts\python.exe -m src.app.main

echo [INFO] GUI exited.
pause
endlocal
