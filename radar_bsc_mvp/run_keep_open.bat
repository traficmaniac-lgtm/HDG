@echo off
setlocal
cd /d %~dp0
python -m pip install -r requirements.txt
python app.py
if errorlevel 1 (
  echo.
  echo [ERROR] Application exited with an error. See the logs above.
)

echo.
echo [INFO] Press any key to close this window...
pause >nul
