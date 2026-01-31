@echo off
cd /d %~dp0

python -m pip install -r requirements.txt
python app.py

if errorlevel 1 (
  echo App exited with code %errorlevel%
  pause
)
