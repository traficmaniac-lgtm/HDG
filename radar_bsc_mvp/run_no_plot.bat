@echo off
cd /d %~dp0
set NO_PLOT=1
python -m pip install -r requirements.txt
python app.py
if errorlevel 1 pause
