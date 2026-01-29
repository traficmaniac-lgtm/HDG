@echo off
cd /d "%~dp0"

git update-index --skip-worktree config/settings.json >nul 2>&1

git add -A
git commit -m "update" >nul 2>&1
git push origin main

pause
