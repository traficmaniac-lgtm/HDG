@echo off
chcp 65001 >nul
setlocal

cd /d "C:\Users\Юрий\Desktop\HDG" || (echo [ERR] Folder not found & pause & exit /b 1)

echo === HDG: GIT STATUS ===
git status
echo.

echo === HDG: ADD ALL ===
git add -A
echo.

echo === HDG: COMMIT (auto) ===
git commit -m "update" 2>nul
if %errorlevel% neq 0 (
  echo [INFO] Nothing to commit (or commit failed). Continuing...
)
echo.

echo === HDG: PUSH ===
git push origin main
echo.

echo Done.
pause
