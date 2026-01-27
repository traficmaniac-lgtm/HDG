@echo off
cd /d C:\Users\Юрий\Desktop\HDG

echo === SAFE PULL FROM GITHUB ===

git fetch origin

git pull origin main --rebase --autostash

echo.
echo DONE. Local files preserved.
pause
