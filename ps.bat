@echo off
chcp 65001 >nul
setlocal enabledelayedexpansion

set "ROOT=C:\Users\Юрий\Desktop\HDG"
set "LOG=%ROOT%\HDG_PUSH.log"

(
  echo === START %date% %time% ===
  echo ROOT=%ROOT%
  echo.

  if not exist "%ROOT%\" (
    echo [ERR] Folder not found: "%ROOT%"
    goto :end
  )

  cd /d "%ROOT%" || (echo [ERR] cd failed & goto :end)

  echo --- where git ---
  where git
  echo.

  echo --- git --version ---
  git --version
  echo.

  echo --- git status ---
  git status
  echo.

  echo --- git add -A ---
  git add -A
  echo.

  echo --- git commit -m "update" ---
  git commit -m "update"
  echo.

  echo --- git push origin main ---
  git push origin main
  echo.

  echo [OK] Done
) > "%LOG%" 2>&1

:end
echo.
echo Log saved to: "%LOG%"
type "%LOG%"
echo.
pause

