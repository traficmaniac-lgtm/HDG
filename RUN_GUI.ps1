Set-Location $PSScriptRoot

$pyCmd = $null
$pyCmdArgs = @()
if (Get-Command py -ErrorAction SilentlyContinue) {
    $pyCmd = "py"
    $pyCmdArgs = @("-3")
} elseif (Get-Command python -ErrorAction SilentlyContinue) {
    $pyCmd = "python"
}

if (-not $pyCmd) {
    Write-Host "[ERROR] Python 3 was not found. Please install Python 3 and try again."
    Read-Host "Press Enter to exit"
    exit 1
}

$pyCmdDisplay = if ($pyCmdArgs.Count -gt 0) { "$pyCmd $($pyCmdArgs -join ' ')" } else { $pyCmd }
Write-Host "[INFO] Using Python: $pyCmdDisplay"

$venvPython = Join-Path -Path $PSScriptRoot -ChildPath ".venv\Scripts\python.exe"
if (-not (Test-Path $venvPython)) {
    Write-Host "[INFO] Creating virtual environment in .venv..."
    & $pyCmd @pyCmdArgs -m venv .venv
    if ($LASTEXITCODE -ne 0) {
        Write-Host "[ERROR] Failed to create virtual environment."
        Read-Host "Press Enter to exit"
        exit 1
    }
}

Write-Host "[INFO] Installing dependencies from requirements.txt..."
& $venvPython -m pip install --upgrade pip
& $venvPython -m pip install -r requirements.txt
if ($LASTEXITCODE -ne 0) {
    Write-Host "[ERROR] Failed to install dependencies."
    Read-Host "Press Enter to exit"
    exit 1
}

Write-Host "[INFO] Starting GUI..."
$logFile = Join-Path -Path $PSScriptRoot -ChildPath "run_gui.log"
if (Test-Path $logFile) {
    Remove-Item $logFile -Force -ErrorAction SilentlyContinue
}

Write-Host "[INFO] Logs will be written to: $logFile"
& $venvPython -m src.app.main 1> $logFile 2>&1
$exitCode = $LASTEXITCODE

if ($exitCode -ne 0) {
    Write-Host "[ERROR] GUI failed to start. Exit code: $exitCode"
    Write-Host "[ERROR] Last output:"
    if (Test-Path $logFile) {
        Get-Content $logFile
    } else {
        Write-Host "[ERROR] Log file not found: $logFile"
    }
    Write-Host ""
    Write-Host "[INFO] Full log: $logFile"
    Read-Host "Press Enter to exit"
    exit $exitCode
}

Write-Host "[INFO] GUI exited."
Read-Host "Press Enter to exit"
