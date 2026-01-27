Set-Location $PSScriptRoot

$pyCmd = $null
if (Get-Command py -ErrorAction SilentlyContinue) {
    $pyCmd = "py -3"
} elseif (Get-Command python -ErrorAction SilentlyContinue) {
    $pyCmd = "python"
}

if (-not $pyCmd) {
    Write-Host "[ERROR] Python 3 was not found. Please install Python 3 and try again."
    Read-Host "Press Enter to exit"
    exit 1
}

Write-Host "[INFO] Using Python: $pyCmd"

$venvPython = Join-Path -Path $PSScriptRoot -ChildPath ".venv\Scripts\python.exe"
if (-not (Test-Path $venvPython)) {
    Write-Host "[INFO] Creating virtual environment in .venv..."
    & $pyCmd -m venv .venv
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
& $venvPython -m src.app.main

Write-Host "[INFO] GUI exited."
Read-Host "Press Enter to exit"
