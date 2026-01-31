Set-Location -Path $PSScriptRoot

python -m pip install PySide6 requests pandas matplotlib python-dateutil

python radar_app.py
if ($LASTEXITCODE -ne 0) {
    Write-Host "App exited with code $LASTEXITCODE"
    Pause
}
