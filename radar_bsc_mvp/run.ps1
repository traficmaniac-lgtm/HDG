Set-Location -Path $PSScriptRoot

python -m pip install -r requirements.txt

python app.py
if ($LASTEXITCODE -ne 0) {
    Write-Host "App exited with code $LASTEXITCODE"
    Pause
}
