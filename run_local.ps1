<#
run_local.ps1

Windows-friendly helper to:
 - create a virtual environment (.venv)
 - install dependencies from requirements.txt
 - run the synthetic data generator
 - optionally publish JSONL to Confluent Cloud using the Kafka producer

Usage:
  .\run_local.ps1            # run generator, then publish (requires filled .env)
  .\run_local.ps1 -SkipPublish  # only generate data

Note: If .env does not exist the script will copy .env.example -> .env and exit so you
can fill credentials securely.
#>

param(
    [switch]$SkipPublish
)

Write-Host "[RetailSync] Setting up virtual environment..."

# Create venv (idempotent)
python -m venv .venv 2>$null

$venvPython = Join-Path $PWD ".venv\Scripts\python.exe"
if (-not (Test-Path $venvPython)) {
    Write-Error "Failed to find venv python at $venvPython. Ensure 'python' is on PATH and try again."
    exit 1
}

Write-Host "[RetailSync] Upgrading pip and installing requirements..."
& $venvPython -m pip install --upgrade pip | Out-Null
& $venvPython -m pip install -r requirements.txt

Write-Host "[RetailSync] Generating synthetic data (writes to data/)..."
& $venvPython 01_ecommerce_data_generator_small.py

if ($SkipPublish) {
    Write-Host "[RetailSync] Skipping publish as requested. Data generation complete."
    exit 0
}

# Ensure .env exists and contains credentials
$envFile = Join-Path $PWD ".env"
if (-not (Test-Path $envFile)) {
    Copy-Item ".env.example" $envFile
    Write-Host "Created .env from .env.example. Please open .env and fill BOOTSTRAP_SERVER, API_KEY, API_SECRET before publishing." -ForegroundColor Yellow
    exit 0
}

Write-Host "[RetailSync] Validating .env credentials..."
$envLines = Get-Content $envFile | ForEach-Object { $_.Trim() } | Where-Object { $_ -and -not ($_.StartsWith('#')) }
$kv = @{}
foreach ($line in $envLines) {
    if ($line -match '^(\s*[^=]+)\s*=\s*(.*)$') {
        $key = $matches[1].Trim()
        $val = $matches[2].Trim()
        $kv[$key] = $val
    }
}

$required = @('BOOTSTRAP_SERVER','API_KEY','API_SECRET')
$missing = @()
foreach ($r in $required) {
    if (-not $kv.ContainsKey($r) -or [string]::IsNullOrWhiteSpace($kv[$r])) { $missing += $r }
}
if ($missing.Count -gt 0) {
    Write-Host "Missing or empty env vars: $($missing -join ', ')." -ForegroundColor Yellow
    Write-Host "Fill them in .env and re-run the script." -ForegroundColor Yellow
    exit 1
}

Write-Host "[RetailSync] Publishing JSONL datasets to Kafka (Confluent Cloud)..."
& $venvPython 02_Kafka_Producer_small.py

Write-Host "[RetailSync] Completed."
