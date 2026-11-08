<#
  Runs every measurement behind the resume bullets and writes load-tests\results\RESULTS.md.
  Prerequisite: the stack is running (docker compose up --build -d).
  Usage (from the project folder):
    .\scripts\run-benchmarks.ps1                    # everything except the bridge (about 12 minutes)
    .\scripts\run-benchmarks.ps1 -WithBridge        # also the 2-minute bridge outage test (+4 min)
    .\scripts\run-benchmarks.ps1 -Rows 200000       # smaller dataset for a quick run
#>
param(
  [int]$Rows = 1000000,
  [int]$Vus = 20,
  [string]$Duration = '60s',
  [switch]$WithBridge,
  [switch]$SkipSearch,
  [switch]$SkipSpike,
  [string]$Where = 'local Docker Desktop'
)
# 'Continue' (not 'Stop'): Windows PowerShell 5.1 treats Docker's progress output on stderr as errors under 'Stop'.
# Failures are detected through $LASTEXITCODE instead.
$ErrorActionPreference = 'Continue'
Set-Location (Split-Path $PSScriptRoot -Parent)
$env:BENCH_WHERE = $Where
New-Item -ItemType Directory -Force -Path load-tests\results | Out-Null

function Step([string]$title, [scriptblock]$cmd) {
  Write-Host "`n=== $title ===" -ForegroundColor Cyan
  & $cmd
  if ($LASTEXITCODE -ne 0) { throw "Step failed: $title (exit $LASTEXITCODE)" }
}

$tools = @('compose', '--profile', 'tools')

if (-not $SkipSearch) {
  Step "Prepare $Rows flights" { docker @tools run --rm bench dist/scripts/bench-prep.js --rows $Rows --index on }
  Step 'EXPLAIN ANALYZE: no index vs composite index' { docker @tools run --rm bench dist/scripts/explain-benchmark.js --runs 50 }
  Step 'Drop the index' { docker @tools run --rm bench dist/scripts/bench-prep.js --index off }
  Step "k6 search, NO index ($Vus VUs, $Duration)" { docker @tools run --rm k6 run -q -e MODE=nocache -e LABEL=search-noindex -e VUS=$Vus -e DURATION=$Duration /scripts/search.js }
  Step 'Recreate the index' { docker @tools run --rm bench dist/scripts/bench-prep.js --index on }
  Step 'k6 search, index, cache bypassed' { docker @tools run --rm k6 run -q -e MODE=nocache -e LABEL=search-index -e VUS=$Vus -e DURATION=$Duration /scripts/search.js }
  Step 'k6 search, index + Redis cache' { docker @tools run --rm k6 run -q -e MODE=cache -e LABEL=search-index-cache -e VUS=$Vus -e DURATION=$Duration /scripts/search.js }
}

Step 'k6 idempotency race (100 identical concurrent requests)' { docker @tools run --rm k6 run -q -e VUS=100 /scripts/idempotency-race.js }

if (-not $SkipSpike) {
  Write-Host "`n=== 10x booking spike + consumer-lag monitor (~3 min) ===" -ForegroundColor Cyan
  cmd /c "docker rm -f skybook-spike-monitor >nul 2>&1"
  docker @tools run -d --name skybook-spike-monitor bench dist/scripts/bench-spike.js --max 900 | Out-Null
  Start-Sleep -Seconds 3
  docker @tools run --rm k6 run -q /scripts/booking-burst.js
  Write-Host 'waiting for consumers to catch up...'
  docker wait skybook-spike-monitor | Out-Null
  docker logs skybook-spike-monitor
  cmd /c "docker rm skybook-spike-monitor >nul 2>&1"
}

if ($WithBridge) {
  Step 'Start the Kafka -> SQS bridge (fake SQS)' { docker compose --profile aws up -d fake-sqs aws-bridge }
  Start-Sleep -Seconds 15
  Step 'Bridge: 2-minute AWS outage test' { docker compose --profile aws --profile tools run --rm bridge-outage-test }
}

Step 'Write RESULTS.md' { docker @tools run --rm bench dist/scripts/bench-report.js }
Write-Host "`nDone. Open load-tests\results\RESULTS.md" -ForegroundColor Green
