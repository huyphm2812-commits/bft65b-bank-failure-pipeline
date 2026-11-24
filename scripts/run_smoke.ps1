[CmdletBinding()]
param(
    [switch]$RunSpark
)

$ErrorActionPreference = "Stop"

$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$repoRoot = Split-Path -Parent $scriptDir
$workspaceRoot = Join-Path (Split-Path -Parent $repoRoot) "02_local_workspace"
$smokeLogs = Join-Path $workspaceRoot "smoke\logs"

if (-not (Test-Path $smokeLogs)) {
    New-Item -ItemType Directory -Path $smokeLogs -Force | Out-Null
}

$smokeConfig = Join-Path $repoRoot "config\config_smoke.yaml"
$serverScript = Join-Path $repoRoot "tests\fixtures\fake_fdic_server.py"

Write-Host "==========================================================" -ForegroundColor Cyan
Write-Host "  BFT65B Bank Failure Pipeline - Offline Smoke Run" -ForegroundColor Cyan
Write-Host "==========================================================" -ForegroundColor Cyan
Write-Host "Starting Fake FDIC Server on http://127.0.0.1:8765/banks..." -ForegroundColor Yellow

$serverProcess = Start-Process python -ArgumentList "`"$serverScript`" 8765" -PassThru -NoNewWindow
Start-Sleep -Seconds 2

try {
    # Helper to run a step and tee output
    function Run-Step([string]$stepName, [string[]]$cmdArgs) {
        Write-Host "`n>>> Running: $stepName" -ForegroundColor Green
        $logFile = Join-Path $smokeLogs "$stepName.txt"
        
        $pythonArgs = @((Join-Path $scriptDir "run_pipeline.py"), "--config", $smokeConfig) + $cmdArgs
        $process = Start-Process -FilePath "python" -ArgumentList $pythonArgs -NoNewWindow -PassThru -Wait -RedirectStandardOutput $logFile -RedirectStandardError (Join-Path $smokeLogs "$stepName.err.txt")
        
        Get-Content $logFile | Write-Host
        if (Test-Path (Join-Path $smokeLogs "$stepName.err.txt")) {
            $errContent = Get-Content (Join-Path $smokeLogs "$stepName.err.txt")
            if ($errContent) {
                Write-Host $errContent -ForegroundColor Red
            }
        }
        
        if ($process.ExitCode -ne 0) {
            throw "Step '$stepName' failed with exit code $($process.ExitCode)"
        }
    }

    # 1. Acquire
    Run-Step "01_acquire" @("acquire")

    # 2. Validate
    Run-Step "02_validate" @("validate")

    # 3. Audit
    Run-Step "03_audit" @("audit")

    # 4. Panel
    Run-Step "04_panel" @("panel")

    # 5. Storage Benchmark (small sample)
    Run-Step "05_benchmark" @("benchmark", "--sample-rows", "50", "--repetitions", "2")

    # 6. Engines (pandas vs polars)
    Run-Step "06_engines" @("engines", "--sample-rows", "50", "--repetitions", "2")

    # 7. Spark (optional)
    if ($RunSpark) {
        Run-Step "07_spark" @("spark", "--sample-rows", "50", "--repetitions", "2")
    } else {
        Write-Host "`n>>> Skipping Spark step (use -RunSpark to enable)" -ForegroundColor DarkGray
    }

    # 8. Model (--dev-only)
    Run-Step "08_model_dev_only" @("model", "--dev-only")

    Write-Host "`n==========================================================" -ForegroundColor Cyan
    Write-Host "  Offline Smoke Run COMPLETED SUCCESSFULLY!" -ForegroundColor Cyan
    Write-Host "  Logs saved to: $smokeLogs" -ForegroundColor Cyan
    Write-Host "==========================================================" -ForegroundColor Cyan
}
finally {
    Write-Host "`nStopping Fake FDIC Server (PID $($serverProcess.Id))..." -ForegroundColor Yellow
    if ($serverProcess -and -not $serverProcess.HasExited) {
        Stop-Process -Id $serverProcess.Id -Force -ErrorAction SilentlyContinue
    }
}
