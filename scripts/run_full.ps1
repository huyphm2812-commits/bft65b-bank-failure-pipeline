[CmdletBinding()]
param(
    [switch]$RunSpark,
    [switch]$SkipAcquire
)

$ErrorActionPreference = "Stop"

$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$repoRoot = Split-Path -Parent $scriptDir
$workspaceRoot = Join-Path (Split-Path -Parent $repoRoot) "02_local_workspace"
$fullLogs = Join-Path $workspaceRoot "logs"

if (-not (Test-Path $fullLogs)) {
    New-Item -ItemType Directory -Path $fullLogs -Force | Out-Null
}

$fullConfig = Join-Path $repoRoot "config\config.yaml"

Write-Host "==========================================================" -ForegroundColor Cyan
Write-Host "  BFT65B Bank Failure Pipeline - FULL PRODUCTION RUN" -ForegroundColor Cyan
Write-Host "  Target Config: $fullConfig" -ForegroundColor Cyan
Write-Host "  NOTE: This script does NOT run 'model --evaluate-test'." -ForegroundColor Yellow
Write-Host "  Held-out test evaluation must be executed manually ONCE." -ForegroundColor Yellow
Write-Host "==========================================================" -ForegroundColor Cyan

function Run-Step([string]$stepName, [string[]]$cmdArgs) {
    Write-Host "`n>>> Running: $stepName" -ForegroundColor Green
    $logFile = Join-Path $fullLogs "$stepName.txt"
    $errFile = Join-Path $fullLogs "$stepName.err.txt"
    
    $pythonArgs = @((Join-Path $scriptDir "run_pipeline.py"), "--config", $fullConfig) + $cmdArgs
    $process = Start-Process -FilePath "python" -ArgumentList $pythonArgs -NoNewWindow -PassThru -Wait -RedirectStandardOutput $logFile -RedirectStandardError $errFile
    
    Get-Content $logFile | Write-Host
    if (Test-Path $errFile) {
        $errContent = Get-Content $errFile
        if ($errContent) {
            Write-Host $errContent -ForegroundColor Red
        }
    }
    
    if ($process.ExitCode -ne 0) {
        throw "Step '$stepName' failed with exit code $($process.ExitCode)"
    }
}

# 1. Connection Check
Run-Step "00_connection" @("connection")

# 2. Acquire (Full ~1 hour unless skipped)
if ($SkipAcquire) {
    Write-Host "`n>>> Skipping acquisition (-SkipAcquire specified)" -ForegroundColor Yellow
} else {
    Run-Step "01_acquire" @("acquire")
}

# 3. Validate
Run-Step "02_validate" @("validate")

# 4. Audit
Run-Step "03_audit" @("audit")

# 5. Panel
Run-Step "04_panel" @("panel")

# 6. Benchmark
Run-Step "05_benchmark" @("benchmark", "--repetitions", "5")

# 7. Engines
Run-Step "06_engines" @("engines", "--repetitions", "5")

# 8. Spark (optional)
if ($RunSpark) {
    Run-Step "07_spark" @("spark", "--repetitions", "2")
} else {
    Write-Host "`n>>> Skipping Spark step (use -RunSpark to enable)" -ForegroundColor DarkGray
}

# 9. Model (--dev-only)
Run-Step "08_model_dev_only" @("model", "--dev-only")

# 10. Check
Run-Step "09_check_reproducibility" @("check")

Write-Host "`n==========================================================" -ForegroundColor Cyan
Write-Host "  FULL PIPELINE RUN (DEVELOPMENT) COMPLETED!" -ForegroundColor Cyan
Write-Host "  Logs saved to: $fullLogs" -ForegroundColor Cyan
Write-Host "  NEXT STEP: Review outputs, freeze decisions in DECISIONS_LOG.md," -ForegroundColor Yellow
Write-Host "  then run: python scripts/run_pipeline.py model --evaluate-test" -ForegroundColor Yellow
Write-Host "==========================================================" -ForegroundColor Cyan
