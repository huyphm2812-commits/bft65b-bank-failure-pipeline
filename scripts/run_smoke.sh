#!/usr/bin/env bash
set -euo pipefail

RUN_SPARK=false
for arg in "$@"; do
    if [ "$arg" == "--run-spark" ]; then
        RUN_SPARK=true
    fi
done

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(dirname "$SCRIPT_DIR")"
WORKSPACE_ROOT="$(dirname "$REPO_ROOT")/02_local_workspace"
SMOKE_LOGS="$WORKSPACE_ROOT/smoke/logs"
mkdir -p "$SMOKE_LOGS"

SMOKE_CONFIG="$REPO_ROOT/config/config_smoke.yaml"
SERVER_SCRIPT="$REPO_ROOT/tests/fixtures/fake_fdic_server.py"

echo "=========================================================="
echo "  BFT65B Bank Failure Pipeline - Offline Smoke Run"
echo "=========================================================="
echo "Starting Fake FDIC Server on http://127.0.0.1:8765/banks..."

python "$SERVER_SCRIPT" 8765 &
SERVER_PID=$!
sleep 2

cleanup() {
    echo "Stopping Fake FDIC Server (PID $SERVER_PID)..."
    kill "$SERVER_PID" 2>/dev/null || true
}
trap cleanup EXIT

run_step() {
    local step_name="$1"
    shift
    echo -e "\n>>> Running: $step_name"
    local log_file="$SMOKE_LOGS/${step_name}.txt"
    python "$SCRIPT_DIR/run_pipeline.py" --config "$SMOKE_CONFIG" "$@" 2>&1 | tee "$log_file"
}

# 1. Acquire
run_step "01_acquire" acquire

# 2. Validate
run_step "02_validate" validate

# 3. Audit
run_step "03_audit" audit

# 4. Panel
run_step "04_panel" panel

# 5. Benchmark (small)
run_step "05_benchmark" benchmark --sample-rows 50 --repetitions 2

# 6. Engines
run_step "06_engines" engines --sample-rows 50 --repetitions 2

# 7. Spark (optional)
if [ "$RUN_SPARK" = true ]; then
    run_step "07_spark" spark --sample-rows 50 --repetitions 2
else
    echo -e "\n>>> Skipping Spark step (use --run-spark to enable)"
fi

# 8. Model (--dev-only)
run_step "08_model_dev_only" model --dev-only

echo "=========================================================="
echo "  Offline Smoke Run COMPLETED SUCCESSFULLY!"
echo "  Logs saved to: $SMOKE_LOGS"
echo "=========================================================="
