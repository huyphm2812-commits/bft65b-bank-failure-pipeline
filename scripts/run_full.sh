#!/usr/bin/env bash
set -euo pipefail

RUN_SPARK=false
SKIP_ACQUIRE=false

for arg in "$@"; do
    if [ "$arg" == "--run-spark" ]; then
        RUN_SPARK=true
    elif [ "$arg" == "--skip-acquire" ]; then
        SKIP_ACQUIRE=true
    fi
done

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(dirname "$SCRIPT_DIR")"
WORKSPACE_ROOT="$(dirname "$REPO_ROOT")/02_local_workspace"
FULL_LOGS="$WORKSPACE_ROOT/logs"
mkdir -p "$FULL_LOGS"

FULL_CONFIG="$REPO_ROOT/config/config.yaml"

echo "=========================================================="
echo "  BFT65B Bank Failure Pipeline - FULL PRODUCTION RUN"
echo "  Target Config: $FULL_CONFIG"
echo "  NOTE: This script does NOT run 'model --evaluate-test'."
echo "  Held-out test evaluation must be executed manually ONCE."
echo "=========================================================="

run_step() {
    local step_name="$1"
    shift
    echo -e "\n>>> Running: $step_name"
    local log_file="$FULL_LOGS/${step_name}.txt"
    python "$SCRIPT_DIR/run_pipeline.py" --config "$FULL_CONFIG" "$@" 2>&1 | tee "$log_file"
}

# 1. Connection Check
run_step "00_connection" connection

# 2. Acquire
if [ "$SKIP_ACQUIRE" = true ]; then
    echo -e "\n>>> Skipping acquisition (--skip-acquire specified)"
else
    run_step "01_acquire" acquire
fi

# 3. Validate
run_step "02_validate" validate

# 4. Audit
run_step "03_audit" audit

# 5. Panel
run_step "04_panel" panel

# 6. Benchmark
run_step "05_benchmark" benchmark --repetitions 5

# 7. Engines
run_step "06_engines" engines --repetitions 5

# 8. Spark (optional)
if [ "$RUN_SPARK" = true ]; then
    run_step "07_spark" spark --repetitions 2
else
    echo -e "\n>>> Skipping Spark step (use --run-spark to enable)"
fi

# 9. Model (--dev-only)
run_step "08_model_dev_only" model --dev-only

# 10. Check
run_step "09_check_reproducibility" check

echo "=========================================================="
echo "  FULL PIPELINE RUN (DEVELOPMENT) COMPLETED!"
echo "  Logs saved to: $FULL_LOGS"
echo "  NEXT STEP: Review outputs, freeze decisions in DECISIONS_LOG.md,"
echo "  then run: python scripts/run_pipeline.py model --evaluate-test"
echo "=========================================================="
