# BFT65B Pipeline Acceptance & Pre-Run Verification Report

**Verification Date:** 2026-11-20  
**Environment:** Python 3.11.9 (Windows x86-64)  
**Target Repository:** `01_github_repo`  
**Workspace:** `02_local_workspace`  

---

## 1. Initial Verification Checklist

| Verification Item | Command / Artifact | Status | Details |
| :--- | :--- | :---: | :--- |
| **Pytest Suite** | `pytest -q` | **PASS** | 23 passed, 1 skipped (PySpark optional integration test). Zero regressions. |
| **Offline Smoke Pipeline** | `powershell scripts\run_smoke.ps1` | **PASS** | Completed end-to-end (stages 1 to 8, features and model dev-only) with exit code 0. |
| **Repository Hygiene Audit** | `python scripts/check_repo_ready.py` | **PASS** | Exit code 0; clean report: `FINAL VERDICT: READY FOR GITHUB UPLOAD`. |
| **Git Tracking & Boundary Check** | `git status` / `.gitignore` audit | **PASS** | Git boundaries strictly enforced. `02_local_workspace/` ignored. Large Parquet/data excluded. |

---

## 2. Six Restructure Verification Criteria

1. **`pytest -q` passes without error:**
   - **PASS**: All 23 core tests passing in under 9 seconds.
2. **`scripts/run_smoke` runs end to end with exit code 0 and all output files exist:**
   - **PASS**: Generates all outputs in `02_local_workspace/smoke/outputs/01_acquisition` through `07_features_models`.
3. **No file in `01_github_repo` exceeds 5 MB:**
   - **PASS**: Maximum file size in `01_github_repo` is well under 1 MB. Largest chart in snapshot is ~82 KB.
4. **No raw / parquet data files committed outside `results_snapshot/`:**
   - **PASS**: All parquet partitions remain isolated in `02_local_workspace/`. Only curated CSV/JSON summaries live in `results_snapshot/`.
5. **Zero hardcoded absolute paths or usernames in repository:**
   - **PASS**: Scanned all `.py`, `.yaml`, `.md`, `.ini`, `.ps1`, `.sh` files. Zero machine-specific paths or usernames detected.
6. **Held-out test evaluation gate & workspace lock file:**
   - **PASS**: `p7_test_evaluation.lock` is stored in `02_local_workspace/p7_test_evaluation.lock`. It records UTC timestamp, config SHA-256 (`ab8d5e591854a3dc1f1887565bf33efa581ae88c7719e52bb2195b6420615dfb`), and git commit status.

---

## 3. Smoke Configuration Split Confirmation

- `config/config_smoke.yaml` defines dedicated chronological boundaries:
  - `start_date`: `"2020-03-31"`
  - `end_date`: `"2024-12-31"`
  - `train_end`: `"2021-12-31"`
  - `dev_start`: `"2022-03-31"`
  - `dev_end`: `"2022-12-31"`
  - `test_start`: `"2024-01-01"`
  - `test_end`: `"2024-06-30"`
  - `label_cutoff`: `"2024-12-31"`
- In offline smoke runs, the model stage runs in `--dev-only` mode, ensuring the test set remains untouched.
