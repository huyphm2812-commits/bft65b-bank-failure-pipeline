# Repository Reorganization Changelog & Path Mapping

This document maps all original flat files from the legacy workspace folder (`final/`) to their new locations in `BFT65B_FINAL/`.
All original files were preserved untouched in `BFT65B_FINAL/04_archive_old_versions/`.

---

## 1. File Path Mapping Table

| Original Flat File (`final/`) | New Path in `BFT65B_FINAL/` | New Functional Scope |
| :--- | :--- | :--- |
| `p1_fdic_client.py` | `01_github_repo/src/bankfail/acquisition/client.py` | Retrying HTTP client for FDIC BankFind API with backoff |
| `p1_storage.py` | `01_github_repo/src/bankfail/acquisition/storage.py` | Parquet conversion, schema typing (`timestamp[ms]`), SHA-256 |
| `p1_downloader.py` | `01_github_repo/src/bankfail/acquisition/downloader.py` | Chunked pagination, resumable raw archiving, manifest logging |
| `p1_validation.py` | `01_github_repo/src/bankfail/acquisition/validation.py` | Integrity checks for 170 expected quarters, totals, schemas |
| `p1_acquisition_pipeline.py` | `01_github_repo/src/bankfail/acquisition/pipeline.py` | Configuration loading, logging, and acquisition orchestration |
| `p2_quality_audit.py` | `01_github_repo/src/bankfail/quality/audit.py` | Non-destructive audit for missingness, duplicates, anomalies |
| `p3_panel.py` | `01_github_repo/src/bankfail/panel/builder.py` | Panel construction, forward failure labels, exit classification |
| `p4_benchmark.py` | `01_github_repo/src/bankfail/benchmarks/storage.py` | Storage format benchmarks (CSV, Gzip, Parquet, Arrow IPC) |
| `p5_processing.py` | `01_github_repo/src/bankfail/processing/engines.py` | Equivalent cleaning and aggregation in Pandas and Polars Lazy |
| `p6_spark.py` | `01_github_repo/src/bankfail/spark/engine.py` | PySpark distributed equivalent, Catalyst physical plan capture |
| `p7_modeling.py` | `01_github_repo/src/bankfail/modeling/pipeline.py` | CAMELS feature engineering, expanding window CV, cost thresholds |
| `p8_reproducibility.py` | `01_github_repo/src/bankfail/repro/audit.py` | Repository hygiene audit, environment hashing, run manifests |
| `run_pipeline.py` | `01_github_repo/scripts/run_pipeline.py` | Unified command-line interface for all 8 pipeline stages |
| `test_pipeline.py` | `01_github_repo/tests/test_pipeline.py` | 24 automated unit and integration tests (added resume test) |
| `config_config.yaml` | `01_github_repo/config/config.yaml` | Production historical configuration (1984Q1–2026Q2) |
| *(New File)* | `01_github_repo/config/config_smoke.yaml` | Fast offline synthetic configuration (2020–2024) |
| *(New File)* | `01_github_repo/tests/fixtures/fake_fdic_server.py` | Local FDIC mock server fixture for internet-free testing |
| *(New File)* | `01_github_repo/scripts/run_smoke.ps1` | PowerShell automated offline smoke runner |
| *(New File)* | `01_github_repo/scripts/run_smoke.sh` | Bash automated offline smoke runner |
| *(New File)* | `01_github_repo/scripts/run_full.ps1` | PowerShell automated full historical runner |
| *(New File)* | `01_github_repo/scripts/run_full.sh` | Bash automated full historical runner |
| *(New File)* | `01_github_repo/scripts/export_results.py` | Results snapshot exporter & report input curator |
| *(New File)* | `01_github_repo/scripts/check_repo_ready.py` | Pre-commit repository hygiene auditor |
| `README.md` | `01_github_repo/README.md` | Complete project documentation and workflow instructions |
| `requirements.txt` | `01_github_repo/requirements.txt` | Fully pinned dependencies (added setuptools for PySpark) |
| `pytest.ini` | `01_github_repo/pytest.ini` | Pytest path and runner configuration |
| `.gitignore` | `01_github_repo/.gitignore` | Git exclusion rules for large workspace data & binaries |
| *(New File)* | `01_github_repo/.python-version` | Pinned Python version (`3.11.9`) |
| *(New File)* | `01_github_repo/docs/DECISIONS_LOG.md` | Comprehensive log of econometric and engineering decisions |
| *(New File)* | `01_github_repo/docs/EVIDENCE_MAP.md` | Rubric parts 1–6 evidence map and page 1 data lookup table |
| *(New File)* | `01_github_repo/docs/CITATIONS.md` | Official documentation, literature, and course references |
| *(New File)* | `01_github_repo/docs/AI_USAGE.md` | Disclosure template for AI assistance and human verification |
| `docs_part2_report_template.md` | `04_archive_old_versions/docs_part2_report_template.md` | Original report template preserved in archive |
| `final_report_bft65b.pdf` | `04_archive_old_versions/final_report_bft65b.pdf` | Course assignment specification preserved in archive |
| *(New File)* | `00_START_HERE.md` | Top-level 1-page operational guide and STOP box |
| *(New Directory)* | `02_local_workspace/` | Local workspace for uncommitted data, logs, and outputs |
| *(New Directory)* | `03_report_inputs/` | Curated copy of finished tables and figures for the paper |
| *(New Directory)* | `04_archive_old_versions/` | Exact untouched copy of all original flat files |

---

## 2. Structural Improvements Summary

1. **Separation of Source Code vs Workspace Data:**
   * Source code resides strictly in `01_github_repo/` (< 2 MB total).
   * Data, Parquet files, raw JSON pages, and large logs reside in `02_local_workspace/` (never committed).
2. **Offline Reproducibility:**
   * Added `fake_fdic_server.py` so any grader can verify the pipeline without network access or hitting FDIC API rate limits.
3. **Execution Guardrails:**
   * Enforced `--evaluate-test` one-time execution gate via `p7_test_evaluation.lock`.
   * Pinned all requirements with exact versions.
