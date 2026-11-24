# Rubric Evidence Map & Report Fast-Lookups

This document maps each grading rubric criterion from `final_report_bft65b.pdf` directly to the CLI execution commands, output artifact paths, and specific figures required for the 20-page academic report.

---

## 1. Executive Summary Numbers (Page 1 of Final Report)

When writing Page 1 of the report, look up these exact empirical numbers from the generated artifacts:

| Metric Required | Source Artifact Path | Field / Key to Cite |
| :--- | :--- | :--- |
| **Download Timestamp** | `outputs/01_acquisition/validation_report.json` | `manifest_summary.created_utc` |
| **Total Call Report Observations** | `outputs/01_acquisition/validation_report.json` | `datasets.financials.manifest_rows` |
| **Total Covered Quarters** | `outputs/01_acquisition/validation_report.json` | `170 expected quarters (1984Q1–2026Q2)` |
| **Total Recorded Failures** | `outputs/02_quality/data_quality_summary.csv` | `dataset: failures, rows: ~4,117` |
| **Distinct Failed Banks in 1984–2026** | `outputs/03_panel/panel_summary.json` | `failures_linked_to_financials` |
| **Development Period (Train + Dev)** | `outputs/07_features_models/p7_model_results.json` | `split.development.min_date` to `max_date` |
| **Distinct Failed Banks in Development** | `outputs/07_features_models/p7_model_results.json` | `split.development.positive_banks` (must be $\ge 20$) |
| **Held-Out Test Period (Crisis Era)** | `outputs/07_features_models/p7_model_results.json` | `split.test.min_date` to `max_date` (2008–2014) |
| **Distinct Failed Banks in Test Set** | `outputs/07_features_models/p7_model_results.json` | `split.test.positive_banks` (must be $\ge 50$) |

---

## 2. Rubric Criteria Evidence Map

### Part 1: Storage & Acquisition (15 Points)
* **Commands:**
  ```powershell
  python scripts/run_pipeline.py connection
  python scripts/run_pipeline.py acquire
  python scripts/run_pipeline.py validate
  ```
* **Output Files:**
  * `02_local_workspace/manifests/acquisition_manifest.json`: Full chunk hashes, HTTP status, row counts.
  * `02_local_workspace/outputs/01_acquisition/validation_report.json`: Validates 170 quarters, record counts, and schema.
  * `02_local_workspace/outputs/01_acquisition/validation_summary.txt`: Human-readable summary.
* **Key Evidence for Report:** Explain chunked pagination, zero-call resume caching, Parquet conversion with millisecond timestamp casting, and removal of internal `ID` column.

### Part 2: Automated Data-Quality Audit (20 Points)
* **Commands:**
  ```powershell
  python scripts/run_pipeline.py audit
  python scripts/run_pipeline.py panel
  ```
* **Output Files:**
  * `02_local_workspace/outputs/02_quality/data_quality_summary.csv`: Table-level missingness and duplicates.
  * `02_local_workspace/outputs/02_quality/failure_anomalies.csv`: Analysis of the 488 missing-CERT rows and 44 repeated CERTs.
  * `02_local_workspace/outputs/02_quality/financial_field_availability.png`: Historical Call Report coverage by year.
  * `02_local_workspace/outputs/02_quality/missingness_top20.png`: Top-20 missing columns bar chart.
  * `02_local_workspace/outputs/03_panel/problem_evidence_handling.csv`: 8 detected anomalies and argued treatments.
  * `02_local_workspace/outputs/03_panel/merger_policy_sensitivity.json`: Sensitivity of censoring vs negative label.
* **Key Evidence for Report:** Contrast data-quality issues before vs after handling. Prove zero label noise from the 488 missing-CERT failures (all occurred pre-1976). Justify negative NTLNLS.

### Part 3: Processing Engines & Scalability (25 Points)
* **Commands:**
  ```powershell
  python scripts/run_pipeline.py benchmark --repetitions 5
  python scripts/run_pipeline.py engines --repetitions 5
  python scripts/run_pipeline.py spark --repetitions 2
  ```
* **Output Files:**
  * `02_local_workspace/outputs/04_storage_benchmark/p4_storage_benchmark_summary.csv`: CSV vs Gzip vs Parquet vs Arrow runtime/memory.
  * `02_local_workspace/outputs/04_storage_benchmark/p4_storage_benchmark_read_times.png`: Storage benchmark chart.
  * `02_local_workspace/outputs/05_engines/p5_engine_benchmark_summary.csv`: Pandas vs Polars LazyFrame speed/memory.
  * `02_local_workspace/outputs/05_engines/p5_output_equivalence.csv`: Exact numeric output equivalence verification.
  * `02_local_workspace/outputs/05_engines/p5_polars_query_plan.txt`: Polars optimized physical execution graph.
  * `02_local_workspace/outputs/05_engines/p5_scaling_benchmark.csv`: Scaling benchmark across 1k, 10k, 50k rows.
  * `02_local_workspace/outputs/06_spark/p6_spark_explain_extended.txt`: Extended Spark Catalyst plan.
  * `02_local_workspace/outputs/06_spark/p6_spark_physical_plan.txt`: Executed Spark physical plan showing Exchange and HashAggregate.
* **Key Evidence for Report:** Address Research Question 1. Use Amdahl's law and measured execution plans to argue why Polars outperforms Pandas on single-node setups and when Spark cluster overhead is justified.

### Part 4: Features & Leakage Prevention (15 Points)
* **Commands:**
  ```powershell
  python scripts/run_pipeline.py model --dev-only
  ```
* **Output Files:**
  * `02_local_workspace/outputs/07_features_models/p7_feature_dictionary.csv`: 14 CAMELS feature definitions.
  * `02_local_workspace/outputs/07_features_models/p7_feature_summary.json`: Verification of point-in-time calculation.
* **Key Evidence for Report:** Address Research Question 2. Present the `future_perturbation_check` passing evidence. Explain why provisional `NETINC` features were excluded (YTD cumulative bias). Detail 4-quarter lag non-bridging.

### Part 5: Modeling, Evaluation & Cost Thresholds (15 Points)
* **Commands:**
  ```powershell
  python scripts/run_pipeline.py model --dev-only
  # After freezing decisions:
  python scripts/run_pipeline.py model --evaluate-test
  ```
* **Output Files:**
  * `02_local_workspace/outputs/07_features_models/p7_model_results.json`: Full metrics (ROC-AUC, PR-AUC, Brier score, Baselines).
  * `02_local_workspace/outputs/07_features_models/p7_threshold_selection.csv`: Comparison of default 0.5, Max F1, and Cost Threshold.
  * `02_local_workspace/outputs/07_features_models/p7_cost_sensitivity.csv`: Expected loss curves across false-positive costs.
  * `02_local_workspace/outputs/07_features_models/p7_development_curves.png`: Precision-Recall and ROC curves.
  * `02_local_workspace/outputs/07_features_models/p7_expanding_window_cv.csv`: Chronological expanding window validation folds.
* **Key Evidence for Report:** Address Research Question 3. Compare Logistic Regression vs Random Forest vs Baselines. Show why threshold tuning on development expected loss dramatically reduces financial failure costs.

### Part 6: Vietnam Banking Comparison (5 Points)
* **Evidence:** Synthesize findings in written report comparing FDIC early-warning mechanisms with State Bank of Vietnam (SBV / NHNN):
  * Circular 41/2016/TT-NHNN [TODO(verify)] (Basel II CAR requirements) vs FDIC Risk-Based Capital (`RBC1AAJ`).
  * Circular 11/2021/TT-NHNN [TODO(verify)] (Asset classification and loan loss provisioning) vs FDIC `NCLNLS` / `LNATRES`.
  * Special Control Regime (Kiểm soát đặc biệt: SCB, CBBank, OceanBank) vs FDIC receivership liquidation.

### Reproducibility (5 Points)
* **Commands:**
  ```powershell
  python scripts/run_pipeline.py check
  python scripts/check_repo_ready.py
  pytest -q
  ```
* **Output Files:**
  * `02_local_workspace/outputs/08_reproducibility/p8_reproducibility_manifest.json`: Full environment fingerprint and source SHA-256 hashes.
