# Round 2 Implementation Changes & Course Alignment Report

**Date:** 2026-11-20  
**Target Repository:** `01_github_repo`  
**Scope:** Course Alignment Refinements & Quality/Modeling/Processing Enhancements  

---

## Summary of Completed Tasks

### 1. Step 0: Verification & Acceptance Report
- Executed `pytest -q`, `scripts/run_smoke.ps1`, `scripts/check_repo_ready.py`, and verified clean Git boundaries.
- Authored [`docs/ACCEPTANCE_REPORT.md`](file:///01_github_repo/docs/ACCEPTANCE_REPORT.md) confirming 100% PASS across all verification checks and the six restructuring criteria.
- Moved `p7_test_evaluation.lock` into `02_local_workspace/p7_test_evaluation.lock` recording UTC timestamp, Git commit status, and config SHA-256 (`ab8d5e591854a3dc1f1887565bf33efa581ae88c7719e52bb2195b6420615dfb`).
- Confirmed that `config/config_smoke.yaml` uses dedicated chronological split boundaries (`train_end: 2021-12-31`, `dev_start: 2022-03-31`, `dev_end: 2022-12-31`, `test_start: 2024-01-01`, `test_end: 2024-06-30`) and runs safely under `--dev-only`.

---

### 2. Step 1: Quality Audit Enhancements (`src/bankfail/quality/audit.py`)
- **Per-Quarter IQR Outlier Detection:** Added `_audit_quarterly_iqr_and_variance` computing quartile fences ($Q1 - 1.5 \times IQR$ and $Q3 + 1.5 \times IQR$) per numeric field for every quarterly Call Report period.
- **Constancy / Zero-Variance Screen:** Calculated per-quarter sample variance and added an explicit `zero_variance_flag` for any field exhibiting zero dispersion in a quarter.
- **Non-Destructive Output:** Rows are never removed or mutated; results are exported to:
  - `02_quality/outlier_iqr_by_quarter.csv` (detailed quarterly record per numeric field)
  - `02_quality/outlier_iqr_summary.csv` (field-level aggregate summary across all quarters)
- **Dynamic Minimalist Figure Titles:** Replaced static chart titles with computed f-strings (`f"Top 20 Missingness Rates Across FDIC Fields (Max: {max_m:.1f}%, Mean: {mean_m:.1f}%)"`) and removed top/right spines.

---

### 3. Step 2: Modeling Pipeline Enhancements (`src/bankfail/modeling/pipeline.py`)
- **Missingness Indicator:** Updated `SimpleImputer` across all pipelines to set `add_indicator=True`, preserving missingness signals as explicit binary features.
- **Scaler Comparison on Development PR-AUC:** Implemented side-by-side comparison of `StandardScaler`, `RobustScaler`, and `QuantileTransformer(output_distribution='normal')` on development PR-AUC and ROC-AUC.
- **Feature Selection:** Added optional `SelectKBest(f_classif)` stage inside the pipeline.
- **Grid Tuning with Purged Annual Expanding Window:** Evaluated a small hyperparameter grid (Logistic Regression `C \in [0.01, 0.1, 1.0, 10.0]`; Random Forest `max_depth \in [None, 10, 15]`, `min_samples_leaf \in [5, 20]`) scored via annual expanding-window validation with full 4-quarter purging.
- **Documented KNN Imputation Subsample:** Evaluated `KNNImputer(k=5)` on a subsample ($N \le 500$) and documented why full-dataset KNN on 1.6M rows is computationally intractable ($O(N^2 \cdot D)$).
- **Comparison Output:** Exported all experimental runs and hyperparameter metrics to `07_features_models/p7_model_tuning_comparison.csv`.
- **Frozen Best Configuration:** Best-performing hyperparameters are frozen into `07_features_models/p7_best_model_config.json`, which `--evaluate-test` loads without tuning.
- **Joblib Model Persistence:** Persisted fitted final pipelines to disk via `joblib.dump`:
  - `07_features_models/p7_fitted_pipeline_lr.joblib`
  - `07_features_models/p7_fitted_pipeline_rf.joblib`
- **Minimalist Distribution Chart:** Added `_plot_feature_distributions` generating `07_features_models/p7_feature_distributions.png` displaying `capital_ratio` and `ROA` distributions by decade, clipped at robust percentiles (1st and 99th), with dynamic f-string title.

---

### 4. Step 3: Processing & Scaling Benchmark (`src/bankfail/processing/engines.py`)
- **Amdahl's Law Scaling Benchmark:** Implemented `_measure_amdahl_scaling` evaluating runtime across multiple dataset sizes ($N \in [500, 2000, 10000, \text{full}]$) and thread counts ($n \in [1, 2, 4]$).
- **Parallel Speedup & Fraction:** Computed empirical speedup $S = T_1 / T_n$ and implied parallel fraction $p = \frac{1 - 1/S}{1 - 1/n}$, writing results to `05_engines/p5_amdahl_scaling.csv`.
- **In-Memory Size vs. RAM Profile:** Added `_record_memory_vs_ram` comparing deep in-memory panel footprint against host physical RAM (`psutil.virtual_memory()`), writing to `05_engines/p5_memory_vs_ram.json`.

---

### 5. Step 4: Spark Catalyst Plan Interpretation (`src/bankfail/spark/engine.py`)
- **Plan Parser & Interpreter:** Implemented `parse_and_interpret_spark_plan` parsing executed Catalyst physical plans to count `Exchange` (shuffle), `Sort`, and `Window` operators, and detect join strategy (`SortMergeJoin` vs `BroadcastHashJoin`).
- **Narrow vs. Wide Classification:** Explicitly categorized pipeline operations into narrow (pipelined, zero-shuffle) vs wide (shuffle boundary) transformations.
- **Broadcast Join Policy & Experiment:** Documented why `broadcast(panel)` is prohibited (avoids driver OOM on 1.6M rows) and included a dedicated analysis of broadcasting the small `Institutions` dimension table.
- **Output:** Generated `06_spark/p6_plan_interpretation.md`.

---

### 6. Step 5: Figure Dynamic Titles & Minimalist Styling
- Audited all Matplotlib figures across `audit.py`, `storage.py`, and `pipeline.py`:
  - Replaced all hardcoded string titles with dynamic f-strings built from computed dataset parameters.
  - Applied minimalist styling (removed top and right axis spines, subtle grid lines, clean color palettes).

---

### 7. Step 6: Toy Hand-Calculation Unit Tests (`tests/test_pipeline.py`)
Added 5 verified hand-calculation unit tests covering course slides:
1. `test_lecture_iqr_hand_calculation`: Verifies IQR fences and outlier detection against Lecture 2 Data Cleaning slide 20 ($Q1=26.35, Q3=35.825, IQR=9.475, \text{fences}=[12.1375, 50.0375]$, flagging 500.0).
2. `test_lecture_standard_scaler_hand_calculation`: Verifies `StandardScaler` against Lecture 2 Data Transform slide 13 population standard deviation ($\mu=4, \sigma=\sqrt{8/3} \approx 1.633, z \approx [-1.225, 0, 1.225]$, test value 8 maps to $2.4495$).
3. `test_lag_gap_handling_missing_quarter_returns_nan`: Verifies that a bank reporting in Q1 and Q3 with missing Q2 receives `NaN` for Q3 lag1 without bridging.
4. `test_pandas_polars_5_row_asset_to_deposit_equivalence`: Verifies exact numerical equivalence between Pandas and Polars on 5 rows with zero and null denominators.
5. `test_parquet_column_projection_byte_efficiency`: Verifies that reading 2 projected columns consumes $<25\%$ of full 20-column table bytes.

**Test Suite Status:** **`28 passed, 1 skipped`** (0 failures).

---

### 8. Step 7: Documentation & Decision Updates
- **`README.md`:** Added comprehensive Lakehouse Medallion Architecture table mapping Bronze (raw payload `.json.gz`), Silver (typed Snappy Parquet tables), and Gold (curated panels, features, and models).
- **`docs/AI_USAGE.md`:** Filled complete disclosure template with user verification sign-off blocks (`complete candidate verification entries`).
- **`docs/DECISIONS_LOG.md`:** Added entries for TimeSeriesSplit deviation (calendar-aware purged expanding window), merger policy sensitivity run, and marked `NETINC` as OPEN pending Call Report dictionary confirmation.
