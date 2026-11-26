# BFT65B Final Submission Compliance Audit

- **Course:** BFT65B – Big Data Analytics in Finance · National Economics University
- **Student:** Phạm Quang Huy (`11230967`)
- **Repository:** https://github.com/huyphm2812-commits/bft65b-bank-failure-pipeline
- **Audit Date:** 2026-11-26 (Pre-Deadline Verification; Deadline 2026-11-26 23:00 Vietnam Time)
- **Primary Reference:** `final_report_bft65b.pdf`

---

## 1. 20-Point Assignment Rubric Audit

| # | Assignment Requirement | Status | Primary Evidence in Repository / Report |
|---:|---|---|---|
| 1 | Data acquisition & completeness verification (1984Q1–2026Q2, Failures, Institutions, CERT) | **SATISFIED** | `src/bankfail/acquisition/`, `results_snapshot/01_acquisition/validation_report.json` (`1,682,738` / `4,117` / `27,835`; 170/170 quarters, 0 duplicate keys) |
| 2 | Efficient storage format, schema, partitioning & compression | **SATISFIED** | Typed Year-Partitioned Snappy Parquet (`238` files, `216.09 MiB`), Report Part 1 & Appendix A |
| 3 | Storage benchmark against raw CSV | **SATISFIED** | `results_snapshot/04_storage_benchmark/p4_storage_benchmark_isolated.csv` & regenerated Fig. 1.1 (`12.09×` full, `37.06×` cols, `78.71×` date, `6.35×` write) |
| 4 | Automated data-quality report | **SATISFIED** | `src/bankfail/quality/`, `results_snapshot/02_quality/data_quality_report.json`, Report Table 2.1 & Appendix Table B.1 |
| 5 | Correct treatment of failed banks vs. other exits | **SATISFIED** | `src/bankfail/panel/builder.py`, `results_snapshot/03_panel/exit_classification.csv` (`RESTYPE=FAILURE` vs `ASSISTANCE`; `64,973` merger rows censored, never labeled failure) |
| 6 | Point-in-time correctness & leakage gate | **SATISFIED** | Strict `REPDTE` alignment, `1,211` post-failure rows (`270` CERTs) excluded, `32` gaps unbridged, 9 outcome/exit columns banned + perturbation test, 45d & 1Q-lag sensitivity (`p7_lag1q_dev_refit_sensitivity.json`) |
| 7 | Polars implementation & engine execution explanation | **SATISFIED** | `src/bankfail/processing/engines.py`, `results_snapshot/05_engines/p5_polars_query_plan.txt` (projection pushdown 4/61 cols, `__POLARS_CSER_*` CSE, Arrow buffers) |
| 8 | Fair benchmark against pandas | **SATISFIED** | `results_snapshot/05_engines/p5_p6_unified_benchmark.csv` (fresh processes without `tracemalloc`: Polars 16 thr `1.281 s` vs. Pandas `7.949 s`, `6.21×`; 2 thr `2.010 s`) |
| 9 | PySpark reproduction & matching results | **SATISFIED** | `src/bankfail/spark/engine.py`, `results_snapshot/06_spark/p6_full_equivalence_1682738.json` (`1,682,738` matched, `0` key diffs, `0` value mismatches across 9 cols at `1e-8`) |
| 10 | Spark execution plan interpretation & cluster crossover | **SATISFIED** | `results_snapshot/06_spark/p6_spark_physical_plan.txt`, `p6_plan_interpretation.md`, `p5_p6_exact_scaleup.csv` (exact `1×–5×` 4 GiB cap crossover: Streaming `3×`, Eager `5×`, Spark `local[4]` flat `~2.1 GiB`) |
| 11 | Leak-free feature engineering | **SATISFIED** | 12 CAMELS Variant 3 predictors (`p7_feature_dictionary.csv`), raw `NETINC` & pre-1990 Basel ratios excluded, strictly backward lags |
| 12 | Preprocessing fitted on training data only | **SATISFIED** | `SimpleImputer(median, add_indicator=True)` + `StandardScaler` inside sklearn `Pipeline`s fitted solely on effective `1984Q1–1991Q4` (`522,402` rows) |
| 13 | Baseline & alternative models | **SATISFIED** | Prior Baseline, Always-Negative, L2 Logistic Regression, Random Forest (+ dev-only capital-ratio rule & calibration checks in Table E.2) |
| 14 | Spark MLlib demonstration | **SATISFIED** | `results_snapshot/07_features_models/p7_spark_mllib_dev_metrics.json` & `p7_mllib_vs_sklearn_fit_timing.json` (aligned MLlib V3 `8.09 s` / PR-AUC `0.1285` vs. sklearn `3.77 s` / `0.1313`) |
| 15 | Time-ordered validation with ≥ 4 quarters separation | **SATISFIED** | 4Q training purge (`1992Q1–1992Q4`) + **8-quarter embargo (`2006Q1–2007Q4`)** between dev (`1993Q1–2005Q4`) and test (`2008Q1–2014Q4`) |
| 16 | At least 50 failures in the test period | **SATISFIED** | **513 distinct failed institutions** (`2,005` positive bank-quarters) in `2008Q1–2014Q4` |
| 17 | Cost-based threshold selection estimated from data | **SATISFIED** | `DEF_A` 100:1 using historical FDIC `COST` (dev median `$5,919.225k`) and `$59.19225k` $C_{FP}$ grounded in FDIC 2023–2024 RMS budgets + fixed-$C_{FP}$ CPI sensitivity (`p7_cpi_fixed_cfp_sensitivity_dev.json`) |
| 18 | Only one final evaluation on the test period | **SATISFIED** | Pre-frozen SHA-256 config (`frozen_model_config.json`), archived pre-eval guard, active `p7_test_evaluation.lock` (`COMPLETED`); never rerun |
| 19 | Complete conclusions answering Q1–Q3, limitations & Vietnam adaptation | **SATISFIED** | Report Part 6 (Sec 6.1–6.3): direct synthesis of Q1–Q3, 5 limitations, 7-point SBV conceptual adaptation with explicit non-equivalence disclaimer |
| 20 | End-to-end reproducibility & public repo hygiene | **SATISFIED** | Public GitHub repo, pinned `requirements.txt`, fixed seed `42`, zero committed datasets (`4.87 MiB` total), fresh-clone verified `pytest -q` (`42 passed, 2 skipped`) & `run_smoke.ps1` (exit `0`, `76.99 s`) |

---

## 2. Format & Artifact Notes

- **Code vs. Notebooks:** `final_report_bft65b.pdf` (Section 4.2) specifies *"the code or notebooks (with outputs and fixed seeds)"*. This project provides a modular, pytest-tested Python package (`src/bankfail/*`, `scripts/*`) with saved outputs in `results_snapshot/` and fixed `seed = 42` rather than Jupyter notebooks; this satisfies the specification cleanly.
- **No Unresolved Leakage Findings:** Automated outcome-column perturbation checks pass (`0` feature delta), and the documented 45-day reporting lag metadata limitation is explicitly disclosed and quantified via read-only positive-exclusion and dev-only 1-quarter lagged-feature refit sensitivity analyses.
- **Submission Corrections Completed in This Pass:**
  1. Pinned all direct imports (`numpy==2.4.0`, `scipy==1.17.1`, `joblib==1.6.0`) in `requirements.txt`.
  2. Expanded `.gitignore` with `*.json.gz`, `*.pkl`, `*.lock`, `.ipynb_checkpoints/`, `.DS_Store`, `Thumbs.db`.
  3. Expanded `README.md` to cover all 13 submission documentation items with verified figures matching the final report.
  4. Verified all 8 mandatory Page 1 items on `BFT65B_Final_Academic_Report.pdf` (19 pages) and `BFT65B_Final_Academic_Appendix.pdf` (9 pages).
