# BFT65B Number Reconciliation & Audit Verification

This document reconciles all core dataset, panel, quality audit, and benchmark metrics against generated files. It explicitly compares:
(a) The authoritative value extracted directly from the generated file.
(b) The value previously reported in prior draft summaries.
(c) The authoritative ground truth and root-cause explanation of discrepancies.

---

## 1. Metric Reconciliation Table

| Metric | (a) Generated File Value | (b) Previously Reported Value | (c) Authoritative Truth & Reconciliation Notes | Citation Source |
| :--- | :---: | :---: | :--- | :--- |
| **Total Panel Rows** | **1,682,738** | 1,682,738 | **1,682,738** (Exact match across all files) | `03_panel/panel_summary.json` |
| **Eligible Rows** | **1,597,372** | 1,280,317 | **1,597,372** is correct. The previous 1,280,317 was based on an outdated draft calculation where merger censoring was overstated. | `03_panel/panel_summary.json` |
| **Positive Labels ($y=1$)** | **10,823** | 10,724 | **10,823** is correct. Exactly 10,823 bank-quarters fall in the 4-quarter pre-failure failure event horizon $(q, q+4]$. | `03_panel/panel_summary.json` |
| **Negative Labels ($y=0$)** | **1,586,549** | 1,269,593 | **1,586,549** is correct. Corresponding to eligible non-failure bank-quarters. | `03_panel/panel_summary.json` |
| **Censored / Unlabeled Rows** | **85,366** | 402,421 | **85,366** is correct. The previous 402,421 incorrectly included assumed non-censored quarters. | `03_panel/panel_summary.json` |
| **Merger Policy Sensitivity Delta** | **+64,902 rows** | +392,460 rows | **+64,902 rows** is correct. Retaining mergers as negatives increases eligible rows from 1,597,372 to 1,662,274. | `03_panel/panel_summary.json` |
| **Negative NTLNLS Count** | **299,759 rows** | 3,675 rows | **299,759 rows** is correct. Call Report item `NTLNLS` (Net Loans and Leases Charged Off) is net of recoveries ($Gross - Recoveries$), producing valid negative numbers in 299,759 quarters. | `02_quality/financials/invalid_values.csv` |
| **Institutions Population** | **27,835** (all institutions)<br>**24,064** (in Financials) | 24,064 | **27,835** total cataloged institutions in FDIC institutions table; **24,064** distinct institutions filed Call Reports. | `data/processed/institutions`, `validation_report.json` |
| **Exit Classification (`merger_other`)** | **20,569** | 11,811 | **20,569** is correct across all 27,835 institutions. | `03_panel/exit_classification.csv` |
| **Exit Classification (`active_or_no_exit`)** | **4,223** | 8,455 | **4,223** is correct across all 27,835 institutions. | `03_panel/exit_classification.csv` |
| **Exit Classification (`failure`)** | **3,036** | 3,595 | **3,036** is correct for institutions mapped in exit classification. | `03_panel/exit_classification.csv` |
| **Exit Classification (`assistance_nonfailure`)** | **7** | 203 | **7** is correct in `exit_classification.csv`. (The 203 figure is the total number of historical assistance records in the failures table from 1934 to 2026, of which 196 subsequently merged or failed). | `03_panel/exit_classification.csv` |
| **Storage Benchmark (Rep 3)** | **4,550.22 s / 1,503.80 MB** | 4,550.22 s / 1,503.80 MB | **Exact Match** (75.84 min wall time, 1,503.80 MB peak RSS RAM). | `02_local_workspace/logs/benchmark_full.txt` |
| **Engines Benchmark (Rep 5)** | **251.05 s / 2,322.13 MB** | 251.05 s / 2,322.13 MB | **Exact Match** (4.18 min wall time, 2,322.13 MB peak RSS RAM). Polars speedup: 14.7x. | `02_local_workspace/logs/engines_full.txt` |
| **Spark Comparison (Rep 2)** | **261.96 s / 3,791.34 MB** | 261.96 s / 3,791.34 MB | **Exact Match** (4.37 min wall time, 3,791.34 MB peak RSS RAM). Output equivalence: 100%. | `02_local_workspace/logs/spark_full.txt` |

---

## 2. Origin of Threshold Selection & Cost Sensitivity Files

**CRITICAL FINDING:**
- The files `p7_threshold_selection.csv` and `p7_cost_sensitivity_ratios.csv` that were previously reported and found in `02_local_workspace/outputs/07_features_models_sample5pct/` and `03_report_inputs/07_features_models_sample5pct/` were generated **strictly from the 5% sample dry run** (`sample_5pct_panel.parquet`).
- In the full dataset run (Step 4), the execution crashed during `_run_model_tuning` (Random Forest expanding-window cross-validation with `numpy.core.exceptions._ArrayMemoryError` at 7.85 GB RAM), before reaching threshold selection or cost sensitivity evaluation.
- **Quarantine Action:** All sample-derived files in `03_report_inputs/07_features_models/` that did not originate from the full pipeline have been identified. Real full-dataset threshold selection and cost sensitivity tables will only be generated once Step 1 (memory fix) and Step 2 (full dev-only pipeline run) complete successfully.
