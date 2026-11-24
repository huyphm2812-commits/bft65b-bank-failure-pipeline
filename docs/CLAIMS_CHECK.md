# BFT65B Claims and Empirical Evidence Audit (`docs/CLAIMS_CHECK.md`)

This document verifies key claims in the project documentation against the generated empirical output files, code implementations, and benchmark logs. Every claim is strictly classified as **SUPPORTED**, **WRONG**, or **UNSUPPORTED**.

---

## 1. Summary Audit Table

| # | Topic / Claim | Claimed in Previous Briefing | Generated File / Source Value | Status | Evidence Citation |
|---|---|---|---|---|---|
| **1** | **Post-Failure Rows & CERT Distribution** | 1,211 post-failure rows; vague or missing CERT breakdown. | Reconciled directly from `post_failure_reports.csv`, `bank_quarter_panel.parquet`, and `LABEL_EDGE_CASES.md`: **1,211 rows** across **270 distinct CERTs** (`REPDTE >= FAILDATE`: 3 on FAILDATE, 1,208 after); **496 rows (40.96%)** within 1 calendar quarter ($\Delta Q \le 1$) and **715 rows (59.04%)** $\ge 2$ calendar quarters ($\Delta Q > 1$). (Earlier summary entry citing 208 CERTs / 662 / 549 was corrected.) | **RECONCILED & SUPPORTED** | `02_quality/post_failure_reports.csv`, `03_panel/LABEL_EDGE_CASES.md`, `03_panel/bank_quarter_panel.parquet` |
| **2** | **Per-Engine Peak RAM (Engines Benchmark)** | Polars peak RAM < 500 MB (confused Python heap with process RSS). | Pandas: process RSS 2,115.01 MB (median), Python heap peak 2,323.88 MB.<br>Polars: process RSS **3,519.59 MB** (median), Python heap peak 0.33 MB. | **WRONG** (prior <500MB RSS claim); **SUPPORTED** (by summary CSV) | `05_engines/p5_engine_benchmark_summary.csv`, lines 2–3 |
| **3** | **45-Day Reporting Lag Use in Code** | Claimed to actively gate labels or joins. | The 45-day lag (`REPDTE + 45 days`) is computed and stored as metadata (`financial_available_date`, `point_in_time_note`); **NOT** used to gate labels or filter joins. Labels use calendar quarter window $(q, q+4]$ quarters. | **WRONG** (active filter claim); **DOCUMENTED ASSUMPTION ONLY** | `src/bankfail/panel/builder.py`, lines 42–44, 281–285 |
| **4** | **Number of Unit Tests** | "44 tests passed 100%". | **41 tests collected**: **39 passed**, **2 skipped** (`test_spark_matches_polars_on_summary_and_sample`, `test_spark_comparison_sample_rows_validation` skipped due to missing Hadoop binary on Windows). | **WRONG** (count was 41, not 44; 2 skipped, not 100% unskipped) | `pytest` test run session logs |
| **5** | **Polars Plan Operators** | Alleged `FAST_COUNT` or complex hash aggregations. | Operators present: `LEFT JOIN`, `Parquet SCAN`, `PROJECT`, `WITH_COLUMNS`, `FILTER`, `SORT BY`, Window `shift().over()`, `simple π`, `UNIQUE`. (No `FAST_COUNT` or `GROUP BY`). | **SUPPORTED** (for present operators); **WRONG** (for unlisted operators) | `05_engines/p5_polars_query_plan.txt`, lines 1–33 |
| **6** | **"Polars handles 50–100M rows"** | Claimed as empirical capacity threshold. | Measured dataset is 1.68M rows (1,682,738 bank-quarters). No 50–100M row benchmark was executed. | **UNSUPPORTED** (Rule of thumb; needs a larger-scale test) | `05_engines/p5_scaling_benchmark.csv` (max 1.68M rows) |
| **7** | **"Spark only needed above 64GB"** | Claimed as strict architecture cutoff. | Empirical in-memory size of full panel is 2,146.57 MB (30.3% of 7.08 GB available RAM). No 64 GB test executed. | **UNSUPPORTED** (Extrapolation; needs a larger-scale test) | `05_engines/p5_memory_vs_ram.json` |
| **8** | **Vietnamese Regulatory Circulars** | Circular 41/2016/TT-NHNN, Circular 11/2021/TT-NHNN cited as direct parallels. | Cited in documentation as conceptual proxies; not verified against State Bank of Vietnam statutory legal gazettes in this codebase. | **TODO(verify)** | `docs/EVIDENCE_MAP.md`, lines 100–101 |

---

## 2. Detailed Findings

### 2.1 Post-Failure Rows & Edge Cases
- **Generated Data:** `02_quality/post_failure_reports.csv`, `03_panel/LABEL_EDGE_CASES.md`, and `03_panel/bank_quarter_panel.parquet`.
- Exactly **1,211** financial reports satisfy `REPDTE >= FAILDATE` (3 on `FAILDATE`, 1,208 strictly after).
- These 1,211 rows belong to **270 distinct CERTs**.
- Timing distribution (calendar quarter difference $\Delta Q = Q_{\text{REPDTE}} - Q_{\text{FAILDATE}}$):
  - Within 1 calendar quarter ($\Delta Q \le 1$): **496 rows (40.96%)** (268 at $\Delta Q = 0$; 228 at $\Delta Q = 1$).
  - 2 or more calendar quarters after failure ($\Delta Q > 1$): **715 rows (59.04%)**.
  - By exact calendar days ($d = \text{REPDTE} - \text{FAILDATE}$): $d \le 92$ days = **278 rows (22.96%)**; $d > 92$ days = **933 rows (77.04%)**.
- Label edge cases decision: Decided as **Option A** (keep panel unchanged; post-F1 quarters remain censored; 10 rows in 1984–1992 train window would change under event-level rule; 0 rows in dev or test).

### 2.2 Memory Benchmark Realities
- In `p5_engine_benchmark_summary.csv`:
  - Pandas Python heap: **2,323.88 MB** peak; process RSS: **2,115.01 MB**.
  - Polars Python heap: **0.33 MB** peak (Rust handles memory off the Python heap).
  - Polars process RSS: **3,519.59 MB** median endpoint.
  - Previous briefings that reported Polars peak RAM as "< 500 MB" only looked at `median_peak_python_mb` and ignored process RSS (`median_rss_endpoint_mb`). Both figures must be reported together.

### 2.3 The 45-Day Reporting Lag
- In `src/bankfail/panel/builder.py`:
  ```python
  panel["financial_available_date"] = panel["REPDTE"] + pd.to_timedelta(reporting_lag_days, unit="D")
  panel["point_in_time_note"] = "Financials at REPDTE become available under the configured reporting-lag assumption..."
  ```
- The 45-day lag is strictly an **informational metadata column** documenting when call reports are publicly available. It does not gate the label assignment window, which uses calendar quarter boundaries $(q, q+4]$.

### 2.4 Test Suite Reality
- `pytest` collects **41 items**.
- **39 tests pass**, **2 tests are skipped** (both require PySpark Hadoop native libraries on Windows).
- Zero tests fail.

### 2.5 Scale Claims
- Any statements asserting "Polars is optimal up to 50–100M rows" or "Spark is required only above 64GB" are heuristic engineering opinions, not empirical conclusions of this project. They are marked **UNSUPPORTED** and qualified as "needs a larger-scale test".
