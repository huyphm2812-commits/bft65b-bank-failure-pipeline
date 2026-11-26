# A Big-Data Pipeline for Bank Failure Early Warning

**Course:** BFT65B – Big Data Analytics in Finance · National Economics University (NEU)  
**Author:** Phạm Quang Huy (Student ID: `11230967`)  
**Repository:** https://github.com/huyphm2812-commits/bft65b-bank-failure-pipeline  
**Final Report & Technical Appendix:** [`docs/BFT65B_Final_Academic_Report.pdf`](docs/BFT65B_Final_Academic_Report.pdf) (19 pages) · [`docs/BFT65B_Final_Academic_Appendix.pdf`](docs/BFT65B_Final_Academic_Appendix.pdf) (9 pages)

---

## 1. Project Objective

This repository implements an end-to-end, reproducible big-data engineering and financial-econometrics pipeline for bank failure early warning. Following the BFT65B central-bank supervision scenario, engineering rigor—complete resumable acquisition, columnar storage benchmarking, automated non-destructive data-quality auditing, point-in-time panel construction, Polars/PySpark scalable processing, and leak-free out-of-time validation—takes precedence over chasing marginal accuracy gains.

The pipeline and report answer three research questions:
- **Q1 (Storage & Scalable Processing):** How should four decades of bank reports (`1.68M` bank-quarters) be stored and processed on a standard laptop, and under what measured conditions is a distributed Spark cluster justified?
- **Q2 (Data Quality & Look-Ahead Prevention):** What quality problems exist in the raw FDIC tables, and how are they handled systematically without introducing look-ahead bias?
- **Q3 (Honestly Validated Early Warning):** Can the pipeline support an honestly validated, cost-sensitive early-warning model evaluated strictly once on held-out out-of-time data, and what changes are required to adapt it conceptually to Vietnam?

---

## 2. FDIC Data Sources & Acquisition Method

All data are public US Federal Deposit Insurance Corporation (FDIC) tables acquired on **2026-11-20** via the **FDIC BankFind Suite REST API** (`https://api.fdic.gov/banks`, no API key required), linked by institutional certificate number (`CERT`):

| Table | Verified Scope & Row Count | Key Integrity & Completeness |
| :--- | :--- | :--- |
| **Financials** (`financials/year=YYYY`) | `1,682,738` bank-quarters (`1984Q1–2026Q2`, 170 quarters, `24,064` distinct filing CERTs) | 100% quarter completeness across 238 Parquet files; `0` duplicate `(CERT, REPDTE)` keys |
| **Failures** (`failures.parquet`) | `4,117` resolution events (`3,524` `RESTYPE=FAILURE`, `593` `RESTYPE=ASSISTANCE`) | `488` missing CERTs all pre-1977 (`0` in 1984–2026); `44` CERTs with repeated entries |
| **Institutions** (`institutions.parquet`) | `27,835` cataloged banks (`1934–2026`) | `20,569` merger/other (`73.89%`), `4,223` active (`15.17%`), `3,036` failures (`10.91%`), `7` assistance non-failures (`0.03%`) |

**Resumable Acquisition Engine (`src/bankfail/acquisition/`):** Deterministic cursor pagination in 10,000-record batches, gzip transport (`Accept-Encoding: gzip`), exponential backoff (1.0 s base, 2.0× multiplier, 60 s cap, up to 6 attempts honoring `Retry-After`), and atomic per-quarter JSON manifests with `meta.total` counts and SHA-256 digests for zero-call resumability.

> **Data Policy:** In compliance with the BFT65B assignment, **no raw or processed FDIC datasets are committed to Git** (enforced by `.gitignore`). Compact verified summaries, execution plans, frozen configs, and figures are preserved in `results_snapshot/`.

---

## 3. Project Architecture & Directory Structure

The codebase is organized as a modular Python package (`src/bankfail/`) with deterministic CLI runners (`scripts/`), YAML configs (`config/`), automated unit tests (`tests/`), and a Lakehouse Medallion local workspace (`../02_local_workspace/`, excluded from Git):

```text
01_github_repo/
├── config/
│   ├── config.yaml                  # Production pipeline configuration (fixed seed = 42)
│   └── config_smoke.yaml            # Offline synthetic smoke-test configuration
├── docs/                            # Final PDF/DOCX reports, audit logs, preregistration & AI disclosure
├── results_snapshot/                # Compact JSON/CSV/TXT/PNG outputs from verified production & dev-only runs
├── scripts/
│   ├── run_pipeline.py              # Unified CLI entrypoint (connection..check)
│   ├── freeze_config.py             # Hardened pre-test configuration freezer with SHA-256 verification
│   ├── export_results.py            # Exports lightweight snapshot artifacts for Git & report inputs
│   ├── run_smoke.ps1 / .sh          # Offline end-to-end synthetic smoke runners
│   └── run_full.ps1 / .sh           # Full ordered production pipeline runners
├── src/bankfail/
│   ├── acquisition/                 # Resumable FDIC API client, downloader & completeness validator
│   ├── quality/                     # Automated non-destructive data quality auditor
│   ├── panel/                       # Exit classification & point-in-time 4Q forward label builder
│   ├── storage/                     # Format, partitioning & compression benchmark suite
│   ├── processing/                  # Polars LazyFrame vs. pandas engine benchmark & scaling
│   ├── spark/                       # PySpark reproduction, equivalence checker & Catalyst plan capture
│   ├── modeling/                    # Leak-free CAMELS pipeline, dev bootstrap, cost thresholds & MLlib demo
│   └── reproducibility/             # Repository hygiene checks & manifest freezer
├── tests/                           # 44 automated pytest tests + offline fake_fdic_server.py fixture
├── .gitignore                       # Blocks all datasets, parquet/csv.gz/joblib/lock artifacts & secrets
├── .python-version                  # Pinned Python 3.11.9
├── requirements.txt                 # Fully pinned package versions
└── README.md
```

| Medallion Tier | Local Workspace Directory (Git-Ignored) | Content |
| :--- | :--- | :--- |
| **Bronze (Raw)** | `../02_local_workspace/data/raw/` | Immutable raw FDIC API payloads (`.json.gz`) + SHA-256 manifests |
| **Silver (Processed)** | `../02_local_workspace/data/processed/` | Typed Year-Partitioned Snappy Parquet (`financials/year=YYYY/`, `failures.parquet`, `institutions.parquet`) |
| **Gold (Analytical)** | `../02_local_workspace/outputs/` | Stage outputs `01_acquisition/` through `08_reproducibility/` |

---

## 4. Environment & Installation

- **Tested Python Version:** `Python 3.11.9` (pinned in `.python-version`; compatible with Python 3.10+).
- **Operating Systems:** Windows 10/11 (PowerShell), Linux, macOS.
- **Java for PySpark:** Local PySpark (`spark` stage and `--spark-mllib`) requires **OpenJDK 17** (`JAVA_HOME` pointing to a JDK 17 installation, e.g., [Eclipse Temurin OpenJDK 17](https://adoptium.net/temurin/releases/)).

```powershell
# From the repository root (01_github_repo/)
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -r requirements.txt
```

---

## 5. Reproduction Mode & Executing the Pipeline

Because the Git repository excludes raw and processed FDIC data (`../02_local_workspace/`), a reviewer cloning the repository on a fresh machine can reproduce and verify the pipeline in two ways:

### A. Fast Offline Synthetic Verification (Fresh-Clone Verified, Zero Internet)

1. **Automated Unit Test Suite (`44` tests; fixed seeds):**
   ```powershell
   pytest -q
   # Optional: include local PySpark integration test (requires OpenJDK 17)
   # $env:BFT65B_RUN_SPARK_TEST="1"; pytest -q
   ```
   *Fresh-clone verification result (2026-11-25, zero pre-existing `02_local_workspace/`):* **`42 passed, 2 skipped` in `21.65 s`** (the 2 skipped tests are the optional live-API and PySpark integration checks).

2. **Offline End-to-End Synthetic Smoke Pipeline (`fake_fdic_server.py`):**
   ```powershell
   powershell -ExecutionPolicy Bypass -File .\scripts\run_smoke.ps1
   # Linux / macOS:
   # ./scripts/run_smoke.sh
   ```
   *Fresh-clone verification result (2026-11-25):* **Exit `0` in `76.99 s`**, generating **143 workspace files** across all 7 offline stages (`01_acquire` through `08_model_dev_only`). *(Synthetic smoke numbers are strictly for algorithmic verification and are never reported in the paper.)*

### B. Full Production Pipeline on Real FDIC Data (1984Q1–2026Q2)

```powershell
# 1. Connection probe (~10 s)
python scripts\run_pipeline.py connection

# 2. Resumable historical API acquisition (~45–60 min; safe to interrupt & resume)
python scripts\run_pipeline.py acquire

# 3–5. Validation, automated quality audit, and point-in-time panel (~4–5 min total)
python scripts\run_pipeline.py validate
python scripts\run_pipeline.py audit
python scripts\run_pipeline.py panel

# 6–8. Storage benchmark, Polars vs. pandas engines, and PySpark Catalyst plan
#      (Tip: pass --sample-rows 50000 to verify mechanics in < 2 min first)
python scripts\run_pipeline.py benchmark --repetitions 5
python scripts\run_pipeline.py engines --repetitions 5
python scripts\run_pipeline.py spark --repetitions 2

# 9–10. Dev-only CAMELS feature selection, bootstrap, cost curves, MLlib demo & hygiene check
python scripts\run_pipeline.py model --dev-only --dev-analysis --spark-mllib
python scripts\run_pipeline.py check
```

Or run the entire development chain via `powershell -ExecutionPolicy Bypass -File .\scripts\run_full.ps1`.

### C. Pre-Test Configuration Freeze, Rehearsal, and One-Time Held-Out Test Lock

```powershell
# 1. Freeze configuration explicitly (supports --dry-run)
python scripts\freeze_config.py `
  --split-train-end 1992-12-31 --split-dev-start 1993-03-31 --split-dev-end 2005-12-31 `
  --split-test-start 2008-01-01 --split-test-end 2014-12-31 `
  --loss-definition DEF_A --fn-fp-ratio 100:1 --feature-variant variant_3 `
  --final-model random_forest --training-policy train_only --seed 42 --threshold 0.103990 `
  --output results_snapshot/07_features_models/frozen_model_config.json

# 2. Rehearsal run (validates test-stage inventory on pre-2006 data using an isolated rehearsal lock)
python scripts\run_pipeline.py model --rehearsal

# 3. Single held-out test evaluation (guarded by p7_test_evaluation.lock; runs strictly once)
python scripts\run_pipeline.py model --config config\config.yaml `
  --frozen-config results_snapshot\07_features_models\frozen_model_config.json --evaluate-test
```

**Soft vs. Strict Hash Mode on Fresh Downloads:** `results_snapshot/07_features_models/frozen_model_config.json` pins the SHA-256 digest of the `2026-11-20` panel. Because FDIC occasionally revises historical Call Reports, a later API download may produce a different panel hash and cause strict `--frozen-config` checks to fail closed; in that case, run development stages without `--frozen-config` or re-freeze your local panel digest via `scripts/freeze_config.py`.

---

## 6. Data-Quality Procedures & Point-in-Time Panel

The automated non-destructive audit (`02_quality/`) systematically uncovered and documented nine issues (full census in `results_snapshot/02_quality/` and Report Table 2.1):
- **Pre-1990 Basel I structural zeros:** `RBCRWAJ` is 100% exact zero during 1984–1989 (`468,480` zeros, `27.84%`); `RBC1AAJ` has `79,508` zeros (`4.72%`) → recoded in sensitivity variants and excluded from frozen Variant 3.
- **Pre-2001 `P3ASSET` uncollection:** `972,047` missing values (`57.77%`) → excluded from Variant 3.
- **Valid negative net charge-offs (`NTLNLS`):** `299,759` negative values (`17.81%`) represent net loan recoveries → naive non-negativity rule rejected and negatives retained.
- **Cumulative YTD `NETINC` flow:** Resets every Q1 → excluded in favor of annualized `ROA`, `ROE`, `NIMY`, and 4Q trailing `roa_rolling_4q_mean`.
- **Repeated failure/assistance CERTs (`44` CERTs) & pre-1977 missing CERTs (`488` rows):** Separated `RESTYPE=FAILURE` from `RESTYPE=ASSISTANCE`; anchored failure date to earliest explicit `FAILURE`.
- **Reporting gaps (`32` gaps) & post-failure filings (`1,211` rows across `270` CERTs with `REPDTE ≥ FAILDATE`):** Never bridged/forward-filled; `1,582` incomplete-future rows and all `1,211` post-failure rows censored.
- **Unassisted mergers & right-censoring:** `64,973` rows within 4Q of non-failure exits and `17,529` post-`2025Q2` rows censored (never labeled failures), yielding `1,597,372` eligible bank-quarters (`10,823` positive, `1,586,549` negative).

---

## 7. Storage, Polars vs. Pandas, and PySpark Benchmarks

All benchmarks were executed on a 12-physical / 16-logical-core workstation (`16,088.49 MiB` total RAM; `7,079.12 MiB` available; binary MiB/GiB throughout):

- **Storage Layout (`results_snapshot/04_storage_benchmark/p4_storage_benchmark_isolated.csv`):** Recommended **Year-Partitioned Snappy Parquet** (`216.09 MiB`, `134.66 B/row`, `1.98×` smaller than `427.33 MiB` raw CSV) delivers **`12.09×`** faster full reads (`0.328 s` vs. `3.970 s`), **`37.06×`** faster like-for-like 5-column reads (`0.055 s` vs. `2.035 s` CSV `usecols`), **`78.71×`** faster date-pruned reads (`0.050 s` vs. `3.936 s`, peak RSS `385.16 MiB`), and **`6.35×`** faster writes (`3.74 s` vs. `23.75 s` single-threaded CSV). Single-file Zstd (`143.93 MiB`) minimizes cold archival size.
- **Reconciled Core Engine Benchmark (`results_snapshot/05_engines/p5_p6_unified_benchmark.csv`, fresh processes, 1 warm-up + 5 reps, identical 33-col scan + sort + gap-aware 1Q lag + 4-col join + collect + 170Q summary):**
  - **Pandas (1 thr):** `7.949 s` (`5,292.12 MiB` peak RSS)
  - **Polars LazyFrame (1 / 2 / 4 / 16 thr):** `3.193 s` / `2.010 s` (`S=1.59×, p=0.74`) / `1.578 s` (`S=2.02×, p=0.67`) / **`1.281 s`** (`6.21×` faster than Pandas; `2,753.22 MiB` peak RSS)
  - **PySpark (`local[1]` / `local[2]` / `local[4]`):** `50.506 s` / **`29.007 s`** (`14.43×` slower than matched 2-thread Polars; `1,920.13 MiB` peak RSS) / `20.733 s`
- **Exact Table 3.1 Workload Scale-Up (`1×–5×`, 4 threads, matched `4,096 MiB` cap; `p5_p6_exact_scaleup.csv`):** Polars stays `4.57×–33.42×` faster than Spark `local[4]` across `1×–5×`, but **Polars Streaming** first exceeds the `4.0 GiB` cap at **`3×` (`5.05M` rows, `4,665 MiB`)** and **Polars Eager** first exceeds it at **`5×` (`8.41M` rows, `6,311 MiB`)**, whereas single-machine Spark `local[4]` stays flat at **`2,063–2,098 MiB`** via disk spill (`87.18 s` at `5×`). A multi-node Spark cluster is therefore unnecessary at `1×` (`30.32%` of available RAM) and justified only beyond `~5×` (`~8.4M+` wide join+window rows under a 4 GiB cap) when local spill latency or multi-terabyte/streaming workloads exceed single-node limits.
- **PySpark Equivalence, Catalyst Plan & MLlib Demo:** Bidirectional full-dataset anti-joins across all `1,682,738` rows (`results_snapshot/06_spark/p6_full_equivalence_1682738.json`) confirmed **`0` key differences and `0` value mismatches** across 9 audited columns (tolerance `10⁻⁸`, rel diff `≤ 2.1×10⁻¹⁴`). Catalyst physical plans (`p6_spark_physical_plan.txt`) confirm 4-partition window shuffles (`Exchange hashpartitioning(CERT, 4)`) and `BroadcastHashJoin`. On identical cached Variant 3 dev data (`p7_mllib_vs_sklearn_fit_timing.json`), aligned Spark MLlib LR fits in `8.09 s` (dev PR-AUC `0.1285`) vs. `3.77 s` for sklearn LR (`0.1313`; `2.1×–2.8×` fit-only ratio).

---

## 8. Feature Engineering, Leakage Prevention & Validation

- **Frozen Variant 3 Features (`12` CAMELS predictors, `p7_feature_dictionary.csv`):** `capital_ratio`, `loan_to_asset`, `noncurrent_loan_to_asset`, `ore_to_asset`, `cash_to_asset`, `securities_to_asset`, `roa_level`, `roe_level`, `nimy_level`, `asset_growth_qoq`, `asset_growth_yoy`, `roa_rolling_4q_mean` (selected via preregistered 1,000-resample bank-cluster bootstrap on `1993Q1–2005Q4`, seed `42`).
- **Leakage Controls (`docs/LEAKAGE_AUDIT.md`, `docs/AI_USAGE.md`):** Strict chronological splitting (`Train 1984Q1–1991Q4` with 4Q label purge before `1993Q1`; `Dev 1993Q1–2005Q4` [100 failed banks]; **`8Q` Embargo `2006Q1–2007Q4`**; `Held-Out Test 2008Q1–2014Q4` [513 failed banks]), training-only `SimpleImputer(strategy='median', add_indicator=True)` + `StandardScaler` inside scikit-learn `Pipeline`s, programmatic exclusion of 9 outcome/exit columns verified by an automated perturbation check, and post-freeze dev-only 45-day / 1-quarter lagged-feature sensitivity audits (`p7_reporting_lag_45d_sensitivity.json`, `p7_lag1q_dev_refit_sensitivity.json`).
- **Cost-Sensitive Thresholding (`DEF_A`, `100:1` FN:FP):** Missed failures incur FDIC `COST` (1 missing dev cost imputed to dev median `$5,919.225k`); false alarms incur `$59.19225k` (grounded via FDIC 2023–2024 RMS budgets `$641.88M` / `$708.60M` across `4,000` / `3,883` exams = `$160.47k` / `$182.49k` per full exam [`$113.62k` in 2005 USD] with an illustrative 30%–55% follow-up share, plus fixed-$C_{FP}$ CPI sensitivity in `p7_cpi_fixed_cfp_sensitivity_dev.json`). Frozen thresholds: RF `τ* = 0.103990`, LR `τ* = 0.601878`.

---

## 9. Key Held-Out Test Results (Evaluated Once: 2008Q1–2014Q4)

Scope: `205,822` eligible rows, `2,005` positive bank-quarters, `513` failed institutions (`p7_model_results.json`, `p7_model_metrics.csv`):

| Model (Frozen Variant 3) | Frozen `τ*` | PR-AUC | ROC-AUC | Brier Score | Row Recall | Bank Capture | Quarterly Alarms | DEF_A Expected Loss |
| :--- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| **Random Forest (Primary)** | `0.103990` | **0.6176** | 0.9748 | **0.006168** | **88.08%** | **97.08%** (498/513) | 271.0 / Q (3.60%) | **$712,941.77k** |
| **Logistic Regression (Baseline)** | `0.601878` | 0.5553 | **0.9803** | 0.028241 | 87.38% | 97.08% (498/513) | 254.6 / Q (3.40%) | $773,022.58k |
| **Prior / Naive Baseline** | `1.000000` | 0.0097 | 0.5000 | 0.009677 | 0.00% | 0.00% (0/513) | 0.0 / Q (0.00%) | $70,453,896.63k |

Under constrained Top-1% quarterly inspection capacity, Random Forest catches **89.47% (`459/513`)** of failing banks vs. **78.75% (`404/513`)** for Logistic Regression (`+10.72 pp`).

**Key Limitations & Vietnam Context:** (1) Macroeconomic regime/prevalence shift (`0.0578%` dev vs. `0.9741%` test) lifted RF alarm volume from `58.3/Q` to `271.0/Q` and caused probability overprediction (`1.91%` vs. `0.97%`), while quarterly Call Reports lag rapid uninsured-deposit runs (2023); (2) the 45-day reporting lag is metadata rather than a filing-timestamp join gate (though descriptive test bank capture excluding `≤45d` rows remains `95.90% [491/512]`); (3) unassisted mergers/forbearance alter hazard rates; (4) aggregate statements omit borrower-level concentration; and (5) adapting this architecture to the State Bank of Vietnam (SBV) requires local identifiers, VAS accounting mappings, domestic special-control/mandatory-transfer target definitions, verified local filing lags, and small-sample regularized ranking rather than transferring US weights.

---

## 10. Academic References & AI Assistance Disclosure

- **FDIC Data & Regulatory Sources:**
  - FDIC BankFind Suite REST API Documentation: https://api.fdic.gov/banks/docs/ (Accessed 2026-11-25)
  - FDIC RISView Field Dictionary (`risview_properties.yaml`): https://api.fdic.gov/banks/docs/risview_properties.yaml (Accessed 2026-11-25)
  - FDIC (2023), *Memorandum on the Proposed 2024 FDIC Operating Budget* (`$641.88M` 2023 / `$708.60M` 2024 RMS budgets); FDIC (2024), *FDIC 2023 Annual Report* (`4,000` exams); FDIC (2025), *FDIC 2024 Annual Report* (`3,883` exams): https://www.fdic.gov/about/financial-reports/ (Accessed 2026-11-25)
  - U.S. Bureau of Labor Statistics (BLS), *Consumer Price Index for All Urban Consumers (CPI-U)*: https://www.bls.gov/cpi/ (Accessed 2026-11-25)
  - FFIEC (2014), *Uniform Financial Institutions Rating System (CAMELS)*; BCBS (1988), *International Convergence of Capital Measurement and Capital Standards*; State Bank of Vietnam portal (https://www.sbv.gov.vn/, Accessed 2026-11-25)
- **Academic & Course Literature:** Amdahl (1967, *AFIPS* 30, 483–485); Wheelock & Wilson (2000, *Rev. Econ. Stat.* 82(1), 127–138); Cole & White (2012, *J. Financ. Serv. Res.* 42(1), 5–29); Pedregosa et al. (2011, *JMLR* 12, 2825–2830); Nguyen, T. L. (2026), *BFT65B – Big Data Analytics in Finance* (Lectures 2–6 & optional starter `fdic_download_starter.py`), National Economics University.
- **Core Software Documentation (Accessed 2026-11-25):** Polars (https://docs.pola.rs/), pandas (https://pandas.pydata.org/docs/), Apache Spark (https://spark.apache.org/docs/latest/), PyArrow (https://arrow.apache.org/docs/python/), Apache Parquet (https://parquet.apache.org/docs/).
- **AI Assistance Disclosure:** Google Antigravity (Gemini AI Assistant), OpenAI ChatGPT, and Anthropic Claude were used as AI-assisted tools for codebase refactoring, test-fixture authoring, drafting, critical review, debugging, and document formatting. All technical claims, timings, and empirical metrics were independently verified against repository code, saved outputs, and primary FDIC documentation. Full verification details are in [`docs/AI_USAGE.md`](docs/AI_USAGE.md) and Appendix F.
