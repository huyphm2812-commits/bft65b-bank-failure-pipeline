# BFT65B Final Report & Submission Package — Audit & Edit Changelog

**Course:** BFT65B – Big Data Analytics in Finance  
**Institution:** National Economics University · School of Banking and Finance  
**Project:** A Big-Data Pipeline for Bank Failure Early Warning  
**Audit & Editing Completion Date:** 2026-11-23  

---

## 1. Executive Summary of Changes

All requested corrections, reconciliations, and academic enhancements have been implemented across the project submission deliverables. No models were refitted, no thresholds or configurations were altered, and no test evaluation was rerun. All empirical figures in the report and appendix derive strictly from verified pipeline artifacts, and every number is audited and cited in `docs/REPORT_NUMBER_AUDIT.csv`.

---

## 2. Comprehensive Section-by-Section Changelog

### A. Front Matter & Page 1 Metadata
- **Placeholder Cleanup:** Replaced draft placeholder strings with clean uppercase tags (`Phạm Quang Huy`, `11230967`, `https://github.com/huyphm2812-commits/bft65b-bank-failure-pipeline`). Deleted all occurrences of the phrase `"(Placeholders retained for final submission)"`.
- **Data Ingestion Method:** Standardized ingestion method specification to `"FDIC BankFind Suite REST API (https://api.fdic.gov/banks)"` (API only), removing `"& Bulk Data Verification"`.
- **Download Date:** Documented official download and census verification date as `"2026-11-20 (Validated across 170 quarterly census files)"`.
- **Dataset Scope Tables:** Verified and presented physical dimensions: Financials panel (1,682,738 rows, 170 quarters, 24,064 distinct filing CERTs), Failures table (4,117 rows), and Master Catalog (27,835 institutions).

### B. Typography & Font Verification (Zero Missing Glyphs)
- **TrueType DejaVu Sans Integration:** Configured ReportLab in `build_pdf.py` to register and utilize TrueType font files from Python's matplotlib package:
  - `DejaVuSans.ttf` (Regular)
  - `DejaVuSans-Bold.ttf` (Bold)
  - `DejaVuSans-Oblique.ttf` (Italic)
  - `DejaVuSans-BoldOblique.ttf` (Bold Italic)
- **Registered Font Family:** Bound `DejaVuSans` as a unified font family so that `<b>`, `<i>`, and `<b><i>` tags render with proper font weights.
- **Glyph Verification:** Ran programmatic PDF text extraction across all pages of both `BFT65B_Final_Academic_Report.pdf` and `BFT65B_Final_Academic_Appendix.pdf`. Confirmed **zero occurrences of black replacement squares ("■") or `\ufffd`**, verifying complete, pristine rendering of Vietnamese diacritics (e.g., *"Ngân hàng Nhà nước Việt Nam"*, *"Cơ quan Thanh tra, Giám sát Ngân hàng"*, *"Bảo hiểm Tiền gửi Việt Nam"*).

### C. Part 1 — Data Acquisition & Storage Layout
- **Resumable Acquisition Architecture:** Added technical details describing HTTP request streaming with `Accept-Encoding: gzip` (reducing wire payload by ~75%), deterministic 10,000-record cursor pagination, verification against API manifest `meta.total` counts before disk serialization, chunk-level SHA-256 integrity validation, and exponential backoff with randomized jitter on HTTP 429/500/503.
- **Schema Justification:** Documented Apache Arrow physical types: `int32` for `CERT` (halving storage over 64-bit defaults while safely handling all IDs up to 59,000), `timestamp[ms]` for `REPDTE` (enforcing strict calendar quarter arithmetic), `float64` for dollar amounts ($k USD) and regulatory percentage points, and `int8`/`bool` for binary indicators.
- **Like-for-Like Storage Benchmark & Write Performance:**
  - Expanded Table 1.3 to include measured write times: CSV 421.00 s (448.09 MB), Gzip CSV 532.00 s (92.40 MB), Single Snappy 1.88 s (96.12 MB), Single Zstd 2.12 s (76.45 MB), Year-Partitioned Snappy 4.87 s (226.59 MB), Year-Partitioned Zstd 5.01 s (188.41 MB).
  - Highlighted that Parquet writes execute 224x faster than CSV, and column projection achieves an 80.19x read speedup (0.057 s vs 4.571 s) while reducing peak Python heap memory by 46.5x (18.23 MB vs 847.72 MB).
- **Implemented vs. Recommended Storage Distinction:**
  - Explicitly stated that **Year-Partitioned Snappy (226.59 MB)** is the *implemented* layout (optimizing multi-engine directory partition pruning for 2008–2014 in 0.165 s).
  - Explicitly stated that **Single Zstandard / Zstd (76.45 MB, 5.86x compression)** or **Year-Partitioned Zstandard (188.41 MB)** is the *analytically recommended* layout for single-node cold storage.
- **Caption & Availability Terminology:**
  - Changed Figure 1.1 caption from `"(Cold Cache, Repetitions = 3)"` to `"(Warm-Only Cache, Repetitions = 3)"`.
  - Replaced `"statutory 45-day lag"` with `"assumed 45-day availability buffer"` throughout the report narrative, clarifying that statutory filing windows are 30–35 days and 45 days is an operational ingestion/audit buffer.

### D. Part 2 — Data Quality Audit & Panel Construction
- **Problem → Evidence → Handling → Rationale Table:** Inserted Table 2.1 (`PROBLEM_EVIDENCE_HANDLING_TABLE`) detailing the ten audited data quality anomalies, empirical evidence, handling protocols, and econometric rationales from `problem_evidence_handling.csv`.
- **Exit Classification Disambiguation:**
  - Updated Table 2.2 to enumerate exit classifications across both the **Master Catalog (27,835 institutions)** and the **Financials Panel (24,064 institutions)**:
    - Voluntary mergers / absorptions: 20,569 (73.89%) in catalog; 16,763 (70.35%) in financials.
    - Active institutions through 2026Q2: 4,223 (15.17%) in catalog; 4,218 (17.70%) in financials.
    - Regulatory closures / failures: 3,036 (10.91%) in catalog; 2,841 (11.92%) in financials.
    - Assistance non-failures: 7 (0.03%) in both catalog and financials.
    - Depository entities in financials not tracked in modern active catalog: 235 (0.98%).
  - Removed the claim that alternative merger policies leave *"rankings unchanged"*; clarified that treating unassisted mergers as non-distressed exits is a conservative choice that defines failure strictly as regulatory closure causing loss to the FDIC Deposit Insurance Fund.
- **Post-Failure Reporting Reconciliation:**
  - Reconciled post-failure reporting counts: **270 distinct CERTs** filed **1,211 Call Reports** after official failure dates (reconciling an earlier draft count of 208 CERTs).
  - Documented timing breakdown: 662 reports (54.67%) were submitted within 1 quarter post-failure (administrative wind-down), while 549 reports (45.33%) were submitted ≥ 2 quarters post-failure (driven by FDIC bridge bank receivership operations). All post-failure quarters are strictly censored from the modeling panel.
- **Training Failure Count Reconciliation:**
  - Reconciled training failure count: exactly **2,153 distinct failed institutions** were active during the 1984Q1–1991Q4 training partition (accounting for 7,955 positive failure-warning bank-quarters out of 522,402 active quarters). Reconciled against an earlier draft subset count of 2,059.
- **Figure 2.1 (Nulls & Exact Zeros):**
  - Regenerated Figure 2.1 (`financial_field_availability.png`) plotting both null percentages and exact zero percentages for `RBC1AAJ`, `RBCRWAJ`, and `P3ASSET` across 1984–2026 with clear, high-contrast labels, regulatory milestone annotations (1990 Basel I, 2001 P3ASSET mandate, 2020 CBLR opt-out), and professional axes.

### E. Part 3 — Scalable Processing with Polars & PySpark
- **Polars Scaling Subsections & Tables:**
  - Inserted Table 3.2 (`SCALING_BENCHMARK_TABLE`) documenting linear $\mathcal{O}(N)$ scaling across panel slices: 10% (168k rows) in 0.12 s (1.40M rows/s) to 100% (1.68M rows) in 1.31 s (1.28M rows/s).
  - Inserted Table 3.3 (`AMDAHL_SCALING_TABLE`) documenting multi-core speedups from 1 core (3.65 s) to 8 cores (0.88 s, 4.15x speedup), establishing an empirical parallel fraction $p = 0.84$ ($84\%$ parallelizable) and serial fraction $s = 0.16$.
  - Analyzed memory footprint: in-memory Arrow representation consumes ~135 bytes/row (~227 MB in RAM for 1.68M rows), providing comfortable RAM headroom to process up to ~5.27 million rows entirely in-memory on a standard 16 GB workstation (12 GB available RAM).
- **Cluster Justification Interpretation:**
  - Contrast single-node Polars (1.31 s) against PySpark (38.54 s isolated Catalyst execution; 261.96 s full CLI pipeline). Demonstrated that for tabular financial panels under ~10–20M rows (~5 GB), single-node vectorized engines eliminate IPC, JVM, serialization, and network shuffle overheads, rendering distributed clusters computationally and economically unjustified.
- **Spark Physical Plan & Shuffle Analysis:**
  - Detailed physical plan execution: label join executes via `BroadcastHashJoin` (failures table is compact, ~4k rows, broadcast to all executors without network shuffle); longitudinal Window lagging operations require an `Exchange hashpartitioning(CERT)` shuffle to co-locate institutional history; and quarterly aggregations require an `Exchange hashpartitioning(REPDTE)` shuffle.
  - Documented equivalence verification: 170 quarterly summaries match 100% between Spark and Polars; row-level equivalence was audited on a deterministic 10,000-row sample (`row_equivalence_sample_size: 10000`), confirming zero numerical divergence within floating-point tolerance ($10^{-5}$).

### F. Part 4 — Feature Pipeline & Leakage Prevention
- **Scikit-Learn Pipeline Architecture:** Detailed implementation of `Pipeline` integrating `SimpleImputer(strategy='median', add_indicator=True)` and `StandardScaler` fitted strictly on pre-1993 training data.
- **ORE Missing Indicator Handling:** Clarified that ORE is imputed using train-only median with `add_indicator=True`, preserving informative structural absence without hard-coded zero fills.
- **`NETINC` Exclusion Rationale:** Explained that Call Reports record `NETINC` as cumulative year-to-date (YTD) flow resetting every Q1. Using raw `NETINC` introduces artificial intra-year seasonality (Q4 appears 4x larger than Q1). Annualized ratios (`ROA`, `ROE`, `NIMY`) and rolling 4Q average `roa_rolling_4q_mean` provide economically sound alternatives.
- **Loss of Basel Ratios:** Documented that `RBCRWAJ` has 100% exact zeros from 1984 to 1989 because Basel I was uncollected. Excluding it from Variant 3 preserves complete temporal validity across all 42 years.
- **Development Bootstrap Table:** Inserted Table 4.2 documenting 1,000 cluster-bootstrap evaluations across Variants 1, 2, 3, and 4, confirming Variant 3's superior stability and discrimination (PR-AUC 0.283 [0.241, 0.327]).

### G. Part 5 — Modeling, Validation & Economic Cost Evaluation
- **Purged Validation Scheme:** Documented purged expanding-window cross-validation with an 8-quarter embargo gap (2006Q1–2007Q4) eliminating forward label overlap.
- **Cost Basis Reconciliation (Actual per-bank COST vs Flat Median):**
  - Clarified that False Negative loss ($L_{FN}$) is computed using the **actual per-bank FDIC resolution loss** (`COST`) from `failures.parquet` ($L_{FN} = \sum_{\text{missed banks}} \text{COST}_i$, with 1 missing bank imputed at development median $5,919.225k USD).
  - Clarified that False Positive cost ($C_{FP}$) is assumed at 1% of the development median resolution cost ($1\% \times \$5,919.225\text{k} = \$59.19225\text{k}$ per on-site review).
  - Updated Table 5.1 with authoritative figures:
    - **Naive Dummy Baseline:** Misses all 513 banks $\rightarrow$ Total Loss = **$70,453,896.63k USD (~$70.45B USD)**.
    - **Capital-Ratio Rule Baseline (`capital_ratio < 3.8876%`):** Misses 62 banks (1,071 FP rows) $\rightarrow$ Total Loss = **$8,547,959.74k USD**.
    - **Random Forest (threshold 0.103990):** Misses 15 banks (5,821 FP rows) $\rightarrow$ Total Loss = **$712,941.77k USD**.
    - **Logistic Regression (threshold 0.601878):** Misses 15 banks (5,377 FP rows) $\rightarrow$ Total Loss = **$773,022.58k USD**.
    - **Reconciliation Note:** Both RF and LR catch 498/513 banks (97.08% catch rate). RF delivers **$60,080.81k USD (~$60.08M USD)** lower loss than LR because the specific 15 institutions missed by LR had higher resolution costs ($454.75M vs $368.38M), outweighing RF's slightly higher examination costs ($344.56M vs $318.28M). Replaced the previously cited "$2.32B savings" (which was an apples-to-oranges comparison against a flat median-cost naive baseline).
- **Cluster Bootstrap 95% Confidence Intervals:**
  - PR-AUC: RF = 0.617611 [0.582328, 0.651022] vs LR = 0.555327 [0.509302, 0.601971], Paired Diff: **+0.062284 [0.028125, 0.101269]** ($p < 0.05$).
  - ROC-AUC: RF = 0.974810 [0.968028, 0.980952] vs LR = 0.980273 [0.975060, 0.984725], Paired Diff: **-0.005463 [-0.009895, -0.001279]**.
- **Post-Hoc Multi-Ratio Sensitivity Table:** Inserted Table 5.3 evaluating ratios 10:1, 50:1, 100:1 (frozen), 500:1, and 1000:1 on the held-out test partition.
- **Figure 5.1 Caption & Curves:** Verified caption `Figure 5.1: Development Precision-Recall and ROC Curves across Feature Variants (Dev Period: 1993Q1–2005Q4)` and clarified that the pipeline generates `p7_development_curves.png` based on development data.

### H. Part 6 — Conclusions, Limitations & Vietnam Adaptation
- **Expanded Limitations:** Macroeconomic shocks (GFC zero lower bound), regulatory forbearance altering closure timing, unassisted merger censoring, assumed 45-day reporting availability buffer, lack of granular borrower-level data.
- **Hedged Question 3 Assessment:** Emphasized that machine learning functions as an automated, cost-sensitive triage filter to prioritize supervisory inspections, but cannot replace on-site audits of underwriting, management governance, IT risk, or statutory legal enforcement.
- **Vietnam Adaptation Roadmap:**
  - Aligned with State Bank of Vietnam (SBV / Ngân hàng Nhà nước Việt Nam), Banking Supervision Agency (Cơ quan Thanh tra, Giám sát Ngân hàng - BSA), and Deposit Insurance of Vietnam (DIV / Bảo hiểm Tiền gửi Việt Nam - BHTGVN).
  - Added explicit `TODO(verify)` tags to all Vietnamese regulatory circulars and statutes awaiting user confirmation:
    - Circular 35/2015/TT-NHNN & Circular 11/2021/TT-NHNN (statistical reporting regimes) `TODO(verify)`.
    - Circular 41/2016/TT-NHNN (Basel II CAR) `TODO(verify)`.
    - Circular 22/2019/TT-NHNN (prudential limits & liquidity ratios) `TODO(verify)`.
    - Circular 11/2021/TT-NHNN (NPL classification & provisioning) `TODO(verify)`.
    - Law on Credit Institutions No. 32/2024/QH15 (effective July 1, 2024) `TODO(verify)`.
  - Data infrastructure adaptations: Redefining target label to regulatory special control / mandatory restructuring; small-sample transfer learning.

### I. Supplementary Technical Appendix
- **Appendix A:** Extended data dictionary with explicit `timestamp[ms]` types and verbatim definition of `NETINC`.
- **Appendix B:** Comprehensive data quality audit tables (rebuilt Table B.1 from `missingness.csv` and `field_availability.csv`).
- **Appendix C:** Verbatim Polars physical execution plan (Listing C.1) and PySpark physical execution plan excerpt.
- **Appendix D:** Cryptographic code SHA-256 hashes manifest and honest, transparent explanation of the archived legacy guard lock (`02_local_workspace/guard_archive/`).
- **Appendix E:** Model probability calibration table across Train, Dev, and Test partitions (`CALIBRATION_TABLE`).
- **Appendix F:** Complete AI assistance disclosure log (`AI_USAGE_TABLE`).

### J. Submission Checklist & Rubric Compliance
- Updated `SUBMISSION_CHECKLIST.md` to remove all self-grading adjectives (`[STATUS: EXCELLENT]`), replacing them with objective verification tags (`[STATUS: VERIFIED & COMPLETE]`).
- Updated measured page counts: Main Academic Report = **10 Pages** (Limit: ≤ 20 Pages); Supplementary Technical Appendix = **2 Pages** (Limit: ≤ 10 Pages).

---

## 3. Verified Deliverables & Artifact Hashes

| Deliverable Name | File Path | Format | Measured Size | Page Count | Status |
|---|---|:---:|:---:|:---:|:---:|
| Main Academic Report | `05_final_report_submission/BFT65B_Final_Academic_Report.docx` | DOCX | ~305 KB | N/A | **READY** |
| Main Academic Report | `05_final_report_submission/BFT65B_Final_Academic_Report.pdf` | PDF | ~352 KB | **10 Pages** | **PASS (≤ 20 Pages)** |
| Technical Appendix | `05_final_report_submission/BFT65B_Final_Academic_Appendix.docx` | DOCX | ~46 KB | N/A | **READY** |
| Technical Appendix | `05_final_report_submission/BFT65B_Final_Academic_Appendix.pdf` | PDF | ~16 KB | **2 Pages** | **PASS (≤ 10 Pages)** |
| Submission Checklist | `05_final_report_submission/SUBMISSION_CHECKLIST.md` | Markdown | ~13 KB | N/A | **READY** |
| Number Audit Manifest | `01_github_repo/docs/REPORT_NUMBER_AUDIT.csv` | CSV | ~18 KB | 150 rows | **AUDITED** |
| Number Audit Manifest | `05_final_report_submission/REPORT_NUMBER_AUDIT.csv` | CSV | ~18 KB | 150 rows | **AUDITED** |
| Audit Changelog | `REPORT_CHANGELOG.md` | Markdown | ~15 KB | N/A | **COMPLETE** |

---

## 4. Questions & Action Items for the User

Before final submission to the National Economics University grading portal or instructor, please confirm or provide the following 5 items:

1. **Candidate Full Name (`Phạm Quang Huy`):**  
   Please provide your full legal name as registered with NEU (e.g., *"Nguyễn Quang Huy"* or your official student record name). Once provided, it will replace `Phạm Quang Huy` on Page 1 of the report and appendix.

2. **Candidate Student ID (`11230967`):**  
   Please provide your official NEU Student ID number (e.g., *"1121xxxx"*).

3. **Public GitHub Repository URL (`https://github.com/huyphm2812-commits/bft65b-bank-failure-pipeline`):**  
   Please provide the exact public GitHub repository URL hosting your code submission (e.g., `https://github.com/your-username/bft65b-bank-failure-pipeline`). Ensure the repository is set to **Public** from the submission deadline until final grades are released.

4. **AI Assistance Table Confirmation (Appendix F):**  
   Review Appendix Table F.1 in the generated report/appendix. Confirm whether you want to add, edit, or adjust any specific AI tools, model versions, or prompt scopes utilized during your research.

5. **Vietnamese Banking Regulations & Statutes (`TODO(verify)`):**  
   The report cites several specific State Bank of Vietnam circulars and statutory laws in Section 6.3 and References. Please confirm whether you want to retain or update these exact circular citations:
   - *Circular 35/2015/TT-NHNN* & *Circular 11/2021/TT-NHNN* (Statistical reporting regime)
   - *Circular 41/2016/TT-NHNN* (Basel II Capital Adequacy Ratio - CAR minimum 8%)
   - *Circular 22/2019/TT-NHNN* (Prudential operational limits and liquidity ratios)
   - *Circular 11/2021/TT-NHNN* (Asset classification and credit risk provisioning)
   - *Law on Credit Institutions No. 32/2024/QH15* (Effective July 1, 2024, early intervention framework)
