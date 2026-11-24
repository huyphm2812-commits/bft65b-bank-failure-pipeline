# BFT65B Final Submission Checklist, Rubric Compliance & Audit Reconciliation

**Project:** A Big-Data Pipeline for Bank Failure Early Warning  
**Course:** BFT65B – Big Data Analytics in Finance  
**Institution:** National Economics University · School of Banking and Finance  
**Student:** Phạm Quang Huy (Student ID: 11230967)  
**GitHub Repository:** `https://github.com/huyphm2812-commits/bft65b-bank-failure-pipeline`  
**Audit Date:** 2026-11-24 (Final Corrected & Redesigned Submission)

---

## 1. Final Submission Deliverables & Page Budget Verification

| # | Deliverable | Format | Path | File Size | Page Count | Budget / Status |
|---|---|---:|---|---:|---:|:---:|
| 1 | Main Academic Report | PDF | `05_final_report_submission/BFT65B_Final_Academic_Report.pdf` | 756,451 B (~739 KB) | **20 pages** | **PASS (≤ 20 pages)** |
| 2 | Main Academic Report | DOCX | `05_final_report_submission/BFT65B_Final_Academic_Report.docx` | 542,593 B (~530 KB) | Synchronized | **PASS** |
| 3 | Technical Appendix | PDF | `05_final_report_submission/BFT65B_Final_Academic_Appendix.pdf` | 308,150 B (~301 KB) | **7 pages** | **PASS (≤ 10 pages)** |
| 4 | Technical Appendix | DOCX | `05_final_report_submission/BFT65B_Final_Academic_Appendix.docx` | 178,357 B (~174 KB) | Synchronized | **PASS** |
| 5 | Submission Checklist | Markdown | `05_final_report_submission/SUBMISSION_CHECKLIST.md` | — | — | **PASS** |

**Typography & Layout Summary (PDF & DOCX Synchronized via `report_data.py`):**
- Page size: **A4** (210 mm × 297 mm), margins 0.75 in L/R, 0.80 in T/B.
- Font family: **Calibri** (TrueType from Windows Fonts; 0 missing Vietnamese or mathematical glyphs; 0 `■`) + **Consolas** for verbatim query plans.
- Type scale: Cover title **25 pt**, H1 **15.5 pt**, H2 **12 pt**, Body **11 pt** (leading 13.6 pt = 1.24×), Table body **9 pt** (**8.5 pt** for 8-column Tables 1.3 & E.1; never below 8.5 pt), Table footnotes **8.5 pt**, Figure captions **9.5 pt**, Running headers/footers **8.5 pt**.

---

## 2. Summary of Factual, Numerical & Methodological Corrections Made

1. **Part 1 — Storage Benchmark Exact Provenance (`p4_storage_benchmark.json` & `p4_storage_benchmark_summary.csv`):**
   - Corrected Table 1.3 file sizes, bytes/row, read runtimes, and memory figures to match `p4_storage_benchmark.json` exactly: Raw CSV 427.33 MiB / 448.09 MB (266.29 B/row; full read 4.571 s; write 421.00 s), Gzip CSV 167.02 MiB (104.08 B/row; read 7.531 s; write 533.00 s), Single Snappy 172.43 MiB (107.45 B/row; full read 2.207 s; selected cols 0.057 s = 79.94× faster; date filter 0.236 s; write 1.88 s), Single Zstd 143.93 MiB (89.69 B/row; 2.97× compression; full read 1.836 s; write 2.12 s), Year-Partitioned Snappy 216.09 MiB (134.66 B/row; date filter 0.182 s = 25.05× faster; write 4.87 s), Year-Partitioned Zstd 179.68 MiB (111.97 B/row; write 5.01 s).
   - Corrected workstation memory headroom to `p5_memory_vs_ram.json`: 2,146.57 MB deep pandas memory on 1,597,372 eligible rows = **30.32%** of 7,079.12 MB available RAM (out of 16,088.49 MB total system RAM). Removed arbitrary universal row-count cutoff claims.

2. **Part 2 — Post-Failure Statistics Reconciliation & Reporting-Lag Wording:**
   - Audited `bank_quarter_panel.parquet`, `02_quality/post_failure_reports.csv`, `02_quality/data_quality_report.json`, and `03_panel/LABEL_EDGE_CASES.md`. All four primary pipeline artifacts agree: under the pipeline's `REPDTE ≥ FAILDATE` rule (earliest explicit `RESTYPE=FAILURE` date), there are **1,211 post-failure rows** across **270 distinct CERTs** (3 rows with `REPDTE == FAILDATE`, 1,208 with `REPDTE > FAILDATE`). By calendar quarter distance $\Delta Q$, **496 rows (40.96%)** have $\Delta Q \le 1$ (268 at $\Delta Q=0$, 228 at $\Delta Q=1$) and **715 rows (59.04%)** have $\Delta Q > 1$. By calendar days, 278 rows (22.96%) fall within 92 days and 933 (77.04%) exceed 92 days.
   - **Resolution of Discrepancy:** The earlier figures (208 CERTs; 662 within 1 quarter; 549 ≥ 2 quarters) originated only from an erroneous summary entry in `01_github_repo/docs/CLAIMS_CHECK.md` that contradicted its own cited source (`LABEL_EDGE_CASES.md`). `CLAIMS_CHECK.md` has been corrected to match the verified pipeline artifacts.
   - **Reporting-Lag Wording:** Explicitly stated that quarterly observations align by `REPDTE`, that `REPDTE + 45 days` (`financial_available_date`) is informational operational metadata (NOT an active filter or join gate), and that the 1.5–10.5 month lead time is an operational interpretation.
   - **Exit Classification Shares:** Corrected Financials CERT shares to use the exact 24,064 denominator (`merger_other` 69.66%, `active_or_no_known_exit` 17.53%, `failure` 11.81%, `assistance_nonfailure` 0.03%, `unmatched_in_catalog` 0.98%).

3. **Part 3 — Benchmark Provenance, Memory Comparability, and Spark Execution Plan:**
   - Corrected pandas core benchmark runtime from `37.820 s` to **`37.930 s`** (±0.513 s, 5 reps warm-only, `p5_engine_benchmark.json`), giving a **28.99×** Polars speedup (`1.308 s`).
   - Removed unsourced peak memory numbers (`1,412.18 MB`, `3,112.45 MB`, `3,791.34 MB`) and replaced them with the verified like-for-like metrics from `p5_engine_benchmark.json`: Process RSS endpoint (Polars **3,519.59 MB** vs. Pandas **2,115.01 MB**) and Python `tracemalloc` heap peak (Polars **0.33 MB** native-off-heap vs. Pandas **2,323.88 MB**).
   - Preserved reconciled Spark runtimes: **38.536 s** isolated core op (`p6_spark_benchmark.json`) vs. **261.960 s** full CLI stage (`logs/spark_full.txt`).
   - Replaced invented scaling slices (`10%/25%/50%`) with verified `p5_scaling_benchmark.csv` slices (`1,000`, `10,000`, `50,000`, `1,682,738` rows) and verified `p5_amdahl_scaling.csv` multi-core measurements (1/2/4 cores, empirical $p=0.0$).
   - Corrected Appendix C Polars and Spark plans to match `p5_polars_query_plan.txt` and `p6_spark_physical_plan.txt` verbatim, explicitly showing **`spark.sql.shuffle.partitions = 4`** (`Exchange hashpartitioning(CERT, 4)` and `Exchange hashpartitioning(CERT, REPDTE, 4)`), NOT 200.

4. **Part 4 — Restored Verified Preregistered Development Metrics (Variants 1–3 Only):**
   - Removed unverified Variant 4 (`0.285`) and erroneous PR-AUCs (`0.283`, `0.248`).
   - Restored exact frozen preregistered results from `bootstrap_evaluation.csv`, `bootstrap_paired_differences.csv`, and `rbcrwa_sensitivity_comparison.csv`:
     - **Variant 1 (14 features):** RF Dev PR-AUC **0.2220 [0.1531, 0.2959]**, ROC-AUC **0.9304 [0.8913, 0.9648]**; LR PR-AUC **0.1316 [0.0919, 0.1820]**.
     - **Variant 2 (13 features, no `rbcrwa_level`):** RF Dev PR-AUC **0.2239 [0.1555, 0.2967]**, ROC-AUC **0.9200 [0.8817, 0.9532]**; LR PR-AUC **0.1319 [0.0925, 0.1821]**.
     - **Variant 3 (12 features, no `rbcrwa_level` or `rbc1a_level` — SELECTED & FROZEN):** RF Dev PR-AUC **0.2436 [0.1723, 0.3221]**, ROC-AUC **0.9399 [0.9081, 0.9681]**; LR PR-AUC **0.1342 [0.0945, 0.1848]**.
   - Replaced stale Figure 5.1 with authoritative Variant 3 curve (`02_local_workspace/outputs/07_features_models/p7_development_curves.png`, PR-AUC 0.242 / ROC-AUC 0.940).

5. **Part 5 — Protection of One-Time Held-Out Test Evaluation (Option B1 Compliance):**
   - **Removed** unauthorized/deferred **test-period Capital-Ratio Rule** metrics, confusion matrix, and cost comparisons.
   - **Removed** unauthorized/deferred **test-period bootstrap confidence intervals** and paired test statistical-significance claims (`p < 0.05` / CI exclusions on test); kept exact verified point estimates from `p7_model_metrics.csv` and `p7_model_results.json`.
   - **Removed** **post-hoc test-period multi-ratio cost sensitivity** table; replaced Table 5.3 with the verified **Development-Only Cost-Ratio Sensitivity Analysis** (`1993Q1–2005Q4`, `p7_cost_threshold_comprehensive.csv`), clearly labeled as Development-Only.
   - Preserved all authorized frozen test metrics for Random Forest (V3, $\tau^* = 0.103990$: PR-AUC `0.617611`, ROC-AUC `0.974810`, Brier `0.006168`, Row Recall `88.08%`, Institution Capture `97.08%` [498/513], Expected Loss `$712,941.77k`), Logistic Regression (V3, $\tau^* = 0.601878$: PR-AUC `0.555327`, ROC-AUC `0.980273`, Brier `0.028241`, Row Recall `87.38%`, Institution Capture `97.08%` [498/513], Expected Loss `$773,022.58k`), Prior/Always-Negative baselines, Top-k% test alarm budgets, and the Spark MLlib development demo.

6. **Part 6 & Appendices B, D, E, F:**
   - Removed all 17 `[VERIFY]` placeholders and unverified Vietnamese statutory circular/bank claims; replaced Section 6.3 with a cautious, academically grounded 7-point Vietnam adaptation roadmap clearly stating that US FDIC data are used exclusively.
   - Corrected Appendix B field missingness/zero/negative counts directly from `missingness.csv`, `invalid_values.csv`, and `bank_quarter_panel.parquet`.
   - Corrected Appendix D active completion lock SHA-256 from erroneous `4eb7e31b...` to exact verified **`fa88ec1cb4640a48e4ae27f1a5d835ae8e270e031b99a985f934b11d94cdd0ed`**.
   - Corrected Appendix E training window label (`1984Q1–1991Q4`, boundary `1992-12-31`, 4Q purge), exact test calibration figures from `p7_test_predictions.parquet`, and swapped stale smoke Figure E.1 for the real 5-decade feature boxplot (`02_local_workspace/outputs/07_features_models/p7_feature_distributions.png`).
   - Corrected Appendix F, `README.md`, `AI_USAGE.md`, and `CITATIONS.md`: removed invented model names (`GPT Luna 5.6`, `Claude Sonnet 5.5`, `Gemini Flash 3.8`) and clarified that eight concrete pipeline controls—not AI or hashes alone—support leakage-resistant design.

---

## 3. Assignment Rubric Compliance Matrix (`final_report_bft65b.pdf`)

| Rubric Section | Weight | Requirements Checked | Report Evidence | Status |
|---|---:|---|---|:---:|
| **Cover & Format** | Gate | English PDF/DOCX; ≤ 20p main + ≤ 10p appendix; Page 1 metadata complete; zero placeholders | Cover Page (Page 1); 20p main PDF + 7p appendix PDF; synchronized DOCX | **PASS** |
| **Part 1 — Acquisition & Storage** | 15 pts | Resumable API ingestion; 100% completeness (170Q, 1.68M rows); Parquet vs CSV benchmark; compression & pruning justified | Section 1.1–1.3; Table 1.1, Table 1.3; Figure 1.1; Q1 callout | **CORRECTED & PASS** |
| **Part 2 — Data Quality & Panel** | 20 pts | Automated audit; justified handling; merger vs failure exits; reconciled post-failure counts; point-in-time & lag clarity | Section 2.1–2.3; Tables 2.1–2.2; Figure 2.1; Appendix B (Tables B.1–B.2, Fig B.1) | **CORRECTED & PASS** |
| **Part 3 — Scalable Processing** | 25 pts | Polars LazyFrame + pushdown; fair pandas benchmark (37.930 s); Spark match & actual 4-partition Catalyst plan; evidence-based cluster argument | Section 3.1–3.4; Tables 3.1–3.3; Appendix C (Listings C.1–C.2) | **CORRECTED & PASS** |
| **Part 4 — Feature Pipeline** | 15 pts | CAMELS motivation; 12 frozen V3 features; NETINC exclusion; train-only Pipeline; verified V1–V3 bootstrap selection | Section 4.1–4.2; Tables 4.1–4.2; Appendix A & E | **CORRECTED & PASS** |
| **Part 5 — Modelling & Validation** | 15 pts | Time-ordered split + purge + 8Q embargo; imbalanced metrics; data-based DEF_A threshold; single locked test; MLlib dev demo; deferred test items removed | Section 5.1–5.3; Tables 5.1A, 5.1B, 5.2, 5.3 (dev-only); Figure 5.1 | **CORRECTED & PASS** |
| **Part 6 — Conclusions** | 5 pts | Explicit Q1–Q3 answers; limitations; cautious Vietnam adaptation with zero `[VERIFY]` tags | Section 6.1–6.3; Q3 callout; References | **CORRECTED & PASS** |
| **Reproducibility & Integrity** | 5 pts | Public GitHub repo, README, pinned requirements, code/tests, no data files committed, source & AI disclosures | `01_github_repo/` prepared & synced; Appendix D & F; **User must push repo publicly before deadline** | **USER ACTION REQUIRED** |

---

## 4. Technical Integrity Verification (Zero Analytical Modification)

All 109 protected files (`01_github_repo/src/`, `01_github_repo/scripts/`, `01_github_repo/tests/`, all three copies of `frozen_model_config.json`, `p7_test_predictions.parquet`, `p7_model_metrics.csv`, `p7_model_results.json`, active `p7_test_evaluation.lock`, and archived `p7_test_evaluation.lock.guard_archive`) were hashed before editing and re-verified after editing: **109/109 SHA-256 digests match identically (0 modifications)**.

---

## 5. Remaining User Actions Required Before Submission

1. **Publish Public GitHub Repository (`USER ACTION REQUIRED`):** Git is not installed in the local shell environment, so the repository could not be pushed automatically. Create the public repository `bft65b-bank-failure-pipeline` under `https://github.com/huyphm2812-commits`, push `BFT65B_FINAL/01_github_repo/` (confirming no raw/processed Parquet or CSV data files are committed), and verify the link is publicly accessible from the submission deadline until grade release.
2. **Final Personal Review:** Open `BFT65B_Final_Academic_Report.pdf` and `BFT65B_Final_Academic_Appendix.pdf` locally to confirm satisfaction before uploading to the NEU submission portal.
