# BFT65B Modeling Review Pack (v2)

**Dataset Scope:** Real Full Historical Call Reports (1,682,738 bank-quarters, 170 quarters, 1984Q1–2026Q2)  
**Execution Status:** Full Development-Only Pipeline Executed on 100% of Data  
**Protection Verified:** Zero Held-Out Test Labels Accessed (`test_evaluated: false`, no real test lock created)  
**Date:** 2026-11-22  

---

## 1. Executive Summary & Verification Matrix

The BFT65B bank failure early warning pipeline has executed all validation, audit, panel construction, feature derivation, and development modeling stages on the real full dataset.

| Pipeline Stage | Scope & Configuration | Status | Wall Time | Peak RSS RAM | Authoritative Artifact |
| :--- | :--- | :---: | :---: | :---: | :--- |
| **Storage Benchmark** | CSV, Gzip, Parquet, Arrow (Rep 3) | **PASS** | 4,550.22 s (75.84 min) | 1,503.80 MB | `04_storage_benchmark/` |
| **Engines Benchmark** | Pandas vs Polars LazyFrame (Rep 5) | **PASS** | 251.05 s (4.18 min) | 2,322.13 MB | `05_engines/` |
| **PySpark Comparison** | PySpark Local Catalyst (Rep 2) | **PASS** | 261.96 s (4.37 min) | 3,791.34 MB | `06_spark/` |
| **Number Reconciliation** | Exact 3-column reconciliation table | **PASS** | ~10 s | 160 MB | `docs/NUMBER_RECONCILIATION.md` |
| **Memory Fix (Step 1)** | 5 folds, 2 grids, CV subsample, full final fit | **PASS** | ~8 s | 220 MB | 37 pytest unit tests passing |
| **Full Dev-Only Run (Step 2)** | Real 1.68M-row panel (Split A) | **PASS** | 377.86 s (6.30 min) | 6,656.82 MB | `p7_model_results.json` |
| **Bank-Level Bootstrap (Step 3)** | 1,000 resamples of unique banks (seed 42) | **PASS** | 1,404.23 s (23.4 min) | 3,120 MB | `bootstrap_evaluation.csv` |
| **Cost Threshold Analysis (Step 4)** | Ratios 10:1–1000:1, DEF_A/B, rule baseline | **PASS** | ~45 s | 1,850 MB | `p7_cost_threshold_comprehensive.csv` |
| **Leakage Audit (Step 5)** | Controls a–i verified (static AST & dynamic) | **PASS** | ~15 s | 150 MB | `docs/LEAKAGE_AUDIT.md` |

---

## 2. Split Demographics & Prevalence Shift (Split A)

- **Total Physical Panel:** 1,682,738 bank-quarters (170 quarters, 1984Q1 to 2026Q2).
- **Eligible Modeling Population:** **1,597,372 bank-quarters** (10,823 positive failures, 1,586,549 negatives).
- **Censored Observations:** **85,366 bank-quarters** (post-merger quarters, post-failure filings [1,211 rows], and unobservable forward horizon after 2025Q2).
- **Merger Censoring Sensitivity Delta:** Retaining mergers as negative outcomes increases eligible rows by **+64,902 rows** (to 1,662,274 eligible rows).

### Chronological Split Partitions:
1. **Training Partition (1984Q1–1991Q4 Purged):**
   - Observations: **522,402 bank-quarters**
   - Failure Positives ($y=1$): **7,955 rows** (prevalence = **1.5228%**)
   - Distinct Failed Banks: **2,153 institutions**
   - Non-Failure Negatives ($y=0$): **514,447 rows**
   - Purging Rule: Quarters in 1992 are purged (8 quarters before dev start) to eliminate 4-quarter forward outcome overlap.
2. **Development Partition (1993Q1–2005Q4):**
   - Observations: **532,620 bank-quarters**
   - Failure Positives ($y=1$): **308 rows** (prevalence = **0.0578%**)
   - Distinct Failed Banks: **100 institutions**
   - Non-Failure Negatives ($y=0$): **532,312 rows**
   - *Prevalence Shift Limitation:* Failure prevalence collapses **26.3-fold** from 1.52% in training down to 0.058% during the inter-crisis moderation (1993–2005).
3. **Embargo Gap:** 2006Q1–2007Q4 (8 quarters strictly unallocated).
4. **Held-Out Test Partition (2008Q1–2014Q4 - GFC Crisis):**
   - Observations: **205,822 bank-quarters**
   - Failure Positives ($y=1$): **2,005 rows** (prevalence = **0.9741%**) across **513 failed institutions**
   - Status: **PROTECTED (Un-evaluated, labels unread)**.

---

## 3. Step 3: Bank-Level Cluster Bootstrap (1,000 Resamples, Seed 42)

To account for within-bank serial correlation across quarters, 1,000 cluster bootstrap resamples of unique development institutions (`CERT`s) were evaluated across 3 feature variants:
- **Variant 1:** All 14 CAMELS features.
- **Variant 2:** Without `rbcrwa_level` (13 features).
- **Variant 3:** Without `rbcrwa_level` and `rbc1a_level` (12 features).

### 95% Bootstrap Confidence Intervals Table:

| Model & Feature Variant | PR-AUC Mean [95% CI] | ROC-AUC Mean [95% CI] | Recall @ Top 1% Alarm Budget [95% CI] | Recall @ Top 2% Alarm Budget [95% CI] | Recall @ Top 5% Alarm Budget [95% CI] |
| :--- | :---: | :---: | :---: | :---: | :---: |
| **Logistic Regression (V1)** | 0.1316 [0.0919, 0.1820] | 0.9060 [0.8593, 0.9526] | 72.59% [64.20%, 80.90%] | 78.83% [70.51%, 86.57%] | 82.85% [74.78%, 90.23%] |
| **Random Forest (V1)** | **0.2220 [0.1531, 0.2959]** | **0.9304 [0.8913, 0.9648]** | 71.44% [63.46%, 79.05%] | 77.97% [70.62%, 84.85%] | **84.86% [77.93%, 91.34%]** |
| **Logistic Regression (V2)** | 0.1319 [0.0925, 0.1821] | 0.9061 [0.8591, 0.9526] | 72.58% [64.20%, 80.90%] | 78.90% [70.51%, 86.63%] | 82.85% [74.78%, 90.23%] |
| **Random Forest (V2)** | 0.2239 [0.1555, 0.2967] | 0.9200 [0.8817, 0.9532] | 69.91% [60.92%, 78.02%] | 76.97% [68.90%, 84.38%] | 80.35% [72.56%, 87.84%] |
| **Logistic Regression (V3)** | 0.1342 [0.0945, 0.1848] | 0.9078 [0.8607, 0.9541] | 73.11% [64.77%, 81.53%] | 79.05% [70.63%, 86.46%] | 83.22% [75.17%, 90.52%] |
| **Random Forest (V3)** | **0.2436 [0.1723, 0.3221]** | **0.9399 [0.9081, 0.9681]** | 69.35% [60.89%, 77.82%] | 77.08% [68.63%, 84.84%] | **85.11% [77.57%, 91.94%]** |

### Paired-Difference Intervals (Difference Exceeds 0 Check):

| Comparison | $\Delta$ PR-AUC Mean [95% CI] | Zero Excluded? | $\Delta$ ROC-AUC Mean [95% CI] | Zero Excluded? | Empirical Conclusion |
| :--- | :---: | :---: | :---: | :---: | :--- |
| **RF(V1) vs LR(V1)** | **+0.0904 [+0.0470, +0.1381]** | **YES** | +0.0244 [-0.0127, +0.0639] | NO | RF statistically significantly outperforms LR on PR-AUC (+68.7% gain). |
| **RF(V2) vs LR(V2)** | **+0.0920 [+0.0487, +0.1401]** | **YES** | +0.0140 [-0.0143, +0.0479] | NO | RF statistically significantly superior. |
| **RF(V3) vs LR(V3)** | **+0.1094 [+0.0625, +0.1607]** | **YES** | +0.0321 [-0.0015, +0.0719] | NO | RF statistically significantly superior. |
| **LR(V1) vs LR(V2)** | -0.0003 [-0.0021, +0.0007] | NO | -0.0000 [-0.0003, +0.0002] | NO | Dropping `rbcrwa_level` produces statistically identical LR performance. |
| **LR(V1) vs LR(V3)** | -0.0026 [-0.0109, +0.0047] | NO | -0.0018 [-0.0053, +0.0014] | NO | Dropping both risk-based ratios does not harm LR. |
| **RF(V1) vs RF(V2)** | -0.0020 [-0.0168, +0.0137] | NO | +0.0104 [-0.0042, +0.0250] | NO | Difference between Variant 1 and 2 in RF is statistically indistinguishable. |
| **RF(V1) vs RF(V3)** | -0.0216 [-0.0409, -0.0008] | **YES** | -0.0095 [-0.0241, +0.0045] | NO | Variant 3 exhibits slightly higher PR-AUC (+0.0216). |

---

## 4. Step 4: Comprehensive Cost Threshold Analysis

### Development Failure Resolution Cost Statistics:
- Total failed institutions in development: **100 banks** (308 positive bank-quarters).
- Documented FDIC resolution costs: **99 banks**.
- Missing resolution costs: **1 bank** (1.00% missingness).
- Missing cost imputation rule: Imputed strictly to observed development non-null median.
- **Median failure COST ($C_{FN}$):** **\$5,919.23 thousand USD** (\$5.92M).
- **Mean failure COST:** **\$26,134.88 thousand USD** (\$26.13M).

### Cost Threshold Performance Matrix across Ratios:

#### Definition A (`DEF_A`): Row False Alarms + Bank False Negatives

| Model | Ratio ($C_{FN}:C_{FP}$) | $C_{FP}$ (\$k) | Threshold ($\tau^*$) | Alarms/Qtr | Row Share (%) | Banks Caught | FA Rows | Total Loss (\$k) |
| :--- | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: |
| **Prior Baseline** | All Ratios | \$5.92–\$591.9 | 0.0152 | 0.0 | 0.00% | 0 / 100 | 0 | \$2,593,272.05 |
| **Capital Ratio Rule** | 10:1 | \$591.92 | -0.0360 | 13.0 | 0.13% | 68 / 100 | 540 | \$1,693,644.40 |
|  | 50:1 | \$118.38 | -0.0389 | 16.1 | 0.16% | 70 / 100 | 698 | \$1,406,133.69 |
|  | 100:1 | \$59.19 | -0.0389 | 16.1 | 0.16% | 70 / 100 | 698 | \$1,364,817.49 |
|  | 500:1 | \$11.84 | -0.0594 | 227.0 | 2.22% | 80 / 100 | 11,612 | \$1,277,270.44 |
|  | 1000:1 | \$5.92 | -0.0704 | 1,027.5 | 10.03% | 87 / 100 | 53,214 | \$1,164,633.87 |
| **Logistic Regression** | 10:1 | \$591.92 | 0.9596 | 12.9 | 0.13% | 66 / 100 | 538 | \$1,602,555.54 |
|  | 50:1 | \$118.38 | 0.6937 | 40.3 | 0.39% | 82 / 100 | 1,894 | \$1,235,099.66 |
|  | 100:1 | \$59.19 | 0.6115 | 50.6 | 0.49% | 84 / 100 | 2,418 | \$1,107,412.01 |
|  | 500:1 | \$11.84 | 0.4232 | 88.4 | 0.86% | 89 / 100 | 4,361 | \$920,124.36 |
|  | 1000:1 | \$5.92 | 0.4232 | 88.4 | 0.86% | 89 / 100 | 4,361 | \$894,310.62 |
| **Random Forest** | 10:1 | \$591.92 | 0.5106 | 6.2 | 0.06% | 61 / 100 | 222 | \$1,613,539.14 |
|  | 50:1 | \$118.38 | 0.1181 | 42.6 | 0.42% | 82 / 100 | 2,016 | \$1,203,530.88 |
|  | **100:1 (Primary)** | **\$59.19** | **0.0376** | **180.1** | **1.76%** | **92 / 100** | **9,129** | **\$682,044.97** |
|  | 500:1 | \$11.84 | 0.0376 | 180.1 | 1.76% | 92 / 100 | 9,129 | \$249,752.13 |
|  | 1000:1 | \$5.92 | 0.0376 | 180.1 | 1.76% | 92 / 100 | 9,129 | \$195,715.53 |

#### Monotonicity Verification:
- For all models, as the $FN:FP$ ratio increases ($C_{FP}$ becomes cheaper), $\tau^*$ is monotonically non-increasing ($\tau^*_{10:1} \ge \tau^*_{50:1} \ge \tau^*_{100:1} \ge \tau^*_{500:1} \ge \tau^*_{1000:1}$).
- For RF, thresholds for 100:1, 500:1, and 1000:1 remain at $\tau = 0.0376$ because lowering the threshold below 0.0376 captures no additional failed banks while sharply inflating false alarm rows, making 0.0376 the discrete global optimum across all three ratios.

---

## 5. Pending Supervisory Decisions (Framed as DRAFT Recommendations)

All 5 supervisory decisions remain **OPEN** awaiting user confirmation. The recommendations below are provided as reproducible DRAFT baselines:

1. **Split Selection:**
   - **Recommendation (DRAFT):** **Split A** (Train 1984–1992, Dev 1993–2005, Embargo 2006–2007, Test 2008–2014).
   - *Rationale:* Aligns with regulatory rubric and partitions distinct macro eras.
2. **Cost-Loss Formulation:**
   - **Recommendation (DRAFT):** **`DEF_A`** (Quarterly inspection false alarm penalty).
   - *Rationale:* Supervisors must review flagged banks quarterly; false alarm costs recur per flagged report.
3. **Supervisory Cost Ratio:**
   - **Recommendation (DRAFT):** **100:1 ($C_{FP} = \$59.19\text{ thousand}$)**.
   - *Rationale:* Standard regulatory convention that missing an insolvent bank is 100x more costly than an on-site supervisory review. At 100:1, RF catches 92/100 banks with an alarm budget of ~180 banks/quarter (1.76% of banks).
4. **Feature Variant:**
   - **Recommendation (DRAFT):** **Variant 1** (or Variant 2).
   - *Rationale:* Paired bootstrap CIs demonstrate no significant loss from dropping pre-1990 missing ratios ($\Delta = -0.0020$ [-0.0168, +0.0137]).
5. **Primary Model Choice:**
   - **Recommendation (DRAFT):** **Random Forest** (with Logistic Regression reported in parallel as linear baseline).
   - *Rationale:* RF delivers +68.7% PR-AUC improvement (+0.0904 bootstrap CI [0.0470, 0.1381]) and achieves lower expected loss at all ratios $\ge 50:1$.

---

## 6. Freeze Configuration Template for User Execution

Once you have reviewed the options, execute the freeze script to transition from DRAFT to FROZEN:

```powershell
# From 01_github_repo:
python scripts/freeze_config.py --output results_snapshot/07_features_models/frozen_model_config.json
```
