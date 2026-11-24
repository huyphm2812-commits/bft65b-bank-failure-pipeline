# BFT65B Bank Failure Prediction: Temporal Split Decision Pack

**Status:** OPEN (Pending User Confirmation)  
**Generated From:** Real Full FDIC Dataset (1,682,738 Financials rows, 4,117 Failures, 27,835 Institutions, 170 quarters)  
**Artifact Location:** `02_local_workspace/outputs/03_panel/SPLIT_DECISION_PACK.md`  

---

## 1. Failure Events per Calendar Year (Event Origin)

Source: FDIC Failures table (`RESTYPE = 'FAILURE'`), distinct insured institutions (`CERT`) failed per calendar year of `FAILDATE` (1980–2026).

| Calendar Year | Total Failure Events | Distinct Failed Banks (`CERT`) | Historical Context |
|:---:|:---:|:---:|:---|
| **1980** | 10 | 10 | Pre-S&L deregulation baseline |
| **1981** | 9 | 9 | Pre-S&L deregulation baseline |
| **1982** | 34 | 34 | Garn–St. Germain Depository Institutions Act |
| **1983** | 50 | 50 | Rising interest rates & agricultural distress |
| **1984** | 83 | 83 | Start of Call Report panel data |
| **1985** | 139 | 139 | S&L / Commercial bank crisis escalates |
| **1986** | 162 | 162 | Oil & real estate collapse (Texas/Southwest) |
| **1987** | 217 | 217 | CEBA passed, Southwest distress |
| **1988** | 232 | 232 | FSLIC insolvency peak |
| **1989** | 530 | 530 | FIRREA enacted; RTC created; peak failure year |
| **1990** | 381 | 381 | RTC active resolution phase; recession |
| **1991** | 268 | 268 | FDICIA passed; early prompt corrective action |
| **1992** | 179 | 179 | Final wave of S&L cleanup |
| **1993** | 50 | 50 | Recovery begins; failure frequency drops |
| **1994** | 15 | 15 | Inter-crisis calm period |
| **1995** | 8 | 8 | Low failure rate |
| **1996** | 6 | 6 | Low failure rate |
| **1997** | 1 | 1 | Lowest failure year |
| **1998** | 3 | 3 | Low failure rate |
| **1999** | 8 | 8 | Low failure rate |
| **2000** | 7 | 7 | Dot-com bust begins |
| **2001** | 4 | 4 | Mild recession |
| **2002** | 11 | 11 | Post-recession uptick |
| **2003** | 3 | 3 | Housing boom expansion |
| **2004** | 4 | 4 | Housing boom expansion |
| **2005** | 0 | 0 | Zero commercial bank failures |
| **2006** | 0 | 0 | Zero commercial bank failures |
| **2007** | 3 | 3 | Early subprime crack (New Century, NetBank) |
| **2008** | 25 | 25 | Global Financial Crisis unfolds (IndyMac, WaMu) |
| **2009** | 140 | 140 | GFC resolution peak wave 1 |
| **2010** | 157 | 157 | GFC resolution peak wave 2 |
| **2011** | 92 | 92 | GFC aftermath / Dodd-Frank implementation |
| **2012** | 51 | 51 | GFC cleanup phase |
| **2013** | 24 | 24 | Post-GFC stabilization |
| **2014** | 18 | 18 | Return to low baseline |
| **2015** | 8 | 8 | Post-crisis calm |
| **2016** | 5 | 5 | Post-crisis calm |
| **2017** | 8 | 8 | Post-crisis calm |
| **2018** | 0 | 0 | Zero failures |
| **2019** | 4 | 4 | Minor local failures |
| **2020** | 4 | 4 | COVID-19 stimulus buffer |
| **2021** | 0 | 0 | Zero failures |
| **2022** | 0 | 0 | Pre-interest rate hike shock |
| **2023** | 5 | 5 | Regional Banking Crisis (SVB, Signature, First Republic) |
| **2024** | 2 | 2 | Isolated regional failures |
| **2025** | 2 | 2 | Isolated |
| **2026** | 4 | 4 | Recent resolutions |

---

## 2. Positive Label Rows and Distinct Failed Banks by REPDTE Year

Source: Bank-quarter modeling panel (`modeling_panel.parquet`, eligible rows).  
Label Definition: `label = 1` if failure occurs within $(t, t+4Q]$ without prior non-failure exit; `0` if 4 quarters of future reporting observed with no failure; censored otherwise.

| REPDTE Year | Eligible Rows | Positive Label Rows (`label = 1`) | Positive Rate | Distinct Failed Banks (`CERT`) |
|:---:|:---:|:---:|:---:|:---:|
| **1984** | 70,134 | 416 | 0.59% | 197 |
| **1985** | 70,335 | 585 | 0.83% | 268 |
| **1986** | 69,791 | 767 | 1.10% | 328 |
| **1987** | 67,207 | 827 | 1.23% | 377 |
| **1988** | 65,173 | 1,853 | 2.84% | 695 |
| **1989** | 62,586 | 1,573 | 2.51% | 658 |
| **1990** | 59,964 | 1,157 | 1.93% | 516 |
| **1991** | 57,212 | 777 | 1.36% | 363 |
| **1992** | 54,477 | 372 | 0.68% | 168 |
| **1993** | 51,642 | 82 | 0.16% | 46 |
| **1994** | 48,749 | 46 | 0.09% | 21 |
| **1995** | 46,356 | 22 | 0.05% | 11 |
| **1996** | 44,100 | 8 | 0.02% | 6 |
| **1997** | 42,207 | 10 | 0.02% | 4 |
| **1998** | 40,590 | 19 | 0.05% | 9 |
| **1999** | 39,613 | 25 | 0.06% | 12 |
| **2000** | 38,645 | 21 | 0.05% | 9 |
| **2001** | 37,742 | 37 | 0.10% | 14 |
| **2002** | 36,998 | 18 | 0.05% | 8 |
| **2003** | 36,113 | 19 | 0.05% | 6 |
| **2004** | 35,273 | 1 | 0.003% | 1 |
| **2005** | 34,592 | 0 | 0.00% | 0 |
| **2006** | 34,009 | 6 | 0.02% | 3 |
| **2007** | 33,474 | 48 | 0.14% | 27 |
| **2008** | 33,050 | 355 | 1.07% | 163 |
| **2009** | 32,148 | 670 | 2.08% | 276 |
| **2010** | 30,654 | 457 | 1.49% | 208 |
| **2011** | 29,419 | 269 | 0.91% | 117 |
| **2012** | 28,111 | 129 | 0.46% | 59 |
| **2013** | 26,875 | 79 | 0.29% | 38 |
| **2014** | 25,565 | 46 | 0.18% | 21 |
| **2015** | 24,377 | 23 | 0.09% | 9 |
| **2016** | 23,341 | 29 | 0.12% | 12 |
| **2017** | 22,281 | 9 | 0.04% | 5 |
| **2018** | 21,330 | 6 | 0.03% | 4 |
| **2019** | 20,455 | 19 | 0.09% | 8 |
| **2020** | 19,901 | 7 | 0.04% | 3 |
| **2021** | 19,226 | 0 | 0.00% | 0 |
| **2022** | 18,761 | 14 | 0.07% | 5 |
| **2023** | 18,375 | 9 | 0.05% | 5 |
| **2024** | 17,868 | 9 | 0.05% | 3 |
| **2025** | 8,653 | 4 | 0.05% | 3 |
| **2026** | 0 | 0 | N/A (Unlabeled post-2025Q2) | 0 |

---

## 3. Comparison of Candidate Splits

### Candidate Split Overview

* **Split A (Baseline / Config Specification):**
  * **Train:** 1984-03-31 to 1992-12-31 (S&L Crisis Peak)
  * **Dev:** 1993-03-31 to 2005-12-31 (Inter-Crisis Period)
  * **Embargo / Gap:** 2006-01-01 to 2007-12-31 (8 Quarters)
  * **Test:** 2008-01-01 to 2014-12-31 (Global Financial Crisis)
  * *Purge Consideration:* Because the forward label window is 4 quarters, training reports from 1992-03-31 through 1992-12-31 have `horizon_end >= 1993-03-31`. The pipeline's automated purge rule `(horizon_end < dev_start)` strictly drops these 54,477 rows (leaving 522,402 active train rows with 7,955 positives and 2,153 distinct failures). Alternatively, `A_clean` defines `train_end = 1991-12-31` with 1992 as an explicit 4Q purge gap.

* **Split B (Enhanced Development Failures Variant):**
  * **Train:** 1984-03-31 to 1989-12-31 (Early & Peak S&L)
  * **Purge Gap 1:** 1990-01-01 to 1991-03-30 (4+ Quarters purge buffer)
  * **Dev:** 1991-03-31 to 2005-12-31 (Late S&L resolution + Inter-crisis)
  * **Embargo / Gap 2:** 2006-01-01 to 2007-12-31 (8 Quarters)
  * **Test:** 2008-01-01 to 2014-12-31 (Global Financial Crisis)

* **Split C (Later Test Window Variant):**
  * **Train:** 1984-03-31 to 1998-12-31 (S&L + Early Calm)
  * **Purge Gap 1:** 1999-01-01 to 2000-03-30 (5 Quarters)
  * **Dev:** 2000-03-31 to 2008-12-31 (Dot-com + Housing boom + Early GFC)
  * **Embargo / Gap 2:** 2009-01-01 to 2009-12-31 (4 Quarters)
  * **Test:** 2010-01-01 to 2016-12-31 (Post-GFC Resolution & Recovery)

### Quantitative Summary Table

| Metric | Split A (Raw Dates) | Split A (Purged / Active) | Split B | Split C |
|:---|:---:|:---:|:---:|:---:|
| **Train Date Range** | 1984Q1–1992Q4 | 1984Q1–1991Q4 (or 1992Q4 purged) | 1984Q1–1989Q4 | 1984Q1–1998Q4 |
| **Train Eligible Rows** | 576,879 | 522,402 | 405,226 | 850,523 |
| **Train Positive Rows** | 8,327 (1.44%) | 7,955 (1.52%) | 6,021 (1.49%) | 8,514 (1.00%) |
| **Train Distinct Failed Banks** | 2,200 | 2,153 | 1,706 | 2,240 |
| **Train-to-Dev Gap** | 1 Quarter (Nominal) | 5 Quarters (4Q Purge) | 5 Quarters | 5 Quarters |
| **Dev Date Range** | 1993Q1–2005Q4 | 1993Q1–2005Q4 | 1991Q1–2005Q4 | 2000Q1–2008Q4 |
| **Dev Eligible Rows** | 532,620 | 532,620 | 644,309 | 319,896 |
| **Dev Positive Rows** | 308 (0.06%) | 308 (0.06%) | 1,457 (0.23%) | 505 (0.16%) |
| **Dev Distinct Failed Banks** | 100 | 100 | **479** | **195** |
| **Dev-to-Test Gap** | 8 Quarters (2006–2007) | 8 Quarters (2006–2007) | 8 Quarters (2006–2007) | 4 Quarters (2009) |
| **Test Date Range** | 2008Q1–2014Q4 | 2008Q1–2014Q4 | 2008Q1–2014Q4 | 2010Q1–2016Q4 |
| **Test Eligible Rows** | 205,822 | 205,822 | 205,822 | 188,342 |
| **Test Positive Rows** | 2,005 (0.97%) | 2,005 (0.97%) | 2,005 (0.97%) | 1,032 (0.55%) |
| **Test Distinct Failed Banks** | 513 | 513 | 513 | 322 |

### Verification of Split Requirements

| Requirement | Split A | Split B | Split C |
|:---|:---:|:---:|:---:|
| **Test has $\ge$ 50 distinct failed banks** | **PASS** (513) | **PASS** (513) | **PASS** (322) |
| **Gap between Dev end and Test start $\ge$ 4Q** | **PASS** (8 quarters: 2006Q1–2007Q4) | **PASS** (8 quarters: 2006Q1–2007Q4) | **PASS** (4 quarters: 2009Q1–2009Q4) |
| **Dev has $\ge$ 30 distinct failed banks** | **PASS** (100 banks $\ge$ 30) | **PASS** (479 banks $\ge$ 30) | **PASS** (195 banks $\ge$ 30) |
| **No Train/Dev label window overlap (Purge check)** | **PASS** (54,477 rows purged automatically by pipeline) | **PASS** (Explicit 5Q gap, 0 rows lost to purge) | **PASS** (Explicit 5Q gap, 0 rows lost to purge) |
| **Label cutoff 2025Q2 respected** | **PASS** (Test ends 2014Q4) | **PASS** (Test ends 2014Q4) | **PASS** (Test ends 2016Q4) |

### Two-Sentence Risk Explanation for Each Candidate Split

* **Split A Risk Assessment:**  
  * *Risk 1 (Class Imbalance in Dev):* Development covers the inter-crisis calm (1993–2005) where the positive rate drops to a razor-thin 0.06% (only 308 positive quarters across 100 banks), which risks extreme threshold sensitivity and poor recall calibration.  
  * *Risk 2 (Regime Shift):* Training is isolated entirely within the 1980s S&L regulatory and accounting regime, making out-of-sample transfer to GFC-era banking (2008–2014) susceptible to substantial structural shift.

* **Split B Risk Assessment:**  
  * *Risk 1 (Regime Overlap in Dev):* Moving 1991–1992 into Development injects 1,149 additional positive rows and 379 failed banks, solving development sparsity but leaving Development partially populated by the same macroeconomic shock as Training.  
  * *Risk 2 (Shorter Training Scope):* Truncating Training at 1989-12-31 reduces training positive rows from 8,327 to 6,021, removing the peak resolution years of FIRREA/FDICIA from the baseline model's fitting history.

* **Split C Risk Assessment:**  
  * *Risk 1 (Contaminated Evaluation Window):* Placing 2010–2016 as Test assesses post-GFC resolution cleanup rather than the onset of the systemic crisis itself, evaluating performance on an era dominated by emergency government liquidity and capital support programs.  
  * *Risk 2 (Mixed Development Regime):* Development spans 2000–2008, blending tranquil expansion with the acute collapse of 2008, making hyperparameter selection dependent on early-crisis distress signals.

---

## 4. Recommendation and Single Biggest Risk

### Recommendation
**Split B** (Train 1984–1989, Gap 1990, Dev 1991–2005, Gap 2006–2007, Test 2008–2014) is strongly recommended from a statistical modeling standpoint, while **Split A** remains the strict reference baseline defined in `config.yaml`.
* If preserving the literal config is paramount, **Split A (with explicit 1992 purge)** is valid and passes all criteria (100 dev failures $> 30$, 513 test failures $> 50$).
* If robust threshold tuning is prioritized, **Split B** provides 479 distinct failed banks in Development (vs. 100 in Split A), dramatically stabilizing cost-sensitive threshold selection without touching the held-out 2008–2014 test set.

### Single Biggest Risk
**Macroeconomic Regime Shift & Accounting Discontinuity:**  
The baseline model is trained during the late-1980s thrift crisis under pre-FDICIA/pre-Basel capital accounting, while the held-out test set occurs 20 years later during the 2008 Global Financial Crisis under Basel II/prompt corrective action and securitized shadow banking. Capital ratios (`RBCRWAJ`) were structurally reported as 0 prior to 1990, meaning the model must rely on balance sheet fundamentals (`capital_ratio`, `noncurrent_loan_to_asset`, `ore_to_asset`, `roa_rolling_4q_mean`) to generalize across two completely different crisis mechanics.

---

## Decision Status
**DECISION: OPEN (Pending User Confirmation)**  
The user must select between:
1. **Option A (Default Config):** Train 1984Q1–1992Q4 (purged to 1991Q4), Dev 1993Q1–2005Q4, Test 2008Q1–2014Q4.
2. **Option B (Robust Dev):** Train 1984Q1–1989Q4, Gap 1990, Dev 1991Q1–2005Q4, Gap 2006–2007, Test 2008Q1–2014Q4.
3. **Option C (Later Window):** Train 1984Q1–1998Q4, Gap 1999, Dev 2000Q1–2008Q4, Gap 2009, Test 2010Q1–2016Q4.
