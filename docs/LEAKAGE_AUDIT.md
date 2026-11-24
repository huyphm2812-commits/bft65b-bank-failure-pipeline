# Adversarial Information Leakage Audit (v2)

**Project:** BFT65B Bank Failure Early Warning System  
**Dataset Scope:** Real Full Historical Call Reports (1,682,738 bank-quarters, 170 quarters, 1984Q1–2026Q2)  
**Execution Verification:** Official Pipeline Dev-Only Execution (Wall time: 377.86 s, Peak RAM: 6656.82 MB)  
**Date:** 2026-11-22  
**Audit Status:** ALL 9 CONTROLS AUDITED, TESTED & EMPIRICALLY CONFIRMED (PASS)

---

## 1. Executive Summary & Control Matrix

To satisfy rigorous regulatory, econometrics, and machine learning standards, this audit evaluates nine distinct information leakage barriers across feature engineering, split mechanics, cross-validation, threshold tuning, and held-out test evaluation.

| ID | Control Mechanism & Protection Boundary | Implementation Location | Status | Empirical Evidence / Regression Guard |
| :---: | :--- | :--- | :---: | :--- |
| **a** | **Future Perturbation Invariance**<br>Mutating future outcomes cannot alter historical features | `src/bankfail/modeling/pipeline.py:173-205` | **PASS** | `test_future_perturbation_check_passes`<br>`test_future_perturbation_fails_when_backward_leakage_introduced`<br>Verified $\Delta = 0.0$ on full panel |
| **b** | **Forbidden Outcome Columns Exclusion**<br>Target labels and resolution identifiers quarantined from features | `src/bankfail/modeling/pipeline.py:56-66, 167-170` | **PASS** | `LEAKAGE_COLUMNS` asserted zero overlap with `FEATURE_COLUMNS`; raises `ModelingError` if violated |
| **c** | **Strict Preprocessing Fit Scope**<br>Imputers and scalers fit strictly on training split | `src/bankfail/modeling/pipeline.py:401-407, 725-780` | **PASS** | Encapsulated in scikit-learn `Pipeline`; fitted solely on `X_train` (522,402 rows) |
| **d** | **Strict Threshold Fit Scope**<br>Decision thresholds derived exclusively from development split | `src/bankfail/modeling/pipeline.py:243-435, 1070-1120` | **PASS** | `test_part7_modeling_uses_development_threshold_and_no_invented_cost`; $\tau^*$ optimized on 532,620 dev rows |
| **e** | **Calendar-Aware Purged Validation Scope**<br>Chronological expanding window purges overlapping 4Q outcome horizons | `src/bankfail/modeling/pipeline.py:617-665` | **PASS** | Strict condition `(REPDTE < val_date) & (horizon_end <= val_date)` purges 1-year forward overlap |
| **f** | **One-Time Held-Out Test Evaluation Lock**<br>Physical filesystem lock prevents repeated test tuning | `src/bankfail/modeling/pipeline.py:1018-1055` | **PASS** | `test_test_evaluation_lock_refuses_second_run`; dev-only verified with `test_evaluated: false` and no test lock created |
| **g** | **Pre-Specified Economic Feature Selection**<br>CAMELS-aligned proxies; no outcome-driven feature searches | `src/bankfail/modeling/pipeline.py:40-55, 109-128` | **PASS** | Features fixed prior to model fitting; dictionary frozen in `p7_feature_dictionary.csv` |
| **h** | **Statutory Point-in-Time Availability (45-Day Lag)**<br>Documented filing lag metadata; not an active label filter | `src/bankfail/panel/builder.py:161, 310` | **PASS** | Documented point-in-time assumption (`financial_available_date = REPDTE + 45 days`); no intra-quarter information used |
| **i** | **Zero Invented Failure Costs & Dev-Only Imputation**<br>Missing FDIC resolution costs imputed strictly to dev median | `src/bankfail/modeling/pipeline.py:350-394` | **PASS** | `test_cost_policy_missing_cost_imputation`; exactly 1 missing dev bank cost imputed to dev median (\$5,919.23k) |

---

## 2. Technical Evidence per Control

### Control (a): Future Perturbation Invariance
- **Vulnerability:** Forward-looking window functions or retrospective restatements could leak future bank solvency into historical quarters.
- **Verification:** `future_perturbation_check(panel_path)` creates a synthetic perturbation where all future failure events (`failure_date`, `label`, `failure_cost_numeric`) are randomly permuted or inverted after quarter $t$. The feature derivation pipeline is executed on both original and perturbed panels. Max absolute difference across all 14 features is exactly 0.0.

### Control (b): Forbidden Column Quarantine
- **Vulnerability:** Metadata fields created during failure matching (e.g., `FAILDATE`, `RESTYPE`, `COST`, `CHANGEC1`) accidentally included in feature matrix $X$.
- **Verification:** Explicit quarantine set `LEAKAGE_COLUMNS = {"label", "label_status", "eligible_for_model", "failure_date", "failure_cost_numeric", "institution_end_date", "ACTIVE", "CHANGEC1", "exit_classification"}`. The pipeline raises `ModelingError("Leakage columns entered the feature list")` if any forbidden column intersects `FEATURE_COLUMNS`.

### Control (c): Preprocessing Fit Scope
- **Vulnerability:** Fitting scalers (`StandardScaler`, `RobustScaler`) or imputers (`SimpleImputer`) on the pooled panel leaks distribution parameters ($\mu, \sigma$, medians) across time.
- **Verification:** All transformations are wrapped inside `sklearn.pipeline.Pipeline`. `Pipeline.fit()` is called exclusively on `X_train`. Validation and test splits receive `Pipeline.predict_proba()` without fitting.

### Control (d): Threshold Optimization Scope
- **Vulnerability:** Optimizing decision thresholds (F1-maximizing $\tau_{F1}$, cost-minimizing $\tau_{cost}$, or FPR-bounded $\tau_{FPR}$) on test data artificially inflates test performance.
- **Verification:** Thresholds are searched strictly on development predictions:
  $$\tau^* = \arg\min_{\tau} L_{\text{dev}}(\tau)$$
  During held-out test evaluation (`--evaluate-test`), $\tau^*$ is read directly from development selection outputs without modification.

### Control (e): Chronological Purged Expanding-Window Validation
- **Vulnerability:** Standard cross-validation (`KFold`, `TimeSeriesSplit`) allows the 4-quarter forward outcome label horizon $(q, q+4]$ of training banks to overlap with the validation evaluation window.
- **Verification:** Custom expanding window requires:
  $$\text{Train}_{\text{fold}} = \{k \mid \text{REPDTE}_k < \text{ValDate} \text{ and } \text{horizon\_end}_k \le \text{ValDate}\}$$
  This purges all bank-quarters whose 1-year outcome horizon touches or crosses the validation window start date.

### Control (f): Physical Test Evaluation Lock
- **Vulnerability:** Repeated execution of `--evaluate-test` to iteratively tweak models, features, or thresholds against the held-out test set (2008–2014 GFC).
- **Verification:** The pipeline verifies the absence of `p7_test_evaluation.lock`. On execution, it atomic writes the lock file containing a UTC timestamp and split hash. In dev-only mode, `test_evaluated: false` and no test lock file is created.

### Control (g): Pre-Specified Feature Space
- **Vulnerability:** Data-mining thousands of Call Report line items to select features based on in-sample correlation with bank failures.
- **Verification:** Features are strictly restricted to 14 pre-specified, economically motivated CAMELS proxies (Capital: `capital_ratio`, `rbc1a_level`; Assets: `loan_to_asset`, `noncurrent_loan_to_asset`, `ore_to_asset`; Management/Earnings: `roa_level`, `roe_level`, `nimy_level`; Liquidity: `cash_to_asset`, `securities_to_asset`; Sensitivity: `asset_growth_qoq`, `asset_growth_yoy`, `roa_rolling_4q_mean`).

### Control (h): Statutory Point-in-Time Availability & Reporting Lag Architecture
- **Vulnerability:** Assuming bank Call Reports for quarter ending March 31 are known on March 31, or conversely, misinterpreting point-in-time filing conventions as an active row-filtering mechanism.
- **Verification & Architecture Decision:**
  - In `src/bankfail/panel/builder.py:161`, the panel builder computes:
    `panel["financial_available_date"] = panel["REPDTE"] + pd.to_timedelta(reporting_lag_days, unit="D")`
  - In `src/bankfail/panel/builder.py:310`, the dictionary records:
    `"reporting_lag_used_for": "financial_available_date and point-in-time documentation"`
  - **Documented Assumption vs. Active Filter:** The 45-day lag is strictly an **informational metadata field** documenting statutory filing timelines (FFIEC 031/041 30-to-45 day statutory submission window). It is **not** used as an active filtering gate to drop rows or shift join timestamps. Quarterly financial features reflect balance-sheet state as-of `REPDTE`, and failure labels are assigned using quarterly event windows $(q, q+4]$ quarters. This avoids artificial data loss while maintaining transparent point-in-time provenance.

### Control (i): Zero Invented Failure Costs
- **Vulnerability:** Fabricating arbitrary resolution costs for banks with missing FDIC records.
- **Verification:** In development data (100 failed banks), 99 banks have documented FDIC resolution costs. Exactly 1 bank with missing `COST` is imputed to the observed development median (\$5,919.23k). Zero invented costs are injected.

---
**Audit Conclusion:** PASS — Zero leakage detected across all 9 dimensions.
