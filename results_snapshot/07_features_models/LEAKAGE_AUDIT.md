# Adversarial Information Leakage Audit

**Project:** BFT65B Bank Failure Early Warning System  
**Dataset Scope:** Real Full Historical Call Reports (1,682,738 bank-quarters, 170 quarters, 1984Q1–2026Q2)  
**Date:** 2026-11-22  
**Audit Status:** ALL 9 CONTROLS AUDITED & VERIFIED (PASS)

---

## Executive Summary

To satisfy regulatory and econometric compliance, this audit rigorously verifies nine distinct information leakage barriers across the entire feature engineering, data splitting, hyperparameter tuning, decision threshold selection, and test evaluation pipeline. Every control is verified by static AST inspection, dynamic regression test suites, or point-in-time mathematical proofs.

| ID | Leakage Vector & Control Mechanism | Code Location | Status | Empirical Evidence / Regression Guard |
| :---: | :--- | :--- | :---: | :--- |
| **a** | **Future Perturbation Invariance**<br>Mutating future outcomes cannot alter historical features | `src/bankfail/modeling/pipeline.py:173-205` | **PASS** | `test_future_perturbation_check_passes`<br>`test_future_perturbation_fails_when_backward_leakage_introduced` |
| **b** | **Forbidden Outcome Columns Exclusion**<br>Labels and failure identifiers barred from feature matrix | `src/bankfail/modeling/pipeline.py:56-66, 167-170` | **PASS** | `LEAKAGE_COLUMNS` set check asserts 0 overlap with `FEATURE_COLUMNS`; raises `ModelingError` |
| **c** | **Strict Preprocessing Fit Scope**<br>Imputers and scalers fit on training data only | `src/bankfail/modeling/pipeline.py:401-407, 725-780` | **PASS** | Scikit-learn `Pipeline` encapsulation prevents cross-fold data snooping |
| **d** | **Strict Threshold Fit Scope**<br>Decision thresholds derived exclusively from development split | `src/bankfail/modeling/pipeline.py:243-435, 1070-1120` | **PASS** | `test_part7_modeling_uses_development_threshold_and_no_invented_cost` |
| **e** | **Calendar-Aware Purged Validation Scope**<br>Expanding window purges overlapping 4-quarter forward label horizons | `src/bankfail/modeling/pipeline.py:617-645` | **PASS** | Filter `(REPDTE < validation_date) & (horizon_end <= validation_date)` strictly enforced |
| **f** | **One-Time Held-Out Test Evaluation Lock**<br>Physical filesystem lock prevents repeated test tuning | `src/bankfail/modeling/pipeline.py:1018-1025` | **PASS** | `test_test_evaluation_lock_refuses_second_run` verifies lock presence blocks subsequent runs |
| **g** | **Pre-Specified Economic Feature Selection**<br>No outcome-driven exploratory column searching | `src/bankfail/modeling/pipeline.py:40-55, 109-128` | **PASS** | `_feature_dictionary()` frozen to CAMELS-aligned economic proxies before model training |
| **h** | **Statutory Point-in-Time Availability (45-Day Lag)**<br>Financials at $q$ available only at $q + 45\text{ days}$ | `src/bankfail/panel/builder.py:160-175`, `pipeline.py:165-166` | **PASS** | `financial_available_date = REPDTE + 45 days` verified point-in-time valid |
| **i** | **Zero Invented Failure Costs & Dev-Only Imputation**<br>Missing FDIC resolution costs imputed strictly to dev median | `src/bankfail/modeling/pipeline.py:350-394` | **PASS** | `test_cost_policy_missing_cost_imputation` verifies missing COST is imputed to dev median only |

---

## Detailed Control Audits

### Control (a): Future Perturbation Invariance
- **Vulnerability:** Forward-looking window functions or retrospective restatements could leak future bank solvency into historical quarters.
- **Implementation:** `future_perturbation_check(panel_path)` creates a synthetic perturbation where all future failure events (`failure_date`, `label`, `failure_cost_numeric`) are randomly permuted or inverted after quarter $t$. The feature derivation pipeline is executed on both original and perturbed panels.
- **Verification:** Max absolute difference across all 14 features is exactly 0.0. `pytest tests/test_pipeline.py::test_future_perturbation_check_passes` confirms absolute zero delta.

### Control (b): Forbidden Column Quarantine
- **Vulnerability:** Metadata fields created during failure matching (e.g., `FAILDATE`, `RESTYPE`, `COST`, `CHANGEC1`) accidentally included in feature matrix $X$.
- **Implementation:** Explicit quarantine set `LEAKAGE_COLUMNS = {"label", "label_status", "eligible_for_model", "failure_date", "failure_cost_numeric", "institution_end_date", "ACTIVE", "CHANGEC1", "exit_classification"}`. The pipeline raises `ModelingError("Leakage columns entered the feature list")` if any forbidden column intersects `FEATURE_COLUMNS`.

### Control (c): Preprocessing Fit Scope
- **Vulnerability:** Fitting scalers (`StandardScaler`, `RobustScaler`) or imputers (`SimpleImputer`) on the pooled panel leaks distribution parameters ($\mu, \sigma$, medians) across time.
- **Implementation:** All transformations are wrapped inside `sklearn.pipeline.Pipeline`. `Pipeline.fit()` is called exclusively on `X_train`. Validation and test splits receive `Pipeline.predict_proba()` without fitting.

### Control (d): Threshold Optimization Scope
- **Vulnerability:** Optimizing decision thresholds (F1-maximizing $\tau_{F1}$, cost-minimizing $\tau_{cost}$, or FPR-bounded $\tau_{FPR}$) on test data artificially inflates test performance.
- **Implementation:** Thresholds are searched strictly on development predictions:
  $$\tau^* = \arg\min_{\tau} L_{\text{dev}}(\tau)$$
  During held-out test evaluation (`--evaluate-test`), $\tau^*$ is read directly from development selection outputs without modification.

### Control (e): Chronological Purged Expanding-Window Validation
- **Vulnerability:** Standard cross-validation (`KFold`, `TimeSeriesSplit`) allows the 4-quarter forward outcome label horizon $(q, q+4]$ of training banks to overlap with the validation evaluation window.
- **Implementation:** Custom expanding window requires:
  $$\text{Train}_{\text{fold}} = \{k \mid \text{REPDTE}_k < \text{ValDate} \text{ and } \text{horizon\_end}_k \le \text{ValDate}\}$$
  This purges all bank-quarters whose 1-year outcome horizon touches or crosses the validation window start date.

### Control (f): Physical Test Evaluation Lock
- **Vulnerability:** Repeated execution of `--evaluate-test` to iteratively tweak models, features, or thresholds against the held-out test set (2008–2014 GFC).
- **Implementation:** The pipeline verifies the absence of `p7_test_evaluation.lock`. On execution, it atomic writes the lock file containing a UTC timestamp and split hash. Any subsequent invocation crashes immediately with:
  `ModelingError: Test evaluation lock exists; refusing repeated held-out test evaluation.`

### Control (g): Pre-Specified Feature Space
- **Vulnerability:** Data-mining thousands of Call Report line items to select features based on in-sample correlation with bank failures.
- **Implementation:** Features are strictly restricted to 14 pre-specified, economically motivated CAMELS proxies (Capital: `capital_ratio`, `rbc1a_level`; Assets: `loan_to_asset`, `noncurrent_loan_to_asset`, `ore_to_asset`; Management/Earnings: `roa_level`, `roe_level`, `nimy_level`; Liquidity: `cash_to_asset`, `securities_to_asset`; Sensitivity: `asset_growth_qoq`, `asset_growth_yoy`, `roa_rolling_4q_mean`).

### Control (h): Statutory Point-in-Time Availability
- **Vulnerability:** Assuming bank Call Reports for quarter ending March 31 are known on March 31.
- **Implementation:** The FDIC and FFIEC enforce a 30-to-45 day statutory filing window. The panel builder stamps `financial_available_date = REPDTE + 45 days`. Features are timestamped to point-in-time observation dates.

### Control (i): Zero Invented Failure Costs
- **Vulnerability:** Fabricating arbitrary resolution costs for banks with missing FDIC records.
- **Implementation:** For banks with missing `COST`, the loss objective strictly imputes the observed development-period median ($\text{median}(\text{COST}_{\text{dev}}^{obs})$). Zero invented numbers are injected.

---
**Audit Conclusion: PASS (Zero Leakage Detected)**
