# Pipeline Engineering & Econometric Decisions Log

This log documents every architectural, data-cleaning, and modeling decision made across the BFT65B bank failure early-warning pipeline.

| Decision | Where in Code | Status | Rational / Empirical Basis | Action / Report Instruction |
| :--- | :--- | :---: | :--- | :--- |
| **1. Primary Identifier & Physical Schema** | `src/bankfail/acquisition/storage.py` | **DECIDED** | Uses FDIC Certificate Number (`CERT`) as primary bank ID. Coerces `REPDTE` to ISO-8601 dates stored as `timestamp[ms]` in Parquet. API internal `ID` column is dropped at ingestion. | Schema is frozen, typed, and covered by automated regression tests. |
| **2. Field Selection for Acquisition** | `src/bankfail/acquisition/downloader.py` | **DECIDED** | Downloaded 26 core financial fields (assets, deposits, equity, loans, non-currents, ROA, ROE, NIMY, RBC1AAJ, RBCRWAJ) covering all 170 quarters (1,682,738 rows). | Field set finalized and validated (`validate = PASS`). |
| **3. Non-Negative Quality Screens** | `src/bankfail/quality/audit.py` | **DECIDED** | Removed `NTLNLS` (Net Loans Charged Off) from strict non-negative check. Net charge-offs = Gross charge-offs − Recoveries, which is legitimately negative when recoveries exceed new charge-offs. | Report `negative_ntlnls_rule_rejected` as a tested-and-rejected rule in Part 2 of report. |
| **4. Missing-CERT Failures Handling** | `src/bankfail/quality/audit.py`, `panel/builder.py` | **DECIDED** | 488 failures in the Failures table have null `CERT`. Empirical check confirmed all 488 occurred between 1934 and 1976. Zero missing CERTs exist in the 1984–2026 study period. Excluded from panel without silent label noise. | Cite historical date range (1934–1976) in Section 2 of report. |
| **5. Exit Classification: Assistance vs Failure** | `src/bankfail/panel/builder.py` | **DECIDED** | `RESTYPE == 'ASSISTANCE'` (FDIC Section 13(c) open-bank assistance transactions, predominantly 1980s S&L assistance plus limited 2008–2009 FDIC systemic assistance) does not terminate bank operations. Only explicit `RESTYPE == 'FAILURE'` constitutes a failure label event. | Document that assisted open banks are treated as non-failures in report Part 2 & 3. |
| **6. Merger Policy & Sensitivity Run** | `src/bankfail/panel/builder.py` | **DECIDED (with Sensitivity)** | Standard policy: censor bank-quarters before merger exits (`label = NaN`) because the rubric defines "mergers are not failures" and their risk exposure ends upon acquisition. A sensitivity run is executed on full data retaining mergers as negative outcomes (`label = 0`), saved to `merger_policy_sensitivity.json`. | Present the delta between standard and sensitivity eligible rows in report Section 3. |
| **7. Forward Label Window & Cutoff** | `src/bankfail/panel/builder.py` | **DECIDED** | Label $y_{i,q} = 1$ if bank $i$ fails in $(q, q+4]$. Banks active through $q+4$ get $y=0$. Quarters after `2025-06-30` are censored because the 4-quarter forward outcome is unobservable. | Report 2025Q2 cutoff rule and censoring justification in Part 3. |
| **8. Reporting Lag Assumption** | `src/bankfail/panel/builder.py`, `modeling/pipeline.py` | **DECIDED** | 45-day statutory Call Report filing lag (`financial_available_date = REPDTE + 45 days`). Documented as point-in-time availability assumption. | Note 45-day lag assumption in feature engineering section. |
| **9. Chronological Split Windows** | `config/config.yaml`, `src/bankfail/modeling/pipeline.py` | **DECIDED (Split A)** | **Split = Split A.** Window definitions: Train (1984Q1–1992Q4, purged to 1991Q4 due to 4Q forward label horizon, 522,402 active rows, 7,955 positives [1.5228%], 2,153 distinct failed banks); Dev (1993Q1–2005Q4, 532,620 rows, 308 positives [0.0578%], 100 distinct failed banks); Embargo Gap (2006Q1–2007Q4, 8 quarters); Test (2008Q1–2014Q4, 205,822 rows, 2,005 positives [0.9741%], 513 distinct failed banks).<br>**Prevalence-Shift Limitation:** Under Split A, the positive label prevalence collapses from 1.5228% in Train down to 0.0578% in Dev (a 26.3-fold drop in positive density during the inter-crisis calm), before surging back to 0.9741% during the GFC test period. Documented as a key econometric regime shift in Part 5. | Split A confirmed and enforced in `frozen_model_config.json`. |
| **10. TimeSeriesSplit Deviation & Purged Expanding Window** | `src/bankfail/modeling/pipeline.py` | **DECIDED** | Standard `sklearn.model_selection.TimeSeriesSplit` is rejected: it splits arbitrarily by row index, ignores calendar quarters and bank clusters, and fails to purge overlapping 4-quarter forward label horizons. The pipeline implements a custom calendar-aware, annual expanding-window validation that purges all rows where `horizon_end >= validation_start`. | Highlight purged annual expanding-window validation as a zero-leakage method in Part 5. |
| **11. `NETINC` Treatment (YTD vs Quarterly)** | `src/bankfail/modeling/pipeline.py` | **DECIDED (Excluded from Primary Features)** | Confirmed from official FDIC schema (`https://api.fdic.gov/banks/docs/risview_properties.yaml`). Exact dictionary text: **Title:** `"Net income"`; **Description:** *`"Net interest income plus total noninterest income plus realized gains (losses) on securities and extraordinary items, less total noninterest expense, loan loss provisions and income taxes."`*<br>**Year-to-Date Inference:** In Call Report Schedule RI, income statement figures are reported cumulatively from January 1 of each calendar year (Q1=3mo, Q2=6mo, Q3=9mo, Q4=12mo). Dividing raw cumulative `NETINC` by contemporaneous assets creates an artificial 4x seasonal distortion ($Q4 \approx 4 \times Q1$). `net_income_to_asset` and `netinc_change_qoq` are strictly excluded from `FEATURE_COLUMNS`. Profitability is captured via `roa_level` (`ROA = NETINCA / ASSET5`, annualized by FDIC) and `roa_rolling_4q_mean`. Full field dictionary analysis in `docs/FIELD_DICTIONARY_NOTES.md`. | Document exclusion of raw YTD `NETINC` and adoption of annualized `ROA` in report Section 5. |
| **12. False-Positive Cost Assumption & Threshold Selection** | `src/bankfail/modeling/pipeline.py`, `dev_analysis.py` | **DECIDED (`DEF_A`, 100:1, $\tau_{\text{V3}}^* = 0.103990$)** | Cost threshold optimizes $\min L_A(\tau)$ under **`DEF_A`** (bank miss loss + bank-quarter false alarms) at a **100:1** false-negative to false-positive ratio ($C_{FP} = 0.01 \times \text{median}(\text{COST}_{\text{dev}}) = \$59.19225\text{k}$). For the selected **Variant 3 (12 features)** primary model (Random Forest, seed 42, `train_only`), the development optimal threshold is **$\tau_{\text{RF, V3}}^* = 0.103990$** (expected development loss **\$1,061,061.21k**, catching **87/100** failed banks with **2,817** false-alarm rows [58.3 alarms/quarter, 0.569%], and 1,000-resample bootstrap median `0.108840` [5th pct: `0.103990`, 95th pct: `0.191453`]). By comparison, the legacy Variant 1 (14 features) threshold was `0.037624` (\$682,044.97k on V1, but \$1,507,005.41k with 10,382 false-alarm rows if misapplied to V3). Sensitivity evaluated across 10:1, 50:1, 100:1, 500:1, 1000:1 under both `DEF_A` and `DEF_B`. | Frozen in `frozen_model_config.json`. |
| **15. Feature Variant & Primary Model Selection** | `src/bankfail/modeling/dev_analysis.py`, `docs/PREREGISTRATION.md` | **DECIDED (`variant_3`, `random_forest`)** | Applied preregistered paired bank-bootstrap parsimony rule across 1,000 resamples (seed 42): **Variant 3 (12 features, excluding `rbcrwa_level` and `rbc1a_level`)** is both the simplest variant and the highest development PR-AUC performer ($0.2436$ [95% CI: $0.1723, 0.3221$], beating V1 by $+0.0216$ and V2 by $+0.0196$ with 95% CIs excluding zero). **Random Forest** is selected as primary model (outperforming Logistic Regression by $\Delta \text{PR-AUC} = +0.0904$ to $+0.1094$, 95% CI excluding zero), with **Logistic Regression** retained as secondary interpretable benchmark under `train_only` policy. | Frozen in `frozen_model_config.json`. |
| **13. One-Time Held-Out Test Evaluation Lock** | `src/bankfail/modeling/pipeline.py` | **DECIDED** | Script enforces that `--evaluate-test` can only execute once. A lock file `p7_test_evaluation.lock` is stored in `02_local_workspace/` and prevents repeated tuning on the test set. | Retain lock file; never tune hyperparameters against held-out test results. |
| **14. Label Edge Cases: Repeated Failures & Post-Failure Censoring** | `src/bankfail/panel/builder.py` | **DECIDED (Option A)** | Retains the conservative bank-level rule $\min(\text{FAILDATE})$. Exactly 3 CERTs (29400 - Platte Valley FS&LA, 31927 - Westwood S&LA, 32059 - Gateway Savings Bank) have two distinct `RESTYPE=FAILURE` records from 1980s FSLIC Management Consignment Program (MCP) conservatorships. Under Option A, their post-$F_1$ quarters stay censored (`label = NaN`, `label_status = 'post_failure_report'`).<br>**Known Limitation:** Exactly 10 rows (all in the 1984–1992 train window, 0 in development or test) would become positive (`label = 1`) under a forward event-level rule targeting $F_2$, and 16 intermediate quarters would be subject to conservatorship classification. Censoring them preserves model purity by excluding distorted conservatorship Call Reports.<br>**Repeated Failure Records Split:** Across the entire Failures dataset, there are 88 records across 44 distinct CERTs with $\ge 2$ entries: **37 CERTs (84.1%)** are `ASSISTANCE` followed by `FAILURE` (open-bank assistance followed years later by regulatory closure), **4 CERTs (9.1%)** are `ASSISTANCE` followed by `ASSISTANCE`, and **3 CERTs (6.8%)** are `FAILURE` followed by `FAILURE`. | Document Option A decision, 10-row train-only limitation, and empirical 37/4/3 repeated-records split in report Section 3. |

---

## Cost Threshold Semantics & Loss Formulations

### 1. Evaluated Population & Row Scope
Threshold optimization is strictly restricted to the development split $\mathcal{D}_{\text{dev}} = \{(x_{i,q}, y_{i,q}, \text{CERT}_i, \text{COST}_i) \mid q \in [\text{dev\_start}, \text{dev\_end}] \text{ and } y_{i,q} \in \{0, 1\}\}$. No training or held-out test rows are read during threshold search.

### 2. Missing COST Handling
For failed banks ($i \in \mathcal{B}^+$ where $\mathcal{B}^+ = \{i \mid \exists q, y_{i,q} = 1\}$), the failure cost is obtained from the first non-null FDIC financial resolution cost:
$$\text{COST}_i = \begin{cases} \text{COST}_i^{\text{obs}} & \text{if } \text{COST}_i^{\text{obs}} \text{ is non-null} \\ \text{median}(\{\text{COST}_k^{\text{obs}} \mid k \in \mathcal{B}^+, \text{COST}_k^{\text{obs}} \neq \text{null}\}) & \text{if } \text{COST}_i^{\text{obs}} \text{ is null} \end{cases}$$
Zero invented costs are introduced; missing resolution costs are imputed strictly to the observed development-period median.

### 3. Mathematical Formulations of Loss Definitions

Let $\hat{P}_{i,q} \in [0, 1]$ denote the model predicted probability for bank $i$ in quarter $q$, and $\tau \in [0, 1]$ denote the decision threshold.

#### Current Implementation Semantics (`DEF_OLD`)
In the original `_select_cost_threshold` and `_bank_level_loss`:
- **Bank Prediction Flag:** $\widehat{Y}_i(\tau) = \max_{q \in Q_i} \mathbb{I}(\hat{P}_{i,q} \ge \tau)$, where $Q_i$ contains **all** development quarters for bank $i$.
- **Positive Banks:** $\mathcal{B}^+ = \{i \mid \exists q \in Q_i, y_{i,q} = 1\}$.
- **Negative Banks:** $\mathcal{B}^- = \{i \mid \exists q \in Q_i, y_{i,q} = 0\}$. *(Note: Failing banks with pre-failure historical rows satisfy $i \in \mathcal{B}^+ \cap \mathcal{B}^-$.)*
- **False Negatives:** $\mathcal{FN}_{\text{old}}(\tau) = \{i \in \mathcal{B}^+ \mid \widehat{Y}_i(\tau) = 0\}$.
  $$L_{FN,\text{old}}(\tau) = \sum_{i \in \mathcal{FN}_{\text{old}}(\tau)} \text{COST}_i$$
- **False Positives:** $\mathcal{FP}_{\text{old}}(\tau) = \{i \in \mathcal{B}^- \mid \widehat{Y}_i(\tau) = 1\}$.
  $$L_{FP,\text{old}}(\tau) = C_{FP} \cdot |\mathcal{FP}_{\text{old}}(\tau)|$$
- **Total Loss:** $L_{\text{old}}(\tau) = L_{FN,\text{old}}(\tau) + L_{FP,\text{old}}(\tau)$.
- *Limitation:* A failing bank $i$ with historical $y=0$ rows that is correctly flagged on its positive rows ($\widehat{Y}_i=1$) escapes $L_{FN}$ but is penalized in $L_{FP}$ because $i \in \mathcal{B}^-$.

#### Definition A (`DEF_A`: Bank Miss Loss + Bank-Quarter False Alarms)
- **Catch Condition:** A failed bank $i \in \mathcal{B}^+$ is caught if at least one of its positive-horizon quarters is flagged:
  $$\widehat{Y}_i^+(\tau) = \max_{q \in Q_i^+} \mathbb{I}(\hat{P}_{i,q} \ge \tau), \quad \text{where } Q_i^+ = \{q \in Q_i \mid y_{i,q} = 1\}$$
- **False Negatives (Bank Misses):**
  $$\mathcal{FN}_A(\tau) = \{i \in \mathcal{B}^+ \mid \widehat{Y}_i^+(\tau) = 0\}$$
  $$L_{FN,A}(\tau) = \sum_{i \in \mathcal{FN}_A(\tau)} \text{COST}_i$$
- **False Positives (Bank-Quarter False Alarms):** Every flagged row where $y_{i,q} = 0$:
  $$\mathcal{FP}_A(\tau) = \{(i, q) \mid y_{i,q} = 0 \text{ and } \hat{P}_{i,q} \ge \tau\}$$
  $$L_{FP,A}(\tau) = C_{FP} \cdot |\mathcal{FP}_A(\tau)|$$
- **Total Expected Loss:**
  $$L_A(\tau) = \sum_{i \in \mathcal{B}^+, \max_{q \in Q_i^+} \hat{P}_{i,q} < \tau} \text{COST}_i + C_{FP} \sum_{(i,q): y_{i,q}=0} \mathbb{I}(\hat{P}_{i,q} \ge \tau)$$

#### Definition B (`DEF_B`: Bank Miss Loss + Pure Negative Bank False Alarms)
- **False Negatives:** Identical to $\text{DEF\_A}$:
  $$L_{FN,B}(\tau) = \sum_{i \in \mathcal{B}^+, \max_{q \in Q_i^+} \hat{P}_{i,q} < \tau} \text{COST}_i$$
- **Pure Negative Banks:** Banks with zero positive-label rows throughout development:
  $$\mathcal{B}_{\text{pure}}^- = \{i \mid \forall q \in Q_i, y_{i,q} = 0\}$$
- **False Positives:** Any pure negative bank with at least one flagged quarter incurs $C_{FP}$ once:
  $$\mathcal{FP}_B(\tau) = \{j \in \mathcal{B}_{\text{pure}}^- \mid \max_{q \in Q_j} \hat{P}_{j,q} \ge \tau\}$$
  $$L_{FP,B}(\tau) = C_{FP} \cdot |\mathcal{FP}_B(\tau)|$$
- **Total Expected Loss:**
  $$L_B(\tau) = \sum_{i \in \mathcal{B}^+, \max_{q \in Q_i^+} \hat{P}_{i,q} < \tau} \text{COST}_i + C_{FP} \sum_{j \in \mathcal{B}_{\text{pure}}^-} \mathbb{I}\left(\max_{q \in Q_j} \hat{P}_{j,q} \ge \tau\right)$$

---

## Protocol Amendment: Test Evaluation Specification Scope Reconciliation (Option B1)

**Date:** 2026-11-23  
**Status:** **RATIFIED PRIOR TO TEST EVALUATION**  
**Pre-Test Condition:** Neither test lock has been modified, `--evaluate-test` has not been executed, and zero test labels, predictions, or metrics were inspected or used.

### 1. Context & Motivation
An independent source-code audit identified that draft documentation in `docs/TEST_EVALUATION_SPEC.md` listed several auxiliary post-hoc files and tables (`p7_test_bootstrap.csv`, `p7_test_confusion_matrix.json`, `p7_test_annual_quarterly_alarms.csv`, `p7_test_posthoc_loss.csv`, `capital_ratio_rule` test evaluation, `p7_test_annual_catch_trajectories.png`, and `p7_spark_mllib_predictions.csv`) that were not generated by `run_modeling_pipeline()`.

### 2. Original Plan vs. Deferred Scope
- **Originally Planned in Draft Spec:** An expansive post-test reporting suite attempting to automatically generate test-period bootstrap resampling, multi-ratio post-hoc loss tables, standalone confusion JSONs, and extra figures within the single-use test evaluation loop.
- **Scope Deferred Under Option B1:**
  - `p7_test_bootstrap.csv`: 1,000-resample bank-cluster bootstrap on the test partition.
  - `p7_test_confusion_matrix.json`: Standalone JSON export (confusion matrix is already present inside `p7_model_results.json`).
  - `p7_test_annual_quarterly_alarms.csv`: Standalone 28-quarter breakdown CSV (predictions are preserved in `p7_test_predictions.parquet`).
  - `p7_test_posthoc_loss.csv`: Multi-ratio loss matrix on test data.
  - `capital_ratio_rule`: Heuristic rule evaluation on test partition.
  - `p7_test_annual_catch_trajectories.png` & `p7_spark_mllib_predictions.csv`: Supplementary plots and exports.

### 3. Rationale for Keeping Pipeline Minimal
1. **Zero Pipeline Churn at Test Gate:** Modifying `run_modeling_pipeline()` to inject extensive non-essential post-hoc analysis logic immediately prior to the single-use evaluation introduces unnecessary regression and crash risks.
2. **Preventing Post-Hoc Shopping:** Preregistered methodology specifies optimizing decisions on development data. Running multi-ratio sensitivity searches post-hoc on the test partition contradicts the strict single-use objective.
3. **Information Completeness:** All core evaluation requirements—Random Forest Variant 3 at frozen threshold 0.103990, Logistic Regression baseline, dummy prevalence baseline, PR-AUC, ROC-AUC, Brier score, 4-cell confusion matrix, precision, recall, F1, FPR, `DEF_A` expected loss (missed banks, failure loss, false alarm rows, false alarm cost, banks caught), top 1%/2%/5% alarm budgets, and row-level test predictions—are completely captured in `p7_model_results.json`, `p7_model_metrics.csv`, and `p7_test_predictions.parquet`.
4. **Deferred Status Guarantee:** None of the deferred outputs will be generated by `--evaluate-test` or claimed as completed in the final report.

### 4. Independence Confirmation
This protocol amendment is formally adopted **before** any test execution. No test data, test labels, or test predictions were accessed, and zero test results influenced this decision.

