# BFT65B Pre-Registration & Evaluation Protocol

**Status:** DRAFT (Awaiting user confirmation)  
**Strict Policy:** Do not edit after the test is run.

---

## 1. Pre-Specified Feature Variant Selection Rule

Before evaluating the variant selection outcome, the decision rule is formally pre-specified as follows:

> **Formal Pre-Specified Variant Rule:**  
> "Choose the simplest feature variant (fewest features) whose development PR-AUC paired difference against the best variant has a 95% bank-bootstrap interval that includes zero."

### Candidate Feature Variants:
1. **Variant 1 (All 14 Features):** Full pre-specified CAMELS proxy set including `rbcrwa_level` and `rbc1a_level` (with pre-1990 structural zeros recoded to `NaN` plus explicit missing indicator).
2. **Variant 2 (13 Features):** Excludes `rbcrwa_level` to eliminate pre-Basel I structural uncollection before 1990.
3. **Variant 3 (12 Features):** Excludes both `rbcrwa_level` and `rbc1a_level`, relying strictly on traditional leverage `capital_ratio` (`EQ/ASSET`).

Complexity Ordering (Simplest to Most Complex):
$$\text{Variant 3 (12 features)} \prec \text{Variant 2 (13 features)} \prec \text{Variant 1 (14 features)}$$

### Empirical Outcome of Pre-Specified Variant Rule (`bootstrap_evaluation.csv` & `bootstrap_paired_differences.csv`):
- **Random Forest Point & Bootstrap Mean PR-AUC (1,000 bank-cluster resamples, seed 42):**
  - Variant 3 (12 features): $\text{PR-AUC} = \mathbf{0.2436}$ [95% CI: $0.1723, 0.3221$], $\text{ROC-AUC} = \mathbf{0.9399}$ [$0.9081, 0.9681$] (**Best Variant**)
  - Variant 2 (13 features): $\text{PR-AUC} = 0.2239$ [95% CI: $0.1555, 0.2967$], $\text{ROC-AUC} = 0.9200$ [$0.8817, 0.9532$]
  - Variant 1 (14 features): $\text{PR-AUC} = 0.2220$ [95% CI: $0.1531, 0.2959$], $\text{ROC-AUC} = 0.9304$ [$0.8913, 0.9648$]
- **Paired Bootstrap Differences Against Best Variant (V3):**
  - $\Delta \text{PR-AUC}(\text{RF V1} - \text{RF V3}) = -0.0216$ [95% CI: $-0.0409, -0.0008$] (Excludes zero)
  - $\Delta \text{PR-AUC}(\text{RF V2} - \text{RF V3}) = -0.0196$ [95% CI: $-0.0363, -0.0008$] (Excludes zero)
  - $\Delta \text{PR-AUC}(\text{LR V1} - \text{LR V3}) = -0.0026$ [95% CI: $-0.0109, +0.0047$] (Includes zero)
  - $\Delta \text{PR-AUC}(\text{LR V2} - \text{LR V3}) = -0.0023$ [95% CI: $-0.0099, +0.0040$] (Includes zero)
- **Rule Application Verdict:** **Variant 3 (`variant_3`, 12 features)** is both the simplest variant (fewest features) and the highest-performing variant on development data. Selected variant: **`variant_3`**.

---

## 2. Pre-Specified Decision Threshold Rule

The decision threshold $\tau^*$ is pre-specified to be selected on development data using the supervisory cost-loss objective:
$$\tau^* = \arg\min_{\tau} L_{\text{dev}}(\tau)$$
Under `DEF_A`:
$$L_A(\tau) = \sum_{i \in \text{Missed Banks}} \text{COST}_i + C_{FP} \sum_{(i,q): y_{i,q}=0} \mathbb{I}(\hat{P}_{i,q} \ge \tau)$$
Where:
- Primary supervisory ratio: **100:1** ($C_{FP} = 0.01 \times \text{median}(\text{COST}_{\text{dev}}^{obs}) = \$59.19225\text{ thousand USD}$).
- Missing failed bank resolution cost is strictly imputed to observed development median (\$5,919.225k).
- A failed institution is counted as caught if $\ge 1$ quarterly report in its pre-failure 4-quarter window is alarmed.
- **Empirical Outcome for Selected Variant 3 (`p7_cost_threshold_comprehensive.csv` & `p7_threshold_stability_rf_100to1.json`):**
  - Variant 3 Random Forest (`rf_v3`, 12 features) optimal development threshold: **$\tau_{\text{RF, V3}}^* = 0.103990$** (expected development loss **\$1,061,061.21k**, catching **87/100** failed banks with **2,817** false-alarm rows [58.3 alarms/quarter, 0.569% of operating bank-quarters]).
  - 1,000-resample bank-cluster bootstrap stability for $\tau_{\text{RF, V3}}^*$: Median = **0.108840** [5th pct: **0.103990**, 95th pct: **0.191453**]; alarms/quarter median = **54.4** [24.8, 61.4].
  - *(Methodological Note: The earlier draft threshold `0.037624` was derived from Variant 1 [14 features]; evaluating Variant 3 at `0.037624` on development data yields \$1,507,005.41k loss and 10,382 false-alarm rows. Prior to any held-out test evaluation, the frozen threshold is aligned to Variant 3's exact development optimum `0.103990`.)*

---

## 3. Pre-Specified Model Selection & Split Protocol

- **Chronological Split:** Split A (Train 1984Q1–1991Q4 purged; Dev 1993Q1–2005Q4; Embargo 2006Q1–2007Q4; Test 2008Q1–2014Q4).
- **Primary Model Candidate:** Random Forest (evaluated alongside Logistic Regression as interpretable benchmark).
- **Training Policy:** Fit strictly on the training partition (1984Q1–1991Q4). No refitting or pooling with development data prior to test evaluation.

---

## 4. Execution Commands (Placeholders for User)

The user will confirm the configuration and run test evaluation with:

```powershell
# Step 1: Freeze Configuration (Requires explicit arguments, no defaults)
python scripts/freeze_config.py `
  --split-train-end 1992-12-31 `
  --split-dev-start 1993-03-31 `
  --split-dev-end 2005-12-31 `
  --split-test-start 2008-01-01 `
  --split-test-end 2014-12-31 `
  --loss-definition DEF_A `
  --fn-fp-ratio 100:1 `
  --feature-variant variant_3 `
  --final-model random_forest `
  --training-policy train_only `
  --seed 42 `
  --threshold 0.103990 `
  --output results_snapshot/07_features_models/frozen_model_config.json

# Step 2: Non-Destructive Frozen Preflight Verification (Zero test access, zero lock writes)
python scripts/run_pipeline.py model `
  --config config/config.yaml `
  --frozen-config results_snapshot/07_features_models/frozen_model_config.json `
  --preflight-only

# Step 3: Single Held-Out Test Evaluation
python scripts/run_pipeline.py model `
  --config config/config.yaml `
  --frozen-config results_snapshot/07_features_models/frozen_model_config.json `
  --evaluate-test
```
