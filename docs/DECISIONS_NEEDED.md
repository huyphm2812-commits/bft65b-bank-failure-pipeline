# BFT65B Pending Supervisory Decisions (Decisions Needed - v2)

**Status:** AWAITING USER CONFIRMATION  
**Policy:** Hard rule enforced: the Agent will NEVER unilaterally choose open decisions for the user.  
**Action Required:** Review the empirical tradeoffs below (computed from the real full dataset and 1,000 bootstrap resamples) and confirm your selections before executing final held-out test evaluation (`--evaluate-test`).

---

### Decision 1: Chronological Split Selection
- **Status:** **OPEN** (Working default: Split A)
- **Options:**
  - **Option A (Split A - Recommended by Rubric):**
    - Train: 1984Q1–1992Q4 (purged to 1991Q4, 522,402 active rows, 7,955 positives [1.52%], 2,153 failed banks).
    - Dev: 1993Q1–2005Q4 (532,620 rows, 308 positives [0.058%], 100 failed banks).
    - Embargo: 2006Q1–2007Q4 (8 quarters).
    - Test: 2008Q1–2014Q4 (205,822 rows, 2,005 positives [0.97%], 513 failed banks).
    - *Tradeoff:* Strictly matches historical crisis periods (1980s S&L crisis -> 1990s inter-crisis calm -> 2008 GFC), but positive prevalence collapses 26.3-fold from 1.52% to 0.058% in development, making threshold calibration sensitive to sparse dev failures.
  - **Option B (Split B - Balanced Failures):**
    - Train: 1984Q1–1989Q4 (373,636 rows, 1,774 failed banks).
    - Dev: 1990Q1–2005Q4 (681,386 rows, 479 failed banks).
    - Embargo: 2006Q1–2007Q4. Test: 2008Q1–2014Q4.
    - *Tradeoff:* Increases development failures from 100 to 479, greatly stabilizing threshold search, but shortens training period by 3 years.
- **Decision Required:** Confirm Split A or select Split B.

---

### Decision 2: Cost-Loss Formulation (`DEF_A` vs `DEF_B`)
- **Status:** **OPEN** (DRAFT recommendation: `DEF_A`)
- **Options:**
  - **`DEF_A` (Bank Miss Loss + Row-Level False Alarms):**
    $$L_A(\tau) = \sum_{i \in \text{Missed Banks}} \text{COST}_i + C_{FP} \sum_{(i,q): y_{i,q}=0} \mathbb{I}(\hat{P}_{i,q} \ge \tau)$$
    - *Semantics:* A failed bank is caught once if any positive-horizon quarter is flagged ($\ge \tau$). Every flagged quarter for surviving institutions incurs $C_{FP}$ inspection cost.
    - *Supervisory Justification:* Supervisors re-inspect flagged banks quarterly; false alarm costs accrue per quarterly alert.
  - **`DEF_B` (Bank Miss Loss + Pure Negative Bank False Alarms):**
    $$L_B(\tau) = \sum_{i \in \text{Missed Banks}} \text{COST}_i + C_{FP} \sum_{j \in \text{Pure Negative Banks}} \mathbb{I}\left(\max_q \hat{P}_{j,q} \ge \tau\right)$$
    - *Semantics:* Penalizes surviving banks only once if flagged at least once across the entire multi-year development window.
    - *Tradeoff:* Treats surviving banks symmetrically with failed banks, but ignores repeat quarterly supervisory overhead.
- **Decision Required:** Choose `DEF_A` or `DEF_B`.

---

### Decision 3: False-Positive Supervisory Cost Ratio ($C_{FN} : C_{FP}$)
- **Status:** **OPEN** (DRAFT recommendation: 100:1 ratio, $C_{FP} = \$59.19\text{ thousand}$)
- **Real Development Failure Resolution Cost:**
  - Median non-null failure cost: **\$5,919.23 thousand USD** (\$5.92M).
  - Mean failure cost: **\$26,134.88 thousand USD** (\$26.13M).
- **Options:**
  - **10:1 ($C_{FP} = \$591.92\text{ thousand}$):** Conservative; RF catches 61/100 banks ($\tau^* = 0.5106$).
  - **50:1 ($C_{FP} = \$118.38\text{ thousand}$):** Moderate; RF catches 82/100 banks ($\tau^* = 0.1181$).
  - **100:1 ($C_{FP} = \$59.19\text{ thousand}$):** Primary supervisory assumption (100x cost ratio); for Variant 3 (12 features), RF catches **87/100 banks** ($\tau_{\text{V3}}^* = 0.103990$, expected development loss **\$1,061,061.21k**) with an alarm budget of **58.3 banks/quarter** (0.569% of operating banks). (For Variant 1 [14 features], $\tau_{\text{V1}}^* = 0.037624$, catching 92/100 banks at 180.1 alarms/quarter).
  - **500:1 ($C_{FP} = \$11.84\text{ thousand}$):** High prevention sensitivity.
  - **1000:1 ($C_{FP} = \$5.92\text{ thousand}$):** Maximum alarm sensitivity.
- **Decision Required:** Choose primary cost ratio ($C_{FN} : C_{FP}$) for the test set evaluation.

---

### Decision 4: Feature Set Variant (RBCRWAJ Handling)
- **Status:** **RECONCILED BY PREREGISTERED RULE** (Selected: **Variant 3 (`variant_3`, 12 features)**)
- **Bootstrap 95% Confidence Intervals (Step 3, 1000 Resamples, `bootstrap_evaluation.csv` & `bootstrap_paired_differences.csv`):**
  - **Variant 1 (All 14 CAMELS features):**
    - RF PR-AUC: **0.2220 [0.1531, 0.2959]**, ROC-AUC: 0.9304 [0.8913, 0.9648], Top 1% Alarm Recall: 71.44%.
  - **Variant 2 (Without `RBCRWAJ` - 13 features):**
    - RF PR-AUC: **0.2239 [0.1555, 0.2967]**, ROC-AUC: 0.9200 [0.8817, 0.9532], Top 1% Alarm Recall: 69.91%.
    - Paired difference vs Variant 1: $\Delta = -0.0020$ [-0.0168, +0.0137] (**Includes 0 — statistically indistinguishable**).
  - **Variant 3 (Without `RBCRWAJ` & `RBC1AAJ` - 12 features):**
    - RF PR-AUC: **0.2436 [0.1723, 0.3221]**, ROC-AUC: 0.9399 [0.9081, 0.9681], Top 1% Alarm Recall: 69.35%.
    - Paired difference (V1 vs V3): $\Delta = -0.0216$ [-0.0409, -0.0008] (Excludes 0; V3 statistically outperforms V1).
    - Paired difference (V2 vs V3): $\Delta = -0.0196$ [-0.0363, -0.0008] (Excludes 0; V3 statistically outperforms V2).
- **Rule Verdict:** Under the preregistered parsimony rule (*"Choose the simplest feature variant whose development PR-AUC paired difference against the best variant has a 95% bank-bootstrap interval that includes zero"*), **Variant 3 (12 features)** is both the simplest and the highest-performing variant.

---

### Decision 5: Primary Model Family
- **Status:** **OPEN** (DRAFT recommendation: Random Forest for score, Logistic Regression for linear interpretation)
- **Bootstrap Paired Difference Comparison:**
  - **Random Forest vs Logistic Regression:**
    - $\Delta \text{PR-AUC} = \mathbf{+0.0904}\ [0.0470, 0.1381]$ (**Excludes 0 — RF statistically significantly outperforms LR by +68.7% relative gain**).
    - $\Delta \text{ROC-AUC} = +0.0244\ [-0.0127, 0.0639]$ (Includes 0).
  - **Loss Comparison under DEF_A (100:1 ratio):**
    - Random Forest: Total Expected Loss = **\$682,044.97 thousand USD** (catches 92/100 banks).
    - Logistic Regression: Total Expected Loss = **\$1,107,412.01 thousand USD** (catches 84/100 banks).
    - Capital Ratio Rule: Total Expected Loss = **\$1,364,817.49 thousand USD** (catches 70/100 banks).
    - Prior Baseline: Total Expected Loss = **\$2,593,272.05 thousand USD** (catches 0/100 banks).
- **Decision Required:** Select primary model family for final scorecard.

---

### Freeze Configuration Execution
When ready, run:
```powershell
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
```
