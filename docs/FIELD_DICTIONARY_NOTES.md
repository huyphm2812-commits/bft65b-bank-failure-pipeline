# FDIC BankFind Suite Field Dictionary Notes

**Source File:** `https://api.fdic.gov/banks/docs/risview_properties.yaml` (Downloaded & Verified)  
**Endpoint:** FDIC Financials / RISVIEW  
**Scope:** Investigation of 10 key income statement, capital, and asset quality variables for quarterly modeling suitability.

---

## Field Summary Table

| Field Name | Quoted Title & Description | Temporal Type (YTD vs Quarterly) | Metric Type (Ratio vs Amount) | Modeling Handling & Feature Implication |
|:---|:---|:---:|:---:|:---|
| **NETINC** | **Title:** `Net income`<br>**Description:** *"Net interest income plus total noninterest income plus realized gains (losses) on securities and extraordinary items, less total noninterest expense, loan loss provisions and income taxes."* | **Year-to-Date (YTD)** (Cumulative from Jan 1) | **Amount** (Thousands of USD) | **Excluded from primary model.** Used in provisional features `net_income_to_asset` and `netinc_change_qoq`. If retained as an income metric, it **must be de-cumulated** within each calendar year ($Q_1 = YTD_{Q1}$, $Q_k = YTD_{Qk} - YTD_{Qk-1}$ for $k \in \{2,3,4\}$) to eliminate artificial 4x seasonal scaling. Replaced in baseline by annualized `ROA`. |
| **RBC1AAJ** | **Title:** `LEVERAGE RATIO-PCA`<br>**Description:** *(None in schema; source mapping formula: `percentage (numerator: RBCT1, denominator: AVASSETJ)`)* | **Quarterly** (Point-in-Time at Quarter End) | **Ratio** (Percent) | **Included as primary feature** `rbc1a_level`. Represents Tier 1 leverage capital divided by average tangible assets for Prompt Corrective Action (PCA). Point-in-time balance sheet ratio; no de-cumulation needed. |
| **RBCRWAJ** | **Title:** `TOTAL RBC RATIO-PCA`<br>**Description:** *(None in schema; source mapping formula: `percentage (numerator: RBC, denominator: RWAJT)`)* | **Quarterly** (Point-in-Time at Quarter End) | **Ratio** (Percent) | **Included as primary feature** `rbcrwa_level`. Total Risk-Based Capital / Risk-Weighted Assets under PCA. Point-in-time balance sheet ratio; no de-cumulation needed.<br>*(Note: Structurally constant 0.0 across 1984–1989 prior to Basel I implementation in 1990; flagged in data audit).* |
| **INTINC** | **Title:** `TOTAL INTEREST INCOME`<br>**Description:** *(None in schema; maps to Schedule RI `INTINC`)* | **Year-to-Date (YTD)** (Cumulative from Jan 1) | **Amount** (Thousands of USD) | **Not used in primary features.** Income statement flow variable. If used to derive quarterly yield or interest margin, it **must be de-cumulated** across quarters. Annualized net interest margin is already captured by `nimy_level` (from `NIMY`). |
| **EINTEXP** | **Title:** `TOTAL INTEREST EXPENSE`<br>**Description:** *(None in schema; maps to Schedule RI `EINTEXP`)* | **Year-to-Date (YTD)** (Cumulative from Jan 1) | **Amount** (Thousands of USD) | **Not used in primary features.** Income statement flow variable. If used to measure cost of funding, it **must be de-cumulated** across quarters. |
| **NONII** | **Title:** `TOTAL NONINTEREST INCOME`<br>**Description:** *(None in schema; maps to Schedule RI `NONII`)* | **Year-to-Date (YTD)** (Cumulative from Jan 1) | **Amount** (Thousands of USD) | **Not used in primary features.** Income statement flow variable (service fees, trading revenue). If used to measure revenue diversification, it **must be de-cumulated** across quarters. |
| **NONIX** | **Title:** `TOTAL NONINTEREST EXPENSE`<br>**Description:** *(None in schema; maps to Schedule RI `NONIX`)* | **Year-to-Date (YTD)** (Cumulative from Jan 1) | **Amount** (Thousands of USD) | **Not used in primary features.** Income statement flow variable (overhead, salaries, premises). If used to construct efficiency ratios, it **must be de-cumulated** across quarters. |
| **ELNATR** | **Title:** `PROVISIONS FOR CREDIT LOSSES`<br>**Description:** *(None in schema; maps to Schedule RI `ELNATR`)* | **Year-to-Date (YTD)** (Cumulative from Jan 1) | **Amount** (Thousands of USD) | **Not used in primary features.** Income statement expense allocated to loan loss reserves. If used to model credit distress anticipation, it **must be de-cumulated** across quarters. Cumulative reserve stock is already captured by balance sheet stock `LNATRES` (Allowance for Loan and Lease Losses). |
| **NTLNLS** | **Title:** `TOTAL LN&LS NET CHARGE-OFFS`<br>**Description:** *(None in schema; maps to Schedule RI-B `NTLNLS`)* | **Year-to-Date (YTD)** (Cumulative from Jan 1) | **Amount** (Thousands of USD) | **Not used in primary features.** Net charge-offs = Gross charge-offs − Recoveries. Cumulative flow across calendar quarters. If modeled, it **must be de-cumulated** across quarters.<br>*(Note: Can legitimately be negative when recoveries exceed gross charge-offs, as verified in Part 2 audit; 299,759 negative rows observed).* |
| **ROA** | **Title:** `Return on assets (ROA)`<br>**Description:** *"Net income after taxes and extraordinary items (annualized) as a percent of average total assets."*<br>*(Formula: `percentage (numerator: NETINCA, denominator: ASSET5)`)* | **Quarterly** (Annualized for the quarter by FDIC) | **Ratio** (Percent, Annualized) | **Included as primary features** `roa_level` and `roa_rolling_4q_mean`. Because the FDIC already normalizes `NETINC` to an annualized rate (`NETINCA`) over average quarterly assets (`ASSET5`), `ROA` requires **no de-cumulation** and cleanly captures quarterly profitability without calendar-quarter seasonality. |

---

## Detailed Methodological Assessment: De-cumulation vs. Exclusion

### 1. The Call Report YTD Accumulation Problem
Under FFIEC Call Report instructions (Schedule RI - Income Statement and Schedule RI-B - Charge-offs), depository institutions report income and expenses cumulatively starting January 1 of each calendar year:
* **Q1 (March 31):** Reflects 3 months of activity.
* **Q2 (June 30):** Reflects 6 months of activity (cumulative $Q1 + Q2$).
* **Q3 (September 30):** Reflects 9 months of activity (cumulative $Q1 + Q2 + Q3$).
* **Q4 (December 31):** Reflects 12 months of activity (full year cumulative).

### 2. Modeling Impact on Provisional Features
If raw cumulative amounts are divided by contemporaneous point-in-time assets (`NETINC / ASSET`), an artificial quarterly progression is introduced:
$$\text{Raw Ratio}_{Q4} \approx 4 \times \text{Raw Ratio}_{Q1}$$
This distorts machine learning models (e.g. tree splits or logistic regression coefficients), creating spurious seasonality correlated with calendar quarter rather than actual bank solvency.

### 3. Pipeline Decisions & Remediation
1. **`NETINC`:** Provisional features `net_income_to_asset` and `netinc_change_qoq` are **strictly excluded from `FEATURE_COLUMNS`** in `pipeline.py`.
2. **Profitability Proxy:** The primary model uses `roa_level` (from `ROA`), which is pre-annualized by FDIC using `NETINCA / ASSET5`, and `roa_rolling_4q_mean` (a 4-quarter rolling average of non-overlapping quarters), eliminating the need for complex flow de-cumulation.
3. **Flow Variables (`INTINC`, `EINTEXP`, `NONII`, `NONIX`, `ELNATR`, `NTLNLS`):** Kept in raw and processed tables for auditing and accounting verification. If future extensions incorporate them into features, they must be converted to discrete quarterly flows via:
   $$\Delta X_{i,q} = \begin{cases} X_{i,q} & \text{if } q = Q1 \\ X_{i,q} - X_{i,q-1} & \text{if } q \in \{Q2, Q3, Q4\} \end{cases}$$
   provided that institution $i$ was continuously active without mid-year merger restructuring.
