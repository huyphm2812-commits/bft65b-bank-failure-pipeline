# AI Assistance Disclosure & Verification Log

*Course:* BFT65B – Big Data Analytics in Finance  
*Institution:* National Economics University · School of Banking and Finance  
*Student Name:* Phạm Quang Huy  
*Student ID:* 11230967  
*GitHub Repository:* https://github.com/huyphm2812-commits/bft65b-bank-failure-pipeline  

---

## 1. Summary of AI Tools Used

| AI Assistant / Tool | Primary Scope of Assisted Work | Human Validation & Academic Responsibility |
| :--- | :--- | :--- |
| **Google Antigravity (Gemini AI Assistant)** | Modular package refactoring (`src/bankfail/*`), offline test-fixture authoring, benchmark orchestration, and PDF/DOCX report layout generation. | All pipeline code, pytest unit tests, saved JSON/CSV/Parquet outputs, and rendered report pages were checked directly against actual artifacts and course rubric requirements. |
| **OpenAI ChatGPT** | AI-assisted drafting, methodological review, debugging support, and documentation editing. | All project-specific technical claims, formulas, and empirical results were checked against repository code, saved outputs, and primary FDIC documentation. |
| **Anthropic Claude** | Prompt drafting, critical review of documentation consistency, and editorial feedback on academic framing. | Every flagged discrepancy was audited directly against raw files; unverified claims or deferred test analyses were removed rather than accepted. |

---

## 2. Concrete Evidence Supporting Leakage-Resistant Design

AI review and cryptographic file hashes alone do not prove the absence of data leakage. Leakage resistance in this pipeline rests on eight concrete methodological and programmatic controls verified in the repository:

1. **Chronological Train / Development / Test Split:** Effective training `1984Q1–1991Q4` (configured boundary `1992-12-31`), development `1993Q1–2005Q4`, held-out test `2008Q1–2014Q4`.
2. **Four-Quarter Label Purge:** Training rows are retained only when their 4-quarter forward label window ends strictly before `1993-03-31`.
3. **Eight-Quarter Embargo Gap:** `2006Q1–2007Q4` excluded between development and test, exceeding the 4-quarter assignment minimum.
4. **Point-in-Time Feature Construction:** Ratios, 1Q/4Q lags, and 4Q rolling means use only filings at or before quarter `q` without bridging reporting gaps.
5. **Training-Only Preprocessing:** Median imputation (`add_indicator=True`) and scaling live inside scikit-learn `Pipeline` objects fitted exclusively on `1984Q1–1991Q4`.
6. **Explicit Leakage-Variable Exclusion:** Nine outcome/exit columns (`ACTIVE`, `CHANGEC1`, `eligible_for_model`, `exit_classification`, `failure_cost_numeric`, `failure_date`, `institution_end_date`, `label`, `label_status`) are banned from model features.
7. **Automated Future-Perturbation Test:** Randomly perturbing all nine forbidden columns produced zero change in any constructed feature (`p7_model_results.json`).
8. **Frozen Configuration & Single-Use Test Lock:** Runtime SHA-256 verification enforced execution of the frozen development configuration once, immediately guarded by `p7_test_evaluation.lock`.
