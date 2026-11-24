# Academic References & Methodology Citations

This document compiles the academic literature, regulatory references, data sources, and pedagogical materials supporting the BFT65B Bank Failure Early-Warning Pipeline.

---

## 1. Regulatory & Official Data Sources

1. **Federal Deposit Insurance Corporation (FDIC) BankFind Suite API**
   * *Documentation:* https://api.fdic.gov/banks/docs/
   * *API Endpoint:* `https://api.fdic.gov/banks`
   * *Data Ingestion:* Call Reports (Financials), Failed Bank List (Failures), and Structure History (Institutions).
2. **FDIC Bulk Data Download Service**
   * *Portal:* https://banks.data.fdic.gov/bankfind-suite/bulkData
3. **FDIC Field Properties & Taxonomy Dictionary**
   * *Schema Specification:* `https://api.fdic.gov/banks/docs/risview_properties.yaml`
   * *Role:* Verification of source definitions for balance sheet items (ASSET, DEP, EQ, LNLSNET), income statements (NETINC), and supervisory ratios (ROA, ROE, NIMY, RBC1AAJ, RBCRWAJ).
4. **Course Starter Codebase**
   * *File Reference:* `fdic_download_starter.py` (BFT65B course repository).
   * *Attribution:* The client request retries, pagination loops, and raw page hashing structure in `src/bankfail/acquisition/` are derived and extended from the course starter script provided by Dr. Nguyen Tuan Long.

---

## 2. Academic Literature & Econometric Foundations

1. **CAMELS Supervisory Rating Framework**
   * Federal Financial Institutions Examination Council (FFIEC). (1997). *Uniform Financial Institutions Rating System*. Federal Register, 62(13), 2726–2736.
   * *Application:* Feature groups representing Capital Adequacy (EQ/ASSET, RBC1AAJ), Asset Quality (NCLNLS/ASSET), Management Quality, Earnings (ROA, ROE, NIMY), and Liquidity (CHBAL/ASSET).
2. **Survival Analysis & Early-Warning Models in Banking**
   * Wheelock, D. C., & Wilson, P. W. (2000). *Why do banks disappear? The determinants of U.S. bank failures and acquisitions*. Review of Economics and Statistics, 82(1), 127–138.
   * Cole, R. A., & White, L. J. (2012). *Déjà vu all over again: The causes of U.S. commercial bank failures this time around*. Journal of Financial Services Research, 42(1), 5–29.
3. **Financial Machine Learning & Leakage Prevention**
   * López de Prado, M. (2018). *Advances in Financial Machine Learning*. John Wiley & Sons.
   * *Application:* Chapter 7 (Purged and Embargoed Cross-Validation) and Chapter 8 (Feature Importance without Look-Ahead Bias).

---

## 3. Course Lectures (BFT65B – National Economics University)

* **Lecture 02 (Foundations of Data Preparation for ML):** Data preparation lifecycle, split-first golden rule, and leakage definitions.
* **Lecture 02 (Data Cleaning & Imputation):** Zero-variance filtering, duplicate key audits, IQR/std outlier detection, and leakage-safe Pipeline imputation.
* **Lecture 02 (Data Transformations):** Feature scaling properties (MinMax vs Standard vs Robust), distribution power transforms, and numerical encoding.
* **Lecture 02 (Feature Selection):** Filter, wrapper (RFE/RFECV), and embedded paradigms with zero-selection leakage.
* **Lecture 02 (Machine Learning Pipelines):** Scikit-learn Pipeline encapsulation, ColumnTransformer, expanding-window CV, and serialization with Joblib.
* **Lecture 03 (Financial Time Series ML):** Lookahead prevention, leak-free lags/rolling targets, walk-forward validation with purge and embargo, and dual-timestamp point-in-time data alignment.
* **Lecture 04–06 (Big Data, Spark & ML Essentials):** Parquet columnar storage, Medallion Lakehouse architecture, Polars lazy query optimization, Amdahl's Law, Spark Catalyst execution plans (narrow vs wide/shuffle), broadcast joins, rare-event imbalanced classification, and cost-sensitive threshold selection.
* **Lectures 11–15 (Data Visualization & Storytelling):** Grammar of graphics, distribution representations (not just means), overplotting mitigation (hexbin/density), Tufte's data-ink ratio, and Knaflic's action-oriented takeaway titling.
* **Vibe Coding Lectures (Methodology for AI-Assisted Programming):** Specification-driven development, plan-first workflow, incremental verification with Git, test-driven validation of AI outputs, and ethical AI disclosure.

---

## 4. AI Assistance Disclosure

* **Student Name:** Phạm Quang Huy (Student ID: 11230967)
* **GitHub Repository:** https://github.com/huyphm2812-commits/bft65b-bank-failure-pipeline
* **AI Tools Consulted:**
  * **Google Antigravity (Gemini AI Assistant):** Codebase modularization (`src/bankfail/`), offline test fixtures, benchmark orchestration, and PDF/DOCX layout generation.
  * **OpenAI ChatGPT:** AI-assisted drafting, methodological review, debugging support, and documentation editing.
  * **Anthropic Claude:** Prompt drafting, documentation consistency review, and editorial feedback.
* **Human Validation & Verification:**
  * All project-specific technical claims, statistical calculations, data filtering thresholds, and evaluation metrics were checked directly against repository code and saved Parquet, JSON, and CSV outputs.
  * Leakage resistance is enforced by chronological splitting, 4-quarter label purging, an 8-quarter embargo, training-only preprocessing, forbidden-column exclusion, future-perturbation testing, and a single-use frozen test lock.
  * The student assumes complete academic responsibility for all submitted code, tables, and prose.
