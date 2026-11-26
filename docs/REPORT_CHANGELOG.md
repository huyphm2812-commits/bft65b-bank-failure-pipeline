# BFT65B Final Report & Appendix Diff Summary / Changelog (9-Task Final Verification Pass)

**Date:** 2026-11-25  
**Protected Artifacts:** Zero changes to models, thresholds, `frozen_model_config.json`, `src/`, test predictions, or evaluation locks (`--evaluate-test` was NOT rerun; zero new test-set analyses). Every new number is backed by a saved read-only/dev-only artifact and cited in `REPORT_NUMBER_AUDIT.csv`.

---

## Diff Summary by Priority Task (Tasks 1–9 + Layout Fixes)

1. **Regenerated Figure 1.1 from Isolated Benchmark (`p4_storage_benchmark_isolated_read_times.png`):**
   - Regenerated Figure 1.1 directly from `p4_storage_benchmark_isolated.csv` (exact 12 read rows, exact medians from `0.036 s` to `5.944 s`, updated axis label `"Isolated-Subprocess Median Read Time (seconds, warm-only cache)"` and caption matching Table 1.3).

2. **Exact Table 3.1 Workload Scale-Up (`1×–5×`, Matched 4 Threads & 4,096 MiB Cap — `p5_p6_exact_scaleup.csv/.json`):**
   - Replaced the light-workload scale-up Table 3.2 with the exact 33-column Table 3.1 workload (scan 238 files, cast/filter, `asset_to_deposit`, sort `(CERT, REPDTE)`, gap-aware 1Q lag, 4-col `modeling_panel` left-join, full `.collect()`/`.count()`, and 170-quarter summary) across `1×, 2×, 3×, 5×` (`1.68M` to `8.41M` rows) under a matched `4`-thread, `4,096 MiB` (`4.0 GiB`) cap.
   - Polars-vs-Spark speed ratios reported at every scale: **`22.40× / 33.42×`** (`1×`), **`6.10× / 14.14×`** (`2×`), **`6.42× / 11.82×`** (`3×`), **`4.57× / 8.60×`** (`5×`) for Eager / Streaming vs. `Spark local[4]`.
   - Measured `4.0 GiB` memory cap crossover: **Polars Streaming** first exceeds `4,096 MiB` at **`3×` (`5.05M` rows; `4,665.34 MiB`, reaching `7,272.41 MiB` at `5×`)**; **Polars Eager** first exceeds `4,096 MiB` at **`5×` (`8.41M` rows; `6,311.27 MiB`)**; **Spark `local[4]` (`spark.driver.memory=4g`)** stays flat at **`2,063–2,098 MiB`** across `1×–5×` via local disk spill (`87.18 s` at `5×`).
   - Added explicit clarification that local Spark is a single-machine JVM (not a multi-node cluster), explained why Table 3.2 cold `1×` timings differ from Tables 3.1/3.3 warmed medians, and unified crossover wording identically across Sec 3.2, Sec 3.4, Q1 Callout, and Sec 6.1.

3. **Post-Freeze Disclosure Sentence:**
   - Added an explicit disclosure sentence in **Section 2.3**, **Section 4.2**, and **Appendix E** stating that the ablation, winsorization, CPI, MLlib, and lag analyses were run after the frozen held-out test evaluation, are strictly dev-only or read-only descriptive, and changed no frozen choice.

4. **Factual Slips, Parquet RSS Explanation, Year-Partitioned Snappy Re-Justification & Bootstrap Parity:**
   - Fixed Sec 1.3 `"fastest write/read"` slip: Single-file Zstd is smallest (`143.93 MiB`), Single-file Snappy is fastest write (`1.63 s`) and 5-column projection (`0.036 s`), and Year-Partitioned Zstd edges Single-file Zstd on full read (`0.313 s` vs. `0.315 s`).
   - Explained why Parquet full-read peak RSS (`1,356–1,845 MiB`) exceeds CSV (`930 MiB`) (multi-threaded Arrow buffers + concurrent `.to_pandas()` copy) and re-justified Year-Partitioned Snappy over Single-file Snappy using exact Table 1.3 numbers (wins date pruning `0.050 s` / `385.16 MiB` vs. `0.074 s` / `606.85 MiB` + append-only Hive ingestion, trading off `+43.66 MiB` disk and `0.055 vs. 0.036 s` projection).
   - Clarified Table 1.3 footnote: `~17.7×` `tracemalloc` hook overhead applied to plain CSV write (`421.00/23.75 s`), vs. `~3.1×` for Gzip CSV (`533.00/169.84 s`).
   - Eliminated the `57×` slip in Sec 3.2 (replaced by exact Table 3.2 ratios) and fixed Table 5.3 `10:1` RF alarms/Q slip (`6.9 / Q -> 7.1 / Q`, matching `p7_cost_threshold_comprehensive.csv` line 32).
   - Reconciled Table 4.1 vs. Table E.2 Variant-3 bootstrap mismatch using `dev_analysis.py`'s exact PRNG stream (`0.2436 [0.1723, 0.3221]` identical in both tables).

5. **Fixed-Absolute-$C_{FP}$ CPI Sensitivity (Dev-Only — `p7_cpi_fixed_cfp_sensitivity_dev.json`):**
   - Replaced proportional CPI scaling in Sec 5.1: holding $C_{FP}$ fixed at `$59.19k` while deflating dev `COST` to constant 2005/2024 USD (effective ratios `127.7:1` / `205.1:1`) shifts dev RF $\tau^*$ downward `0.103990 -> 0.025405` (`93/100`, `346.9/Q`) and shifts dev LR in 2024 USD `0.601878 -> 0.377848` (`90/100`, `100.5/Q`). Anchoring $C_{FP}$ instead to matched real FDIC full-exam averages (`$113.62k` in 2005 USD / `$182.49k` in 2024 USD; effective `66.5:1`) nudges dev RF slightly to `0.108840` (`85/100`, `54.8/Q`) while dev LR stays `0.601878`. Frozen test outputs remain unchanged.

6. **Tightened $C_{FP}$ Derivation & Primary FDIC Citations:**
   - Replaced generic `fdic.gov` with *Memorandum on the Proposed 2024 FDIC Operating Budget* (FDIC, 2023), *FDIC 2023 Annual Report* (FDIC, 2024), and *FDIC 2024 Annual Report* (FDIC, 2025).
   - Reported exact exam counts (`1,247` RMS + `2,753` Specialty = `4,000` in 2023; `1,200` RMS + `2,683` Specialty = `3,883` in 2024), exact arithmetic (`$641.88M / 4,000 = $160.47k` in 2023 USD; `$708.60M / 3,883 = $182.49k` in 2024 USD = `$113.62k` in 2005 USD), and explicitly labeled the `30%–55%` targeted follow-up cost share as an illustrative supervisory assumption.

7. **Removed Headline Ratios & Hypothesis Framing:**
   - Removed headline `64.6×` (end-to-end MLlib) and `204.6×` (full Spark CLI vs. Polars core) ratios; kept fit-only `2.1×–2.8×` (`10.50 s` / `8.09 s` vs. `3.77 s`).
   - Framed the winsorization mechanism in Sec 4.2 explicitly as a hypothesis (not tested causally).

8. **Dev-Only 1-Quarter Lagged-Feature Refit & Multiple-Comparisons Note (`p7_lag1q_dev_refit_sensitivity.json`, Table E.2):**
   - Added dev-only 1-quarter lagged-feature refit (`q-1` predicting `(q, q+4Q]`, guaranteeing $\ge 90\text{d}$ lead): RF dev PR-AUC `0.1276` (ROC `0.9197`, `85.00% [85/100]` capture, `137.0/Q`) and LR dev PR-AUC `0.0820` (ROC `0.8952`, `80.00% [80/100]`, `72.4/Q`), reported right next to the positive-exclusion result (`0.1985`, `82.11% [78/95]`).
   - Added Bonferroni-adjusted 99% CIs ($\alpha = 0.05/5 = 0.01$) and a multiple-comparisons caveat to Appendix Table E.2: Capital's unadjusted 95% CI (`[−0.0702, −0.0150]`) excludes zero, while its Bonferroni 99% CI (`[−0.0802, +0.0011]`) narrowly straddles zero.

9. **Fresh-Clone Reproduction Mode Test & README Update:**
   - Executed `pytest -q` and `powershell -ExecutionPolicy Bypass -File .\scripts\run_smoke.ps1` on a fresh isolated clone (no `02_local_workspace/`). Caught and synced a stale pre-Variant-3 copy of `p7_cost_threshold_comprehensive.csv` in `01_github_repo/results_snapshot/07_features_models/` (copying the exact generated Variant-3 CSV from `03_report_inputs/`). Re-verified fresh clone: **`pytest -q` -> `42 passed, 2 skipped` (`21.65 s`)**; **`run_smoke.ps1` -> exit `0` (`76.99 s`, `143` workspace files)**; deleted temporary clone directory and updated `01_github_repo/README.md`.

10. **Layout & Pagination Fixes (`build_pdf.py`, `report_data.py`):**
    - Wrapped Appendix H1 + HR + first paragraph in `KeepTogether` in `build_pdf.py` to eliminate the orphaned `Appendix D` heading.
    - Adjusted figure widths (`Figure 1.1` `5.2 in`, `Figure 2.1` `5.2 in`, `Figure 5.1` `4.9 in`, `Figure B.1` `4.9 in`, `Figure E.1` `4.7 in`) to reclaim sparse trailing whitespace and keep Main Report $\le 20$ pages and Appendix $\le 10$ pages.
