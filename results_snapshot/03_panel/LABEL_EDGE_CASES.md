# Part 1: Label Edge Cases Audit & Event-Level Analysis

## Overview
This document evaluates the edge cases in failure event linkage, duplicate failure records, post-failure reporting, and the impact of switching from the current bank-level failure rule ($\min(\text{FAILDATE})$) to an event-level failure assignment rule.

---

## 1. CERTs with Two or More `RESTYPE=FAILURE` Records

Across the entire FDIC Failures table (4,117 total records, 3,524 explicit `RESTYPE=FAILURE` records), exactly **3 CERTs** have two distinct `RESTYPE=FAILURE` events. All three were thrift institutions enrolled in the FHLBB / FSLIC Management Consignment Program (MCP) during the 1980s S&L crisis: they were initially placed into conservatorship/receivership ($F_1$), operated under interim management while continuing to file quarterly Call Reports, and were subsequently resolved through a final receivership/transfer ($F_2$).

| CERT | Institution Name | First Failure ($F_1$) | Second Failure ($F_2$) | First REPDTE | Last REPDTE | Reports Between $F_1$ & $F_2$? | Inter-Failure Reports Count |
| :---: | :--- | :---: | :---: | :---: | :---: | :---: | :---: |
| **29400** | Platte Valley Federal S&LA | 1988-07-21 | 1989-02-17 | 1984-03-31 | 1989-06-30 | **Yes** | **2 quarters** |
| **31927** | Westwood Savings and Loan Assoc. | 1986-03-27 | 1989-02-17 | 1984-03-31 | 1990-06-30 | **Yes** | **12 quarters** |
| **32059** | Gateway Savings Bank | 1986-04-14 | 1989-04-06 | 1984-03-31 | 1989-06-30 | **Yes** | **12 quarters** |

### Historical Event Details
1. **CERT 29400 (Platte Valley Federal S&LA, Gering, NE):**
   - $F_1 = \text{1988-07-21}$: Initial failure/conservatorship (COST = missing).
   - $F_2 = \text{1989-02-17}$: Final resolution under FSLIC (COST = \$181,099.39k).
   - Reports filed between $F_1$ and $F_2$: 1988-09-30, 1988-12-31 (both within 4 quarters of $F_2$).
2. **CERT 31927 (Westwood Savings and Loan Assoc., Los Angeles, CA):**
   - $F_1 = \text{1986-03-27}$: Initial conservatorship (COST = missing).
   - $F_2 = \text{1989-02-17}$: Final resolution/closure (COST = \$236,661.11k).
   - Reports filed between $F_1$ and $F_2$: 12 quarters (1986-03-31 to 1988-12-31).
3. **CERT 32059 (Gateway Savings Bank, Oakland, CA):**
   - $F_1 = \text{1986-04-14}$: Initial conservatorship (COST = missing).
   - $F_2 = \text{1989-04-06}$: Final resolution/closure (COST = \$81,304.51k).
   - Reports filed between $F_1$ and $F_2$: 12 quarters (1986-06-30 to 1989-03-31).

---

## 2. Top 10 CERTs by Quarters Filed After `FAILDATE`

Across the entire dataset, 270 distinct CERTs have reports with report date $q \ge \text{FAILDATE}$. The top 10 CERTs account for 209 (17.3%) of all 1,211 post-failure quarterly reports.

| Rank | CERT | Institution Name | RESTYPE | Failure Date ($F_1$) | Last REPDTE | Post-Failure Quarters | Second Failure Record? |
| :---: | :---: | :--- | :---: | :---: | :---: | :---: | :---: |
| 1 | **28244** | Talman Home Federal S&LA | FAILURE | 1982-02-20 | 1999-12-31 | **64** | No |
| 2 | **28678** | First Federal S&LA of Rochester | FAILURE | 1981-09-08 | 1996-12-31 | **52** | No |
| 3 | **31927** | Westwood Savings and Loan Assoc. | FAILURE | 1986-03-27 | 1990-06-30 | **18** | **Yes** (1989-02-17) |
| 4 | **32059** | Gateway Savings Bank | FAILURE | 1986-04-14 | 1989-06-30 | **13** | **Yes** (1989-04-06) |
| 5 | **32205** | Manhattan Beach S&LA | FAILURE | 1986-01-09 | 1988-12-31 | **12** | No |
| 6 | **29204** | Liberty County Federal S&LA | FAILURE | 1989-03-09 | 1991-06-30 | **10** | No |
| 7 | **32616** | Continental Savings, A FS&LA | FAILURE | 1989-03-09 | 1991-06-30 | **10** | No |
| 8 | **30311** | Franklin Savings Association | FAILURE | 1990-02-16 | 1992-06-30 | **10** | No |
| 9 | **29267** | Jasper Federal S&LA | FAILURE | 1989-03-16 | 1991-03-31 | **9** | No |
| 10 | **32006** | American Diversified Savings Bank | FAILURE | 1986-02-14 | 1988-03-31 | **9** | No |

---

## 3. Post-Failure Quarters: Within 1 Quarter vs. More

There are exactly **1,211** total post-failure quarterly records in the panel (assigned `label_status = 'post_failure_report'`).

Evaluating the distance between report date $q$ and failure date $F_1$:

### By Calendar Quarter Difference ($\Delta Q = Q_{\text{REPDTE}} - Q_{\text{FAILDATE}}$):
- **Within 1 quarter ($\Delta Q \le 1$):** **496 rows** (41.0%)
  - Same quarter of failure ($\Delta Q = 0$): 268 rows
  - Exactly 1 quarter following failure ($\Delta Q = 1$): 228 rows
- **More than 1 quarter ($\Delta Q > 1$):** **715 rows** (59.0%)
  - $\Delta Q = 2$: 179 rows
  - $\Delta Q = 3$: 162 rows
  - $\Delta Q = 4$: 122 rows
  - $\Delta Q \ge 5$: 252 rows

### By Calendar Days ($d = \text{REPDTE} - \text{FAILDATE}$):
- **Within 92 days ($d \le 92$):** **278 rows** (23.0%)
- **More than 92 days ($d > 92$):** **933 rows** (77.0%)

*Interpretation:* Approximately 41% of post-failure filings occur during the immediate administrative transition/closing quarter ($\Delta Q \le 1$), while 59% represent prolonged conservatorship reporting or ongoing receivership estate administration extending across multiple quarters.

---

## 4. Decision: Would Any Label Change Under an Event-Level Rule?

### The Candidate Event-Level Rule
> *"Assign positive label ($1$) if ANY failure event falls in $(q, q+4]$; censor a report only after the LAST failure of that CERT unless the bank has no later events and no reports in between."*

### Current Implementation vs. Candidate Rule
- **Current Pipeline Implementation (`src/bankfail/panel/builder.py:75-78`):**
  Aggregates failures by CERT taking $\min(\text{FAILDATE})$. Any quarterly report where $q \ge \min(\text{FAILDATE})$ is censored as `post_failure_report` (`label = <NA>`).
- **Candidate Event-Level Rule:**
  Allows reports between $F_1$ and $F_2$ to be evaluated against $F_2$. If $F_2 \in (q, q+4]$, the observation receives a positive label ($1$).

### Quantitative Impact: Exactly 10 Rows Change to Positive (`label = 1`)
Under the candidate rule, exactly **10 quarterly reports** across the 3 repeated-failure CERTs change from `<NA>` (`post_failure_report`) to `1` (`positive_failure_in_forward_window`):

| Row # | CERT | Institution Name | REPDTE ($q$) | 4-Quarter Horizon ($q+4$) | Current Label | Current Status | New Label | New Status | Triggering Failure ($F_2$) |
| :---: | :---: | :--- | :---: | :---: | :---: | :---: | :---: | :---: | :---: |
| 1 | **29400** | Platte Valley Federal S&LA | 1988-09-30 | 1989-09-30 | `<NA>` | `post_failure_report` | **`1`** | `positive` | 1989-02-17 |
| 2 | **29400** | Platte Valley Federal S&LA | 1988-12-31 | 1989-12-31 | `<NA>` | `post_failure_report` | **`1`** | `positive` | 1989-02-17 |
| 3 | **31927** | Westwood Savings and Loan Assoc. | 1988-03-31 | 1989-03-31 | `<NA>` | `post_failure_report` | **`1`** | `positive` | 1989-02-17 |
| 4 | **31927** | Westwood Savings and Loan Assoc. | 1988-06-30 | 1989-06-30 | `<NA>` | `post_failure_report` | **`1`** | `positive` | 1989-02-17 |
| 5 | **31927** | Westwood Savings and Loan Assoc. | 1988-09-30 | 1989-09-30 | `<NA>` | `post_failure_report` | **`1`** | `positive` | 1989-02-17 |
| 6 | **31927** | Westwood Savings and Loan Assoc. | 1988-12-31 | 1989-12-31 | `<NA>` | `post_failure_report` | **`1`** | `positive` | 1989-02-17 |
| 7 | **32059** | Gateway Savings Bank | 1988-06-30 | 1989-06-30 | `<NA>` | `post_failure_report` | **`1`** | `positive` | 1989-04-06 |
| 8 | **32059** | Gateway Savings Bank | 1988-09-30 | 1989-09-30 | `<NA>` | `post_failure_report` | **`1`** | `positive` | 1989-04-06 |
| 9 | **32059** | Gateway Savings Bank | 1988-12-31 | 1989-12-31 | `<NA>` | `post_failure_report` | **`1`** | `positive` | 1989-04-06 |
| 10 | **32059** | Gateway Savings Bank | 1989-03-31 | 1990-03-31 | `<NA>` | `post_failure_report` | **`1`** | `positive` | 1989-04-06 |

### Intermediate Observations ($F_1 < q \le F_2 - 4Q$): Exactly 16 Rows
Additionally, there are **16 quarters** filed by CERT 31927 (8 quarters: 1986-03-31 to 1987-12-31) and CERT 32059 (8 quarters: 1986-06-30 to 1988-03-31) that occurred more than 4 quarters before $F_2$.
- Under the current pipeline, these 16 rows are censored as `post_failure_report`.
- Under an unrestricted event-level rule that only censors after $F_{\text{last}}$, these rows would have complete future reports and would become **negative** observations ($0$). However, because these institutions were operating under conservatorship, treating them as healthy solvent negatives would introduce artificial distortions.

---

## 5. Conclusion & Mandatory Stop
Because the event-level rule changes labels by **10 rows** ($> 0$), execution is halted per prompt instructions for user review and approval before any panel modifications or subsequent modeling steps are run.
