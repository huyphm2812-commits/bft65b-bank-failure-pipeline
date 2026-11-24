# PySpark Physical Plan Analysis & Catalyst Interpretation

**Generated File:** `p6_plan_interpretation.md`  
**Workload:** Part 6 Financials Clean, Safe Ratio, Lag Window, and Panel Join  

---

## 1. Physical Plan Operator Census

| Catalyst Physical Operator | Occurrences in Executed Plan | Classification | Algorithmic Impact |
| :--- | :---: | :--- | :--- |
| **Exchange (Shuffle)** | **3** | **Wide Dependency** | Data repartitioned across partitions via hash/network transfer. |
| **Sort** | **1** | **Wide / Narrow** | Sorts partitions prior to Window computation or SortMergeJoin. |
| **Window / WindowExec** | **1** | **Pipelined Window** | Evaluates quarter lags (`ASSET_lag1`) partitioned by `CERT`. |
| **Join Strategy Chosen** | **BroadcastHashJoin** | **Join Algorithm** | Determined by Catalyst optimizer based on table statistics. |

---

## 2. Narrow vs. Wide Dependency Breakdown

### Narrow Transformations (Pipelined, Zero Shuffle Network Transfer)
Narrow transformations do not require shuffling across cluster nodes; operations execute in-memory within the same partition:
- `FileScan parquet`: Columnar scan reading only projected analytical columns.
- `Filter (isNotNull)`: Predicate pushdown filtering invalid CERT and REPDTE rows.
- `Project`: Computing derived expressions (`asset_to_deposit`, `quarter_index`) per row.

### Wide Transformations (Shuffle Boundaries, Stage Barriers)
Wide transformations require data to be reorganized across cluster partitions:
- `Exchange hashpartitioning(CERT)`: Redistributes rows so that all observations for a given bank CERT reside on the same partition for Windowing.
- `Sort [CERT, REPDTE]`: Sorts historical quarterly reports chronologically per bank.
- `HashAggregate`: Computes quarterly summary aggregates across reporting dates.

---

## 3. Join Strategy Justification (Do NOT Force Broadcast on Panel)

- **Panel Join Policy:** The Part 3 Panel is large (~1.6M bank-quarters). Forcing `broadcast(panel)` would serialize and collect the entire multi-hundred-megabyte panel to the Spark driver and broadcast it to all executors, introducing driver Out-Of-Memory (OOM) risks and heavy memory pressure.
- **Catalyst Decision:** Without forcing broadcast, Catalyst prudently selects `BroadcastHashJoin`, ensuring stable distributed execution even under constrained memory environments.

---

### Institutions Broadcast Join Experiment
- **Standard Join (No Hint):**
  When joining Financials (~1.6M rows) with Institutions without a hint, if Institutions exceeds `spark.sql.autoBroadcastJoinThreshold` (10 MB default), Spark selects `SortMergeJoin`. Both tables undergo `Exchange hashpartitioning(CERT)` (wide shuffle) followed by `Sort [CERT ASC]`.
- **Broadcast Hint (`broadcast(institutions)`):**
  When broadcasting the Institutions dimension table (`F.broadcast(institutions)`):
  1. The large Financials table avoids all `Exchange` shuffles and remains in its native partition order.
  2. The Institutions table is broadcast to every executor via `BroadcastExchange`.
  3. The join operator transforms from `SortMergeJoin` into a zero-shuffle, map-side `BroadcastHashJoin`.
  4. **Performance Impact:** Eliminates cross-cluster network I/O on the 1.6M-row Fact table, dramatically reducing execution wall time and memory spill.

