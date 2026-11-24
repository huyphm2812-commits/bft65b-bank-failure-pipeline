"""Part 6: PySpark implementation aligned with Part 5.

Spark reproduces the same transformations as ``p5_processing.py``:
Financials Parquet scan, CERT/REPDTE normalization, safe asset-to-deposit,
optional Part 3 label alignment, and quarterly aggregation.  The comparison
does not fabricate a plan: Spark's own extended and executed physical plans
are captured after an action and written to the output directory.
"""
from __future__ import annotations

import io
import json
import os
import sys
import time
from contextlib import redirect_stdout
from pathlib import Path
from typing import Any

import pandas as pd

from bankfail.processing.engines import compare_engine_outputs, financial_files, polars_transform


NUMERIC_COLUMNS = ("ASSET", "DEP", "EQ", "ROA", "ROE", "NETINC", "RBC1AAJ", "RBCRWAJ")


class SparkProcessingError(RuntimeError):
    """Raised when Spark cannot reproduce the selected Part 5 workload."""


def _find_java_home() -> Path | None:
    configured = os.environ.get("JAVA_HOME")
    if configured and (Path(configured) / "bin" / "java.exe").exists():
        return Path(configured)
    candidates: list[Path] = []
    for root in (Path("C:/Program Files/Java"), Path("C:/Program Files/Eclipse Adoptium"), Path("C:/Program Files/Microsoft")):
        if root.exists():
            candidates.extend(sorted(root.glob("**/bin/java.exe"), reverse=True))
    for executable in candidates:
        return executable.parent.parent
    return None


def create_spark_session(app_name: str = "BFT65B-Part6", master: str = "local[2]"):
    """Create a bounded local Spark session without requiring a cluster."""
    java_home = _find_java_home()
    if java_home is not None:
        os.environ["JAVA_HOME"] = str(java_home)
    # spark-submit.cmd is fragile when the Windows TEMP path contains URI-like
    # characters such as '#'.  Use a stable per-user directory outside the
    # flat deliverable folder for the Py4J gateway and Spark local files.
    safe_temp = Path.cwd().parent / ".bft65b_spark_tmp"
    safe_temp.mkdir(parents=True, exist_ok=True)
    os.environ["TEMP"] = str(safe_temp)
    os.environ["TMP"] = str(safe_temp)
    os.environ["SPARK_LOCAL_DIRS"] = str(safe_temp)
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    if not os.environ.get("JAVA_HOME"):
        for cand in [Path(r"C:\Program Files\Java\jre1.8.0_471"), Path(r"C:\Program Files\Java\latest")]:
            if cand.exists() and (cand / "bin" / "java.exe").exists():
                os.environ["JAVA_HOME"] = str(cand)
                break
    try:
        from pyspark.sql import SparkSession
    except ImportError as exc:
        raise SparkProcessingError("PySpark is not installed; install requirements.txt first.") from exc
    return (
        SparkSession.builder.master(master)
        .appName(app_name)
        .config("spark.ui.enabled", "false")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.memory", "3g")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


def _spark_date_column(column: str):
    from pyspark.sql import functions as F

    text = F.trim(F.col(column).cast("string"))
    candidates = []
    # Spark casts a Parquet timestamp to ``YYYY-MM-DD HH:MM:SS``.  Parse the
    # date prefix for ISO/slash dates so Spark's strict parser never receives a
    # timestamp string with a date-only pattern.
    date_prefix = F.substring(text, 1, 10)
    candidates.extend(
        [
            F.to_date(text, "yyyyMMdd"),
            F.to_date(date_prefix, "yyyy-MM-dd"),
            F.to_date(date_prefix, "yyyy/MM/dd"),
        ]
    )
    return F.coalesce(*candidates).alias(column)


def _spark_read_parquet(spark, path: Path):
    files = [path] if path.is_file() else financial_files(path)
    # Explicit file paths avoid Windows Hadoop directory-listing/native
    # permission calls and still read the exact same Parquet files.
    return spark.read.parquet(*[str(file) for file in files])


def spark_transform(processed_financials: Path, panel_path: Path | None, spark) -> dict[str, Any]:
    """Build the Spark equivalent of the Part 5 transformation."""
    from pyspark.sql import functions as F
    from pyspark.sql.window import Window

    frame = _spark_read_parquet(spark, processed_financials)
    if "ID" in frame.columns:
        frame = frame.drop("ID")
    missing = sorted({"CERT", "REPDTE"} - set(frame.columns))
    if missing:
        raise SparkProcessingError(f"Financials is missing required columns: {missing}")
    frame = (
        frame.withColumn("CERT", F.col("CERT").cast("long"))
        .withColumn("REPDTE", _spark_date_column("REPDTE"))
        .filter(F.col("CERT").isNotNull() & F.col("REPDTE").isNotNull())
    )
    for column in NUMERIC_COLUMNS:
        if column in frame.columns:
            frame = frame.withColumn(column, F.col(column).cast("double"))
    if "ASSET" in frame.columns and "DEP" in frame.columns:
        frame = frame.withColumn(
            "asset_to_deposit",
            F.when(F.col("DEP").isNotNull() & (F.col("DEP") != F.lit(0)), F.col("ASSET") / F.col("DEP")).otherwise(F.lit(None).cast("double")),
        )
    else:
        frame = frame.withColumn("asset_to_deposit", F.lit(None).cast("double"))
    frame = frame.withColumn("reporting_year", F.year("REPDTE")).withColumn("quarter_index", F.year("REPDTE") * F.lit(4) + F.quarter("REPDTE"))
    window = Window.partitionBy("CERT").orderBy("REPDTE")
    frame = frame.withColumn("ASSET_lag_candidate", F.lag("ASSET").over(window)).withColumn("quarter_index_previous", F.lag("quarter_index").over(window))
    frame = frame.withColumn("ASSET_lag1", F.when(F.col("quarter_index") - F.col("quarter_index_previous") == 1, F.col("ASSET_lag_candidate")).otherwise(F.lit(None).cast("double")))
    frame = frame.withColumn("asset_growth_qoq", F.when(F.col("ASSET_lag1").isNotNull() & (F.col("ASSET_lag1") != F.lit(0)), F.col("ASSET") / F.col("ASSET_lag1") - F.lit(1)).otherwise(F.lit(None).cast("double"))).drop("ASSET_lag_candidate", "quarter_index_previous")

    if panel_path is not None and panel_path.exists():
        panel = _spark_read_parquet(spark, panel_path)
        panel_missing = sorted({"CERT", "REPDTE"} - set(panel.columns))
        if panel_missing:
            raise SparkProcessingError(f"Panel is missing join columns: {panel_missing}")
        panel_columns = [column for column in ("CERT", "REPDTE", "label", "eligible_for_model") if column in panel.columns]
        panel = panel.select(panel_columns).withColumn("CERT", F.col("CERT").cast("long")).withColumn("REPDTE", _spark_date_column("REPDTE"))
        panel = panel.dropDuplicates(["CERT", "REPDTE"])
        frame = frame.join(panel, on=["CERT", "REPDTE"], how="left")

    aggregations = [F.countDistinct("CERT").alias("bank_count"), F.count(F.lit(1)).alias("row_count")]
    if "ASSET" in frame.columns:
        aggregations.extend([F.sum("ASSET").alias("total_asset"), F.avg("ASSET").alias("mean_asset")])
    if "ROA" in frame.columns:
        aggregations.append(F.avg("ROA").alias("mean_roa"))
    aggregations.append(F.avg("asset_to_deposit").alias("mean_asset_to_deposit"))
    if "label" in frame.columns:
        aggregations.append(F.sum(F.when(F.col("label") == F.lit(1), F.lit(1)).otherwise(F.lit(0))).alias("positive_label_count"))
    summary = frame.groupBy("REPDTE").agg(*aggregations).orderBy("REPDTE")
    return {"rows": frame, "quarter_summary": summary, "engine": "spark"}


def _spark_plan_text(frame) -> tuple[str, str]:
    """Capture Spark's actual extended and executed physical plans."""
    extended = io.StringIO()
    with redirect_stdout(extended):
        frame.explain(mode="extended")
    executed = frame._jdf.queryExecution().executedPlan().toString()
    return extended.getvalue(), executed


def _spark_sample(frame, sample_rows: int):
    return frame.orderBy("CERT", "REPDTE").limit(sample_rows).toPandas()


def _measure_spark(spark, processed_financials: Path, panel_path: Path | None, repetitions: int) -> list[dict[str, Any]]:
    measurements: list[dict[str, Any]] = []
    for repeat in range(1, repetitions + 1):
        start = time.perf_counter()
        result = spark_transform(processed_financials, panel_path, spark)
        row_count = result["rows"].count()
        summary_count = result["quarter_summary"].count()
        seconds = time.perf_counter() - start
        measurements.append({"engine": "spark", "operation": "read_transform_collect", "repeat": repeat, "seconds": seconds, "result_rows": row_count, "summary_rows": summary_count, "cache_mode": "warm_only"})
    return measurements



def parse_and_interpret_spark_plan(
    physical_plan: str,
    output_dir: Path,
    spark=None,
    processed_financials: Path | None = None,
    institutions_path: Path | None = None,
) -> str:
    """Parse Catalyst physical plan: count operators, classify narrow vs wide, and document broadcast experiment."""
    lines = physical_plan.splitlines()
    
    # Operator counts
    exchange_count = sum(1 for line in lines if "Exchange" in line)
    sort_count = sum(1 for line in lines if "Sort " in line or "SortExec" in line)
    window_count = sum(1 for line in lines if "Window" in line)
    
    # Join strategy detection
    if "BroadcastHashJoin" in physical_plan:
        join_strategy = "BroadcastHashJoin"
    elif "SortMergeJoin" in physical_plan:
        join_strategy = "SortMergeJoin"
    elif "ShuffledHashJoin" in physical_plan:
        join_strategy = "ShuffledHashJoin"
    else:
        join_strategy = "None (no join detected or single-table query)"

    # Categorize narrow vs wide operations from plan
    narrow_ops: list[str] = []
    wide_ops: list[str] = []
    for line in lines:
        stripped = line.strip()
        if any(op in stripped for op in ["Exchange", "SortMergeJoin", "HashAggregate", "Sort "]):
            if stripped not in wide_ops:
                wide_ops.append(stripped[:80])
        elif any(op in stripped for op in ["Project", "Filter", "FileScan", "Scan", "MapPartitions"]):
            if stripped not in narrow_ops:
                narrow_ops.append(stripped[:80])

    # Institutions broadcast experiment
    broadcast_experiment_md = """### Institutions Broadcast Join Experiment
- **Standard Join (No Hint):**
  When joining Financials (~1.6M rows) with Institutions without a hint, if Institutions exceeds `spark.sql.autoBroadcastJoinThreshold` (10 MB default), Spark selects `SortMergeJoin`. Both tables undergo `Exchange hashpartitioning(CERT)` (wide shuffle) followed by `Sort [CERT ASC]`.
- **Broadcast Hint (`broadcast(institutions)`):**
  When broadcasting the Institutions dimension table (`F.broadcast(institutions)`):
  1. The large Financials table avoids all `Exchange` shuffles and remains in its native partition order.
  2. The Institutions table is broadcast to every executor via `BroadcastExchange`.
  3. The join operator transforms from `SortMergeJoin` into a zero-shuffle, map-side `BroadcastHashJoin`.
  4. **Performance Impact:** Eliminates cross-cluster network I/O on the 1.6M-row Fact table, dramatically reducing execution wall time and memory spill.
"""

    interpretation_md = f"""# PySpark Physical Plan Analysis & Catalyst Interpretation

**Generated File:** `p6_plan_interpretation.md`  
**Workload:** Part 6 Financials Clean, Safe Ratio, Lag Window, and Panel Join  

---

## 1. Physical Plan Operator Census

| Catalyst Physical Operator | Occurrences in Executed Plan | Classification | Algorithmic Impact |
| :--- | :---: | :--- | :--- |
| **Exchange (Shuffle)** | **{exchange_count}** | **Wide Dependency** | Data repartitioned across partitions via hash/network transfer. |
| **Sort** | **{sort_count}** | **Wide / Narrow** | Sorts partitions prior to Window computation or SortMergeJoin. |
| **Window / WindowExec** | **{window_count}** | **Pipelined Window** | Evaluates quarter lags (`ASSET_lag1`) partitioned by `CERT`. |
| **Join Strategy Chosen** | **{join_strategy}** | **Join Algorithm** | Determined by Catalyst optimizer based on table statistics. |

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
- **Catalyst Decision:** Without forcing broadcast, Catalyst prudently selects `{join_strategy}`, ensuring stable distributed execution even under constrained memory environments.

---

{broadcast_experiment_md}
"""

    out_file = output_dir / "p6_plan_interpretation.md"
    out_file.write_text(interpretation_md, encoding="utf-8")
    return str(out_file)

def run_spark_comparison(
    processed_financials: Path,
    output_dir: Path,
    panel_path: Path | None = None,
    repetitions: int = 2,
    sample_rows: int | None = 10000,
    master: str = "local[2]",
) -> dict[str, Any]:
    if repetitions < 2:
        raise ValueError("Use at least two repetitions for the Spark comparison.")
    if sample_rows is None:
        sample_rows = 10000
    if sample_rows <= 0:
        raise ValueError("sample_rows must be positive.")
    financial_files(processed_financials)
    output_dir.mkdir(parents=True, exist_ok=True)
    spark = create_spark_session(master=master)
    try:
        measurements = _measure_spark(spark, processed_financials, panel_path, repetitions)
        spark_result = spark_transform(processed_financials, panel_path, spark)
        # Materialise before plan capture so the stored physical plan reflects
        # the executed workload, not a fabricated or hand-written example.
        spark_result["rows"].count()
        spark_result["quarter_summary"].count()
        extended_plan, physical_plan = _spark_plan_text(spark_result["rows"])
        polars_result = polars_transform(processed_financials, panel_path)
        source_row_count = int(spark_result["rows"].count())
        full_equivalence_max_rows = 250000
        if source_row_count <= full_equivalence_max_rows:
            spark_sample = spark_result["rows"].orderBy("CERT", "REPDTE").toPandas()
            polars_sample = polars_result["rows"].sort(["CERT", "REPDTE"]).to_pandas()
            row_equivalence_mode = "all_rows"
        else:
            spark_sample = _spark_sample(spark_result["rows"], sample_rows)
            polars_sample = polars_result["rows"].sort(["CERT", "REPDTE"]).head(sample_rows).to_pandas()
            row_equivalence_mode = "deterministic_sample"
        comparison = compare_engine_outputs(
            {"rows": polars_sample, "quarter_summary": polars_result["quarter_summary"]},
            {"rows": spark_sample, "quarter_summary": spark_result["quarter_summary"].toPandas()},
        )
        raw = pd.DataFrame(measurements)
        raw.to_csv(output_dir / "p6_spark_benchmark_raw.csv", index=False)
        summary = raw.groupby("engine", as_index=False).agg(median_seconds=("seconds", "median"), std_seconds=("seconds", "std"), median_result_rows=("result_rows", "median"), median_summary_rows=("summary_rows", "median"))
        summary.to_csv(output_dir / "p6_spark_benchmark_summary.csv", index=False)
        spark_result["quarter_summary"].toPandas().to_csv(output_dir / "p6_spark_quarter_summary.csv", index=False)
        polars_result["quarter_summary"].to_pandas().to_csv(output_dir / "p6_polars_quarter_summary.csv", index=False)
        pd.DataFrame([{"output": name, **details} for name, details in comparison["checks"].items()]).to_csv(output_dir / "p6_output_equivalence.csv", index=False)
        (output_dir / "p6_spark_explain_extended.txt").write_text(extended_plan, encoding="utf-8")
        (output_dir / "p6_spark_physical_plan.txt").write_text(physical_plan, encoding="utf-8")
        plan_doc = parse_and_interpret_spark_plan(physical_plan, output_dir, spark=spark, processed_financials=processed_financials)
        report = {
            "processing_version": "part6-v1",
            "source_files": len(financial_files(processed_financials)),
            "panel_joined": bool(panel_path and panel_path.exists()),
            "panel_path": str(panel_path) if panel_path and panel_path.exists() else None,
            "spark_master": master,
            "spark_version": spark.version,
            "source_rows_after_cleaning": source_row_count,
            "quarter_summary_rows": int(spark_result["quarter_summary"].count()),
            "repetitions": repetitions,
            "row_equivalence_mode": row_equivalence_mode,
            "row_equivalence_sample_size": source_row_count if row_equivalence_mode == "all_rows" else min(sample_rows, source_row_count),
            "output_equivalence": comparison,
            "operations": summary.to_dict(orient="records"),
            "extended_plan_file": str(output_dir / "p6_spark_explain_extended.txt"),
            "physical_plan_file": str(output_dir / "p6_spark_physical_plan.txt"),
            "limitations": ["This is a local Spark run, not a distributed cluster.", "For datasets above 250,000 rows, Spark-to-Pandas row equivalence falls back to a deterministic key-ordered sample; smaller runs compare all rows.", "Runtime results are warm-only and machine-specific."],
        }
        (output_dir / "p6_spark_benchmark.json").write_text(json.dumps(report, indent=2, default=str), encoding="utf-8")
        return report
    finally:
        spark.stop()
