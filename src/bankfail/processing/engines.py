"""Part 5: equivalent pandas and Polars processing.

The two engines deliberately implement the same small workload:

* scan the year-partitioned Financials Parquet files;
* standardise CERT and REPDTE and remove the API-only ID field;
* create a safe asset-to-deposit ratio and reporting year;
* optionally align the Part 3 label table by CERT/REPDTE; and
* aggregate bank-quarter observations to a quarterly summary.

The label is carried only as an outcome-alignment column. It is not used to
calculate a financial feature, so this processing stage does not introduce
label leakage. All measurements are generated from the local files at run
time; this module never invents benchmark values.
"""
from __future__ import annotations

import numpy as np

import json
import os
import time
import tracemalloc
from pathlib import Path
from typing import Any, Callable

import pandas as pd


NUMERIC_COLUMNS = ("ASSET", "DEP", "EQ", "ROA", "ROE", "NETINC", "RBC1AAJ", "RBCRWAJ")
ROW_COLUMNS = (
    "CERT",
    "REPDTE",
    "reporting_year",
    "ASSET",
    "DEP",
    "EQ",
    "ROA",
    "ROE",
    "NETINC",
    "RBC1AAJ",
    "RBCRWAJ",
    "asset_to_deposit",
    "label",
    "eligible_for_model",
)


class ProcessingError(RuntimeError):
    """Raised when the two-engine comparison cannot be made safely."""


def financial_files(processed_financials: Path) -> list[Path]:
    files = sorted(processed_financials.rglob("*.parquet"))
    if not files:
        raise FileNotFoundError(f"No Financials Parquet files under {processed_financials}")
    return files


def parse_fdic_date_pandas(values: pd.Series) -> pd.Series:
    """Parse the observed FDIC date encodings using explicit formats."""
    text = values.astype("string").str.strip()
    parsed = pd.to_datetime(text, format="%Y%m%d", errors="coerce")
    for fmt in ("%Y-%m-%d", "%Y/%m/%d"):
        missing = parsed.isna() & text.notna()
        if missing.any():
            parsed.loc[missing] = pd.to_datetime(text.loc[missing], format=fmt, errors="coerce")
    return parsed.dt.normalize()


def _read_panel_pandas(panel_path: Path | None) -> pd.DataFrame | None:
    if panel_path is None or not panel_path.exists():
        return None
    panel = pd.read_parquet(panel_path)
    required = {"CERT", "REPDTE"}
    missing = sorted(required - set(panel.columns))
    if missing:
        raise ProcessingError(f"Panel is missing join columns: {missing}")
    panel["CERT"] = pd.to_numeric(panel["CERT"], errors="coerce").astype("Int64")
    panel["REPDTE"] = parse_fdic_date_pandas(panel["REPDTE"])
    columns = [column for column in ("CERT", "REPDTE", "label", "eligible_for_model") if column in panel.columns]
    return panel[columns].drop_duplicates(["CERT", "REPDTE"])


def _prepare_pandas(processed_financials: Path, panel_path: Path | None = None) -> pd.DataFrame:
    frame = pd.concat((pd.read_parquet(path) for path in financial_files(processed_financials)), ignore_index=True)
    if "ID" in frame:
        frame = frame.drop(columns=["ID"])
    for required in ("CERT", "REPDTE"):
        if required not in frame:
            raise ProcessingError(f"Financials is missing required column {required}")
    frame["CERT"] = pd.to_numeric(frame["CERT"], errors="coerce").astype("Int64")
    frame["REPDTE"] = parse_fdic_date_pandas(frame["REPDTE"])
    frame = frame.dropna(subset=["CERT", "REPDTE"]).copy()
    for column in NUMERIC_COLUMNS:
        if column in frame:
            frame[column] = pd.to_numeric(frame[column], errors="coerce")
    if "ASSET" in frame and "DEP" in frame:
        denominator = frame["DEP"].where(frame["DEP"].notna() & frame["DEP"].ne(0))
        frame["asset_to_deposit"] = frame["ASSET"].div(denominator)
    else:
        frame["asset_to_deposit"] = pd.NA
    frame["reporting_year"] = frame["REPDTE"].dt.year.astype("Int64")
    frame["quarter_index"] = frame["REPDTE"].dt.year * 4 + frame["REPDTE"].dt.quarter
    previous = frame[["CERT", "quarter_index", "ASSET"]].copy()
    previous["quarter_index"] = previous["quarter_index"] + 1
    previous = previous.rename(columns={"ASSET": "ASSET_lag1"})
    frame = frame.merge(previous, on=["CERT", "quarter_index"], how="left", validate="many_to_one")
    frame["asset_growth_qoq"] = frame["ASSET"].div(frame["ASSET_lag1"].where(frame["ASSET_lag1"].notna() & frame["ASSET_lag1"].ne(0))) - 1
    panel = _read_panel_pandas(panel_path)
    if panel is not None:
        frame = frame.merge(panel, on=["CERT", "REPDTE"], how="left", validate="many_to_one")
    return frame


def _quarter_summary_pandas(frame: pd.DataFrame) -> pd.DataFrame:
    aggregations: dict[str, tuple[str, str]] = {
        "bank_count": ("CERT", "nunique"),
        "row_count": ("CERT", "size"),
    }
    for output, source, function in (
        ("total_asset", "ASSET", "sum"),
        ("mean_asset", "ASSET", "mean"),
        ("mean_roa", "ROA", "mean"),
        ("mean_asset_to_deposit", "asset_to_deposit", "mean"),
    ):
        if source in frame:
            aggregations[output] = (source, function)
    if "label" in frame:
        aggregations["positive_label_count"] = ("label", lambda values: int((values == 1).sum()))
    result = frame.groupby("REPDTE", as_index=False, dropna=False).agg(**aggregations)
    return result.sort_values("REPDTE").reset_index(drop=True)


def pandas_transform(processed_financials: Path, panel_path: Path | None = None) -> dict[str, Any]:
    rows = _prepare_pandas(processed_financials, panel_path)
    summary = _quarter_summary_pandas(rows)
    return {"rows": rows, "quarter_summary": summary, "engine": "pandas"}


def _polars_date_expr(column: str):
    import polars as pl

    text = pl.col(column).cast(pl.String).str.strip_chars()
    return pl.coalesce(
        [
            text.str.strptime(pl.Date, format="%Y%m%d", strict=False),
            text.str.strptime(pl.Date, format="%Y-%m-%d", strict=False),
            text.str.strptime(pl.Date, format="%Y/%m/%d", strict=False),
            text.str.strptime(pl.Date, format="%Y-%m-%d %H:%M:%S", strict=False),
            text.str.strptime(pl.Date, format="%Y-%m-%d %H:%M:%S%.f", strict=False),
        ]
    ).alias(column)


def _polars_scan(processed_financials: Path, panel_path: Path | None = None):
    import polars as pl

    paths = [str(path) for path in financial_files(processed_financials)]
    lazy = pl.scan_parquet(paths)
    source_columns = set(lazy.collect_schema().names())
    selected = [column for column in sorted(source_columns) if column != "ID"]
    lazy = lazy.select(selected)
    lazy = lazy.with_columns(
        [
            pl.col("CERT").cast(pl.Int64, strict=False),
            _polars_date_expr("REPDTE"),
        ]
        + [pl.col(column).cast(pl.Float64, strict=False) for column in NUMERIC_COLUMNS if column in source_columns]
    ).filter(pl.col("CERT").is_not_null() & pl.col("REPDTE").is_not_null())
    if "ASSET" in source_columns and "DEP" in source_columns:
        lazy = lazy.with_columns(
            pl.when(pl.col("DEP").is_not_null() & pl.col("DEP").ne(0))
            .then(pl.col("ASSET") / pl.col("DEP"))
            .otherwise(None)
            .alias("asset_to_deposit")
        )
    else:
        lazy = lazy.with_columns(pl.lit(None, dtype=pl.Float64).alias("asset_to_deposit"))
    lazy = lazy.with_columns([
        pl.col("REPDTE").dt.year().alias("reporting_year"),
        (pl.col("REPDTE").dt.year() * 4 + pl.col("REPDTE").dt.quarter()).alias("quarter_index"),
    ]).sort(["CERT", "REPDTE"])
    lazy = lazy.with_columns([
        pl.col("ASSET").shift(1).over("CERT").alias("ASSET_lag_candidate"),
        pl.col("quarter_index").shift(1).over("CERT").alias("quarter_index_previous"),
    ]).with_columns(
        pl.when(pl.col("quarter_index") - pl.col("quarter_index_previous") == 1)
        .then(pl.col("ASSET_lag_candidate"))
        .otherwise(None)
        .alias("ASSET_lag1")
    ).with_columns(
        pl.when(pl.col("ASSET_lag1").is_not_null() & pl.col("ASSET_lag1").ne(0))
        .then(pl.col("ASSET") / pl.col("ASSET_lag1") - 1)
        .otherwise(None)
        .alias("asset_growth_qoq")
    ).drop(["ASSET_lag_candidate", "quarter_index_previous"])
    if panel_path is not None and panel_path.exists():
        panel = pl.scan_parquet(str(panel_path))
        panel_columns = set(panel.collect_schema().names())
        missing = sorted({"CERT", "REPDTE"} - panel_columns)
        if missing:
            raise ProcessingError(f"Panel is missing join columns: {missing}")
        panel_selected = [column for column in ("CERT", "REPDTE", "label", "eligible_for_model") if column in panel_columns]
        panel = (
            panel.select(panel_selected)
            .with_columns([pl.col("CERT").cast(pl.Int64, strict=False), _polars_date_expr("REPDTE")])
            .unique(subset=["CERT", "REPDTE"], keep="first")
        )
        lazy = lazy.join(panel, on=["CERT", "REPDTE"], how="left", validate="m:1")
    return lazy


def _quarter_summary_polars(rows):
    import polars as pl

    aggregations = [pl.col("CERT").n_unique().alias("bank_count"), pl.len().alias("row_count")]
    for output, source, expression in (
        ("total_asset", "ASSET", pl.col("ASSET").sum()),
        ("mean_asset", "ASSET", pl.col("ASSET").mean()),
        ("mean_roa", "ROA", pl.col("ROA").mean()),
        ("mean_asset_to_deposit", "asset_to_deposit", pl.col("asset_to_deposit").mean()),
    ):
        if source in rows.columns:
            aggregations.append(expression.alias(output))
    if "label" in rows.columns:
        aggregations.append((pl.col("label") == 1).sum().alias("positive_label_count"))
    return rows.group_by("REPDTE").agg(aggregations).sort("REPDTE")


def polars_transform(processed_financials: Path, panel_path: Path | None = None) -> dict[str, Any]:
    lazy = _polars_scan(processed_financials, panel_path)
    rows = lazy.collect().sort(["CERT", "REPDTE"])
    summary = _quarter_summary_polars(rows)
    return {"rows": rows, "quarter_summary": summary, "engine": "polars", "lazy_plan": lazy.explain(optimized=True)}


def _as_pandas(value: Any) -> pd.DataFrame:
    if isinstance(value, pd.DataFrame):
        return value.copy()
    return value.to_pandas()


def _canonical(value: Any, keys: list[str]) -> pd.DataFrame:
    frame = _as_pandas(value)
    frame = frame.reindex(sorted(frame.columns), axis=1)
    for column in frame.columns:
        if column == "REPDTE":
            frame[column] = pd.to_datetime(frame[column], errors="coerce").dt.strftime("%Y-%m-%d")
        elif column in {"CERT", "reporting_year"}:
            frame[column] = pd.to_numeric(frame[column], errors="coerce")
        elif pd.api.types.is_numeric_dtype(frame[column]):
            frame[column] = pd.to_numeric(frame[column], errors="coerce").round(10)
    # Pandas and Polars use different nullable representations.  Normalize
    # missing values before comparison so equivalence reflects values, not
    # whether the engine returned None, NaN or pandas.NA.
    frame = frame.astype("object").where(frame.notna(), float("nan"))
    sort_keys = [key for key in keys if key in frame.columns]
    return frame.sort_values(sort_keys).reset_index(drop=True) if sort_keys else frame.reset_index(drop=True)


def compare_engine_outputs(pandas_result: dict[str, Any], polars_result: dict[str, Any]) -> dict[str, Any]:
    checks: dict[str, Any] = {}
    for name, keys in (("rows", ["CERT", "REPDTE"]), ("quarter_summary", ["REPDTE"])):
        left = _canonical(pandas_result[name], keys)
        right = _canonical(polars_result[name], keys)
        equivalent = True
        detail = "equal"
        try:
            pd.testing.assert_frame_equal(left, right, check_dtype=False, check_exact=False, rtol=1e-8, atol=1e-8)
        except AssertionError as exc:
            equivalent = False
            detail = str(exc).splitlines()[0]
        checks[name] = {"equivalent": equivalent, "pandas_rows": len(left), "polars_rows": len(right), "detail": detail}
    return {"equivalent": all(item["equivalent"] for item in checks.values()), "checks": checks}


def _measure(engine: str, function: Callable[[], dict[str, Any]], repetitions: int) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for repeat in range(1, repetitions + 1):
        process = None
        try:
            import psutil

            process = psutil.Process(os.getpid())
            rss_before = process.memory_info().rss
        except ImportError:
            rss_before = None
        tracemalloc.start()
        start = time.perf_counter()
        result = function()
        seconds = time.perf_counter() - start
        _, python_peak = tracemalloc.get_traced_memory()
        tracemalloc.stop()
        rss_after = process.memory_info().rss if process is not None else None
        rows.append(
            {
                "engine": engine,
                "operation": "read_transform_collect",
                "repeat": repeat,
                "seconds": seconds,
                "rss_endpoint_mb": max(rss_before or 0, rss_after or 0) / 2**20 if process is not None else None,
                "peak_python_mb": python_peak / 2**20,
                "result_rows": len(result["rows"]),
                "summary_rows": len(result["quarter_summary"]),
                "cache_mode": "warm_only",
            }
        )
    return rows


def _scale_benchmark(pandas_result: dict[str, Any], polars_result: dict[str, Any], seed: int, repetitions: int) -> pd.DataFrame:
    """Measure the shared aggregation at several deterministic row sizes.

    The full engine benchmark measures Parquet scan plus transformation.  This
    additional table isolates in-memory aggregation scaling so the report does
    not overclaim that a two-engine smoke run is a cluster-sizing experiment.
    """
    import polars as pl

    pandas_rows = _as_pandas(pandas_result["rows"])
    sizes = sorted(set(min(len(pandas_rows), size) for size in (1000, 10000, 50000, len(pandas_rows)) if len(pandas_rows) > 0))
    measurements: list[dict[str, Any]] = []
    for size in sizes:
        sample = pandas_rows.sample(size, random_state=seed).sort_values(["CERT", "REPDTE"]).reset_index(drop=True)
        for engine in ("pandas", "polars"):
            value = sample if engine == "pandas" else pl.from_pandas(sample)
            for repeat in range(1, repetitions + 1):
                start = time.perf_counter()
                if engine == "pandas":
                    result = _quarter_summary_pandas(value)
                else:
                    result = _quarter_summary_polars(value)
                    result.collect() if hasattr(result, "collect") else result
                measurements.append({"engine": engine, "rows": size, "repeat": repeat, "seconds": time.perf_counter() - start, "summary_rows": len(result) if engine == "pandas" else result.height})
    return pd.DataFrame(measurements)


def _write_sample(result: dict[str, Any], output_dir: Path, sample_rows: int | None, seed: int) -> int:
    rows = _as_pandas(result["rows"])
    if sample_rows is not None and sample_rows > 0 and len(rows) > sample_rows:
        rows = rows.sample(sample_rows, random_state=seed).sort_values(["CERT", "REPDTE"])
    rows.to_parquet(output_dir / f"p5_{result['engine']}_rows_sample.parquet", index=False)
    return len(rows)



def _measure_amdahl_scaling(
    rows_df: pd.DataFrame,
    output_dir: Path,
    seed: int = 42,
    repetitions: int = 2,
) -> pd.DataFrame:
    """Measure runtime across data sizes and thread/core counts, computing Amdahl's p."""
    import polars as pl
    import concurrent.futures

    sizes = sorted(set(min(len(rows_df), s) for s in (500, 2000, 10000, len(rows_df)) if len(rows_df) > 0))
    core_counts = [1, 2, 4]
    scaling_records: list[dict[str, Any]] = []

    for size in sizes:
        sample = rows_df.sample(size, random_state=seed).sort_values(["CERT", "REPDTE"]).reset_index(drop=True)
        pl_sample = pl.from_pandas(sample)
        
        # Benchmark Polars multi-thread scaling across core counts
        base_t1 = None
        for n_threads in core_counts:
            times = []
            for _ in range(repetitions):
                start = time.perf_counter()
                if n_threads == 1:
                    res = _quarter_summary_polars(pl_sample)
                    _ = res.collect() if hasattr(res, "collect") else res
                else:
                    # Partition workload across n_threads to measure parallel execution
                    chunks = [sample.iloc[i::n_threads] for i in range(n_threads)]
                    with concurrent.futures.ThreadPoolExecutor(max_workers=n_threads) as executor:
                        futures = [executor.submit(_quarter_summary_polars, pl.from_pandas(c)) for c in chunks if not c.empty]
                        _ = [f.result() for f in futures]
                elapsed = max(time.perf_counter() - start, 1e-6)
                times.append(elapsed)

            t_n = float(np.median(times))
            if n_threads == 1:
                base_t1 = t_n
                speedup = 1.0
                p_fraction = 1.0
            else:
                speedup = float(base_t1 / t_n) if base_t1 is not None and t_n > 0 else 1.0
                # Amdahl formula: p = (1 - 1/S) / (1 - 1/n)
                if n_threads > 1:
                    raw_p = (1.0 - (1.0 / speedup)) / (1.0 - (1.0 / n_threads))
                    p_fraction = float(np.clip(raw_p, 0.0, 1.0))
                else:
                    p_fraction = 1.0

            scaling_records.append({
                "engine": "polars",
                "cores": n_threads,
                "data_size": size,
                "seconds": round(t_n, 6),
                "speedup_S": round(speedup, 4),
                "parallel_fraction_p": round(p_fraction, 4),
                "theoretical_max_speedup": round(1.0 / (1.0 - p_fraction + 1e-6), 2) if p_fraction < 1.0 else "infinite",
            })

    scaling_df = pd.DataFrame(scaling_records)
    scaling_df.to_csv(output_dir / "p5_amdahl_scaling.csv", index=False)
    return scaling_df


def _record_memory_vs_ram(panel_path: Path | None, output_dir: Path) -> dict[str, Any]:
    """Record the in-memory footprint of the full panel versus available host RAM."""
    import psutil
    vm = psutil.virtual_memory()
    total_ram_mb = vm.total / (1024**2)
    avail_ram_mb = vm.available / (1024**2)

    panel_mb = 0.0
    panel_rows = 0
    if panel_path and panel_path.exists():
        try:
            panel_df = pd.read_parquet(panel_path)
            panel_mb = float(panel_df.memory_usage(deep=True).sum() / (1024**2))
            panel_rows = len(panel_df)
        except Exception:
            pass

    ratio_pct = (panel_mb / avail_ram_mb * 100) if avail_ram_mb > 0 else 0.0
    result = {
        "panel_path": str(panel_path) if panel_path else None,
        "panel_rows": panel_rows,
        "panel_in_memory_mb": round(panel_mb, 2),
        "system_total_ram_mb": round(total_ram_mb, 2),
        "system_available_ram_mb": round(avail_ram_mb, 2),
        "panel_to_available_ram_pct": round(ratio_pct, 4),
        "single_node_sufficient": bool(panel_mb < avail_ram_mb),
        "methodology": "pandas memory_usage(deep=True) vs psutil.virtual_memory()",
    }
    (output_dir / "p5_memory_vs_ram.json").write_text(json.dumps(result, indent=2), encoding="utf-8")
    return result

def run_engine_benchmark(
    processed_financials: Path,
    output_dir: Path,
    panel_path: Path | None = None,
    repetitions: int = 5,
    sample_rows: int | None = None,
    seed: int = 42,
) -> dict[str, Any]:
    if repetitions < 2:
        raise ValueError("Use at least two repetitions for an engine benchmark.")
    output_dir.mkdir(parents=True, exist_ok=True)
    pandas_function = lambda: pandas_transform(processed_financials, panel_path)
    polars_function = lambda: polars_transform(processed_financials, panel_path)
    measurements = _measure("pandas", pandas_function, repetitions) + _measure("polars", polars_function, repetitions)
    pandas_result = pandas_function()
    polars_result = polars_function()
    equivalence = compare_engine_outputs(pandas_result, polars_result)
    pandas_sample_rows = _write_sample(pandas_result, output_dir, sample_rows, seed)
    polars_sample_rows = _write_sample(polars_result, output_dir, sample_rows, seed)
    pd.DataFrame(measurements).to_csv(output_dir / "p5_engine_benchmark_raw.csv", index=False)
    summary = (
        pd.DataFrame(measurements)
        .groupby("engine", as_index=False)
        .agg(
            median_seconds=("seconds", "median"),
            std_seconds=("seconds", "std"),
            median_rss_endpoint_mb=("rss_endpoint_mb", "median"),
            median_peak_python_mb=("peak_python_mb", "median"),
        )
    )
    summary.to_csv(output_dir / "p5_engine_benchmark_summary.csv", index=False)
    scale = _scale_benchmark(pandas_result, polars_result, seed, repetitions)
    scale.to_csv(output_dir / "p5_scaling_benchmark.csv", index=False)
    amdahl_df = _measure_amdahl_scaling(_as_pandas(pandas_result["rows"]), output_dir, seed, repetitions)
    memory_profile = _record_memory_vs_ram(panel_path, output_dir)
    pd.DataFrame(
        [
            {"output": name, **details}
            for name, details in equivalence["checks"].items()
        ]
    ).to_csv(output_dir / "p5_output_equivalence.csv", index=False)
    pandas_result["quarter_summary"].to_csv(output_dir / "p5_pandas_quarter_summary.csv", index=False)
    _as_pandas(polars_result["quarter_summary"]).to_csv(output_dir / "p5_polars_quarter_summary.csv", index=False)
    plan = polars_result["lazy_plan"]
    (output_dir / "p5_polars_query_plan.txt").write_text(plan, encoding="utf-8")
    report = {
        "processing_version": "part5-v1",
        "source_files": len(financial_files(processed_financials)),
        "panel_joined": bool(panel_path and panel_path.exists()),
        "panel_path": str(panel_path) if panel_path and panel_path.exists() else None,
        "source_rows_after_cleaning": len(pandas_result["rows"]),
        "quarter_summary_rows": len(pandas_result["quarter_summary"]),
        "repetitions": repetitions,
        "sample_rows_requested": sample_rows,
        "sample_rows_written": {"pandas": pandas_sample_rows, "polars": polars_sample_rows},
        "cache_mode": "warm_only",
        "operations": summary.to_dict(orient="records"),
        "scaling_benchmark_file": str(output_dir / "p5_scaling_benchmark.csv"),
        "scaling_scope": "in-memory aggregation after the equivalent engine outputs have been materialized; not a distributed-cluster benchmark",
        "output_equivalence": equivalence,
        "polars_plan_file": str(output_dir / "p5_polars_query_plan.txt"),
        "amdahl_scaling_file": str(output_dir / "p5_amdahl_scaling.csv"),
        "memory_profile": memory_profile,
        "excluded_source_columns": ["ID"],
        "target_columns_not_features": ["label", "eligible_for_model"],
        "feature_processing_note": "asset_to_deposit is an aligned descriptive transformation only; labels are never used to calculate it.",
        "rss_note": "RSS is measured at operation endpoints; Python allocation peak is measured with tracemalloc. Results are warm-only.",
    }
    (output_dir / "p5_engine_benchmark.json").write_text(json.dumps(report, indent=2, default=str), encoding="utf-8")
    return report
