"""Part 4: fair storage-format benchmark on the actual Financials layout.

The benchmark never invents performance numbers. It writes comparable sample
representations, measures the same operations, and saves raw measurements plus
medians and standard deviations for the report.
"""
from __future__ import annotations

import json
import os
import shutil
import time
import tracemalloc
from pathlib import Path
from typing import Callable

import pandas as pd


def _financial_files(processed_financials: Path) -> list[Path]:
    files = sorted(processed_financials.rglob("*.parquet"))
    if not files:
        raise FileNotFoundError(f"No Financials Parquet files under {processed_financials}")
    return files


def load_financial_frame(processed_financials: Path) -> pd.DataFrame:
    frame = pd.concat((pd.read_parquet(path) for path in _financial_files(processed_financials)), ignore_index=True)
    if "REPDTE" in frame:
        frame["REPDTE"] = pd.to_datetime(frame["REPDTE"], errors="coerce")
    return frame


def stratified_sample(frame: pd.DataFrame, sample_rows: int | None, seed: int = 42) -> tuple[pd.DataFrame, dict[str, object]]:
    """Take a reproducible random sample stratified by reporting year."""
    if sample_rows is None or sample_rows >= len(frame):
        return frame.copy(), {"strategy": "full_dataset", "requested_rows": sample_rows, "seed": seed, "rows": len(frame)}
    if sample_rows <= 0:
        raise ValueError("sample_rows must be positive")
    dates = pd.to_datetime(frame["REPDTE"], errors="coerce")
    years = dates.dt.year.astype("Int64").astype("string").fillna("unknown")
    work = frame.copy()
    work["__benchmark_year"] = years
    groups = work.groupby("__benchmark_year", dropna=False, sort=True)
    n_groups = groups.ngroups
    if sample_rows < n_groups:
        sampled = work.sample(n=sample_rows, random_state=seed).drop(columns=["__benchmark_year"])
        return sampled, {"strategy": "random_sample_due_to_small_request", "requested_rows": sample_rows, "seed": seed, "rows": len(sampled), "year_counts": {str(k): int(v) for k, v in sampled.assign(year=pd.to_datetime(sampled["REPDTE"], errors="coerce").dt.year.astype("Int64")).groupby("year").size().items()}}
    allocations = {key: max(1, int(round(sample_rows * len(group) / len(work)))) for key, group in groups}
    while sum(allocations.values()) > sample_rows:
        key = max(allocations, key=lambda item: allocations[item])
        if allocations[key] > 1:
            allocations[key] -= 1
        else:
            break
    while sum(allocations.values()) < sample_rows:
        key = max(allocations, key=lambda item: len(groups.get_group(item)) - allocations[item])
        allocations[key] += 1
    sampled = pd.concat([group.sample(n=min(allocations[key], len(group)), random_state=seed) for key, group in groups], ignore_index=True)
    sampled = sampled.drop(columns=["__benchmark_year"])
    return sampled, {"strategy": "random_stratified_by_reporting_year", "requested_rows": sample_rows, "seed": seed, "rows": len(sampled), "year_counts": {str(k): int(v) for k, v in sampled.assign(year=pd.to_datetime(sampled["REPDTE"], errors="coerce").dt.year.astype("Int64")).groupby("year").size().items()}}


class _RSSSampler:
    def __init__(self):
        self.max_rss = 0
        self._process = None

    def start(self):
        try:
            import psutil
            self._process = psutil.Process(os.getpid())
        except ImportError:
            return False
        # A per-operation background sampler caused native access violations
        # on Windows when PyArrow was read concurrently.  Keep the benchmark
        # portable: record the larger endpoint RSS and exact tracemalloc peak.
        self.max_rss = self._process.memory_info().rss
        return True

    def stop(self):
        if self._process is not None:
            self.max_rss = max(self.max_rss, self._process.memory_info().rss)


def measure(operation: str, fn: Callable[[], object], repetitions: int, cache_mode: str) -> list[dict[str, object]]:
    rows = []
    for repeat in range(1, repetitions + 1):
        sampler = _RSSSampler()
        rss_available = sampler.start()
        tracemalloc.start()
        start = time.perf_counter()
        result = fn()
        elapsed = time.perf_counter() - start
        _, python_peak = tracemalloc.get_traced_memory()
        tracemalloc.stop()
        sampler.stop()
        rows.append({"operation": operation, "repeat": repeat, "seconds": elapsed, "peak_rss_mb": sampler.max_rss / 2**20 if rss_available else None, "peak_python_mb": python_peak / 2**20, "cache_mode": cache_mode, "result_rows": len(result) if hasattr(result, "__len__") else None})
    return rows


def _write_partitioned(frame: pd.DataFrame, root: Path, compression: str) -> None:
    if root.exists():
        shutil.rmtree(root)
    root.mkdir(parents=True, exist_ok=True)
    years = pd.to_datetime(frame["REPDTE"], errors="coerce").dt.year
    for year, group in frame.assign(__year=years).groupby("__year", dropna=False):
        directory = root / f"year={year}"
        directory.mkdir(parents=True, exist_ok=True)
        group.drop(columns=["__year"]).to_parquet(directory / "part-00000.parquet", index=False, compression=compression)


def _size_bytes(path: Path) -> int:
    if path.is_file():
        return path.stat().st_size
    return sum(file.stat().st_size for file in path.rglob("*") if file.is_file())


def _read_partitioned(path: Path, columns=None, date_filter: bool = False) -> pd.DataFrame:
    filters = [("REPDTE", ">=", pd.Timestamp("2020-01-01"))] if date_filter else None
    return pd.read_parquet(path, columns=columns, filters=filters)


def _write_arrow(frame: pd.DataFrame, path: Path) -> None:
    import pyarrow as pa
    import pyarrow.ipc as ipc
    table = pa.Table.from_pandas(frame, preserve_index=False)
    with path.open("wb") as stream:
        with ipc.new_file(stream, table.schema) as writer:
            writer.write_table(table)


def _read_arrow(path: Path) -> pd.DataFrame:
    import pyarrow.ipc as ipc
    with path.open("rb") as stream:
        return ipc.open_file(stream).read_all().to_pandas()


def run_storage_benchmark(processed_financials: Path, output_dir: Path, repetitions: int = 5, sample_rows: int | None = None, seed: int = 42, include_arrow: bool = False, cold_cache_requested: bool = False) -> dict[str, object]:
    if repetitions < 2:
        raise ValueError("Use at least two repetitions; the assignment benchmark default is five.")
    source = load_financial_frame(processed_financials)
    frame, sample_meta = stratified_sample(source, sample_rows, seed)
    output_dir.mkdir(parents=True, exist_ok=True)
    artifacts = output_dir / "artifacts"
    if artifacts.exists():
        shutil.rmtree(artifacts)
    artifacts.mkdir(parents=True)
    csv_path = artifacts / "financials.csv"
    gzip_path = artifacts / "financials.csv.gz"
    single_snappy = artifacts / "financials_single_snappy.parquet"
    single_zstd = artifacts / "financials_single_zstd.parquet"
    partition_snappy = artifacts / "financials_year_snappy"
    partition_zstd = artifacts / "financials_year_zstd"
    arrow_path = artifacts / "financials.arrow"

    write_functions: dict[str, Callable[[], object]] = {
        "write_csv": lambda: frame.to_csv(csv_path, index=False),
        "write_gzip_csv": lambda: frame.to_csv(gzip_path, index=False, compression="gzip"),
        "write_single_parquet_snappy": lambda: frame.to_parquet(single_snappy, index=False, compression="snappy"),
        "write_single_parquet_zstd": lambda: frame.to_parquet(single_zstd, index=False, compression="zstd"),
        "write_year_partitioned_snappy": lambda: _write_partitioned(frame, partition_snappy, "snappy"),
        "write_year_partitioned_zstd": lambda: _write_partitioned(frame, partition_zstd, "zstd"),
    }
    if include_arrow:
        write_functions["write_arrow_ipc"] = lambda: _write_arrow(frame, arrow_path)
    raw_measurements = []
    for name, function in write_functions.items():
        raw_measurements.extend(measure(name, function, repetitions, "warm_only"))

    selected_columns = [column for column in ["CERT", "REPDTE", "ASSET", "DEP", "EQ"] if column in frame.columns]
    DATE_FILTER = pd.Timestamp("2020-01-01")
    read_functions: dict[str, Callable[[], object]] = {
        "read_csv_full": lambda: pd.read_csv(csv_path),
        "read_gzip_csv_full": lambda: pd.read_csv(gzip_path),
        "read_single_parquet_snappy_full": lambda: pd.read_parquet(single_snappy),
        "read_single_parquet_zstd_full": lambda: pd.read_parquet(single_zstd),
        "read_year_partitioned_snappy_full": lambda: pd.read_parquet(partition_snappy),
        "read_year_partitioned_zstd_full": lambda: pd.read_parquet(partition_zstd),
        "read_csv_selected_columns": lambda: pd.read_csv(csv_path, usecols=selected_columns),
        "read_single_parquet_selected_columns": lambda: pd.read_parquet(single_snappy, columns=selected_columns),
        "read_year_partitioned_selected_columns": lambda: pd.read_parquet(partition_snappy, columns=selected_columns),
        "read_csv_date_filter": lambda: pd.read_csv(csv_path, parse_dates=["REPDTE"]).loc[lambda data: data["REPDTE"] >= DATE_FILTER],
        "read_single_parquet_date_filter": lambda: pd.read_parquet(single_snappy, filters=[("REPDTE", ">=", DATE_FILTER)]),
        "read_year_partitioned_date_filter": lambda: _read_partitioned(partition_snappy, date_filter=True),
    }
    if include_arrow:
        read_functions["read_arrow_full"] = lambda: _read_arrow(arrow_path)
    for name, function in read_functions.items():
        raw_measurements.extend(measure(name, function, repetitions, "warm_only"))

    file_paths = {"csv": csv_path, "gzip_csv": gzip_path, "single_snappy": single_snappy, "single_zstd": single_zstd, "year_partitioned_snappy": partition_snappy, "year_partitioned_zstd": partition_zstd}
    if include_arrow:
        file_paths["arrow_ipc"] = arrow_path
    sizes = [{"format": name, "bytes": _size_bytes(path), "path": str(path)} for name, path in file_paths.items()]
    measurements = pd.DataFrame(raw_measurements)
    measurements.to_csv(output_dir / "p4_storage_benchmark_raw.csv", index=False)
    summary = measurements.groupby("operation", as_index=False).agg(median_seconds=("seconds", "median"), std_seconds=("seconds", "std"), min_seconds=("seconds", "min"), max_seconds=("seconds", "max"), median_peak_rss_mb=("peak_rss_mb", "median"), median_peak_python_mb=("peak_python_mb", "median"))
    summary.to_csv(output_dir / "p4_storage_benchmark_summary.csv", index=False)
    equivalence = check_equivalence(csv_path, gzip_path, single_snappy, single_zstd, partition_snappy, partition_zstd, selected_columns, include_arrow, arrow_path)
    report = {"benchmark_version": "part4-v1", "rows": len(frame), "source_rows": len(source), "sample": sample_meta, "repetitions": repetitions, "cache_mode": "warm_only", "cold_cache_requested": cold_cache_requested, "cold_cache_note": "Cold-cache eviction is not performed by this cross-platform script; Windows cache eviction would require system-specific privileges. Results are explicitly warm-only." if cold_cache_requested else "Cold-cache runs were not requested; results are warm-only.", "rss_sampling_note": "RSS is the larger before/after endpoint measurement per operation; Python allocation peak is measured exactly with tracemalloc. A background RSS sampler is avoided because concurrent native PyArrow reads caused Windows access violations.", "formats": sizes, "operations": summary.to_dict(orient="records"), "date_filter": {"column": "REPDTE", "operator": ">=", "value": pd.Timestamp("2020-01-01").isoformat(), "parquet_filter_type": "pandas.Timestamp"}, "output_equivalence": equivalence}
    (output_dir / "p4_storage_benchmark.json").write_text(json.dumps(report, indent=2, default=str), encoding="utf-8")
    try:
        import matplotlib
        matplotlib.use("Agg", force=True)
        import matplotlib.pyplot as plt
        plot = summary[summary["operation"].str.contains("read_")].copy()
        if not plot.empty:
            fig, ax = plt.subplots(figsize=(12, 7))
            plot = plot.sort_values("median_seconds")
            fastest_op = plot.iloc[0]["operation"]
            fastest_sec = plot.iloc[0]["median_seconds"]
            total_r = summary["rows"].iloc[0] if "rows" in summary.columns else len(plot)
            ax.barh(plot["operation"], plot["median_seconds"], color="#336699", edgecolor="none")
            ax.set_xlabel("Median seconds")
            ax.set_title(f"Storage Format Read Benchmark ({total_r:,} Rows; Fastest: {fastest_op} @ {fastest_sec:.3f}s)")
            ax.spines["top"].set_visible(False)
            ax.spines["right"].set_visible(False)
            fig.tight_layout()
            fig.savefig(output_dir / "p4_storage_benchmark_read_times.png", dpi=150)
            plt.close(fig)
    except ImportError:
        report["plot_note"] = "Matplotlib unavailable; no benchmark chart was generated."
    return report


def _canonical(frame: pd.DataFrame, selected_columns: list[str]) -> pd.DataFrame:
    result = frame[selected_columns].copy()
    if "REPDTE" in result:
        result["REPDTE"] = pd.to_datetime(result["REPDTE"], errors="coerce").dt.strftime("%Y-%m-%d")
    if "CERT" in result:
        result["CERT"] = pd.to_numeric(result["CERT"], errors="coerce")
    for column in result.columns:
        if column not in {"CERT", "REPDTE"}:
            result[column] = pd.to_numeric(result[column], errors="coerce").round(10)
    keys = [key for key in ["CERT", "REPDTE"] if key in result]
    return result.sort_values(keys).reset_index(drop=True)


def check_equivalence(csv_path: Path, gzip_path: Path, single_snappy: Path, single_zstd: Path, partition_snappy: Path, partition_zstd: Path, selected_columns: list[str], include_arrow: bool, arrow_path: Path) -> dict[str, object]:
    reference = _canonical(pd.read_csv(csv_path, usecols=selected_columns), selected_columns)
    readers: dict[str, Callable[[], pd.DataFrame]] = {
        "gzip_csv": lambda: pd.read_csv(gzip_path, usecols=selected_columns),
        "single_snappy": lambda: pd.read_parquet(single_snappy, columns=selected_columns),
        "single_zstd": lambda: pd.read_parquet(single_zstd, columns=selected_columns),
        "year_partitioned_snappy": lambda: pd.read_parquet(partition_snappy, columns=selected_columns),
        "year_partitioned_zstd": lambda: pd.read_parquet(partition_zstd, columns=selected_columns),
    }
    if include_arrow:
        readers["arrow_ipc"] = lambda: _read_arrow(arrow_path)
    results: dict[str, object] = {"reference": "csv", "selected_columns": selected_columns, "checks": {}}
    for name, reader in readers.items():
        try:
            actual = _canonical(reader(), selected_columns)
            pd.testing.assert_frame_equal(reference, actual, check_dtype=False, check_exact=False, rtol=1e-8, atol=1e-8)
            results["checks"][name] = {"equivalent": True, "rows": len(actual)}
        except AssertionError as exc:
            results["checks"][name] = {"equivalent": False, "error": str(exc)[:500]}
    return results
