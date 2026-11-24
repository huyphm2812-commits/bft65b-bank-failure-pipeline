from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pandas as pd

from bankfail.acquisition.client import quarter_ends
from bankfail.acquisition.storage import DATASET_SCHEMA_DEFINITIONS, sha256


def _chunk_filter(chunk: dict[str, Any]) -> str:
    params = chunk.get("params", {})
    return str(params.get("filters", chunk.get("query", {}).get("filter", "all")))


def _check_parquet_schema(path: Path, dataset: str) -> list[str]:
    import pyarrow.parquet as pq

    errors: list[str] = []
    schema = pq.read_schema(path)
    expected = DATASET_SCHEMA_DEFINITIONS[dataset]
    if schema.names != list(expected):
        errors.append(f"{path}: schema columns differ from explicit {dataset} schema")
    if "ID" in schema.names:
        errors.append(f"{path}: API ID column was not removed")
    actual_types = {field.name: str(field.type) for field in schema}
    for name, kind in expected.items():
        expected_type = {"float64": "double", "int64": "int64", "string": "string", "timestamp[ms]": "timestamp[ms]"}[kind]
        if actual_types.get(name) != expected_type:
            errors.append(f"{path}: {name} has type {actual_types.get(name)!r}; expected {expected_type!r}")
    return errors


def _validate_manifest(
    processed_root: Path,
    manifest: dict[str, Any],
    start_date: str,
    end_date: str,
) -> tuple[dict[str, Any], list[str]]:
    errors: list[str] = []
    chunks = manifest.get("chunks", {})
    if not isinstance(chunks, dict):
        return {"chunks": 0}, ["manifest.chunks is not a mapping"]
    by_dataset: dict[str, list[dict[str, Any]]] = {name: [] for name in DATASET_SCHEMA_DEFINITIONS}
    for key, chunk in chunks.items():
        dataset = str(chunk.get("dataset", str(key).split("/", 1)[0]))
        if dataset not in by_dataset:
            errors.append(f"manifest chunk {key}: unknown dataset {dataset!r}")
            continue
        by_dataset[dataset].append(chunk)
        if chunk.get("status") != "complete":
            errors.append(f"manifest chunk {key}: status is not complete")
        raw_path = Path(chunk.get("raw_path", ""))
        parquet_path = Path(chunk.get("parquet_path", ""))
        if not raw_path.exists():
            errors.append(f"manifest chunk {key}: raw file is missing")
        elif chunk.get("raw_sha256") != sha256(raw_path):
            errors.append(f"manifest chunk {key}: raw SHA-256 mismatch")
        if not parquet_path.exists():
            errors.append(f"manifest chunk {key}: Parquet file is missing")
            continue
        if chunk.get("parquet", {}).get("sha256") != sha256(parquet_path):
            errors.append(f"manifest chunk {key}: Parquet SHA-256 mismatch")
        errors.extend(_check_parquet_schema(parquet_path, dataset))
        actual_rows = len(pd.read_parquet(parquet_path))
        if int(chunk.get("record_count", -1)) != actual_rows:
            errors.append(f"manifest chunk {key}: record_count does not match Parquet rows")
        if int(chunk.get("parquet", {}).get("rows", -1)) != actual_rows:
            errors.append(f"manifest chunk {key}: parquet.rows does not match Parquet rows")
        if "api_total" not in chunk:
            errors.append(f"manifest chunk {key}: api_total is missing")

    expected_quarters = quarter_ends(start_date, end_date)
    quarter_totals: dict[str, dict[str, Any]] = {}
    for quarter in expected_quarters:
        filter_value = f"REPDTE:{quarter.replace('-', '')}"
        matching = [chunk for chunk in by_dataset["financials"] if _chunk_filter(chunk) == filter_value]
        api_totals = {chunk.get("api_total") for chunk in matching}
        record_total = sum(int(chunk.get("record_count", 0)) for chunk in matching)
        api_total = next(iter(api_totals)) if len(api_totals) == 1 else None
        complete = bool(matching) and len(api_totals) == 1 and record_total == api_total and api_total is not None and int(api_total) >= 1
        quarter_totals[quarter] = {
            "chunks": len(matching),
            "record_count_sum": record_total,
            "api_total": api_total,
            "complete": complete,
        }
        if not matching:
            errors.append(f"financials: missing expected quarter {quarter}")
        elif not complete:
            errors.append(f"financials {quarter}: missing records or sum(record_count) does not equal a positive api_total")

    expected_filters = {f"REPDTE:{quarter.replace('-', '')}" for quarter in expected_quarters}
    observed_filters = {_chunk_filter(chunk) for chunk in by_dataset["financials"]}
    unexpected = sorted(observed_filters - expected_filters)
    if unexpected:
        errors.append(f"financials: unexpected quarter filters present: {unexpected[:10]}")

    small_table_totals: dict[str, Any] = {}
    for dataset in ("failures", "institutions"):
        matching = [chunk for chunk in by_dataset[dataset] if _chunk_filter(chunk) == "all"]
        api_totals = {chunk.get("api_total") for chunk in matching}
        record_total = sum(int(chunk.get("record_count", 0)) for chunk in matching)
        api_total = next(iter(api_totals)) if len(api_totals) == 1 else None
        complete = bool(matching) and len(api_totals) == 1 and record_total == api_total
        small_table_totals[dataset] = {
            "chunks": len(matching),
            "record_count_sum": record_total,
            "api_total": api_total,
            "complete": complete,
        }
        if not matching:
            errors.append(f"{dataset}: missing complete-table API chunks")
        elif not complete:
            errors.append(f"{dataset}: sum(record_count) does not equal api_total")

    return {
        "chunks": len(chunks),
        "expected_quarters": len(expected_quarters),
        "observed_quarters": sum(bool(item["chunks"]) for item in quarter_totals.values()),
        "quarter_totals": quarter_totals,
        "small_table_totals": small_table_totals,
    }, errors


def _read_dataset(processed_root: Path, dataset: str) -> pd.DataFrame:
    files = sorted((processed_root / dataset).rglob("*.parquet"))
    if not files:
        return pd.DataFrame()
    return pd.concat((pd.read_parquet(file) for file in files), ignore_index=True)


def validate_dataset(processed_root: Path, manifest_path: Path, start_date: str, end_date: str) -> dict[str, Any]:
    manifest = json.loads(manifest_path.read_text(encoding="utf-8")) if manifest_path.exists() else {}
    manifest_summary, errors = _validate_manifest(processed_root, manifest, start_date, end_date)
    result: dict[str, Any] = {
        "status": "ok",
        "requested_period": {"start": start_date, "end": end_date},
        "manifest": manifest_summary,
        "datasets": {},
        "errors": errors,
    }
    loaded: dict[str, pd.DataFrame] = {}
    for dataset in ("financials", "failures", "institutions"):
        files = sorted((processed_root / dataset).rglob("*.parquet"))
        frame = _read_dataset(processed_root, dataset)
        if not files:
            result["datasets"][dataset] = {"status": "missing", "files": 0}
            errors.append(f"{dataset}: no Parquet files found")
            continue
        loaded[dataset] = frame
        item: dict[str, Any] = {
            "status": "ok",
            "files": len(files),
            "rows": len(frame),
            "unique_cert": int(frame["CERT"].nunique(dropna=True)) if "CERT" in frame else None,
            "missing_cert": int(frame["CERT"].isna().sum()) if "CERT" in frame else None,
        }
        if dataset == "financials" and "REPDTE" in frame:
            dates = pd.to_datetime(frame["REPDTE"], errors="coerce")
            duplicate_keys = int(frame.duplicated(["CERT", "REPDTE"]).sum())
            item.update({
                "min_repdte": dates.min().date().isoformat() if dates.notna().any() else None,
                "max_repdte": dates.max().date().isoformat() if dates.notna().any() else None,
                "unique_repdte": int(dates.nunique()),
                "missing_repdte": int(dates.isna().sum()),
                "duplicate_cert_repdte": duplicate_keys,
            })
            if duplicate_keys:
                errors.append(f"financials: {duplicate_keys} duplicate CERT/REPDTE rows")
            if frame["CERT"].isna().any() or dates.isna().any():
                errors.append("financials: missing bank-quarter key fields")
        if dataset == "failures":
            item["missing_faildate"] = int(frame["FAILDATE"].isna().sum()) if "FAILDATE" in frame else None
            item["duplicate_cert_nonnull"] = int(frame.loc[frame["CERT"].notna()].duplicated(["CERT"]).sum()) if "CERT" in frame else None
            if "FAILDATE" in frame and frame["FAILDATE"].isna().any():
                errors.append("failures: missing FAILDATE")
        if dataset == "institutions":
            item["duplicate_cert"] = int(frame.duplicated(["CERT"]).sum()) if "CERT" in frame else None
            if item["duplicate_cert"]:
                errors.append(f"institutions: {item['duplicate_cert']} duplicate CERT rows")
        result["datasets"][dataset] = item

    cert_sets = {name: set(frame["CERT"].dropna().astype("int64")) for name, frame in loaded.items() if "CERT" in frame}
    if cert_sets:
        result["cross_table_certificates"] = {
            "financials_not_in_institutions": sorted(cert_sets.get("financials", set()) - cert_sets.get("institutions", set()))[:100],
            "failures_not_in_institutions": sorted(cert_sets.get("failures", set()) - cert_sets.get("institutions", set()))[:100],
            "institutions_not_in_financials": sorted(cert_sets.get("institutions", set()) - cert_sets.get("financials", set()))[:100],
            "truncated_to": 100,
        }
    result["status"] = "fail" if errors else "ok"
    return result


def write_validation_report(report: dict[str, Any], json_path: Path, summary_path: Path) -> None:
    json_path.parent.mkdir(parents=True, exist_ok=True)
    payload = json.dumps(report, indent=2, sort_keys=True, default=str)
    json_path.write_text(payload, encoding="utf-8")
    summary_path.write_text("FDIC acquisition validation summary\n\n" + payload, encoding="utf-8")
