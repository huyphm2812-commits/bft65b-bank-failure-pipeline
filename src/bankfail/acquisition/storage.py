from __future__ import annotations

import gzip
import hashlib
import json
import os
from pathlib import Path
from typing import Any

import pandas as pd


# The API returns an ``ID`` field even when it is not requested.  It is an
# API-row identifier, not a modelling key, so it is intentionally absent from
# every analytical schema below.  Keeping one schema per dataset prevents a
# page with all-null values from changing the Parquet type of a column.
DATASET_SCHEMA_DEFINITIONS: dict[str, dict[str, str]] = {
    "financials": {
        "CERT": "int64",
        "REPDTE": "timestamp[ms]",
        "STNAME": "string",
        "ASSET": "float64",
        "DEP": "float64",
        "DEPDOM": "float64",
        "EQ": "float64",
        "BRO": "float64",
        "CHBAL": "float64",
        "SC": "float64",
        "LNLSGR": "float64",
        "LNLSNET": "float64",
        "LNRE": "float64",
        "LNCI": "float64",
        "LNCON": "float64",
        "LNATRES": "float64",
        "NCLNLS": "float64",
        "P3ASSET": "float64",
        "P9ASSET": "float64",
        "NAASSET": "float64",
        "NTLNLS": "float64",
        "ORE": "float64",
        "NETINC": "float64",
        "INTINC": "float64",
        "EINTEXP": "float64",
        "NONII": "float64",
        "NONIX": "float64",
        "ELNATR": "float64",
        "ROA": "float64",
        "ROE": "float64",
        "NIMY": "float64",
        "RBC1AAJ": "float64",
        "RBCRWAJ": "float64",
    },
    "failures": {
        "CERT": "int64",
        "NAME": "string",
        "CITYST": "string",
        "FAILDATE": "timestamp[ms]",
        "FAILYR": "int64",
        "COST": "float64",
        "QBFASSET": "float64",
        "QBFDEP": "float64",
        "RESTYPE": "string",
    },
    "institutions": {
        "CERT": "int64",
        "NAME": "string",
        "STNAME": "string",
        "BKCLASS": "string",
        "ACTIVE": "int64",
        "ESTYMD": "timestamp[ms]",
        "ENDEFYMD": "timestamp[ms]",
        "CHANGEC1": "float64",
        "INSDATE": "timestamp[ms]",
    },
}

DATE_COLUMNS = {
    dataset: {name for name, kind in definition.items() if kind.startswith("timestamp")}
    for dataset, definition in DATASET_SCHEMA_DEFINITIONS.items()
}


def _pyarrow_schema(dataset: str):
    import pyarrow as pa

    try:
        definition = DATASET_SCHEMA_DEFINITIONS[dataset]
    except KeyError as exc:
        raise ValueError(f"Unknown FDIC dataset: {dataset}") from exc
    fields = []
    for name, kind in definition.items():
        if kind == "string":
            data_type = pa.string()
        elif kind == "int64":
            data_type = pa.int64()
        elif kind == "float64":
            data_type = pa.float64()
        elif kind == "timestamp[ms]":
            data_type = pa.timestamp("ms")
        else:  # pragma: no cover - guarded by the definitions above
            raise ValueError(f"Unsupported schema type {kind!r} for {dataset}.{name}")
        fields.append(pa.field(name, data_type, nullable=True))
    return pa.schema(fields)


def parse_fdic_dates(values: pd.Series) -> pd.Series:
    """Parse the date encodings observed in FDIC responses with fixed formats."""
    text = values.astype("string").str.strip()
    parsed = pd.Series(pd.NaT, index=values.index, dtype="datetime64[ns]")
    formats = (
        "%Y%m%d",
        "%Y-%m-%d",
        "%Y/%m/%d",
        "%m/%d/%Y",
        "%m/%d/%y",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M:%S.%f",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%dT%H:%M:%S.%f",
    )
    for date_format in formats:
        missing = parsed.isna() & text.notna()
        if missing.any():
            parsed.loc[missing] = pd.to_datetime(text.loc[missing], format=date_format, errors="coerce")
    return parsed.dt.normalize().astype("datetime64[ms]")


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def atomic_write_bytes(path: Path, data: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_suffix(path.suffix + ".part")
    temp.write_bytes(data)
    os.replace(temp, path)


def write_raw_page(path: Path, payload: dict[str, Any]) -> None:
    raw = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    atomic_write_bytes(path, gzip.compress(raw, compresslevel=6))


def analytical_frame(records: list[dict[str, Any]], dataset: str) -> pd.DataFrame:
    import logging

    definition = DATASET_SCHEMA_DEFINITIONS.get(dataset)
    if definition is None:
        raise ValueError(f"Unknown FDIC dataset: {dataset}")
    raw = pd.DataFrame([r.get("data", r) for r in records]).drop(columns=["ID"], errors="ignore")
    unexpected = sorted(set(raw.columns) - set(definition))
    if unexpected:
        raise ValueError(f"Unexpected columns in {dataset} API page: {unexpected}")
    frame = raw.reindex(columns=list(definition))
    for column, kind in definition.items():
        if kind == "string":
            frame[column] = frame[column].astype("string")
        elif kind == "int64":
            frame[column] = pd.to_numeric(frame[column], errors="coerce").astype("Int64")
        elif kind == "float64":
            frame[column] = pd.to_numeric(frame[column], errors="coerce").astype("float64")
        elif kind == "timestamp[ms]":
            frame[column] = parse_fdic_dates(frame[column])
    if raw.empty:
        logging.getLogger(__name__).debug("Writing an empty %s page with its explicit schema", dataset)
    return frame


def write_parquet_page(path: Path, records: list[dict[str, Any]], dataset: str, compression: str = "snappy") -> dict[str, Any]:
    import pyarrow as pa
    import pyarrow.parquet as pq

    frame = analytical_frame(records, dataset)
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_suffix(path.suffix + ".part")
    schema = _pyarrow_schema(dataset)
    table = pa.Table.from_pandas(frame, schema=schema, preserve_index=False, safe=False)
    pq.write_table(table, temp, compression=compression)
    os.replace(temp, path)
    schema_text = ";".join(f"{name}:{kind}" for name, kind in DATASET_SCHEMA_DEFINITIONS[dataset].items())
    return {
        "rows": int(len(frame)),
        "columns": list(frame.columns),
        "schema": schema_text,
        "sha256": sha256(path),
        "file_size": path.stat().st_size,
    }
