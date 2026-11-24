"""Part 2: streaming data-quality audit for the Part 1 FDIC outputs.

The audit is deliberately non-destructive. It reports missingness, duplicates,
invalid values, identifier mismatches, reporting gaps, field availability and
distribution summaries without deleting, imputing or silently rewriting data.
"""
from __future__ import annotations

import json
import logging
import math
from collections import defaultdict
from pathlib import Path
from typing import Any, Iterable

import pandas as pd

from bankfail.acquisition.storage import parse_fdic_dates

LOG = logging.getLogger(__name__)

FINANCIAL_NUMERIC_FIELDS = [
    "ASSET", "DEP", "DEPDOM", "EQ", "BRO", "CHBAL", "SC", "LNLSGR", "LNLSNET",
    "LNRE", "LNCI", "LNCON", "LNATRES", "NCLNLS", "P3ASSET", "P9ASSET", "NAASSET",
    "NTLNLS", "ORE", "NETINC", "INTINC", "EINTEXP", "NONII", "NONIX", "ELNATR",
    "ROA", "ROE", "NIMY", "RBC1AAJ", "RBCRWAJ",
]
NON_NEGATIVE_FIELDS = {
    "ASSET", "DEP", "DEPDOM", "BRO", "CHBAL", "SC", "LNLSGR", "LNLSNET", "LNRE",
    "LNCI", "LNCON", "LNATRES", "NCLNLS", "P3ASSET", "P9ASSET", "NAASSET", "ORE",
}
PLAUSIBLE_BOUNDS = {
    "ROA": (-100.0, 100.0), "ROE": (-1000.0, 1000.0), "NIMY": (-100.0, 100.0),
    "RBC1AAJ": (-100.0, 1000.0), "RBCRWAJ": (-100.0, 1000.0),
}


def _jsonable(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(k): _jsonable(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_jsonable(v) for v in value]
    if isinstance(value, (pd.Timestamp, pd.Period)):
        return str(value)
    if hasattr(value, "item"):
        return value.item()
    if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
        return None
    return value


def _parquet_files(processed_root: Path, dataset: str) -> list[Path]:
    return sorted((processed_root / dataset).rglob("*.parquet"))


def _quarter_end(value: Any) -> bool:
    timestamp = pd.to_datetime(value, errors="coerce")
    if pd.isna(timestamp):
        return False
    return timestamp.date() in {
        (timestamp + pd.offsets.QuarterEnd(0)).date(),
    }


def _normalised_cert(value: Any) -> int | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if math.isnan(number):
        return None
    return int(number)


def _normalised_date(value: Any) -> str | None:
    timestamp = pd.to_datetime(value, errors="coerce")
    return None if pd.isna(timestamp) else timestamp.date().isoformat()


def _write_csv(rows: Iterable[dict[str, Any]], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(list(rows)).to_csv(path, index=False)


def _concat_nonempty_csv(paths: list[Path]) -> pd.DataFrame:
    frames = []
    for path in paths:
        if not path.exists() or path.stat().st_size == 0:
            continue
        try:
            frame = pd.read_csv(path)
        except pd.errors.EmptyDataError:
            continue
        if not frame.empty:
            frames.append(frame)
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def _read_frames(files: list[Path]):
    for path in files:
        try:
            yield path, pd.read_parquet(path)
        except Exception as exc:  # preserve the file name in the audit failure
            raise RuntimeError(f"Could not read Parquet audit input: {path}") from exc


def _audit_table(dataset: str, files: list[Path], output_dir: Path) -> tuple[dict[str, Any], set[int], list[dict[str, Any]]]:
    key_columns = ["CERT", "REPDTE"] if dataset == "financials" else ["CERT"]
    required = ["CERT", "REPDTE"] if dataset == "financials" else ["CERT"]
    if dataset == "failures":
        required.append("FAILDATE")
    total_rows = 0
    columns: set[str] = set()
    null_counts: dict[str, int] = defaultdict(int)
    seen_counts: dict[tuple[Any, ...], int] = defaultdict(int)
    duplicate_examples: list[dict[str, Any]] = []
    invalid_rows: list[dict[str, Any]] = []
    distributions: dict[str, list[float]] = defaultdict(list)
    field_availability: list[dict[str, Any]] = []
    certs: set[int] = set()
    cert_dates: dict[int, set[str]] = defaultdict(set)
    date_counts: dict[str, int] = defaultdict(int)
    invalid_counts: dict[str, int] = defaultdict(int)
    invalid_samples: dict[str, list[Any]] = defaultdict(list)

    for file_path, frame in _read_frames(files):
        total_rows += len(frame)
        columns.update(frame.columns)
        period = "all"
        if "REPDTE" in frame and not frame["REPDTE"].empty:
            parsed_dates = pd.to_datetime(frame["REPDTE"], errors="coerce")
            valid_dates = parsed_dates.dropna()
            if not valid_dates.empty:
                period = str(valid_dates.iloc[0].year)
        expected_fields = set(required)
        if dataset == "financials":
            expected_fields.update(FINANCIAL_NUMERIC_FIELDS)
        for column in sorted(set(frame.columns) | expected_fields):
            if column not in frame:
                field_availability.append({
                    "dataset": dataset, "file": str(file_path), "period": period,
                    "field": column, "present_in_file": False, "rows": len(frame),
                    "non_null": 0, "non_null_pct": 0.0, "dtype": None,
                })
                continue
            null_counts[column] += int(frame[column].isna().sum())
            field_availability.append({
                "dataset": dataset, "file": str(file_path), "period": period,
                "field": column, "present_in_file": True, "rows": len(frame),
                "non_null": int(frame[column].notna().sum()),
                "non_null_pct": round(float(frame[column].notna().mean() * 100), 6) if len(frame) else None,
                "dtype": str(frame[column].dtype),
            })
        key_frame = frame[key_columns].copy() if all(c in frame for c in key_columns) else None
        if key_frame is not None:
            key_frame["CERT"] = pd.to_numeric(key_frame["CERT"], errors="coerce").astype("Int64")
            if "REPDTE" in key_frame:
                key_frame["REPDTE"] = pd.to_datetime(key_frame["REPDTE"], errors="coerce").dt.strftime("%Y-%m-%d")
            # A missing CERT cannot identify a duplicate bank event.  Keep its
            # missingness count, but do not let all null rows form one giant
            # duplicate key group.
            key_frame = key_frame[key_frame["CERT"].notna()]
            if not key_frame.empty:
                grouped = key_frame.groupby(key_columns, dropna=False, sort=False).size()
                for key, count in grouped.items():
                    normalised_key = key if isinstance(key, tuple) else (key,)
                    normalised_key = tuple(_normalised_cert(value) if index == 0 else (None if pd.isna(value) else str(value)) for index, value in enumerate(normalised_key))
                    seen_counts[normalised_key] += int(count)
                    if count > 1 and len(duplicate_examples) < 100:
                        duplicate_examples.append({"dataset": dataset, "key": "|".join(str(value) for value in normalised_key), "file": str(file_path)})
        if "CERT" in frame:
            for cert in pd.to_numeric(frame["CERT"], errors="coerce").dropna().astype(int):
                certs.add(int(cert))
        if dataset == "financials" and "REPDTE" in frame:
            cert_values = pd.to_numeric(frame.get("CERT", pd.Series(index=frame.index, dtype=object)), errors="coerce").astype("Int64")
            date_values = pd.to_datetime(frame["REPDTE"], errors="coerce").dt.strftime("%Y-%m-%d")
            pairs = pd.DataFrame({"CERT": cert_values, "REPDTE": date_values}).dropna()
            if not pairs.empty:
                grouped_dates = pairs.groupby("CERT")["REPDTE"].agg(lambda values: set(values.astype(str)))
                for cert, values in grouped_dates.items():
                    cert_dates[int(cert)].update(values)
                date_counts.update(pairs["REPDTE"].value_counts().to_dict())
        for required_field in required:
            if required_field not in frame:
                invalid_counts[f"missing_column:{required_field}"] += 1
                continue
            count = int(frame[required_field].isna().sum())
            if count:
                invalid_counts[f"missing_required:{required_field}"] += count
        if dataset == "financials":
            if "REPDTE" in frame:
                dates = pd.to_datetime(frame["REPDTE"], errors="coerce")
                invalid_date_count = int(dates.isna().sum())
                invalid_counts["invalid_repdte"] += invalid_date_count
                valid_dates = dates.dropna()
                non_quarter_end = valid_dates.ne(valid_dates + pd.offsets.QuarterEnd(0))
                invalid_counts["non_quarter_end_repdte"] += int(non_quarter_end.sum())
            for field in FINANCIAL_NUMERIC_FIELDS:
                if field not in frame:
                    continue
                numeric = pd.to_numeric(frame[field], errors="coerce")
                parse_failures = int((frame[field].notna() & numeric.isna()).sum())
                invalid_counts[f"nonnumeric:{field}"] += parse_failures
                if parse_failures and len(invalid_samples[f"nonnumeric:{field}"]) < 5:
                    invalid_samples[f"nonnumeric:{field}"].extend(frame.loc[frame[field].notna() & numeric.isna(), field].head(5).tolist())
                if field in NON_NEGATIVE_FIELDS:
                    invalid_counts[f"negative:{field}"] += int((numeric < 0).sum())
                elif field == "NTLNLS":
                    # Deliberately report, but do not fail, observed negatives:
                    # the audit rule was tested and rejected after the smoke
                    # data showed that negative net charge-offs are common.
                    invalid_counts["observed_negative:NTLNLS"] += int((numeric < 0).sum())
                if field in PLAUSIBLE_BOUNDS:
                    low, high = PLAUSIBLE_BOUNDS[field]
                    invalid_counts[f"outside_plausible_range:{field}"] += int(((numeric < low) | (numeric > high)).sum())
                distributions[field].extend(numeric.dropna().sample(min(5000, numeric.notna().sum()), random_state=42).tolist())
        elif dataset == "failures":
            if "FAILDATE" in frame:
                invalid_counts["invalid_faildate"] += int(parse_fdic_dates(frame["FAILDATE"]).isna().sum())

    missingness = []
    for column in sorted(columns):
        count = null_counts[column]
        missingness.append({"dataset": dataset, "field": column, "rows": total_rows, "missing_count": count, "missing_pct": round(count / total_rows * 100, 6) if total_rows else None})
    duplicates = [{"dataset": dataset, "key": "|".join("<NULL>" if x is None else str(x) for x in key), "count": count} for key, count in seen_counts.items() if count > 1]
    _write_csv(missingness, output_dir / "missingness.csv")
    _write_csv(duplicate_examples + [{"dataset": dataset, "key": row["key"], "count": row["count"]} for row in duplicates[:100]], output_dir / "duplicates.csv")
    _write_csv([{"dataset": dataset, "rule": rule, "count": count, "sample_values": json.dumps(invalid_samples.get(rule, []), default=str)} for rule, count in sorted(invalid_counts.items()) if count], output_dir / "invalid_values.csv")
    _write_csv(field_availability, output_dir / "field_availability.csv")
    distribution_rows = []
    for field, values in distributions.items():
        series = pd.Series(values, dtype="float64")
        distribution_rows.append({"dataset": dataset, "field": field, "sample_n": len(series), "sample_based": True, "mean": series.mean(), "std": series.std(), "min": series.min(), "p01": series.quantile(.01), "p50": series.quantile(.50), "p99": series.quantile(.99), "max": series.max()})
    _write_csv(distribution_rows, output_dir / "distribution_summary.csv")
    gaps = []
    if dataset == "financials":
        observed_by_cert = {cert: set(values) for cert, values in cert_dates.items()}
        for cert, values in observed_by_cert.items():
            dates = sorted(pd.to_datetime(list(values)))
            if len(dates) < 2:
                continue
            expected = pd.date_range(dates[0], dates[-1], freq="QE-DEC")
            missing = [d.date().isoformat() for d in expected if d.date().isoformat() not in values]
            if missing:
                gaps.append({"CERT": cert, "observed_quarters": len(dates), "first_repdte": dates[0].date().isoformat(), "last_repdte": dates[-1].date().isoformat(), "missing_quarter_count": len(missing), "missing_quarters": ",".join(missing)})
    _write_csv(gaps, output_dir / "reporting_gaps.csv")
    duplicate_rows = sum(count - 1 for count in seen_counts.values() if count > 1)
    missing_key_rows = sum(null_counts.get(field, 0) for field in required)
    summary = {
        "dataset": dataset,
        "files": len(files),
        "rows": total_rows,
        "columns": sorted(columns),
        "unique_cert": len(certs),
        "missingness_fields": len(missingness),
        "missing_key_rows": missing_key_rows,
        "duplicate_key_groups": len(duplicates),
        "duplicate_nonnull_key_groups": len(duplicates),
        "duplicate_rows": duplicate_rows,
        "duplicate_nonnull_rows": duplicate_rows,
        "invalid_rule_counts": {k: v for k, v in invalid_counts.items() if v},
        "reporting_gap_banks": len(gaps),
        "unique_reporting_dates": len(date_counts),
        "min_reporting_date": min(date_counts) if date_counts else None,
        "max_reporting_date": max(date_counts) if date_counts else None,
    }
    return summary, certs, field_availability


def _failure_event_audit(processed_root: Path, output_dir: Path) -> dict[str, Any]:
    """Produce evidence for missing CERTs, repeated events, RESTYPE and exits."""
    failure_files = _parquet_files(processed_root, "failures")
    financial_files = _parquet_files(processed_root, "financials")
    institution_files = _parquet_files(processed_root, "institutions")
    if not failure_files:
        return {"failure_anomalies_file": None, "post_failure_reports_file": None}
    failures = pd.concat((pd.read_parquet(path) for path in failure_files), ignore_index=True)
    failures["CERT_normalized"] = pd.to_numeric(failures.get("CERT"), errors="coerce").astype("Int64")
    failures["FAILDATE_normalized"] = parse_fdic_dates(failures["FAILDATE"]) if "FAILDATE" in failures else pd.NaT
    failures["RESTYPE_normalized"] = failures.get("RESTYPE", pd.Series(index=failures.index, dtype="string")).astype("string").str.strip().str.upper()
    linked_assistance = failures[failures["CERT_normalized"].notna() & failures["RESTYPE_normalized"].eq("ASSISTANCE")].copy()

    evidence: list[dict[str, Any]] = []
    missing = failures[failures["CERT_normalized"].isna()]
    missing_dates = missing["FAILDATE_normalized"].dropna()
    evidence.append({
        "problem": "failures_missing_cert",
        "count": int(len(missing)),
        "faildate_min": missing_dates.min().date().isoformat() if not missing_dates.empty else None,
        "faildate_max": missing_dates.max().date().isoformat() if not missing_dates.empty else None,
        "faildate_ge_1984": int((missing_dates >= pd.Timestamp("1984-01-01")).sum()),
        "restype_counts": json.dumps(missing["RESTYPE_normalized"].value_counts(dropna=False).to_dict(), default=str),
        "handling": "Retain in the source audit but exclude from bank-linked panel rows because CERT is required for linkage.",
        "rationale": "A missing CERT cannot be matched to a Financials bank-quarter key; the date range is reported to test historical label impact.",
    })

    nonnull = failures.dropna(subset=["CERT_normalized"]).copy()
    repeated = nonnull[nonnull.duplicated("CERT_normalized", keep=False)]
    repeated_groups = repeated.groupby("CERT_normalized", sort=True)
    for cert, group in repeated_groups:
        evidence.append({
            "problem": "repeated_failure_events",
            "CERT": int(cert),
            "count": int(len(group)),
            "faildate_min": group["FAILDATE_normalized"].min().date().isoformat() if group["FAILDATE_normalized"].notna().any() else None,
            "faildate_max": group["FAILDATE_normalized"].max().date().isoformat() if group["FAILDATE_normalized"].notna().any() else None,
            "restype_counts": json.dumps(group["RESTYPE_normalized"].value_counts(dropna=False).to_dict(), default=str),
            "names": " | ".join(group.get("NAME", pd.Series(dtype="string")).dropna().astype(str).unique()),
            "handling": "Treat RESTYPE=FAILURE as the failure event; keep RESTYPE=ASSISTANCE as non-failure event evidence.",
            "rationale": "The same CERT can appear for an assistance transaction and a later failure; taking min(all dates) would create label leakage/misclassification.",
        })

    evidence.append({
        "problem": "assistance_events",
        "count": int((failures["RESTYPE_normalized"] == "ASSISTANCE").sum()),
        "unique_cert": int(failures.loc[failures["RESTYPE_normalized"] == "ASSISTANCE", "CERT_normalized"].nunique()),
        "restype_counts": json.dumps(failures["RESTYPE_normalized"].value_counts(dropna=False).to_dict(), default=str),
        "handling": "Do not classify assistance as failure; retain it as an audit field and expose assistance_nonfailure evidence in the exit table.",
        "rationale": "The smoke data contains assistance records for banks that continue to report; the failure label uses explicit RESTYPE=FAILURE only.",
    })

    if "COST" in failures:
        cost = pd.to_numeric(failures["COST"], errors="coerce")
        evidence.append({
            "problem": "failure_cost_quality",
            "count": int(cost.isna().sum()),
            "negative_cost": int((cost < 0).sum()),
            "handling": "Keep COST as nullable numeric evidence; do not impute or use it as a feature.",
            "rationale": "Failure cost is an outcome-side field and must not enter point-in-time features.",
        })

    anomaly_path = output_dir / "failure_anomalies.csv"
    _write_csv(evidence, anomaly_path)

    assistance_review_path = output_dir / "assistance_bank_review.csv"
    assistance_review: list[dict[str, Any]] = []
    if financial_files and not linked_assistance.empty:
        financials = pd.concat((pd.read_parquet(path) for path in financial_files), ignore_index=True)
        financials["CERT_normalized"] = pd.to_numeric(financials["CERT"], errors="coerce").astype("Int64")
        financials["REPDTE_normalized"] = pd.to_datetime(financials["REPDTE"], errors="coerce")
        assistance_dates = linked_assistance.groupby("CERT_normalized", as_index=False).agg(assistance_date=("FAILDATE_normalized", "min"), assistance_event_count=("CERT_normalized", "size"))
        after = financials.merge(assistance_dates, on="CERT_normalized", how="inner")
        after = after[after["REPDTE_normalized"] >= after["assistance_date"]]
        if not after.empty:
            institution_lookup = pd.DataFrame()
            if institution_files:
                institution_lookup = pd.concat((pd.read_parquet(path) for path in institution_files), ignore_index=True)
                institution_lookup["CERT_normalized"] = pd.to_numeric(institution_lookup["CERT"], errors="coerce").astype("Int64")
                institution_lookup["institution_end_date"] = parse_fdic_dates(institution_lookup["ENDEFYMD"]) if "ENDEFYMD" in institution_lookup else pd.NaT
                institution_lookup = institution_lookup.sort_values("CERT_normalized").drop_duplicates("CERT_normalized")
            for cert, group in after.groupby("CERT_normalized", sort=True):
                institution_row = institution_lookup.loc[institution_lookup["CERT_normalized"] == cert].iloc[0] if not institution_lookup.empty and (institution_lookup["CERT_normalized"] == cert).any() else None
                assistance_review.append({
                    "CERT": int(cert),
                    "assistance_date": group["assistance_date"].iloc[0].date().isoformat(),
                    "assistance_event_count": int(group["assistance_event_count"].iloc[0]),
                    "reports_after_assistance": int(len(group)),
                    "last_report_date": group["REPDTE_normalized"].max().date().isoformat(),
                    "ACTIVE_snapshot": None if institution_row is None else institution_row.get("ACTIVE"),
                    "institution_end_date": None if institution_row is None or pd.isna(institution_row.get("institution_end_date")) else institution_row["institution_end_date"].date().isoformat(),
                    "handling": "Review as non-failure assistance; do not convert to a failure label solely because the bank kept reporting.",
                })
    _write_csv(assistance_review, assistance_review_path)

    post_path = output_dir / "post_failure_reports.csv"
    post_rows: list[dict[str, Any]] = []
    explicit_failures = failures[failures["RESTYPE_normalized"] == "FAILURE"].dropna(subset=["CERT_normalized", "FAILDATE_normalized"])
    if financial_files and not explicit_failures.empty:
        financials = pd.concat((pd.read_parquet(path) for path in financial_files), ignore_index=True)
        financials["CERT_normalized"] = pd.to_numeric(financials["CERT"], errors="coerce").astype("Int64")
        financials["REPDTE_normalized"] = pd.to_datetime(financials["REPDTE"], errors="coerce")
        first_failures = explicit_failures.groupby("CERT_normalized", as_index=False).agg(failure_date=("FAILDATE_normalized", "min"))
        joined = financials.merge(first_failures, on="CERT_normalized", how="inner")
        joined = joined[joined["REPDTE_normalized"] >= joined["failure_date"]]
        for cert, group in joined.groupby("CERT_normalized", sort=True):
            post_rows.append({
                "CERT": int(cert),
                "failure_date": group["failure_date"].iloc[0].date().isoformat(),
                "post_report_count": int(len(group)),
                "post_report_dates": "|".join(sorted(group["REPDTE_normalized"].dt.date.astype(str).unique())),
                "handling": "Censor post-failure reports; inspect the source event separately.",
            })
    _write_csv(post_rows, post_path)
    return {
        "failure_anomalies_file": str(anomaly_path),
        "assistance_bank_review_file": str(assistance_review_path),
        "post_failure_reports_file": str(post_path),
        "post_failure_report_rows": int(sum(row["post_report_count"] for row in post_rows)),
        "post_failure_report_banks": len(post_rows),
        "assistance_banks_with_later_reports": len(assistance_review),
    }


def _make_figures(output_dir: Path, summaries: list[dict[str, Any]]) -> list[str]:
    try:
        import matplotlib
        matplotlib.use("Agg", force=True)
        import matplotlib.pyplot as plt
    except ImportError:
        return []
    figures: list[str] = []
    missing_paths = sorted(output_dir.glob("*/missingness.csv"))
    if missing_paths:
        missing_frame = _concat_nonempty_csv(missing_paths)
        frame = missing_frame.sort_values("missing_pct", ascending=False).head(20) if not missing_frame.empty else pd.DataFrame()
        if not frame.empty:
            fig, ax = plt.subplots(figsize=(10, 6))
            labels = frame["dataset"].astype(str) + ":" + frame["field"].astype(str)
            ax.barh(labels.iloc[::-1], frame["missing_pct"].iloc[::-1], color="#336699", edgecolor="none")
            ax.set_xlabel("Missing values (%)")
            max_m = frame['missing_pct'].max()
            mean_m = frame['missing_pct'].mean()
            ax.set_title(f"Top 20 Missingness Rates Across FDIC Fields (Max: {max_m:.1f}%, Mean: {mean_m:.1f}%)")
            ax.spines['top'].set_visible(False)
            ax.spines['right'].set_visible(False)
            fig.tight_layout()
            path = output_dir / "figures" / "missingness_top20.png"
            path.parent.mkdir(parents=True, exist_ok=True)
            fig.savefig(path, dpi=150)
            plt.close(fig)
            figures.append(str(path))
    coverage_paths = sorted(output_dir.glob("*/field_availability.csv"))
    if coverage_paths:
        frame = _concat_nonempty_csv(coverage_paths)
        frame = frame[(frame["dataset"] == "financials") & (frame["field"].isin(["ASSET", "DEP", "EQ", "NETINC", "ROA", "RBC1AAJ"]))]
        if not frame.empty:
            grouped = frame.groupby(["period", "field"], as_index=False)["non_null_pct"].mean()
            fig, ax = plt.subplots(figsize=(10, 6))
            for field, group in grouped.groupby("field"):
                group = group.sort_values("period")
                ax.plot(group["period"].astype(str), group["non_null_pct"], marker=".", label=field)
            ax.set_ylim(0, 105)
            ax.set_ylabel("Non-null (%)")
            p_min = grouped['period'].min()
            p_max = grouped['period'].max()
            p_mean = grouped['non_null_pct'].mean()
            ax.set_title(f"Financials Key Field Availability ({p_min}–{p_max}, Mean Non-Null: {p_mean:.1f}%)")
            ax.spines['top'].set_visible(False)
            ax.spines['right'].set_visible(False)
            ax.legend(ncol=3, frameon=False)
            fig.tight_layout()
            path = output_dir / "figures" / "financial_field_availability.png"
            path.parent.mkdir(parents=True, exist_ok=True)
            fig.savefig(path, dpi=150)
            plt.close(fig)
            figures.append(str(path))
    return figures


def _audit_quarterly_iqr_and_variance(files: list[Path], output_dir: Path) -> dict[str, Any]:
    """Calculate per-quarter IQR fences and zero-variance flags per numeric field.

    Rows are never deleted; this is a non-destructive audit pass flagging outliers
    and constancy.
    """
    iqr_rows: list[dict[str, Any]] = []
    quarter_frames: dict[str, list[pd.DataFrame]] = defaultdict(list)
    for path in files:
        try:
            df = pd.read_parquet(path)
            if "REPDTE" in df.columns and not df["REPDTE"].empty:
                df["REPDTE_norm"] = pd.to_datetime(df["REPDTE"], errors="coerce").dt.strftime("%Y-%m-%d")
                for q, grp in df.groupby("REPDTE_norm"):
                    quarter_frames[str(q)].append(grp)
        except Exception as exc:
            LOG.warning("Could not read Parquet file for IQR audit %s: %s", path, exc)

    for q_date in sorted(quarter_frames.keys()):
        q_df = pd.concat(quarter_frames[q_date], ignore_index=True)
        for field in FINANCIAL_NUMERIC_FIELDS:
            if field not in q_df.columns:
                continue
            series = pd.to_numeric(q_df[field], errors="coerce").dropna()
            n = len(series)
            if n == 0:
                continue
            q1 = float(series.quantile(0.25))
            q3 = float(series.quantile(0.75))
            iqr = float(q3 - q1)
            lower_fence = float(q1 - 1.5 * iqr)
            upper_fence = float(q3 + 1.5 * iqr)
            outlier_low = int((series < lower_fence).sum())
            outlier_high = int((series > upper_fence).sum())
            total_outliers = outlier_low + outlier_high
            outlier_pct = round(total_outliers / n * 100, 4) if n > 0 else 0.0
            variance = float(series.var(ddof=1)) if n > 1 else 0.0
            std = float(series.std(ddof=1)) if n > 1 else 0.0
            is_zero_variance = bool(n > 1 and (variance == 0.0 or series.min() == series.max()))
            iqr_rows.append({
                "quarter": q_date,
                "field": field,
                "count": n,
                "mean": round(float(series.mean()), 4),
                "std": round(std, 4),
                "variance": round(variance, 6),
                "q1": round(q1, 4),
                "q3": round(q3, 4),
                "iqr": round(iqr, 4),
                "lower_fence": round(lower_fence, 4),
                "upper_fence": round(upper_fence, 4),
                "outlier_low_count": outlier_low,
                "outlier_high_count": outlier_high,
                "total_outliers": total_outliers,
                "outlier_pct": outlier_pct,
                "zero_variance_flag": is_zero_variance,
            })

    by_quarter_path = output_dir / "outlier_iqr_by_quarter.csv"
    _write_csv(iqr_rows, by_quarter_path)

    summary_path = output_dir / "outlier_iqr_summary.csv"
    if iqr_rows:
        sum_df = pd.DataFrame(iqr_rows)
        grouped = sum_df.groupby("field", as_index=False).agg(
            total_records=("count", "sum"),
            total_outliers=("total_outliers", "sum"),
            quarters_evaluated=("quarter", "count"),
            zero_variance_quarters=("zero_variance_flag", "sum"),
            mean_iqr=("iqr", "mean"),
            avg_lower_fence=("lower_fence", "mean"),
            avg_upper_fence=("upper_fence", "mean"),
        )
        grouped["overall_outlier_pct"] = (grouped["total_outliers"] / grouped["total_records"] * 100).round(4)
        _write_csv(grouped.to_dict(orient="records"), summary_path)
    else:
        _write_csv([], summary_path)

    return {
        "outlier_iqr_by_quarter_file": str(by_quarter_path),
        "outlier_iqr_summary_file": str(summary_path),
        "total_evaluations": len(iqr_rows),
        "total_outliers_flagged": sum(r["total_outliers"] for r in iqr_rows),
        "zero_variance_instances": sum(r["zero_variance_flag"] for r in iqr_rows),
    }


def run_quality_audit(processed_root: Path, output_dir: Path, manifest_path: Path | None = None) -> dict[str, Any]:
    output_dir.mkdir(parents=True, exist_ok=True)
    summaries: list[dict[str, Any]] = []
    cert_sets: dict[str, set[int]] = {}
    for dataset in ("financials", "failures", "institutions"):
        files = _parquet_files(processed_root, dataset)
        if not files:
            summaries.append({"dataset": dataset, "status": "missing", "files": 0})
            continue
        summary, certs, _ = _audit_table(dataset, files, output_dir / dataset)
        summaries.append({"status": "ok", **summary})
        cert_sets[dataset] = certs
    cross_rows = []
    for left, right in (("financials", "institutions"), ("failures", "institutions"), ("institutions", "financials")):
        if left in cert_sets and right in cert_sets:
            unmatched = sorted(cert_sets[left] - cert_sets[right])
            cross_rows.append({"left_dataset": left, "right_dataset": right, "left_unique_cert": len(cert_sets[left]), "right_unique_cert": len(cert_sets[right]), "unmatched_count": len(unmatched), "unmatched_sample": ",".join(map(str, unmatched[:100]))})
    _write_csv(cross_rows, output_dir / "identifier_consistency.csv")
    manifest_summary = None
    if manifest_path and manifest_path.exists():
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        chunks = list(manifest.get("chunks", {}).values())
        manifest_summary = {"chunks": len(chunks), "complete_chunks": sum(c.get("status") == "complete" for c in chunks), "incomplete_chunks": sum(c.get("status") != "complete" for c in chunks)}
    report = {"audit_version": "part2-v1", "non_destructive": True, "datasets": summaries, "identifier_consistency": cross_rows, "manifest": manifest_summary, "artifacts": [str(p) for p in sorted(output_dir.rglob("*")) if p.is_file()]}
    report = _jsonable(report)
    (output_dir / "data_quality_report.json").write_text(json.dumps(report, indent=2, sort_keys=True), encoding="utf-8")
    pd.DataFrame(summaries).to_csv(output_dir / "data_quality_summary.csv", index=False)
    event_evidence = _failure_event_audit(processed_root, output_dir)
    fin_files = _parquet_files(processed_root, "financials")
    iqr_evidence = _audit_quarterly_iqr_and_variance(fin_files, output_dir)
    figures = _make_figures(output_dir, summaries)
    report["figures"] = figures
    report["failure_event_evidence"] = event_evidence
    report["outlier_iqr_audit"] = iqr_evidence
    report["quality_gate"] = {
        "status": "pass" if not any(item.get("dataset") == "financials" and item.get("duplicate_cert_repdte", 0) for item in summaries) else "fail",
        "rule": "Financials CERT/REPDTE must be unique; source anomalies are retained and handled explicitly rather than silently dropped.",
    }
    (output_dir / "data_quality_report.json").write_text(json.dumps(_jsonable(report), indent=2, sort_keys=True), encoding="utf-8")
    return report
