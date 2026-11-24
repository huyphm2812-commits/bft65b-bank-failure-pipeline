"""Part 3: bank-quarter panel, exit classification and forward labels.

This module is deliberately conservative. It never fills missing quarters, never
uses institution snapshot fields as modelling features, and masks labels after
2025Q2 because the four-quarter outcome window is not fully in scope there.
"""
from __future__ import annotations

import ast
import json
from pathlib import Path
from typing import Any

import pandas as pd

from bankfail.acquisition.storage import parse_fdic_dates


LABEL_CUTOFF = pd.Timestamp("2025-06-30")
QUARTER_FREQUENCY = "QE-DEC"


class PanelBuildError(RuntimeError):
    """Raised when the source data cannot support an unambiguous panel."""


def _cast_datetime_columns_to_ms(frame: pd.DataFrame) -> pd.DataFrame:
    """Make every persisted datetime Spark-compatible timestamp[ms]."""
    result = frame.copy()
    for column in result.columns:
        if pd.api.types.is_datetime64_any_dtype(result[column]):
            result[column] = result[column].astype("datetime64[ms]")
    return result


def parse_fdic_date(values: pd.Series) -> pd.Series:
    return parse_fdic_dates(values)


def _cert(values: pd.Series) -> pd.Series:
    return pd.to_numeric(values, errors="coerce").astype("Int64")


def _require_columns(frame: pd.DataFrame, required: list[str], name: str) -> None:
    missing = sorted(set(required) - set(frame.columns))
    if missing:
        raise PanelBuildError(f"{name} is missing required columns: {missing}")


def read_processed_dataset(processed_root: Path, dataset: str) -> pd.DataFrame:
    files = sorted((processed_root / dataset).rglob("*.parquet"))
    if not files:
        raise FileNotFoundError(f"No Parquet files found for {dataset} under {processed_root}")
    return pd.concat((pd.read_parquet(path) for path in files), ignore_index=True)


def classify_exits(failures: pd.DataFrame, institutions: pd.DataFrame) -> pd.DataFrame:
    _require_columns(failures, ["CERT", "FAILDATE"], "Failures")
    _require_columns(institutions, ["CERT"], "Institutions")
    failure = failures.copy()
    institution = institutions.copy()
    failure["CERT"] = _cert(failure["CERT"])
    failure["failure_date"] = parse_fdic_date(failure["FAILDATE"])
    if "RESTYPE" in failure:
        failure["RESTYPE_normalized"] = failure["RESTYPE"].astype("string").str.strip().str.upper()
    else:
        # Synthetic/legacy fixtures may predate RESTYPE.  The acquired schema
        # always contains it, but treating a missing legacy column as the
        # historical failure table keeps the panel API backwards-compatible.
        failure["RESTYPE_normalized"] = "FAILURE"
    failure["is_explicit_failure"] = failure["RESTYPE_normalized"].eq("FAILURE")
    failure["is_assistance"] = failure["RESTYPE_normalized"].eq("ASSISTANCE")
    linked_failure = failure[failure["CERT"].notna() & failure["is_explicit_failure"]].copy()
    linked_assistance = failure[failure["CERT"].notna() & failure["is_assistance"]].copy()
    failure_summary = linked_failure.groupby("CERT", as_index=False).agg(
        failure_date=("failure_date", "min"),
        failure_record_count=("CERT", "size"),
    )
    if "COST" in linked_failure:
        linked_failure["failure_cost_numeric"] = pd.to_numeric(linked_failure["COST"], errors="coerce")
        cost_summary = linked_failure.groupby("CERT", as_index=False)["failure_cost_numeric"].first()
        failure_summary = failure_summary.merge(cost_summary, on="CERT", how="left")
    assistance_summary = linked_assistance.groupby("CERT", as_index=False).agg(
        assistance_date=("failure_date", "min"), assistance_record_count=("CERT", "size")
    )
    restype_summary = failure.dropna(subset=["CERT"]).groupby("CERT", as_index=False).agg(
        failure_event_count=("is_explicit_failure", "sum"),
        assistance_event_count=("is_assistance", "sum"),
        restype_values=("RESTYPE_normalized", lambda values: "|".join(sorted(set(values.dropna().astype(str))))),
    )

    institution["CERT"] = _cert(institution["CERT"])
    if "ENDEFYMD" in institution:
        institution["institution_end_date"] = parse_fdic_date(institution["ENDEFYMD"])
    else:
        institution["institution_end_date"] = pd.NaT
    keep = ["CERT", "institution_end_date"]
    for column in ("ACTIVE", "CHANGEC1", "NAME"):
        if column in institution:
            keep.append(column)
    institution_summary = institution.dropna(subset=["CERT"])[keep].copy()
    institution_summary = institution_summary.sort_values(["CERT"]).drop_duplicates("CERT", keep="first")
    if "institution_end_date" in institution_summary:
        end_summary = institution.dropna(subset=["CERT"]).groupby("CERT", as_index=False)["institution_end_date"].min()
        institution_summary = institution_summary.drop(columns=["institution_end_date"]).merge(end_summary, on="CERT", how="left")

    exits = institution_summary.merge(restype_summary, on="CERT", how="outer")
    exits = exits.merge(failure_summary, on="CERT", how="outer")
    exits = exits.merge(assistance_summary, on="CERT", how="outer")
    exits["has_failure_record"] = exits["failure_date"].notna()
    exits["has_end_date"] = exits["institution_end_date"].notna()
    exits["has_assistance_record"] = exits["assistance_date"].notna()
    exits["exit_classification"] = "active_or_no_known_exit"
    exits.loc[exits["has_end_date"] & ~exits["has_failure_record"], "exit_classification"] = "merger_other"
    exits.loc[exits["has_assistance_record"] & ~exits["has_failure_record"] & ~exits["has_end_date"], "exit_classification"] = "assistance_nonfailure"
    exits.loc[exits["has_failure_record"], "exit_classification"] = "failure"
    exits["classification_note"] = "Only RESTYPE=FAILURE defines failure; RESTYPE=ASSISTANCE is non-failure evidence. ENDEFYMD is treated as a non-failure exit date, not proof of merger."
    return exits


def _future_report_completeness(panel: pd.DataFrame, cutoff: pd.Timestamp) -> tuple[pd.Series, pd.Series]:
    """Check q+1..q+4 using observed records; never bridge a missing quarter."""
    in_scope = panel["REPDTE"] <= cutoff
    observed = pd.MultiIndex.from_frame(panel[["CERT", "REPDTE"]])
    missing_count = pd.Series(0, index=panel.index, dtype="int64")
    for offset in range(1, 5):
        future_date = panel["REPDTE"] + pd.offsets.QuarterEnd(offset)
        candidate = pd.MultiIndex.from_arrays([panel["CERT"], future_date])
        missing = ~candidate.isin(observed) & in_scope.to_numpy()
        missing_count = missing_count + missing.astype("int64")
    return missing_count == 0, missing_count


def build_panel_from_frames(
    financials: pd.DataFrame,
    failures: pd.DataFrame,
    institutions: pd.DataFrame,
    label_cutoff: str | pd.Timestamp = LABEL_CUTOFF,
    reporting_lag_days: int = 45,
    merger_policy: str = "censor",
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Build panel and return (panel, exit_table, period_summary)."""
    cutoff = pd.Timestamp(label_cutoff)
    _require_columns(financials, ["CERT", "REPDTE"], "Financials")
    panel = financials.copy()
    panel["CERT"] = _cert(panel["CERT"])
    panel["REPDTE"] = parse_fdic_date(panel["REPDTE"])
    panel = panel.dropna(subset=["CERT", "REPDTE"]).copy()
    non_quarter_end = panel["REPDTE"] != (panel["REPDTE"] + pd.offsets.QuarterEnd(0))
    if non_quarter_end.any():
        raise PanelBuildError(f"Financials contains {int(non_quarter_end.sum())} non-quarter-end observations; resolve date quality before panel construction.")
    duplicate_mask = panel.duplicated(["CERT", "REPDTE"], keep=False)
    if duplicate_mask.any():
        count = int(duplicate_mask.sum())
        raise PanelBuildError(f"Financials contains {count} rows in duplicate CERT/REPDTE keys; resolve using Part 2 evidence before panel construction.")

    exits = classify_exits(failures, institutions)
    panel = panel.merge(exits, on="CERT", how="left", validate="many_to_one")
    if "STNAME" in panel:
        panel["state_at_report"] = panel["STNAME"]
    panel["financial_available_date"] = panel["REPDTE"] + pd.to_timedelta(reporting_lag_days, unit="D")
    panel["horizon_end"] = panel["REPDTE"] + pd.offsets.QuarterEnd(4)
    panel["future_reports_complete"], panel["future_missing_quarter_count"] = _future_report_completeness(panel, cutoff)

    q = panel["REPDTE"]
    failure_date = panel["failure_date"]
    end_date = panel["institution_end_date"]
    in_scope = q <= cutoff
    post_failure = failure_date.notna() & (failure_date <= q)
    post_other_exit = end_date.notna() & (end_date <= q) & ~failure_date.notna()
    failure_in_window = failure_date.notna() & (failure_date > q) & (failure_date <= panel["horizon_end"])
    other_exit_in_window = end_date.notna() & (end_date > q) & (end_date <= panel["horizon_end"])
    other_precedes_failure = other_exit_in_window & (failure_date.isna() | (end_date < failure_date))
    positive = in_scope & ~post_failure & ~post_other_exit & failure_in_window & ~other_precedes_failure
    censored_other = in_scope & ~post_failure & ~post_other_exit & other_precedes_failure
    if merger_policy not in {"censor", "negative"}:
        raise PanelBuildError("merger_policy must be 'censor' or 'negative'")
    merger_negative = merger_policy == "negative" and other_exit_in_window & ~failure_date.notna()
    negative = in_scope & ~post_failure & ~post_other_exit & ~failure_in_window & ~other_exit_in_window & panel["future_reports_complete"]
    negative = negative | (in_scope & ~post_failure & ~post_other_exit & merger_negative)
    incomplete = in_scope & ~post_failure & ~post_other_exit & ~positive & ~censored_other & ~negative

    panel["label"] = pd.Series(pd.NA, index=panel.index, dtype="Int64")
    panel.loc[positive, "label"] = 1
    panel.loc[negative, "label"] = 0
    panel["label_status"] = "unlabeled_after_2025Q2"
    panel.loc[post_failure, "label_status"] = "post_failure_report"
    panel.loc[post_other_exit, "label_status"] = "post_nonfailure_exit_report"
    panel.loc[censored_other, "label_status"] = "censored_merger_other_exit"
    panel.loc[positive, "label_status"] = "positive_failure_in_forward_window"
    panel.loc[negative, "label_status"] = "negative_observed_through_horizon"
    panel.loc[incomplete, "label_status"] = "censored_incomplete_future_reports"
    panel["eligible_for_model"] = panel["label"].notna()
    panel["point_in_time_ok"] = ~post_failure & ~post_other_exit
    panel["point_in_time_note"] = "Financials at REPDTE become available under the configured reporting-lag assumption; institution ACTIVE/CHANGEC1 are snapshot evidence, not modelling features."

    period = panel["REPDTE"].dt.to_period("Q").astype(str)
    panel["quarter"] = period
    period_summary = panel.groupby(["quarter", "label_status"], dropna=False).agg(
        observations=("CERT", "size"), eligible_for_model=("eligible_for_model", "sum"), failures=("label", lambda x: int((x == 1).sum())), negatives=("label", lambda x: int((x == 0).sum())),
    ).reset_index()
    return panel, exits, period_summary


def _evidence_table(quality_dir: Path) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    summary_path = quality_dir / "data_quality_summary.csv"
    try:
        summary = pd.read_csv(summary_path) if summary_path.exists() else pd.DataFrame()
    except pd.errors.EmptyDataError:
        summary = pd.DataFrame()
    anomaly_path = quality_dir / "failure_anomalies.csv"
    try:
        anomalies = pd.read_csv(anomaly_path) if anomaly_path.exists() and anomaly_path.stat().st_size else pd.DataFrame()
    except pd.errors.EmptyDataError:
        anomalies = pd.DataFrame()
    post_path = quality_dir / "post_failure_reports.csv"
    try:
        post = pd.read_csv(post_path) if post_path.exists() and post_path.stat().st_size else pd.DataFrame()
    except pd.errors.EmptyDataError:
        post = pd.DataFrame()

    def count(dataset: str, column: str) -> Any:
        if summary.empty or dataset not in set(summary.get("dataset", [])):
            return None
        if column not in summary.columns:
            return None
        value = summary.loc[summary["dataset"] == dataset, column]
        return None if value.empty else value.iloc[0]

    def invalid_count(dataset: str, rule: str) -> Any:
        if summary.empty or dataset not in set(summary.get("dataset", [])) or "invalid_rule_counts" not in summary.columns:
            return None
        value = summary.loc[summary["dataset"] == dataset, "invalid_rule_counts"]
        if value.empty:
            return None
        try:
            return ast.literal_eval(str(value.iloc[0])).get(rule, 0)
        except (SyntaxError, ValueError):
            return None

    def anomaly(problem: str, column: str = "count") -> Any:
        if anomalies.empty or problem not in set(anomalies.get("problem", [])):
            return None
        value = anomalies.loc[anomalies["problem"] == problem, column]
        return None if value.empty else value.iloc[0]

    rows.extend([
        {"problem": "failures_missing_cert", "evidence_source": "failure_anomalies.csv", "evidence_status": "generated_file_present" if not anomalies.empty else "audit_output_missing", "evidence_count": anomaly("failures_missing_cert"), "handling": "Retain source rows; exclude from linked panel because CERT is required.", "rationale": "The audit reports the date range and whether any missing-CERT event is in the modelling era."},
        {"problem": "repeated_failure_events", "evidence_source": "failure_anomalies.csv", "evidence_status": "generated_file_present" if not anomalies.empty else "audit_output_missing", "evidence_count": int((anomalies.get("problem", pd.Series(dtype=str)) == "repeated_failure_events").sum()) if not anomalies.empty else None, "handling": "Use the earliest explicit RESTYPE=FAILURE; do not take min(all event dates).", "rationale": "Assistance and failure events can share a CERT."},
        {"problem": "assistance_events", "evidence_source": "failure_anomalies.csv", "evidence_status": "generated_file_present" if not anomalies.empty else "audit_output_missing", "evidence_count": anomaly("assistance_events"), "handling": "Treat RESTYPE=ASSISTANCE as non-failure evidence and keep it out of the positive label.", "rationale": "The source distinguishes assistance from failure and some assisted banks continue reporting."},
        {"problem": "post_failure_report", "evidence_source": "post_failure_reports.csv", "evidence_status": "generated_file_present" if post_path.exists() else "audit_output_missing", "evidence_count": int(post.get("post_report_count", pd.Series(dtype=float)).sum()) if not post.empty else 0, "handling": "Censor reports after an explicit failure event and inspect the event source.", "rationale": "A post-failure report cannot be used as a pre-failure observation."},
        {"problem": "negative_ntlnls_rule_rejected", "evidence_source": "data_quality_summary.csv and field audit", "evidence_status": "generated_file_present" if summary_path.exists() else "audit_output_missing", "evidence_count": invalid_count("financials", "observed_negative:NTLNLS"), "handling": "NTLNLS is not treated as universally non-negative; retain observed negatives and document the rejected rule.", "rationale": "The smoke audit found frequent negative values, so flagging every negative as invalid would be a false-positive quality rule."},
        {"problem": "duplicate_bank_quarter", "evidence_source": "data_quality_summary.csv", "evidence_status": "generated_file_present" if summary_path.exists() else "audit_output_missing", "evidence_count": count("financials", "duplicate_nonnull_key_groups"), "handling": "Fail closed if a Financials CERT/REPDTE key is duplicated.", "rationale": "A bank-quarter panel key must be unique."},
        {"problem": "reporting_gaps", "evidence_source": "financials/reporting_gaps.csv", "evidence_status": "generated_file_present" if (quality_dir / "financials" / "reporting_gaps.csv").exists() else "audit_output_missing", "evidence_count": count("financials", "reporting_gap_banks"), "handling": "Do not fill gaps; censor a negative label when future exposure is incomplete.", "rationale": "Bridging a gap would create an unobserved outcome."},
        {"problem": "post_cutoff_outcomes", "evidence_source": "assignment rule", "evidence_status": "assignment rule", "evidence_count": None, "handling": "Set labels missing after 2025Q2.", "rationale": "The four-quarter outcome window is incomplete after the cutoff."},
    ])
    return pd.DataFrame(rows)


def build_panel_from_paths(processed_root: Path, output_dir: Path, quality_dir: Path | None = None, label_cutoff: str = "2025-06-30", reporting_lag_days: int = 45, merger_policy: str = "censor") -> dict[str, Any]:
    financials = read_processed_dataset(processed_root, "financials")
    failures = read_processed_dataset(processed_root, "failures")
    institutions = read_processed_dataset(processed_root, "institutions")
    panel, exits, period_summary = build_panel_from_frames(financials, failures, institutions, label_cutoff, reporting_lag_days, merger_policy)
    output_dir.mkdir(parents=True, exist_ok=True)
    panel = _cast_datetime_columns_to_ms(panel)
    exits = _cast_datetime_columns_to_ms(exits)
    panel.to_parquet(output_dir / "bank_quarter_panel.parquet", index=False)
    panel.loc[panel["eligible_for_model"]].to_parquet(output_dir / "modeling_panel.parquet", index=False)
    exits.to_csv(output_dir / "exit_classification.csv", index=False)
    period_summary.to_csv(output_dir / "period_summary.csv", index=False)
    annual = panel.assign(
        reporting_year=panel["REPDTE"].dt.year,
        failed_row=panel["label"].eq(1).fillna(False),
        failed_cert=panel["CERT"].where(panel["label"].eq(1)),
    )
    failures_per_year = (
        annual.groupby("reporting_year", as_index=False)
        .agg(
            observations=("CERT", "size"),
            eligible_rows=("eligible_for_model", "sum"),
            failed_rows=("failed_row", "sum"),
            distinct_failed_banks=("failed_cert", "nunique"),
        )
    )
    failures_per_year.to_csv(output_dir / "failures_per_year.csv", index=False)
    evidence = _evidence_table(quality_dir or Path("outputs/quality_reports"))
    evidence.to_csv(output_dir / "problem_evidence_handling.csv", index=False)
    sensitivity: dict[str, Any] | None = None
    if merger_policy == "censor":
        sensitivity_panel, _, _ = build_panel_from_frames(financials, failures, institutions, label_cutoff, reporting_lag_days, "negative")
        sensitivity = {
            "standard_policy": "censor",
            "sensitivity_policy": "negative",
            "standard_eligible_rows": int(panel["eligible_for_model"].sum()),
            "sensitivity_eligible_rows": int(sensitivity_panel["eligible_for_model"].sum()),
            "standard_positive_labels": int((panel["label"] == 1).sum()),
            "sensitivity_positive_labels": int((sensitivity_panel["label"] == 1).sum()),
            "standard_negative_labels": int((panel["label"] == 0).sum()),
            "sensitivity_negative_labels": int((sensitivity_panel["label"] == 0).sum()),
            "difference_in_eligible_rows": int(sensitivity_panel["eligible_for_model"].sum() - panel["eligible_for_model"].sum()),
            "note": "Sensitivity only; the standard model uses merger/other censoring because the assignment defines mergers as not failures and their future exposure is not a comparable failure-negative outcome.",
        }
        (output_dir / "merger_policy_sensitivity.json").write_text(json.dumps(sensitivity, indent=2), encoding="utf-8")
    summary = {
        "rows": int(len(panel)), "eligible_for_model": int(panel["eligible_for_model"].sum()),
        "positive_labels": int((panel["label"] == 1).sum()), "negative_labels": int((panel["label"] == 0).sum()),
        "censored_or_unlabeled": int(panel["label"].isna().sum()), "label_cutoff": pd.Timestamp(label_cutoff).date().isoformat(),
        "reporting_lag_days": reporting_lag_days, "reporting_lag_used_for": "financial_available_date and point-in-time documentation", "quarter_frequency": QUARTER_FREQUENCY,
        "label_rule": "1 iff failure_date in (REPDTE, REPDTE+4Q] and no earlier non-failure exit; 0 only with complete observed future reports and no exit; otherwise missing.",
        "excluded_from_features": ["failure_date", "failure_cost_numeric", "institution_end_date", "ACTIVE", "CHANGEC1", "label", "label_status", "eligible_for_model"],
        "split_applied": False, "merger_policy": merger_policy, "merger_policy_sensitivity": sensitivity, "failures_per_year_file": str(output_dir / "failures_per_year.csv"),
    }
    (output_dir / "panel_summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
    return summary
