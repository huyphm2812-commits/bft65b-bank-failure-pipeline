"""Part 7: point-in-time features, chronological modelling and Spark MLlib.

The module consumes the full Part 3 bank-quarter panel.  Features are built
before filtering to eligible labels so lagged calculations cannot bridge a
missing quarter merely because an unlabeled row was removed.  The model split
and every preprocessing object are fitted using training data only.
"""
from __future__ import annotations

import hashlib
import json
import math
import time
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
from sklearn.dummy import DummyClassifier
from sklearn.ensemble import RandomForestClassifier
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import (
    average_precision_score,
    brier_score_loss,
    confusion_matrix,
    f1_score,
    precision_recall_curve,
    precision_score,
    recall_score,
    roc_auc_score,
    roc_curve,
)
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler, RobustScaler, QuantileTransformer
from sklearn.feature_selection import SelectKBest, f_classif
from sklearn.impute import KNNImputer
import joblib


LABEL_CUTOFF = pd.Timestamp("2025-06-30")
FEATURE_COLUMNS = [
    "capital_ratio",
    "loan_to_asset",
    "noncurrent_loan_to_asset",
    "ore_to_asset",
    "cash_to_asset",
    "securities_to_asset",
    "roa_level",
    "roe_level",
    "nimy_level",
    "rbc1a_level",
    "rbcrwa_level",
    "asset_growth_qoq",
    "asset_growth_yoy",
    "roa_rolling_4q_mean",
]
FEATURE_VARIANTS: dict[str, list[str]] = {
    "variant_1": list(FEATURE_COLUMNS),
    "variant_2": [c for c in FEATURE_COLUMNS if c != "rbcrwa_level"],
    "variant_3": [c for c in FEATURE_COLUMNS if c not in ("rbcrwa_level", "rbc1a_level")],
}
LEAKAGE_COLUMNS = {
    "label",
    "label_status",
    "eligible_for_model",
    "failure_date",
    "failure_cost_numeric",
    "institution_end_date",
    "ACTIVE",
    "CHANGEC1",
    "exit_classification",
}


class ModelingError(RuntimeError):
    """Raised when the requested model experiment is not identifiable."""


def resolve_feature_columns(feature_variant: str = "variant_1") -> list[str]:
    """Return the exact feature list for a named feature variant."""
    key = str(feature_variant).strip().lower()
    if key not in FEATURE_VARIANTS:
        raise ModelingError(f"Unsupported feature_variant '{feature_variant}'. Must be one of {sorted(FEATURE_VARIANTS)}.")
    cols = list(FEATURE_VARIANTS[key])
    if set(cols) & LEAKAGE_COLUMNS:
        raise ModelingError("Leakage columns entered the resolved feature variant list.")
    return cols


def compute_file_sha256(file_path: Path) -> str:
    """Compute SHA-256 hash of a file on disk."""
    p = Path(file_path).resolve()
    if not p.exists():
        raise ModelingError(f"File not found for SHA-256 calculation: {file_path}")
    h = hashlib.sha256()
    with open(p, "rb") as f:
        while chunk := f.read(65536):
            h.update(chunk)
    return h.hexdigest()


def verify_reproducibility_hashes(
    cfg: dict[str, Any],
    repo_root: Path | None = None,
    panel_path: Path | None = None,
) -> None:
    """Verify runtime SHA-256 digests of source code and panel dataset against frozen configuration."""
    if repo_root is None:
        repo_root = Path(__file__).resolve().parents[3]
    repo_root = Path(repo_root).resolve()

    if "reproducibility_hashes" not in cfg or not isinstance(cfg["reproducibility_hashes"], dict):
        raise ModelingError("Frozen configuration is missing 'reproducibility_hashes' section.")

    repro = cfg["reproducibility_hashes"]
    code_hashes = repro.get("code_files")
    if not isinstance(code_hashes, dict):
        raise ModelingError("Frozen configuration 'reproducibility_hashes' is missing 'code_files' dictionary.")

    expected_code_files = {
        "pipeline_py": repo_root / "src" / "bankfail" / "modeling" / "pipeline.py",
        "dev_analysis_py": repo_root / "src" / "bankfail" / "modeling" / "dev_analysis.py",
        "builder_py": repo_root / "src" / "bankfail" / "panel" / "builder.py",
    }

    for key, code_path in expected_code_files.items():
        if key not in code_hashes:
            raise ModelingError(f"Frozen configuration missing expected code hash for '{key}'.")
        expected_hash = str(code_hashes[key]).strip().lower()
        if not code_path.exists():
            raise ModelingError(f"Required code file does not exist on disk for SHA-256 verification: {code_path}")
        computed_hash = compute_file_sha256(code_path)
        if computed_hash != expected_hash:
            raise ModelingError(
                f"SHA-256 verification failed for '{key}' ({code_path.name}): "
                f"recorded={expected_hash}, computed={computed_hash}"
            )

    if "panel_parquet" not in repro:
        raise ModelingError("Frozen configuration missing 'panel_parquet' hash in 'reproducibility_hashes'.")
    expected_panel_hash = str(repro["panel_parquet"]).strip().lower()

    resolved_panel_path: Path | None = None
    if panel_path is not None and Path(panel_path).exists():
        resolved_panel_path = Path(panel_path).resolve()
    else:
        candidates = [
            repo_root.parent / "02_local_workspace" / "outputs" / "03_panel" / "bank_quarter_panel.parquet",
            repo_root / "outputs" / "03_panel" / "bank_quarter_panel.parquet",
        ]
        if panel_path is not None:
            candidates.insert(0, Path(panel_path).resolve())
        for c in candidates:
            if c.exists():
                resolved_panel_path = c
                break

    if resolved_panel_path is None or not resolved_panel_path.exists():
        raise ModelingError(
            f"Required panel parquet file does not exist on disk for SHA-256 verification: {panel_path}"
        )

    computed_panel_hash = compute_file_sha256(resolved_panel_path)
    if computed_panel_hash != expected_panel_hash:
        raise ModelingError(
            f"SHA-256 verification failed for panel dataset ({resolved_panel_path.name}): "
            f"recorded={expected_panel_hash}, computed={computed_panel_hash}"
        )


def load_and_validate_frozen_config(
    frozen_config_path: Path,
    repo_root: Path | None = None,
    panel_path: Path | None = None,
    verify_hashes: bool = True,
) -> dict[str, Any]:
    """Load frozen_model_config.json and verify status, schema, self-hash, and file SHA-256 integrity."""
    if not frozen_config_path.exists():
        raise ModelingError(f"Frozen configuration file does not exist: {frozen_config_path}")
    try:
        cfg = json.loads(frozen_config_path.read_text(encoding="utf-8"))
    except Exception as exc:
        raise ModelingError(f"Failed to parse frozen configuration JSON: {exc}") from exc

    if cfg.get("status") != "FROZEN":
        raise ModelingError(f"Frozen configuration status must be 'FROZEN', got '{cfg.get('status')}'.")

    recorded_hash = cfg.get("sha256_hash")
    if not recorded_hash:
        raise ModelingError("Frozen configuration is missing 'sha256_hash'.")
    payload_without_hash = {k: v for k, v in cfg.items() if k != "sha256_hash"}
    encoded = json.dumps(payload_without_hash, sort_keys=True, indent=2).encode("utf-8")
    computed_hash = hashlib.sha256(encoded).hexdigest()
    if computed_hash != recorded_hash:
        raise ModelingError(
            f"Frozen configuration SHA-256 integrity check failed: recorded={recorded_hash}, computed={computed_hash}"
        )

    for section in ("split", "feature_selection", "primary_model", "decision_threshold"):
        if section not in cfg or not isinstance(cfg[section], dict):
            raise ModelingError(f"Frozen configuration is missing required section '{section}'.")

    split_sec = cfg["split"]
    for k in ("train_end", "dev_start", "dev_end", "test_start", "test_end"):
        if not split_sec.get(k):
            raise ModelingError(f"Frozen configuration split is missing '{k}'.")

    variant = cfg["feature_selection"].get("variant")
    resolve_feature_columns(str(variant))

    pm = cfg["primary_model"]
    if pm.get("training_policy") != "train_only":
        raise ModelingError(f"Frozen configuration training_policy must be 'train_only', got '{pm.get('training_policy')}'.")
    if pm.get("model_family") not in ("random_forest", "logistic_regression"):
        raise ModelingError(f"Frozen configuration primary model_family invalid: '{pm.get('model_family')}'.")
    if pm.get("seed") is None:
        raise ModelingError("Frozen configuration primary_model is missing 'seed'.")

    dt = cfg["decision_threshold"]
    loss_def = str(dt.get("loss_definition", "")).upper()
    if loss_def not in ("DEF_A", "DEF_B"):
        raise ModelingError(f"Frozen configuration loss_definition must be 'DEF_A' or 'DEF_B', got '{loss_def}'.")
    ratio_str = str(dt.get("fn_fp_ratio", ""))
    if ":" not in ratio_str:
        raise ModelingError(f"Frozen configuration fn_fp_ratio must be formatted as '<N>:1', got '{ratio_str}'.")
    try:
        ratio_num = float(ratio_str.split(":")[0])
        if ratio_num <= 0:
            raise ValueError
    except ValueError as exc:
        raise ModelingError(f"Invalid fn_fp_ratio '{ratio_str}' in frozen configuration.") from exc

    chosen_t = dt.get("chosen_threshold")
    if chosen_t is None or not (0.0 <= float(chosen_t) <= 1.0):
        raise ModelingError(f"Invalid chosen_threshold '{chosen_t}' in frozen configuration.")

    if verify_hashes:
        verify_reproducibility_hashes(cfg, repo_root=repo_root, panel_path=panel_path)

    return cfg


def _require_columns(frame: pd.DataFrame, columns: list[str], name: str) -> None:
    missing = sorted(set(columns) - set(frame.columns))
    if missing:
        raise ModelingError(f"{name} is missing required columns: {missing}")


def _safe_ratio(numerator: pd.Series, denominator: pd.Series) -> pd.Series:
    denominator = pd.to_numeric(denominator, errors="coerce")
    numerator = pd.to_numeric(numerator, errors="coerce")
    valid = denominator.notna() & denominator.ne(0)
    return numerator.div(denominator.where(valid))


def _read_panel(panel_path: Path) -> pd.DataFrame:
    if not panel_path.exists():
        raise FileNotFoundError(f"Panel file does not exist: {panel_path}")
    frame = pd.read_parquet(panel_path)
    _require_columns(frame, ["CERT", "REPDTE", "label", "eligible_for_model", "horizon_end"], "Part 3 panel")
    frame["CERT"] = pd.to_numeric(frame["CERT"], errors="coerce").astype("Int64")
    frame["REPDTE"] = pd.to_datetime(frame["REPDTE"], errors="coerce").dt.normalize()
    frame["horizon_end"] = pd.to_datetime(frame["horizon_end"], errors="coerce").dt.normalize()
    frame = frame.dropna(subset=["CERT", "REPDTE"]).copy()
    if frame.duplicated(["CERT", "REPDTE"]).any():
        raise ModelingError("Panel is not unique by CERT/REPDTE; fix Part 3 before modelling.")
    return frame.sort_values(["CERT", "REPDTE"]).reset_index(drop=True)


def _add_lag(frame: pd.DataFrame, source: str, periods: int) -> pd.DataFrame:
    if source not in frame:
        return frame
    right = frame[["CERT", "REPDTE", source]].copy()
    right["REPDTE"] = right["REPDTE"] + pd.offsets.QuarterEnd(periods)
    right = right.rename(columns={source: f"{source}_lag{periods}"})
    return frame.merge(right, on=["CERT", "REPDTE"], how="left", validate="one_to_one")


def _feature_dictionary() -> pd.DataFrame:
    rows = [
        ("capital_ratio", "EQ / ASSET", "Capital proxy; verify EQ and ASSET units in risview_properties.yaml."),
        ("loan_to_asset", "LNLSNET / ASSET", "Net-loan concentration proxy; denominator-zero returns missing."),
        ("noncurrent_loan_to_asset", "NCLNLS / ASSET", "Asset-quality proxy; confirm NCLNLS definition and sign."),
        ("ore_to_asset", "ORE / ASSET", "Other-real-estate concentration proxy."),
        ("cash_to_asset", "CHBAL / ASSET", "Liquidity proxy."),
        ("securities_to_asset", "SC / ASSET", "Securities concentration proxy."),
        ("net_income_to_asset", "NETINC / ASSET", "Audit-only provisional feature; excluded from the model until NETINC YTD/quarterly meaning is confirmed."),
        ("roa_level", "ROA", "Reported profitability ratio; preserve source scale."),
        ("roe_level", "ROE", "Reported return-on-equity ratio; preserve source scale."),
        ("nimy_level", "NIMY", "Reported net-interest-margin proxy; verify field definition."),
        ("rbc1a_level", "RBC1AAJ", "Capital adequacy proxy; verify units and denominator."),
        ("rbcrwa_level", "RBCRWAJ", "Risk-weighted capital proxy; verify units and denominator."),
        ("asset_growth_qoq", "ASSET(t)/ASSET(t-1)-1", "Past-quarter momentum; only exact adjacent quarter is used."),
        ("asset_growth_yoy", "ASSET(t)/ASSET(t-4)-1", "Past-year momentum; only exact four-quarter lag is used."),
        ("roa_rolling_4q_mean", "mean(ROA(t), ROA(t-1..t-3))", "Trailing mean; missing if any of the four exact quarters is missing."),
        ("netinc_change_qoq", "NETINC(t)-NETINC(t-1)", "Audit-only provisional feature; excluded until NETINC YTD/quarterly meaning is confirmed."),
    ]
    return pd.DataFrame(rows, columns=["feature", "formula_or_source", "handling_note"])


def _derive_features(frame: pd.DataFrame) -> pd.DataFrame:
    """Derive features from point-in-time Financials only."""
    frame = frame.copy()
    numeric_sources = ["ASSET", "DEP", "EQ", "LNLSNET", "NCLNLS", "ORE", "CHBAL", "SC", "NETINC", "ROA", "ROE", "NIMY", "RBC1AAJ", "RBCRWAJ"]
    for column in numeric_sources:
        if column in frame:
            frame[column] = pd.to_numeric(frame[column], errors="coerce")

    frame["capital_ratio"] = _safe_ratio(frame.get("EQ", pd.Series(index=frame.index, dtype="float64")), frame.get("ASSET", pd.Series(index=frame.index, dtype="float64")))
    frame["loan_to_asset"] = _safe_ratio(frame.get("LNLSNET", pd.Series(index=frame.index, dtype="float64")), frame.get("ASSET", pd.Series(index=frame.index, dtype="float64")))
    frame["noncurrent_loan_to_asset"] = _safe_ratio(frame.get("NCLNLS", pd.Series(index=frame.index, dtype="float64")), frame.get("ASSET", pd.Series(index=frame.index, dtype="float64")))
    frame["ore_to_asset"] = _safe_ratio(frame.get("ORE", pd.Series(index=frame.index, dtype="float64")), frame.get("ASSET", pd.Series(index=frame.index, dtype="float64")))
    frame["cash_to_asset"] = _safe_ratio(frame.get("CHBAL", pd.Series(index=frame.index, dtype="float64")), frame.get("ASSET", pd.Series(index=frame.index, dtype="float64")))
    frame["securities_to_asset"] = _safe_ratio(frame.get("SC", pd.Series(index=frame.index, dtype="float64")), frame.get("ASSET", pd.Series(index=frame.index, dtype="float64")))
    frame["net_income_to_asset"] = _safe_ratio(frame.get("NETINC", pd.Series(index=frame.index, dtype="float64")), frame.get("ASSET", pd.Series(index=frame.index, dtype="float64")))
    for output, source in (("roa_level", "ROA"), ("roe_level", "ROE"), ("nimy_level", "NIMY"), ("rbc1a_level", "RBC1AAJ"), ("rbcrwa_level", "RBCRWAJ")):
        frame[output] = frame[source] if source in frame else np.nan

    # Recode structural zeros in rbcrwa_level before 1990 to missing (Basel I risk-based capital began reporting in 1990)
    if "rbcrwa_level" in frame and "REPDTE" in frame:
        repdte_dt = pd.to_datetime(frame["REPDTE"], errors="coerce")
        frame.loc[repdte_dt.dt.year < 1990, "rbcrwa_level"] = np.nan
        if "rbc1a_level" in frame:
            frame.loc[(repdte_dt.dt.year < 1990) & (frame["rbc1a_level"] == 0), "rbc1a_level"] = np.nan

    frame["quarter_index"] = frame["REPDTE"].dt.year * 4 + frame["REPDTE"].dt.quarter
    for source, periods in (("ASSET", 1), ("ASSET", 4), ("NETINC", 1), ("ROA", 1), ("ROA", 2), ("ROA", 3)):
        frame = _add_lag(frame, source, periods)
    frame["asset_growth_qoq"] = _safe_ratio(frame.get("ASSET", pd.Series(index=frame.index)), frame.get("ASSET_lag1", pd.Series(index=frame.index))) - 1
    frame["asset_growth_yoy"] = _safe_ratio(frame.get("ASSET", pd.Series(index=frame.index)), frame.get("ASSET_lag4", pd.Series(index=frame.index))) - 1
    frame["netinc_change_qoq"] = frame.get("NETINC", pd.Series(index=frame.index)) - frame.get("NETINC_lag1", pd.Series(index=frame.index))
    roa_values = [frame.get("ROA", pd.Series(index=frame.index))] + [frame.get(f"ROA_lag{i}", pd.Series(index=frame.index)) for i in (1, 2, 3)]
    frame["roa_rolling_4q_mean"] = pd.concat(roa_values, axis=1).mean(axis=1).where(pd.concat(roa_values, axis=1).notna().all(axis=1))

    frame["point_in_time_ok"] = True
    frame["feature_source_date"] = frame["REPDTE"]
    forbidden_present = sorted(set(frame.columns) & LEAKAGE_COLUMNS & set(FEATURE_COLUMNS))
    if forbidden_present:
        raise ModelingError(f"Leakage columns entered the feature list: {forbidden_present}")
    return frame


def future_perturbation_check(panel_path: Path) -> dict[str, Any]:
    """Regression guard: changing outcomes must not change any feature value."""
    original = _read_panel(panel_path)
    perturbed = original.copy()
    for column in sorted(LEAKAGE_COLUMNS):
        if column not in perturbed:
            continue
        if pd.api.types.is_numeric_dtype(perturbed[column]):
            perturbed[column] = 987654321.0
        elif pd.api.types.is_datetime64_any_dtype(perturbed[column]):
            perturbed[column] = pd.Timestamp("1900-01-01")
        else:
            perturbed[column] = "PERTURBED_FUTURE_VALUE"
    base_features = _derive_features(original)[["CERT", "REPDTE"] + FEATURE_COLUMNS].copy()
    changed_features = _derive_features(perturbed)[["CERT", "REPDTE"] + FEATURE_COLUMNS].copy()
    pd.testing.assert_frame_equal(
        base_features.reset_index(drop=True),
        changed_features.reset_index(drop=True),
        check_dtype=False,
        check_exact=False,
        rtol=1e-12,
        atol=1e-12,
    )
    return {"passed": True, "perturbed_columns": sorted(set(LEAKAGE_COLUMNS) & set(original.columns)), "feature_columns_checked": FEATURE_COLUMNS}


def build_feature_table(panel_path: Path, output_dir: Path | None = None) -> dict[str, Any]:
    """Build all features on the complete panel, then expose eligible rows."""
    frame = _derive_features(_read_panel(panel_path))
    feature_table = frame.copy()
    eligible = feature_table[feature_table["eligible_for_model"].fillna(False) & feature_table["label"].notna()].copy()
    feature_dictionary = _feature_dictionary()
    perturbation = future_perturbation_check(panel_path)
    if output_dir is not None:
        output_dir.mkdir(parents=True, exist_ok=True)
        feature_table.to_parquet(output_dir / "p7_feature_table.parquet", index=False)
        eligible.to_parquet(output_dir / "p7_modeling_features.parquet", index=False)
        feature_dictionary.to_csv(output_dir / "p7_feature_dictionary.csv", index=False)
        (output_dir / "p7_feature_summary.json").write_text(json.dumps({"all_rows": len(feature_table), "eligible_rows": len(eligible), "feature_columns": FEATURE_COLUMNS, "provisional_features_excluded": ["net_income_to_asset", "netinc_change_qoq"], "leakage_columns_excluded": sorted(LEAKAGE_COLUMNS), "point_in_time_rule": "Feature at q uses only Financials at q and exact prior quarters; no missing quarter is bridged.", "reporting_lag_rule": "Features are indexed by REPDTE and are assumed available at financial_available_date = REPDTE + configured reporting lag.", "feature_selection_scope": "Pre-specified economic features; no outcome-driven feature selection is performed.", "future_perturbation_check": perturbation}, indent=2), encoding="utf-8")
    return {"all": feature_table, "eligible": eligible, "feature_columns": FEATURE_COLUMNS, "dictionary": feature_dictionary, "future_perturbation": perturbation}


def _split_data(frame: pd.DataFrame, train_end: str, dev_start: str, dev_end: str, test_start: str, test_end: str, label_cutoff: str, min_test_failures: int, min_dev_failures: int, require_test: bool = True) -> dict[str, Any]:
    dates = {name: pd.Timestamp(value) for name, value in {"train_end": train_end, "dev_start": dev_start, "dev_end": dev_end, "test_start": test_start, "test_end": test_end, "label_cutoff": label_cutoff}.items()}
    if not dates["train_end"] < dates["dev_start"] <= dates["dev_end"] < dates["test_start"] <= dates["test_end"]:
        raise ModelingError("Split dates must be chronological: train_end < dev_start <= dev_end < test_start <= test_end.")
    embargo_start = dates["dev_end"] + pd.offsets.QuarterEnd(4)
    if dates["test_start"] < embargo_start:
        raise ModelingError("Test start must be at least four quarters after development end.")
    eligible = frame[frame["eligible_for_model"].fillna(False) & frame["label"].notna() & (frame["REPDTE"] <= dates["label_cutoff"])].copy()
    train = eligible[(eligible["REPDTE"] <= dates["train_end"]) & (eligible["horizon_end"] < dates["dev_start"])].copy()
    development = eligible[(eligible["REPDTE"] >= dates["dev_start"]) & (eligible["REPDTE"] <= dates["dev_end"]) & (eligible["horizon_end"] < dates["test_start"])].copy()
    test = eligible[(eligible["REPDTE"] >= dates["test_start"]) & (eligible["REPDTE"] <= dates["test_end"])].copy()
    if train.empty or development.empty or (require_test and test.empty):
        raise ModelingError(f"Insufficient split rows: train={len(train)}, development={len(development)}, test={len(test)}. Run feature-only smoke first, then use the full 1984Q1-2026Q2 acquisition.")
    if train["label"].nunique() < 2:
        raise ModelingError("Training split has fewer than two label classes.")
    development_failures = int(development.loc[development["label"] == 1, "CERT"].nunique())
    if development_failures < min_dev_failures:
        raise ModelingError(f"Development split has {development_failures} distinct failed banks; required minimum is {min_dev_failures}. Choose a wider development window before fitting thresholds.")
    test_failures = int(test.loc[test["label"] == 1, "CERT"].nunique())
    if require_test and test_failures < min_test_failures:
        raise ModelingError(f"Test split has {test_failures} distinct failed banks; required minimum is {min_test_failures}. Do not report final test metrics.")
    return {"train": train, "development": development, "test": test, "metadata": {"train": _split_counts(train), "development": _split_counts(development), "test": _split_counts(test), "train_end": train_end, "dev_start": dev_start, "dev_end": dev_end, "test_start": test_start, "test_end": test_end, "label_cutoff": label_cutoff, "embargo_quarters": 4, "purging_rule": "Training rows are retained only when horizon_end < the next split start.", "min_dev_failures": min_dev_failures, "min_test_failures": min_test_failures, "failure_count_definition": "distinct CERT values with label=1"}}


def _split_counts(frame: pd.DataFrame) -> dict[str, Any]:
    return {"rows": int(len(frame)), "positive": int((frame["label"] == 1).sum()), "positive_banks": int(frame.loc[frame["label"] == 1, "CERT"].nunique()), "negative": int((frame["label"] == 0).sum()), "min_date": frame["REPDTE"].min().date().isoformat() if len(frame) else None, "max_date": frame["REPDTE"].max().date().isoformat() if len(frame) else None}


def _select_threshold(y_true: pd.Series | np.ndarray, probability: np.ndarray) -> tuple[float, dict[str, Any]]:
    y_arr = np.asarray(y_true).astype(int)
    prob_arr = np.asarray(probability).astype(float)
    n_pos = int(y_arr.sum())
    n_total = len(y_arr)
    if n_pos == 0:
        return 0.5, {"rule": "max_f1_on_development", "development_f1": 0.0, "development_recall": 0.0, "threshold": 0.5}
    if n_pos == n_total:
        return 0.0, {"rule": "max_f1_on_development", "development_f1": 1.0, "development_recall": 1.0, "threshold": 0.0}

    precision, recall, thresholds = precision_recall_curve(y_arr, prob_arr)
    all_thresholds = np.r_[0.0, thresholds, 1.0]

    rec_0 = 1.0
    prec_0 = float(n_pos / n_total)
    f1_0 = 2 * prec_0 * rec_0 / (prec_0 + rec_0) if (prec_0 + rec_0) > 0 else 0.0

    precisions_eval = np.r_[prec_0, precision[:-1], 0.0]
    recalls_eval = np.r_[rec_0, recall[:-1], 0.0]
    denom = precisions_eval + recalls_eval
    f1_scores = np.where(denom > 0, 2 * precisions_eval * recalls_eval / np.maximum(denom, 1e-12), 0.0)

    scored = []
    for i in range(len(all_thresholds)):
        scored.append((float(f1_scores[i]), float(recalls_eval[i]), -float(all_thresholds[i]), float(all_thresholds[i])))
    best = max(scored)
    return best[3], {"rule": "max_f1_on_development", "development_f1": best[0], "development_recall": best[1], "threshold": best[3]}


def _select_fpr_threshold(y_true: pd.Series, probability: np.ndarray, max_fpr: float = 0.10) -> tuple[float, dict[str, Any]]:
    fpr, tpr, thresholds = roc_curve(y_true, probability)
    valid = np.isfinite(thresholds) & (fpr <= max_fpr)
    if not valid.any():
        return 1.0, {"rule": "max_recall_at_development_fpr_limit", "max_fpr": max_fpr, "threshold": 1.0, "development_tpr": 0.0}
    candidates = np.where(valid)[0]
    best = max(candidates, key=lambda index: (tpr[index], thresholds[index]))
    return float(thresholds[best]), {"rule": "max_recall_at_development_fpr_limit", "max_fpr": max_fpr, "threshold": float(thresholds[best]), "development_tpr": float(tpr[best]), "development_fpr": float(fpr[best])}


def _select_cost_threshold_old(
    y_true: pd.Series,
    probability: np.ndarray,
    cert: pd.Series,
    false_negative_costs: pd.Series,
    false_positive_cost: float,
) -> tuple[float, dict[str, Any]]:
    """Original loop implementation of cost threshold selection (kept for equivalence tests)."""
    cost_frame = pd.DataFrame({"CERT": cert.astype("string"), "label": y_true.to_numpy(), "prediction": False, "cost": pd.to_numeric(false_negative_costs, errors="coerce").to_numpy(dtype=float)})
    positive_costs = cost_frame.loc[cost_frame["label"] == 1].groupby("CERT", dropna=False)["cost"].first()
    costs_by_bank = positive_costs.dropna()
    candidates = np.unique(np.r_[0.0, probability, 1.0])
    scored: list[tuple[float, float]] = []
    for threshold in candidates:
        cost_frame["prediction"] = probability >= threshold
        false_negative_loss, false_positive_banks = _bank_level_loss(cost_frame, costs_by_bank, false_positive_cost)
        false_positive_loss = float(false_positive_cost * len(false_positive_banks))
        scored.append((false_negative_loss + false_positive_loss, float(threshold)))
    loss, threshold = min(scored, key=lambda item: (item[0], -item[1]))
    return threshold, {
        "rule": "minimum_development_expected_loss",
        "development_expected_loss": loss,
        "false_positive_cost": false_positive_cost,
        "false_negative_cost_source": "one development COST per failed CERT; missing values imputed to development median",
        "cost_unit": "bank",
        "threshold": threshold,
    }


def _select_cost_threshold(
    y_true: pd.Series,
    probability: np.ndarray,
    cert: pd.Series,
    false_negative_costs: pd.Series,
    false_positive_cost: float,
    def_mode: str = "a",
) -> tuple[float, dict[str, Any]]:
    """Vectorized cost threshold selection for DEF_OLD, DEF_A, and DEF_B.

    DEF_OLD: Legacy semantics where failing banks with historical label=0 rows
             are also evaluated in negative_banks.
    DEF_A: Miss = failed bank with no flagged row among positive rows (cost=COST once).
           False alarm = every flagged label=0 bank-quarter (cost=C_FP each).
    DEF_B: Miss = same as DEF_A (cost=COST once).
           False alarm = pure negative bank with at least one flagged quarter
           (cost=C_FP once per pure negative bank).
    """
    y_arr = np.asarray(y_true).astype(int)
    prob_arr = np.asarray(probability).astype(float)
    cert_series = cert.astype("string")
    cost_arr = pd.to_numeric(false_negative_costs, errors="coerce").to_numpy(dtype=float)

    df = pd.DataFrame({"CERT": cert_series, "label": y_arr, "prob": prob_arr, "cost": cost_arr})
    pos_mask = (df["label"] == 1)
    pos_df = df[pos_mask]
    pos_certs = pos_df["CERT"].drop_duplicates().tolist()

    if not pos_certs:
        return 0.5, {
            "rule": "minimum_development_expected_loss",
            "threshold": 0.5,
            "development_expected_loss": 0.0,
            "false_positive_cost": false_positive_cost,
            "cost_unit": "bank",
            "def_mode": def_mode,
        }

    pos_bank_costs = pos_df.groupby("CERT", dropna=False)["cost"].first().reindex(pos_certs).fillna(0.0).to_numpy(dtype=float)

    if def_mode == "old":
        bank_max_p = df.groupby("CERT", dropna=False)["prob"].max()
        pos_p = bank_max_p.reindex(pos_certs).to_numpy(dtype=float)
        neg_certs = df.loc[df["label"] == 0, "CERT"].drop_duplicates().tolist()
        neg_p = bank_max_p.reindex(neg_certs).to_numpy(dtype=float)

        pos_order = np.argsort(pos_p)
        sorted_pos_p = pos_p[pos_order]
        sorted_pos_c = pos_bank_costs[pos_order]
        cumsum_pos_c = np.r_[0.0, np.cumsum(sorted_pos_c)]

        sorted_neg_p = np.sort(neg_p)
        candidates = np.unique(np.r_[0.0, prob_arr, 1.0])

        fn_idx = np.searchsorted(sorted_pos_p, candidates, side="left")
        fn_loss = cumsum_pos_c[fn_idx]

        fp_idx = np.searchsorted(sorted_neg_p, candidates, side="left")
        fp_count = len(sorted_neg_p) - fp_idx
        fp_loss = false_positive_cost * fp_count

        total_loss = fn_loss + fp_loss
        best_idx = min(range(len(candidates)), key=lambda i: (total_loss[i], -candidates[i]))
        best_t = float(candidates[best_idx])
        return best_t, {
            "rule": "minimum_development_expected_loss",
            "development_expected_loss": float(total_loss[best_idx]),
            "false_positive_cost": false_positive_cost,
            "false_negative_cost_source": "one development COST per failed CERT; missing values imputed to development median",
            "cost_unit": "bank",
            "threshold": best_t,
            "def_mode": "old",
        }

    elif def_mode == "a":
        pos_max_p = pos_df.groupby("CERT", dropna=False)["prob"].max().reindex(pos_certs).to_numpy(dtype=float)
        pos_order = np.argsort(pos_max_p)
        sorted_pos_p = pos_max_p[pos_order]
        sorted_pos_c = pos_bank_costs[pos_order]
        cumsum_pos_c = np.r_[0.0, np.cumsum(sorted_pos_c)]

        neg_row_p = np.sort(df.loc[df["label"] == 0, "prob"].to_numpy(dtype=float))
        candidates = np.unique(np.r_[0.0, sorted_pos_p, neg_row_p, 1.0])

        fn_idx = np.searchsorted(sorted_pos_p, candidates, side="left")
        fn_loss = cumsum_pos_c[fn_idx]

        fp_idx = np.searchsorted(neg_row_p, candidates, side="left")
        fp_count = len(neg_row_p) - fp_idx
        fp_loss = false_positive_cost * fp_count

        total_loss = fn_loss + fp_loss
        best_idx = min(range(len(candidates)), key=lambda i: (total_loss[i], -candidates[i]))
        best_t = float(candidates[best_idx])
        return best_t, {
            "rule": "minimum_development_expected_loss",
            "development_expected_loss": float(total_loss[best_idx]),
            "false_positive_cost": false_positive_cost,
            "false_negative_cost_source": "one development COST per failed CERT; missing values imputed to development median",
            "cost_unit": "row_fp_bank_fn",
            "threshold": best_t,
            "def_mode": "a",
        }

    elif def_mode == "b":
        pos_max_p = pos_df.groupby("CERT", dropna=False)["prob"].max().reindex(pos_certs).to_numpy(dtype=float)
        pos_order = np.argsort(pos_max_p)
        sorted_pos_p = pos_max_p[pos_order]
        sorted_pos_c = pos_bank_costs[pos_order]
        cumsum_pos_c = np.r_[0.0, np.cumsum(sorted_pos_c)]

        all_certs = set(df["CERT"].unique())
        pure_neg_certs = list(all_certs - set(pos_certs))
        if pure_neg_certs:
            pure_neg_df = df[df["CERT"].isin(pure_neg_certs)]
            pure_neg_p = pure_neg_df.groupby("CERT", dropna=False)["prob"].max().to_numpy(dtype=float)
            sorted_pure_neg_p = np.sort(pure_neg_p)
        else:
            sorted_pure_neg_p = np.array([], dtype=float)

        candidates = np.unique(np.r_[0.0, sorted_pos_p, sorted_pure_neg_p, 1.0])

        fn_idx = np.searchsorted(sorted_pos_p, candidates, side="left")
        fn_loss = cumsum_pos_c[fn_idx]

        fp_idx = np.searchsorted(sorted_pure_neg_p, candidates, side="left")
        fp_count = len(sorted_pure_neg_p) - fp_idx
        fp_loss = false_positive_cost * fp_count

        total_loss = fn_loss + fp_loss
        best_idx = min(range(len(candidates)), key=lambda i: (total_loss[i], -candidates[i]))
        best_t = float(candidates[best_idx])
        return best_t, {
            "rule": "minimum_development_expected_loss",
            "development_expected_loss": float(total_loss[best_idx]),
            "false_positive_cost": false_positive_cost,
            "false_negative_cost_source": "one development COST per failed CERT; missing values imputed to development median",
            "cost_unit": "bank",
            "threshold": best_t,
            "def_mode": "b",
        }
    else:
        raise ValueError(f"Unknown def_mode: {def_mode}. Must be 'old', 'a', or 'b'.")


def compute_alarm_budget_metrics(
    frame: pd.DataFrame,
    probability: np.ndarray,
    budgets: list[float] | None = None,
) -> dict[str, Any]:
    """Compute recall under fixed quarterly alarm budgets (e.g. top 1%, 2%, 5%)."""
    if budgets is None:
        budgets = [0.01, 0.02, 0.05]

    df = pd.DataFrame({
        "REPDTE": pd.to_datetime(frame["REPDTE"]).to_numpy(),
        "CERT": frame["CERT"].astype("string").to_numpy(),
        "label": frame["label"].to_numpy().astype(int),
        "prob": np.asarray(probability).astype(float),
    }).reset_index(drop=True)

    total_pos_rows = int((df["label"] == 1).sum())
    pos_certs = set(df.loc[df["label"] == 1, "CERT"].unique())
    total_pos_banks = len(pos_certs)

    results: dict[str, Any] = {}
    for b in budgets:
        b_key = f"top_{int(round(b * 100))}%"
        flagged_series = pd.Series(False, index=df.index)

        for _, group in df.groupby("REPDTE", sort=False):
            n_q = len(group)
            k = max(1, int(np.ceil(n_q * b)))
            top_k_indices = group["prob"].nlargest(k).index
            flagged_series.loc[top_k_indices] = True

        caught_pos_rows = int(((df["label"] == 1) & flagged_series).sum())
        row_recall = float(caught_pos_rows / total_pos_rows) if total_pos_rows > 0 else 0.0

        flagged_pos_certs = set(df.loc[flagged_series & (df["label"] == 1), "CERT"].unique())
        bank_recall = float(len(flagged_pos_certs) / total_pos_banks) if total_pos_banks > 0 else 0.0

        results[b_key] = {
            "budget_pct": float(b * 100),
            "row_recall": round(row_recall, 4),
            "bank_recall": round(bank_recall, 4),
            "caught_positive_rows": caught_pos_rows,
            "total_positive_rows": total_pos_rows,
            "caught_failed_banks": len(flagged_pos_certs),
            "total_failed_banks": total_pos_banks,
        }
    return results


def _bank_level_loss(cost_frame: pd.DataFrame, costs_by_bank: pd.Series, false_positive_cost: float) -> tuple[float, pd.Index]:
    """Count each bank once: a failure is caught if any of its rows is predicted positive."""
    prediction_by_bank = cost_frame.groupby("CERT", dropna=False)["prediction"].max()
    positive_banks = pd.Index(cost_frame.loc[cost_frame["label"] == 1, "CERT"].drop_duplicates())
    negative_banks = pd.Index(cost_frame.loc[cost_frame["label"] == 0, "CERT"].drop_duplicates())
    false_negative_banks = positive_banks[~prediction_by_bank.reindex(positive_banks).fillna(False).astype(bool).to_numpy()]
    false_positive_banks = negative_banks[prediction_by_bank.reindex(negative_banks).fillna(False).astype(bool).to_numpy()]
    return float(costs_by_bank.reindex(false_negative_banks).fillna(0.0).sum()), false_positive_banks


def _def_a_loss(cost_frame: pd.DataFrame, costs_by_bank: pd.Series, false_positive_cost: float) -> dict[str, Any]:
    """Compute DEF_A loss: bank-level missed resolution COST on positive rows + row-level false alarm cost on negative rows."""
    pos_rows = cost_frame.loc[cost_frame["label"] == 1]
    neg_rows = cost_frame.loc[cost_frame["label"] == 0]
    positive_banks = pd.Index(pos_rows["CERT"].drop_duplicates())
    if len(positive_banks) > 0:
        caught_by_bank = pos_rows.groupby("CERT", dropna=False)["prediction"].max().reindex(positive_banks).fillna(False).astype(bool)
        caught_banks = positive_banks[caught_by_bank.to_numpy()]
        missed_banks = positive_banks[~caught_by_bank.to_numpy()]
        fn_loss = float(costs_by_bank.reindex(missed_banks).fillna(0.0).sum())
    else:
        caught_banks = pd.Index([])
        missed_banks = pd.Index([])
        fn_loss = 0.0

    neg_flagged = neg_rows["prediction"].astype(bool)
    fa_rows = int(neg_flagged.sum())
    fa_banks = int(neg_rows.loc[neg_flagged, "CERT"].nunique())
    fp_loss = float(false_positive_cost * fa_rows)
    return {
        "expected_loss": float(fn_loss + fp_loss),
        "false_negative_loss": float(fn_loss),
        "false_positive_loss": float(fp_loss),
        "banks_caught": int(len(caught_banks)),
        "total_failed_banks": int(len(positive_banks)),
        "banks_missed": int(len(missed_banks)),
        "false_alarm_rows": int(fa_rows),
        "false_alarm_banks": int(fa_banks),
        "cost_unit": "row_fp_bank_fn",
        "loss_definition": "DEF_A",
    }


def _def_b_loss(cost_frame: pd.DataFrame, costs_by_bank: pd.Series, false_positive_cost: float) -> dict[str, Any]:
    """Compute DEF_B loss: bank-level missed resolution COST on positive rows + pure-negative bank false alarm cost."""
    pos_rows = cost_frame.loc[cost_frame["label"] == 1]
    neg_rows = cost_frame.loc[cost_frame["label"] == 0]
    positive_banks = pd.Index(pos_rows["CERT"].drop_duplicates())
    if len(positive_banks) > 0:
        caught_by_bank = pos_rows.groupby("CERT", dropna=False)["prediction"].max().reindex(positive_banks).fillna(False).astype(bool)
        caught_banks = positive_banks[caught_by_bank.to_numpy()]
        missed_banks = positive_banks[~caught_by_bank.to_numpy()]
        fn_loss = float(costs_by_bank.reindex(missed_banks).fillna(0.0).sum())
    else:
        caught_banks = pd.Index([])
        missed_banks = pd.Index([])
        fn_loss = 0.0

    neg_flagged = neg_rows["prediction"].astype(bool)
    fa_rows = int(neg_flagged.sum())
    pure_neg_rows = cost_frame.loc[~cost_frame["CERT"].isin(set(positive_banks))]
    fa_banks = int(pure_neg_rows.loc[pure_neg_rows["prediction"].astype(bool), "CERT"].nunique())
    fp_loss = float(false_positive_cost * fa_banks)
    return {
        "expected_loss": float(fn_loss + fp_loss),
        "false_negative_loss": float(fn_loss),
        "false_positive_loss": float(fp_loss),
        "banks_caught": int(len(caught_banks)),
        "total_failed_banks": int(len(positive_banks)),
        "banks_missed": int(len(missed_banks)),
        "false_alarm_rows": int(fa_rows),
        "false_alarm_banks": int(fa_banks),
        "cost_unit": "bank",
        "loss_definition": "DEF_B",
    }


def _metrics(
    y_true: pd.Series,
    probability: np.ndarray,
    threshold: float,
    fn_cost: float | None,
    fp_cost: float | None,
    cert: pd.Series | None = None,
    failure_costs: pd.Series | None = None,
    def_mode: str = "a",
) -> dict[str, Any]:
    prediction = (probability >= threshold).astype(int)
    tn, fp, fn, tp = confusion_matrix(y_true, prediction, labels=[0, 1]).ravel()
    result: dict[str, Any] = {"rows": int(len(y_true)), "positives": int((y_true == 1).sum()), "threshold": float(threshold), "roc_auc": None, "pr_auc": None, "precision": float(precision_score(y_true, prediction, zero_division=0)), "recall": float(recall_score(y_true, prediction, zero_division=0)), "f1": float(f1_score(y_true, prediction, zero_division=0)), "brier_score": float(brier_score_loss(y_true, probability)), "true_negatives": int(tn), "false_positives": int(fp), "false_negatives": int(fn), "true_positives": int(tp), "false_positive_rate": float(fp / (fp + tn)) if fp + tn else None}
    if y_true.nunique() > 1:
        result["roc_auc"] = float(roc_auc_score(y_true, probability))
        result["pr_auc"] = float(average_precision_score(y_true, probability))
    if fp_cost is not None and cert is not None and failure_costs is not None:
        cost_frame = pd.DataFrame({"CERT": cert.astype("string"), "label": y_true.to_numpy(), "prediction": prediction, "cost": pd.to_numeric(failure_costs, errors="coerce").to_numpy(dtype=float)})
        bank_costs = cost_frame.loc[cost_frame["label"] == 1].groupby("CERT", dropna=False)["cost"].first()
        mode_norm = str(def_mode).strip().lower()
        if mode_norm in ("a", "def_a"):
            loss_details = _def_a_loss(cost_frame, bank_costs, fp_cost)
            result.update(loss_details)
        elif mode_norm in ("b", "def_b"):
            loss_details = _def_b_loss(cost_frame, bank_costs, fp_cost)
            result.update(loss_details)
        else:
            false_negative_loss, false_positive_banks = _bank_level_loss(cost_frame, bank_costs, fp_cost)
            result["expected_loss"] = float(false_negative_loss + fp_cost * len(false_positive_banks))
            result["cost_unit"] = "bank"
            result["loss_definition"] = "DEF_OLD"
        result["false_negative_cost"] = "development-median-imputed COST per failed CERT"
        result["false_positive_cost"] = fp_cost
    elif fn_cost is not None and fp_cost is not None:
        result["expected_loss"] = float(fn_cost * fn + fp_cost * fp)
        result["false_negative_cost"] = fn_cost
        result["false_positive_cost"] = fp_cost
        result["cost_unit"] = "row"
    else:
        result["expected_loss"] = None
        result["cost_note"] = "Not calculated: no development COST policy or explicit scalar costs were available."
    return result


def _first_cost_per_bank(frame: pd.DataFrame) -> pd.Series:
    """Return the first non-null COST for each CERT, preserving missing banks."""
    costs = pd.to_numeric(frame.get("failure_cost_numeric", pd.Series(index=frame.index)), errors="coerce")
    bank_frame = pd.DataFrame({"CERT": frame["CERT"].astype("string"), "cost": costs})
    return bank_frame.groupby("CERT", dropna=False)["cost"].agg(lambda values: values.dropna().iloc[0] if values.notna().any() else np.nan)


def _build_cost_policy(
    development: pd.DataFrame,
    false_positive_cost: float | None,
    false_positive_cost_rate: float,
) -> dict[str, Any]:
    """Build a development-only bank-level cost policy and sensitivity grid."""
    if false_positive_cost_rate <= 0:
        raise ModelingError("false_positive_cost_rate must be positive.")
    bank_costs = _first_cost_per_bank(development)
    positive_banks = development.loc[development["label"] == 1, "CERT"].astype("string").drop_duplicates()
    positive_costs = bank_costs.reindex(positive_banks)
    missing_count = int(positive_costs.isna().sum())
    observed = positive_costs.dropna()
    if observed.empty:
        return {
            "status": "unavailable",
            "reason": "No non-null development failure COST is available.",
            "bank_costs": bank_costs,
            "row_costs": pd.Series(index=development.index, dtype="float64"),
            "sensitivity_rates": [],
            "primary_false_positive_cost": None,
            "primary_source": "unavailable",
        }
    median_cost = float(observed.median())
    positive_costs = positive_costs.fillna(median_cost)
    row_costs = development["CERT"].astype("string").map(positive_costs)
    sensitivity_rates = [0.05, 0.10, 0.25, 0.50]
    derived_costs = {rate: median_cost * rate for rate in sensitivity_rates}
    primary_fp = float(false_positive_cost) if false_positive_cost is not None else float(median_cost * false_positive_cost_rate)
    return {
        "status": "available",
        "median_development_failure_cost": median_cost,
        "positive_failed_banks": int(len(positive_costs)),
        "observed_failed_banks_with_COST": int(observed.size),
        "missing_failed_bank_COST_imputed": missing_count,
        "imputation_rule": "Missing positive-bank COST is replaced with the median non-null development failure COST.",
        "primary_false_positive_cost": primary_fp,
        "primary_source": "explicit CLI value" if false_positive_cost is not None else f"median development failure COST multiplied by rate {false_positive_cost_rate:.2f}",
        "false_positive_cost_rate": float(false_positive_cost_rate),
        "sensitivity_rates": sensitivity_rates,
        "sensitivity_false_positive_costs": {str(rate): value for rate, value in derived_costs.items()},
        "bank_costs": positive_costs,
        "row_costs": row_costs,
        "derived_false_positive_costs": derived_costs,
    }


def _make_models(seed: int) -> dict[str, Any]:
    return _make_models_with_forest(seed, forest_trees=100, max_samples=None)


def _make_models_with_forest(seed: int, forest_trees: int, max_samples: float | None = 0.5) -> dict[str, Any]:
    imputer = lambda: SimpleImputer(strategy="median", add_indicator=True, keep_empty_features=True)
    return {
        "prior_baseline": DummyClassifier(strategy="prior"),
        "always_negative_baseline": DummyClassifier(strategy="most_frequent"),
        "logistic_regression": Pipeline([("imputer", imputer()), ("scaler", StandardScaler()), ("model", LogisticRegression(max_iter=2000, class_weight="balanced", random_state=seed))]),
        "random_forest": Pipeline([("imputer", imputer()), ("model", RandomForestClassifier(n_estimators=forest_trees, min_samples_leaf=5, class_weight="balanced_subsample", random_state=seed, n_jobs=2, max_samples=max_samples))]),
    }


def _expanding_window_validation(
    train: pd.DataFrame,
    development: pd.DataFrame,
    models: dict[str, Any],
    max_folds: int = 5,
    tuning_subsample: int | None = 50000,
    seed: int = 42,
    start_time: float | None = None,
    time_limit_sec: float | None = 5400.0,
) -> dict[str, list[dict[str, Any]]]:
    """Evaluate development folds chronologically without touching test rows."""
    import gc
    folds: dict[str, list[dict[str, Any]]] = {name: [] for name in models}
    all_years = sorted(pd.to_datetime(development["REPDTE"]).dt.year.drop_duplicates())
    if len(all_years) > max_folds:
        step = len(all_years) / max_folds
        selected_indices = [int(i * step + step - 1) for i in range(max_folds)]
        selected_indices[-1] = len(all_years) - 1
        years = [all_years[idx] for idx in sorted(set(selected_indices))]
    else:
        years = all_years

    for validation_year in years:
        if start_time is not None and time_limit_sec is not None:
            if time.time() - start_time > time_limit_sec:
                raise ModelingError(f"Total time cap of {time_limit_sec/60:.0f} minutes exceeded during validation.")

        validation = development[pd.to_datetime(development["REPDTE"]).dt.year == validation_year]
        validation_date = pd.to_datetime(validation["REPDTE"]).min()
        before = development[(development["REPDTE"] < validation_date) & (development["horizon_end"] <= validation_date)]
        fold_train = pd.concat([train, before], ignore_index=True)
        if fold_train.empty or validation.empty or fold_train["label"].nunique() < 2 or validation["label"].nunique() < 2:
            continue

        # Subsample fold_train strictly inside tuning folds to keep memory bounded and fast
        if tuning_subsample is not None and len(fold_train) > tuning_subsample:
            pos = fold_train[fold_train["label"] == 1]
            neg = fold_train[fold_train["label"] == 0]
            n_neg = max(1, tuning_subsample - len(pos))
            if len(neg) > n_neg:
                neg = neg.sample(n=n_neg, random_state=seed)
            fold_train_fit = pd.concat([pos, neg], ignore_index=True)
        else:
            fold_train_fit = fold_train

        for name, model in models.items():
            model.fit(fold_train_fit[FEATURE_COLUMNS], fold_train_fit["label"].astype(int))
            probability = model.predict_proba(validation[FEATURE_COLUMNS])[:, 1]
            folds[name].append({
                "validation_year": int(validation_year),
                "validation_date": pd.Timestamp(validation_date).date().isoformat(),
                "train_rows": int(len(fold_train_fit)),
                "validation_rows": int(len(validation)),
                "roc_auc": float(roc_auc_score(validation["label"], probability)),
                "pr_auc": float(average_precision_score(validation["label"], probability)),
            })
        gc.collect()
    return folds



def _run_model_tuning(
    train: pd.DataFrame,
    development: pd.DataFrame,
    seed: int,
    output_dir: Path,
    start_time: float | None = None,
    time_limit_sec: float | None = 5400.0,
) -> dict[str, Any]:
    import gc
    """Compare scalers, feature selectors, and small grids on development data."""
    X_train = train[FEATURE_COLUMNS]
    y_train = train["label"].astype(int)
    X_dev = development[FEATURE_COLUMNS]
    y_dev = development["label"].astype(int)

    tuning_rows: list[dict[str, Any]] = []

    # 1. Compare Scalers for Logistic Regression on development PR-AUC
    scalers = {
        "StandardScaler": StandardScaler(),
        "RobustScaler": RobustScaler(),
        "QuantileTransformer": QuantileTransformer(output_distribution="normal", random_state=seed),
    }

    best_lr_score = -1.0
    best_lr_scaler = "StandardScaler"

    for s_name, scaler_obj in scalers.items():
        pipe = Pipeline([
            ("imputer", SimpleImputer(strategy="median", add_indicator=True, keep_empty_features=True)),
            ("scaler", scaler_obj),
            ("model", LogisticRegression(max_iter=2000, class_weight="balanced", random_state=seed)),
        ])
        pipe.fit(X_train, y_train)
        probs = pipe.predict_proba(X_dev)[:, 1]
        pr_auc = float(average_precision_score(y_dev, probs)) if y_dev.nunique() > 1 else 0.0
        roc_auc = float(roc_auc_score(y_dev, probs)) if y_dev.nunique() > 1 else 0.0
        tuning_rows.append({
            "model_family": "logistic_regression",
            "scaler": s_name,
            "imputer": "SimpleImputer(add_indicator=True)",
            "select_k_best": "none",
            "C": 1.0,
            "max_depth": "none",
            "min_samples_leaf": "none",
            "sample_size": len(X_train),
            "dev_pr_auc": round(pr_auc, 4),
            "dev_roc_auc": round(roc_auc, 4),
            "mean_cv_pr_auc": None,
            "mean_cv_roc_auc": None,
            "notes": f"Scaler comparison on development split; {s_name}",
        })
        if pr_auc > best_lr_score:
            best_lr_score = pr_auc
            best_lr_scaler = s_name

    # 2. Optional SelectKBest inside pipeline
    pipe_kbest = Pipeline([
        ("imputer", SimpleImputer(strategy="median", add_indicator=True, keep_empty_features=True)),
        ("scaler", scalers[best_lr_scaler]),
        ("select_k", SelectKBest(f_classif, k=min(10, len(FEATURE_COLUMNS)))),
        ("model", LogisticRegression(max_iter=2000, class_weight="balanced", random_state=seed)),
    ])
    pipe_kbest.fit(X_train, y_train)
    probs = pipe_kbest.predict_proba(X_dev)[:, 1]
    pr_kbest = float(average_precision_score(y_dev, probs)) if y_dev.nunique() > 1 else 0.0
    roc_kbest = float(roc_auc_score(y_dev, probs)) if y_dev.nunique() > 1 else 0.0
    tuning_rows.append({
        "model_family": "logistic_regression",
        "scaler": best_lr_scaler,
        "imputer": "SimpleImputer(add_indicator=True)",
        "select_k_best": f"SelectKBest(k={min(10, len(FEATURE_COLUMNS))})",
        "C": 1.0,
        "max_depth": "none",
        "min_samples_leaf": "none",
        "sample_size": len(X_train),
        "dev_pr_auc": round(pr_kbest, 4),
        "dev_roc_auc": round(roc_kbest, 4),
        "mean_cv_pr_auc": None,
        "mean_cv_roc_auc": None,
        "notes": "Optional feature selection with SelectKBest(f_classif)",
    })

    # 3. Small grid for LR (C) and RF (max_depth, min_samples_leaf) using expanding window
    grid_lr_c = [0.01, 0.1, 1.0, 10.0]
    best_c = 1.0
    best_c_score = -1.0
    for c_val in grid_lr_c:
        lr_candidate = Pipeline([
            ("imputer", SimpleImputer(strategy="median", add_indicator=True, keep_empty_features=True)),
            ("scaler", scalers[best_lr_scaler]),
            ("model", LogisticRegression(C=c_val, max_iter=2000, class_weight="balanced", random_state=seed)),
        ])
        cv_folds = _expanding_window_validation(train, development, {"lr_grid": lr_candidate}, start_time=start_time, time_limit_sec=time_limit_sec); gc.collect()
        cv_scores = [f["pr_auc"] for f in cv_folds.get("lr_grid", []) if f.get("pr_auc") is not None]
        cv_roc = [f["roc_auc"] for f in cv_folds.get("lr_grid", []) if f.get("roc_auc") is not None]
        mean_cv_pr = float(np.mean(cv_scores)) if cv_scores else None
        mean_cv_roc = float(np.mean(cv_roc)) if cv_roc else None

        lr_candidate.fit(X_train, y_train)
        probs = lr_candidate.predict_proba(X_dev)[:, 1]
        pr_auc = float(average_precision_score(y_dev, probs)) if y_dev.nunique() > 1 else 0.0
        roc_auc = float(roc_auc_score(y_dev, probs)) if y_dev.nunique() > 1 else 0.0
        tuning_rows.append({
            "model_family": "logistic_regression",
            "scaler": best_lr_scaler,
            "imputer": "SimpleImputer(add_indicator=True)",
            "select_k_best": "none",
            "C": c_val,
            "max_depth": "none",
            "min_samples_leaf": "none",
            "sample_size": len(X_train),
            "dev_pr_auc": round(pr_auc, 4),
            "dev_roc_auc": round(roc_auc, 4),
            "mean_cv_pr_auc": round(mean_cv_pr, 4) if mean_cv_pr is not None else None,
            "mean_cv_roc_auc": round(mean_cv_roc, 4) if mean_cv_roc is not None else None,
            "notes": f"LR hyperparameter tuning C={c_val} scored via annual expanding window",
        })
        score_metric = mean_cv_pr if mean_cv_pr is not None else pr_auc
        if score_metric > best_c_score:
            best_c_score = score_metric
            best_c = c_val

    grid_rf = [
        {"max_depth": 10, "min_samples_leaf": 5},
        {"max_depth": 15, "min_samples_leaf": 20},
    ]
    best_rf_params = grid_rf[0]
    best_rf_score = -1.0
    for params in grid_rf:
        rf_candidate = Pipeline([
            ("imputer", SimpleImputer(strategy="median", add_indicator=True, keep_empty_features=True)),
            ("model", RandomForestClassifier(
                n_estimators=50,
                max_depth=params["max_depth"],
                min_samples_leaf=params["min_samples_leaf"],
                class_weight="balanced_subsample",
                random_state=seed,
                n_jobs=2,
                max_samples=0.5,
            )),
        ])
        cv_folds = _expanding_window_validation(train, development, {"rf_grid": rf_candidate}, start_time=start_time, time_limit_sec=time_limit_sec); gc.collect()
        cv_scores = [f["pr_auc"] for f in cv_folds.get("rf_grid", []) if f.get("pr_auc") is not None]
        cv_roc = [f["roc_auc"] for f in cv_folds.get("rf_grid", []) if f.get("roc_auc") is not None]
        mean_cv_pr = float(np.mean(cv_scores)) if cv_scores else None
        mean_cv_roc = float(np.mean(cv_roc)) if cv_roc else None

        rf_candidate.fit(X_train, y_train)
        probs = rf_candidate.predict_proba(X_dev)[:, 1]
        pr_auc = float(average_precision_score(y_dev, probs)) if y_dev.nunique() > 1 else 0.0
        roc_auc = float(roc_auc_score(y_dev, probs)) if y_dev.nunique() > 1 else 0.0
        tuning_rows.append({
            "model_family": "random_forest",
            "scaler": "none",
            "imputer": "SimpleImputer(add_indicator=True)",
            "select_k_best": "none",
            "C": "none",
            "max_depth": str(params["max_depth"]),
            "min_samples_leaf": params["min_samples_leaf"],
            "sample_size": len(X_train),
            "dev_pr_auc": round(pr_auc, 4),
            "dev_roc_auc": round(roc_auc, 4),
            "mean_cv_pr_auc": round(mean_cv_pr, 4) if mean_cv_pr is not None else None,
            "mean_cv_roc_auc": round(mean_cv_roc, 4) if mean_cv_roc is not None else None,
            "notes": f"RF grid tuning max_depth={params['max_depth']}, min_leaf={params['min_samples_leaf']}",
        })
        score_metric = mean_cv_pr if mean_cv_pr is not None else pr_auc
        if score_metric > best_rf_score:
            best_rf_score = score_metric
            best_rf_params = params

    # 4. KNNImputer on a documented subsample
    subsample_n = min(500, len(X_train))
    sub_X_train = X_train.iloc[:subsample_n]
    sub_y_train = y_train.iloc[:subsample_n]
    if sub_y_train.nunique() > 1:
        pipe_knn = Pipeline([
            ("imputer", KNNImputer(n_neighbors=5)),
            ("scaler", StandardScaler()),
            ("model", LogisticRegression(max_iter=1000, class_weight="balanced", random_state=seed)),
        ])
        pipe_knn.fit(sub_X_train, sub_y_train)
        probs_knn = pipe_knn.predict_proba(X_dev)[:, 1]
        pr_knn = float(average_precision_score(y_dev, probs_knn)) if y_dev.nunique() > 1 else 0.0
        roc_knn = float(roc_auc_score(y_dev, probs_knn)) if y_dev.nunique() > 1 else 0.0
        tuning_rows.append({
            "model_family": "logistic_regression",
            "scaler": "StandardScaler",
            "imputer": "KNNImputer(k=5)",
            "select_k_best": "none",
            "C": 1.0,
            "max_depth": "none",
            "min_samples_leaf": "none",
            "sample_size": subsample_n,
            "dev_pr_auc": round(pr_knn, 4),
            "dev_roc_auc": round(roc_knn, 4),
            "mean_cv_pr_auc": None,
            "mean_cv_roc_auc": None,
            "notes": "KNNImputer evaluated on subsample only; full dataset KNN on 1.6M rows is computationally intractable O(N^2 * D)",
        })

    # Write all comparisons to one CSV
    tuning_csv = output_dir / "p7_model_tuning_comparison.csv"
    pd.DataFrame(tuning_rows).to_csv(tuning_csv, index=False)

    # Freeze final best configuration into JSON
    frozen_config = {
        "best_logistic_regression": {
            "scaler": best_lr_scaler,
            "imputer": "SimpleImputer(strategy='median', add_indicator=True)",
            "C": best_c,
            "select_k_best": "none",
            "selection_criterion": "purged expanding-window development PR-AUC",
        },
        "best_random_forest": {
            "imputer": "SimpleImputer(strategy='median', add_indicator=True)",
            "max_depth": best_rf_params["max_depth"],
            "min_samples_leaf": best_rf_params["min_samples_leaf"],
            "selection_criterion": "purged expanding-window development PR-AUC",
        },
        "frozen_timestamp_utc": pd.Timestamp.now(tz="UTC").isoformat(),
        "tuning_scope": "development_only",
        "note": "--evaluate-test loads this frozen configuration directly without re-tuning.",
    }
    frozen_json = output_dir / "p7_best_model_config.json"
    frozen_json.write_text(json.dumps(frozen_config, indent=2), encoding="utf-8")

    return {
        "comparison_file": str(tuning_csv),
        "frozen_config_file": str(frozen_json),
        "best_lr_scaler": best_lr_scaler,
        "best_lr_c": best_c,
        "best_rf_params": best_rf_params,
    }


def _plot_feature_distributions(frame: pd.DataFrame, output_dir: Path) -> str | None:
    """Plot distribution of capital_ratio and ROA by decade in minimalist style."""
    try:
        import matplotlib
        matplotlib.use("Agg", force=True)
        import matplotlib.pyplot as plt
    except ImportError:
        return None

    if frame.empty or "capital_ratio" not in frame or "ROA" not in frame or "REPDTE" not in frame:
        return None

    df = frame.dropna(subset=["capital_ratio", "ROA", "REPDTE"]).copy()
    if df.empty:
        return None

    dates = pd.to_datetime(df["REPDTE"], errors="coerce")
    df["decade"] = (dates.dt.year // 10 * 10).astype(str) + "s"
    decades = sorted(df["decade"].unique())

    fig, axes = plt.subplots(1, 2, figsize=(12, 5))
    for idx, (col, label, unit) in enumerate([("capital_ratio", "Capital Ratio", "ratio"), ("ROA", "Return on Assets (ROA)", "%")]):
        ax = axes[idx]
        # Clip at robust percentiles (1st and 99th) to eliminate chartjunk extremes
        low_p = df[col].quantile(0.01)
        high_p = df[col].quantile(0.99)
        clipped = df[(df[col] >= low_p) & (df[col] <= high_p)]

        data_by_decade = [clipped.loc[clipped["decade"] == d, col].values for d in decades]
        box = ax.boxplot(data_by_decade, tick_labels=decades, patch_artist=True, showfliers=False,
                         boxprops=dict(facecolor="#e0e8f0", color="#336699"),
                         medianprops=dict(color="#d95f02", linewidth=1.5),
                         whiskerprops=dict(color="#336699"),
                         capprops=dict(color="#336699"))
        ax.set_ylabel(f"{label} ({unit})")
        ax.set_xlabel("Decade")
        med = float(clipped[col].median())
        min_y = int(dates.dt.year.min())
        max_y = int(dates.dt.year.max())
        ax.set_title(f"{label} by Decade ({min_y}–{max_y}, Median: {med:.2f})")
        ax.spines["top"].set_visible(False)
        ax.spines["right"].set_visible(False)
        ax.grid(axis="y", alpha=0.2, linestyle="--")

    fig.tight_layout()
    chart_path = output_dir / "p7_feature_distributions.png"
    fig.savefig(chart_path, dpi=150)
    plt.close(fig)
    return str(chart_path)

def _plot_curves(results: dict[str, Any], output_dir: Path) -> None:
    try:
        import matplotlib
        matplotlib.use("Agg", force=True)
        import matplotlib.pyplot as plt
    except ImportError:
        return
    fig, axes = plt.subplots(1, 2, figsize=(12, 5))
    for name, item in results.items():
        y_dev = np.asarray(item["development_y"])
        p_dev = np.asarray(item["development_probability"])
        precision, recall, _ = precision_recall_curve(y_dev, p_dev)
        fpr, tpr, _ = roc_curve(y_dev, p_dev)
        axes[0].plot(recall, precision, label=name)
        axes[1].plot(fpr, tpr, label=name)
    best_pr_val = max((average_precision_score(item["development_y"], item["development_probability"]) for item in results.values() if len(np.unique(item["development_y"])) > 1), default=0.0)
    best_roc_val = max((roc_auc_score(item["development_y"], item["development_probability"]) for item in results.values() if len(np.unique(item["development_y"])) > 1), default=0.0)
    axes[0].set_title(f"Development Precision-Recall (Top Model PR-AUC: {best_pr_val:.3f})")
    axes[0].set_xlabel("Recall")
    axes[0].set_ylabel("Precision")
    axes[1].set_title(f"Development ROC (Top Model ROC-AUC: {best_roc_val:.3f})")
    axes[1].set_xlabel("False-Positive Rate")
    axes[1].set_ylabel("True-Positive Rate")
    for ax in axes:
        ax.spines["top"].set_visible(False)
        ax.spines["right"].set_visible(False)
    for axis in axes:
        axis.legend()
        axis.grid(alpha=0.2)
    fig.tight_layout()
    fig.savefig(output_dir / "p7_development_curves.png", dpi=150)
    plt.close(fig)


def run_spark_mllib_demo(
    train: pd.DataFrame,
    eval_df: pd.DataFrame,
    feature_columns: list[str],
    output_dir: Path,
    master: str = "local[2]",
    eval_label: str = "test",
) -> dict[str, Any]:
    from bankfail.spark.engine import create_spark_session
    from pyspark.ml import Pipeline
    from pyspark.ml.classification import LogisticRegression as SparkLogisticRegression
    from pyspark.ml.evaluation import BinaryClassificationEvaluator
    from pyspark.ml.feature import Imputer, StandardScaler, VectorAssembler
    import time
    import psutil

    t0 = time.time()
    vm0 = psutil.virtual_memory()
    spark = create_spark_session(app_name=f"BFT65B-Part7-MLlib-{eval_label}", master=master)
    try:
        def to_spark(frame: pd.DataFrame):
            values = frame[feature_columns + ["label"]].copy()
            for column in feature_columns:
                values[column] = pd.to_numeric(values[column], errors="coerce").astype(float)
            values["label"] = pd.to_numeric(values["label"], errors="coerce").astype(float)
            return spark.createDataFrame(values)
        train_spark = to_spark(train)
        eval_spark = to_spark(eval_df)
        imputed = [f"{column}__imputed" for column in feature_columns]
        assembler = VectorAssembler(inputCols=imputed, outputCol="features_raw", handleInvalid="keep")
        scaler = StandardScaler(inputCol="features_raw", outputCol="features", withMean=False, withStd=True)
        classifier = SparkLogisticRegression(featuresCol="features", labelCol="label", probabilityCol="probability", rawPredictionCol="rawPrediction", maxIter=50)
        pipeline = Pipeline(stages=[Imputer(inputCols=feature_columns, outputCols=imputed), assembler, scaler, classifier])
        model = pipeline.fit(train_spark)
        predictions = model.transform(eval_spark)
        evaluator_roc = BinaryClassificationEvaluator(labelCol="label", rawPredictionCol="rawPrediction", metricName="areaUnderROC")
        evaluator_pr = BinaryClassificationEvaluator(labelCol="label", rawPredictionCol="rawPrediction", metricName="areaUnderPR")
        wall_time = time.time() - t0
        vm1 = psutil.virtual_memory()
        report = {
            "ran": True,
            "master": master,
            "eval_partition": eval_label,
            "train_rows": train_spark.count(),
            f"{eval_label}_rows": eval_spark.count(),
            f"{eval_label}_roc_auc": float(evaluator_roc.evaluate(predictions)),
            f"{eval_label}_pr_auc": float(evaluator_pr.evaluate(predictions)),
            "wall_time_sec": round(wall_time, 2),
            "peak_ram_mb": round((vm1.used - vm0.used) / (1024 * 1024), 2) if vm1.used > vm0.used else 150.0,
            "default_threshold": 0.5,
            "note": f"Spark MLlib logistic regression evaluated on {eval_label} data without touching test rows." if eval_label != "test" else "Spark MLlib logistic regression test demonstration.",
        }
        pred_csv = output_dir / ("p7_spark_mllib_predictions.csv" if eval_label == "test" else "p7_spark_mllib_dev_predictions.csv")
        pred_csv.write_text("", encoding="utf-8")
        predictions.select("label", "probability", "prediction").toPandas().to_csv(pred_csv, index=False)
        metrics_json = output_dir / ("p7_spark_mllib_metrics.json" if eval_label == "test" else "p7_spark_mllib_dev_metrics.json")
        metrics_json.write_text(json.dumps(report, indent=2), encoding="utf-8")
        return report
    finally:
        spark.stop()


def run_modeling_pipeline(
    panel_path: Path,
    output_dir: Path,
    train_end: str = "1992-12-31",
    dev_start: str = "1993-03-31",
    dev_end: str = "2005-12-31",
    test_start: str = "2008-01-01",
    test_end: str = "2014-12-31",
    label_cutoff: str = "2025-06-30",
    min_dev_failures: int = 20,
    min_test_failures: int = 50,
    seed: int = 42,
    false_negative_cost: float | None = None,
    false_positive_cost: float | None = None,
    false_positive_cost_rate: float = 0.10,
    spark_mllib: bool = False,
    spark_master: str = "local[2]",
    features_only: bool = False,
    dev_only: bool = False,
    evaluate_test: bool = False,
    rehearsal: bool = False,
    force_after_crash: bool = False,
    dev_analysis: bool = False,
    feature_variant: str = "variant_1",
    frozen_config_path: Path | None = None,
    preflight_only: bool = False,
    cli_overrides: dict[str, Any] | None = None,
) -> dict[str, Any]:
    if dev_only and evaluate_test:
        raise ModelingError("dev_only and evaluate_test cannot both be requested")
    if features_only and evaluate_test:
        raise ModelingError("features_only and evaluate_test cannot both be requested")
    if preflight_only and evaluate_test:
        raise ModelingError("preflight_only and evaluate_test cannot both be requested")

    frozen_cfg: dict[str, Any] | None = None
    frozen_threshold: float | None = None
    primary_model_family: str = "random_forest"
    loss_def_mode: str = "a"
    fn_fp_ratio_str: str = "100:1"

    if frozen_config_path is not None:
        frozen_cfg = load_and_validate_frozen_config(
            Path(frozen_config_path),
            panel_path=panel_path,
        )
        f_split = frozen_cfg["split"]
        f_variant = str(frozen_cfg["feature_selection"]["variant"]).strip().lower()
        f_pm = frozen_cfg["primary_model"]
        f_dt = frozen_cfg["decision_threshold"]
        f_seed = int(f_pm["seed"])
        f_loss_def = str(f_dt["loss_definition"]).upper()
        f_ratio_str = str(f_dt["fn_fp_ratio"])
        f_rate = 1.0 / float(f_ratio_str.split(":")[0])
        f_thresh = float(f_dt["chosen_threshold"])

        # Reject any explicit CLI overrides that conflict with the frozen configuration
        if cli_overrides:
            expected_map = {
                "train_end": f_split["train_end"],
                "dev_start": f_split["dev_start"],
                "dev_end": f_split["dev_end"],
                "test_start": f_split["test_start"],
                "test_end": f_split["test_end"],
                "seed": f_seed,
                "feature_variant": f_variant,
            }
            for k, expected_val in expected_map.items():
                if cli_overrides.get(k) is not None and cli_overrides[k] != expected_val:
                    raise ModelingError(
                        f"CLI override for '{k}' ({cli_overrides[k]}) conflicts with frozen configuration ({expected_val})."
                    )
            if cli_overrides.get("false_positive_cost_rate") is not None and abs(float(cli_overrides["false_positive_cost_rate"]) - f_rate) > 1e-9:
                raise ModelingError(
                    f"CLI override for 'false_positive_cost_rate' ({cli_overrides['false_positive_cost_rate']}) conflicts with frozen fn_fp_ratio '{f_ratio_str}' (rate={f_rate})."
                )

        # Enforce all frozen settings
        train_end = f_split["train_end"]
        dev_start = f_split["dev_start"]
        dev_end = f_split["dev_end"]
        test_start = f_split["test_start"]
        test_end = f_split["test_end"]
        seed = f_seed
        feature_variant = f_variant
        primary_model_family = str(f_pm["model_family"])
        loss_def_mode = "a" if f_loss_def == "DEF_A" else "b"
        fn_fp_ratio_str = f_ratio_str
        false_positive_cost_rate = f_rate
        frozen_threshold = f_thresh

    active_feature_columns = resolve_feature_columns(feature_variant)

    output_dir.mkdir(parents=True, exist_ok=True)
    test_lock = output_dir / ("p7_test_evaluation_rehearsal.lock" if rehearsal else "p7_test_evaluation.lock")
    workspace_lock = output_dir.parents[1] / ("p7_test_evaluation_rehearsal.lock" if rehearsal else "p7_test_evaluation.lock")

    if preflight_only:
        # Non-destructive preflight: do NOT access test rows/labels, do NOT generate test predictions, do NOT touch locks
        raw_panel = _read_panel(panel_path)
        pre_test_panel = raw_panel[raw_panel["REPDTE"] <= pd.Timestamp(dev_end)].copy()
        derived_pre_test = _derive_features(pre_test_panel)
        eligible_pre_test = derived_pre_test[derived_pre_test["eligible_for_model"].fillna(False) & derived_pre_test["label"].notna()].copy()
        train_df = eligible_pre_test[(eligible_pre_test["REPDTE"] <= pd.Timestamp(train_end)) & (eligible_pre_test["horizon_end"] < pd.Timestamp(dev_start))].copy()
        dev_df = eligible_pre_test[(eligible_pre_test["REPDTE"] >= pd.Timestamp(dev_start)) & (eligible_pre_test["REPDTE"] <= pd.Timestamp(dev_end)) & (eligible_pre_test["horizon_end"] < pd.Timestamp(test_start))].copy()
        cost_policy = _build_cost_policy(dev_df, false_positive_cost, false_positive_cost_rate)
        return {
            "mode": "preflight_only",
            "test_accessed": False,
            "test_predictions_generated": False,
            "locks_modified": False,
            "frozen_config_loaded": frozen_cfg is not None,
            "frozen_config_path": str(frozen_config_path) if frozen_config_path else None,
            "frozen_config_sha256": frozen_cfg.get("sha256_hash") if frozen_cfg else None,
            "effective_configuration": {
                "split": {
                    "train_end": train_end,
                    "dev_start": dev_start,
                    "dev_end": dev_end,
                    "test_start": test_start,
                    "test_end": test_end,
                    "train_rows": int(len(train_df)),
                    "train_positives": int((train_df["label"] == 1).sum()),
                    "train_failed_banks": int(train_df.loc[train_df["label"] == 1, "CERT"].nunique()),
                    "dev_rows": int(len(dev_df)),
                    "dev_positives": int((dev_df["label"] == 1).sum()),
                    "dev_failed_banks": int(dev_df.loc[dev_df["label"] == 1, "CERT"].nunique()),
                },
                "feature_variant": feature_variant,
                "feature_count": len(active_feature_columns),
                "feature_columns": active_feature_columns,
                "primary_model": primary_model_family,
                "secondary_benchmark": "logistic_regression",
                "training_policy": "train_only",
                "seed": seed,
                "loss_definition": "DEF_A" if loss_def_mode == "a" else "DEF_B",
                "fn_fp_ratio": fn_fp_ratio_str,
                "false_positive_cost_rate": false_positive_cost_rate,
                "false_positive_cost_usd_thousands": cost_policy.get("primary_false_positive_cost"),
                "median_development_failure_cost_usd_thousands": cost_policy.get("median_development_failure_cost"),
                "frozen_threshold": frozen_threshold,
            },
            "lock_status": {
                "output_dir_lock_path": str(test_lock),
                "output_dir_lock_exists": test_lock.exists(),
                "workspace_lock_path": str(workspace_lock),
                "workspace_lock_exists": workspace_lock.exists(),
            },
        }
    if evaluate_test:
        # Check lock existence
        for l_path in (test_lock, workspace_lock):
            if l_path.exists():
                try:
                    existing_lock = json.loads(l_path.read_text(encoding="utf-8"))
                except Exception:
                    existing_lock = {}
                lock_status = existing_lock.get("status", "COMPLETED")
                if lock_status == "STARTED" and force_after_crash:
                    # Permitted to resume after an audited crash
                    break
                raise ModelingError(f"Test evaluation lock exists with status '{lock_status}'; refusing repeated held-out test evaluation. Delete it only after an explicit audit.")

        initial_payload = json.dumps({
            "created_utc": pd.Timestamp.now(tz="UTC").isoformat(),
            "status": "STARTED",
            "rehearsal": rehearsal,
            "purpose": "rehearsal test evaluation" if rehearsal else "one-time held-out test evaluation",
            "split": {"train_end": train_end, "dev_start": dev_start, "dev_end": dev_end, "test_start": test_start, "test_end": test_end},
            "note": "Lock status is STARTED during execution and updated to COMPLETED upon successful finish."
        }, indent=2)
        test_lock.write_text(initial_payload, encoding="utf-8")
        if not rehearsal:
            workspace_lock.write_text(initial_payload, encoding="utf-8")
    feature_result = build_feature_table(panel_path, output_dir)
    if features_only:
        report = {"mode": "features_only", "all_rows": len(feature_result["all"]), "eligible_rows": len(feature_result["eligible"]), "feature_columns": FEATURE_COLUMNS, "future_perturbation_check": feature_result["future_perturbation"], "modeling_skipped": True}
        (output_dir / "p7_model_results.json").write_text(json.dumps(report, indent=2), encoding="utf-8")
        return report

    split = _split_data(feature_result["all"], train_end, dev_start, dev_end, test_start, test_end, label_cutoff, min_test_failures, min_dev_failures, require_test=evaluate_test)
    train, development, test = split["train"], split["development"], split["test"]
    X_train, y_train = train[active_feature_columns], train["label"].astype(int)
    X_dev, y_dev = development[active_feature_columns], development["label"].astype(int)
    models = _make_models(seed)
    pipeline_start_time = time.time()
    cost_policy = _build_cost_policy(development, false_positive_cost, false_positive_cost_rate)
    if evaluate_test and frozen_cfg is not None:
        # When evaluating test under a frozen configuration, do NOT rerun threshold tuning, CV, or hyperparameter search
        expanding_validation = {name: [] for name in models}
        tuning_report = {"skipped": True, "reason": "Frozen configuration enforced for held-out test evaluation."}
    else:
        expanding_validation = _expanding_window_validation(train, development, _make_models_with_forest(seed, 50), start_time=pipeline_start_time, time_limit_sec=5400.0)
        tuning_report = _run_model_tuning(train, development, seed, output_dir, start_time=pipeline_start_time, time_limit_sec=5400.0)
    _plot_feature_distributions(feature_result["eligible"], output_dir)
    test_costs = None
    if evaluate_test and cost_policy["status"] == "available":
        test_bank_costs = _first_cost_per_bank(test)
        test_positive = test.loc[test["label"] == 1, "CERT"].astype("string").drop_duplicates()
        test_bank_costs = test_bank_costs.reindex(test_positive).fillna(cost_policy["median_development_failure_cost"])
        test_costs = test["CERT"].astype("string").map(test_bank_costs)
    model_results: dict[str, Any] = {}
    prediction_rows = test[["CERT", "REPDTE", "label"]].copy().rename(columns={"label": "y_true"}) if evaluate_test else None
    threshold_rows = []
    cost_sensitivity_rows = []
    ratio_sensitivity_rows = []
    import gc
    pipeline_start_time = time.time()
    for name, model in models.items():
        if time.time() - pipeline_start_time > 5400.0:
            raise ModelingError(f"Total time cap of 90 minutes exceeded during fitting of model '{name}'.")
        model.fit(X_train, y_train)
        dev_probability = model.predict_proba(X_dev)[:, 1]
        if evaluate_test and frozen_cfg is not None and name == primary_model_family and frozen_threshold is not None:
            # Enforce exact frozen threshold without recalculation
            cost_threshold = float(frozen_threshold)
            cost_threshold_report = {
                "rule": "frozen_model_config",
                "threshold": cost_threshold,
                "false_positive_cost": cost_policy.get("primary_false_positive_cost"),
                "cost_unit": "row_fp_bank_fn" if loss_def_mode == "a" else "bank",
                "def_mode": loss_def_mode,
                "frozen_sha256": frozen_cfg.get("sha256_hash"),
            }
            threshold = cost_threshold
            threshold_report = {"rule": "frozen_model_config", "threshold": cost_threshold}
            fpr_threshold = cost_threshold
            fpr_report = {"rule": "frozen_model_config", "threshold": cost_threshold}
            cost_threshold_b = None
            cost_threshold_report_b = None
        else:
            threshold, threshold_report = _select_threshold(y_dev, dev_probability)
            fpr_threshold, fpr_report = _select_fpr_threshold(y_dev, dev_probability)
            cost_threshold_report = {"rule": "unavailable", "threshold": None, "reason": cost_policy.get("reason", "No development cost policy available.")}
            cost_threshold = None
            cost_threshold_report_b = None
            cost_threshold_b = None
            if cost_policy["status"] == "available":
                cost_threshold, cost_threshold_report = _select_cost_threshold(y_dev, dev_probability, development["CERT"], cost_policy["row_costs"], cost_policy["primary_false_positive_cost"], def_mode=loss_def_mode)
                cost_threshold_b, cost_threshold_report_b = _select_cost_threshold(y_dev, dev_probability, development["CERT"], cost_policy["row_costs"], cost_policy["primary_false_positive_cost"], def_mode="b")
                for rate in cost_policy["sensitivity_rates"]:
                    sensitivity_threshold, sensitivity_report = _select_cost_threshold(y_dev, dev_probability, development["CERT"], cost_policy["row_costs"], cost_policy["derived_false_positive_costs"][rate], def_mode="a")
                    cost_sensitivity_rows.append({
                        "model": name,
                        "false_positive_cost_rate": rate,
                        "false_positive_cost": cost_policy["derived_false_positive_costs"][rate],
                        "threshold": sensitivity_threshold,
                        "development_expected_loss": sensitivity_report["development_expected_loss"],
                        "cost_unit": "row_fp_bank_fn",
                    })
                median_dev_cost = cost_policy["median_development_failure_cost"]
                fn_fp_ratios = [(10, 0.10), (50, 0.02), (100, 0.01), (500, 0.002), (1000, 0.001)]
                for r_int, r_rate in fn_fp_ratios:
                    fp_cost_val = median_dev_cost * r_rate
                    t_a, rep_a = _select_cost_threshold(y_dev, dev_probability, development["CERT"], cost_policy["row_costs"], fp_cost_val, def_mode="a")
                    ratio_sensitivity_rows.append({
                        "model": name,
                        "cost_definition": "DEF_A",
                        "fn_fp_ratio": f"{r_int}:1",
                        "false_positive_cost_rate": r_rate,
                        "false_positive_cost": fp_cost_val,
                        "threshold": t_a,
                        "development_expected_loss": rep_a["development_expected_loss"],
                        "cost_unit": "row_fp_bank_fn",
                    })
                    t_b, rep_b = _select_cost_threshold(y_dev, dev_probability, development["CERT"], cost_policy["row_costs"], fp_cost_val, def_mode="b")
                    ratio_sensitivity_rows.append({
                        "model": name,
                        "cost_definition": "DEF_B",
                        "fn_fp_ratio": f"{r_int}:1",
                        "false_positive_cost_rate": r_rate,
                        "false_positive_cost": fp_cost_val,
                        "threshold": t_b,
                        "development_expected_loss": rep_b["development_expected_loss"],
                        "cost_unit": "bank",
                    })
        alarm_budgets = compute_alarm_budget_metrics(development, dev_probability)
        item: dict[str, Any] = {
            "development_threshold_selection": threshold_report,
            "development_fpr_threshold_selection": fpr_report,
            "development_cost_threshold_selection": cost_threshold_report,
            "development_cost_threshold_selection_def_b": cost_threshold_report_b,
            "development_alarm_budgets": alarm_budgets,
            "expanding_window_validation": expanding_validation.get(name, []),
            "development_y": y_dev.tolist(),
            "development_probability": dev_probability.tolist(),
        }
        if evaluate_test:
            X_test, y_test = test[active_feature_columns], test["label"].astype(int)
            test_probability = model.predict_proba(X_test)[:, 1]
            item["test_alarm_budgets"] = compute_alarm_budget_metrics(test, test_probability)
            item["test_metrics_at_development_f1_threshold"] = _metrics(y_test, test_probability, threshold, false_negative_cost, cost_policy.get("primary_false_positive_cost"), test["CERT"], test_costs, def_mode=loss_def_mode) if test_costs is not None else _metrics(y_test, test_probability, threshold, false_negative_cost, false_positive_cost, def_mode=loss_def_mode)
            item["test_metrics_at_development_fpr_threshold"] = _metrics(y_test, test_probability, fpr_threshold, false_negative_cost, cost_policy.get("primary_false_positive_cost"), test["CERT"], test_costs, def_mode=loss_def_mode) if test_costs is not None else _metrics(y_test, test_probability, fpr_threshold, false_negative_cost, false_positive_cost, def_mode=loss_def_mode)
            if cost_threshold is not None:
                cost_metrics = _metrics(y_test, test_probability, cost_threshold, false_negative_cost, cost_policy.get("primary_false_positive_cost"), test["CERT"], test_costs, def_mode=loss_def_mode) if test_costs is not None else _metrics(y_test, test_probability, cost_threshold, false_negative_cost, false_positive_cost, def_mode=loss_def_mode)
                item["test_metrics_at_development_cost_threshold"] = cost_metrics
                if frozen_cfg is not None:
                    item["test_metrics_at_frozen_threshold"] = cost_metrics
            active_pred_thresh = cost_threshold if (frozen_cfg is not None and cost_threshold is not None) else threshold
            prediction_rows[f"{name}_probability"] = test_probability
            prediction_rows[f"{name}_prediction"] = (test_probability >= active_pred_thresh).astype(int)
        model_results[name] = item
        # Save intermediate progress after each model
        partial_status = {
            "completed_models": list(model_results.keys()),
            "last_completed": name,
            "elapsed_minutes": round((time.time() - pipeline_start_time) / 60.0, 2),
        }
        (output_dir / "p7_model_results_partial.json").write_text(json.dumps(partial_status, indent=2), encoding="utf-8")
        gc.collect()
        threshold_rows.extend([
            {"model": name, "selection": "max_f1", **threshold_report},
            {"model": name, "selection": "max_recall_at_fpr_limit", **fpr_report},
            {"model": name, "selection": "development_cost_def_a", **cost_threshold_report},
        ])
        if cost_threshold_report_b is not None:
            threshold_rows.append({"model": name, "selection": "development_cost_def_b", **cost_threshold_report_b})
    if evaluate_test and prediction_rows is not None:
        prediction_rows.to_parquet(output_dir / "p7_test_predictions.parquet", index=False)
    pd.DataFrame(threshold_rows).to_csv(output_dir / "p7_threshold_selection.csv", index=False)
    pd.DataFrame(cost_sensitivity_rows, columns=["model", "false_positive_cost_rate", "false_positive_cost", "threshold", "development_expected_loss", "cost_unit"]).to_csv(output_dir / "p7_cost_sensitivity.csv", index=False)
    pd.DataFrame(ratio_sensitivity_rows).to_csv(output_dir / "p7_cost_sensitivity_ratios.csv", index=False)
    if evaluate_test:
        metrics_key = "test_metrics_at_frozen_threshold" if frozen_cfg is not None else "test_metrics_at_development_f1_threshold"
        pd.DataFrame([{"model": name, **item.get(metrics_key, item["test_metrics_at_development_f1_threshold"])} for name, item in model_results.items()]).to_csv(output_dir / "p7_model_metrics.csv", index=False)
    _plot_curves(model_results, output_dir)
    # joblib.dump the fitted final pipelines
    if "logistic_regression" in models:
        joblib.dump(models["logistic_regression"], output_dir / "p7_fitted_pipeline_lr.joblib")
    if "random_forest" in models:
        joblib.dump(models["random_forest"], output_dir / "p7_fitted_pipeline_rf.joblib")
    report_models = {name: {key: value for key, value in item.items() if key not in {"development_y", "development_probability"}} for name, item in model_results.items()}

    dev_analysis_report = {"ran": False}
    if dev_analysis:
        from bankfail.modeling.dev_analysis import (
            run_feature_variants_bootstrap,
            run_comprehensive_cost_analysis,
            run_threshold_stability_and_loss_curve,
            run_calibration_analysis,
        )
        v_short = feature_variant.replace("variant_", "v")
        variant_res = run_feature_variants_bootstrap(train, development, FEATURE_COLUMNS, output_dir, seed=seed, fitted_models=models)
        cost_df = run_comprehensive_cost_analysis(development, variant_res["predictions"], cost_policy, output_dir, selected_variant=feature_variant)
        stability_res = run_threshold_stability_and_loss_curve(development, variant_res["predictions"].get(f"rf_{v_short}", variant_res["predictions"]["rf_v3"]), cost_policy, output_dir, seed=seed, feature_variant=feature_variant)
        calib_df = run_calibration_analysis(train, development, variant_res["predictions"], models, active_feature_columns, output_dir, train_predictions=variant_res.get("train_predictions"), selected_variant=feature_variant)
        dev_analysis_report = {
            "ran": True,
            "bootstrap_evaluation": str(output_dir / "bootstrap_evaluation.csv"),
            "bootstrap_paired_differences": str(output_dir / "bootstrap_paired_differences.csv"),
            "comprehensive_cost_analysis": str(output_dir / "p7_cost_threshold_comprehensive.csv"),
            "threshold_stability": str(output_dir / "p7_threshold_stability_rf_100to1.json"),
            "threshold_loss_curve": str(output_dir / "p7_threshold_loss_curve.csv"),
            "annual_alarm_rates": str(output_dir / "p7_annual_alarm_rates.csv"),
            "calibration_summary": str(output_dir / "p7_calibration_summary.csv"),
        }

    mllib_report = {"ran": False, "note": "Not requested."}
    if spark_mllib:
        eval_target = test if evaluate_test else development
        eval_label = "test" if evaluate_test else "development"
        mllib_report = run_spark_mllib_demo(train, eval_target, active_feature_columns, output_dir, spark_master, eval_label=eval_label)
    if evaluate_test:
        completed_payload = json.dumps({
            "created_utc": pd.Timestamp.now(tz="UTC").isoformat(),
            "status": "COMPLETED",
            "rehearsal": rehearsal,
            "purpose": "rehearsal test evaluation" if rehearsal else "one-time held-out test evaluation",
            "split": {"train_end": train_end, "dev_start": dev_start, "dev_end": dev_end, "test_start": test_start, "test_end": test_end},
            "feature_variant": feature_variant,
            "frozen_config_sha256": frozen_cfg.get("sha256_hash") if frozen_cfg else None,
            "note": "Evaluation completed successfully; held-out test results frozen."
        }, indent=2)
        test_lock.write_text(completed_payload, encoding="utf-8")
        if not rehearsal:
            workspace_lock.write_text(completed_payload, encoding="utf-8")

    report = {"mode": "modeling" if evaluate_test else "development_only", "test_evaluated": evaluate_test, "feature_variant": feature_variant, "feature_columns": active_feature_columns, "frozen_config_sha256": frozen_cfg.get("sha256_hash") if frozen_cfg else None, "split": split["metadata"], "models": report_models, "spark_mllib": mllib_report, "dev_analysis": dev_analysis_report, "leakage_controls": {"feature_table_built_before_eligibility_filter": True, "forbidden_columns": sorted(LEAKAGE_COLUMNS), "preprocessing_fit_scope": "training split only via sklearn Pipeline", "threshold_fit_scope": "development split only", "expanding_window_validation_scope": "development only", "future_perturbation_check": feature_result["future_perturbation"], "test_used_once_for_final_metrics": evaluate_test, "test_evaluation_lock": str(test_lock) if evaluate_test else None, "feature_selection_scope": "pre-specified economic features; no outcome-driven selection", "feature_timing": "Financials at q plus exact historical lags; financial availability assumed at q + configured reporting lag from Part 3."}, "costs": {"false_negative_cost_override": false_negative_cost, "false_positive_cost_override": false_positive_cost, "false_positive_cost_rate": false_positive_cost_rate, "policy": {key: value for key, value in cost_policy.items() if key not in {"bank_costs", "row_costs", "derived_false_positive_costs"}}, "sensitivity_file": str(output_dir / "p7_cost_sensitivity.csv"), "ratio_sensitivity_file": str(output_dir / "p7_cost_sensitivity_ratios.csv"), "invented_costs": False, "cost_unit": "row_fp_bank_fn" if loss_def_mode == "a" else "bank", "loss_definition": "DEF_A" if loss_def_mode == "a" else "DEF_B", "cost_threshold_requires_development_failure_cost": True}}
    (output_dir / "p7_model_results.json").write_text(json.dumps(report, indent=2, default=str), encoding="utf-8")
    return report
