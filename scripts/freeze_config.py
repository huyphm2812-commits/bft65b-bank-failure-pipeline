#!/usr/bin/env python3
"""Hardened freeze configuration script for BFT65B.

Requires explicit supervisory arguments for all open decisions without default fallbacks.
Verifies inputs against development results, stores file and dataset SHA-256 digests,
and supports --dry-run validation mode.
"""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
import sys
import pandas as pd


def compute_file_sha256(file_path: Path) -> str:
    """Compute SHA-256 hash of a file on disk."""
    if not file_path.exists():
        return "file_not_found"
    h = hashlib.sha256()
    with open(file_path, "rb") as f:
        while chunk := f.read(65536):
            h.update(chunk)
    return h.hexdigest()


def verify_and_build_frozen_config(
    repo_root: Path,
    split_train_end: str,
    split_dev_start: str,
    split_dev_end: str,
    split_test_start: str,
    split_test_end: str,
    loss_definition: str,
    fn_fp_ratio: str,
    feature_variant: str,
    final_model: str,
    training_policy: str,
    seed: int,
    threshold: float,
    dev_results_dir: Path | None = None,
) -> dict:
    """Verify parameters against dev outputs and assemble frozen config payload."""
    if training_policy != "train_only":
        raise ValueError(f"Unsupported training policy '{training_policy}'. Must be 'train_only'.")

    loss_def = loss_definition.upper()
    if loss_def not in ("DEF_A", "DEF_B"):
        raise ValueError(f"Invalid loss definition '{loss_definition}'. Must be 'DEF_A' or 'DEF_B'.")

    # Locate dev results directory
    if dev_results_dir is None:
        candidates = [
            repo_root.parent / "02_local_workspace" / "outputs" / "07_features_models",
            repo_root / "results_snapshot" / "07_features_models",
            repo_root.parent / "03_report_inputs" / "07_features_models",
        ]
        for c in candidates:
            if (c / "p7_model_results.json").exists():
                dev_results_dir = c
                break

    if dev_results_dir is None or not (dev_results_dir / "p7_model_results.json").exists():
        raise FileNotFoundError(f"Could not locate development results (p7_model_results.json). Looked in {candidates}")

    # 1. Verify split dates
    with open(dev_results_dir / "p7_model_results.json", encoding="utf-8") as f:
        dev_res = json.load(f)

    split_meta = dev_res.get("split", {})
    if split_meta:
        if split_meta.get("train_end") != split_train_end:
            raise ValueError(f"split_train_end '{split_train_end}' does not match dev result '{split_meta.get('train_end')}'")
        if split_meta.get("dev_start") != split_dev_start:
            raise ValueError(f"split_dev_start '{split_dev_start}' does not match dev result '{split_meta.get('dev_start')}'")
        if split_meta.get("dev_end") != split_dev_end:
            raise ValueError(f"split_dev_end '{split_dev_end}' does not match dev result '{split_meta.get('dev_end')}'")

    # 2. Verify threshold against dev results
    cost_csv_candidates = [
        dev_results_dir / "p7_cost_threshold_comprehensive.csv",
        dev_results_dir / "p7_cost_sensitivity_ratios.csv",
    ]
    matched_row = None
    for c_path in cost_csv_candidates:
        if c_path.exists():
            df_cost = pd.read_csv(c_path)
            # Filter by model, cost_definition, fn_fp_ratio, and feature_variant (if present)
            sub = df_cost[(df_cost["model"] == final_model) & 
                          (df_cost["cost_definition"] == loss_def) & 
                          (df_cost["fn_fp_ratio"] == fn_fp_ratio)]
            if "feature_variant" in sub.columns:
                sub_var = sub[sub["feature_variant"].isin([feature_variant, "all"])]
                if not sub_var.empty:
                    sub = sub_var
            if not sub.empty:
                matched_row = sub.iloc[0]
                break

    if matched_row is not None:
        expected_t = float(matched_row["threshold"])
        if abs(threshold - expected_t) > 1e-3:
            raise ValueError(
                f"Specified threshold {threshold} does not match development result {expected_t} "
                f"for model '{final_model}', variant '{feature_variant}', definition '{loss_def}', ratio '{fn_fp_ratio}'."
            )
    else:
        # Fallback to checking threshold selection in p7_model_results.json
        model_info = dev_res.get("models", {}).get(final_model, {})
        def_key = "development_cost_threshold_selection" if loss_def == "DEF_A" else "development_cost_threshold_selection_def_b"
        expected_t = model_info.get(def_key, {}).get("threshold")
        if expected_t is not None and abs(threshold - float(expected_t)) > 1e-3:
            raise ValueError(f"Specified threshold {threshold} does not match {def_key} threshold {expected_t}")

    # 3. Compute code and dataset hashes
    code_hashes = {
        "pipeline_py": compute_file_sha256(repo_root / "src" / "bankfail" / "modeling" / "pipeline.py"),
        "dev_analysis_py": compute_file_sha256(repo_root / "src" / "bankfail" / "modeling" / "dev_analysis.py"),
        "builder_py": compute_file_sha256(repo_root / "src" / "bankfail" / "panel" / "builder.py"),
    }
    panel_path = repo_root.parent / "02_local_workspace" / "outputs" / "03_panel" / "bank_quarter_panel.parquet"
    panel_hash = compute_file_sha256(panel_path) if panel_path.exists() else "not_found"

    # Assemble payload
    payload = {
        "status": "FROZEN",
        "purpose": "Final frozen configuration for held-out test evaluation",
        "frozen_timestamp_utc": pd.Timestamp.now(tz="UTC").isoformat(),
        "split": {
            "train_end": split_train_end,
            "dev_start": split_dev_start,
            "dev_end": split_dev_end,
            "test_start": split_test_start,
            "test_end": split_test_end,
        },
        "feature_selection": {
            "variant": feature_variant,
            "rule": "simplest variant whose development PR-AUC paired difference against best has 95% bootstrap CI including zero",
        },
        "primary_model": {
            "model_family": final_model,
            "seed": seed,
            "training_policy": training_policy,
            "hyperparameters": {
                "n_estimators": 100,
                "min_samples_leaf": 5,
                "class_weight": "balanced_subsample",
                "max_samples": None,
                "n_jobs": 2,
            } if final_model == "random_forest" else {
                "scaler": "StandardScaler",
                "imputer": "SimpleImputer(strategy='median', add_indicator=True)",
                "C": 1.0,
                "class_weight": "balanced",
                "max_iter": 2000,
            },
        },
        "decision_threshold": {
            "loss_definition": loss_def,
            "fn_fp_ratio": fn_fp_ratio,
            "chosen_threshold": float(threshold),
            "expected_loss_development": float(matched_row["total_cost"]) if matched_row is not None and "total_cost" in matched_row else None,
            "banks_caught_development": str(matched_row["banks_caught"]) if matched_row is not None and "banks_caught" in matched_row else None,
        },
        "reproducibility_hashes": {
            "code_files": code_hashes,
            "panel_parquet": panel_hash,
        },
    }

    # Configuration SHA-256
    encoded = json.dumps(payload, sort_keys=True, indent=2).encode("utf-8")
    payload["sha256_hash"] = hashlib.sha256(encoded).hexdigest()
    return payload


def main() -> int:
    parser = argparse.ArgumentParser(description="Hardened freeze configuration CLI for BFT65B.")
    # Explicit required arguments without defaults
    parser.add_argument("--split-train-end", required=True, help="Train end date (YYYY-MM-DD)")
    parser.add_argument("--split-dev-start", required=True, help="Dev start date (YYYY-MM-DD)")
    parser.add_argument("--split-dev-end", required=True, help="Dev end date (YYYY-MM-DD)")
    parser.add_argument("--split-test-start", required=True, help="Test start date (YYYY-MM-DD)")
    parser.add_argument("--split-test-end", required=True, help="Test end date (YYYY-MM-DD)")
    parser.add_argument("--loss-definition", required=True, choices=["DEF_A", "DEF_B", "def_a", "def_b"], help="Loss definition")
    parser.add_argument("--fn-fp-ratio", required=True, help="False-negative to false-positive cost ratio (e.g., 100:1)")
    parser.add_argument("--feature-variant", required=True, help="Feature variant (e.g., variant_1, variant_2, variant_3)")
    parser.add_argument("--final-model", required=True, choices=["random_forest", "logistic_regression"], help="Primary model family")
    parser.add_argument("--training-policy", required=True, help="Training policy (e.g., train_only)")
    parser.add_argument("--seed", type=int, required=True, help="Deterministic random seed")
    parser.add_argument("--threshold", type=float, required=True, help="Development-computed threshold value")
    parser.add_argument("--output", "-o", type=Path, default=Path("frozen_model_config.json"), help="Output path")
    parser.add_argument("--dev-results-dir", type=Path, default=None, help="Directory containing dev results")
    parser.add_argument("--dry-run", action="store_true", help="Validate and print config without writing to disk")

    args = parser.parse_args()
    repo_root = Path(__file__).resolve().parents[1]

    try:
        config = verify_and_build_frozen_config(
            repo_root=repo_root,
            split_train_end=args.split_train_end,
            split_dev_start=args.split_dev_start,
            split_dev_end=args.split_dev_end,
            split_test_start=args.split_test_start,
            split_test_end=args.split_test_end,
            loss_definition=args.loss_definition,
            fn_fp_ratio=args.fn_fp_ratio,
            feature_variant=args.feature_variant,
            final_model=args.final_model,
            training_policy=args.training_policy,
            seed=args.seed,
            threshold=args.threshold,
            dev_results_dir=args.dev_results_dir,
        )
    except Exception as e:
        print(f"ERROR: Configuration validation failed: {e}", file=sys.stderr)
        return 1

    print("==================================================")
    print("CONFIGURATION VALIDATION PASSED")
    print(f"Status: {config['status']}")
    print(f"Primary Model: {config['primary_model']['model_family']}")
    print(f"Threshold: {config['decision_threshold']['chosen_threshold']}")
    print(f"Loss Definition: {config['decision_threshold']['loss_definition']}")
    print(f"Ratio: {config['decision_threshold']['fn_fp_ratio']}")
    print(f"SHA-256 Digest: {config['sha256_hash']}")
    print("==================================================")

    if args.dry_run:
        print("DRY RUN: Validation passed successfully. Zero files written to disk.")
        return 0

    args.output.parent.mkdir(parents=True, exist_ok=True)
    with open(args.output, "w", encoding="utf-8") as f:
        json.dump(config, f, indent=2)
    print(f"Configuration frozen and written to: {args.output.resolve()}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
