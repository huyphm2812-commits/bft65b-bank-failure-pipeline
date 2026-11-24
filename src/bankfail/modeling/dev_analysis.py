"""Comprehensive development analysis module for BFT65B.

This module provides:
1. Bank-level cluster bootstrap (1,000 resamples, seed 42) for feature variants and models.
2. Paired difference confidence intervals across variants (V1-V2, V1-V3, V2-V3) and models (RF-LR).
3. Comprehensive cost threshold optimization across 10:1 to 1000:1 ratios under DEF_A and DEF_B.
4. Threshold stability analysis (1,000 bootstrap resamples re-optimizing threshold) and loss curves.
5. Brute-force verification of global minimum and annual alarm rate breakdowns.
6. Pre-test calibration analysis across train and development partitions.
"""

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import joblib
from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import RandomForestClassifier
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler
from sklearn.impute import SimpleImputer
from sklearn.metrics import roc_auc_score, average_precision_score, brier_score_loss


def run_feature_variants_bootstrap(
    train: pd.DataFrame,
    development: pd.DataFrame,
    feature_columns: list[str],
    output_dir: Path,
    seed: int = 42,
    n_bootstraps: int = 1000,
    fitted_models: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Train 3 feature variants and run 1,000 bank-level cluster bootstrap iterations."""
    v1_cols = feature_columns
    v2_cols = [c for c in feature_columns if c != "rbcrwa_level"]
    v3_cols = [c for c in feature_columns if c not in ["rbcrwa_level", "rbc1a_level"]]

    y_train = train["label"].astype(int)
    dev_df = development[["CERT", "REPDTE", "label"]].copy().reset_index(drop=True)
    y_dev_arr = dev_df["label"].to_numpy().astype(int)
    repdte_arr = dev_df["REPDTE"].to_numpy()
    unique_certs = dev_df["CERT"].unique()
    n_certs = len(unique_certs)
    cert_groups = dev_df.groupby("CERT").indices

    predictions: dict[str, np.ndarray] = {}
    train_predictions: dict[str, np.ndarray] = {}

    # Variant 1
    if fitted_models and "logistic_regression" in fitted_models and "random_forest" in fitted_models and list(getattr(fitted_models["random_forest"], "feature_names_in_", v1_cols)) == list(v1_cols):
        predictions["lr_v1"] = fitted_models["logistic_regression"].predict_proba(development[v1_cols])[:, 1]
        predictions["rf_v1"] = fitted_models["random_forest"].predict_proba(development[v1_cols])[:, 1]
        train_predictions["lr_v1"] = fitted_models["logistic_regression"].predict_proba(train[v1_cols])[:, 1]
        train_predictions["rf_v1"] = fitted_models["random_forest"].predict_proba(train[v1_cols])[:, 1]
    else:
        pipe_lr_v1 = Pipeline([
            ("imputer", SimpleImputer(strategy="median", add_indicator=True, keep_empty_features=True)),
            ("scaler", StandardScaler()),
            ("model", LogisticRegression(max_iter=2000, class_weight="balanced", random_state=seed)),
        ])
        pipe_lr_v1.fit(train[v1_cols], y_train)
        predictions["lr_v1"] = pipe_lr_v1.predict_proba(development[v1_cols])[:, 1]
        train_predictions["lr_v1"] = pipe_lr_v1.predict_proba(train[v1_cols])[:, 1]

        pipe_rf_v1 = Pipeline([
            ("imputer", SimpleImputer(strategy="median", add_indicator=True, keep_empty_features=True)),
            ("model", RandomForestClassifier(n_estimators=100, min_samples_leaf=5, class_weight="balanced_subsample", random_state=seed, n_jobs=1)),
        ])
        pipe_rf_v1.fit(train[v1_cols], y_train)
        predictions["rf_v1"] = pipe_rf_v1.predict_proba(development[v1_cols])[:, 1]
        train_predictions["rf_v1"] = pipe_rf_v1.predict_proba(train[v1_cols])[:, 1]

    # Variants 2 and 3
    for v_name, cols in [("v2", v2_cols), ("v3", v3_cols)]:
        if fitted_models and "logistic_regression" in fitted_models and "random_forest" in fitted_models and list(getattr(fitted_models["random_forest"], "feature_names_in_", [])) == list(cols):
            predictions[f"lr_{v_name}"] = fitted_models["logistic_regression"].predict_proba(development[cols])[:, 1]
            predictions[f"rf_{v_name}"] = fitted_models["random_forest"].predict_proba(development[cols])[:, 1]
            train_predictions[f"lr_{v_name}"] = fitted_models["logistic_regression"].predict_proba(train[cols])[:, 1]
            train_predictions[f"rf_{v_name}"] = fitted_models["random_forest"].predict_proba(train[cols])[:, 1]
        else:
            pipe_lr = Pipeline([
                ("imputer", SimpleImputer(strategy="median", add_indicator=True, keep_empty_features=True)),
                ("scaler", StandardScaler()),
                ("model", LogisticRegression(max_iter=2000, class_weight="balanced", random_state=seed)),
            ])
            pipe_lr.fit(train[cols], y_train)
            predictions[f"lr_{v_name}"] = pipe_lr.predict_proba(development[cols])[:, 1]
            train_predictions[f"lr_{v_name}"] = pipe_lr.predict_proba(train[cols])[:, 1]

            pipe_rf = Pipeline([
                ("imputer", SimpleImputer(strategy="median", add_indicator=True, keep_empty_features=True)),
                ("model", RandomForestClassifier(n_estimators=100, min_samples_leaf=5, class_weight="balanced_subsample", random_state=seed, n_jobs=1)),
            ])
            pipe_rf.fit(train[cols], y_train)
            predictions[f"rf_{v_name}"] = pipe_rf.predict_proba(development[cols])[:, 1]
            train_predictions[f"rf_{v_name}"] = pipe_rf.predict_proba(train[cols])[:, 1]

    # Baselines
    prior_prob = y_train.mean()
    predictions["prior"] = np.full(len(development), prior_prob)
    cap_ratio = development["capital_ratio"].fillna(development["capital_ratio"].median()).to_numpy()
    predictions["rule_cap_ratio"] = -cap_ratio

    # Bootstrap Evaluation
    np.random.seed(seed)
    models_to_eval = ["lr_v1", "rf_v1", "lr_v2", "rf_v2", "lr_v3", "rf_v3"]
    boot_results: dict[str, dict[str, list[float]]] = {m: {"roc": [], "pr": [], "r1": [], "r2": [], "r5": []} for m in models_to_eval}

    for _ in range(n_bootstraps):
        sampled_c = np.random.choice(unique_certs, size=n_certs, replace=True)
        idx_list = [cert_groups[c] for c in sampled_c]
        boot_idx = np.concatenate(idx_list)

        b_y = y_dev_arr[boot_idx]
        b_repdte = repdte_arr[boot_idx]

        if np.sum(b_y) == 0 or len(np.unique(b_y)) < 2:
            continue

        for m in models_to_eval:
            b_p = predictions[m][boot_idx]
            roc, pr, r1, r2, r5 = _compute_metrics_on_sample(b_y, b_repdte, b_p)
            boot_results[m]["roc"].append(roc)
            boot_results[m]["pr"].append(pr)
            boot_results[m]["r1"].append(r1)
            boot_results[m]["r2"].append(r2)
            boot_results[m]["r5"].append(r5)

    bootstrap_rows = []
    for m in models_to_eval:
        row = {"model_variant": m}
        for metric in ["pr", "roc", "r1", "r2", "r5"]:
            arr = np.array(boot_results[m][metric])
            row[f"{metric}_mean"] = round(float(np.mean(arr)), 4)
            row[f"{metric}_ci_lower"] = round(float(np.percentile(arr, 2.5)), 4)
            row[f"{metric}_ci_upper"] = round(float(np.percentile(arr, 97.5)), 4)
        bootstrap_rows.append(row)

    df_boot = pd.DataFrame(bootstrap_rows)
    df_boot.to_csv(output_dir / "bootstrap_evaluation.csv", index=False)

    # Paired differences
    diff_comparisons = [
        ("rf_v1 vs lr_v1", "rf_v1", "lr_v1"),
        ("rf_v2 vs lr_v2", "rf_v2", "lr_v2"),
        ("rf_v3 vs lr_v3", "rf_v3", "lr_v3"),
        ("lr_v1 vs lr_v2", "lr_v1", "lr_v2"),
        ("lr_v1 vs lr_v3", "lr_v1", "lr_v3"),
        ("lr_v2 vs lr_v3", "lr_v2", "lr_v3"),
        ("rf_v1 vs rf_v2", "rf_v1", "rf_v2"),
        ("rf_v1 vs rf_v3", "rf_v1", "rf_v3"),
        ("rf_v2 vs rf_v3", "rf_v2", "rf_v3"),
    ]

    paired_diffs = []
    for label, m_a, m_b in diff_comparisons:
        diff_row = {"comparison": label}
        for metric in ["pr", "roc", "r1", "r2", "r5"]:
            diffs = np.array(boot_results[m_a][metric]) - np.array(boot_results[m_b][metric])
            low = float(np.percentile(diffs, 2.5))
            high = float(np.percentile(diffs, 97.5))
            mean = float(np.mean(diffs))
            diff_row[f"{metric}_diff_mean"] = round(mean, 4)
            diff_row[f"{metric}_diff_ci_lower"] = round(low, 4)
            diff_row[f"{metric}_diff_ci_upper"] = round(high, 4)
            diff_row[f"{metric}_excludes_zero"] = bool(low > 0 or high < 0)
        paired_diffs.append(diff_row)

    df_diffs = pd.DataFrame(paired_diffs)
    df_diffs.to_csv(output_dir / "bootstrap_paired_differences.csv", index=False)

    return {
        "predictions": predictions,
        "train_predictions": train_predictions,
        "bootstrap_summary": df_boot.to_dict(orient="records"),
        "paired_differences": df_diffs.to_dict(orient="records"),
    }


def _compute_metrics_on_sample(sub_y: np.ndarray, sub_repdte: np.ndarray, sub_prob: np.ndarray):
    """Compute ROC, PR, and alarm recall at 1%, 2%, 5% on a bootstrap sample."""
    if len(np.unique(sub_y)) < 2:
        return 0.5, 0.0, 0.0, 0.0, 0.0
    roc = float(roc_auc_score(sub_y, sub_prob))
    pr = float(average_precision_score(sub_y, sub_prob))

    sub_q_list = np.unique(sub_repdte)
    caught_1 = 0
    caught_2 = 0
    caught_5 = 0
    total_pos = np.sum(sub_y)

    if total_pos == 0:
        return roc, pr, 0.0, 0.0, 0.0

    for q in sub_q_list:
        q_mask = (sub_repdte == q)
        q_y = sub_y[q_mask]
        q_p = sub_prob[q_mask]
        n_q = len(q_y)
        if n_q == 0 or np.sum(q_y) == 0:
            continue
        order = np.argsort(-q_p)
        top1_k = max(1, int(np.ceil(0.01 * n_q)))
        top2_k = max(1, int(np.ceil(0.02 * n_q)))
        top5_k = max(1, int(np.ceil(0.05 * n_q)))

        caught_1 += np.sum(q_y[order[:top1_k]])
        caught_2 += np.sum(q_y[order[:top2_k]])
        caught_5 += np.sum(q_y[order[:top5_k]])

    return roc, pr, float(caught_1 / total_pos), float(caught_2 / total_pos), float(caught_5 / total_pos)


def run_comprehensive_cost_analysis(
    development: pd.DataFrame,
    predictions: dict[str, np.ndarray],
    cost_policy: dict[str, Any],
    output_dir: Path,
    selected_variant: str = "variant_3",
) -> pd.DataFrame:
    """Evaluate DEF_A and DEF_B across 10:1 to 1000:1 ratios with baselines and all feature variants."""
    median_cost = cost_policy["median_development_failure_cost"]
    bank_costs_dict = cost_policy["bank_costs"]

    fn_fp_ratios = [
        (10, 0.10),
        (50, 0.02),
        (100, 0.01),
        (500, 0.002),
        (1000, 0.001),
    ]

    variant_order = ["variant_3", "variant_1", "variant_2"]
    if selected_variant in variant_order:
        variant_order.remove(selected_variant)
        variant_order.insert(0, selected_variant)

    model_entries: list[tuple[str, str, np.ndarray]] = [
        ("prior_baseline", "all", predictions["prior"]),
        ("capital_ratio_rule", "all", predictions["rule_cap_ratio"]),
    ]
    for v_tag in variant_order:
        v_short = v_tag.replace("variant_", "v")
        if f"lr_{v_short}" in predictions:
            model_entries.append(("logistic_regression", v_tag, predictions[f"lr_{v_short}"]))
        if f"rf_{v_short}" in predictions:
            model_entries.append(("random_forest", v_tag, predictions[f"rf_{v_short}"]))

    certs_arr = development["CERT"].to_numpy().astype(str)
    y_dev_arr = development["label"].to_numpy().astype(int)
    quarters = development["REPDTE"].unique()
    n_quarters = len(quarters)

    cost_rows = []
    for m_name, v_tag, m_probs in model_entries:
        for r_int, r_rate in fn_fp_ratios:
            c_fp_val = median_cost * r_rate
            for d_mode in ["DEF_A", "DEF_B"]:
                stats = _eval_cost_threshold(y_dev_arr, m_probs, certs_arr, c_fp_val, "a" if d_mode == "DEF_A" else "b", bank_costs_dict, median_cost, n_quarters)
                cost_rows.append({
                    "model": m_name,
                    "feature_variant": v_tag,
                    "cost_definition": d_mode,
                    "fn_fp_ratio": f"{r_int}:1",
                    "false_positive_cost_rate": r_rate,
                    "false_positive_cost": round(c_fp_val, 4),
                    "threshold": round(stats["threshold"], 6),
                    "alarm_rate_per_quarter": round(stats["alarm_rate_per_quarter"], 1),
                    "alarm_row_share_pct": stats["alarm_row_share_pct"],
                    "banks_caught": f"{stats['banks_caught']}/{stats['total_failed_banks']}",
                    "bank_catch_rate_pct": round(stats["bank_catch_rate"] * 100, 1),
                    "false_alarm_rows": stats["false_alarm_rows"],
                    "false_alarm_banks": stats["false_alarm_banks"],
                    "total_cost": round(stats["total_cost"], 2),
                })

    df_cost = pd.DataFrame(cost_rows)
    df_cost.to_csv(output_dir / "p7_cost_threshold_comprehensive.csv", index=False)
    return df_cost


def _eval_cost_threshold(sub_y, sub_prob, sub_certs, c_fp, def_mode, bank_costs_dict, median_cost, n_quarters):
    pos_mask = (sub_y == 1)
    neg_mask = (sub_y == 0)
    pos_probs = sub_prob[pos_mask]
    cand_thresh = np.unique(np.concatenate([
        np.linspace(sub_prob.min(), sub_prob.max(), 100),
        np.sort(pos_probs),
        np.array([sub_prob.min() - 1e-6, sub_prob.max() + 1e-6])
    ]))

    pos_df = pd.DataFrame({"CERT": sub_certs[pos_mask], "prob": pos_probs})
    pos_max_series = pos_df.groupby("CERT", sort=False)["prob"].max()
    failed_certs = pos_max_series.index.to_numpy()
    n_failed_banks = len(failed_certs)
    if n_failed_banks == 0:
        return {
            "threshold": 0.5,
            "banks_caught": 0,
            "total_failed_banks": 0,
            "bank_catch_rate": 0.0,
            "false_alarm_rows": 0,
            "false_alarm_banks": 0,
            "total_cost": 0.0,
            "alarm_rate_per_quarter": 0.0,
            "alarm_row_share_pct": 0.0,
        }

    pos_max_p = pos_max_series.to_numpy(dtype=float)
    pos_costs = np.array([float(bank_costs_dict.get(c, median_cost)) for c in failed_certs], dtype=float)
    pos_order = np.argsort(pos_max_p)
    sorted_pos_p = pos_max_p[pos_order]
    sorted_pos_c = pos_costs[pos_order]
    cumsum_pos_c = np.r_[0.0, np.cumsum(sorted_pos_c)]

    fn_idx = np.searchsorted(sorted_pos_p, cand_thresh, side="left")
    fn_cost_vec = cumsum_pos_c[fn_idx]

    if def_mode == "a":
        sorted_neg_p = np.sort(sub_prob[neg_mask])
        fp_idx = np.searchsorted(sorted_neg_p, cand_thresh, side="left")
        fa_rows_vec = len(sorted_neg_p) - fp_idx
        fp_cost_vec = fa_rows_vec * c_fp
    else:
        failed_set = set(failed_certs)
        all_df = pd.DataFrame({"CERT": sub_certs, "prob": sub_prob})
        pure_neg_df = all_df[~all_df["CERT"].isin(failed_set)]
        if not pure_neg_df.empty:
            pure_neg_max = np.sort(pure_neg_df.groupby("CERT", sort=False)["prob"].max().to_numpy(dtype=float))
        else:
            pure_neg_max = np.array([], dtype=float)
        fp_idx = np.searchsorted(pure_neg_max, cand_thresh, side="left")
        fa_banks_vec = len(pure_neg_max) - fp_idx
        fp_cost_vec = fa_banks_vec * c_fp

    total_loss_vec = fn_cost_vec + fp_cost_vec
    best_idx = int(np.argmin(total_loss_vec))
    best_t = float(cand_thresh[best_idx])
    best_loss = float(total_loss_vec[best_idx])

    alarm_mask = (sub_prob >= best_t)
    fa_rows = int(np.sum(alarm_mask & neg_mask))
    if def_mode == "a":
        fa_banks = int(len(pd.unique(sub_certs[alarm_mask & neg_mask])))
    else:
        alarmed_certs = pd.unique(sub_certs[alarm_mask])
        failed_set = set(failed_certs)
        fa_banks = int(sum(1 for c in alarmed_certs if c not in failed_set))

    caught_count = int(n_failed_banks - fn_idx[best_idx])
    return {
        "threshold": best_t,
        "banks_caught": caught_count,
        "total_failed_banks": n_failed_banks,
        "bank_catch_rate": caught_count / n_failed_banks,
        "false_alarm_rows": fa_rows,
        "false_alarm_banks": fa_banks,
        "total_cost": best_loss,
        "alarm_rate_per_quarter": float(np.sum(alarm_mask) / n_quarters),
        "alarm_row_share_pct": round(float(np.mean(alarm_mask) * 100), 3),
    }


def run_threshold_stability_and_loss_curve(
    development: pd.DataFrame,
    rf_probabilities: np.ndarray,
    cost_policy: dict[str, Any],
    output_dir: Path,
    seed: int = 42,
    n_resamples: int = 1000,
    feature_variant: str = "variant_3",
) -> dict[str, Any]:
    """Evaluate threshold stability on 1,000 resamples and generate loss-vs-threshold curve."""
    median_cost = cost_policy["median_development_failure_cost"]
    bank_costs_dict = cost_policy["bank_costs"]
    c_fp = median_cost * 0.01  # 100:1 ratio ($59.19225k)

    dev_df = development[["CERT", "REPDTE", "label"]].copy().reset_index(drop=True)
    y_dev_arr = dev_df["label"].to_numpy().astype(int)
    certs_arr = dev_df["CERT"].to_numpy().astype(str)
    quarters = dev_df["REPDTE"].unique()
    n_quarters = len(quarters)

    point_opt = _eval_cost_threshold(y_dev_arr, rf_probabilities, certs_arr, c_fp, "a", bank_costs_dict, median_cost, n_quarters)
    chosen_t = round(float(point_opt["threshold"]), 6)

    unique_certs = dev_df["CERT"].unique()
    n_certs = len(unique_certs)
    cert_groups = dev_df.groupby("CERT").indices

    # 1. Threshold stability bootstrap
    np.random.seed(seed)
    thresh_list = []
    alarms_per_q_list = []
    caught_list = []
    loss_list = []

    for _ in range(n_resamples):
        sampled_c = np.random.choice(unique_certs, size=n_certs, replace=True)
        idx_list = [cert_groups[c] for c in sampled_c]
        b_idx = np.concatenate(idx_list)

        b_y = y_dev_arr[b_idx]
        b_prob = rf_probabilities[b_idx]
        b_certs = certs_arr[b_idx]

        if np.sum(b_y) == 0:
            continue

        res = _eval_cost_threshold(b_y, b_prob, b_certs, c_fp, "a", bank_costs_dict, median_cost, n_quarters)
        thresh_list.append(res["threshold"])
        alarms_per_q_list.append(res["alarm_rate_per_quarter"])
        caught_list.append(res["banks_caught"])
        loss_list.append(res["total_cost"])

    stability_stats = {
        "model": "random_forest",
        "feature_variant": feature_variant,
        "point_estimate_threshold": chosen_t,
        "cost_definition": "DEF_A",
        "ratio": "100:1",
        "c_fp": c_fp,
        "n_resamples": n_resamples,
        "threshold": {
            "median": round(float(np.median(thresh_list)), 6),
            "p5": round(float(np.percentile(thresh_list, 5)), 6),
            "p95": round(float(np.percentile(thresh_list, 95)), 6),
        },
        "alarms_per_quarter": {
            "median": round(float(np.median(alarms_per_q_list)), 1),
            "p5": round(float(np.percentile(alarms_per_q_list, 5)), 1),
            "p95": round(float(np.percentile(alarms_per_q_list, 95)), 1),
        },
        "banks_caught": {
            "median": round(float(np.median(caught_list)), 1),
            "p5": round(float(np.percentile(caught_list, 5)), 1),
            "p95": round(float(np.percentile(caught_list, 95)), 1),
        },
        "total_loss": {
            "median": round(float(np.median(loss_list)), 2),
            "p5": round(float(np.percentile(loss_list, 5)), 2),
            "p95": round(float(np.percentile(loss_list, 95)), 2),
        },
    }
    (output_dir / "p7_threshold_stability_rf_100to1.json").write_text(json.dumps(stability_stats, indent=2), encoding="utf-8")

    # 2. Fine Grid Loss Curve & Brute-Force Verification
    fine_grid = np.linspace(0.0, 1.0, 1001)
    failed_certs = np.unique(certs_arr[y_dev_arr == 1])
    n_failed = len(failed_certs)

    loss_curve_rows = []
    best_loss = float("inf")
    brute_force_best_t = chosen_t

    for t in fine_grid:
        # Brute-force scalar calculation (non-vectorized logic)
        fa_rows = int(np.sum((rf_probabilities >= t) & (y_dev_arr == 0)))
        alarmed_failed = set(certs_arr[(rf_probabilities >= t) & (y_dev_arr == 1)])
        caught = len(alarmed_failed)
        missed = [c for c in failed_certs if c not in alarmed_failed]
        fn_loss = sum(bank_costs_dict.get(c, median_cost) for c in missed)
        fp_loss = fa_rows * c_fp
        tot = fp_loss + fn_loss

        is_chosen = bool(abs(t - chosen_t) < 0.001)
        loss_curve_rows.append({
            "threshold": round(float(t), 4),
            "false_alarm_rows": fa_rows,
            "false_positive_cost": round(fp_loss, 2),
            "banks_caught": caught,
            "banks_missed": len(missed),
            "false_negative_cost": round(fn_loss, 2),
            "total_loss": round(tot, 2),
            "alarms_per_quarter": round(float(np.sum(rf_probabilities >= t) / n_quarters), 1),
            "is_chosen_threshold": is_chosen,
        })
        if tot < best_loss:
            best_loss = tot
            brute_force_best_t = t

    df_curve = pd.DataFrame(loss_curve_rows)
    df_curve.to_csv(output_dir / "p7_threshold_loss_curve.csv", index=False)

    # 3. Annual alarm rates at chosen threshold
    dev_df["year"] = pd.to_datetime(dev_df["REPDTE"]).dt.year
    dev_df["is_alarm"] = (rf_probabilities >= chosen_t)
    years = sorted(dev_df["year"].unique())

    annual_rows = []
    for y in years:
        sub_y = dev_df[dev_df["year"] == y]
        n_obs = len(sub_y)
        n_alarms = int(sub_y["is_alarm"].sum())
        pos_sub = sub_y[sub_y["label"] == 1]
        failed_banks_y = pos_sub["CERT"].nunique()
        caught_banks_y = pos_sub[pos_sub["is_alarm"]]["CERT"].nunique()
        annual_rows.append({
            "year": int(y),
            "total_bank_quarters": n_obs,
            "total_alarms": n_alarms,
            "alarm_rate_pct": round(float(n_alarms / n_obs * 100), 2),
            "active_failed_banks": failed_banks_y,
            "caught_failed_banks": caught_banks_y,
        })

    df_annual = pd.DataFrame(annual_rows)
    df_annual.to_csv(output_dir / "p7_annual_alarm_rates.csv", index=False)

    return {
        "stability": stability_stats,
        "brute_force_best_threshold": float(brute_force_best_t),
        "brute_force_best_loss": float(best_loss),
        "annual_rates": df_annual.to_dict(orient="records"),
    }


def run_calibration_analysis(
    train: pd.DataFrame,
    development: pd.DataFrame,
    predictions: dict[str, np.ndarray],
    fitted_models: dict[str, Any],
    feature_columns: list[str],
    output_dir: Path,
    train_predictions: dict[str, np.ndarray] | None = None,
    selected_variant: str = "variant_3",
) -> pd.DataFrame:
    """Evaluate mean predicted probability vs actual prevalence by period for each model."""
    y_train = train["label"].to_numpy().astype(int)
    y_dev = development["label"].to_numpy().astype(int)

    v_short = selected_variant.replace("variant_", "v")
    dev_lr_key = f"lr_{v_short}" if f"lr_{v_short}" in predictions else "lr_v1"
    dev_rf_key = f"rf_{v_short}" if f"rf_{v_short}" in predictions else "rf_v1"

    # Obtain train predictions
    train_preds = {}
    train_preds["prior"] = np.full(len(train), y_train.mean())
    cap_train = train["capital_ratio"].fillna(train["capital_ratio"].median()).to_numpy()
    train_preds["rule_cap_ratio"] = -cap_train

    if train_predictions and dev_lr_key in train_predictions:
        train_preds["logistic_regression"] = train_predictions[dev_lr_key]
    elif "logistic_regression" in fitted_models:
        train_preds["logistic_regression"] = fitted_models["logistic_regression"].predict_proba(train[feature_columns])[:, 1]

    if train_predictions and dev_rf_key in train_predictions:
        train_preds["random_forest"] = train_predictions[dev_rf_key]
    elif "random_forest" in fitted_models:
        train_preds["random_forest"] = fitted_models["random_forest"].predict_proba(train[feature_columns])[:, 1]

    calib_rows = []
    models_to_score = [
        ("prior_baseline", train_preds["prior"], predictions["prior"]),
        ("capital_ratio_rule", train_preds["rule_cap_ratio"], predictions["rule_cap_ratio"]),
        ("logistic_regression", train_preds.get("logistic_regression"), predictions[dev_lr_key]),
        ("random_forest", train_preds.get("random_forest"), predictions[dev_rf_key]),
    ]

    for m_name, tr_p, dev_p in models_to_score:
        # Train period
        if tr_p is not None:
            # normalize rule scores to [0, 1] range for fair Brier comparison
            p_tr_norm = (tr_p - tr_p.min()) / (tr_p.max() - tr_p.min() + 1e-8) if "rule" in m_name else tr_p
            brier_tr = float(brier_score_loss(y_train, p_tr_norm))
            calib_rows.append({
                "model": m_name,
                "period": "train (1984Q1-1991Q4)",
                "rows": len(train),
                "actual_positives": int(np.sum(y_train)),
                "actual_prevalence_pct": round(float(np.mean(y_train) * 100), 4),
                "mean_predicted_probability_pct": round(float(np.mean(p_tr_norm) * 100), 4),
                "calibration_gap_pct": round(float((np.mean(p_tr_norm) - np.mean(y_train)) * 100), 4),
                "brier_score": round(brier_tr, 6),
            })
        # Dev period
        p_dev_norm = (dev_p - dev_p.min()) / (dev_p.max() - dev_p.min() + 1e-8) if "rule" in m_name else dev_p
        brier_dev = float(brier_score_loss(y_dev, p_dev_norm))
        calib_rows.append({
            "model": m_name,
            "period": "development (1993Q1-2005Q4)",
            "rows": len(development),
            "actual_positives": int(np.sum(y_dev)),
            "actual_prevalence_pct": round(float(np.mean(y_dev) * 100), 4),
            "mean_predicted_probability_pct": round(float(np.mean(p_dev_norm) * 100), 4),
            "calibration_gap_pct": round(float((np.mean(p_dev_norm) - np.mean(y_dev)) * 100), 4),
            "brier_score": round(brier_dev, 6),
        })

    df_calib = pd.DataFrame(calib_rows)
    df_calib.to_csv(output_dir / "p7_calibration_summary.csv", index=False)
    return df_calib


def recalculate_dev_thresholds_for_variant(
    features_parquet: Path,
    output_dir: Path,
    selected_variant: str = "variant_3",
    seed: int = 42,
) -> dict[str, Any]:
    """Recalculate development cost thresholds, stability, loss curve, and calibration for selected variant without touching test rows."""
    from bankfail.modeling.pipeline import FEATURE_COLUMNS, _build_cost_policy

    cols_to_read = FEATURE_COLUMNS + ["CERT", "REPDTE", "label", "horizon_end", "failure_cost_numeric"]
    df = pd.read_parquet(features_parquet, columns=cols_to_read)
    df["REPDTE"] = pd.to_datetime(df["REPDTE"]).dt.normalize()
    df["horizon_end"] = pd.to_datetime(df["horizon_end"]).dt.normalize()

    # Strictly pre-2006 train and development splits only (zero test rows read or evaluated)
    train = df[(df["REPDTE"] <= pd.Timestamp("1992-12-31")) & (df["horizon_end"] < pd.Timestamp("1993-03-31"))].copy()
    development = df[(df["REPDTE"] >= pd.Timestamp("1993-03-31")) & (df["REPDTE"] <= pd.Timestamp("2005-12-31")) & (df["horizon_end"] < pd.Timestamp("2008-01-01"))].copy()
    del df

    assert train["REPDTE"].max() <= pd.Timestamp("1991-12-31")
    assert development["REPDTE"].max() <= pd.Timestamp("2005-12-31")

    v1_cols = FEATURE_COLUMNS
    v2_cols = [c for c in FEATURE_COLUMNS if c != "rbcrwa_level"]
    v3_cols = [c for c in FEATURE_COLUMNS if c not in ["rbcrwa_level", "rbc1a_level"]]
    y_train = train["label"].astype(int)

    predictions: dict[str, np.ndarray] = {}
    train_predictions: dict[str, np.ndarray] = {}

    lr_v1_path = output_dir / "p7_fitted_pipeline_lr.joblib"
    rf_v1_path = output_dir / "p7_fitted_pipeline_rf.joblib"
    fitted_v1: dict[str, Any] = {}
    if lr_v1_path.exists() and rf_v1_path.exists():
        fitted_v1["logistic_regression"] = joblib.load(lr_v1_path)
        fitted_v1["random_forest"] = joblib.load(rf_v1_path)
        if hasattr(fitted_v1["random_forest"], "named_steps") and "model" in fitted_v1["random_forest"].named_steps:
            fitted_v1["random_forest"].named_steps["model"].n_jobs = 1

    for v_name, cols in [("v1", v1_cols), ("v3", v3_cols)]:
        if v_name == "v1" and "logistic_regression" in fitted_v1 and "random_forest" in fitted_v1 and list(getattr(fitted_v1["random_forest"], "feature_names_in_", [])) == list(v1_cols):
            predictions["lr_v1"] = fitted_v1["logistic_regression"].predict_proba(development[v1_cols])[:, 1]
            predictions["rf_v1"] = fitted_v1["random_forest"].predict_proba(development[v1_cols])[:, 1]
            train_predictions["lr_v1"] = fitted_v1["logistic_regression"].predict_proba(train[v1_cols])[:, 1]
            train_predictions["rf_v1"] = fitted_v1["random_forest"].predict_proba(train[v1_cols])[:, 1]
        else:
            pipe_lr = Pipeline([
                ("imputer", SimpleImputer(strategy="median", add_indicator=True, keep_empty_features=True)),
                ("scaler", StandardScaler()),
                ("model", LogisticRegression(max_iter=2000, class_weight="balanced", random_state=seed)),
            ])
            pipe_lr.fit(train[cols], y_train)
            predictions[f"lr_{v_name}"] = pipe_lr.predict_proba(development[cols])[:, 1]
            train_predictions[f"lr_{v_name}"] = pipe_lr.predict_proba(train[cols])[:, 1]

            pipe_rf = Pipeline([
                ("imputer", SimpleImputer(strategy="median", add_indicator=True, keep_empty_features=True)),
                ("model", RandomForestClassifier(n_estimators=100, min_samples_leaf=5, class_weight="balanced_subsample", random_state=seed, n_jobs=1)),
            ])
            pipe_rf.fit(train[cols], y_train)
            predictions[f"rf_{v_name}"] = pipe_rf.predict_proba(development[cols])[:, 1]
            train_predictions[f"rf_{v_name}"] = pipe_rf.predict_proba(train[cols])[:, 1]

    prior_prob = y_train.mean()
    predictions["prior"] = np.full(len(development), prior_prob)
    cap_ratio = development["capital_ratio"].fillna(development["capital_ratio"].median()).to_numpy()
    predictions["rule_cap_ratio"] = -cap_ratio

    cost_policy = _build_cost_policy(development, None, 0.01)
    v_short = selected_variant.replace("variant_", "v")
    cost_df = run_comprehensive_cost_analysis(development, predictions, cost_policy, output_dir, selected_variant=selected_variant)
    stability_res = run_threshold_stability_and_loss_curve(
        development,
        predictions[f"rf_{v_short}"],
        cost_policy,
        output_dir,
        seed=seed,
        feature_variant=selected_variant,
    )
    calib_df = run_calibration_analysis(
        train,
        development,
        predictions,
        fitted_v1,
        FEATURE_COLUMNS,
        output_dir,
        train_predictions=train_predictions,
        selected_variant=selected_variant,
    )
    return {
        "selected_variant": selected_variant,
        "stability": stability_res["stability"],
        "brute_force_best_threshold": stability_res["brute_force_best_threshold"],
        "brute_force_best_loss": stability_res["brute_force_best_loss"],
        "cost_rows_count": len(cost_df),
        "calibration_rows_count": len(calib_df),
    }

