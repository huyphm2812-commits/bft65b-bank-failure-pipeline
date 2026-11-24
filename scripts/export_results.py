from __future__ import annotations

import os
import shutil
import sys
from pathlib import Path

MAX_CSV_SIZE_BYTES = 1 * 1024 * 1024  # 1 MB

RUBRIC_MAPPING = {
    "01_acquisition": ("Part 1: Acquisition & Manifest", [
        ("validation_report.json", "Validates 170 quarters, record counts, checksums, and schema integrity"),
        ("validation_summary.txt", "Human-readable summary of FDIC download completeness"),
    ]),
    "02_quality": ("Part 2: Data Quality Audit", [
        ("data_quality_summary.csv", "Missingness and duplicate metrics per FDIC table"),
        ("data_quality_report.json", "Full non-destructive audit metadata and parameters"),
        ("outlier_iqr_by_quarter.csv", "Per-quarter IQR fences and zero-variance flags per numeric field"),
        ("outlier_iqr_summary.csv", "Field-level aggregate summary of quarterly IQR outliers"),
        ("failure_anomalies.csv", "Detailed breakdown of missing-CERT failures and assistance rows"),
        ("assistance_bank_review.csv", "Audit of FDIC assisted banks (RESTYPE=ASSISTANCE) that continued operating"),
        ("financial_field_availability.png", "Figure showing availability of Call Report items across decades"),
        ("missingness_top20.png", "Figure of 20 fields with highest missing rate"),
    ]),
    "03_panel": ("Part 2 & 3: Panel & Exit Classification", [
        ("exit_classification.csv", "Classification of failures vs mergers vs active institutions"),
        ("period_summary.csv", "Quarter-by-quarter active banks and failure counts"),
        ("panel_summary.json", "Summary statistics of bank-quarter panel and forward 4Q label"),
        ("problem_evidence_handling.csv", "Evidence-based problem handling decisions with counts"),
        ("merger_policy_sensitivity.json", "Sensitivity analysis comparing censored vs negative merger label"),
    ]),
    "04_storage_benchmark": ("Part 4: Storage Formats & Benchmarking", [
        ("p4_storage_benchmark_summary.csv", "Median write/read runtime, variance, and memory footprint across CSV, Gzip, Parquet, Arrow"),
        ("p4_storage_benchmark_read_times.png", "Figure comparing read times across storage formats"),
        ("p4_storage_benchmark_report.json", "Format file size and query pushdown measurements"),
    ]),
    "05_engines": ("Part 5: Polars & Pandas Processing", [
        ("p5_engine_benchmark_summary.csv", "Runtime and RSS memory comparison of Pandas vs Polars LazyFrame"),
        ("p5_output_equivalence.csv", "Validation showing exact numeric equivalence between processing engines"),
        ("p5_scaling_benchmark.csv", "In-memory aggregation scaling measurements across row counts"),
        ("p5_amdahl_scaling.csv", "Empirical speedup S and Amdahl implied parallel fraction p across threads"),
        ("p5_memory_vs_ram.json", "Deep in-memory panel footprint versus host available RAM"),
        ("p5_polars_query_plan.txt", "Polars optimized lazy execution DAG"),
        ("p5_pandas_quarter_summary.csv", "Aggregated quarterly metrics produced by pandas"),
        ("p5_polars_quarter_summary.csv", "Aggregated quarterly metrics produced by Polars"),
    ]),
    "06_spark": ("Part 6: PySpark Distributed Execution", [
        ("p6_spark_benchmark_summary.csv", "Local Spark runtime and memory measurements"),
        ("p6_spark_output_equivalence.csv", "Validation showing exact Spark equivalence to Polars summary"),
        ("p6_spark_explain_extended.txt", "Extended Catalyst query plan"),
        ("p6_spark_physical_plan.txt", "Executed Catalyst physical execution plan (Exchange/HashAggregate)"),
        ("p6_plan_interpretation.md", "Catalyst physical plan analysis, operator census, and broadcast experiment"),
        ("p6_spark_quarter_summary.csv", "Aggregated quarterly metrics produced by PySpark"),
    ]),
    "07_features_models": ("Part 7: Features, Modeling & Early Warning", [
        ("p7_feature_dictionary.csv", "CAMELS feature formulas, sources, and accounting definitions"),
        ("p7_feature_summary.json", "Feature point-in-time check, leakage verification, and 45-day lag rule"),
        ("p7_model_tuning_comparison.csv", "Grid tuning comparison for LR/RF and scalers on expanding window CV"),
        ("p7_best_model_config.json", "Frozen best hyperparameter configuration for held-out test evaluation"),
        ("p7_feature_distributions.png", "Minimalist distribution of capital_ratio and ROA by decade"),
        ("p7_model_results.json", "Expanding window CV metrics, PR-AUC, ROC-AUC, Brier score, baselines"),
        ("p7_threshold_selection.csv", "Threshold tuning on dev set comparing Max F1 vs Cost Threshold"),
        ("p7_cost_sensitivity.csv", "Expected loss sensitivity across different false-positive cost assumptions"),
        ("p7_development_curves.png", "ROC and Precision-Recall curves on development window"),
        ("p7_expanding_window_cv.csv", "Fold-by-fold chronological expanding window metrics"),
        ("p7_spark_mllib_predictions.csv", "Sample predictions from PySpark MLlib logistic regression pipeline"),
    ]),
    "08_reproducibility": ("Part 8: Reproducibility & Environment", [
        ("p8_reproducibility_manifest.json", "Audit manifest recording Python version, packages, config hash, and source SHA-256 digests"),
    ]),
}


def export_results(workspace_outputs: Path, repo_snapshot: Path, report_inputs: Path) -> None:
    repo_snapshot.mkdir(parents=True, exist_ok=True)
    report_inputs.mkdir(parents=True, exist_ok=True)

    snapshot_files: list[tuple[str, int]] = []
    report_files: list[tuple[str, int]] = []

    # Clean existing files in repo_snapshot
    for item in repo_snapshot.iterdir():
        if item.is_file() and item.name != ".gitkeep":
            item.unlink()

    # Traverse workspace outputs
    for root, _, files in os.walk(workspace_outputs):
        root_path = Path(root)
        for fname in sorted(files):
            file_path = root_path / fname
            suffix = file_path.suffix.lower()
            size = file_path.stat().st_size

            # Strictly reject data / binary formats
            if suffix in {".parquet", ".gz", ".zip", ".tar", ".ipc", ".arrow", ".bin"}:
                continue

            rel_path = file_path.relative_to(workspace_outputs)

            # Copy to 03_report_inputs/ (curated inputs for paper, all text/json/csv/figures)
            if suffix in {".json", ".txt", ".csv", ".png", ".pdf", ".svg", ".md"}:
                dest_report = report_inputs / rel_path
                dest_report.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(file_path, dest_report)
                report_files.append((str(rel_path), size))

            # Copy to 01_github_repo/results_snapshot/ (strict limits for Git repository)
            can_snapshot = False
            if suffix in {".json", ".txt", ".png", ".md"}:
                can_snapshot = True
            elif suffix == ".csv":
                if size <= MAX_CSV_SIZE_BYTES:
                    can_snapshot = True
                else:
                    print(f"Skipping CSV > 1MB for results_snapshot: {rel_path} ({size / 1024 / 1024:.2f} MB)")

            if can_snapshot:
                dest_snap = repo_snapshot / rel_path
                dest_snap.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(file_path, dest_snap)
                snapshot_files.append((str(rel_path), size))

    # Print Size Report
    total_snap_bytes = sum(s for _, s in snapshot_files)
    print("\n==========================================================")
    print("  RESULTS SNAPSHOT EXPORT SUMMARY")
    print("==========================================================")
    print(f"Total files copied to results_snapshot: {len(snapshot_files)}")
    print(f"Total snapshot size: {total_snap_bytes / 1024:.1f} KB ({total_snap_bytes / 1024 / 1024:.2f} MB)")
    for path, size in snapshot_files:
        print(f"  + {path:<50} ({size / 1024:.1f} KB)")

    # Generate 03_report_inputs/INDEX.md
    index_md = generate_index_markdown(report_inputs)
    (report_inputs / "INDEX.md").write_text(index_md, encoding="utf-8")
    print(f"\nGenerated report evidence index at: {(report_inputs / 'INDEX.md').resolve()}")


def generate_index_markdown(report_inputs: Path) -> str:
    lines = [
        "# Curated Report Inputs & Evidence Index",
        "",
        "This directory contains finished artifacts, tables, execution plans, and figures generated by the pipeline.",
        "Use these files to write the BFT65B 20-page academic report.",
        "",
        "## Rubric Evidence Map",
        "",
    ]

    for stage_dir, (part_title, key_files) in RUBRIC_MAPPING.items():
        lines.append(f"### {part_title} (`{stage_dir}/`)")
        lines.append("| Artifact | Rubric Role | Status |")
        lines.append("| :--- | :--- | :---: |")
        for fname, desc in key_files:
            present = (report_inputs / stage_dir / fname).exists()
            status_badge = "Available" if present else "Pending Full Run"
            lines.append(f"| `{fname}` | {desc} | {status_badge} |")
        lines.append("")

    return "\n".join(lines)


if __name__ == "__main__":
    script_dir = Path(__file__).resolve().parent
    repo_root = script_dir.parent
    bft_base = repo_root.parent

    workspace_out = bft_base / "02_local_workspace" / "outputs"
    repo_snap = repo_root / "results_snapshot"
    rep_inputs = bft_base / "03_report_inputs"

    if len(sys.argv) > 1 and sys.argv[1] == "--smoke":
        workspace_out = bft_base / "02_local_workspace" / "smoke" / "outputs"

    export_results(workspace_out, repo_snap, rep_inputs)
