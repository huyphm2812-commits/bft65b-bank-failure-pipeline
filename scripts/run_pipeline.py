from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

# Ensure src/ and repo root are on sys.path
SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPT_DIR.parent
SRC_DIR = REPO_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from bankfail.acquisition import FDICClient, load_config, run, validate_dataset, write_validation_report
from bankfail.quality import run_quality_audit
from bankfail.panel import build_panel_from_paths
from bankfail.benchmarks import run_storage_benchmark
from bankfail.processing import run_engine_benchmark
from bankfail.spark import run_spark_comparison
from bankfail.modeling import ModelingError, run_modeling_pipeline
from bankfail.repro import audit_repository, write_run_manifest


def resolve_config_path(config_arg: str | None) -> Path:
    if not config_arg:
        # Default candidate locations
        for cand in [REPO_ROOT / "config" / "config.yaml", SCRIPT_DIR / "config_config.yaml", Path("config/config.yaml"), Path("config_config.yaml")]:
            if cand.exists():
                return cand.resolve()
        return (REPO_ROOT / "config" / "config.yaml").resolve()
    cand = Path(config_arg)
    if cand.is_absolute():
        return cand
    for base in [Path.cwd(), REPO_ROOT, REPO_ROOT / "config"]:
        if (base / cand).exists():
            return (base / cand).resolve()
    return (Path.cwd() / cand).resolve()


def main() -> int:
    parser = argparse.ArgumentParser(description="FDIC bank-failure pipeline: acquisition, validation, benchmark, and modeling.")
    parser.add_argument("--config", default=None, help="Path to YAML configuration file")
    sub = parser.add_subparsers(dest="command", required=True)

    def add_config(command: argparse.ArgumentParser) -> None:
        command.add_argument("--config", default=argparse.SUPPRESS, help="Path to YAML configuration file")

    connection = sub.add_parser("connection", help="Test one small live FDIC API request")
    add_config(connection)

    acquire = sub.add_parser("acquire", help="Resume/download FDIC raw pages and Parquet")
    add_config(acquire)
    acquire.add_argument("--start", help="Override start date, YYYY-MM-DD")
    acquire.add_argument("--end", help="Override end date, YYYY-MM-DD")

    validate = sub.add_parser("validate", help="Validate acquired Parquet and manifest")
    add_config(validate)
    validate.add_argument("--start", help="Validation start date, YYYY-MM-DD")
    validate.add_argument("--end", help="Validation end date, YYYY-MM-DD")

    benchmark = sub.add_parser("benchmark", help="Benchmark CSV, gzip CSV and Parquet")
    add_config(benchmark)
    benchmark.add_argument("--repetitions", type=int, default=5)
    benchmark.add_argument("--sample-rows", type=int, default=None)
    benchmark.add_argument("--seed", type=int, default=42)
    benchmark.add_argument("--include-arrow", action="store_true")
    benchmark.add_argument("--cold-cache", action="store_true", help="Record that cold cache was requested; no cache eviction is attempted")

    audit = sub.add_parser("audit", help="Run the non-destructive Part 2 audit")
    add_config(audit)
    audit.add_argument("--processed-root", default=None)
    audit.add_argument("--output-dir", default=None)

    panel = sub.add_parser("panel", help="Build the Part 3 bank-quarter panel and forward labels")
    add_config(panel)
    panel.add_argument("--processed-root", default=None)
    panel.add_argument("--quality-dir", default=None)
    panel.add_argument("--output-dir", default=None)
    panel.add_argument("--label-cutoff", default="2025-06-30")
    panel.add_argument("--reporting-lag-days", type=int, default=45)
    panel.add_argument("--merger-policy", choices=("censor", "negative"), default="censor", help="Censor merger/other exits (standard) or retain them as negatives for sensitivity analysis")

    engines = sub.add_parser("engines", help="Compare equivalent pandas and Polars processing")
    add_config(engines)
    engines.add_argument("--processed-root", default=None)
    engines.add_argument("--panel-path", default=None)
    engines.add_argument("--output-dir", default=None)
    engines.add_argument("--repetitions", type=int, default=5)
    engines.add_argument("--sample-rows", type=int, default=None)
    engines.add_argument("--seed", type=int, default=42)

    spark = sub.add_parser("spark", help="Run the PySpark equivalent and capture actual plans")
    add_config(spark)
    spark.add_argument("--processed-root", default=None)
    spark.add_argument("--panel-path", default=None)
    spark.add_argument("--output-dir", default=None)
    spark.add_argument("--repetitions", type=int, default=2)
    spark.add_argument("--sample-rows", type=int, default=10000)
    spark.add_argument("--master", default="local[2]")

    model = sub.add_parser("model", help="Train baseline and primary models with leakage prevention")
    add_config(model)
    model.add_argument("--panel-path", default=None)
    model.add_argument("--output-dir", default=None)
    model.add_argument("--train-end", default=None)
    model.add_argument("--dev-start", default=None)
    model.add_argument("--dev-end", default=None)
    model.add_argument("--test-start", default=None)
    model.add_argument("--test-end", default=None)
    model.add_argument("--label-cutoff", default=None)
    model.add_argument("--min-dev-failures", type=int, default=None)
    model.add_argument("--min-test-failures", type=int, default=None)
    model.add_argument("--seed", type=int, default=None)
    model.add_argument("--false-negative-cost", type=float, default=None)
    model.add_argument("--false-positive-cost", type=float, default=None)
    model.add_argument("--false-positive-cost-rate", type=float, default=None)
    model.add_argument("--feature-variant", default=None, help="Feature variant (variant_1, variant_2, variant_3)")
    model.add_argument("--frozen-config", default=None, help="Path to frozen_model_config.json to strictly enforce frozen settings")
    model.add_argument("--preflight-only", action="store_true", help="Validate frozen config and print effective settings without accessing test data or locks")
    model.add_argument("--spark-mllib", action="store_true")
    model.add_argument("--spark-master", default="local[2]")
    model.add_argument("--features-only", action="store_true", help="Build features only; skip split and modeling (safe for small smoke data)")
    model.add_argument("--dev-only", action="store_true", help="Fit on train and evaluate expanding validation inside development only; do not touch test rows")
    model.add_argument("--evaluate-test", action="store_true", help="Evaluate the held-out test set once; writes a lock file to prevent repeated evaluations")
    model.add_argument("--rehearsal", action="store_true", help="Rehearsal run for test evaluation on pre-2006 windows without touching real test lock")
    model.add_argument("--force-after-crash", action="store_true", help="Allow restarting held-out evaluation if previous run crashed in STARTED state")
    model.add_argument("--dev-analysis", action="store_true", help="Run comprehensive development analysis (bootstrap, stability, curves, calibration) in dev-only mode")

    check = sub.add_parser("check", help="Audit repository structure, manifest and dependencies")
    add_config(check)
    check.add_argument("--output-dir", default=None)

    all_cmd = sub.add_parser("all", help="Execute the complete pipeline end-to-end")
    add_config(all_cmd)
    all_cmd.add_argument("--start", help="Override start date, YYYY-MM-DD")
    all_cmd.add_argument("--end", help="Override end date, YYYY-MM-DD")
    all_cmd.add_argument("--skip-acquire", action="store_true", help="Skip acquisition step if raw data is already present")
    all_cmd.add_argument("--benchmark-repetitions", type=int, default=5)
    all_cmd.add_argument("--benchmark-sample-rows", type=int, default=None)
    all_cmd.add_argument("--run-spark", action="store_true")
    all_cmd.add_argument("--spark-repetitions", type=int, default=2)
    all_cmd.add_argument("--spark-sample-rows", type=int, default=None)
    all_cmd.add_argument("--spark-mllib", action="store_true")
    all_cmd.add_argument("--features-only", action="store_true")

    args = parser.parse_args()

    config_path = resolve_config_path(args.config)
    cfg = load_config(config_path)
    paths = cfg["paths"]

    # Resolution of numbered output directories
    out_dir_arg = getattr(args, "output_dir", None)
    out_acq = Path(paths.get("out_acquisition", paths.get("outputs", paths["outputs"])))
    out_qual = Path(out_dir_arg or paths.get("out_quality", Path(paths["outputs"]).parent / "02_quality" if "01_acquisition" in str(paths["outputs"]) else Path(paths["outputs"]).parent / "quality_reports"))
    out_panel = Path(out_dir_arg or paths.get("out_panel", Path(paths["outputs"]).parent / "03_panel" if "01_acquisition" in str(paths["outputs"]) else Path(paths["outputs"]).parent / "panel"))
    out_bench = Path(paths.get("out_benchmark", Path(paths["outputs"]) / "04_storage_benchmark" if "01_acquisition" in str(paths["outputs"]) else Path(paths["outputs"]) / "benchmarks"))
    out_eng = Path(out_dir_arg or paths.get("out_engines", Path(paths["outputs"]) / "05_engines" if "01_acquisition" in str(paths["outputs"]) else Path(paths["outputs"]) / "processing_benchmarks"))
    out_spark = Path(out_dir_arg or paths.get("out_spark", Path(paths["outputs"]) / "06_spark" if "01_acquisition" in str(paths["outputs"]) else Path(paths["outputs"]) / "spark_benchmarks"))
    out_models = Path(out_dir_arg or paths.get("out_models", Path(paths["outputs"]) / "07_features_models" if "01_acquisition" in str(paths["outputs"]) else Path(paths["outputs"]) / "model_results"))
    out_repro = Path(out_dir_arg or paths.get("out_repro", Path(paths["outputs"]) / "08_reproducibility" if "01_acquisition" in str(paths["outputs"]) else Path(paths["outputs"]) / "reproducibility"))

    if args.command == "connection":
        client = FDICClient(cfg["api_base"], cfg["timeout_seconds"], cfg["max_retries"], cfg["backoff_seconds"])
        page = next(client.iter_pages("failures", ["CERT", "FAILDATE", "COST"], sort_by="CERT", page_size=2))
        print(f"Connection OK: received {len(page['data'])} records; API total={page['meta']['total']}")
    elif args.command == "acquire":
        run(config_path, args.start, args.end)
    elif args.command == "validate":
        report = validate_dataset(Path(paths["processed"]), Path(paths["manifests"]) / "acquisition_manifest.json", args.start or cfg["start_date"], args.end or cfg["end_date"])
        out_acq.mkdir(parents=True, exist_ok=True)
        write_validation_report(report, out_acq / "validation_report.json", out_acq / "validation_summary.txt")
        failed = report.get("status") != "ok"
        print(f"Validation written to {out_acq.resolve()}; status={'FAIL' if failed else 'PASS'}")
        return 1 if failed else 0
    elif args.command == "benchmark":
        out_bench.mkdir(parents=True, exist_ok=True)
        result = run_storage_benchmark(Path(paths["processed"]) / "financials", out_bench, args.repetitions, args.sample_rows, args.seed, args.include_arrow, args.cold_cache)
        print(f"Benchmark complete: {result['rows']:,} rows written to {out_bench.resolve()}")
    elif args.command == "audit":
        out_qual.mkdir(parents=True, exist_ok=True)
        report = run_quality_audit(Path(args.processed_root or paths["processed"]), out_qual, Path(paths["manifests"]) / "acquisition_manifest.json")
        print(f"Quality audit complete: {out_qual.resolve()}")
        for item in report["datasets"]:
            print(item)
    elif args.command == "panel":
        out_panel.mkdir(parents=True, exist_ok=True)
        quality_dir = Path(args.quality_dir or out_qual)
        cutoff = args.label_cutoff or cfg.get("label_cutoff", "2025-06-30")
        summary = build_panel_from_paths(Path(args.processed_root or paths["processed"]), out_panel, quality_dir, cutoff, args.reporting_lag_days, args.merger_policy)
        print(json.dumps(summary, indent=2))
    elif args.command == "engines":
        processed = Path(args.processed_root or Path(paths["processed"]) / "financials")
        out_eng.mkdir(parents=True, exist_ok=True)
        panel_path = Path(args.panel_path) if args.panel_path else out_panel / "modeling_panel.parquet"
        panel = panel_path if panel_path.exists() else None
        result = run_engine_benchmark(processed, out_eng, panel, args.repetitions, args.sample_rows, args.seed)
        print(json.dumps({"output_dir": str(out_eng.resolve()), "equivalent": result["output_equivalence"]["equivalent"], "rows": result["source_rows_after_cleaning"]}, indent=2))
    elif args.command == "spark":
        processed = Path(args.processed_root or Path(paths["processed"]) / "financials")
        out_spark.mkdir(parents=True, exist_ok=True)
        panel_path = Path(args.panel_path) if args.panel_path else out_panel / "modeling_panel.parquet"
        panel = panel_path if panel_path.exists() else None
        result = run_spark_comparison(processed, out_spark, panel, args.repetitions, args.sample_rows, args.master)
        print(json.dumps({"output_dir": str(out_spark.resolve()), "equivalent": result["output_equivalence"]["equivalent"], "rows": result["source_rows_after_cleaning"], "spark_version": result["spark_version"]}, indent=2))
    elif args.command == "model":
        out_models.mkdir(parents=True, exist_ok=True)
        try:
            panel_path = Path(args.panel_path) if args.panel_path else out_panel / "bank_quarter_panel.parquet"
            train_end = args.train_end or cfg.get("train_end", "1992-12-31")
            dev_start = args.dev_start or cfg.get("dev_start", "1993-03-31")
            dev_end = args.dev_end or cfg.get("dev_end", "2005-12-31")
            test_start = args.test_start or cfg.get("test_start", "2008-01-01")
            test_end = args.test_end or cfg.get("test_end", "2014-12-31")
            label_cutoff = args.label_cutoff or cfg.get("label_cutoff", "2025-06-30")
            min_dev_fail = args.min_dev_failures if args.min_dev_failures is not None else cfg.get("min_dev_failures", 20)
            min_test_fail = args.min_test_failures if args.min_test_failures is not None else cfg.get("min_test_failures", 50)
            seed_val = args.seed if args.seed is not None else int(cfg.get("random_seed", 42))
            fp_rate_val = args.false_positive_cost_rate if args.false_positive_cost_rate is not None else 0.10
            feat_var_val = args.feature_variant or "variant_1"
            frozen_cfg_path = Path(args.frozen_config) if args.frozen_config else None
            cli_overrides = {
                "train_end": args.train_end,
                "dev_start": args.dev_start,
                "dev_end": args.dev_end,
                "test_start": args.test_start,
                "test_end": args.test_end,
                "seed": args.seed,
                "feature_variant": args.feature_variant,
                "false_positive_cost_rate": args.false_positive_cost_rate,
            }
            result = run_modeling_pipeline(
                panel_path, out_models,
                train_end=train_end, dev_start=dev_start, dev_end=dev_end,
                test_start=test_start, test_end=test_end, label_cutoff=label_cutoff,
                min_dev_failures=min_dev_fail, min_test_failures=min_test_fail,
                seed=seed_val,
                false_negative_cost=args.false_negative_cost,
                false_positive_cost=args.false_positive_cost,
                false_positive_cost_rate=fp_rate_val,
                spark_mllib=args.spark_mllib, spark_master=args.spark_master,
                features_only=args.features_only, dev_only=args.dev_only, evaluate_test=args.evaluate_test,
                rehearsal=getattr(args, "rehearsal", False), force_after_crash=getattr(args, "force_after_crash", False), dev_analysis=getattr(args, "dev_analysis", False),
                feature_variant=feat_var_val,
                frozen_config_path=frozen_cfg_path,
                preflight_only=getattr(args, "preflight_only", False),
                cli_overrides=cli_overrides,
            )
        except ModelingError as exc:
            print(f"Modeling not run: {exc}", file=sys.stderr)
            return 1
        if getattr(args, "preflight_only", False):
            print(json.dumps(result, indent=2))
        else:
            print(json.dumps({"output_dir": str(out_models.resolve()), "mode": result["mode"], "eligible_rows": result.get("eligible_rows", result.get("split", {}).get("test", {}).get("rows"))}, indent=2))
    elif args.command == "check":
        report = audit_repository(REPO_ROOT)
        out_repro.mkdir(parents=True, exist_ok=True)
        manifest = write_run_manifest(REPO_ROOT, out_repro, config_path, ["python", "run_pipeline.py", "check"], {"repository_audit": report})
        print(json.dumps({"ok": report["ok"], "manifest": str(manifest.resolve()), "audit": report}, indent=2))
        return 0 if report["ok"] else 1
    elif args.command == "all":
        root_outputs = Path(paths["outputs"])
        processed_root = Path(paths["processed"])
        panel_output = out_panel
        panel_path = panel_output / "bank_quarter_panel.parquet"
        stage_details: dict[str, Any] = {}
        if not args.skip_acquire:
            run(config_path, args.start, args.end)
            stage_details["acquire"] = "completed"
        else:
            stage_details["acquire"] = "skipped"
        validation = validate_dataset(processed_root, Path(paths["manifests"]) / "acquisition_manifest.json", args.start or cfg["start_date"], args.end or cfg["end_date"])
        out_acq.mkdir(parents=True, exist_ok=True)
        write_validation_report(validation, out_acq / "validation_report.json", out_acq / "validation_summary.txt")
        stage_details["validate"] = validation.get("datasets", {})
        if validation.get("status") != "ok":
            print(json.dumps({"stages": stage_details, "validation_errors": validation.get("errors", [])}, indent=2), file=sys.stderr)
            return 1
        quality = run_quality_audit(processed_root, out_qual, Path(paths["manifests"]) / "acquisition_manifest.json")
        stage_details["audit"] = quality.get("datasets", [])
        panel_summary = build_panel_from_paths(processed_root, panel_output, out_qual, cfg.get("label_cutoff", "2025-06-30"), 45)
        stage_details["panel"] = panel_summary
        engine_result = run_engine_benchmark(processed_root / "financials", out_eng, panel_output / "modeling_panel.parquet", args.benchmark_repetitions, args.benchmark_sample_rows, int(cfg.get("random_seed", 42)))
        stage_details["engines"] = {"equivalent": engine_result["output_equivalence"]["equivalent"], "rows": engine_result["source_rows_after_cleaning"]}
        if args.run_spark:
            spark_result = run_spark_comparison(processed_root / "financials", out_spark, panel_output / "modeling_panel.parquet", args.spark_repetitions, args.spark_sample_rows)
            stage_details["spark"] = {"equivalent": spark_result["output_equivalence"]["equivalent"], "rows": spark_result["source_rows_after_cleaning"]}
        else:
            stage_details["spark"] = "skipped"
        model_result = run_modeling_pipeline(panel_path, out_models, seed=int(cfg.get("random_seed", 42)), spark_mllib=args.spark_mllib, features_only=args.features_only)
        stage_details["model"] = {"mode": model_result["mode"], "eligible_rows": model_result.get("eligible_rows")}
        out_repro.mkdir(parents=True, exist_ok=True)
        manifest = write_run_manifest(REPO_ROOT, out_repro, config_path, ["python", "run_pipeline.py", "all"], stage_details)
        print(json.dumps({"stages": stage_details, "manifest": str(manifest.resolve())}, indent=2, default=str))
    return 0


if __name__ == "__main__":
    sys.exit(main())
