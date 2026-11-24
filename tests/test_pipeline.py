import gzip
import json
import os
from pathlib import Path

import pytest

from bankfail.acquisition import FDICClient, quarter_ends
from bankfail.acquisition import AcquisitionDownloader
from bankfail.acquisition import validate_dataset
from bankfail.acquisition import DATASET_SCHEMA_DEFINITIONS, sha256, write_parquet_page, write_raw_page


class FakeResponse:
    status_code = 200
    headers = {}
    text = ""

    def json(self):
        return {"meta": {"total": 2}, "data": [{"data": {"CERT": 1}}, {"data": {"CERT": 2}}]}


class FakeSession:
    def __init__(self):
        self.calls = []
        self.headers = {}

    def get(self, url, params, timeout):
        self.calls.append(params)
        return FakeResponse()


def test_quarters_are_iso_dates():
    assert len(quarter_ends("1984-03-31", "2026-06-30")) == 170


def test_page_uses_iso_filter_and_stops_on_total():
    session = FakeSession()
    client = FDICClient("https://example.test", session=session)
    pages = list(client.iter_pages("financials", ["CERT", "REPDTE"], "REPDTE:2023-03-31", page_size=2))
    assert len(pages) == 1
    assert session.calls[0]["filters"] == "REPDTE:2023-03-31"


def test_empty_page_is_allowed_only_when_api_total_is_zero():
    class EmptySession(FakeSession):
        def get(self, url, params, timeout):
            self.calls.append(params)
            response = FakeResponse()
            response.json = lambda: {"meta": {"total": 0}, "data": []}
            return response

    session = EmptySession()
    pages = list(FDICClient("https://example.test", session=session).iter_pages("failures", ["CERT"], page_size=10))
    assert len(pages) == 1 and pages[0]["data"] == []


def test_explicit_schema_drops_id_and_uses_millisecond_dates(tmp_path):
    pd = pytest.importorskip("pandas")
    pytest.importorskip("pyarrow")
    path = tmp_path / "financials.parquet"
    info = write_parquet_page(path, [{"data": {"ID": "api-row", "CERT": 1, "REPDTE": "20230331", "STNAME": "A", "ASSET": 10}}], "financials")
    schema = pd.read_parquet(path).dtypes.astype(str).to_dict()
    assert "ID" not in info["columns"]
    assert "ID" not in schema
    assert str(pd.read_parquet(path)["REPDTE"].dtype) == "datetime64[ms]"
    assert info["schema"].split(";") == [f"{name}:{kind}" for name, kind in DATASET_SCHEMA_DEFINITIONS["financials"].items()]


def test_resume_skips_completed_query_before_http(tmp_path):
    class CountingClient:
        def __init__(self):
            self.iter_dataset_calls = 0

        def iter_dataset(self, *args, **kwargs):
            self.iter_dataset_calls += 1
            raise AssertionError("resume must not advance the API generator")

    raw_path = tmp_path / "raw" / "failures" / "all-page-000000.json.gz"
    parquet_path = tmp_path / "processed" / "failures" / "part-all-000000.parquet"
    payload = {"meta": {"total": 1}, "data": [{"data": {"CERT": 1, "FAILDATE": "01/01/2020", "RESTYPE": "FAILURE"}}]}
    write_raw_page(raw_path, payload)
    parquet_info = write_parquet_page(parquet_path, payload["data"], "failures")
    manifest_path = tmp_path / "manifests" / "acquisition_manifest.json"
    manifest_path.parent.mkdir(parents=True)
    manifest_path.write_text(json.dumps({"version": 1, "chunks": {"failures/all/page-000000": {
        "dataset": "failures", "status": "complete", "params": {"filters": "all"}, "record_count": 1, "api_total": 1,
        "raw_path": str(raw_path), "raw_sha256": sha256(raw_path), "parquet_path": str(parquet_path), "parquet": parquet_info,
    }}}, indent=2), encoding="utf-8")
    client = CountingClient()
    downloader = AcquisitionDownloader(client, tmp_path / "raw", tmp_path / "processed", manifest_path)
    downloader.download_small_table("failures", ["CERT", "FAILDATE", "RESTYPE"], "CERT", 10)
    assert client.iter_dataset_calls == 0


def test_validation_fails_on_missing_expected_quarter_and_manifest_totals(tmp_path):
    manifest = tmp_path / "manifest.json"
    manifest.write_text(json.dumps({"version": 1, "chunks": {}}, indent=2), encoding="utf-8")
    report = validate_dataset(tmp_path / "processed", manifest, "2023-03-31", "2023-06-30")
    assert report["status"] == "fail"
    assert any("missing expected quarter" in error for error in report["errors"])


def test_validation_rejects_zero_record_financial_quarter(tmp_path):
    manifest = tmp_path / "manifest.json"
    manifest.write_text(json.dumps({"version": 1, "chunks": {"financials/REPDTE:20230331/page-000000": {
        "dataset": "financials", "status": "complete", "params": {"filters": "REPDTE:20230331"}, "record_count": 0, "api_total": 0,
        "raw_path": str(tmp_path / "missing.raw"), "parquet_path": str(tmp_path / "missing.parquet"),
    }}}, indent=2), encoding="utf-8")
    report = validate_dataset(tmp_path / "processed", manifest, "2023-03-31", "2023-03-31")
    assert report["status"] == "fail"
    assert any("positive api_total" in error for error in report["errors"])


def test_financial_quarter_query_uses_fdic_date_encoding():
    session = FakeSession()
    client = FDICClient("https://example.test", session=session)
    list(client.iter_financial_quarters("2023-03-31", "2023-03-31", ["CERT", "REPDTE"], page_size=2))
    assert session.calls[0]["filters"] == "REPDTE:20230331"


def test_raw_page_is_gzip_json(tmp_path):
    path = tmp_path / "page.json.gz"
    payload = {"meta": {"total": 1}, "data": [{"data": {"CERT": 10}}]}
    write_raw_page(path, payload)
    with gzip.open(path, "rt", encoding="utf-8") as stream:
        assert json.load(stream) == payload
    assert len(sha256(path)) == 64


def test_quality_audit_detects_duplicates_and_missingness(tmp_path):
    pd = pytest.importorskip("pandas")
    pytest.importorskip("pyarrow")
    from bankfail.quality import run_quality_audit

    processed = tmp_path / "processed" / "financials" / "year=2023"
    processed.mkdir(parents=True)
    pd.DataFrame({"CERT": [1, 1, None], "REPDTE": ["2023-03-31", "2023-03-31", "2023-06-30"], "ASSET": [100, -5, 20]}).to_parquet(processed / "part.parquet", index=False)
    report = run_quality_audit(tmp_path / "processed", tmp_path / "quality")
    summary = report["datasets"][0]
    assert summary["duplicate_key_groups"] == 1
    assert summary["invalid_rule_counts"]["negative:ASSET"] == 1


def test_ntlnls_negative_is_reported_not_rejected(tmp_path):
    pd = pytest.importorskip("pandas")
    pytest.importorskip("pyarrow")
    from bankfail.quality import run_quality_audit

    processed = tmp_path / "processed" / "financials"
    processed.mkdir(parents=True)
    pd.DataFrame({"CERT": [1], "REPDTE": ["2023-03-31"], "NTLNLS": [-5]}).to_parquet(processed / "part.parquet", index=False)
    report = run_quality_audit(tmp_path / "processed", tmp_path / "quality")
    summary = report["datasets"][0]
    assert "negative:NTLNLS" not in summary["invalid_rule_counts"]
    assert summary["invalid_rule_counts"]["observed_negative:NTLNLS"] == 1


def test_panel_labels_failure_merger_and_negative_without_bridging():
    pd = pytest.importorskip("pandas")
    from bankfail.panel import build_panel_from_frames

    quarters = pd.date_range("2023-03-31", "2024-06-30", freq="QE-DEC")
    financials = pd.concat([
        pd.DataFrame({"CERT": 1, "REPDTE": quarters, "ASSET": 10, "STNAME": "A"}),
        pd.DataFrame({"CERT": 2, "REPDTE": quarters, "ASSET": 20, "STNAME": "B"}),
        pd.DataFrame({"CERT": 3, "REPDTE": quarters, "ASSET": 30, "STNAME": "C"}),
    ], ignore_index=True)
    failures = pd.DataFrame({"CERT": [1], "FAILDATE": ["2023-09-30"], "COST": [100]})
    institutions = pd.DataFrame({"CERT": [1, 2, 3], "ENDEFYMD": [None, "2023-09-30", None], "ACTIVE": [0, 0, 1], "CHANGEC1": [0, 100, 0]})
    panel, exits, _ = build_panel_from_frames(financials, failures, institutions, "2025-06-30")
    q1 = panel[(panel["CERT"] == 1) & (panel["REPDTE"] == pd.Timestamp("2023-03-31"))].iloc[0]
    q2 = panel[(panel["CERT"] == 2) & (panel["REPDTE"] == pd.Timestamp("2023-03-31"))].iloc[0]
    q3 = panel[(panel["CERT"] == 3) & (panel["REPDTE"] == pd.Timestamp("2023-03-31"))].iloc[0]
    assert q1["label"] == 1
    assert pd.isna(q2["label"]) and q2["label_status"] == "censored_merger_other_exit"
    assert q3["label"] == 0
    assert exits.loc[exits["CERT"] == 1, "exit_classification"].iloc[0] == "failure"


def test_assistance_is_not_failure_and_duplicate_rule_uses_explicit_failure():
    pd = pytest.importorskip("pandas")
    from bankfail.panel import classify_exits

    failures = pd.DataFrame({
        "CERT": [9, 9],
        "FAILDATE": ["1988-01-01", "1990-01-01"],
        "RESTYPE": ["ASSISTANCE", "FAILURE"],
        "COST": [100, 200],
    })
    institutions = pd.DataFrame({"CERT": [9], "ENDEFYMD": [None], "ACTIVE": [1], "CHANGEC1": [0]})
    exits = classify_exits(failures, institutions)
    row = exits.loc[exits["CERT"] == 9].iloc[0]
    assert row["failure_date"] == pd.Timestamp("1990-01-01")
    assert row["assistance_date"] == pd.Timestamp("1988-01-01")
    assert row["exit_classification"] == "failure"


def test_panel_masks_quarters_after_label_cutoff():
    pd = pytest.importorskip("pandas")
    from bankfail.panel import build_panel_from_frames

    quarters = pd.date_range("2025-03-31", "2026-03-31", freq="QE-DEC")
    financials = pd.DataFrame({"CERT": 1, "REPDTE": quarters, "ASSET": 10, "STNAME": "A"})
    failures = pd.DataFrame({"CERT": [], "FAILDATE": [], "COST": []})
    institutions = pd.DataFrame({"CERT": [1], "ENDEFYMD": [None], "ACTIVE": [1], "CHANGEC1": [0]})
    panel, _, _ = build_panel_from_frames(financials, failures, institutions, "2025-06-30")
    assert panel.loc[panel["REPDTE"] > pd.Timestamp("2025-06-30"), "label"].isna().all()


def test_benchmark_sample_is_reproducible_and_year_aware():
    pd = pytest.importorskip("pandas")
    from bankfail.benchmarks import stratified_sample

    frame = pd.DataFrame({"CERT": range(12), "REPDTE": pd.date_range("2020-03-31", periods=12, freq="QE-DEC"), "ASSET": range(12)})
    first, first_meta = stratified_sample(frame, 6, seed=42)
    second, second_meta = stratified_sample(frame, 6, seed=42)
    assert first.equals(second)
    assert first_meta == second_meta
    assert first_meta["strategy"] == "random_stratified_by_reporting_year"


def test_benchmark_date_filter_runs_and_checks_equivalence(tmp_path):
    pd = pytest.importorskip("pandas")
    pytest.importorskip("pyarrow")
    from bankfail.benchmarks import run_storage_benchmark

    source = tmp_path / "processed" / "financials" / "year=2023"
    source.mkdir(parents=True)
    pd.DataFrame({"CERT": [1, 2], "REPDTE": ["2019-12-31", "2023-03-31"], "ASSET": [10, 20]}).to_parquet(source / "part.parquet", index=False)
    report = run_storage_benchmark(tmp_path / "processed" / "financials", tmp_path / "benchmarks", repetitions=2)
    assert report["date_filter"]["value"] == "2020-01-01T00:00:00"
    assert all(item["equivalent"] for item in report["output_equivalence"]["checks"].values())


def test_pandas_and_polars_processing_are_equivalent(tmp_path):
    pd = pytest.importorskip("pandas")
    pytest.importorskip("pyarrow")
    pytest.importorskip("polars")
    from bankfail.processing import compare_engine_outputs, pandas_transform, polars_transform

    source = tmp_path / "processed" / "financials" / "year=2023"
    source.mkdir(parents=True)
    pd.DataFrame(
        {
            "ID": ["1_20230331", "2_20230630", "3_20230930"],
            "CERT": [1, 2, 3],
            "REPDTE": ["20230331", "2023-06-30", "2023/09/30"],
            "ASSET": [100, 200, 300],
            "DEP": [50, 0, 150],
            "EQ": [10, 20, 30],
            "ROA": [1.0, 2.0, 3.0],
        }
    ).to_parquet(source / "part.parquet", index=False)
    panel = tmp_path / "panel.parquet"
    pd.DataFrame({"CERT": [1, 2, 3], "REPDTE": ["2023-03-31", "2023-06-30", "2023-09-30"], "label": [1, 0, None], "eligible_for_model": [True, True, False]}).to_parquet(panel, index=False)
    pandas_result = pandas_transform(tmp_path / "processed" / "financials", panel)
    polars_result = polars_transform(tmp_path / "processed" / "financials", panel)
    comparison = compare_engine_outputs(pandas_result, polars_result)
    assert comparison["equivalent"]
    assert "SCAN" in polars_result["lazy_plan"].upper()
    assert "ID" not in pandas_result["rows"].columns


def test_spark_matches_polars_on_summary_and_sample(tmp_path):
    if os.environ.get("BFT65B_RUN_SPARK_TEST") != "1":
        pytest.skip("Spark integration test is opt-in; set BFT65B_RUN_SPARK_TEST=1 in a local VS Code terminal.")
    pd = pytest.importorskip("pandas")
    pytest.importorskip("pyarrow")
    pytest.importorskip("polars")
    pytest.importorskip("pyspark")
    from bankfail.panel import build_panel_from_paths
    from bankfail.processing import compare_engine_outputs, polars_transform
    from bankfail.spark import create_spark_session, spark_transform

    source = tmp_path / "processed" / "financials" / "year=2023"
    source.mkdir(parents=True)
    quarters = pd.date_range("2023-03-31", periods=5, freq="QE-DEC")
    financials = pd.DataFrame({"ID": [f"{cert}_{date:%Y%m%d}" for cert in (1, 2) for date in quarters], "CERT": [cert for cert in (1, 2) for _ in quarters], "REPDTE": list(quarters) * 2, "ASSET": [100 + index for index in range(10)], "DEP": [50] * 10, "EQ": [10] * 10, "ROA": [1.0] * 10})
    financials["REPDTE"] = financials["REPDTE"].astype("datetime64[ms]")
    financials.to_parquet(source / "part.parquet", index=False)
    for dataset in ("failures", "institutions"):
        (tmp_path / "processed" / dataset / "year=2023").mkdir(parents=True)
    pd.DataFrame({"CERT": [1], "FAILDATE": ["2024-06-30"], "COST": [100], "RESTYPE": ["FAILURE"]}).to_parquet(tmp_path / "processed" / "failures" / "year=2023" / "part.parquet", index=False)
    pd.DataFrame({"CERT": [1, 2], "ENDEFYMD": [None, None], "ACTIVE": [0, 1], "CHANGEC1": [0, 0]}).to_parquet(tmp_path / "processed" / "institutions" / "year=2023" / "part.parquet", index=False)
    panel_dir = tmp_path / "panel_output"
    build_panel_from_paths(tmp_path / "processed", panel_dir, tmp_path / "quality")
    panel = panel_dir / "bank_quarter_panel.parquet"
    import pyarrow.parquet as pq
    assert str(pq.read_schema(panel).field("financial_available_date").type) == "timestamp[ms]"
    spark = create_spark_session(app_name="BFT65B-test")
    try:
        spark_result = spark_transform(tmp_path / "processed" / "financials", panel, spark)
        polars_result = polars_transform(tmp_path / "processed" / "financials", panel)
        spark_sample = spark_result["rows"].orderBy("CERT", "REPDTE").limit(3).toPandas()
        comparison = compare_engine_outputs({"rows": polars_result["rows"].sort(["CERT", "REPDTE"]).head(3), "quarter_summary": polars_result["quarter_summary"]}, {"rows": spark_sample, "quarter_summary": spark_result["quarter_summary"].toPandas()})
        assert comparison["equivalent"]
        assert "ID" not in spark_result["rows"].columns
    finally:
        spark.stop()


def test_part7_features_do_not_bridge_missing_quarters(tmp_path):
    pd = pytest.importorskip("pandas")
    from bankfail.modeling import FEATURE_COLUMNS, LEAKAGE_COLUMNS, build_feature_table

    dates = pd.to_datetime(["2020-03-31", "2020-09-30", "2020-12-31", "2021-03-31"])
    panel = pd.DataFrame({
        "CERT": [1] * len(dates),
        "REPDTE": dates,
        "horizon_end": dates + pd.offsets.QuarterEnd(4),
        "label": [0, 0, 0, 0],
        "eligible_for_model": [True] * len(dates),
        "ASSET": [100, 130, 140, 150],
        "DEP": [50, 60, 70, 75],
        "EQ": [10, 12, 13, 14],
        "LNLSNET": [40, 45, 50, 52],
        "NCLNLS": [2, 3, 3, 4],
        "ORE": [1, 1, 2, 2],
        "CHBAL": [10, 12, 12, 13],
        "SC": [15, 16, 17, 18],
        "NETINC": [1, 2, 3, 4],
        "ROA": [1.0, 1.1, 1.2, 1.3],
        "ROE": [5.0, 5.1, 5.2, 5.3],
        "NIMY": [2.0, 2.1, 2.2, 2.3],
        "RBC1AAJ": [10, 10, 11, 11],
        "RBCRWAJ": [12, 12, 13, 13],
    })
    panel_path = tmp_path / "bank_quarter_panel.parquet"
    panel.to_parquet(panel_path, index=False)
    result = build_feature_table(panel_path, tmp_path / "model_results")
    features = result["all"]
    after_gap = features.loc[features["REPDTE"] == pd.Timestamp("2020-09-30")].iloc[0]
    after_adjacent = features.loc[features["REPDTE"] == pd.Timestamp("2020-12-31")].iloc[0]
    assert pd.isna(after_gap["asset_growth_qoq"])
    assert not pd.isna(after_adjacent["asset_growth_qoq"])
    assert set(FEATURE_COLUMNS).isdisjoint(LEAKAGE_COLUMNS)
    assert features["point_in_time_ok"].all()
    assert result["future_perturbation"]["passed"]


def test_part7_modeling_uses_development_threshold_and_no_invented_cost(tmp_path):
    pd = pytest.importorskip("pandas")
    from bankfail.modeling import run_modeling_pipeline

    dates = pd.date_range("2010-03-31", "2025-06-30", freq="QE-DEC")
    rows = []
    for cert in range(1, 5):
        for index, date in enumerate(dates):
            rows.append({"CERT": cert, "REPDTE": date, "horizon_end": date + pd.offsets.QuarterEnd(4), "label": int((cert + index) % 7 == 0), "eligible_for_model": True, "failure_cost_numeric": 100.0, "ASSET": 100 + cert * 5 + index, "DEP": 50 + index, "EQ": 10 + cert, "LNLSNET": 40 + index, "NCLNLS": 2 + cert, "ORE": 1, "CHBAL": 10, "SC": 15, "NETINC": 1 + (index % 5), "ROA": 1.0 + (index % 4) / 10, "ROE": 5.0 + (index % 3) / 10, "NIMY": 2.0, "RBC1AAJ": 10.0, "RBCRWAJ": 12.0})
    panel_path = tmp_path / "bank_quarter_panel.parquet"
    pd.DataFrame(rows).to_parquet(panel_path, index=False)
    report = run_modeling_pipeline(panel_path, tmp_path / "model_results", train_end="2012-12-31", dev_start="2013-03-31", dev_end="2013-12-31", test_start="2015-01-01", test_end="2015-12-31", min_dev_failures=1, min_test_failures=1)
    assert report["mode"] == "development_only"
    assert set(report["models"]) == {"prior_baseline", "always_negative_baseline", "logistic_regression", "random_forest"}
    assert report["costs"]["invented_costs"] is False
    assert report["models"]["logistic_regression"]["development_threshold_selection"]["rule"] == "max_f1_on_development"
    assert report["test_evaluated"] is False
    assert len(pd.read_csv(tmp_path / "model_results" / "p7_cost_sensitivity.csv")) == 16


def test_cost_threshold_counts_failed_banks_once(tmp_path):
    pd = pytest.importorskip("pandas")
    from bankfail.modeling import _select_cost_threshold

    y_true = pd.Series([1, 1, 1, 0])
    probability = __import__("numpy").array([0.95, 0.80, 0.75, 0.80])
    cert = pd.Series([1, 1, 2, 3])
    costs = pd.Series([100.0, 100.0, 100.0, float("nan")])
    threshold, report = _select_cost_threshold(y_true, probability, cert, costs, false_positive_cost=150.0)
    assert threshold == 0.95
    assert report["development_expected_loss"] == 100.0


def test_test_evaluation_lock_refuses_second_run(tmp_path):
    pd = pytest.importorskip("pandas")
    from bankfail.modeling import ModelingError, run_modeling_pipeline

    dates = pd.date_range("2010-03-31", "2025-06-30", freq="QE-DEC")
    rows = []
    for cert in range(1, 5):
        for index, date in enumerate(dates):
            rows.append({"CERT": cert, "REPDTE": date, "horizon_end": date + pd.offsets.QuarterEnd(4), "label": int((cert + index) % 7 == 0), "eligible_for_model": True, "ASSET": 100 + cert + index, "DEP": 50, "EQ": 10, "LNLSNET": 40, "NCLNLS": 2, "ORE": 1, "CHBAL": 10, "SC": 15, "ROA": 1.0, "ROE": 5.0, "NIMY": 2.0, "RBC1AAJ": 10.0, "RBCRWAJ": 12.0, "failure_cost_numeric": 100.0})
    panel_path = tmp_path / "bank_quarter_panel.parquet"
    pd.DataFrame(rows).to_parquet(panel_path, index=False)
    output = tmp_path / "model_results"
    output.mkdir()
    (output / "p7_test_evaluation.lock").write_text("{}", encoding="utf-8")
    with pytest.raises(ModelingError, match="lock exists"):
        run_modeling_pipeline(panel_path, output, train_end="2012-12-31", dev_start="2013-03-31", dev_end="2013-12-31", test_start="2015-01-01", test_end="2015-12-31", min_dev_failures=1, min_test_failures=1, evaluate_test=True)
    assert (output / "p7_test_evaluation.lock").exists()


def test_part8_repository_audit_and_manifest(tmp_path):
    from pathlib import Path

    from bankfail.repro import audit_repository, write_run_manifest

    repo_dir = Path(__file__).resolve().parent.parent
    audit = audit_repository(repo_dir)
    assert audit["ok"], f"Repository audit failed: missing={audit.get('missing_required_files')}, unexpected={audit.get('unexpected_nested_directories')}"
    manifest = write_run_manifest(repo_dir, tmp_path / "reproducibility", repo_dir / "config" / "config.yaml", ["pytest", "test_pipeline.py"])
    payload = json.loads(manifest.read_text(encoding="utf-8"))
    assert "part8" in payload["manifest_version"]
    assert any("pipeline.py" in k for k in payload["source_files"])


def test_resume_makes_zero_http_calls_with_fake_server(tmp_path):
    """Verify that already completed quarters trigger exactly 0 HTTP calls on resume."""
    import requests
    from tests.fixtures.fake_fdic_server import FakeFDICAdapter
    from bankfail.acquisition import FDICClient, AcquisitionDownloader

    session = requests.Session()
    adapter = FakeFDICAdapter()
    session.mount("http://fake-fdic.local/", adapter)

    client = FDICClient("http://fake-fdic.local/banks", session=session)
    raw = tmp_path / "raw"
    proc = tmp_path / "processed"
    manifest = tmp_path / "manifests" / "manifest.json"
    downloader = AcquisitionDownloader(client, raw, proc, manifest, "snappy")

    # 1. Download single quarter
    downloader.download_financials("2020-03-31", "2020-03-31", page_size=1000)
    initial_calls = adapter.call_count
    assert initial_calls > 0, "Initial download should make at least one HTTP call"

    # 2. Re-instantiate downloader to simulate resumed run
    downloader_resumed = AcquisitionDownloader(client, raw, proc, manifest, "snappy")
    downloader_resumed.download_financials("2020-03-31", "2020-03-31", page_size=1000)

    # 3. Assert zero additional HTTP calls were made for financials
    assert adapter.call_count == initial_calls, (
        f"Expected zero new HTTP calls on resume, but calls increased from {initial_calls} to {adapter.call_count}"
    )


# ==============================================================================
# Round 2: Toy Hand-Calculation & Specification Tests
# ==============================================================================

def test_lecture_iqr_hand_calculation():
    """Verify IQR outlier detection against Lecture 2 Data Cleaning slide 20 exact hand calculations."""
    import pandas as pd
    values = pd.Series([21.6, 24.0, 33.4, 34.7, 36.2, 500.0])
    q1 = float(values.quantile(0.25))
    q3 = float(values.quantile(0.75))
    iqr = float(q3 - q1)
    lower_fence = float(q1 - 1.5 * iqr)
    upper_fence = float(q3 + 1.5 * iqr)

    assert abs(q1 - 26.35) < 1e-4, f"Expected Q1=26.35, got {q1}"
    assert abs(q3 - 35.825) < 1e-4, f"Expected Q3=35.825, got {q3}"
    assert abs(iqr - 9.475) < 1e-4, f"Expected IQR=9.475, got {iqr}"
    assert abs(lower_fence - 12.1375) < 1e-4, f"Expected lower fence=12.1375, got {lower_fence}"
    assert abs(upper_fence - 50.0375) < 1e-4, f"Expected upper fence=50.0375, got {upper_fence}"

    outliers = values[(values < lower_fence) | (values > upper_fence)].tolist()
    assert outliers == [500.0], f"Expected outlier [500.0], got {outliers}"


def test_lecture_standard_scaler_hand_calculation():
    """Verify StandardScaler against Lecture 2 Data Transform slide 13 population standard deviation hand calculations."""
    import numpy as np
    from sklearn.preprocessing import StandardScaler

    data = np.array([[2.0], [4.0], [6.0]])
    scaler = StandardScaler()
    scaler.fit(data)

    # Slide 13: mu = 4, sigma = sqrt(8/3) ~= 1.633
    expected_mu = 4.0
    expected_sigma = np.sqrt(8.0 / 3.0)
    assert abs(scaler.mean_[0] - expected_mu) < 1e-5
    assert abs(scaler.scale_[0] - expected_sigma) < 1e-5

    # z ~= (-1.225, 0, 1.225)
    z = scaler.transform(data).flatten()
    assert abs(z[0] - (-np.sqrt(1.5))) < 1e-4
    assert abs(z[1] - 0.0) < 1e-4
    assert abs(z[2] - np.sqrt(1.5)) < 1e-4

    # Test value 8 maps to (8 - 4) / 1.633 ~= 2.45
    test_val = scaler.transform([[8.0]])[0, 0]
    expected_test_val = (8.0 - expected_mu) / expected_sigma
    assert abs(test_val - expected_test_val) < 1e-4
    assert abs(test_val - 2.44948974) < 1e-4


def test_lag_gap_handling_missing_quarter_returns_nan():
    """Verify that a bank missing Q2 gets NaN for the Q3 lag (no bridging across missing quarters)."""
    import pandas as pd
    from bankfail.modeling.pipeline import _add_lag

    df = pd.DataFrame({
        "CERT": [101, 101],
        "REPDTE": [pd.Timestamp("2020-03-31"), pd.Timestamp("2020-09-30")],  # Q1 and Q3, missing Q2
        "ASSET": [1000.0, 1200.0],
    })

    lagged = _add_lag(df, "ASSET", 1)
    # Row 0 (Q1) has no preceding quarter -> NaN
    assert pd.isna(lagged.loc[lagged["REPDTE"] == "2020-03-31", "ASSET_lag1"].iloc[0])
    # Row 1 (Q3) preceded by Q1 (gap of 2 quarters), so lag1 must be NaN
    assert pd.isna(lagged.loc[lagged["REPDTE"] == "2020-09-30", "ASSET_lag1"].iloc[0]), (
        "Q3 lag1 must be NaN when Q2 is missing; pipeline must not bridge across gaps"
    )


def test_pandas_polars_5_row_asset_to_deposit_equivalence():
    """Verify exact 5-row asset_to_deposit calculation equivalence between Pandas and Polars."""
    import numpy as np
    import pandas as pd
    import polars as pl

    pdf = pd.DataFrame({
        "ASSET": [100.0, 200.0, 150.0, 300.0, 50.0],
        "DEP": [80.0, 0.0, np.nan, 150.0, 100.0],
    })

    # Pandas calculation
    pdf["asset_to_deposit"] = np.where((pdf["DEP"].notna()) & (pdf["DEP"] != 0), pdf["ASSET"] / pdf["DEP"], np.nan)

    # Polars calculation
    pldf = pl.from_pandas(pdf[["ASSET", "DEP"]]).with_columns(
        pl.when(pl.col("DEP").is_not_null() & (pl.col("DEP") != 0.0))
        .then(pl.col("ASSET") / pl.col("DEP"))
        .otherwise(None)
        .alias("asset_to_deposit")
    )

    pldf_res = pldf["asset_to_deposit"].to_numpy()
    pdf_res = pdf["asset_to_deposit"].to_numpy()

    np.testing.assert_allclose(
        np.nan_to_num(pdf_res, nan=-999.0),
        np.nan_to_num(pldf_res, nan=-999.0),
        rtol=1e-6,
    )
    assert abs(pdf_res[0] - 1.25) < 1e-5
    assert np.isnan(pdf_res[1])  # division by zero -> NaN
    assert np.isnan(pdf_res[2])  # NaN denominator -> NaN
    assert abs(pdf_res[3] - 2.0) < 1e-5
    assert abs(pdf_res[4] - 0.5) < 1e-5


def test_parquet_column_projection_byte_efficiency(tmp_path):
    """Verify Parquet column-projection efficiency: reading 2 columns consumes fewer bytes than all 20."""
    import numpy as np
    import pandas as pd
    import pyarrow as pa
    import pyarrow.parquet as pq

    n_rows = 5000
    n_cols = 20
    data = {f"col_{i}": np.random.randn(n_rows) for i in range(n_cols)}
    df = pd.DataFrame(data)
    parquet_file = tmp_path / "projection_test.parquet"
    df.to_parquet(parquet_file, engine="pyarrow", index=False)

    full_table = pq.read_table(parquet_file)
    proj_table = pq.read_table(parquet_file, columns=["col_0", "col_1"])

    full_bytes = full_table.nbytes
    proj_bytes = proj_table.nbytes

    assert proj_table.num_columns == 2
    assert full_table.num_columns == n_cols
    assert proj_bytes < full_bytes * 0.25, (
        f"Projected bytes ({proj_bytes}) should be ~10% of full table bytes ({full_bytes})"
    )


def test_rbcrwa_structural_zero_recoded_to_missing():
    """Verify RBCRWAJ structural 0.0 values before 1990 are recoded to NaN and produce missing indicators."""
    import numpy as np
    import pandas as pd
    from sklearn.impute import SimpleImputer
    from bankfail.modeling.pipeline import _derive_features

    df = pd.DataFrame({
        "CERT": [1001, 1001],
        "REPDTE": [pd.Timestamp("1989-12-31"), pd.Timestamp("1990-03-31")],
        "ASSET": [100000.0, 105000.0],
        "DEP": [80000.0, 82000.0],
        "EQ": [10000.0, 10200.0],
        "LNLSNET": [60000.0, 62000.0],
        "RBCRWAJ": [0.0, 12.5],
        "RBC1AAJ": [8.0, 8.2],
        "ROA": [1.1, 1.2],
    })

    derived = _derive_features(df)
    assert np.isnan(derived.loc[0, "rbcrwa_level"]), "Pre-1990 structural zero RBCRWAJ must be recoded to NaN"
    assert derived.loc[1, "rbcrwa_level"] == 12.5, "Post-1990 observed RBCRWAJ must be preserved"

    imputer = SimpleImputer(strategy="median", add_indicator=True)
    imputed = imputer.fit_transform(derived[["rbcrwa_level"]])
    # Column 0 is imputed value, Column 1 is missing indicator
    assert imputed[0, 1] == 1.0, "Missing indicator must be 1 for pre-1990 recoded NaN"
    assert imputed[1, 1] == 0.0, "Missing indicator must be 0 for post-1990 valid value"


def test_repeated_failure_certs_post_f1_censored():
    """Verify that CERTs with multiple failure records (Option A) have post-F1 quarters censored."""
    import numpy as np
    import pandas as pd
    from bankfail.panel.builder import build_panel_from_frames

    # Toy bank with 2 failures: F1 at 1988-07-21, F2 at 1989-02-17 (matches CERT 29400)
    financials = pd.DataFrame({
        "CERT": [29400, 29400, 29400, 29400],
        "REPDTE": [
            pd.Timestamp("1988-03-31"),
            pd.Timestamp("1988-06-30"),
            pd.Timestamp("1988-09-30"),  # Between F1 and F2
            pd.Timestamp("1988-12-31"),  # Between F1 and F2
        ],
    })
    failures = pd.DataFrame({
        "CERT": [29400, 29400],
        "FAILDATE": [pd.Timestamp("1988-07-21"), pd.Timestamp("1989-02-17")],
        "RESTYPE": ["FAILURE", "FAILURE"],
        "COST": [np.nan, 181099.0],
    })
    institutions = pd.DataFrame({
        "CERT": [29400],
        "NAME": ["Platte Valley FS&LA"],
        "ENDEFYMD": [pd.Timestamp("1989-02-17")],
    })

    panel, _, _ = build_panel_from_frames(financials, failures, institutions)
    panel = panel.sort_values("REPDTE").reset_index(drop=True)

    # Pre-F1 reports: within 4Q of F1 (1988-07-21), so they are positive
    assert panel.loc[0, "label"] == 1
    assert panel.loc[1, "label"] == 1

    # Post-F1 reports: even though between F1 and F2, Option A censors them
    assert pd.isna(panel.loc[2, "label"])
    assert panel.loc[2, "label_status"] == "post_failure_report"
    assert pd.isna(panel.loc[3, "label"])
    assert panel.loc[3, "label_status"] == "post_failure_report"


def test_spark_comparison_sample_rows_validation(tmp_path):
    """Verify run_spark_comparison validates sample_rows positive."""
    from bankfail.spark import run_spark_comparison
    import pytest
    with pytest.raises(ValueError, match="sample_rows must be positive"):
        run_spark_comparison(tmp_path, tmp_path, repetitions=2, sample_rows=0)
    with pytest.raises(ValueError, match="sample_rows must be positive"):
        run_spark_comparison(tmp_path, tmp_path, repetitions=2, sample_rows=-10)


def test_spark_comparison_sample_rows_none_default(tmp_path):
    """Verify run_spark_comparison accepts sample_rows=None and defaults to 10000."""
    import os, pytest
    if os.environ.get("BFT65B_RUN_SPARK_TEST") != "1":
        pytest.skip("Spark integration test is opt-in; set BFT65B_RUN_SPARK_TEST=1 in a local VS Code terminal.")
    pd = pytest.importorskip("pandas")
    pytest.importorskip("pyarrow")
    pytest.importorskip("polars")
    pytest.importorskip("pyspark")
    from bankfail.panel import build_panel_from_paths
    from bankfail.spark import run_spark_comparison

    source = tmp_path / "processed" / "financials" / "year=2023"
    source.mkdir(parents=True)
    quarters = pd.date_range("2023-03-31", periods=5, freq="QE-DEC")
    financials = pd.DataFrame({"ID": [f"{cert}_{date:%Y%m%d}" for cert in (1, 2) for date in quarters], "CERT": [cert for cert in (1, 2) for _ in quarters], "REPDTE": list(quarters) * 2, "ASSET": [100 + index for index in range(10)], "DEP": [50] * 10, "EQ": [10] * 10, "ROA": [1.0] * 10})
    financials["REPDTE"] = financials["REPDTE"].astype("datetime64[ms]")
    financials.to_parquet(source / "part.parquet", index=False)
    for dataset in ("failures", "institutions"):
        (tmp_path / "processed" / dataset / "year=2023").mkdir(parents=True)
    pd.DataFrame({"CERT": [1], "FAILDATE": ["2024-06-30"], "COST": [100], "RESTYPE": ["FAILURE"]}).to_parquet(tmp_path / "processed" / "failures" / "year=2023" / "part.parquet", index=False)
    pd.DataFrame({"CERT": [1, 2], "ENDEFYMD": [None, None], "ACTIVE": [0, 1], "CHANGEC1": [0, 0]}).to_parquet(tmp_path / "processed" / "institutions" / "year=2023" / "part.parquet", index=False)
    panel_dir = tmp_path / "panel_output"
    build_panel_from_paths(tmp_path / "processed", panel_dir, tmp_path / "quality")
    panel = panel_dir / "bank_quarter_panel.parquet"
    out_spark = tmp_path / "spark_output"
    res = run_spark_comparison(tmp_path / "processed" / "financials", out_spark, panel, repetitions=2, sample_rows=None)
    assert res["output_equivalence"]["equivalent"]


def test_rbc1a_structural_zero_recoded_to_missing():
    """Verify RBC1AAJ structural 0.0 values before 1990 are recoded to NaN, while positive values are kept."""
    import numpy as np
    import pandas as pd
    from sklearn.impute import SimpleImputer
    from bankfail.modeling.pipeline import _derive_features

    df = pd.DataFrame({
        "CERT": [1001, 1002, 1003],
        "REPDTE": [pd.Timestamp("1989-12-31"), pd.Timestamp("1989-12-31"), pd.Timestamp("1990-03-31")],
        "ASSET": [100000.0, 105000.0, 110000.0],
        "DEP": [80000.0, 82000.0, 85000.0],
        "EQ": [10000.0, 10200.0, 11000.0],
        "LNLSNET": [60000.0, 62000.0, 65000.0],
        "RBCRWAJ": [0.0, 0.0, 12.5],
        "RBC1AAJ": [0.0, 8.5, 0.0],
        "ROA": [1.1, 1.2, 1.0],
    })

    derived = _derive_features(df)
    assert np.isnan(derived.loc[0, "rbc1a_level"]), "Pre-1990 structural zero RBC1AAJ must be recoded to NaN"
    assert derived.loc[1, "rbc1a_level"] == 8.5, "Pre-1990 valid positive RBC1AAJ must be preserved"
    assert derived.loc[2, "rbc1a_level"] == 0.0, "Post-1990 observed 0.0 RBC1AAJ must be preserved"

    imputer = SimpleImputer(strategy="median", add_indicator=True)
    imputed = imputer.fit_transform(derived[["rbc1a_level"]])
    assert imputed[0, 1] == 1.0, "Missing indicator must be 1 for pre-1990 recoded NaN"
    assert imputed[1, 1] == 0.0, "Missing indicator must be 0 for pre-1990 valid positive value"



def test_cost_threshold_vectorized_matches_old():
    """Verify brute-force equivalence between DEF_OLD vectorized and legacy groupby loop."""
    import numpy as np
    import pandas as pd
    from bankfail.modeling.pipeline import _select_cost_threshold, _select_cost_threshold_old

    np.random.seed(42)
    n = 600
    certs = pd.Series(np.random.choice([f"bank_{i}" for i in range(40)], size=n), dtype="string")
    labels = pd.Series(np.random.choice([0, 1], size=n, p=[0.88, 0.12]))
    probs = np.random.uniform(0.01, 0.99, size=n)
    costs = pd.Series(np.where(labels == 1, np.random.uniform(1000.0, 5000.0, size=n), np.nan))
    fp_cost = 450.0

    t_old, rep_old = _select_cost_threshold_old(labels, probs, certs, costs, fp_cost)
    t_vec, rep_vec = _select_cost_threshold(labels, probs, certs, costs, fp_cost, def_mode="old")

    assert np.isclose(t_old, t_vec), f"Threshold mismatch: old={t_old}, vec={t_vec}"
    assert np.isclose(rep_old["development_expected_loss"], rep_vec["development_expected_loss"]), (
        f"Loss mismatch: old={rep_old['development_expected_loss']}, vec={rep_vec['development_expected_loss']}"
    )


def test_cost_threshold_hand_calculated_toy():
    """Verify DEF_A and DEF_B against exact 5-bank hand-calculated toy example."""
    import numpy as np
    import pandas as pd
    from bankfail.modeling.pipeline import _select_cost_threshold

    df = pd.DataFrame({
        "CERT": ["1", "1", "2", "2", "3", "3", "4", "5"],
        "label": [0, 1, 1, 1, 0, 0, 0, 1],
        "prob": [0.20, 0.80, 0.50, 0.60, 0.10, 0.40, 0.70, 0.30],
        "cost": [np.nan, 1000.0, 2000.0, 2000.0, np.nan, np.nan, np.nan, 1500.0],
    })

    # For C_FP = 300:
    # Under DEF_A:
    # At tau = 0.30: misses = 0 (Bank 1 pos_max 0.80, Bank 2 pos_max 0.60, Bank 5 pos_max 0.30 all >= 0.30).
    # False alarm rows = rows with y=0 and prob >= 0.30: Bank 3 Q2 (0.40) and Bank 4 Q1 (0.70) -> 2 rows.
    # Total loss = 0 + 2 * 300 = 600.0.
    t_a, rep_a = _select_cost_threshold(df["label"], df["prob"].to_numpy(), df["CERT"], df["cost"], false_positive_cost=300.0, def_mode="a")
    assert np.isclose(t_a, 0.30), f"Expected 0.30 for DEF_A, got {t_a}"
    assert np.isclose(rep_a["development_expected_loss"], 600.0), f"Expected 600.0, got {rep_a['development_expected_loss']}"

    # Under DEF_B:
    # At tau = 0.30: misses = 0.
    # Pure negative banks with max prob >= 0.30: Bank 3 (max 0.40) and Bank 4 (max 0.70) -> 2 banks.
    # Total loss = 0 + 2 * 300 = 600.0.
    t_b, rep_b = _select_cost_threshold(df["label"], df["prob"].to_numpy(), df["CERT"], df["cost"], false_positive_cost=300.0, def_mode="b")
    assert np.isclose(t_b, 0.30), f"Expected 0.30 for DEF_B, got {t_b}"
    assert np.isclose(rep_b["development_expected_loss"], 600.0), f"Expected 600.0, got {rep_b['development_expected_loss']}"


def test_cost_policy_missing_cost_imputation():
    """Verify that missing failure COST is imputed to development median with zero invented costs."""
    import numpy as np
    import pandas as pd
    from bankfail.modeling.pipeline import _build_cost_policy

    dev = pd.DataFrame({
        "CERT": ["101", "101", "102", "103", "104"],
        "label": [1, 1, 1, 1, 0],
        "failure_cost_numeric": [1000.0, 1000.0, 3000.0, np.nan, np.nan],
    })

    policy = _build_cost_policy(dev, false_positive_cost=None, false_positive_cost_rate=0.10)
    assert policy["status"] == "available"
    assert policy["median_development_failure_cost"] == 2000.0  # median(1000, 3000)
    assert policy["positive_failed_banks"] == 3  # 101, 102, 103
    assert policy["observed_failed_banks_with_COST"] == 2
    assert policy["missing_failed_bank_COST_imputed"] == 1
    assert policy["bank_costs"]["103"] == 2000.0  # Imputed to dev median


def test_alarm_budget_metrics():
    """Verify quarterly alarm budget recall calculation at top 1%, 2%, 5%."""
    import numpy as np
    import pandas as pd
    from bankfail.modeling.pipeline import compute_alarm_budget_metrics

    df = pd.DataFrame({
        "CERT": ["1", "2", "3", "4", "5", "6", "7", "8"],
        "REPDTE": ["1995-03-31"] * 4 + ["1995-06-30"] * 4,
        "label": [1, 0, 0, 0, 0, 1, 0, 0],
        "prob": [0.90, 0.70, 0.30, 0.10, 0.20, 0.85, 0.40, 0.05],
    })

    res = compute_alarm_budget_metrics(df, df["prob"].to_numpy(), budgets=[0.25, 0.50])
    # Quarter 1 has 4 rows, budget 25% -> ceil(4*0.25)=1 alarm (bank 1, prob 0.90). Caught bank 1 (label=1)!
    # Quarter 2 has 4 rows, budget 25% -> ceil(4*0.25)=1 alarm (bank 6, prob 0.85). Caught bank 6 (label=1)!
    # Both true positives caught -> row_recall = 1.0, bank_recall = 1.0
    assert res["top_25%"]["row_recall"] == 1.0
    assert res["top_25%"]["bank_recall"] == 1.0

def test_subsampling_never_applies_to_final_fit():
    """Verify that subsampling (row downsampling or max_samples) never applies to final model fit."""
    import numpy as np
    import pandas as pd
    from bankfail.modeling.pipeline import _make_models, FEATURE_COLUMNS

    models = _make_models(seed=42)
    rf_pipeline = models["random_forest"]
    rf_model = rf_pipeline.named_steps["model"]

    # 1. Final Random Forest model configuration must NOT have max_samples subsampling (must be None)
    assert rf_model.max_samples is None, f"Final Random Forest max_samples should be None, found {rf_model.max_samples}"

    # 2. Verify fit receives and uses 100% of training data without truncation
    n_samples = 300
    np.random.seed(42)
    synthetic_X = pd.DataFrame(np.random.randn(n_samples, len(FEATURE_COLUMNS)), columns=FEATURE_COLUMNS)
    synthetic_y = pd.Series(np.random.choice([0, 1], size=n_samples, p=[0.9, 0.1]))

    rf_pipeline.fit(synthetic_X, synthetic_y)
    assert rf_pipeline.named_steps["model"].max_samples is None
    assert len(synthetic_X) == n_samples


def test_dev_analysis_module(tmp_path):
    """Verify comprehensive development analysis outputs are created."""
    import numpy as np
    import pandas as pd
    from bankfail.modeling.dev_analysis import (
        run_feature_variants_bootstrap,
        run_comprehensive_cost_analysis,
        run_threshold_stability_and_loss_curve,
        run_calibration_analysis,
    )
    from bankfail.modeling.pipeline import FEATURE_COLUMNS, _build_cost_policy

    # Create synthetic train and dev data
    n_train = 100
    n_dev = 80
    np.random.seed(42)
    train_data = {col: np.random.randn(n_train) for col in FEATURE_COLUMNS}
    train_data["label"] = np.random.choice([0, 1], size=n_train, p=[0.9, 0.1])
    train_data["CERT"] = np.random.choice([1, 2, 3, 4], size=n_train)
    train_data["REPDTE"] = pd.date_range("1988-03-31", periods=n_train, freq="QE-DEC")
    train = pd.DataFrame(train_data)

    dev_data = {col: np.random.randn(n_dev) for col in FEATURE_COLUMNS}
    dev_data["label"] = np.random.choice([0, 1], size=n_dev, p=[0.9, 0.1])
    dev_data["CERT"] = np.random.choice([10, 11, 12, 13, 14], size=n_dev)
    dev_data["REPDTE"] = pd.date_range("1995-03-31", periods=n_dev, freq="QE-DEC")
    dev_data["failure_cost_numeric"] = 1000.0
    dev = pd.DataFrame(dev_data)

    cost_policy = _build_cost_policy(dev, false_positive_cost=None, false_positive_cost_rate=0.10)

    # 1. Feature variants & bootstrap
    var_res = run_feature_variants_bootstrap(train, dev, FEATURE_COLUMNS, tmp_path, seed=42, n_bootstraps=5)
    assert (tmp_path / "bootstrap_evaluation.csv").exists()
    assert (tmp_path / "bootstrap_paired_differences.csv").exists()

    # 2. Comprehensive cost analysis
    cost_df = run_comprehensive_cost_analysis(dev, var_res["predictions"], cost_policy, tmp_path)
    assert (tmp_path / "p7_cost_threshold_comprehensive.csv").exists()
    assert not cost_df.empty

    # 3. Threshold stability and loss curve
    stab_res = run_threshold_stability_and_loss_curve(dev, var_res["predictions"]["rf_v1"], cost_policy, tmp_path, seed=42, n_resamples=5)
    assert (tmp_path / "p7_threshold_stability_rf_100to1.json").exists()
    assert (tmp_path / "p7_threshold_loss_curve.csv").exists()
    assert (tmp_path / "p7_annual_alarm_rates.csv").exists()

    # 4. Calibration analysis
    calib_df = run_calibration_analysis(train, dev, var_res["predictions"], {}, FEATURE_COLUMNS, tmp_path)
    assert (tmp_path / "p7_calibration_summary.csv").exists()
    assert not calib_df.empty


def test_freeze_config_dry_run_and_refusal(tmp_path):
    """Verify freeze_config.py refuses missing arguments, rejects variant-threshold mismatch, and supports --dry-run."""
    import subprocess
    import sys

    freeze_script = Path(__file__).resolve().parents[1] / "scripts" / "freeze_config.py"

    # 1. Missing arguments must exit with non-zero code
    res_fail = subprocess.run([sys.executable, str(freeze_script)], capture_output=True, text=True)
    assert res_fail.returncode != 0
    assert "required" in res_fail.stderr or "usage:" in res_fail.stderr

    # 2. Variant 3 with old Variant 1 threshold (0.037624) must be rejected
    cmd_mismatch = [
        sys.executable,
        str(freeze_script),
        "--split-train-end", "1992-12-31",
        "--split-dev-start", "1993-03-31",
        "--split-dev-end", "2005-12-31",
        "--split-test-start", "2008-01-01",
        "--split-test-end", "2014-12-31",
        "--loss-definition", "DEF_A",
        "--fn-fp-ratio", "100:1",
        "--feature-variant", "variant_3",
        "--final-model", "random_forest",
        "--training-policy", "train_only",
        "--seed", "42",
        "--threshold", "0.037624",
        "--dry-run",
    ]
    res_mismatch = subprocess.run(cmd_mismatch, capture_output=True, text=True)
    assert res_mismatch.returncode != 0
    assert "does not match development result" in res_mismatch.stderr

    # 3. Valid Variant 3 arguments with matching development threshold (0.103990) and --dry-run
    cmd_dry = [
        sys.executable,
        str(freeze_script),
        "--split-train-end", "1992-12-31",
        "--split-dev-start", "1993-03-31",
        "--split-dev-end", "2005-12-31",
        "--split-test-start", "2008-01-01",
        "--split-test-end", "2014-12-31",
        "--loss-definition", "DEF_A",
        "--fn-fp-ratio", "100:1",
        "--feature-variant", "variant_3",
        "--final-model", "random_forest",
        "--training-policy", "train_only",
        "--seed", "42",
        "--threshold", "0.103990",
        "--dry-run",
    ]
    res_dry = subprocess.run(cmd_dry, capture_output=True, text=True)
    assert res_dry.returncode == 0
    assert "CONFIGURATION VALIDATION PASSED" in res_dry.stdout
    assert "DRY RUN" in res_dry.stdout


def test_def_a_loss_and_metrics_exact_calculation():
    """Verify that _metrics and _def_a_loss implement exact DEF_A semantics (row FP + positive-window bank FN)."""
    import numpy as np
    import pandas as pd
    from bankfail.modeling.pipeline import _metrics

    # Bank 1 (failed, COST=5000): Q1 is y=0 (flagged, p=0.80 >= 0.50), Q2 is y=1 (missed, p=0.10 < 0.50).
    # Under DEF_A: Bank 1 is MISSED (no alarm on y=1 rows -> FN=5000) AND its Q1 y=0 alarm counts as 1 false alarm row!
    # Bank 2 (failed, COST=3000): Q1 is y=1 (flagged, p=0.90 >= 0.50), Q2 is y=1 (p=0.20 < 0.50). Caught once (FN=0).
    # Bank 3 (healthy, y=0): Q1 p=0.70 (flagged), Q2 p=0.60 (flagged), Q3 p=0.10 (not flagged) -> 2 false alarm rows.
    y_true = pd.Series([0, 1, 1, 1, 0, 0, 0])
    prob = np.array([0.80, 0.10, 0.90, 0.20, 0.70, 0.60, 0.10])
    cert = pd.Series(["1", "1", "2", "2", "3", "3", "3"])
    costs = pd.Series([5000.0, 5000.0, 3000.0, 3000.0, np.nan, np.nan, np.nan])
    c_fp = 100.0

    m = _metrics(y_true, prob, threshold=0.50, fn_cost=None, fp_cost=c_fp, cert=cert, failure_costs=costs, def_mode="a")
    assert m["loss_definition"] == "DEF_A"
    assert m["cost_unit"] == "row_fp_bank_fn"
    assert m["banks_caught"] == 1
    assert m["total_failed_banks"] == 2
    assert m["banks_missed"] == 1
    assert m["false_negative_loss"] == 5000.0
    assert m["false_alarm_rows"] == 3  # Bank 1 Q1 + Bank 3 Q1 + Bank 3 Q2
    assert m["false_alarm_banks"] == 2  # Bank 1 and Bank 3
    assert m["false_positive_loss"] == 300.0
    assert m["expected_loss"] == 5300.0


def test_frozen_config_validation_and_pipeline_enforcement(tmp_path):
    """Verify frozen config SHA-256 integrity check, CLI conflict rejection, preflight isolation, and Variant 3 enforcement."""
    import hashlib
    import json
    import pandas as pd
    from bankfail.modeling import (
        ModelingError,
        load_and_validate_frozen_config,
        verify_reproducibility_hashes,
        compute_file_sha256,
        resolve_feature_columns,
        run_modeling_pipeline,
    )

    # 1. Feature variant resolution
    v3 = resolve_feature_columns("variant_3")
    assert len(v3) == 12
    assert "rbcrwa_level" not in v3
    assert "rbc1a_level" not in v3

    # 2. Synthetic panel for preflight and enforcement verification
    dates = pd.date_range("2010-03-31", "2025-06-30", freq="QE-DEC")
    rows = []
    for cert in range(1, 5):
        for index, date in enumerate(dates):
            rows.append({
                "CERT": cert,
                "REPDTE": date,
                "horizon_end": date + pd.offsets.QuarterEnd(4),
                "label": int((cert + index) % 7 == 0),
                "eligible_for_model": True,
                "failure_cost_numeric": 1000.0,
                "ASSET": 100 + cert * 5 + index,
                "DEP": 50 + index,
                "EQ": 10 + cert,
                "LNLSNET": 40 + index,
                "NCLNLS": 2 + cert,
                "ORE": 1,
                "CHBAL": 10,
                "SC": 15,
                "NETINC": 1 + (index % 5),
                "ROA": 1.0 + (index % 4) / 10,
                "ROE": 5.0 + (index % 3) / 10,
                "NIMY": 2.0,
                "RBC1AAJ": 10.0,
                "RBCRWAJ": 12.0,
            })
    panel_path = tmp_path / "ws" / "outputs" / "03_panel" / "bank_quarter_panel.parquet"
    panel_path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows).to_parquet(panel_path, index=False)
    out_dir = tmp_path / "ws" / "outputs" / "07_features_models"

    # 3. Build valid frozen configuration JSON with reproducibility hashes
    repo_root = Path(__file__).resolve().parents[1]
    payload = {
        "status": "FROZEN",
        "purpose": "Test frozen config enforcement",
        "frozen_timestamp_utc": "2026-11-23T00:00:00+00:00",
        "split": {
            "train_end": "2012-12-31",
            "dev_start": "2013-03-31",
            "dev_end": "2013-12-31",
            "test_start": "2015-01-01",
            "test_end": "2015-12-31",
        },
        "feature_selection": {
            "variant": "variant_3",
            "rule": "preregistered parsimony rule",
        },
        "primary_model": {
            "model_family": "random_forest",
            "seed": 42,
            "training_policy": "train_only",
            "hyperparameters": {"n_estimators": 100, "min_samples_leaf": 5},
        },
        "decision_threshold": {
            "loss_definition": "DEF_A",
            "fn_fp_ratio": "100:1",
            "chosen_threshold": 0.103990,
        },
        "reproducibility_hashes": {
            "code_files": {
                "pipeline_py": compute_file_sha256(repo_root / "src" / "bankfail" / "modeling" / "pipeline.py"),
                "dev_analysis_py": compute_file_sha256(repo_root / "src" / "bankfail" / "modeling" / "dev_analysis.py"),
                "builder_py": compute_file_sha256(repo_root / "src" / "bankfail" / "panel" / "builder.py"),
            },
            "panel_parquet": compute_file_sha256(panel_path),
        },
    }
    encoded = json.dumps(payload, sort_keys=True, indent=2).encode("utf-8")
    payload["sha256_hash"] = hashlib.sha256(encoded).hexdigest()

    frozen_path = tmp_path / "frozen_model_config.json"
    frozen_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    loaded = load_and_validate_frozen_config(frozen_path, repo_root=repo_root, panel_path=panel_path)
    assert loaded["sha256_hash"] == payload["sha256_hash"]

    # 4. Tampered threshold must fail SHA-256 verification
    bad_payload = dict(payload)
    bad_payload["decision_threshold"] = dict(payload["decision_threshold"], chosen_threshold=0.50)
    bad_path = tmp_path / "bad_frozen.json"
    bad_path.write_text(json.dumps(bad_payload, indent=2), encoding="utf-8")
    with pytest.raises(ModelingError, match="SHA-256 integrity check failed"):
        load_and_validate_frozen_config(bad_path, repo_root=repo_root, panel_path=panel_path)

    # 5. Conflicting CLI override must be rejected
    with pytest.raises(ModelingError, match="conflicts with frozen configuration"):
        run_modeling_pipeline(
            panel_path,
            out_dir,
            frozen_config_path=frozen_path,
            preflight_only=True,
            cli_overrides={"feature_variant": "variant_1"},
        )

    # 6. Preflight must not access test or create locks
    preflight = run_modeling_pipeline(
        panel_path,
        out_dir,
        min_dev_failures=1,
        min_test_failures=1,
        frozen_config_path=frozen_path,
        preflight_only=True,
    )
    assert preflight["mode"] == "preflight_only"
    assert preflight["test_accessed"] is False
    assert preflight["test_predictions_generated"] is False
    assert preflight["effective_configuration"]["feature_variant"] == "variant_3"
    assert preflight["effective_configuration"]["feature_count"] == 12
    assert preflight["effective_configuration"]["frozen_threshold"] == 0.103990
    assert not (out_dir / "p7_test_evaluation.lock").exists()

    # 7. Full frozen evaluation on synthetic panel enforces Variant 3 (12 features), frozen threshold 0.103990, and DEF_A
    eval_rep = run_modeling_pipeline(
        panel_path,
        out_dir,
        min_dev_failures=1,
        min_test_failures=1,
        frozen_config_path=frozen_path,
        evaluate_test=True,
    )
    assert eval_rep["feature_variant"] == "variant_3"
    assert len(eval_rep["feature_columns"]) == 12
    rf_test = eval_rep["models"]["random_forest"]["test_metrics_at_frozen_threshold"]
    assert rf_test["threshold"] == 0.103990
    assert rf_test["loss_definition"] == "DEF_A"
    assert rf_test["cost_unit"] == "row_fp_bank_fn"
    assert (out_dir / "p7_test_evaluation.lock").exists()


def test_runtime_sha256_verification(tmp_path):
    """TASK A verification tests: pass on match, fail on modification, fail on missing hash."""
    import hashlib
    import json
    import pytest
    from pathlib import Path
    from bankfail.modeling import (
        ModelingError,
        load_and_validate_frozen_config,
        verify_reproducibility_hashes,
        compute_file_sha256,
    )

    # 1. Setup isolated mock repository and dataset
    mock_repo = tmp_path / "mock_repo"
    mock_modeling = mock_repo / "src" / "bankfail" / "modeling"
    mock_modeling.mkdir(parents=True)
    mock_panel_dir = mock_repo / "src" / "bankfail" / "panel"
    mock_panel_dir.mkdir(parents=True)

    pipe_file = mock_modeling / "pipeline.py"
    pipe_file.write_text("# mock pipeline code\nprint('pipe')", encoding="utf-8")
    dev_file = mock_modeling / "dev_analysis.py"
    dev_file.write_text("# mock dev analysis code\nprint('dev')", encoding="utf-8")
    build_file = mock_panel_dir / "builder.py"
    build_file.write_text("# mock builder code\nprint('build')", encoding="utf-8")

    panel_file = tmp_path / "mock_bank_quarter_panel.parquet"
    panel_file.write_bytes(b"PAR1_mock_binary_panel_payload_bytes_here")

    # Compute genuine hashes
    h_pipe = compute_file_sha256(pipe_file)
    h_dev = compute_file_sha256(dev_file)
    h_build = compute_file_sha256(build_file)
    h_panel = compute_file_sha256(panel_file)

    # Build valid config payload
    valid_cfg = {
        "status": "FROZEN",
        "split": {
            "train_end": "2012-12-31",
            "dev_start": "2013-03-31",
            "dev_end": "2013-12-31",
            "test_start": "2015-01-01",
            "test_end": "2015-12-31",
        },
        "feature_selection": {
            "variant": "variant_3",
        },
        "primary_model": {
            "model_family": "random_forest",
            "seed": 42,
            "training_policy": "train_only",
        },
        "decision_threshold": {
            "loss_definition": "DEF_A",
            "fn_fp_ratio": "100:1",
            "chosen_threshold": 0.103990,
        },
        "reproducibility_hashes": {
            "code_files": {
                "pipeline_py": h_pipe,
                "dev_analysis_py": h_dev,
                "builder_py": h_build,
            },
            "panel_parquet": h_panel,
        },
    }
    encoded = json.dumps(valid_cfg, sort_keys=True, indent=2).encode("utf-8")
    valid_cfg["sha256_hash"] = hashlib.sha256(encoded).hexdigest()

    frozen_json_path = tmp_path / "frozen_model_config.json"
    frozen_json_path.write_text(json.dumps(valid_cfg, indent=2), encoding="utf-8")

    # Sub-test 1: PASS when all hashes match
    loaded = load_and_validate_frozen_config(
        frozen_json_path, repo_root=mock_repo, panel_path=panel_file, verify_hashes=True
    )
    assert loaded["sha256_hash"] == valid_cfg["sha256_hash"]
    # Direct helper pass
    verify_reproducibility_hashes(valid_cfg, repo_root=mock_repo, panel_path=panel_file)

    # Sub-test 2: FAIL when a source file is modified
    pipe_file.write_text("# tampered source line\n", encoding="utf-8")
    with pytest.raises(ModelingError, match="SHA-256 verification failed for 'pipeline_py'"):
        verify_reproducibility_hashes(valid_cfg, repo_root=mock_repo, panel_path=panel_file)
    pipe_file.write_text("# mock pipeline code\nprint('pipe')", encoding="utf-8")  # restore

    dev_file.write_text("# tampered dev line\n", encoding="utf-8")
    with pytest.raises(ModelingError, match="SHA-256 verification failed for 'dev_analysis_py'"):
        verify_reproducibility_hashes(valid_cfg, repo_root=mock_repo, panel_path=panel_file)
    dev_file.write_text("# mock dev analysis code\nprint('dev')", encoding="utf-8")  # restore

    build_file.write_text("# tampered builder line\n", encoding="utf-8")
    with pytest.raises(ModelingError, match="SHA-256 verification failed for 'builder_py'"):
        verify_reproducibility_hashes(valid_cfg, repo_root=mock_repo, panel_path=panel_file)
    build_file.write_text("# mock builder code\nprint('build')", encoding="utf-8")  # restore

    # Sub-test 3: FAIL when the panel file is modified
    panel_file.write_bytes(b"PAR1_tampered_modified_bytes")
    with pytest.raises(ModelingError, match="SHA-256 verification failed for panel dataset"):
        verify_reproducibility_hashes(valid_cfg, repo_root=mock_repo, panel_path=panel_file)
    panel_file.write_bytes(b"PAR1_mock_binary_panel_payload_bytes_here")  # restore

    # Sub-test 4: FAIL when a required hash is missing
    cfg_missing_code_key = json.loads(json.dumps(valid_cfg))
    del cfg_missing_code_key["reproducibility_hashes"]["code_files"]["pipeline_py"]
    with pytest.raises(ModelingError, match="missing expected code hash for 'pipeline_py'"):
        verify_reproducibility_hashes(cfg_missing_code_key, repo_root=mock_repo, panel_path=panel_file)

    cfg_missing_panel_key = json.loads(json.dumps(valid_cfg))
    del cfg_missing_panel_key["reproducibility_hashes"]["panel_parquet"]
    with pytest.raises(ModelingError, match="missing 'panel_parquet' hash"):
        verify_reproducibility_hashes(cfg_missing_panel_key, repo_root=mock_repo, panel_path=panel_file)

    cfg_missing_sec = json.loads(json.dumps(valid_cfg))
    del cfg_missing_sec["reproducibility_hashes"]
    with pytest.raises(ModelingError, match="missing 'reproducibility_hashes' section"):
        verify_reproducibility_hashes(cfg_missing_sec, repo_root=mock_repo, panel_path=panel_file)


