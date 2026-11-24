from __future__ import annotations

import logging
import datetime as dt
from pathlib import Path
from typing import Any

from bankfail.acquisition.downloader import AcquisitionDownloader, FAILURE_FIELDS, INSTITUTION_FIELDS
from bankfail.acquisition.client import FDICClient


def load_config(path: Path) -> dict[str, Any]:
    try:
        import yaml
    except ImportError as exc:
        raise RuntimeError("PyYAML is required. Run: python -m pip install -r requirements.txt") from exc
    config = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(config, dict):
        raise ValueError("Configuration must be a YAML mapping")
    for key in ("start_date", "end_date"):
        value = config.get(key)
        if isinstance(value, (dt.date, dt.datetime)):
            config[key] = value.isoformat()
        elif value is not None:
            config[key] = str(value)
    base = path.resolve().parent
    root = base.parent if base.name == "config" else base
    for name, value in config.get("paths", {}).items():
        candidate = Path(value)
        if not candidate.is_absolute():
            config["paths"][name] = str((root / candidate).resolve())
    return config


def setup_logging(log_path: Path) -> None:
    log_path.parent.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s - %(message)s", handlers=[logging.FileHandler(log_path, encoding="utf-8"), logging.StreamHandler()])


def run(config_path: Path, start_date: str | None = None, end_date: str | None = None) -> None:
    config = load_config(config_path)
    paths = config["paths"]
    setup_logging(Path(paths["logs"]) / "acquisition.log")
    client = FDICClient(config["api_base"], int(config["timeout_seconds"]), int(config["max_retries"]), float(config["backoff_seconds"]))
    downloader = AcquisitionDownloader(client, Path(paths["raw"]), Path(paths["processed"]), Path(paths["manifests"]) / "acquisition_manifest.json", config["compression"])
    downloader.download_financials(start_date or config["start_date"], end_date or config["end_date"], int(config["page_size"]))
    downloader.download_small_table("failures", FAILURE_FIELDS, "FAILDATE", int(config["page_size"]))
    downloader.download_small_table("institutions", INSTITUTION_FIELDS, "CERT", int(config["page_size"]))
