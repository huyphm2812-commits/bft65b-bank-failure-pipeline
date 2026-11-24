"""Acquisition package."""
from bankfail.acquisition.client import FDICAPIError, FDICClient, quarter_ends
from bankfail.acquisition.storage import (
    DATASET_SCHEMA_DEFINITIONS,
    parse_fdic_dates,
    sha256,
    write_parquet_page,
    write_raw_page,
)
from bankfail.acquisition.downloader import AcquisitionDownloader, FAILURE_FIELDS, INSTITUTION_FIELDS
from bankfail.acquisition.validation import validate_dataset, write_validation_report
from bankfail.acquisition.pipeline import load_config, run, setup_logging

__all__ = [
    "FDICAPIError", "FDICClient", "quarter_ends",
    "DATASET_SCHEMA_DEFINITIONS", "parse_fdic_dates", "sha256",
    "write_parquet_page", "write_raw_page",
    "AcquisitionDownloader", "FAILURE_FIELDS", "INSTITUTION_FIELDS",
    "validate_dataset", "write_validation_report",
    "load_config", "run", "setup_logging",
]
