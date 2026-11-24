from __future__ import annotations

import datetime as dt
import json
import logging
from pathlib import Path
from typing import Any

from bankfail.acquisition.client import FDICClient, quarter_ends
from bankfail.acquisition.storage import sha256, write_parquet_page, write_raw_page

LOG = logging.getLogger(__name__)

FINANCIAL_FIELDS = ["CERT", "REPDTE", "STNAME", "ASSET", "DEP", "DEPDOM", "EQ", "BRO", "CHBAL", "SC", "LNLSGR", "LNLSNET", "LNRE", "LNCI", "LNCON", "LNATRES", "NCLNLS", "P3ASSET", "P9ASSET", "NAASSET", "NTLNLS", "ORE", "NETINC", "INTINC", "EINTEXP", "NONII", "NONIX", "ELNATR", "ROA", "ROE", "NIMY", "RBC1AAJ", "RBCRWAJ"]
FAILURE_FIELDS = ["CERT", "NAME", "CITYST", "FAILDATE", "FAILYR", "COST", "QBFASSET", "QBFDEP", "RESTYPE"]
INSTITUTION_FIELDS = ["CERT", "NAME", "STNAME", "BKCLASS", "ACTIVE", "ESTYMD", "ENDEFYMD", "CHANGEC1", "INSDATE"]


class AcquisitionDownloader:
    def __init__(self, client: FDICClient, raw_root: Path, processed_root: Path, manifest_path: Path, compression: str = "snappy"):
        self.client, self.raw_root, self.processed_root = client, raw_root, processed_root
        self.manifest_path, self.compression = manifest_path, compression
        self.manifest_path.parent.mkdir(parents=True, exist_ok=True)
        self.manifest: dict[str, Any] = json.loads(self.manifest_path.read_text(encoding="utf-8")) if self.manifest_path.exists() else {"version": 1, "chunks": {}}

    def _save_manifest(self) -> None:
        temp = self.manifest_path.with_suffix(".json.part")
        temp.write_text(json.dumps(self.manifest, indent=2, sort_keys=True), encoding="utf-8")
        temp.replace(self.manifest_path)

    @staticmethod
    def _file_matches(path_value: str | None, expected_sha256: str | None) -> bool:
        path = Path(path_value) if path_value else None
        return bool(path and path.exists() and expected_sha256 and sha256(path) == expected_sha256)

    def _query_is_complete(self, dataset: str, filter_value: str) -> bool:
        """Return True only after checking every manifest file and API total.

        This check intentionally runs before creating/advancing the API page
        generator.  A resumed completed quarter therefore makes zero HTTP
        calls.  Old manifests without ``api_total`` are not trusted and will
        be refreshed once.
        """
        prefix = f"{dataset}/{filter_value}/"
        chunks = [
            chunk for key, chunk in self.manifest.get("chunks", {}).items()
            if key.startswith(prefix)
        ]
        if not chunks or any(chunk.get("status") != "complete" for chunk in chunks):
            return False
        if any(not self._file_matches(chunk.get("raw_path"), chunk.get("raw_sha256")) for chunk in chunks):
            return False
        if any(not self._file_matches(chunk.get("parquet_path"), chunk.get("parquet", {}).get("sha256")) for chunk in chunks):
            return False
        totals = {chunk.get("api_total") for chunk in chunks}
        if len(totals) != 1 or None in totals:
            return False
        return sum(int(chunk.get("record_count", -1)) for chunk in chunks) == int(next(iter(totals)))

    def _download_pages(self, dataset: str, pages, query_context: dict[str, Any], partition_by_year: bool = False) -> None:
        for page_no, page in enumerate(pages):
            filter_value = page["params"].get("filters", "all")
            key = f"{dataset}/{filter_value}/page-{page_no:06d}"
            records = page["data"]
            raw_path = self.raw_root / dataset / f"{filter_value.replace(':', '_')}-page-{page_no:06d}.json.gz"
            existing = self.manifest["chunks"].get(key)
            if existing and existing.get("status") == "complete" and self._file_matches(existing.get("raw_path"), existing.get("raw_sha256")) and self._file_matches(existing.get("parquet_path"), existing.get("parquet", {}).get("sha256")):
                LOG.info("Skipping completed %s", key)
                continue
            if existing and existing.get("status") == "complete":
                LOG.warning("Checksum mismatch; refreshing %s", key)
            write_raw_page(raw_path, page)
            if partition_by_year and records:
                year = str(pd_year(records[0].get("data", records[0]).get("REPDTE")))
                parquet_path = self.processed_root / dataset / f"year={year}" / f"part-{filter_value.replace(':', '_')}-{page_no:06d}.parquet"
            else:
                parquet_path = self.processed_root / dataset / f"part-{filter_value.replace(':', '_')}-{page_no:06d}.parquet"
            parquet_info = write_parquet_page(parquet_path, records, dataset, self.compression)
            self.manifest["chunks"][key] = {
                "dataset": dataset, "status": "complete", "query": query_context,
                "params": page["params"], "page": page_no, "record_count": len(records),
                "api_total": int(page.get("meta", {}).get("total", len(records))),
                "meta": page.get("meta", {}),
                "downloaded_at_utc": dt.datetime.now(dt.timezone.utc).isoformat(),
                "raw_path": str(raw_path), "raw_size": raw_path.stat().st_size, "raw_sha256": sha256(raw_path),
                "parquet_path": str(parquet_path), "parquet": parquet_info,
            }
            self._save_manifest()
            LOG.info("Completed %s: %s records", key, len(records))

    def download_financials(self, start_date: str, end_date: str, page_size: int) -> None:
        for quarter in quarter_ends(start_date, end_date):
            filter_value = f"REPDTE:{quarter.replace('-', '')}"
            if self._query_is_complete("financials", filter_value):
                LOG.info("Skipping completed quarter %s before any HTTP request", quarter)
                continue
            pages = self.client.iter_pages("financials", FINANCIAL_FIELDS, filter_value, "CERT", page_size)
            self._download_pages("financials", pages, {"quarter": quarter, "filter": filter_value, "fields": FINANCIAL_FIELDS}, True)

    def download_small_table(self, dataset: str, fields: list[str], sort_by: str, page_size: int) -> None:
        if self._query_is_complete(dataset, "all"):
            LOG.info("Skipping completed %s before any HTTP request", dataset)
            return
        self._download_pages(dataset, self.client.iter_dataset(dataset, fields, sort_by, page_size), {"fields": fields, "sort_by": sort_by})


def pd_year(value: Any) -> int:
    import pandas as pd
    return int(pd.to_datetime(value).year)
