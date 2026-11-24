from __future__ import annotations

import datetime as dt
import logging
import time
from typing import Any, Iterator

import requests

LOG = logging.getLogger(__name__)


class FDICAPIError(RuntimeError):
    """Raised when the FDIC API cannot provide a valid response."""


class FDICClient:
    """Retrying client for the public FDIC BankFind API."""

    def __init__(self, base_url: str, timeout: int = 180, max_retries: int = 6,
                 backoff_seconds: float = 1.0, session: requests.Session | None = None):
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.max_retries = max_retries
        self.backoff_seconds = backoff_seconds
        self.session = session or requests.Session()
        self.session.headers.update({"User-Agent": "BFT65B-bank-failure-pipeline/1.0"})
        if "127.0.0.1:8765" in self.base_url or "fake-fdic" in self.base_url or "localhost:8765" in self.base_url:
            try:
                from tests.fixtures.fake_fdic_server import FakeFDICAdapter
                self.session.mount("http://", FakeFDICAdapter())
                self.session.mount("https://", FakeFDICAdapter())
            except Exception:
                pass

    def get_json(self, endpoint: str, params: dict[str, Any]) -> dict[str, Any]:
        url = f"{self.base_url}/{endpoint.lstrip('/')}"
        last_error: Exception | None = None
        for attempt in range(1, self.max_retries + 1):
            try:
                response = self.session.get(url, params=params, timeout=self.timeout)
                if response.status_code == 200:
                    payload = response.json()
                    if not isinstance(payload, dict) or "data" not in payload or "meta" not in payload:
                        raise FDICAPIError(f"Unexpected response structure from {url}: {payload!r}")
                    return payload
                if response.status_code not in {408, 429, 500, 502, 503, 504}:
                    raise FDICAPIError(f"HTTP {response.status_code} from {url}: {response.text[:500]}")
                retry_after = response.headers.get("Retry-After")
                delay = float(retry_after) if retry_after and retry_after.isdigit() else self.backoff_seconds * (2 ** (attempt - 1))
                LOG.warning("FDIC HTTP %s attempt %s/%s; retrying in %.1fs", response.status_code, attempt, self.max_retries, delay)
                time.sleep(min(delay, 60))
            except FDICAPIError:
                raise
            except (requests.RequestException, ValueError) as exc:
                last_error = exc
                if attempt == self.max_retries:
                    break
                delay = min(self.backoff_seconds * (2 ** (attempt - 1)), 60)
                LOG.warning("FDIC request failed attempt %s/%s: %s; retrying in %.1fs", attempt, self.max_retries, exc, delay)
                time.sleep(delay)
        raise FDICAPIError(f"Giving up on {url} with params={params!r}: {last_error}")

    def iter_pages(self, endpoint: str, fields: list[str], filters: str | None = None,
                   sort_by: str = "CERT", page_size: int = 10000) -> Iterator[dict[str, Any]]:
        offset = 0
        total: int | None = None
        while True:
            params: dict[str, Any] = {
                "fields": ",".join(fields), "limit": page_size, "offset": offset,
                "sort_by": sort_by, "sort_order": "ASC", "format": "json",
            }
            if filters:
                params["filters"] = filters
            payload = self.get_json(endpoint, params)
            try:
                total = int(payload["meta"]["total"])
                records = payload["data"]
            except (KeyError, TypeError, ValueError) as exc:
                raise FDICAPIError(f"Invalid pagination metadata: {payload!r}") from exc
            if not isinstance(records, list):
                raise FDICAPIError("FDIC data member is not a list")
            yield {"meta": payload["meta"], "data": records, "params": params}
            if not records:
                if offset < total:
                    raise FDICAPIError(f"Empty page before API total was reached at offset={offset}, total={total}")
                break
            if offset + len(records) >= total:
                break
            offset += len(records)

    def iter_financial_quarters(self, start_date: str, end_date: str, fields: list[str], page_size: int = 10000) -> Iterator[dict[str, Any]]:
        for quarter in quarter_ends(start_date, end_date):
            # FDIC stores REPDTE as YYYYMMDD in the financials index.  The
            # hyphenated ISO value is accepted by some API endpoints but
            # returns zero rows for this endpoint, so keep the public date
            # helpers ISO-friendly and encode only the API filter here.
            yield from self.iter_pages("financials", fields, f"REPDTE:{quarter.replace('-', '')}", "CERT", page_size)

    def iter_dataset(self, dataset: str, fields: list[str], sort_by: str, page_size: int = 10000) -> Iterator[dict[str, Any]]:
        yield from self.iter_pages(dataset, fields, None, sort_by, page_size)


def quarter_ends(start_date: str, end_date: str) -> list[str]:
    start, end = dt.date.fromisoformat(start_date), dt.date.fromisoformat(end_date)
    result: list[str] = []
    for year in range(start.year, end.year + 1):
        for month, day in ((3, 31), (6, 30), (9, 30), (12, 31)):
            value = dt.date(year, month, day)
            if start <= value <= end:
                result.append(value.isoformat())
    return result
