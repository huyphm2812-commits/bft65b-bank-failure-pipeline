from __future__ import annotations

import json
import re
import sys
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
from io import BytesIO
from typing import Any
from urllib.parse import parse_qs, urlparse

import requests
from requests.adapters import BaseAdapter
from requests.models import Response

FAILURES_DATA = [
    {"CERT": None, "FAILDATE": "1955-04-12", "NAME": "Historic Pre-Call Bank", "CITYST": "Chicago, IL", "FAILYR": 1955, "RESTYPE": "FAILURE", "COST": 10.0, "QBFASSET": 50.0, "QBFDEP": 45.0},
    {"CERT": None, "FAILDATE": "1965-11-20", "NAME": "Historic Thrift", "CITYST": "Denver, CO", "FAILYR": 1965, "RESTYPE": "FAILURE", "COST": 15.0, "QBFASSET": 70.0, "QBFDEP": 60.0},
    {"CERT": 2, "FAILDATE": "2021-03-31", "NAME": "Second State Bank", "CITYST": "Houston, TX", "FAILYR": 2021, "RESTYPE": "FAILURE", "COST": 20000.0, "QBFASSET": 50000.0, "QBFDEP": 45000.0},
    {"CERT": 3, "FAILDATE": "2022-09-30", "NAME": "Dev Target Bank", "CITYST": "Austin, TX", "FAILYR": 2022, "RESTYPE": "FAILURE", "COST": 150000.0, "QBFASSET": 80000.0, "QBFDEP": 70000.0},
    {"CERT": 4, "FAILDATE": "2024-06-30", "NAME": "Test Target Bank", "CITYST": "Miami, FL", "FAILYR": 2024, "RESTYPE": "FAILURE", "COST": 250000.0, "QBFASSET": 90000.0, "QBFDEP": 80000.0},
    {"CERT": 5, "FAILDATE": "2021-06-30", "NAME": "Assisted Metro Bank", "CITYST": "New York, NY", "FAILYR": 2021, "RESTYPE": "ASSISTANCE", "COST": 50000.0, "QBFASSET": 40000.0, "QBFDEP": 35000.0},
    {"CERT": 5, "FAILDATE": "1990-01-15", "NAME": "Assisted Metro Bank Prior", "CITYST": "New York, NY", "FAILYR": 1990, "RESTYPE": "ASSISTANCE", "COST": 10000.0, "QBFASSET": 20000.0, "QBFDEP": 18000.0},
]

INSTITUTIONS_DATA = [
    {"CERT": 1, "NAME": "First National Bank", "STNAME": "Texas", "BKCLASS": "N", "ACTIVE": 1, "ESTYMD": "1980-01-01", "ENDEFYMD": "9999-12-31", "CHANGEC1": 0.0, "INSDATE": "1980-01-01"},
    {"CERT": 2, "NAME": "Second State Bank", "STNAME": "Texas", "BKCLASS": "SM", "ACTIVE": 0, "ESTYMD": "1980-01-01", "ENDEFYMD": "2021-03-31", "CHANGEC1": 0.0, "INSDATE": "1980-01-01"},
    {"CERT": 3, "NAME": "Dev Target Bank", "STNAME": "Texas", "BKCLASS": "NM", "ACTIVE": 0, "ESTYMD": "1980-01-01", "ENDEFYMD": "2022-09-30", "CHANGEC1": 0.0, "INSDATE": "1980-01-01"},
    {"CERT": 4, "NAME": "Test Target Bank", "STNAME": "Florida", "BKCLASS": "NM", "ACTIVE": 0, "ESTYMD": "1980-01-01", "ENDEFYMD": "2024-06-30", "CHANGEC1": 0.0, "INSDATE": "1980-01-01"},
    {"CERT": 5, "NAME": "Assisted Metro Bank", "STNAME": "New York", "BKCLASS": "NM", "ACTIVE": 1, "ESTYMD": "1980-01-01", "ENDEFYMD": "9999-12-31", "CHANGEC1": 0.0, "INSDATE": "1980-01-01"},
    {"CERT": 6, "NAME": "Merged Community Bank", "STNAME": "Georgia", "BKCLASS": "NM", "ACTIVE": 0, "ESTYMD": "1980-01-01", "ENDEFYMD": "2022-06-30", "CHANGEC1": 100.0, "INSDATE": "1980-01-01"},
    {"CERT": 7, "NAME": "Safe Capital Bank", "STNAME": "Massachusetts", "BKCLASS": "N", "ACTIVE": 1, "ESTYMD": "1980-01-01", "ENDEFYMD": "9999-12-31", "CHANGEC1": 0.0, "INSDATE": "1980-01-01"},
    {"CERT": 8, "NAME": "Valley Commerce Bank", "STNAME": "Arizona", "BKCLASS": "NM", "ACTIVE": 1, "ESTYMD": "1980-01-01", "ENDEFYMD": "9999-12-31", "CHANGEC1": 0.0, "INSDATE": "1980-01-01"},
    {"CERT": 9, "NAME": "Pacific Trust Bank", "STNAME": "Washington", "BKCLASS": "SM", "ACTIVE": 1, "ESTYMD": "1980-01-01", "ENDEFYMD": "9999-12-31", "CHANGEC1": 0.0, "INSDATE": "1980-01-01"},
    {"CERT": 10, "NAME": "Midwest Rural Bank", "STNAME": "Nebraska", "BKCLASS": "NM", "ACTIVE": 1, "ESTYMD": "1980-01-01", "ENDEFYMD": "9999-12-31", "CHANGEC1": 0.0, "INSDATE": "1980-01-01"},
]


def _generate_financials_for_quarter(repdte_str: str) -> list[dict[str, Any]]:
    norm = repdte_str.replace("-", "")
    iso = f"{norm[:4]}-{norm[4:6]}-{norm[6:8]}"
    year = int(norm[:4])
    quarter = (int(norm[4:6]) - 1) // 3 + 1

    rows = []
    for cert in range(1, 11):
        if cert == 2 and norm > "20210331":
            continue
        if cert == 3 and norm > "20220930":
            continue
        if cert == 4 and norm > "20240630":
            continue
        if cert == 6 and norm > "20220630":
            continue

        base_asset = 100000.0 * cert + (year - 2020) * 5000.0
        if cert == 2:
            base_asset = 80000.0
            dep = base_asset * 0.85
            eq = base_asset * 0.02
            roa = -2.0
            roe = -20.0
            nclnls = base_asset * 0.10
        elif cert == 3:
            base_asset = max(20000.0, 150000.0 - (year - 2020) * 30000.0)
            dep = base_asset * 0.85
            eq = base_asset * max(0.01, 0.08 - (year - 2020) * 0.03)
            roa = -2.5 if year >= 2022 else -0.5
            roe = -25.0 if year >= 2022 else -5.0
            nclnls = base_asset * 0.12
        elif cert == 4:
            base_asset = max(30000.0, 200000.0 - (year - 2022) * 40000.0)
            dep = base_asset * 0.80
            eq = base_asset * max(0.01, 0.09 - (year - 2022) * 0.035)
            roa = -3.0 if year >= 2024 else 0.2
            roe = -30.0 if year >= 2024 else 2.0
            nclnls = base_asset * 0.15
        else:
            dep = base_asset * 0.75
            eq = base_asset * 0.10
            roa = 1.2
            roe = 12.0
            nclnls = base_asset * 0.01

        rows.append({
            "CERT": cert,
            "REPDTE": norm,
            "STNAME": "Texas",
            "ASSET": base_asset,
            "DEP": dep,
            "DEPDOM": dep * 0.95,
            "BRO": dep * 0.05,
            "EQ": eq,
            "LNLSGR": base_asset * 0.65,
            "LNLSNET": base_asset * 0.63,
            "LNRE": base_asset * 0.40,
            "LNCI": base_asset * 0.15,
            "LNCON": base_asset * 0.08,
            "LNATRES": base_asset * 0.02,
            "NCLNLS": nclnls,
            "P3ASSET": nclnls * 0.5,
            "P9ASSET": nclnls * 0.3,
            "NAASSET": nclnls * 0.2,
            "ORE": base_asset * 0.005,
            "CHBAL": base_asset * 0.05,
            "SC": base_asset * 0.18,
            "NETINC": base_asset * (roa / 100.0) * (quarter / 4.0),
            "ROA": roa,
            "ROE": roe,
            "NIMY": 3.45,
            "RBC1AAJ": (eq / base_asset) * 100.0,
            "RBCRWAJ": (eq / (base_asset * 0.8)) * 100.0,
            "ID": f"{cert}_{norm}",
        })
    return rows


def get_mock_response(path_and_query: str) -> tuple[int, dict[str, Any]]:
    parsed = urlparse(path_and_query)
    path = parsed.path.rstrip("/")
    query = parse_qs(parsed.query)

    endpoint = path.split("/")[-1] if "/" in path else path
    limit = int(query.get("limit", [10000])[0])
    offset = int(query.get("offset", [0])[0])

    if endpoint == "financials":
        filters = query.get("filters", [""])[0]
        repdte_match = re.search(r"REPDTE:([0-9\-]+)", filters)
        if repdte_match:
            date_val = repdte_match.group(1)
            all_records = _generate_financials_for_quarter(date_val)
        else:
            all_records = _generate_financials_for_quarter("20200331")
    elif endpoint == "failures":
        all_records = FAILURES_DATA
    elif endpoint == "institutions":
        all_records = INSTITUTIONS_DATA
    else:
        return 404, {"error": "not found"}

    total = len(all_records)
    slice_records = all_records[offset: offset + limit]
    payload = {
        "meta": {"total": total},
        "data": [{"data": rec} for rec in slice_records],
    }
    return 200, payload


class FakeFDICAdapter(BaseAdapter):
    """In-process requests adapter that intercepts FDIC calls without opening network sockets."""
    def __init__(self) -> None:
        super().__init__()
        self.call_count = 0
        self.endpoint_counts: dict[str, int] = {}

    def send(self, request, **kwargs):
        self.call_count += 1
        parsed = urlparse(request.url)
        endpoint = parsed.path.rstrip("/").split("/")[-1]
        self.endpoint_counts[endpoint] = self.endpoint_counts.get(endpoint, 0) + 1

        status_code, data = get_mock_response(request.url)
        response = Response()
        response.status_code = status_code
        response.raw = BytesIO(json.dumps(data).encode("utf-8"))
        response.headers["Content-Type"] = "application/json"
        response.url = request.url
        return response

    def close(self):
        pass


class FakeFDICServerHandler(BaseHTTPRequestHandler):
    request_counts: dict[str, int] = {}

    def log_message(self, format: str, *args: Any) -> None:
        pass

    def do_GET(self) -> None:
        FakeFDICServerHandler.request_counts["total"] = FakeFDICServerHandler.request_counts.get("total", 0) + 1
        parsed = urlparse(self.path)
        endpoint = parsed.path.rstrip("/").split("/")[-1]
        FakeFDICServerHandler.request_counts[endpoint] = FakeFDICServerHandler.request_counts.get(endpoint, 0) + 1

        status_code, payload = get_mock_response(self.path)
        body = json.dumps(payload).encode("utf-8")
        self.send_response(status_code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


def create_fake_server(host: str = "127.0.0.1", port: int = 8765) -> HTTPServer:
    FakeFDICServerHandler.request_counts.clear()
    return HTTPServer((host, port), FakeFDICServerHandler)


def run_server_forever(host: str = "127.0.0.1", port: int = 8765) -> None:
    server = create_fake_server(host, port)
    print(f"Fake FDIC server running at http://{host}:{port}/banks")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        server.server_close()
        print("Fake FDIC server stopped.")


if __name__ == "__main__":
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 8765
    run_server_forever(port=port)
