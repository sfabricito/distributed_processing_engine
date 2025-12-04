#!/usr/bin/env python3
"""
End-to-end tests against a running master at 127.0.0.1:8080.
Covers the main operators (map, filter, flat_map, reduce_by_key, join) with
5 partitions per task using real datasets:
  - transactions_data.csv
  - cards_data.csv (for joins)
Outputs a markdown report to reports/e2e_report.md.
"""

import time
from pathlib import Path
from typing import Dict, Any

import requests

BASE = "http://127.0.0.1:8080"
REPORT_DIR = Path("reports")
REPORT_DIR.mkdir(exist_ok=True)


def submit_job(dag: Dict[str, Any]) -> str:
    resp = requests.post(f"{BASE}/api/v1/jobs", json=dag, timeout=10)
    resp.raise_for_status()
    return resp.json()["job_id"]


def poll_job(job_id: str, timeout: int = 180) -> Dict[str, Any]:
    deadline = time.time() + timeout
    while time.time() < deadline:
        resp = requests.get(f"{BASE}/api/v1/jobs/{job_id}", timeout=10)
        if resp.status_code != 200:
            time.sleep(1)
            continue
        data = resp.json()
        status = str(data.get("status", "")).upper()
        if status in ("SUCCEEDED", "COMPLETED"):
            return data
        if status == "FAILED":
            raise RuntimeError(f"Job {job_id} failed: {data}")
        time.sleep(1)
    raise RuntimeError(f"Job {job_id} timed out")


def fetch_results(job_id: str) -> Any:
    resp = requests.get(f"{BASE}/api/v1/jobs/{job_id}/results", timeout=20)
    resp.raise_for_status()
    return resp.json()


def write_report(lines):
    (REPORT_DIR / "e2e_report.md").write_text("\n".join(lines))


def run_map_filter() -> Dict[str, Any]:
    dag = {
        "partitions": 5,
        "nodes": [
            {"id": "read", "operator": {"Read": {"uri": "./data/transactions_data.csv", "format": "csv"}}},
            {"id": "map", "operator": {"Map": {"script": "identity"}}},
            {"id": "filter", "operator": {"Filter": {"predicate": "always_true"}}},
        ],
        "edges": [
            {"from": "read", "to": "map"},
            {"from": "map", "to": "filter"},
        ],
    }
    start = time.monotonic()
    job_id = submit_job(dag)
    status = poll_job(job_id)
    duration = time.monotonic() - start
    results = fetch_results(job_id)
    assert results, "map_filter: empty results"
    return {"name": "map_filter", "job_id": job_id, "status": status, "duration": duration}


def run_flat_map_reduce() -> Dict[str, Any]:
    dag = {
        "partitions": 5,
        "nodes": [
            {"id": "read", "operator": {"Read": {"uri": "./data/transactions_data.csv", "format": "csv"}}},
            {"id": "flat", "operator": {"FlatMap": {"func": "identity"}}},
            {"id": "reduce", "operator": {"ReduceByKey": {"key": "use_chip", "reducer": "count"}}},
        ],
        "edges": [
            {"from": "read", "to": "flat"},
            {"from": "flat", "to": "reduce"},
        ],
    }
    start = time.monotonic()
    job_id = submit_job(dag)
    status = poll_job(job_id)
    duration = time.monotonic() - start
    results = fetch_results(job_id)
    assert results, "flat_map_reduce_by_key: empty results"
    return {"name": "flat_map_reduce_by_key", "job_id": job_id, "status": status, "duration": duration}


def run_join() -> Dict[str, Any]:
    dag = {
        "partitions": 5,
        "nodes": [
            {"id": "txn", "operator": {"Read": {"uri": "./data/transactions_data.csv", "format": "csv"}}},
            {"id": "cards", "operator": {"Read": {"uri": "./data/cards_data.csv", "format": "csv"}}},
            {"id": "join", "operator": {"Join": {"key": "client_id", "join_type": "Inner"}}},
        ],
        "edges": [
            {"from": "txn", "to": "join"},
            {"from": "cards", "to": "join"},
        ],
    }
    start = time.monotonic()
    job_id = submit_job(dag)
    status = poll_job(job_id)
    duration = time.monotonic() - start
    results = fetch_results(job_id)
    assert results, "join: empty results"
    return {"name": "join_client_id", "job_id": job_id, "status": status, "duration": duration}


def main():
    lines = ["# E2E Report", f"Started: {time.ctime()}", ""]
    passed = True
    for func in (run_map_filter, run_flat_map_reduce, run_join):
        try:
            info = func()
            lines.append(f"## {info['name']}")
            lines.append(f"- job_id: {info['job_id']}")
            lines.append(f"- duration_sec: {info['duration']:.2f}")
            lines.append(f"- status: {info['status'].get('status')}")
            lines.append("")
        except Exception as e:
            passed = False
            lines.append(f"## {func.__name__} FAILED")
            lines.append(f"- error: {e}")
            lines.append("")
    lines.append(f"Overall: {'PASS' if passed else 'FAIL'}")
    lines.append(f"Ended: {time.ctime()}")
    write_report(lines)
    if not passed:
        raise SystemExit("E2E tests failed")


if __name__ == "__main__":
    main()
