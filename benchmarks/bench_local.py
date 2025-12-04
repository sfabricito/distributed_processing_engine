#!/usr/bin/env python3
"""
Performance benchmarks against a running master at 127.0.0.1:8080.
Uses the provided CSV datasets:
  - data/transactions_data.csv
  - data/cards_data.csv (for joins)
Outputs reports/benchmark_report.md.
"""

import os
import statistics
import sys
import time
from pathlib import Path

import requests

BASE = "http://127.0.0.1:8080"
REPORT_DIR = Path("reports")
REPORT_DIR.mkdir(exist_ok=True)

TXN_PATH = Path("data/transactions_data.csv")
CARDS_PATH = Path("data/cards_data.csv")


def submit_job(dag):
    r = requests.post(f"{BASE}/api/v1/jobs", json=dag, timeout=10)
    r.raise_for_status()
    return r.json()["job_id"]


def poll(job_id, timeout=300):
    start = time.monotonic()
    while time.monotonic() - start < timeout:
        r = requests.get(f"{BASE}/api/v1/jobs/{job_id}", timeout=5)
        if r.status_code != 200:
            time.sleep(1)
            continue
        data = r.json()
        status = str(data.get("status", "")).upper()
        if status in ("SUCCEEDED", "COMPLETED"):
            return data, time.monotonic() - start
        if status == "FAILED":
            raise RuntimeError(f"job {job_id} failed: {data}")
        time.sleep(1)
    raise RuntimeError(f"job {job_id} timed out")


def run_bench(name, dag, runs=3, rows=0):
    durations = []
    for _ in range(runs):
        job_id = submit_job(dag)
        _, dur = poll(job_id)
        durations.append(dur)
    durations.sort()
    mean = statistics.mean(durations)
    p95 = durations[int(len(durations) * 0.95) - 1] if durations else 0.0
    throughput = rows / mean if mean > 0 and rows else None
    return {
        "name": name,
        "runs": runs,
        "mean_sec": mean,
        "p95_sec": p95,
        "runs_detail": durations,
        "rows": rows,
        "throughput_rows_per_sec": throughput,
    }


def write_report(benches, errors=None):
    lines = ["# Benchmark Report", f"Started: {time.ctime()}", ""]
    for b in benches:
        lines.append(f"## {b['name']}")
        lines.append(f"- runs: {b['runs']}")
        lines.append(f"- mean_sec: {b['mean_sec']:.2f}")
        lines.append(f"- p95_sec: {b['p95_sec']:.2f}")
        lines.append(f"- runs_detail: {b['runs_detail']}")
        if b.get("rows"):
            lines.append(f"- rows: {b['rows']}")
        if b.get("throughput_rows_per_sec"):
            lines.append(f"- throughput_rows_per_sec: {b['throughput_rows_per_sec']:.2f}")
        lines.append("")
    if errors:
        lines.append("## Errors")
        lines.extend([f"- {e}" for e in errors])
        lines.append("")
    lines.append(f"Ended: {time.ctime()}")
    out = REPORT_DIR / "benchmark_report.md"
    out.write_text("\n".join(lines))
    print(f"[bench] wrote report to {out.resolve()}")


def count_rows(path: Path) -> int:
    try:
        with path.open() as f:
            return max(sum(1 for _ in f) - 1, 0)  # subtract header
    except FileNotFoundError:
        return 0


def main():
    benches = []
    errors = []
    try:
        partitions = int(os.environ.get("BENCH_PARTITIONS", "5"))
        txn_rows = count_rows(TXN_PATH)
        cards_rows = count_rows(CARDS_PATH)

        dag_map = {
            "partitions": partitions,
            "nodes": [
                {"id": "read", "operator": {"Read": {"uri": str(TXN_PATH), "format": "csv"}}},
                {"id": "map", "operator": {"Map": {"script": "identity"}}},
            ],
            "edges": [{"from": "read", "to": "map"}],
        }
        benches.append(run_bench("map_transactions", dag_map, runs=3, rows=txn_rows))

        dag_reduce = {
            "partitions": partitions,
            "nodes": [
                {"id": "read", "operator": {"Read": {"uri": str(TXN_PATH), "format": "csv"}}},
                {"id": "reduce", "operator": {"ReduceByKey": {"key": "merchant_state", "reducer": "count"}}},
            ],
            "edges": [{"from": "read", "to": "reduce"}],
        }
        benches.append(run_bench("reduce_by_key_state", dag_reduce, runs=3, rows=txn_rows))

        dag_join = {
            "partitions": partitions,
            "nodes": [
                {"id": "txn", "operator": {"Read": {"uri": str(TXN_PATH), "format": "csv"}}},
                {"id": "cards", "operator": {"Read": {"uri": str(CARDS_PATH), "format": "csv"}}},
                {"id": "join", "operator": {"Join": {"key": "client_id", "join_type": "Inner"}}},
            ],
            "edges": [
                {"from": "txn", "to": "join"},
                {"from": "cards", "to": "join"},
            ],
        }
        benches.append(run_bench("join_client_id", dag_join, runs=2, rows=txn_rows + cards_rows))
    except Exception as e:
        errors.append(str(e))
        print(f"[bench] error: {e}", file=sys.stderr)
    write_report(benches, errors)
    if errors:
        raise SystemExit("benchmark run failed")


if __name__ == "__main__":
    main()
