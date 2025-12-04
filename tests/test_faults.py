#!/usr/bin/env python3
"""
Fault-tolerance tests against a running master at 127.0.0.1:8080.
No process control; uses DAGs and bad inputs to simulate failure/retries.
Generates reports/faults_report.md.
"""

import time
from pathlib import Path
import requests

BASE = "http://127.0.0.1:8080"
REPORT_DIR = Path("reports")
REPORT_DIR.mkdir(exist_ok=True)


def submit_job(dag):
    r = requests.post(f"{BASE}/api/v1/jobs", json=dag, timeout=5)
    r.raise_for_status()
    return r.json()["job_id"]


def poll_status(job_id, timeout=120):
    deadline = time.time() + timeout
    while time.time() < deadline:
        r = requests.get(f"{BASE}/api/v1/jobs/{job_id}", timeout=5)
        if r.status_code != 200:
            time.sleep(1)
            continue
        data = r.json()
        status = str(data.get("status", "")).upper()
        if status in ("SUCCEEDED", "COMPLETED", "FAILED"):
            return data
        time.sleep(1)
    raise RuntimeError("timeout")


def write_report(lines):
    (REPORT_DIR / "faults_report.md").write_text("\n".join(lines))


def run_invalid_operator():
    dag = {
        "partitions": 5,
        "nodes": [
            {
                "id": "read",
                "operator": {"Read": {"uri": "./data/transactions_data.csv", "format": "csv"}},
            },
            {"id": "bad", "operator": {"Filter": {"predicate": "invalid_predicate"}}},
        ],
        "edges": [{"from": "read", "to": "bad"}],
    }
    job_id = submit_job(dag)
    status = poll_status(job_id)
    return job_id, status


def run_forced_failure():
    dag = {
        "partitions": 5,
        "nodes": [
            {
                "id": "read",
                "operator": {"Read": {"uri": "./data/transactions_data.csv", "format": "csv"}},
            },
            {"id": "map", "operator": {"Map": {"script": "unknown_function"}}},
        ],
        "edges": [{"from": "read", "to": "map"}],
    }
    job_id = submit_job(dag)
    status = poll_status(job_id)
    return job_id, status


def run_invalid_heartbeat():
    r = requests.post(
        f"{BASE}/api/v1/heartbeat",
        json={"worker_id": "invalid", "address": "0.0.0.0", "metrics": {}, "timestamp": time.time()},
        timeout=5,
    )
    return r.status_code, r.text


def main():
    lines = ["# Faults Report", f"Started: {time.ctime()}", ""]
    passed = True

    try:
        job_id, status = run_invalid_operator()
        lines.append("## Invalid Operator")
        lines.append(f"- job_id: {job_id}")
        lines.append(f"- status: {status.get('status')}")
        lines.append("")
    except Exception as e:
        passed = False
        lines.append(f"## Invalid Operator FAILED: {e}")

    try:
        job_id, status = run_forced_failure()
        lines.append("## Forced Failure")
        lines.append(f"- job_id: {job_id}")
        lines.append(f"- status: {status.get('status')}")
        lines.append("")
    except Exception as e:
        passed = False
        lines.append(f"## Forced Failure FAILED: {e}")

    try:
        code, text = run_invalid_heartbeat()
        lines.append("## Invalid Heartbeat")
        lines.append(f"- status_code: {code}")
        lines.append(f"- body: {text}")
        lines.append("")
    except Exception as e:
        passed = False
        lines.append(f"## Invalid Heartbeat FAILED: {e}")

    lines.append(f"Overall: {'PASS' if passed else 'FAIL'}")
    lines.append(f"Ended: {time.ctime()}")
    write_report(lines)
    if not passed:
        raise SystemExit("Fault tests failed")


if __name__ == "__main__":
    main()
