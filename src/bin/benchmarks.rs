//! Benchmark harness implemented in Rust to integrate directly with the engine binaries,
//! minimizing external dependencies and avoiding Python interpreter overhead. Rust gives
//! tighter control over process handling and timing while reusing existing crates.

use std::{
    fs,
    path::PathBuf,
    process::Child,
    thread,
    time::{Duration, Instant},
};

use serde_json::json;
use uuid::Uuid;

use distributed_processing_engine::tests_support::{
    fetch_results, poll_status, start_master, start_worker, submit_job, wait_for_master,
    write_report,
};

fn kill(child: &mut Child) {
    let _ = child.kill();
    let _ = child.wait();
}

fn gen_dataset(path: &PathBuf, rows: usize) -> anyhow::Result<()> {
    let mut data = Vec::with_capacity(rows);
    for i in 0..rows {
        data.push(json!({"text": format!("hello world {}", i), "Product": format!("p{}", i % 100), "Quantity": 1}));
    }
    fs::create_dir_all(path.parent().unwrap())?;
    fs::write(path, serde_json::to_string(&data)?)?;
    Ok(())
}

fn run_benchmark(
    name: &str,
    dag: serde_json::Value,
    master_host: &str,
    master_port: u16,
    report: &mut Vec<serde_json::Value>,
) -> anyhow::Result<()> {
    let job_id = submit_job(master_host, master_port, &dag)?;
    let start = Instant::now();
    let status = poll_status(master_host, master_port, job_id, 120)?;
    let elapsed = start.elapsed().as_secs_f32();
    let results = fetch_results(master_host, master_port, job_id)?;
    report.push(json!({
        "name": name,
        "job_id": job_id.to_string(),
        "duration_sec": elapsed,
        "status": status,
        "results_count": results.as_array().map(|a| a.len()).unwrap_or(0)
    }));
    Ok(())
}

fn main() -> anyhow::Result<()> {
    let master_host = "127.0.0.1";
    let master_port = 8082u16;
    let mut master = start_master(master_host, master_port)?;
    let mut w1 = start_worker(
        master_host,
        master_port,
        "127.0.0.1",
        9400,
        Some("bench-w1"),
    )?;
    let mut w2 = start_worker(
        master_host,
        master_port,
        "127.0.0.1",
        9500,
        Some("bench-w2"),
    )?;

    let result = (|| -> anyhow::Result<()> {
        wait_for_master(master_host, master_port, 30)?;
        let data_dir = std::env::current_dir()?.join("data");
        let wc_path = data_dir.join("bench_wc.json");
        let join_right = data_dir.join("bench_join_right.json");
        gen_dataset(&wc_path, 50_000)?;
        gen_dataset(&join_right, 50_000)?;

        let mut report = Vec::new();

        // Benchmark 1: wordcount-like
        let dag_wc = json!({
            "partitions": 4,
            "nodes": [
                { "id": "read", "operator": { "Read": { "uri": wc_path.to_str().unwrap(), "format": "json" } } },
                { "id": "map", "operator": { "Map": { "script": "identity" } } }
            ],
            "edges": [
                { "from": "read", "to": "map" }
            ]
        });
        run_benchmark(
            "wordcount_like",
            dag_wc,
            master_host,
            master_port,
            &mut report,
        )?;

        // Benchmark 2: join
        let dag_join = json!({
            "partitions": 4,
            "nodes": [
                { "id": "left", "operator": { "Read": { "uri": wc_path.to_str().unwrap(), "format": "json" } } },
                { "id": "right", "operator": { "Read": { "uri": join_right.to_str().unwrap(), "format": "json" } } },
                { "id": "join", "operator": { "Join": { "key": "Product", "join_type": "Inner" } } }
            ],
            "edges": [
                { "from": "left", "to": "join" },
                { "from": "right", "to": "join" }
            ]
        });
        run_benchmark("join", dag_join, master_host, master_port, &mut report)?;

        let report_json = json!({
            "host": master_host,
            "port": master_port,
            "benchmarks": report
        });
        write_report(
            &PathBuf::from("target/benchmarks/benchmarks_report.json"),
            &report_json,
        )?;
        Ok(())
    })();

    kill(&mut w2);
    kill(&mut w1);
    kill(&mut master);

    if let Err(err) = result {
        eprintln!("[BENCH] failed: {err:?}");
        std::process::exit(1);
    }
    Ok(())
}
