use std::{
    fs,
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

fn main() -> anyhow::Result<()> {
    let master_host = "127.0.0.1";
    let master_port = 8081u16;
    let mut master = start_master(master_host, master_port)?;
    let mut w1 = start_worker(
        master_host,
        master_port,
        "127.0.0.1",
        9200,
        Some("worker-1"),
    )?;
    let mut w2 = start_worker(
        master_host,
        master_port,
        "127.0.0.1",
        9300,
        Some("worker-2"),
    )?;

    let result = (|| -> anyhow::Result<()> {
        wait_for_master(master_host, master_port, 30)?;
        let data_dir = std::env::current_dir()?.join("data");
        fs::create_dir_all(&data_dir)?;
        let input_path = data_dir.join("fault_long.json");
        let mut rows = Vec::new();
        for i in 0..2000 {
            rows.push(json!({"value": i}));
        }
        fs::write(&input_path, serde_json::to_string(&rows)?)?;

        let dag = json!({
            "partitions": 4,
            "nodes": [
                { "id": "read", "operator": { "Read": { "uri": input_path.to_str().unwrap(), "format": "json" } } },
                { "id": "map", "operator": { "Map": { "script": "identity" } } }
            ],
            "edges": [{ "from": "read", "to": "map" }]
        });

        let job_id = submit_job(master_host, master_port, &dag)?;
        let inject_at = Instant::now() + Duration::from_secs(2);
        let mut retries = 0u32;

        // Monitor loop
        let client = reqwest::blocking::Client::new();
        let status_url = format!(
            "http://{}:{}/api/v1/jobs/{}",
            master_host, master_port, job_id
        );

        loop {
            if Instant::now() >= inject_at {
                // kill one worker once
                kill(&mut w1);
                retries += 1;
            }

            let resp = client.get(&status_url).send()?.error_for_status()?;
            let v: serde_json::Value = resp.json()?;
            let st = v
                .get("status")
                .and_then(|s| s.as_str())
                .unwrap_or("")
                .to_uppercase();
            if st == "SUCCEEDED" || st == "COMPLETED" {
                break;
            }
            if st == "FAILED" {
                return Err(anyhow::anyhow!("job failed after fault: {:?}", v));
            }
            thread::sleep(Duration::from_millis(500));
        }

        let status = poll_status(master_host, master_port, job_id, 30)?;
        let results = fetch_results(master_host, master_port, job_id)?;
        let report = json!({
            "test": "fault_worker_kill",
            "job_id": job_id.to_string(),
            "worker_killed": "worker-1",
            "retries": retries,
            "status": status,
            "results": results,
            "pass": true
        });
        write_report(
            &std::path::PathBuf::from("target/test_reports/faults_report.json"),
            &report,
        )?;
        Ok(())
    })();

    kill(&mut w2);
    kill(&mut master);

    if let Err(err) = result {
        eprintln!("[FAULTS] failed: {err:?}");
        std::process::exit(1);
    }
    Ok(())
}
