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

#[test]
fn e2e_local_pipeline() {
    let master_port = 8080u16;
    let master_host = "127.0.0.1";
    let mut master = start_master(master_host, master_port).expect("spawn master");
    let mut worker =
        start_worker(master_host, master_port, "127.0.0.1", 9100, None).expect("spawn worker");

    let result = (|| -> anyhow::Result<()> {
        wait_for_master(master_host, master_port, 30)?;

        // prepare small input
        let data_dir = std::env::current_dir()?.join("data");
        fs::create_dir_all(&data_dir)?;
        let input_path = data_dir.join("e2e_local.json");
        fs::write(
            &input_path,
            serde_json::to_string(&vec![json!({"value": "hello"}), json!({"value": "world"})])?,
        )?;

        let dag = json!({
            "partitions": 1,
            "nodes": [
                { "id": "read", "operator": { "Read": { "uri": input_path.to_str().unwrap(), "format": "json" } } },
                { "id": "map",  "operator": { "Map": { "script": "identity" } } }
            ],
            "edges": [
                { "from": "read", "to": "map" }
            ]
        });

        let job_id = submit_job(master_host, master_port, &dag)?;
        let started = Instant::now();
        let status = poll_status(master_host, master_port, job_id, 60)?;
        let duration = started.elapsed().as_secs_f32();
        let results = fetch_results(master_host, master_port, job_id)?;

        // Basic validation
        assert!(
            matches!(
                status
                    .get("status")
                    .and_then(|s| s.as_str())
                    .unwrap_or("")
                    .to_uppercase()
                    .as_str(),
                "SUCCEEDED" | "COMPLETED"
            ),
            "unexpected status: {:?}",
            status
        );
        assert!(
            results.as_array().map(|a| !a.is_empty()).unwrap_or(false),
            "no results returned"
        );

        let report = json!({
            "test": "e2e_local_pipeline",
            "job_id": job_id.to_string(),
            "duration_sec": duration,
            "status": status,
            "results": results,
            "pass": true
        });
        write_report(
            &std::path::PathBuf::from("target/test_reports/e2e_report.json"),
            &report,
        )?;
        Ok(())
    })();

    kill(&mut worker);
    kill(&mut master);

    if let Err(err) = result {
        panic!("E2E test failed: {err:?}");
    }
}
