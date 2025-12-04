use std::{
    fs::{create_dir_all, File},
    io::Write,
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    thread,
    time::{Duration, Instant},
};

use anyhow::{anyhow, Context, Result};
use reqwest::blocking::Client;
use serde_json::Value;
use uuid::Uuid;

/// Start the master process on the given host/port. Returns the child handle.
pub fn start_master(host: &str, port: u16) -> Result<Child> {
    let bin = binary_path("distributed_processing_engine")?;
    let mut cmd = Command::new(bin);
    cmd.env("MASTER_HOST", host)
        .env("MASTER_PORT", port.to_string())
        .env("WORKER_BASE_PORT", "9100")
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .arg("master");
    cmd.spawn().context("spawn master")
}

/// Start a worker pointing at the given master address, binding to host/port.
pub fn start_worker(
    master_host: &str,
    master_port: u16,
    bind_host: &str,
    port: u16,
    worker_id: Option<&str>,
) -> Result<Child> {
    let bin = binary_path("distributed_processing_engine")?;
    let mut cmd = Command::new(bin);
    cmd.env("MASTER_HOST", master_host)
        .env("MASTER_PORT", master_port.to_string())
        .env("WORKER_BIND_HOST", bind_host)
        .env("WORKER_ADVERTISE_HOST", "127.0.0.1")
        .env("WORKER_BASE_PORT", port.to_string())
        .env("WORKER_ID", worker_id.unwrap_or(""))
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .arg("worker");
    cmd.spawn().context("spawn worker")
}

fn binary_path(name: &str) -> Result<PathBuf> {
    let mut path = std::env::current_dir()?;
    path.push("target");
    path.push("debug");
    path.push(name);
    if path.exists() {
        return Ok(path);
    }
    // fallback to release
    path = path.with_file_name("release");
    path.push(name);
    Ok(path)
}

/// Wait until master responds to /api/v1/health (or /jobs) within timeout_secs.
pub fn wait_for_master(host: &str, port: u16, timeout_secs: u64) -> Result<()> {
    let client = Client::builder()
        .timeout(Duration::from_secs(2))
        .build()
        .unwrap();
    let base = format!("http://{}:{}", host, port);
    let deadline = Instant::now() + Duration::from_secs(timeout_secs);
    while Instant::now() < deadline {
        if client
            .get(format!("{}/api/v1/health", base))
            .send()
            .and_then(|r| r.error_for_status())
            .is_ok()
        {
            return Ok(());
        }
        if client
            .get(format!("{}/api/v1/jobs", base))
            .send()
            .and_then(|r| r.error_for_status())
            .is_ok()
        {
            return Ok(());
        }
        thread::sleep(Duration::from_millis(500));
    }
    Err(anyhow!("master not ready"))
}

pub fn submit_job(master_host: &str, master_port: u16, dag: &Value) -> Result<Uuid> {
    let client = Client::new();
    let url = format!("http://{}:{}/api/v1/jobs", master_host, master_port);
    let resp = client.post(url).json(dag).send()?.error_for_status()?;
    let v: Value = resp.json()?;
    let job_id_str = v
        .get("job_id")
        .and_then(|v| v.as_str())
        .ok_or_else(|| anyhow!("missing job_id in response"))?;
    Ok(Uuid::parse_str(job_id_str)?)
}

pub fn poll_status(
    master_host: &str,
    master_port: u16,
    job_id: Uuid,
    timeout_secs: u64,
) -> Result<Value> {
    let client = Client::new();
    let url = format!(
        "http://{}:{}/api/v1/jobs/{}",
        master_host, master_port, job_id
    );
    let deadline = Instant::now() + Duration::from_secs(timeout_secs);
    while Instant::now() < deadline {
        let resp = client.get(&url).send()?.error_for_status()?;
        let v: Value = resp.json()?;
        let status = v
            .get("status")
            .and_then(|s| s.as_str())
            .unwrap_or("UNKNOWN")
            .to_uppercase();
        if status == "SUCCEEDED" || status == "COMPLETED" {
            return Ok(v);
        }
        if status == "FAILED" {
            return Err(anyhow!("job failed: {:?}", v));
        }
        thread::sleep(Duration::from_millis(500));
    }
    Err(anyhow!("timeout waiting for job {}", job_id))
}

pub fn fetch_results(master_host: &str, master_port: u16, job_id: Uuid) -> Result<Value> {
    let client = Client::new();
    let url = format!(
        "http://{}:{}/api/v1/jobs/{}/results",
        master_host, master_port, job_id
    );
    let resp = client.get(url).send()?.error_for_status()?;
    Ok(resp.json()?)
}

pub fn write_report(path: &Path, content: &Value) -> Result<()> {
    if let Some(parent) = path.parent() {
        create_dir_all(parent)?;
    }
    let mut f = File::create(path)?;
    f.write_all(serde_json::to_string_pretty(content)?.as_bytes())?;
    Ok(())
}
