use std::{fs, path::PathBuf};

use anyhow::{Context, Result};
use clap::Subcommand;
use reqwest::StatusCode;
use tracing::warn;
use uuid::Uuid;

use crate::common::{
    config::Config,
    dag::{DagEdge, DagNode, DagSpecification, OperatorType},
    types::{JobId, JobStatus, TaskResult},
};

#[derive(Subcommand, Debug)]
pub enum ClientCommand {
    /// Submit a DAG from a JSON file
    Submit {
        #[arg(long)]
        dag: PathBuf,
        #[arg(long)]
        input: Option<String>,
        #[arg(long)]
        partitions: Option<usize>,
    },
    /// Check the status for a job id
    Status {
        #[arg(long)]
        job_id: String,
    },
    /// Download job results
    GetResult {
        #[arg(long)]
        job_id: String,
        #[arg(long)]
        output: Option<PathBuf>,
    },
    /// Send a pre-defined wordcount DAG
    ExampleWordcount {
        #[arg(long)]
        input: PathBuf,
        #[arg(long, default_value_t = 4)]
        partitions: usize,
    },
}

pub async fn execute_client(cmd: ClientCommand, config: &Config) -> Result<()> {
    let master_url = format!("http://{}", config.master_addr());
    match cmd {
        ClientCommand::Submit {
            dag,
            input,
            partitions,
        } => {
            let dag_json = fs::read_to_string(&dag)
                .with_context(|| format!("reading dag file {}", dag.display()))?;
            let mut spec: DagSpecification =
                serde_json::from_str(&dag_json).context("parsing dag json")?;
            if let Some(input) = input {
                set_read_uri(&mut spec, input)?;
            }
            if let Some(parts) = partitions {
                spec.partitions = parts;
            }
            let job_id = submit_job(&master_url, spec).await?;
            println!("job submitted: {job_id}");
        }
        ClientCommand::Status { job_id } => {
            let status = get_status(&master_url, &job_id).await?;
            println!("job {job_id}: {:?}", status);
        }
        ClientCommand::GetResult { job_id, output } => {
            let results = get_results(&master_url, &job_id).await?;
            if let Some(path) = output {
                let json = serde_json::to_string_pretty(&results)?;
                fs::write(&path, json)?;
                println!("saved results to {}", path.display());
            } else {
                println!("results: {:?}", results);
            }
        }
        ClientCommand::ExampleWordcount { input, partitions } => {
            let dag = example_wordcount(input, partitions);
            let job_id = submit_job(&master_url, dag).await?;
            println!("wordcount submitted: {job_id}");
        }
    }
    Ok(())
}

async fn submit_job(master_url: &str, dag: DagSpecification) -> Result<JobId> {
    let client = reqwest::Client::new();
    let url = format!("{master_url}/api/v1/jobs");
    let resp = client.post(url).json(&dag).send().await?;
    if resp.status() != StatusCode::OK {
        let body = resp.text().await.unwrap_or_default();
        anyhow::bail!("failed to submit job: {body}");
    }
    let payload: serde_json::Value = resp.json().await?;
    let id = payload
        .get("job_id")
        .and_then(|v| v.as_str())
        .ok_or_else(|| anyhow::anyhow!("missing job_id in response"))?;
    let job_id = Uuid::parse_str(id)?;
    Ok(job_id)
}

async fn get_status(master_url: &str, job_id: &str) -> Result<JobStatus> {
    let client = reqwest::Client::new();
    let url = format!("{master_url}/api/v1/jobs/{job_id}");
    let resp = client.get(url).send().await?;
    if resp.status() == StatusCode::NOT_FOUND {
        warn!("job not found");
    }
    let payload: serde_json::Value = resp.json().await?;
    let status = payload
        .get("status")
        .and_then(|s| serde_json::from_value(s.clone()).ok())
        .ok_or_else(|| anyhow::anyhow!("missing status in response"))?;
    let progress = payload
        .get("progress")
        .and_then(|p| p.as_f64())
        .unwrap_or(0.0);
    let metrics = payload.get("metrics").cloned().unwrap_or_default();
    println!("status: {:?}, progress: {:.1}%", status, progress);
    if metrics != serde_json::Value::Null {
        println!("metrics: {}", metrics);
    }
    Ok(status)
}

async fn get_results(master_url: &str, job_id: &str) -> Result<Vec<TaskResult>> {
    let client = reqwest::Client::new();
    let url = format!("{master_url}/api/v1/jobs/{job_id}/results");
    let resp = client.get(url).send().await?;
    let results: Vec<TaskResult> = resp.json().await?;
    Ok(results)
}

fn example_wordcount(input: PathBuf, partitions: usize) -> DagSpecification {
    DagSpecification {
        nodes: vec![
            DagNode {
                id: "read".into(),
                operator: OperatorType::Read {
                    uri: input.display().to_string(),
                    format: "csv".into(),
                },
            },
            DagNode {
                id: "map".into(),
                operator: OperatorType::Map {
                    script: "split lines".into(),
                },
            },
            DagNode {
                id: "reduce".into(),
                operator: OperatorType::Reduce {
                    reducer: "sum counts".into(),
                },
            },
        ],
        edges: vec![
            DagEdge {
                from: "read".into(),
                to: "map".into(),
            },
            DagEdge {
                from: "map".into(),
                to: "reduce".into(),
            },
        ],
        partitions,
    }
}

fn set_read_uri(spec: &mut DagSpecification, uri: String) -> Result<()> {
    for node in spec.nodes.iter_mut() {
        if let OperatorType::Read { uri: u, .. } = &mut node.operator {
            *u = uri.clone();
            return Ok(());
        }
    }
    anyhow::bail!("DAG does not contain a Read operator to set input");
}
