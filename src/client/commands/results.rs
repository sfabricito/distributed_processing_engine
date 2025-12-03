use crate::client::commands::fetch_json;
use crate::client::types::job::JobResponse;
use crate::client::AppContext;
use colored::Colorize;
use std::fs;
use std::path::Path;
use tokio::time::{sleep, Duration};

pub async fn run(ctx: &AppContext, job_id: &str, output_dir: &Path) -> anyhow::Result<()> {
    let url = ctx.url(&format!("/api/v1/jobs/{}", job_id));
    let job: JobResponse = fetch_json(ctx, &url).await?;
    let outputs = if !job.output_files.is_empty() {
        job.output_files.clone()
    } else {
        job.outputs.clone()
    };

    if outputs.is_empty() {
        println!("{}", "No output files reported for this job.".yellow());
        return Ok(());
    }

    fs::create_dir_all(output_dir)?;
    println!("Downloading outputs...");
    for path in outputs {
        download_file(ctx, &path, output_dir).await?;
    }
    println!("{} {}", "Saved to".green(), output_dir.display());
    Ok(())
}

async fn download_file(ctx: &AppContext, remote_path: &str, base_dir: &Path) -> anyhow::Result<()> {
    let rel = remote_path.trim_start_matches('/');
    let dest_path = base_dir.join(rel);
    if let Some(parent) = dest_path.parent() {
        fs::create_dir_all(parent)?;
    }

    let url = format!("{}/{}", ctx.master, rel);
    let mut last_err: Option<anyhow::Error> = None;
    for _ in 0..3 {
        match ctx.client.get(&url).send().await {
            Ok(resp) if resp.status().is_success() => {
                let bytes = resp.bytes().await?;
                fs::write(&dest_path, &bytes)?;
                println!("{} {}", "✓".green(), rel);
                return Ok(());
            }
            Ok(resp) => {
                let status = resp.status();
                let body = resp.text().await.unwrap_or_default();
                last_err = Some(anyhow::anyhow!("http {} {}", status, body));
            }
            Err(err) => {
                last_err = Some(err.into());
            }
        }
        sleep(Duration::from_millis(500)).await;
    }

    Err(last_err.unwrap_or_else(|| anyhow::anyhow!("failed to download {}", remote_path)))
}
