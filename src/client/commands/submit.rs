use crate::client::commands::post_json;
use crate::client::types::job::JobResponse;
use crate::client::AppContext;
use colored::Colorize;
use std::path::Path;

/// Submit a DAG JSON file to the master.
pub async fn run(ctx: &AppContext, file: &Path) -> anyhow::Result<()> {
    let body = std::fs::read_to_string(file)?;
    let dag: serde_json::Value = serde_json::from_str(&body)?;
    let url = ctx.url("/api/v1/jobs");
    let resp: JobResponse = post_json(ctx, &url, &dag).await?;
    println!(
        "{} {} ({})",
        "Job submitted:".green().bold(),
        resp.job_id,
        resp.status
    );
    Ok(())
}
