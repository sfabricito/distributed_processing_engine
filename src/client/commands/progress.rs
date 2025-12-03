use crate::client::commands::fetch_json;
use crate::client::types::job::JobResponse;
use crate::client::AppContext;

/// Print only progress percentage.
pub async fn run(ctx: &AppContext, job_id: &str) -> anyhow::Result<()> {
    let url = ctx.url(&format!("/api/v1/jobs/{}", job_id));
    let job: JobResponse = fetch_json(ctx, &url).await?;
    println!("Progress: {:.1}%", job.progress);
    Ok(())
}
