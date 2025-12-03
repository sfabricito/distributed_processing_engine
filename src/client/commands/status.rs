use crate::client::commands::{fetch_json, watch_until_terminal};
use crate::client::types::job::JobResponse;
use crate::client::types::worker::WorkerInfo;
use crate::client::{color_status, AppContext};
use colored::Colorize;
use prettytable::{row, Table};

/// Show status; if watch=true poll every 2 seconds.
pub async fn run(ctx: &AppContext, job_id: &str, watch: bool) -> anyhow::Result<()> {
    if watch {
        let bar = indicatif::ProgressBar::new(100);
        bar.set_style(
            indicatif::ProgressStyle::with_template(
                "{spinner:.green} [{elapsed_precise}] {wide_bar} {pos:.1}/{len:.1}% {msg}",
            )?
            .progress_chars("#>-"),
        );
        watch_until_terminal(|| async {
            let job = fetch_job(ctx, job_id).await?;
            bar.set_position(job.progress as u64);
            bar.set_message(format!("status={}", job.status));
            if is_terminal(&job.status) {
                bar.finish_and_clear();
                print_status(&job, None);
                return Ok(true);
            }
            Ok(false)
        })
        .await?;
    } else {
        let workers = fetch_workers(ctx).await.ok();
        let job = fetch_job(ctx, job_id).await?;
        print_status(&job, workers.as_ref());
    }
    Ok(())
}

fn is_terminal(status: &str) -> bool {
    matches!(
        status.to_uppercase().as_str(),
        "FAILED" | "SUCCEEDED" | "COMPLETED"
    )
}

async fn fetch_job(ctx: &AppContext, job_id: &str) -> anyhow::Result<JobResponse> {
    let url = ctx.url(&format!("/api/v1/jobs/{}", job_id));
    fetch_json(ctx, &url).await
}

async fn fetch_workers(ctx: &AppContext) -> anyhow::Result<Vec<WorkerInfo>> {
    let url = ctx.url("/api/v1/workers");
    fetch_json(ctx, &url).await
}

fn print_status(job: &JobResponse, workers: Option<&Vec<WorkerInfo>>) {
    println!("Job ID: {}", job.job_id);
    println!("State: {}", color_status(&job.status).bold());
    println!("Progress: {:.1}%", job.progress);

    if let Some(metrics) = job.metrics.as_ref() {
        if let (Some(total), Some(done)) = (metrics.tasks_total, metrics.tasks_completed) {
            println!(
                "Tasks: {} / {} completed, failed: {}",
                done,
                total,
                metrics.tasks_failed.unwrap_or_default()
            );
        }
    }

    if !job.stages.is_empty() {
        let mut table = Table::new();
        table.add_row(row!["Stage", "State", "Tasks", "Errors"]);
        for st in &job.stages {
            let tasks_str = match (
                st.tasks_completed,
                st.tasks_running,
                st.tasks_failed,
                st.tasks_total,
            ) {
                (Some(c), Some(r), Some(f), Some(t)) => {
                    format!("{}/{} running={}, failed={}", c, t, r, f)
                }
                _ => "-".to_string(),
            };
            let state = st.status.clone().unwrap_or_else(|| "UNKNOWN".into());
            let err_count = st
                .errors
                .as_ref()
                .map(|v| v.len().to_string())
                .unwrap_or_else(|| "0".into());
            table.add_row(row![
                st.stage_id
                    .map(|s| s.to_string())
                    .unwrap_or_else(|| st.id.clone().unwrap_or_default()),
                color_status(&state),
                tasks_str,
                err_count
            ]);
        }
        table.printstd();
    }

    if let Some(err) = job.errors.first() {
        eprintln!("Last error: {}", err.red());
    }

    if let Some(ws) = workers {
        println!("\nWorkers:");
        for w in ws {
            let status = w.status.clone().unwrap_or_else(|| "UNKNOWN".into());
            let metrics = w.metrics.clone().unwrap_or_default();
            println!(
                "{} {} cpu={:.1}% mem={}MB tasks={} last={}",
                w.id,
                color_status(&status),
                metrics.cpu_pct,
                metrics.memory_mb,
                metrics.tasks_in_flight,
                w.last_heartbeat.clone().unwrap_or_else(|| "-".into())
            );
        }
    }
}
