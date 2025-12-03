use crate::client::commands::fetch_json;
use crate::client::types::job::JobResponse;
use crate::client::types::worker::WorkerInfo;
use crate::client::AppContext;
use colored::Colorize;
use prettytable::{row, Table};

pub async fn run(ctx: &AppContext, job_id: &str) -> anyhow::Result<()> {
    let job_url = ctx.url(&format!("/api/v1/jobs/{}", job_id));
    let workers_url = ctx.url("/api/v1/workers");
    let (job, workers): (JobResponse, Vec<WorkerInfo>) =
        tokio::try_join!(fetch_json(ctx, &job_url), fetch_json(ctx, &workers_url))?;

    print_job_metrics(&job);
    print_worker_metrics(&workers);
    Ok(())
}

fn print_job_metrics(job: &JobResponse) {
    println!("Job: {}", job.job_id);
    if let Some(m) = job.metrics.as_ref() {
        let mut table = Table::new();
        table.add_row(row!["Metric", "Value"]);
        table.add_row(row![
            "Execution time (ms)",
            m.execution_time_ms.unwrap_or_default()
        ]);
        table.add_row(row!["Tasks total", m.tasks_total.unwrap_or_default()]);
        table.add_row(row![
            "Tasks completed",
            m.tasks_completed.unwrap_or_default()
        ]);
        table.add_row(row!["Tasks failed", m.tasks_failed.unwrap_or_default()]);
        table.add_row(row![
            "Records processed",
            m.records_processed.unwrap_or_default()
        ]);
        table.printstd();

        if let Some(stages) = m.stages.as_ref() {
            println!("\nStages:");
            let mut st_table = Table::new();
            st_table.add_row(row!["Stage", "Duration(ms)", "Done/Total", "Failed"]);
            for s in stages {
                st_table.add_row(row![
                    s.stage_id,
                    s.duration_ms,
                    format!("{}/{}", s.tasks_completed, s.tasks_total),
                    s.tasks_failed
                ]);
            }
            st_table.printstd();
        }
    } else {
        println!("{}", "No metrics available".yellow());
    }
}

fn print_worker_metrics(workers: &[WorkerInfo]) {
    println!("\nWorkers:");
    let mut table = Table::new();
    table.add_row(row![
        "ID",
        "Status",
        "CPU %",
        "Mem MB",
        "Tasks",
        "Completed",
        "Failed",
        "Records",
        "Last HB"
    ]);

    for w in workers {
        let status = w.status.clone().unwrap_or_else(|| "UNKNOWN".into());
        let m = w.metrics.clone().unwrap_or_default();
        table.add_row(row![
            w.id,
            status,
            format!("{:.1}", m.cpu_pct),
            m.memory_mb,
            m.tasks_in_flight,
            m.tasks_completed,
            m.tasks_failed,
            m.records_processed,
            w.last_heartbeat.clone().unwrap_or_else(|| "-".into())
        ]);
    }
    table.printstd();
}
