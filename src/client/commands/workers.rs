use crate::client::commands::fetch_json;
use crate::client::types::worker::WorkerInfo;
use crate::client::{color_status, AppContext};
use colored::Colorize;
use prettytable::{row, Table};

pub async fn run(ctx: &AppContext) -> anyhow::Result<()> {
    let url = ctx.url("/api/v1/workers");
    let workers: Vec<WorkerInfo> = fetch_json(ctx, &url).await?;
    if workers.is_empty() {
        println!("{}", "No workers registered".yellow());
        return Ok(());
    }

    let mut table = Table::new();
    table.add_row(row![
        "Worker",
        "Status",
        "CPU%",
        "Mem MB",
        "Tasks",
        "Completed",
        "Failed",
        "Last Heartbeat"
    ]);

    for w in workers {
        let status = w.status.clone().unwrap_or_else(|| "UNKNOWN".into());
        let m = w.metrics.clone().unwrap_or_default();
        table.add_row(row![
            w.id,
            color_status(&status),
            format!("{:.1}", m.cpu_pct),
            m.memory_mb,
            m.tasks_in_flight,
            m.tasks_completed,
            m.tasks_failed,
            w.last_heartbeat.unwrap_or_else(|| "-".into())
        ]);
    }

    table.printstd();
    Ok(())
}
