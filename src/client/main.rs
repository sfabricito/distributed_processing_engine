use clap::{Parser, Subcommand};
use distributed_processing_engine::client::{commands, AppContext};
use std::path::PathBuf;

#[derive(Parser, Debug)]
#[command(
    name = "mini-spark-cli",
    about = "CLI for the mini-Spark batch engine",
    version
)]
struct Cli {
    /// Master URL (default: http://127.0.0.1:8080 or MASTER_URL env)
    #[arg(long)]
    master: Option<String>,

    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand, Debug)]
enum Command {
    /// Submit a DAG job definition (JSON file)
    Submit { job_file: PathBuf },
    /// Show job status (optionally watch)
    Status {
        job_id: String,
        #[arg(long)]
        watch: bool,
    },
    /// Show only progress %
    Progress { job_id: String },
    /// Show job and worker metrics
    Metrics { job_id: String },
    /// Download job outputs
    Results {
        job_id: String,
        #[arg(long)]
        output: PathBuf,
    },
    /// Render DAG for job
    Dag { job_id: String },
    /// List workers
    Workers,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    let ctx = AppContext::new(cli.master.as_deref())?;

    match cli.command {
        Command::Submit { job_file } => commands::submit::run(&ctx, &job_file).await?,
        Command::Status { job_id, watch } => commands::status::run(&ctx, &job_id, watch).await?,
        Command::Progress { job_id } => commands::progress::run(&ctx, &job_id).await?,
        Command::Metrics { job_id } => commands::metrics::run(&ctx, &job_id).await?,
        Command::Results { job_id, output } => {
            commands::results::run(&ctx, &job_id, &output).await?
        }
        Command::Dag { job_id } => commands::dag::run(&ctx, &job_id).await?,
        Command::Workers => commands::workers::run(&ctx).await?,
    }

    Ok(())
}
