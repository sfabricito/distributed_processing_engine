use clap::{Parser, Subcommand};
use distributed_processing_engine::{
    client,
    common::config::Config,
    master::Master,
    worker::Worker,
};
use tracing::info;
use tracing_subscriber::prelude::*;

#[derive(Parser, Debug)]
#[command(name = "dpe", about = "Distributed batch DAG mini-Spark runtime")]
struct EngineCli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand, Debug)]
enum Command {
    /// Run the master/coordinator service
    Master,
    /// Run a worker process
    Worker {
        /// Override worker port (for HTTP/task intake)
        #[arg(long)]
        port: Option<u16>,
        /// Provide a static worker id (defaults to random UUID)
        #[arg(long)]
        id: Option<String>,
    },
    /// Invoke the client subcommands (submit, status, etc.)
    Client {
        #[command(subcommand)]
        action: client::cli::ClientCommand,
    },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenvy::dotenv().ok();
    init_tracing();

    let cli = EngineCli::parse();
    let config = Config::from_env()?;
    let api_base = "/api/v1";

    match cli.command {
        Command::Master => {
            let master = Master::new(config.clone());
            info!("starting master node on {}:{}", config.master_host, config.master_port);
            master.start(api_base).await?;
        }
        Command::Worker { port, id } => {
            let worker = Worker::new(config.clone(), id, port);
            worker.start().await?;
        }
        Command::Client { action } => {
            client::cli::execute_client(action, &config).await?;
        }
    }

    Ok(())
}

fn init_tracing() {
    let fmt_layer = tracing_subscriber::fmt::layer().json().with_target(false);
    let env_filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| "info".into());
    let subscriber = tracing_subscriber::registry()
        .with(env_filter)
        .with(fmt_layer);
    let _ = tracing::subscriber::set_global_default(subscriber);
}
