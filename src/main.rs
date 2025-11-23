use clap::{Parser, Subcommand};
use distributed_processing_engine::{
    client, common::config::Config, master::Master, worker::Worker,
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
    Master,
    Worker {
        #[arg(long)]
        port: Option<u16>,
        #[arg(long)]
        id: Option<String>,
    },
    Client {
        #[command(subcommand)]
        action: client::cli::ClientCommand,
    },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenvy::dotenv().ok();

    let cli = EngineCli::parse();
    let config = Config::from_env()?;
    init_tracing(&config.logs_dir);
    let api_base = "/api/v1";

    match cli.command {
        Command::Master => {
            let master = Master::new(config.clone());
            info!(
                "starting master node on {}:{}",
                config.master_host, config.master_port
            );
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

fn init_tracing(logs_dir: &std::path::Path) {
    use tracing::Level;
    use tracing_appender::non_blocking;
    use tracing_appender::rolling;
    use tracing_subscriber::filter::filter_fn;
    use tracing_subscriber::fmt;

    let _ = std::fs::create_dir_all(logs_dir);

    let file_info = rolling::never(logs_dir, "info.txt");
    let (info_writer, info_guard) = non_blocking(file_info);

    let file_warn = rolling::never(logs_dir, "warn.txt");
    let (warn_writer, warn_guard) = non_blocking(file_warn);

    let file_error = rolling::never(logs_dir, "error.txt");
    let (error_writer, error_guard) = non_blocking(file_error);

    let _ = Box::leak(Box::new(info_guard));
    let _ = Box::leak(Box::new(warn_guard));
    let _ = Box::leak(Box::new(error_guard));

    let info_layer = fmt::layer()
        .json()
        .with_writer(info_writer)
        .with_filter(filter_fn(|meta| meta.level() == &Level::INFO));

    let warn_layer = fmt::layer()
        .json()
        .with_writer(warn_writer)
        .with_filter(filter_fn(|meta| meta.level() == &Level::WARN));

    let error_layer = fmt::layer()
        .json()
        .with_writer(error_writer)
        .with_filter(filter_fn(|meta| meta.level() == &Level::ERROR));

    let console_layer = fmt::layer().json().with_target(false);

    let env_filter =
        tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into());

    let subscriber = tracing_subscriber::registry()
        .with(env_filter)
        .with(info_layer)
        .with(warn_layer)
        .with(error_layer)
        .with(console_layer);

    let _ = tracing::subscriber::set_global_default(subscriber);
}
