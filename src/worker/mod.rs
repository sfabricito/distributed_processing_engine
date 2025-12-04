use std::net::TcpListener;
use std::sync::Arc;

use anyhow::Result;
use axum::{extract::State, http::StatusCode, routing::post, Json, Router};
use tracing::info;
use uuid::Uuid;

use crate::common::{
    config::Config,
    types::{Task, WorkerId, WorkerInfo, WorkerMetrics, WorkerStatus},
};

pub mod executor;
pub mod monitor;
pub mod ops;
pub mod partition;

pub struct Worker {
    pub id: WorkerId,
    pub config: Config,
    pub master_addr: String,
    pub listen_addr: String,
    pub advertise_addr: String,
    executor: Arc<executor::Executor>,
}

impl Worker {
    pub fn new(config: Config, id: Option<String>, port: Option<u16>) -> Self {
        let worker_id = id
            .and_then(|raw| Uuid::parse_str(&raw).ok())
            .unwrap_or_else(Uuid::new_v4);
        let chosen_port = choose_available_port(port.unwrap_or(config.worker_base_port));
        let listen_addr = format!("{}:{}", config.worker_bind_host, chosen_port);
        let advertise_addr = format!("{}:{}", config.worker_advertise_host, chosen_port);
        Self {
            id: worker_id,
            listen_addr,
            advertise_addr,
            master_addr: config.master_addr(),
            executor: Arc::new(executor::Executor::new(
                config.clone(),
                worker_id,
                config.master_addr(),
            )),
            config,
        }
    }

    pub async fn start(self) -> Result<()> {
        info!(worker_id = %self.id, "starting worker");
        self.register().await?;
        monitor::spawn_heartbeat(
            self.id,
            self.listen_addr.clone(),
            self.master_addr.clone(),
            self.config.heartbeat_interval_ms,
            self.executor.clone(),
        );

        self.spawn_task_api().await?;
        self.executor.run().await?;
        Ok(())
    }

    async fn register(&self) -> Result<()> {
        let client = reqwest::Client::new();
        let url = format!("http://{}/api/v1/register", self.master_addr);
        let payload = WorkerInfo {
            id: self.id,
            address: self.advertise_addr.clone(),
            status: WorkerStatus::Active,
            last_heartbeat: std::time::SystemTime::now(),
            metrics: WorkerMetrics::default(),
        };
        let resp = client.post(url).json(&payload).send().await?;
        if !resp.status().is_success() {
            anyhow::bail!("failed to register worker: {}", resp.status());
        }

        info!(worker_id = %payload.id, address = %payload.address, "registered worker with master");
        Ok(())
    }

    pub async fn handle_task(&self, task: Task) -> Result<()> {
        self.executor.execute_and_report(task).await?;
        Ok(())
    }

    async fn spawn_task_api(&self) -> Result<()> {
        let state = TaskApiState {
            executor: self.executor.clone(),
        };
        let app = Router::new()
            .route("/api/v1/tasks/execute", post(execute_task))
            .with_state(state);
        let listener = tokio::net::TcpListener::bind(&self.listen_addr).await?;
        info!(addr = %self.listen_addr, "worker listening for tasks");

        tokio::spawn(async move {
            if let Err(err) = axum::serve(listener, app.into_make_service()).await {
                tracing::warn!("task API server error: {err}");
            }
        });

        Ok(())
    }
}

fn choose_available_port(start_port: u16) -> u16 {
    for offset in 0..50u16 {
        let port = start_port.saturating_add(offset);
        if TcpListener::bind(("0.0.0.0", port)).is_ok() {
            return port;
        }
    }
    start_port
}

#[derive(Clone)]
struct TaskApiState {
    executor: Arc<executor::Executor>,
}

async fn execute_task(State(state): State<TaskApiState>, Json(task): Json<Task>) -> StatusCode {
    match state.executor.execute_and_report(task).await {
        Ok(_) => StatusCode::ACCEPTED,
        Err(err) => {
            tracing::warn!("task execution failed: {err}");
            StatusCode::INTERNAL_SERVER_ERROR
        }
    }
}
