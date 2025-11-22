use std::sync::Arc;

use anyhow::Result;
use tracing::info;
use uuid::Uuid;

use crate::common::{
    config::Config,
    types::{Task, WorkerId, WorkerInfo, WorkerMetrics, WorkerStatus},
};

pub mod executor;
pub mod monitor;
pub mod partition;

pub struct Worker {
    pub id: WorkerId,
    pub config: Config,
    pub master_addr: String,
    pub listen_addr: String,
    executor: Arc<executor::Executor>,
}

impl Worker {
    pub fn new(config: Config, id: Option<String>, port: Option<u16>) -> Self {
        let worker_id = id
            .and_then(|raw| Uuid::parse_str(&raw).ok())
            .unwrap_or_else(Uuid::new_v4);
        let listen_addr = format!(
            "{}:{}",
            config.master_host,
            port.unwrap_or(config.worker_base_port)
        );
        Self {
            id: worker_id,
            listen_addr,
            master_addr: config.master_addr(),
            executor: Arc::new(executor::Executor::new(config.clone(), worker_id)),
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
        );

        // In a full implementation this would start an HTTP/TCP server to receive tasks.
        // For now we just keep the executor alive to illustrate scheduling.
        self.executor.run().await?;
        Ok(())
    }

    async fn register(&self) -> Result<()> {
        let client = reqwest::Client::new();
        let url = format!("http://{}/api/v1/register", self.master_addr);
        let payload = WorkerInfo {
            id: self.id,
            address: self.listen_addr.clone(),
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
        self.executor.execute(task).await
    }
}
