use std::time::{Duration, SystemTime};

use std::sync::Arc;
use tracing::{debug, warn};

use crate::common::types::{HeartbeatMessage, WorkerId};
use crate::worker::executor::Executor;

pub fn spawn_heartbeat(
    worker_id: WorkerId,
    worker_addr: String,
    master_addr: String,
    interval_ms: u64,
    executor: Arc<Executor>,
) {
    tokio::spawn(async move {
        let client = reqwest::Client::new();
        let url = format!("http://{master_addr}/api/v1/heartbeat");

        loop {
            let metrics = executor.snapshot_metrics();
            let payload = HeartbeatMessage {
                worker_id,
                address: worker_addr.clone(),
                metrics,
                timestamp: SystemTime::now(),
            };

            match client.post(&url).json(&payload).send().await {
                Ok(_) => debug!(worker_id = %worker_id, "sent heartbeat"),
                Err(err) => warn!(worker_id = %worker_id, "failed to send heartbeat: {err}"),
            }

            tokio::time::sleep(Duration::from_millis(interval_ms)).await;
        }
    });
}
