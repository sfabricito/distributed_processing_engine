use std::{
    collections::HashMap,
    time::{Duration, SystemTime},
};

use tracing::{info, warn};

use crate::common::types::{WorkerId, WorkerInfo, WorkerMetrics, WorkerStatus};

#[derive(Debug, Default)]
pub struct Registry {
    workers: HashMap<WorkerId, WorkerInfo>,
}

impl Registry {
    pub fn add_worker(&mut self, info: WorkerInfo) {
        info!(worker_id = %info.id, address = %info.address, "registry: add worker");
        self.workers.insert(info.id, info);
    }

    pub fn remove_worker(&mut self, worker_id: &WorkerId) {
        self.workers.remove(worker_id);
    }

    pub fn update_status(&mut self, worker_id: &WorkerId, status: WorkerStatus) {
        if let Some(worker) = self.workers.get_mut(worker_id) {
            worker.status = status;
        }
    }

    pub fn record_heartbeat(
        &mut self,
        worker_id: WorkerId,
        metrics: WorkerMetrics,
        address: String,
    ) {
        if let Some(worker) = self.workers.get_mut(&worker_id) {
            worker.last_heartbeat = SystemTime::now();
            worker.metrics = metrics;
            worker.status = WorkerStatus::Active;
        } else {
            warn!(
                worker_id = %worker_id,
                "received heartbeat for unknown worker, auto-registering"
            );
            self.workers.insert(
                worker_id,
                WorkerInfo {
                    id: worker_id,
                    address,
                    status: WorkerStatus::Active,
                    last_heartbeat: SystemTime::now(),
                    metrics,
                },
            );
        }
    }

    pub fn get_worker(&self, worker_id: &WorkerId) -> Option<WorkerInfo> {
        self.workers.get(worker_id).cloned()
    }

    pub fn available_workers(&self) -> Vec<WorkerInfo> {
        self.workers
            .values()
            .filter(|w| matches!(w.status, WorkerStatus::Active))
            .cloned()
            .collect()
    }

    pub fn mark_stale_workers(&mut self, grace: Duration) {
        let now = SystemTime::now();
        for worker in self.workers.values_mut() {
            if worker.status == WorkerStatus::Active {
                if let Ok(elapsed) = now.duration_since(worker.last_heartbeat) {
                    if elapsed > grace {
                        worker.status = WorkerStatus::Down;
                        warn!(worker_id = %worker.id, "marking worker as down (missed heartbeats)");
                    }
                }
            }
        }
    }
}
