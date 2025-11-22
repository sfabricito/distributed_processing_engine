use std::sync::Arc;
use std::time::Instant;

use anyhow::Result;
use tokio::task::JoinSet;
use tokio::time::Duration;
use tracing::{info, warn};

use crate::common::{
    config::Config,
    dag::OperatorType,
    types::{ResultLocation, Task, TaskMetrics, TaskResult},
};

use super::partition::PartitionStore;

pub struct Executor {
    config: Config,
    worker_id: uuid::Uuid,
    store: Arc<PartitionStore>,
}

impl Executor {
    pub fn new(config: Config, worker_id: uuid::Uuid) -> Self {
        Self {
            store: Arc::new(PartitionStore::new(config.clone())),
            config,
            worker_id,
        }
    }

    pub async fn run(&self) -> Result<()> {
        info!(worker_id = %self.worker_id, "executor ready to accept tasks");
        // In a full implementation this would block on a channel/HTTP server.
        loop {
            tokio::time::sleep(Duration::from_secs(3600)).await;
        }
    }

    pub async fn execute(&self, task: Task) -> Result<()> {
        let handle = self.execute_command(task.clone());
        let result = handle.await?;

        info!(
            worker_id = %self.worker_id,
            task_id = %task.task_id,
            duration_ms = result.metrics.duration_ms,
            "task finished"
        );

        // Normally notify master about completion here.
        Ok(())
    }

    async fn execute_command(&self, task: Task) -> Result<TaskResult> {
        let mut join_set = JoinSet::new();
        join_set.spawn(Self::apply_operator(
            task.clone(),
            self.store.clone(),
            self.config.clone(),
        ));

        let fallback = || TaskResult {
            task_id: task.task_id,
            job_id: task.job_id,
            stage_id: task.stage_id,
            partition: task.partition,
            result_location: ResultLocation {
                path: self
                    .store
                    .partition_path(task.job_id, task.stage_id, task.partition)
                    .display()
                    .to_string(),
                size_bytes: 0,
            },
            metrics: TaskMetrics {
                duration_ms: 0,
                processed_records: 0,
            },
        };

        let result = match join_set.join_next().await {
            Some(Ok(inner)) => inner?,
            Some(Err(err)) => {
                warn!(task_id = %task.task_id, "task join failed: {err}");
                fallback()
            }
            None => {
                warn!(task_id = %task.task_id, "task join set returned none");
                fallback()
            }
        };

        Ok(result)
    }

    async fn apply_operator(
        task: Task,
        store: Arc<PartitionStore>,
        _config: Config,
    ) -> Result<TaskResult> {
        let start = Instant::now();

        match task.operator {
            OperatorType::Map { .. } => tokio::time::sleep(Duration::from_millis(50)).await,
            OperatorType::Filter { .. } => tokio::time::sleep(Duration::from_millis(50)).await,
            OperatorType::Reduce { .. } => tokio::time::sleep(Duration::from_millis(100)).await,
            OperatorType::Join { .. } => tokio::time::sleep(Duration::from_millis(150)).await,
            OperatorType::Aggregate { .. } => tokio::time::sleep(Duration::from_millis(120)).await,
            OperatorType::Identity => tokio::time::sleep(Duration::from_millis(10)).await,
        }

        let duration_ms = start.elapsed().as_millis();
        let path = store.partition_path(task.job_id, task.stage_id, task.partition);
        store.write_placeholder(&path)?;

        Ok(TaskResult {
            task_id: task.task_id,
            job_id: task.job_id,
            stage_id: task.stage_id,
            partition: task.partition,
            result_location: ResultLocation {
                path: path.display().to_string(),
                size_bytes: 0,
            },
            metrics: TaskMetrics {
                processed_records: 0,
                duration_ms,
            },
        })
    }
}
