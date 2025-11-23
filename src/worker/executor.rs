use std::panic::{self, AssertUnwindSafe};
use std::sync::Arc;
use std::time::Instant;

use anyhow::Result;
use tokio::sync::Semaphore;
use tracing::{info, warn};

use crate::common::{
    config::Config,
    types::{ResultLocation, Task, TaskMetrics, TaskResult, TaskStatus},
};
use crate::worker::ops::{self, ExecutableOp};

use super::partition::PartitionStore;

pub struct Executor {
    config: Config,
    worker_id: uuid::Uuid,
    master_addr: String,
    store: Arc<PartitionStore>,
    client: reqwest::Client,
    permits: Arc<Semaphore>,
}

impl Executor {
    pub fn new(config: Config, worker_id: uuid::Uuid, master_addr: String) -> Self {
        let max_parallel = config.max_parallel_tasks.max(1);
        Self {
            store: Arc::new(PartitionStore::new(config.clone())),
            worker_id,
            master_addr,
            client: reqwest::Client::new(),
            permits: Arc::new(Semaphore::new(max_parallel)),
            config,
        }
    }

    pub async fn run(&self) -> Result<()> {
        info!(
            worker_id = %self.worker_id,
            "executor ready to accept tasks (idle loop)"
        );
        std::future::pending::<()>().await;
        Ok(())
    }

    /// Execute a task in an isolated thread, retrying on failure according to configuration.
    pub async fn execute_task(&self, task: Task) -> TaskResult {
        let _permit = self
            .permits
            .clone()
            .acquire_owned()
            .await
            .expect("semaphore closed");
        let mut attempt = 0;
        loop {
            attempt += 1;
            let result = self.execute_once(task.clone()).await;
            let is_failed = matches!(result.status, TaskStatus::Failed(_));
            if is_failed && attempt <= self.config.task_retry_limit {
                warn!(
                    task_id = %task.task_id,
                    attempt,
                    "task failed; retrying"
                );
                continue;
            }
            return result;
        }
    }

    /// Execute a task and POST its result back to the master.
    pub async fn execute_and_report(&self, task: Task) -> Result<TaskResult> {
        let result = self.execute_task(task).await;
        if let Err(err) = self.send_result_to_master(&result).await {
            warn!(task_id = %result.task_id, "failed to send result to master: {err}");
        }
        Ok(result)
    }

    async fn execute_once(&self, task: Task) -> TaskResult {
        let task_id = task.task_id;
        let job_id = task.job_id;
        let stage_id = task.stage_id;
        let partition = task.partition;
        let operator_def = task.operator.clone();
        let failure_path = self.store.partition_path(job_id, stage_id, partition);

        let store = self.store.clone();
        let config = self.config.clone();

        let join =
            tokio::task::spawn_blocking(move || Self::execute_sync(task, store, config)).await;

        match join {
            Ok(result) => result,
            Err(err) => {
                warn!("task join failed: {err}");
                Self::failed_result(
                    task_id,
                    job_id,
                    format!("{:?}", operator_def),
                    operator_def,
                    stage_id,
                    partition,
                    "task panicked while joining".into(),
                    failure_path,
                    0,
                )
            }
        }
    }

    fn execute_sync(task: Task, store: Arc<PartitionStore>, config: Config) -> TaskResult {
        let start = Instant::now();
        let partition_id = task.partition as usize;
        let total_partitions = config.max_parallel_tasks.max(1);
        let task_id = task.task_id;
        let job_id = task.job_id;
        let stage_id = task.stage_id;
        let partition = task.partition;
        let operator_def = task.operator.clone();

        let read_op =
            ops::read::ReadOp::new(task.input_uri.clone(), partition_id, total_partitions);
        let operator = ops::Operator::try_from(operator_def.clone())
            .unwrap_or_else(|_| ops::Operator::Read(read_op.clone()));
        let operator_name = operator.name().to_string();

        let pipeline = move || -> anyhow::Result<ops::PartitionData> {
            let base = read_op.execute(ops::PartitionData::empty(partition_id))?;
            let data = match &operator {
                ops::Operator::Read(op) => op.execute(base)?,
                ops::Operator::Map(op) => op.execute(base)?,
                ops::Operator::Filter(op) => op.execute(base)?,
                _ => operator.execute(base)?,
            };
            Ok(data)
        };

        let result_data = panic::catch_unwind(AssertUnwindSafe(pipeline));
        let duration_ms = start.elapsed().as_millis();
        let path = store.partition_path(job_id, stage_id, partition);

        match result_data {
            Ok(Ok(partition_data)) => {
                let processed_records = partition_data.records.len();
                let size_bytes = store
                    .write_records(&path, &partition_data.records)
                    .unwrap_or(0);
                TaskResult {
                    task_id,
                    job_id,
                    operator: operator_def,
                    stage_id,
                    partition,
                    result_location: ResultLocation {
                        path: path.display().to_string(),
                        size_bytes,
                    },
                    metrics: TaskMetrics {
                        processed_records,
                        duration_ms,
                    },
                    status: TaskStatus::Completed,
                }
            }
            Ok(Err(err)) => {
                let name = operator_name.clone();
                let message = format!("operator {name} failed: {err}");
                Self::failed_result(
                    task_id,
                    job_id,
                    name,
                    operator_def.clone(),
                    stage_id,
                    partition,
                    message,
                    path,
                    duration_ms,
                )
            }
            Err(_) => {
                let name = operator_name;
                let message = format!("operator {name} panicked");
                Self::failed_result(
                    task_id,
                    job_id,
                    name,
                    operator_def.clone(),
                    stage_id,
                    partition,
                    message,
                    path,
                    duration_ms,
                )
            }
        }
    }

    fn failed_result(
        task_id: uuid::Uuid,
        job_id: uuid::Uuid,
        operator_name: String,
        operator: crate::common::dag::OperatorType,
        stage_id: u64,
        partition: u64,
        error: String,
        path: std::path::PathBuf,
        duration_ms: u128,
    ) -> TaskResult {
        TaskResult {
            task_id,
            job_id,
            operator,
            stage_id,
            partition,
            result_location: ResultLocation {
                path: path.display().to_string(),
                size_bytes: 0,
            },
            metrics: TaskMetrics {
                processed_records: 0,
                duration_ms,
            },
            status: TaskStatus::Failed(format!("{operator_name}: {error}")),
        }
    }

    async fn send_result_to_master(&self, result: &TaskResult) -> Result<()> {
        let url = format!("http://{}/api/v1/tasks/complete", self.master_addr);
        self.client.post(url).json(result).send().await?;
        Ok(())
    }
}
