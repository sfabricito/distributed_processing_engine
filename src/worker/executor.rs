use std::panic::{self, AssertUnwindSafe};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc, Mutex,
};
use std::time::Instant;

use anyhow::Result;
use tokio::sync::Semaphore;
use tracing::{info, warn};

use crate::common::{
    config::Config,
    types::{
        ExecutionTrace, PartitionInfo, ResultLocation, StageTrace, Task, TaskMetrics, TaskResult,
        TaskStatus,
    },
};
use crate::worker::ops;

use super::partition::PartitionStore;

pub struct Executor {
    config: Config,
    worker_id: uuid::Uuid,
    master_addr: String,
    store: Arc<PartitionStore>,
    client: reqwest::Client,
    permits: Arc<Semaphore>,
    tasks_completed: AtomicUsize,
    tasks_failed: AtomicUsize,
    records_processed: AtomicUsize,
    active_task: Mutex<Option<uuid::Uuid>>,
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
            tasks_completed: AtomicUsize::new(0),
            tasks_failed: AtomicUsize::new(0),
            records_processed: AtomicUsize::new(0),
            active_task: Mutex::new(None),
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
            {
                let mut guard = self.active_task.lock().unwrap_or_else(|e| e.into_inner());
                *guard = Some(task.task_id);
            }
            let result = self.execute_once(task.clone()).await;
            let is_failed = matches!(result.status, TaskStatus::Failed(_));
            match result.status {
                TaskStatus::Completed => {
                    self.tasks_completed.fetch_add(1, Ordering::Relaxed);
                    self.records_processed
                        .fetch_add(result.metrics.processed_records, Ordering::Relaxed);
                }
                TaskStatus::Failed(_) => {
                    self.tasks_failed.fetch_add(1, Ordering::Relaxed);
                }
                _ => {}
            }
            if is_failed && attempt <= self.config.task_retry_limit {
                warn!(
                    task_id = %task.task_id,
                    attempt,
                    "task failed; retrying"
                );
                continue;
            }
            {
                let mut guard = self.active_task.lock().unwrap_or_else(|e| e.into_inner());
                *guard = None;
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

    pub fn snapshot_metrics(&self) -> crate::common::types::WorkerMetrics {
        let max_parallel = self.config.max_parallel_tasks.max(1);
        let available = self.permits.available_permits();
        crate::common::types::WorkerMetrics {
            tasks_in_flight: max_parallel.saturating_sub(available),
            cpu_pct: 0.0,
            memory_mb: 0,
            tasks_completed: self.tasks_completed.load(Ordering::Relaxed),
            tasks_failed: self.tasks_failed.load(Ordering::Relaxed),
            records_processed: self.records_processed.load(Ordering::Relaxed),
            current_task: self
                .active_task
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .clone(),
        }
    }

    async fn execute_once(&self, task: Task) -> TaskResult {
        let task_id = task.task_id;
        let job_id = task.job_id;
        let stage_id = task
            .operators
            .last()
            .map(|_| task.operators.len().saturating_sub(1) as u64)
            .unwrap_or(task.stage_id);
        let partition = task.partition;
        let operator_def = task
            .operators
            .last()
            .cloned()
            .unwrap_or_else(|| task.operator.clone());
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
        let total_partitions = task.total_partitions.max(1) as usize;
        let task_id = task.task_id;
        let job_id = task.job_id;
        let partition = task.partition;
        let cache_limit = config.partition_cache_limit_bytes;
        let operator_ids = if task.operator_ids.is_empty() {
            (0..task.operators.len())
                .map(|idx| format!("op-{idx}"))
                .collect()
        } else {
            task.operator_ids.clone()
        };
        let incoming_edges = if task.incoming_edges.is_empty() {
            vec![Vec::new(); operator_ids.len()]
        } else {
            task.incoming_edges.clone()
        };

        let operator_defs = if task.operators.is_empty() {
            vec![task.operator.clone()]
        } else {
            task.operators.clone()
        };
        let final_stage_id = operator_defs.len().saturating_sub(1) as u64;
        let spill_path = store.spill_path(job_id, final_stage_id, partition);
        let reported_operator = operator_defs
            .last()
            .cloned()
            .unwrap_or_else(|| task.operator.clone());

        let operators: Vec<ops::Operator> = match operator_defs
            .iter()
            .map(|op_def| {
                ops::Operator::from_type(
                    op_def.clone(),
                    partition_id,
                    total_partitions,
                    cache_limit,
                    spill_path.clone(),
                )
            })
            .collect()
        {
            Ok(ops) => ops,
            Err(err) => {
                let name = format!("{:?}", reported_operator);
                let path = store.partition_path(job_id, final_stage_id, partition);
                return Self::failed_result(
                    task_id,
                    job_id,
                    name,
                    reported_operator,
                    final_stage_id,
                    partition,
                    format!("failed to build operator pipeline: {err}"),
                    path,
                    0,
                );
            }
        };

        let operator_name = operators
            .last()
            .map(|op| op.name().to_string())
            .unwrap_or_else(|| "unknown".into());

        let op_ids_for_trace = operator_ids.clone();
        let incoming_edges_for_trace = incoming_edges.clone();
        let job_for_trace = job_id;
        let pipeline = move || -> anyhow::Result<(Vec<ops::PartitionData>, ExecutionTrace)> {
            let mut partitions = vec![ops::PartitionData::empty(
                partition_id,
                cache_limit,
                spill_path.clone(),
            )];
            let mut trace = ExecutionTrace {
                job_id: job_for_trace.to_string(),
                stages: Vec::new(),
            };

            for (idx, op) in operators.into_iter().enumerate() {
                let total_records: usize = partitions.iter().map(|p| p.record_count()).sum();
                let op_name = op.name();
                info!(
                    job = %job_for_trace,
                    stage = idx,
                    operator = op_name,
                    partitions = partitions.len(),
                    records = total_records,
                    "starting operator"
                );
                partitions = op.execute(partitions)?;
                let stage_trace = StageTrace {
                    stage_id: idx,
                    operator_id: op_ids_for_trace
                        .get(idx)
                        .cloned()
                        .unwrap_or_else(|| op.name().to_string()),
                    incoming_edges: incoming_edges_for_trace
                        .get(idx)
                        .cloned()
                        .unwrap_or_default(),
                    output_partitions: partitions
                        .iter()
                        .map(|p| PartitionInfo {
                            partition_id: p.partition_id,
                            record_count: p.record_count(),
                            spill_path: p.has_spill().then(|| p.spill_path().display().to_string()),
                        })
                        .collect(),
                };
                trace.stages.push(stage_trace);
                let total_records: usize = partitions.iter().map(|p| p.record_count()).sum();
                info!(
                    job = %job_for_trace,
                    stage = idx,
                    operator = op_name,
                    partitions = partitions.len(),
                    records = total_records,
                    "completed operator"
                );
            }

            Ok((partitions, trace))
        };

        let result_data = panic::catch_unwind(AssertUnwindSafe(pipeline));
        let duration_ms = start.elapsed().as_millis();
        let path = store.partition_path(job_id, final_stage_id, partition);

        match result_data {
            Ok(Ok((partitions, mut trace))) => {
                let processed_records: usize = partitions.iter().map(|p| p.record_count()).sum();
                let write_result = (|| -> anyhow::Result<u64> {
                    let mut final_bytes = 0u64;
                    let mut final_partitions = Vec::new();
                    for partition_data in partitions {
                        let pid = partition_data.partition_id;
                        let data_path = store.partition_path(job_id, final_stage_id, pid as u64);
                        let (_, _, records) = partition_data.into_parts()?;
                        let record_count = records.len();
                        let bytes = store.write_records(&data_path, &records)?;
                        final_partitions.push(PartitionInfo {
                            partition_id: pid,
                            record_count,
                            spill_path: None,
                        });
                        if pid as u64 == partition {
                            final_bytes = bytes;
                        }
                    }
                    trace.stages.last_mut().map(|s| {
                        s.output_partitions = final_partitions.clone();
                    });
                    Ok(final_bytes)
                })();

                match write_result {
                    Ok(final_bytes) => {
                        let final_partitions =
                            trace.stages.last().map(|s| s.output_partitions.clone());
                        TaskResult {
                            task_id,
                            job_id,
                            operator: reported_operator,
                            stage_id: final_stage_id,
                            partition,
                            result_location: ResultLocation {
                                path: path.display().to_string(),
                                size_bytes: final_bytes,
                            },
                            metrics: TaskMetrics {
                                processed_records,
                                duration_ms,
                                cpu_time_ms: duration_ms,
                                wall_time_ms: duration_ms,
                            },
                            status: TaskStatus::Completed,
                            trace: Some(trace.clone()),
                            final_partitions,
                        }
                    }
                    Err(err) => {
                        let name = operator_name.clone();
                        let message = format!("operator {name} failed writing output: {err}");
                        Self::failed_result(
                            task_id,
                            job_id,
                            name,
                            reported_operator.clone(),
                            final_stage_id,
                            partition,
                            message,
                            path,
                            duration_ms,
                        )
                    }
                }
            }
            Ok(Err(err)) => {
                let name = operator_name.clone();
                let message = format!("operator {name} failed: {err}");
                Self::failed_result(
                    task_id,
                    job_id,
                    name,
                    reported_operator.clone(),
                    final_stage_id,
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
                    reported_operator.clone(),
                    final_stage_id,
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
                cpu_time_ms: duration_ms,
                wall_time_ms: duration_ms,
            },
            status: TaskStatus::Failed(format!("{operator_name}: {error}")),
            trace: None,
            final_partitions: None,
        }
    }

    async fn send_result_to_master(&self, result: &TaskResult) -> Result<()> {
        let url = format!("http://{}/api/v1/tasks/complete", self.master_addr);
        self.client.post(url).json(result).send().await?;
        Ok(())
    }
}
