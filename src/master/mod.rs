use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::Duration,
};

use anyhow::Result;
use tokio::task::JoinHandle;
use tracing::{info, warn};
use uuid::Uuid;

use crate::common::state_store::StateStore;
use crate::common::{
    config::Config,
    dag::DagSpecification,
    types::{
        EngineError, HeartbeatMessage, JobId, JobStatus, Task, TaskId, TaskResult, TaskStatus,
        WorkerId, WorkerInfo, WorkerStatus,
    },
};
use crate::http;
use crate::master::scheduler::SchedulerStrategy;

pub mod registry;
pub mod scheduler;

pub struct Master {
    pub config: Config,
    pub registry: Arc<Mutex<registry::Registry>>,
    scheduler: Arc<Mutex<scheduler::RoundRobinScheduler>>,
    jobs: Arc<Mutex<HashMap<JobId, JobState>>>,
    state_store: StateStore,
}

#[derive(Debug, Clone)]
pub struct JobState {
    pub dag: DagSpecification,
    pub status: JobStatus,
    pub tasks: HashMap<TaskId, TaskStatus>,
    pub results: Vec<TaskResult>,
    pub metrics: crate::common::types::JobMetrics,
    pub error: Option<String>,
}

impl Master {
    pub fn new(config: Config) -> Arc<Self> {
        let state_store = StateStore::new(config.result_dir.clone());

        let master = Arc::new(Self {
            scheduler: Arc::new(Mutex::new(scheduler::RoundRobinScheduler::default())),
            registry: Arc::new(Mutex::new(registry::Registry::default())),
            jobs: Arc::new(Mutex::new(HashMap::new())),
            state_store,
            config,
        });

        // Load persisted jobs and tasks (best-effort; failures are logged but do not stop startup)
        if let Ok(saved_jobs) = master.state_store.load_all_jobs() {
            let mut jobs_map = master.jobs.lock().unwrap_or_else(|e| e.into_inner());
            for (job_id, status, dag) in saved_jobs {
                let job_state = JobState {
                    dag,
                    status,
                    tasks: HashMap::new(),
                    results: Vec::new(),
                    metrics: crate::common::types::JobMetrics::default(),
                    error: None,
                };
                jobs_map.insert(job_id, job_state);
            }
        }

        let mut tasks_to_requeue = Vec::new();
        if let Ok(saved_tasks) = master.state_store.load_all_tasks() {
            let mut jobs_map = master.jobs.lock().unwrap_or_else(|e| e.into_inner());
            for (task_id, job_id, task_status, opt_result) in saved_tasks {
                let normalized_status = match task_status {
                    TaskStatus::Assigned(_) | TaskStatus::Running(_) => {
                        tasks_to_requeue.push((task_id, job_id));
                        TaskStatus::Queued
                    }
                    other => other,
                };

                if let Some(job_state) = jobs_map.get_mut(&job_id) {
                    job_state.tasks.insert(task_id, normalized_status.clone());
                    if let Some(result) = opt_result {
                        if matches!(result.status, TaskStatus::Completed) {
                            job_state.results.push(result);
                        }
                    }
                } else {
                    // Orphan tasks: create a minimal placeholder job state so tasks aren't lost
                    let mut tasks = HashMap::new();
                    tasks.insert(task_id, normalized_status.clone());
                    let mut results = Vec::new();
                    if let Some(r) = opt_result {
                        if matches!(r.status, TaskStatus::Completed) {
                            results.push(r);
                        }
                    }
                    let placeholder = JobState {
                        dag: DagSpecification {
                            nodes: Vec::new(),
                            edges: Vec::new(),
                            partitions: 0,
                        },
                        status: JobStatus::Pending,
                        tasks,
                        results,
                        metrics: crate::common::types::JobMetrics::default(),
                        error: None,
                    };
                    jobs_map.insert(job_id, placeholder);
                }
            }
        }

        for (task_id, job_id) in tasks_to_requeue {
            let _ = master
                .state_store
                .persist_task_status(task_id, job_id, TaskStatus::Queued);
        }

        master
    }

    pub async fn start(self: Arc<Self>, base_path: &str) -> Result<()> {
        self.clone().spawn_watchdog();
        http::server::start_http_server(base_path, self.clone()).await
    }

    pub fn config(&self) -> &Config {
        &self.config
    }

    pub async fn submit_job(&self, dag: DagSpecification) -> Result<JobId> {
        dag.validate()?;
        let job_id = Uuid::new_v4();
        let tasks = dag.materialize_tasks(job_id);

        let mut job_state = JobState {
            dag: dag.clone(),
            status: JobStatus::Accepted,
            tasks: HashMap::new(),
            results: Vec::new(),
            metrics: crate::common::types::JobMetrics {
                total_tasks: tasks.len(),
                pending_tasks: tasks.len(),
                stages: dag
                    .to_stages()
                    .iter()
                    .map(|s| crate::common::types::StageMetrics {
                        stage_id: s.stage_id,
                        tasks_total: s.partitions,
                        ..Default::default()
                    })
                    .collect(),
                ..Default::default()
            },
            error: None,
        };

        // Persist the job row before tasks to satisfy the FK constraint.
        self.state_store
            .persist_job(job_id, &dag, JobStatus::Accepted)?;

        for task in &tasks {
            job_state.tasks.insert(task.task_id, TaskStatus::Queued);
            self.state_store
                .persist_task_status(task.task_id, job_id, TaskStatus::Queued)?;
        }

        let _ = self.state_store.persist_job_status(
            job_id,
            JobStatus::Accepted,
            0.0,
            &job_state.metrics,
            None,
            &[],
        );

        self.jobs
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(job_id, job_state);

        info!(job_id = %job_id, "new job submitted");
        for task in tasks {
            self.schedule_task(task).await?;
        }

        Ok(job_id)
    }

    pub fn get_job_status(
        &self,
        job_id: JobId,
    ) -> Result<crate::http::handlers::JobStatusResponse, EngineError> {
        let guard = self.jobs.lock().unwrap_or_else(|e| e.into_inner());
        let job = guard
            .get(&job_id)
            .ok_or_else(|| EngineError::NotFound(format!("job {job_id} not found")))?;

        let progress = if job.metrics.total_tasks == 0 {
            0.0
        } else {
            (job.metrics.completed_tasks as f32 / job.metrics.total_tasks as f32) * 100.0
        };
        let mut metrics = job.metrics.clone();
        if let Ok(registry) = self.registry.lock() {
            for worker in registry.available_workers() {
                let agg = crate::common::types::WorkerAggregate {
                    worker_id: worker.id,
                    tasks_completed: worker.metrics.tasks_completed,
                    tasks_failed: worker.metrics.tasks_failed,
                    records_processed: worker.metrics.records_processed,
                    cpu_time_ms: 0,
                    wall_time_ms: 0,
                };
                if let Some(existing) = metrics
                    .workers
                    .iter_mut()
                    .find(|w| w.worker_id == worker.id)
                {
                    *existing = agg;
                } else {
                    metrics.workers.push(agg);
                }
            }
        }
        Ok(crate::http::handlers::JobStatusResponse {
            job_id,
            status: job.status.clone(),
            progress,
            metrics,
            error: job.error.clone(),
            outputs: job
                .results
                .iter()
                .map(|r| r.result_location.path.clone())
                .collect(),
        })
    }

    pub fn get_job_results(&self, job_id: JobId) -> Result<Vec<TaskResult>, EngineError> {
        let guard = self.jobs.lock().unwrap_or_else(|e| e.into_inner());
        guard
            .get(&job_id)
            .map(|state| state.results.clone())
            .ok_or_else(|| EngineError::NotFound(format!("job {job_id} not found")))
    }

    pub fn handle_heartbeat(self: &Arc<Self>, heartbeat: HeartbeatMessage) {
        if let Ok(mut registry) = self.registry.lock() {
            registry.record_heartbeat(
                heartbeat.worker_id,
                heartbeat.metrics.clone(),
                heartbeat.address.clone(),
            );
        }

        let _ = self.state_store.persist_worker_metrics(
            heartbeat.worker_id,
            WorkerStatus::Active,
            &heartbeat.metrics,
        );

        let master = self.clone();
        tokio::spawn(async move {
            master.schedule_all_queued_tasks().await;
        });
    }

    pub fn register_worker(self: Arc<Self>, info: WorkerInfo) -> Result<WorkerId> {
        let worker_id = info.id;
        if let Ok(mut registry) = self.registry.lock() {
            registry.add_worker(info.clone());
        }
        info!(
            worker_id = %worker_id,
            address = %info.address,
            "worker registered"
        );

        // After registering a new worker, try to schedule any queued tasks across jobs.
        // We spawn a background task so the registration HTTP response is fast.
        let master = self.clone();
        tokio::spawn(async move {
            master.schedule_all_queued_tasks().await;
        });

        Ok(worker_id)
    }

    pub fn complete_task(&self, result: TaskResult) -> Result<()> {
        let mut jobs = self.jobs.lock().unwrap();
        if let Some(job_state) = jobs.get_mut(&result.job_id) {
            job_state
                .tasks
                .insert(result.task_id, result.status.clone());
            self.state_store.persist_task_result(&result)?;

            match &result.status {
                TaskStatus::Completed => {
                    job_state.results.push(result.clone());
                    job_state.metrics.completed_tasks += 1;
                    job_state.metrics.pending_tasks = job_state.metrics.total_tasks.saturating_sub(
                        job_state.metrics.completed_tasks + job_state.metrics.failed_tasks,
                    );
                    job_state
                        .metrics
                        .stages
                        .iter_mut()
                        .find(|s| s.stage_id == result.stage_id)
                        .map(|s| {
                            s.tasks_completed += 1;
                            s.duration_ms += result.metrics.duration_ms;
                        });
                    // per-worker aggregation (best effort from registry)
                    let worker_id = match job_state.tasks.get(&result.task_id) {
                        Some(TaskStatus::Assigned(w)) | Some(TaskStatus::Running(w)) => Some(*w),
                        _ => None,
                    };
                    if let Some(wid) = worker_id {
                        if let Some(worker_metric) = job_state
                            .metrics
                            .workers
                            .iter_mut()
                            .find(|w| w.worker_id == wid)
                        {
                            worker_metric.tasks_completed += 1;
                            worker_metric.records_processed += result.metrics.processed_records;
                            worker_metric.cpu_time_ms += result.metrics.cpu_time_ms;
                            worker_metric.wall_time_ms += result.metrics.wall_time_ms;
                        } else {
                            job_state
                                .metrics
                                .workers
                                .push(crate::common::types::WorkerAggregate {
                                    worker_id: wid,
                                    tasks_completed: 1,
                                    tasks_failed: 0,
                                    records_processed: result.metrics.processed_records,
                                    cpu_time_ms: result.metrics.cpu_time_ms,
                                    wall_time_ms: result.metrics.wall_time_ms,
                                });
                        }
                    }
                    let pending = job_state
                        .tasks
                        .values()
                        .any(|status| !matches!(status, TaskStatus::Completed));
                    if !pending {
                        job_state.status = JobStatus::Succeeded;
                        let dag = job_state.dag.clone();
                        let _ =
                            self.state_store
                                .persist_job(result.job_id, &dag, JobStatus::Succeeded);
                        let _ = self.state_store.persist_job_status(
                            result.job_id,
                            JobStatus::Succeeded,
                            100.0,
                            &job_state.metrics,
                            None,
                            &job_state
                                .results
                                .iter()
                                .map(|r| r.result_location.path.clone())
                                .collect::<Vec<_>>(),
                        );
                    } else {
                        let progress = if job_state.metrics.total_tasks == 0 {
                            0.0
                        } else {
                            (job_state.metrics.completed_tasks as f32
                                / job_state.metrics.total_tasks as f32)
                                * 100.0
                        };
                        let _ = self.state_store.persist_job_status(
                            result.job_id,
                            job_state.status.clone(),
                            progress,
                            &job_state.metrics,
                            job_state.error.clone(),
                            &job_state
                                .results
                                .iter()
                                .map(|r| r.result_location.path.clone())
                                .collect::<Vec<_>>(),
                        );
                    }
                }
                TaskStatus::Failed(_) => {
                    job_state.metrics.failed_tasks += 1;
                    job_state.metrics.pending_tasks = job_state.metrics.total_tasks.saturating_sub(
                        job_state.metrics.completed_tasks + job_state.metrics.failed_tasks,
                    );
                    job_state.status = JobStatus::Failed;
                    job_state.error = match &result.status {
                        TaskStatus::Failed(msg) => Some(msg.clone()),
                        _ => None,
                    };
                    if let Some(wid) = match job_state.tasks.get(&result.task_id) {
                        Some(TaskStatus::Assigned(w)) | Some(TaskStatus::Running(w)) => Some(*w),
                        _ => None,
                    } {
                        if let Some(worker_metric) = job_state
                            .metrics
                            .workers
                            .iter_mut()
                            .find(|w| w.worker_id == wid)
                        {
                            worker_metric.tasks_failed += 1;
                        } else {
                            job_state
                                .metrics
                                .workers
                                .push(crate::common::types::WorkerAggregate {
                                    worker_id: wid,
                                    tasks_completed: 0,
                                    tasks_failed: 1,
                                    records_processed: 0,
                                    cpu_time_ms: 0,
                                    wall_time_ms: 0,
                                });
                        }
                    }
                    let dag = job_state.dag.clone();
                    let _ = self
                        .state_store
                        .persist_job(result.job_id, &dag, JobStatus::Failed);
                    let progress = if job_state.metrics.total_tasks == 0 {
                        0.0
                    } else {
                        (job_state.metrics.completed_tasks as f32
                            / job_state.metrics.total_tasks as f32)
                            * 100.0
                    };
                    let _ = self.state_store.persist_job_status(
                        result.job_id,
                        JobStatus::Failed,
                        progress,
                        &job_state.metrics,
                        job_state.error.clone(),
                        &job_state
                            .results
                            .iter()
                            .map(|r| r.result_location.path.clone())
                            .collect::<Vec<_>>(),
                    );
                }
                _ => {}
            }
        }
        Ok(())
    }

    async fn schedule_task(&self, task: Task) -> Result<()> {
        let worker = {
            let registry = self.registry.lock().unwrap_or_else(|e| e.into_inner());
            let mut scheduler = self.scheduler.lock().unwrap_or_else(|e| e.into_inner());
            scheduler.select_worker(&registry, &task)
        };

        if let Some(worker_id) = worker {
            {
                let mut jobs = self.jobs.lock().unwrap_or_else(|e| e.into_inner());
                if let Some(job) = jobs.get_mut(&task.job_id) {
                    job.tasks
                        .insert(task.task_id, TaskStatus::Assigned(worker_id));
                }
            }

            // persist task assignment
            let _ = self.state_store.persist_task_status(
                task.task_id,
                task.job_id,
                TaskStatus::Assigned(worker_id),
            );

            // When a task is assigned to a worker, mark the job as Running (if not already)
            {
                let mut jobs = self.jobs.lock().unwrap_or_else(|e| e.into_inner());
                if let Some(job_state) = jobs.get_mut(&task.job_id) {
                    if job_state.status != JobStatus::Running {
                        job_state.status = JobStatus::Running;
                        let dag = job_state.dag.clone();
                        let _ = self
                            .state_store
                            .persist_job(task.job_id, &dag, JobStatus::Running);
                        let progress = if job_state.metrics.total_tasks == 0 {
                            0.0
                        } else {
                            (job_state.metrics.completed_tasks as f32
                                / job_state.metrics.total_tasks as f32)
                                * 100.0
                        };
                        let _ = self.state_store.persist_job_status(
                            task.job_id,
                            JobStatus::Running,
                            progress,
                            &job_state.metrics,
                            job_state.error.clone(),
                            &job_state
                                .results
                                .iter()
                                .map(|r| r.result_location.path.clone())
                                .collect::<Vec<_>>(),
                        );
                    }
                }
            }

            // Dispatch the task to the worker via HTTP.
            if let Some(worker_info) = {
                let registry = self.registry.lock().unwrap_or_else(|e| e.into_inner());
                registry.get_worker(&worker_id)
            } {
                let task_payload = task.clone();
                let url = format!("http://{}/api/v1/tasks/execute", worker_info.address);
                tokio::spawn(async move {
                    let client = reqwest::Client::new();
                    if let Err(err) = client.post(url).json(&task_payload).send().await {
                        warn!(
                            worker = %worker_id,
                            task_id = %task_payload.task_id,
                            "failed to dispatch task: {err}"
                        );
                    }
                });
            }

            info!(
                job_id = %task.job_id,
                task_id = %task.task_id,
                worker = %worker_id,
                "assigned task to worker"
            );
            // In a full implementation we would issue an HTTP call to the worker here.
        } else {
            warn!(
                job_id = %task.job_id,
                task_id = %task.task_id,
                "no available worker to schedule task"
            );
        }

        Ok(())
    }

    fn queued_tasks_snapshot(&self) -> Vec<Task> {
        let guard = self.jobs.lock().unwrap_or_else(|e| e.into_inner());
        let mut tasks = Vec::new();

        for (job_id, job_state) in guard.iter() {
            for task in job_state.dag.materialize_tasks(*job_id) {
                if matches!(job_state.tasks.get(&task.task_id), Some(TaskStatus::Queued)) {
                    tasks.push(task);
                }
            }
        }

        tasks
    }

    async fn schedule_all_queued_tasks(self: Arc<Self>) {
        let tasks = self.queued_tasks_snapshot();
        for task in tasks {
            let _ = self.clone().schedule_task(task).await;
        }
    }

    fn spawn_watchdog(self: Arc<Self>) -> JoinHandle<()> {
        tokio::spawn(async move {
            let mut ticker =
                tokio::time::interval(Duration::from_millis(self.config.heartbeat_interval_ms * 2));
            loop {
                ticker.tick().await;
                self.garbage_collect_workers();
            }
        })
    }

    fn garbage_collect_workers(&self) {
        let mut registry = self.registry.lock().unwrap_or_else(|e| e.into_inner());
        let deadline = Duration::from_millis(self.config.heartbeat_interval_ms * 2);
        registry.mark_stale_workers(deadline);
    }
}
