use std::{
    collections::HashMap,
    path::PathBuf,
    sync::{Arc, Mutex},
    time::Duration,
};

use anyhow::Result;
use tokio::task::JoinHandle;
use tracing::{info, warn};
use uuid::Uuid;

use crate::common::{
    config::Config,
    dag::DagSpecification,
    types::{
        EngineError, HeartbeatMessage, JobId, JobStatus, Task, TaskId, TaskResult, TaskStatus,
        WorkerId, WorkerInfo,
    },
};
use crate::master::scheduler::SchedulerStrategy;
use crate::http;

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
}

impl Master {
    pub fn new(config: Config) -> Arc<Self> {
        Arc::new(Self {
            scheduler: Arc::new(Mutex::new(scheduler::RoundRobinScheduler::default())),
            registry: Arc::new(Mutex::new(registry::Registry::default())),
            jobs: Arc::new(Mutex::new(HashMap::new())),
            state_store: StateStore::new(config.result_dir.clone()),
            config,
        })
    }

    pub async fn start(self: Arc<Self>, base_path: &str) -> Result<()> {
        self.clone().spawn_watchdog();
        http::server::start_http_server(base_path, self.clone()).await
    }

    pub fn config(&self) -> &Config {
        &self.config
    }

    pub async fn submit_job(&self, dag: DagSpecification) -> Result<JobId> {
        let job_id = Uuid::new_v4();
        let tasks = dag.materialize_tasks(job_id);

        let mut job_state = JobState {
            dag: dag.clone(),
            status: JobStatus::Pending,
            tasks: HashMap::new(),
            results: Vec::new(),
        };

        for task in &tasks {
            job_state.tasks.insert(task.task_id, TaskStatus::Queued);
        }

        self.jobs.lock().unwrap().insert(job_id, job_state);
        self.state_store.persist_job(job_id, &dag)?;

        info!(job_id = %job_id, "new job submitted");
        for task in tasks {
            self.schedule_task(task).await?;
        }

        if let Some(job_state) = self.jobs.lock().unwrap().get_mut(&job_id) {
            job_state.status = JobStatus::Running;
        }

        Ok(job_id)
    }

    pub fn get_job_status(&self, job_id: JobId) -> Result<JobStatus, EngineError> {
        let guard = self.jobs.lock().unwrap();
        guard
            .get(&job_id)
            .map(|state| state.status.clone())
            .ok_or_else(|| EngineError::NotFound(format!("job {job_id} not found")))
    }

    pub fn get_job_results(&self, job_id: JobId) -> Result<Vec<TaskResult>, EngineError> {
        let guard = self.jobs.lock().unwrap();
        guard
            .get(&job_id)
            .map(|state| state.results.clone())
            .ok_or_else(|| EngineError::NotFound(format!("job {job_id} not found")))
    }

    pub fn handle_heartbeat(&self, heartbeat: HeartbeatMessage) {
        if let Ok(mut registry) = self.registry.lock() {
            registry.record_heartbeat(
                heartbeat.worker_id,
                heartbeat.metrics,
                heartbeat.address.clone(),
            );
        }
    }

    pub fn register_worker(&self, info: WorkerInfo) -> Result<WorkerId> {
        let worker_id = info.id;
        if let Ok(mut registry) = self.registry.lock() {
            registry.add_worker(info.clone());
        }
        info!(
            worker_id = %worker_id,
            address = %info.address,
            "worker registered"
        );
        Ok(worker_id)
    }

    pub fn complete_task(&self, result: TaskResult) -> Result<()> {
        let mut jobs = self.jobs.lock().unwrap();
        if let Some(job_state) = jobs.get_mut(&result.job_id) {
            job_state.tasks.insert(result.task_id, TaskStatus::Completed);
            job_state.results.push(result.clone());
            self.state_store.persist_task_result(&result)?;

            let pending = job_state
                .tasks
                .values()
                .any(|status| !matches!(status, TaskStatus::Completed));
            if !pending {
                job_state.status = JobStatus::Completed;
            }
        }
        Ok(())
    }

    async fn schedule_task(&self, task: Task) -> Result<()> {
        let worker = {
            let registry = self.registry.lock().unwrap();
            let mut scheduler = self.scheduler.lock().unwrap();
            scheduler.select_worker(&registry, &task)
        };

        if let Some(worker_id) = worker {
            {
                let mut jobs = self.jobs.lock().unwrap();
                if let Some(job) = jobs.get_mut(&task.job_id) {
                    job.tasks
                        .insert(task.task_id, TaskStatus::Assigned(worker_id));
                }
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

    fn spawn_watchdog(self: Arc<Self>) -> JoinHandle<()> {
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(Duration::from_millis(
                self.config.heartbeat_interval_ms * 2,
            ));
            loop {
                ticker.tick().await;
                self.garbage_collect_workers();
            }
        })
    }

    fn garbage_collect_workers(&self) {
        let mut registry = self.registry.lock().unwrap();
        let deadline = Duration::from_millis(self.config.heartbeat_interval_ms * 2);
        registry.mark_stale_workers(deadline);
    }
}

#[derive(Debug, Clone)]
pub struct StateStore {
    base_path: PathBuf,
}

impl StateStore {
    pub fn new(base_path: PathBuf) -> Self {
        let _ = std::fs::create_dir_all(&base_path);
        Self { base_path }
    }

    pub fn persist_job(&self, job_id: JobId, dag: &DagSpecification) -> Result<()> {
        let path = self.base_path.join(format!("{job_id}.dag.json"));
        let json = serde_json::to_string_pretty(dag)?;
        std::fs::write(path, json)?;
        Ok(())
    }

    pub fn persist_task_result(&self, result: &TaskResult) -> Result<()> {
        let path = self.base_path.join(format!("{}_result.json", result.task_id));
        let json = serde_json::to_string_pretty(result)?;
        std::fs::write(path, json)?;
        Ok(())
    }
}
