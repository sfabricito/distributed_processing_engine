use crate::common::dag::OperatorType;
use serde::{Deserialize, Serialize};
use std::time::SystemTime;
use thiserror::Error;
use uuid::Uuid;

pub type JobId = Uuid;
pub type TaskId = Uuid;
pub type WorkerId = Uuid;
pub type StageId = u64;
pub type PartitionId = u64;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum WorkerStatus {
    Active,
    Down,
    Idle,
    Running,
    Failed,
    Lost,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum JobStatus {
    Accepted,
    Pending,
    Running,
    Completed,
    Succeeded,
    Failed,
}

impl Default for JobStatus {
    fn default() -> Self {
        JobStatus::Pending
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum TaskStatus {
    Queued,
    Assigned(WorkerId),
    Running(WorkerId),
    Completed,
    Failed(String),
    Retrying(u32),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkerMetrics {
    pub tasks_in_flight: usize,
    pub cpu_pct: f32,
    pub memory_mb: usize,
    #[serde(default)]
    pub tasks_completed: usize,
    #[serde(default)]
    pub tasks_failed: usize,
    #[serde(default)]
    pub records_processed: usize,
    #[serde(default)]
    pub current_task: Option<TaskId>,
}

impl Default for WorkerMetrics {
    fn default() -> Self {
        Self {
            tasks_in_flight: 0,
            cpu_pct: 0.0,
            memory_mb: 0,
            tasks_completed: 0,
            tasks_failed: 0,
            records_processed: 0,
            current_task: None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkerInfo {
    pub id: WorkerId,
    pub address: String,
    pub status: WorkerStatus,
    pub last_heartbeat: SystemTime,
    pub metrics: WorkerMetrics,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Task {
    pub task_id: TaskId,
    pub job_id: JobId,
    pub stage_id: StageId,
    pub attempt: u32,
    pub operator: crate::common::dag::OperatorType,
    #[serde(default)]
    pub operators: Vec<crate::common::dag::OperatorType>,
    #[serde(default)]
    pub operator_ids: Vec<String>,
    #[serde(default)]
    pub incoming_edges: Vec<Vec<String>>,
    pub partition: PartitionId,
    pub input_uri: String,
    pub input_format: String,
    pub total_partitions: PartitionId,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskResult {
    pub task_id: TaskId,
    pub job_id: JobId,
    pub operator: OperatorType,
    pub stage_id: StageId,
    pub partition: PartitionId,
    pub result_location: ResultLocation,
    pub metrics: TaskMetrics,
    pub status: TaskStatus,
    #[serde(default)]
    pub trace: Option<ExecutionTrace>,
    #[serde(default)]
    pub final_partitions: Option<Vec<PartitionInfo>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskMetrics {
    pub processed_records: usize,
    pub duration_ms: u128,
    #[serde(default)]
    pub cpu_time_ms: u128,
    #[serde(default)]
    pub wall_time_ms: u128,
}

impl Default for TaskMetrics {
    fn default() -> Self {
        Self {
            processed_records: 0,
            duration_ms: 0,
            cpu_time_ms: 0,
            wall_time_ms: 0,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResultLocation {
    pub path: String,
    pub size_bytes: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ExecutionTrace {
    pub job_id: String,
    pub stages: Vec<StageTrace>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StageTrace {
    pub stage_id: usize,
    pub operator_id: String,
    pub incoming_edges: Vec<String>,
    pub output_partitions: Vec<PartitionInfo>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PartitionInfo {
    pub partition_id: usize,
    pub record_count: usize,
    pub spill_path: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct JobStateView {
    pub job_id: JobId,
    pub state: JobStatus,
    pub progress: f32,
    pub metrics: JobMetrics,
    pub error: Option<String>,
    pub outputs: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct JobMetrics {
    pub total_tasks: usize,
    pub completed_tasks: usize,
    pub failed_tasks: usize,
    pub pending_tasks: usize,
    #[serde(default)]
    pub stages: Vec<StageMetrics>,
    #[serde(default)]
    pub workers: Vec<WorkerAggregate>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct StageMetrics {
    pub stage_id: StageId,
    pub tasks_total: usize,
    pub tasks_completed: usize,
    pub tasks_failed: usize,
    pub duration_ms: u128,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct WorkerAggregate {
    pub worker_id: WorkerId,
    pub tasks_completed: usize,
    pub tasks_failed: usize,
    pub records_processed: usize,
    pub cpu_time_ms: u128,
    pub wall_time_ms: u128,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HeartbeatMessage {
    pub worker_id: WorkerId,
    pub address: String,
    pub metrics: WorkerMetrics,
    pub timestamp: SystemTime,
}

#[derive(Debug, Error)]
pub enum EngineError {
    #[error("network error: {0}")]
    Network(String),
    #[error("io error: {0}")]
    Io(String),
    #[error("invalid input: {0}")]
    Validation(String),
    #[error("not found: {0}")]
    NotFound(String),
}

impl From<std::io::Error> for EngineError {
    fn from(err: std::io::Error) -> Self {
        Self::Io(err.to_string())
    }
}

impl From<reqwest::Error> for EngineError {
    fn from(err: reqwest::Error) -> Self {
        Self::Network(err.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn serializes_task_status() {
        let status = TaskStatus::Retrying(2);
        let json = serde_json::to_string(&status).unwrap();
        assert!(json.contains("Retrying"));
    }
}
