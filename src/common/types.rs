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
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum JobStatus {
    Pending,
    Running,
    Completed,
    Failed,
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
}

impl Default for WorkerMetrics {
    fn default() -> Self {
        Self {
            tasks_in_flight: 0,
            cpu_pct: 0.0,
            memory_mb: 0,
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
    pub partition: PartitionId,
    pub input_uri: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskResult {
    pub task_id: TaskId,
    pub job_id: JobId,
    pub stage_id: StageId,
    pub partition: PartitionId,
    pub result_location: ResultLocation,
    pub metrics: TaskMetrics,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskMetrics {
    pub processed_records: usize,
    pub duration_ms: u128,
}

impl Default for TaskMetrics {
    fn default() -> Self {
        Self {
            processed_records: 0,
            duration_ms: 0,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResultLocation {
    pub path: String,
    pub size_bytes: u64,
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
