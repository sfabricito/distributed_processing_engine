use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
pub struct JobResponse {
    pub job_id: String,
    #[serde(default)]
    pub status: String,
    #[serde(default)]
    pub progress: f32,
    #[serde(default)]
    pub stages: Vec<StageInfo>,
    #[serde(default)]
    pub tasks: Vec<TaskInfo>,
    #[serde(default)]
    pub metrics: Option<JobMetrics>,
    #[serde(default)]
    pub output_files: Vec<String>,
    #[serde(default)]
    pub outputs: Vec<String>,
    #[serde(default)]
    pub errors: Vec<String>,
    #[serde(default)]
    pub dag: Option<DagSpec>,
}

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
pub struct StageInfo {
    #[serde(default)]
    pub stage_id: Option<u64>,
    #[serde(default)]
    pub id: Option<String>,
    #[serde(default)]
    pub status: Option<String>,
    #[serde(default)]
    pub tasks_total: Option<usize>,
    #[serde(default)]
    pub tasks_running: Option<usize>,
    #[serde(default)]
    pub tasks_completed: Option<usize>,
    #[serde(default)]
    pub tasks_failed: Option<usize>,
    #[serde(default)]
    pub errors: Option<Vec<String>>,
}

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
pub struct TaskInfo {
    #[serde(default)]
    pub task_id: Option<String>,
    #[serde(default)]
    pub status: Option<String>,
    #[serde(default)]
    pub stage_id: Option<u64>,
}

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
pub struct JobMetrics {
    #[serde(default)]
    pub execution_time_ms: Option<u128>,
    #[serde(default)]
    pub tasks_total: Option<usize>,
    #[serde(default)]
    pub tasks_completed: Option<usize>,
    #[serde(default)]
    pub tasks_failed: Option<usize>,
    #[serde(default)]
    pub records_processed: Option<usize>,
    #[serde(default)]
    pub stages: Option<Vec<StageMetric>>,
    #[serde(default)]
    pub workers: Option<Vec<crate::client::types::metrics::WorkerAggregate>>,
}

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
pub struct StageMetric {
    pub stage_id: u64,
    #[serde(default)]
    pub duration_ms: u128,
    #[serde(default)]
    pub tasks_total: usize,
    #[serde(default)]
    pub tasks_completed: usize,
    #[serde(default)]
    pub tasks_failed: usize,
}

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
pub struct DagSpec {
    #[serde(default)]
    pub nodes: Vec<DagNode>,
    #[serde(default)]
    pub edges: Vec<DagEdge>,
}

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
pub struct DagNode {
    pub id: String,
    pub operator: serde_json::Value,
}

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
pub struct DagEdge {
    pub from: String,
    pub to: String,
}
