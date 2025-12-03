use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
pub struct WorkerInfo {
    #[serde(alias = "worker_id", alias = "id")]
    pub id: String,
    #[serde(default)]
    pub address: Option<String>,
    #[serde(default)]
    pub status: Option<String>,
    #[serde(default)]
    pub last_heartbeat: Option<String>,
    #[serde(default)]
    pub metrics: Option<WorkerMetrics>,
}

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
pub struct WorkerMetrics {
    #[serde(default)]
    pub tasks_in_flight: usize,
    #[serde(default)]
    pub cpu_pct: f32,
    #[serde(default)]
    pub memory_mb: usize,
    #[serde(default)]
    pub tasks_completed: usize,
    #[serde(default)]
    pub tasks_failed: usize,
    #[serde(default)]
    pub records_processed: usize,
    #[serde(default)]
    pub current_task: Option<String>,
}
