use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
pub struct WorkerAggregate {
    pub worker_id: String,
    #[serde(default)]
    pub tasks_completed: usize,
    #[serde(default)]
    pub tasks_failed: usize,
    #[serde(default)]
    pub records_processed: usize,
    #[serde(default)]
    pub cpu_time_ms: u128,
    #[serde(default)]
    pub wall_time_ms: u128,
}
