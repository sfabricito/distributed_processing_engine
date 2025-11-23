use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::{ExecutableOp, OpResult, PartitionData};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FilterOp {
    pub predicate: String,
}

impl ExecutableOp for FilterOp {
    fn execute(&self, partition: PartitionData) -> OpResult {
        let mut filtered = Vec::with_capacity(partition.records.len());

        for record in partition.records {
            let keep = match self.predicate.as_str() {
                "non_empty" => match &record {
                    Value::String(s) => !s.trim().is_empty(),
                    Value::Array(arr) => !arr.is_empty(),
                    Value::Object(map) => !map.is_empty(),
                    Value::Null => false,
                    _ => true,
                },
                "is_positive" => record
                    .as_f64()
                    .map(|v| v.is_sign_positive() && v != 0.0)
                    .unwrap_or(true),
                pred if pred.starts_with("contains:") => {
                    let needle = pred.trim_start_matches("contains:").to_lowercase();
                    record
                        .as_str()
                        .map(|s| s.to_lowercase().contains(&needle))
                        .unwrap_or(false)
                }
                _ => true,
            };

            if keep {
                filtered.push(record);
            }
        }

        Ok(PartitionData {
            records: filtered,
            partition_id: partition.partition_id,
        })
    }
}
