use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::{ExecutableOp, OpResult, PartitionData};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MapOp {
    pub func: String,
}

impl ExecutableOp for MapOp {
    fn execute(&self, partition: PartitionData) -> OpResult {
        let mut mapped = Vec::with_capacity(partition.records.len());
        for record in partition.records {
            let next = match self.func.as_str() {
                "identity" => record,
                "uppercase" => match record {
                    Value::String(s) => Value::String(s.to_uppercase()),
                    other => other,
                },
                "lowercase" => match record {
                    Value::String(s) => Value::String(s.to_lowercase()),
                    other => other,
                },
                "double" => match record {
                    Value::Number(n) => n
                        .as_f64()
                        .map(|v| Value::from(v * 2.0))
                        .unwrap_or(Value::Null),
                    other => other,
                },
                _ => record,
            };
            mapped.push(next);
        }

        Ok(PartitionData {
            records: mapped,
            partition_id: partition.partition_id,
        })
    }
}
