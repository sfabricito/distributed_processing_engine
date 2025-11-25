use anyhow::{anyhow, Context};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::{ExecutableOp, OpResult, PartitionData};

#[derive(thiserror::Error, Debug)]
pub enum FlatMapError {
    #[error("unknown flat_map function: {0}")]
    UnknownFunction(String),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FlatMapOp {
    pub func: String,
    /// NEW: Optional field to apply the flat_map on (e.g. "Product")
    pub target_col: Option<String>,
}

impl ExecutableOp for FlatMapOp {
    fn execute(&self, partition: PartitionData) -> OpResult {
        let mut out = Vec::new();

        for record in partition.records {
            // 1. Determine the value to operate on
            let val_to_process = if let Some(ref col) = self.target_col {
                match record.get(col) {
                    Some(v) => v.clone(),
                    None => continue, // Skip records missing the target column
                }
            } else {
                record // Use the whole record if no column specified
            };

            // 2. Apply the function (tokenize, split, etc)
            let generated = apply_flat_fn(val_to_process, &self.func)
                .with_context(|| format!("flat_map applying '{}'", self.func))?;

            // 3. Flatten results into the output
            for v in generated {
                out.push(v);
            }
        }

        Ok(PartitionData {
            records: out,
            partition_id: partition.partition_id,
        })
    }
}

fn apply_flat_fn(value: Value, func: &str) -> Result<Vec<Value>, anyhow::Error> {
    // Handle "split:<delimiter>" logic
    if let Some(delim) = func.strip_prefix("split:") {
        return match value {
            Value::String(s) => {
                let toks = s
                    .split(delim)
                    .map(|t| Value::String(t.to_string()))
                    .collect();
                Ok(toks)
            }
            // If it's not a string, return as-is (or empty vec depending on preference)
            other => Ok(vec![other]),
        };
    }

    match func {
        "identity" => Ok(vec![value]),

        "tokenize" => match value {
            Value::String(s) => {
                let toks: Vec<Value> = s
                    .split_whitespace()
                    .map(|t| Value::String(t.to_string()))
                    .collect();
                Ok(toks)
            }
            other => Ok(vec![other]),
        },

        "explode_array" => match value {
            Value::Array(arr) => Ok(arr),
            other => Ok(vec![other]),
        },

        other => Err(anyhow!(FlatMapError::UnknownFunction(other.to_string()))),
    }
}