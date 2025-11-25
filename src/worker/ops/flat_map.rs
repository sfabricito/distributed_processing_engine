use anyhow::{anyhow, Context};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

use super::{ExecutableOp, OpResult, PartitionData};

#[derive(thiserror::Error, Debug)]
pub enum FlatMapError {
    #[error("unknown flat_map function: {0}")]
    UnknownFunction(String),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FlatMapOp {
    pub func: String,
    /// Optional column to pull input from (defaults to entire record)
    pub input_col: Option<String>,
    /// Optional column to write output into (defaults to emitting raw values)
    pub target_col: Option<String>,
}

impl ExecutableOp for FlatMapOp {
    fn execute(&self, partition: PartitionData) -> OpResult {
        let mut out = Vec::new();

        for record in partition.records {
            let source_value = match self.input_col.as_deref() {
                Some(col) => match record.get(col) {
                    Some(v) => v.clone(),
                    None => continue,
                },
                None => record.clone(),
            };

            let generated = apply_flat_fn(source_value, &self.func)
                .with_context(|| format!("flat_map applying '{}'", self.func))?;

            for v in generated {
                if let Some(ref target) = self.target_col {
                    let mut obj = match record.as_object() {
                        Some(map) => map.clone(),
                        None => Map::new(),
                    };
                    obj.insert(target.clone(), v);
                    out.push(Value::Object(obj));
                } else {
                    out.push(v);
                }
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
