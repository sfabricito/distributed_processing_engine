use anyhow::anyhow;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::{ExecutableOp, OpResult, PartitionData};

#[derive(thiserror::Error, Debug)]
pub enum MapError {
    #[error("unknown map function: {0}")]
    UnknownFunction(String),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MapOp {
    pub func: String,
}

impl ExecutableOp for MapOp {
    fn execute(&self, partitions: Vec<PartitionData>) -> OpResult {
        let mut outputs = Vec::with_capacity(partitions.len());

        for partition in partitions.into_iter() {
            let pid = partition.partition_id;
            let (limit, spill_path, records) = partition.into_parts()?;
            let mut mapped = Vec::with_capacity(records.len());

            for record in records {
                let transformed = apply_fn(record, self.func.as_str())?;

                mapped.push(transformed);
            }

            outputs.push(PartitionData::from_records(pid, limit, spill_path, mapped)?);
        }

        Ok(outputs)
    }
}

fn apply_fn(value: Value, func: &str) -> Result<Value, anyhow::Error> {
    match value {
        Value::String(s) => match func {
            "identity" => Ok(Value::String(s)),
            "uppercase" => Ok(Value::String(s.to_uppercase())),
            "lowercase" => Ok(Value::String(s.to_lowercase())),
            "double" => {
                // Best-effort: try to double numeric-looking strings, otherwise leave unchanged.
                if let Ok(i) = s.parse::<i64>() {
                    Ok(Value::Number(serde_json::Number::from(i * 2)))
                } else if let Ok(f) = s.parse::<f64>() {
                    serde_json::Number::from_f64(f * 2.0)
                        .map(Value::Number)
                        .ok_or_else(|| anyhow!("failed to convert doubled float from string"))
                } else {
                    Ok(Value::String(s))
                }
            }
            _ => Err(anyhow!(MapError::UnknownFunction(func.to_string()))),
        },
        Value::Number(n) => match func {
            "double" => {
                if let Some(i) = n.as_i64() {
                    Ok(Value::Number(serde_json::Number::from(i * 2)))
                } else if let Some(f) = n.as_f64() {
                    serde_json::Number::from_f64(f * 2.0)
                        .map(Value::Number)
                        .ok_or_else(|| anyhow!("failed to convert doubled float"))
                } else {
                    Ok(Value::Null)
                }
            }
            "identity" | "uppercase" | "lowercase" => Ok(Value::Number(n)),
            _ => Err(anyhow!(MapError::UnknownFunction(func.to_string()))),
        },
        Value::Array(arr) => {
            let mut out = Vec::with_capacity(arr.len());
            for v in arr {
                out.push(apply_fn(v, func)?);
            }
            Ok(Value::Array(out))
        }
        Value::Object(mut map) => {
            for val in map.values_mut() {
                let new_val = apply_fn(val.take(), func)?;
                *val = new_val;
            }
            Ok(Value::Object(map))
        }
        other => Ok(other),
    }
}
