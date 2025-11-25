use anyhow::{anyhow};
use serde::{Deserialize, Serialize};
use serde_json::{Value, Number, Map};

use super::{ExecutableOp, OpResult, PartitionData};

#[derive(thiserror::Error, Debug)]
pub enum ReduceError {
    #[error("unknown reduce function: {0}")]
    UnknownFunction(String),

    #[error("expected object with key '{0}'")]
    MissingKey(String),

    #[error("target column '{0}' not found in record")]
    MissingTargetCol(String),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReduceOp {
    /// If `Some(key)` → reduce_by_key (groups data first)
    /// If `None`      → global reduce (aggregates everything)
    pub key: Option<String>,
    pub func: String,
    /// NEW: Specifies which field to aggregate (e.g., "TotalRevenue")
    pub target_col: Option<String>, 
}

impl ExecutableOp for ReduceOp {
    fn execute(&self, partition: PartitionData) -> OpResult {
        let result = if let Some(ref k) = self.key {
            // Group by key, then reduce each group
            reduce_by_key(&partition.records, k, &self.func, self.target_col.as_deref())?
        } else {
            // Reduce everything into a single value
            vec![reduce_global(&partition.records, &self.func, self.target_col.as_deref())?]
        };

        Ok(PartitionData {
            records: result,
            partition_id: partition.partition_id,
        })
    }
}

/// Aggregates a slice of records. 
/// If `col` is Some, it extracts that field before aggregating.
fn reduce_global(records: &[Value], func: &str, col: Option<&str>) -> Result<Value, anyhow::Error> {
    match func {
        "sum" => {
            let mut acc = 0.0;
            for r in records {
                // Logic: Use the whole record if no col specified, otherwise extract col
                let val_to_sum = if let Some(c) = col {
                    r.get(c)
                } else {
                    Some(r)
                };

                if let Some(v) = val_to_sum {
                    if let Some(n) = v.as_f64() {
                        acc += n;
                    }
                }
            }
            Ok(Value::Number(Number::from_f64(acc).unwrap()))
        }

        "count" => Ok(Value::Number(Number::from(records.len() as i64))),

        "concat" => {
            let mut acc = String::new();
            for r in records {
                let val_to_concat = if let Some(c) = col {
                    r.get(c)
                } else {
                    Some(r)
                };

                if let Some(v) = val_to_concat {
                    if let Some(s) = v.as_str() {
                        acc.push_str(s);
                    }
                }
            }
            Ok(Value::String(acc))
        }

        other => Err(anyhow!(ReduceError::UnknownFunction(other.to_string()))),
    }
}

fn reduce_by_key(
    records: &[Value],
    key: &str,
    func: &str,
    target_col: Option<&str>,
) -> Result<Vec<Value>, anyhow::Error> {

    // 1. Group records by the value of `key`
    let mut groups: std::collections::HashMap<String, Vec<Value>>
        = std::collections::HashMap::new();

    for r in records {
        let obj = r.as_object().ok_or_else(|| {
            anyhow!(ReduceError::MissingKey(key.to_string()))
        })?;

        let k_val = obj
            .get(key)
            .and_then(|v| v.as_str().map(|s| s.to_string()))
            .ok_or_else(|| anyhow!(ReduceError::MissingKey(key.to_string())))?;

        groups.entry(k_val).or_default().push(r.clone());
    }

    // 2. Reduce each group
    let mut out = Vec::new();

    for (k, vals) in groups {
        let reduced_val = reduce_global(&vals, func, target_col)?;
        
        let mut obj = Map::new();
        obj.insert(key.to_string(), Value::String(k));
        // We output the result in a standard "value" field
        obj.insert("value".to_string(), reduced_val);
        out.push(Value::Object(obj));
    }

    Ok(out)
}