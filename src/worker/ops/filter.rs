use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::{ExecutableOp, OpResult, PartitionData};

#[derive(Debug, thiserror::Error)]
pub enum FilterError {
    #[error("unknown predicate: {0}")]
    UnknownPredicate(String),
    #[error("invalid predicate: {0}")]
    InvalidPredicate(String),
}

#[derive(Debug, Clone)]
enum Predicate {
    AlwaysTrue,
    AlwaysFalse,
    NonEmpty,
    HasField(String),
    Equals(String, Value),
    Gt(String, f64),
    Lt(String, f64),
}

impl Predicate {
    fn parse(raw: &str) -> Result<Self, FilterError> {
        if raw == "non_empty" {
            return Ok(Predicate::NonEmpty);
        }
        if raw == "always_true" {
            return Ok(Predicate::AlwaysTrue);
        }
        if raw == "always_false" {
            return Ok(Predicate::AlwaysFalse);
        }
        if let Some(rest) = raw.strip_prefix("has_field:") {
            return Ok(Predicate::HasField(rest.to_string()));
        }
        if let Some(rest) = raw.strip_prefix("equals:") {
            let mut parts = rest.splitn(2, '=');
            let field = parts
                .next()
                .filter(|s| !s.is_empty())
                .ok_or_else(|| FilterError::InvalidPredicate(raw.to_string()))?;
            let val_str = parts
                .next()
                .ok_or_else(|| FilterError::InvalidPredicate(raw.to_string()))?;
            let val = Value::String(val_str.to_string());
            return Ok(Predicate::Equals(field.to_string(), val));
        }
        if let Some(rest) = raw.strip_prefix("gt:") {
            let mut parts = rest.splitn(2, '=');
            let field = parts
                .next()
                .filter(|s| !s.is_empty())
                .ok_or_else(|| FilterError::InvalidPredicate(raw.to_string()))?;
            let num: f64 = parts
                .next()
                .ok_or_else(|| FilterError::InvalidPredicate(raw.to_string()))?
                .parse()
                .map_err(|_| FilterError::InvalidPredicate(raw.to_string()))?;
            return Ok(Predicate::Gt(field.to_string(), num));
        }
        if let Some(rest) = raw.strip_prefix("lt:") {
            let mut parts = rest.splitn(2, '=');
            let field = parts
                .next()
                .filter(|s| !s.is_empty())
                .ok_or_else(|| FilterError::InvalidPredicate(raw.to_string()))?;
            let num: f64 = parts
                .next()
                .ok_or_else(|| FilterError::InvalidPredicate(raw.to_string()))?
                .parse()
                .map_err(|_| FilterError::InvalidPredicate(raw.to_string()))?;
            return Ok(Predicate::Lt(field.to_string(), num));
        }

        Err(FilterError::UnknownPredicate(raw.to_string()))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FilterOp {
    pub predicate: String,
}

impl ExecutableOp for FilterOp {
    fn execute(&self, partitions: Vec<PartitionData>) -> OpResult {
        let predicate = Predicate::parse(&self.predicate).map_err(|e| anyhow::anyhow!(e))?;

        let mut outputs = Vec::with_capacity(partitions.len());
        for partition in partitions {
            let mut filtered = Vec::with_capacity(partition.records.len());
            for record in partition.records {
                if eval_predicate(&record, &predicate) {
                    filtered.push(record);
                }
            }
            outputs.push(PartitionData {
                records: filtered,
                partition_id: partition.partition_id,
            });
        }

        Ok(outputs)
    }
}

fn eval_predicate(record: &Value, pred: &Predicate) -> bool {
    match pred {
        Predicate::AlwaysTrue => true,
        Predicate::AlwaysFalse => false,
        Predicate::NonEmpty => match record {
            Value::Object(map) => {
                !map.is_empty() && map.values().all(|v| !matches!(v, Value::Null))
            }
            _ => false,
        },
        Predicate::HasField(field) => match record {
            Value::Object(map) => map.contains_key(field),
            _ => false,
        },
        Predicate::Equals(field, expected) => match record {
            Value::Object(map) => match (map.get(field), expected) {
                (Some(Value::String(actual)), Value::String(exp)) => {
                    actual.to_lowercase() == exp.to_lowercase()
                }
                (Some(val), exp) => val == exp,
                _ => false,
            },
            _ => false,
        },
        Predicate::Gt(field, threshold) => match record {
            Value::Object(map) => map
                .get(field)
                .and_then(|v| v.as_f64())
                .map(|n| n > *threshold)
                .unwrap_or(false),
            _ => false,
        },
        Predicate::Lt(field, threshold) => match record {
            Value::Object(map) => map
                .get(field)
                .and_then(|v| v.as_f64())
                .map(|n| n < *threshold)
                .unwrap_or(false),
            _ => false,
        },
    }
}
