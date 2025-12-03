use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

use super::{ExecutableOp, OpResult, PartitionData};

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct FlatMapOp {
    pub func: String,
}

#[derive(thiserror::Error, Debug)]
pub enum FlatMapError {
    #[error("flat_map: unknown operation '{0}'")]
    UnknownOperation(String),
    #[error("flat_map: expected object for operation")]
    ExpectedObject,
}

impl ExecutableOp for FlatMapOp {
    fn execute(&self, partitions: Vec<PartitionData>) -> OpResult {
        let mut outputs = Vec::with_capacity(partitions.len());
        for partition in partitions.into_iter() {
            let pid = partition.partition_id;
            let (limit, spill, records) = partition.into_parts()?;
            let mut new_records = Vec::new();
            for record in records {
                let expanded = apply(&record, &self.func)?;
                new_records.extend(expanded);
            }
            outputs.push(PartitionData::from_records(pid, limit, spill, new_records)?);
        }
        Ok(outputs)
    }
}

fn apply(record: &Value, func: &str) -> Result<Vec<Value>, anyhow::Error> {
    if func == "identity" {
        return Ok(vec![record.clone()]);
    }

    if let Some(field) = func.strip_prefix("split:") {
        return split_field(record, field);
    }
    if let Some(field) = func.strip_prefix("explode_array:") {
        return explode_array(record, field);
    }
    if func == "json_expand" {
        return json_expand(record);
    }

    Err(anyhow::anyhow!(FlatMapError::UnknownOperation(
        func.to_string()
    )))
}

fn split_field(record: &Value, field: &str) -> Result<Vec<Value>, anyhow::Error> {
    let Value::Object(map) = record else {
        return Ok(vec![record.clone()]);
    };
    let Some(val) = map.get(field) else {
        return Ok(vec![record.clone()]);
    };
    if let Some(s) = val.as_str() {
        let tokens: Vec<String> = s
            .split(|c: char| c.is_whitespace() || c == ',')
            .filter(|t| !t.trim().is_empty())
            .map(|t| t.trim().to_string())
            .collect();
        if tokens.is_empty() {
            return Ok(vec![record.clone()]);
        }
        let mut out = Vec::with_capacity(tokens.len());
        for tok in tokens {
            let mut new_map = map.clone();
            new_map.insert(field.to_string(), Value::String(tok));
            out.push(Value::Object(new_map));
        }
        Ok(out)
    } else {
        Ok(vec![record.clone()])
    }
}

fn explode_array(record: &Value, field: &str) -> Result<Vec<Value>, anyhow::Error> {
    let Value::Object(map) = record else {
        return Ok(vec![record.clone()]);
    };
    let Some(val) = map.get(field) else {
        return Ok(vec![record.clone()]);
    };
    if let Some(arr) = val.as_array() {
        if arr.is_empty() {
            return Ok(vec![]);
        }
        let mut out = Vec::with_capacity(arr.len());
        for elem in arr {
            let mut new_map = map.clone();
            new_map.insert(field.to_string(), elem.clone());
            out.push(Value::Object(new_map));
        }
        Ok(out)
    } else {
        Ok(vec![record.clone()])
    }
}

fn json_expand(record: &Value) -> Result<Vec<Value>, anyhow::Error> {
    let Value::Object(map) = record else {
        return Ok(vec![record.clone()]);
    };
    let mut out = Vec::with_capacity(map.len().max(1));
    for (k, v) in map {
        let mut obj = Map::new();
        obj.insert(k.clone(), v.clone());
        out.push(Value::Object(obj));
    }
    if out.is_empty() {
        out.push(record.clone());
    }
    Ok(out)
}
