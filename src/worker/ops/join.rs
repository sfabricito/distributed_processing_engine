use std::collections::HashMap;

use anyhow::anyhow;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

use crate::common::dag::JoinType;

use super::{ExecutableOp, OpResult, PartitionData};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JoinOp {
    pub key: String,
    pub join_type: JoinType,
}

#[derive(thiserror::Error, Debug)]
pub enum JoinError {
    #[error("JoinOp requires exactly two input partitions (received {0})")]
    WrongInputCount(usize),
    #[error("JoinOp: record is not an object")]
    NonObject,
    #[error("JoinOp: missing key \"{0}\"")]
    MissingKey(String),
    #[error("JoinOp: join key must be string or number")]
    InvalidKey,
}

impl ExecutableOp for JoinOp {
    fn execute(&self, partitions: Vec<PartitionData>) -> OpResult {
        if partitions.len() != 2 {
            return Err(anyhow!(JoinError::WrongInputCount(partitions.len())));
        }

        let mut parts_iter = partitions.into_iter();
        let left = parts_iter.next().unwrap();
        let right = parts_iter.next().unwrap();

        let pid = left.partition_id.min(right.partition_id);
        let limit = left.limit_bytes();
        let spill = left.spill_path();

        let (_, _, left_records) = left.into_parts()?;
        let (_, _, right_records) = right.into_parts()?;

        let mut right_map: HashMap<String, Vec<Map<String, Value>>> = HashMap::new();
        let mut right_index: Vec<(String, Map<String, Value>)> = Vec::new();

        for rec in right_records {
            let obj = rec
                .as_object()
                .cloned()
                .ok_or_else(|| anyhow!(JoinError::NonObject))?;
            let key = extract_key(&obj, &self.key)?;
            right_map.entry(key.clone()).or_default().push(obj.clone());
            right_index.push((key, obj));
        }

        let mut matched_right = vec![false; right_index.len()];
        let mut output = Vec::new();

        for lrec in &left_records {
            let lobj = lrec
                .as_object()
                .cloned()
                .ok_or_else(|| anyhow!(JoinError::NonObject))?;
            let lkey = extract_key(&lobj, &self.key)?;
            if let Some(rvec) = right_map.get(&lkey) {
                for robj in rvec.iter() {
                    // mark matched
                    // find global index
                    if let Some(pos) = right_index
                        .iter()
                        .position(|(k, obj)| k == &lkey && obj == robj)
                    {
                        matched_right[pos] = true;
                    }
                    let combined = combine_records(&lobj, robj);
                    output.push(Value::Object(combined));
                }
            } else if matches!(self.join_type, JoinType::Left | JoinType::Full) {
                let combined = combine_left_only(&lobj);
                output.push(Value::Object(combined));
            }
        }

        if matches!(self.join_type, JoinType::Right | JoinType::Full) {
            for (idx, (_k, robj)) in right_index.iter().enumerate() {
                if matched_right.get(idx).copied().unwrap_or(false) {
                    continue;
                }
                let combined = combine_right_only(robj);
                output.push(Value::Object(combined));
            }
        }

        let result = PartitionData::from_records(pid, limit, spill, output)?;
        Ok(vec![result])
    }
}

fn extract_key(obj: &Map<String, Value>, key: &str) -> Result<String, anyhow::Error> {
    let Some(val) = obj.get(key) else {
        return Err(anyhow!(JoinError::MissingKey(key.to_string())));
    };
    match val {
        Value::String(s) => Ok(s.clone()),
        Value::Number(n) => Ok(n.to_string()),
        _ => Err(anyhow!(JoinError::InvalidKey)),
    }
}

fn prefix_fields(obj: &Map<String, Value>, prefix: &str) -> Map<String, Value> {
    let mut out = Map::new();
    for (k, v) in obj {
        out.insert(format!("{}{}", prefix, k), v.clone());
    }
    out
}

fn combine_records(left: &Map<String, Value>, right: &Map<String, Value>) -> Map<String, Value> {
    let mut out = prefix_fields(left, "left_");
    for (k, v) in prefix_fields(right, "right_") {
        out.insert(k, v);
    }
    out
}

fn combine_left_only(left: &Map<String, Value>) -> Map<String, Value> {
    let mut out = prefix_fields(left, "left_");
    out.insert("right_".to_string() + "missing", Value::Null);
    out
}

fn combine_right_only(right: &Map<String, Value>) -> Map<String, Value> {
    let mut out = prefix_fields(right, "right_");
    out.insert("left_".to_string() + "missing", Value::Null);
    out
}
