use std::collections::HashMap;

use anyhow::anyhow;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use crate::common::dag::JoinType;

use super::{ExecutableOp, OpResult, PartitionData};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JoinOp {
    pub left_on: String,
    pub right_on: String,
    pub join_type: JoinType,
}

impl ExecutableOp for JoinOp {
    fn execute(&self, mut partitions: Vec<PartitionData>) -> OpResult {
        if partitions.len() != 2 {
            return Err(anyhow!(
                "JoinOp requires exactly two input partitions (received {})",
                partitions.len()
            ));
        }

        let right = partitions.pop().unwrap();
        let left = partitions.pop().unwrap();
        let mut right_index: HashMap<String, Vec<usize>> = HashMap::new();
        for (idx, record) in right.records.iter().enumerate() {
            if let Value::Object(map) = record {
                if let Some(key) = map.get(&self.right_on) {
                    right_index.entry(key.to_string()).or_default().push(idx);
                }
            }
        }

        let mut right_matched = vec![false; right.records.len()];
        let mut output = Vec::new();

        for record in &left.records {
            let Value::Object(map) = record else {
                continue;
            };

            if let Some(key) = map.get(&self.left_on) {
                if let Some(matches) = right_index.get(&key.to_string()) {
                    for idx in matches {
                        if let Some(r) = right.records.get(*idx) {
                            right_matched[*idx] = true;
                            output.push(Self::compose_record(Some(record), Some(r)));
                        }
                    }
                } else if matches!(self.join_type, JoinType::Left | JoinType::Full) {
                    output.push(Self::compose_record(Some(record), None));
                }
            } else if matches!(self.join_type, JoinType::Left | JoinType::Full) {
                output.push(Self::compose_record(Some(record), None));
            }
        }

        if matches!(self.join_type, JoinType::Right | JoinType::Full) {
            for (idx, record) in right.records.iter().enumerate() {
                if !matches!(record, Value::Object(_)) {
                    continue;
                }
                if right_matched.get(idx).copied().unwrap_or(false) {
                    continue;
                }
                output.push(Self::compose_record(None, Some(record)));
            }
        }

        Ok(vec![PartitionData {
            records: output,
            partition_id: left.partition_id.min(right.partition_id),
        }])
    }
}

impl JoinOp {
    fn compose_record(left: Option<&Value>, right: Option<&Value>) -> Value {
        json!({
            "left": left.cloned().unwrap_or(Value::Null),
            "right": right.cloned().unwrap_or(Value::Null),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn performs_inner_join() {
        let op = JoinOp {
            left_on: "id".into(),
            right_on: "id".into(),
            join_type: JoinType::Inner,
        };
        let left = PartitionData {
            partition_id: 0,
            records: vec![json!({"id": 1, "left": "a"}), json!({"id": 2, "left": "b"})],
        };
        let right = PartitionData {
            partition_id: 0,
            records: vec![json!({"id": 1, "right": "c"})],
        };

        let result = op.execute(vec![left, right]).expect("join should succeed");
        let partition = result.first().expect("join should produce a partition");
        assert_eq!(partition.records.len(), 1);
        let row = partition.records.first().unwrap();
        assert_eq!(
            row.get("left").and_then(|v| v.get("left")),
            Some(&json!("a"))
        );
        assert_eq!(
            row.get("right").and_then(|v| v.get("right")),
            Some(&json!("c"))
        );
    }

    #[test]
    fn includes_unmatched_on_full_join() {
        let op = JoinOp {
            left_on: "id".into(),
            right_on: "id".into(),
            join_type: JoinType::Full,
        };
        let left = PartitionData {
            partition_id: 0,
            records: vec![json!({"id": 1, "left": "a"})],
        };
        let right = PartitionData {
            partition_id: 0,
            records: vec![json!({"id": 2, "right": "c"})],
        };

        let result = op.execute(vec![left, right]).expect("join should succeed");
        let partition = result.first().expect("join should produce a partition");
        assert_eq!(partition.records.len(), 2);
        assert!(partition
            .records
            .iter()
            .any(|row| row["right"].is_null()));
        assert!(partition.records.iter().any(|row| row["left"].is_null()));
    }
}
