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
        let (left_limit, left_spill, left_data) = left.into_parts()?;
        let (_right_limit, _right_spill, right_data) = right.into_parts()?;
        let mut right_index: HashMap<String, Vec<usize>> = HashMap::new();
        for (idx, record) in right_data.iter().enumerate() {
            if let Value::Object(map) = record {
                if let Some(key) = map.get(&self.right_on) {
                    right_index.entry(key.to_string()).or_default().push(idx);
                }
            }
        }

        let mut right_matched = vec![false; right_data.len()];
        let mut output = Vec::new();

        for record in &left_data {
            let Value::Object(map) = record else {
                continue;
            };

            if let Some(key) = map.get(&self.left_on) {
                if let Some(matches) = right_index.get(&key.to_string()) {
                    for idx in matches {
                        if let Some(r) = right_data.get(*idx) {
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
            for (idx, record) in right_data.iter().enumerate() {
                if !matches!(record, Value::Object(_)) {
                    continue;
                }
                if right_matched.get(idx).copied().unwrap_or(false) {
                    continue;
                }
                output.push(Self::compose_record(None, Some(record)));
            }
        }

        Ok(vec![PartitionData::from_records(
            left.partition_id.min(right.partition_id),
            left_limit,
            left_spill,
            output,
        )?])
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
        let spill = std::env::temp_dir().join("join-spill-1");
        let left = PartitionData::from_records(
            0,
            usize::MAX,
            spill.clone(),
            vec![json!({"id": 1, "left": "a"}), json!({"id": 2, "left": "b"})],
        )
        .unwrap();
        let right = PartitionData::from_records(
            0,
            usize::MAX,
            spill,
            vec![json!({"id": 1, "right": "c"})],
        )
        .unwrap();

        let result = op.execute(vec![left, right]).expect("join should succeed");
        let mut partition = result.into_iter().next().expect("join should produce a partition");
        let (_, _, records) = partition.into_parts().unwrap();
        assert_eq!(records.len(), 1);
        let row = records.first().unwrap();
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
        let spill = std::env::temp_dir().join("join-spill-2");
        let left = PartitionData::from_records(
            0,
            usize::MAX,
            spill.clone(),
            vec![json!({"id": 1, "left": "a"})],
        )
        .unwrap();
        let right = PartitionData::from_records(
            0,
            usize::MAX,
            spill,
            vec![json!({"id": 2, "right": "c"})],
        )
        .unwrap();

        let result = op.execute(vec![left, right]).expect("join should succeed");
        let mut partition = result.into_iter().next().expect("join should produce a partition");
        let (_, _, records) = partition.into_parts().unwrap();
        assert_eq!(records.len(), 2);
        assert!(records.iter().any(|row| row["right"].is_null()));
        assert!(records.iter().any(|row| row["left"].is_null()));
    }
}
