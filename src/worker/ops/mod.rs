use std::convert::TryFrom;

use anyhow::{anyhow, Result};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::common::dag::OperatorType;

pub mod filter;
pub mod flat_map;
pub mod map;
pub mod read;
pub mod reduce;

pub type Record = Value;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PartitionData {
    pub records: Vec<Record>,
    pub partition_id: usize,
}

impl PartitionData {
    pub fn empty(partition_id: usize) -> Self {
        Self {
            records: Vec::new(),
            partition_id,
        }
    }
}

pub type OpResult = Result<PartitionData>;

pub trait ExecutableOp: Send + Sync {
    fn execute(&self, partition: PartitionData) -> OpResult;
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Operator {
    Read(read::ReadOp),
    Map(map::MapOp),
    Filter(filter::FilterOp),
    FlatMap(flat_map::FlatMapOp),
    Reduce(reduce::ReduceOp),
    Shuffle(ShuffleOp),
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ShuffleOp {
    pub strategy: String,
}

impl TryFrom<OperatorType> for Operator {
    type Error = anyhow::Error;

    fn try_from(value: OperatorType) -> Result<Self, Self::Error> {
        let op = match value {
            OperatorType::Read { uri, format } => Operator::Read(read::ReadOp {
                path: uri,
                format,
                partition_id: 0,
                total_partitions: 1,
            }),
            OperatorType::Map { script } => Operator::Map(map::MapOp { func: script }),
            OperatorType::Filter { predicate } => Operator::Filter(filter::FilterOp { predicate }),
            OperatorType::FlatMap {
                func,
                input_col,
                target_col,
            } => Operator::FlatMap(flat_map::FlatMapOp {
                func,
                input_col,
                target_col,
            }),
            OperatorType::Reduce {
                key,
                func,
                target_col,
            } => Operator::Reduce(reduce::ReduceOp {
                key,
                func,
                target_col,
            }),
            OperatorType::Identity => Operator::Map(map::MapOp {
                func: "identity".into(),
            }),
            OperatorType::Join { .. } => {
                return Err(anyhow!("Join operator not supported in this build"))
            }
            OperatorType::Aggregate { .. } => {
                return Err(anyhow!("Aggregate operator not implemented yet"))
            }
        };

        Ok(op)
    }
}

impl Operator {
    pub fn name(&self) -> &'static str {
        match self {
            Operator::Read(_) => "read",
            Operator::Map(_) => "map",
            Operator::Filter(_) => "filter",
            Operator::FlatMap(_) => "flat_map",
            Operator::Reduce(_) => "reduce",
            Operator::Shuffle(_) => "shuffle",
        }
    }

    pub fn execute(&self, input: PartitionData) -> OpResult {
        match self {
            Operator::Read(op) => op.execute(input),
            Operator::Map(op) => op.execute(input),
            Operator::Filter(op) => op.execute(input),
            Operator::FlatMap(op) => op.execute(input),
            Operator::Reduce(op) => op.execute(input),
            Operator::Shuffle(op) => op.execute(input),
        }
    }
}

impl ExecutableOp for ShuffleOp {
    fn execute(&self, _partition: PartitionData) -> OpResult {
        Err(anyhow!(
            "ShuffleOp not implemented yet (strategy={})",
            self.strategy
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn flatmap_operator_wires_config() {
        let op = Operator::try_from(OperatorType::FlatMap {
            func: "tokenize".into(),
            input_col: Some("text".into()),
            target_col: Some("word".into()),
        })
        .expect("flatmap conversion");

        let input = PartitionData {
            partition_id: 0,
            records: vec![serde_json::json!({"text": "hello world", "other": 1})],
        };

        let output = op.execute(input).expect("flatmap execute");
        assert_eq!(output.records.len(), 2);
        for rec in &output.records {
            let word = rec.get("word").and_then(|v| v.as_str()).unwrap();
            assert!(word == "hello" || word == "world");
            assert_eq!(rec.get("other").and_then(|v| v.as_i64()), Some(1));
        }
    }

    #[test]
    fn reduce_operator_wires_config() {
        let op = Operator::try_from(OperatorType::Reduce {
            key: Some("k".into()),
            func: "sum".into(),
            target_col: Some("v".into()),
        })
        .expect("reduce conversion");

        let input = PartitionData {
            partition_id: 0,
            records: vec![
                serde_json::json!({"k": "a", "v": 1}),
                serde_json::json!({"k": "a", "v": 2}),
            ],
        };

        let output = op.execute(input).expect("reduce execute");
        assert_eq!(output.records.len(), 1);
        let rec = output.records.first().unwrap();
        assert_eq!(rec.get("k").and_then(|v| v.as_str()), Some("a"));
        assert_eq!(rec.get("value").and_then(|v| v.as_f64()), Some(3.0));
    }
}
