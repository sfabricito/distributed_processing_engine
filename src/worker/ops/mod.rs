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
        match value {
            OperatorType::Read(cfg) => Ok(Operator::Read(read::ReadOp {
                path: cfg.uri,
                format: cfg.format,
                partition_id: 0,
                total_partitions: 1,
            })),

            OperatorType::Map(cfg) => Ok(Operator::Map(map::MapOp {
                func: cfg.script,
            })),

            OperatorType::Filter(cfg) => Ok(Operator::Filter(filter::FilterOp {
                predicate: cfg.predicate,
            })),

            OperatorType::FlatMap(cfg) => Ok(Operator::FlatMap(flat_map::FlatMapOp {
                func: cfg.func,
                input_col: cfg.input_col,
                target_col: cfg.target_col,
            })),

            OperatorType::Reduce(cfg) => Ok(Operator::Reduce(reduce::ReduceOp {
                key: cfg.key,
                func: cfg.func,
                target_col: cfg.target_col,
            })),

            OperatorType::Join(_) => Err(anyhow!("Join operator not supported in this build")),

            OperatorType::Aggregate(_) => Err(anyhow!("Aggregate operator not implemented yet")),

            OperatorType::Identity => Ok(Operator::Map(map::MapOp {
                func: "identity".into(),
            })),
        }
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