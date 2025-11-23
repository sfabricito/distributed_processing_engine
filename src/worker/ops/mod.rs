use std::convert::TryFrom;

use anyhow::{anyhow, Result};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::common::dag::OperatorType;

pub mod filter;
pub mod map;
pub mod read;

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
    // Placeholders for future operators
    FlatMap(FlatMapOp),
    Reduce(ReduceOp),
    ReduceByKey(ReduceByKeyOp),
    Join(JoinOp),
    Shuffle(ShuffleOp),
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct FlatMapOp {
    pub func: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ReduceOp {
    pub reducer: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ReduceByKeyOp {
    pub reducer: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct JoinOp {
    pub on: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ShuffleOp {
    pub strategy: String,
}

impl TryFrom<OperatorType> for Operator {
    type Error = anyhow::Error;

    fn try_from(value: OperatorType) -> Result<Self, Self::Error> {
        let op = match value {
            OperatorType::Map { script } => Operator::Map(map::MapOp { func: script }),
            OperatorType::Filter { predicate } => Operator::Filter(filter::FilterOp { predicate }),
            OperatorType::Join { on } => Operator::Join(JoinOp { on }),
            OperatorType::Reduce { reducer } => Operator::Reduce(ReduceOp { reducer }),
            OperatorType::Aggregate { aggregation } => Operator::Reduce(ReduceOp {
                reducer: aggregation,
            }),
            OperatorType::Identity => Operator::Map(map::MapOp {
                func: "identity".into(),
            }),
            // Reading is derived from task input URI; OperatorType does not encode it explicitly.
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
            Operator::ReduceByKey(_) => "reduce_by_key",
            Operator::Join(_) => "join",
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
            Operator::ReduceByKey(op) => op.execute(input),
            Operator::Join(op) => op.execute(input),
            Operator::Shuffle(op) => op.execute(input),
        }
    }
}

impl ExecutableOp for FlatMapOp {
    fn execute(&self, _partition: PartitionData) -> OpResult {
        Err(anyhow!(
            "FlatMapOp not implemented yet (func={})",
            self.func
        ))
    }
}

impl ExecutableOp for ReduceOp {
    fn execute(&self, _partition: PartitionData) -> OpResult {
        Err(anyhow!(
            "ReduceOp not implemented yet (reducer={})",
            self.reducer
        ))
    }
}

impl ExecutableOp for ReduceByKeyOp {
    fn execute(&self, _partition: PartitionData) -> OpResult {
        Err(anyhow!(
            "ReduceByKeyOp not implemented yet (reducer={})",
            self.reducer
        ))
    }
}

impl ExecutableOp for JoinOp {
    fn execute(&self, _partition: PartitionData) -> OpResult {
        Err(anyhow!("JoinOp not implemented yet (on={})", self.on))
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
