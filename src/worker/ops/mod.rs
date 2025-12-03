use std::path::PathBuf;

use anyhow::{anyhow, Result};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::common::dag::OperatorType;
use crate::worker::partition::PartitionCache;

pub mod filter;
pub mod flat_map;
pub mod map;
pub mod read;
pub mod reduce;
#[cfg(test)]
mod tests;

pub type Record = Value;

#[derive(Debug, Clone)]
pub struct PartitionData {
    pub partition_id: usize,
    pub cache: PartitionCache,
}

impl PartitionData {
    pub fn empty(partition_id: usize, limit_bytes: usize, spill_path: PathBuf) -> Self {
        Self {
            partition_id,
            cache: PartitionCache::new(limit_bytes, spill_path),
        }
    }

    pub fn from_records(
        partition_id: usize,
        limit_bytes: usize,
        spill_path: PathBuf,
        records: Vec<Record>,
    ) -> Result<Self> {
        let mut cache = PartitionCache::new(limit_bytes, spill_path);
        cache.push_batch(records)?;
        Ok(Self {
            partition_id,
            cache,
        })
    }

    pub fn record_count(&self) -> usize {
        self.cache.record_count()
    }

    pub fn limit_bytes(&self) -> usize {
        self.cache.limit_bytes()
    }

    pub fn spill_path(&self) -> PathBuf {
        self.cache.spill_path()
    }

    pub fn has_spill(&self) -> bool {
        self.cache.has_spill()
    }

    pub fn into_parts(mut self) -> Result<(usize, PathBuf, Vec<Record>)> {
        let limit = self.cache.limit_bytes();
        let spill_path = self.cache.spill_path();
        let records = self.cache.drain_all()?;
        Ok((limit, spill_path, records))
    }
}

pub type OpResult<T = Vec<PartitionData>> = Result<T>;

pub trait ExecutableOp: Send + Sync {
    fn execute(&self, partitions: Vec<PartitionData>) -> OpResult;
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Operator {
    Read(read::ReadOp),
    Map(map::MapOp),
    Filter(filter::FilterOp),
    FlatMap(flat_map::FlatMapOp),
    Reduce(reduce::ReduceOp),
    ReduceByKey(reduce::ReduceByKeyOp),
    Join(JoinOp),
    Shuffle(ShuffleOp),
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct JoinOp {
    pub on: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ShuffleOp {
    pub strategy: String,
}

impl Operator {
    pub fn from_type(
        value: OperatorType,
        partition_id: usize,
        total_partitions: usize,
        cache_limit_bytes: usize,
        spill_path: PathBuf,
    ) -> Result<Self, anyhow::Error> {
        let op = match value {
            OperatorType::Read { uri, format } => Operator::Read(read::ReadOp {
                path: uri,
                format,
                partition_id,
                total_partitions: total_partitions.max(1),
                cache_limit_bytes,
                spill_path,
            }),
            OperatorType::Map { script } => Operator::Map(map::MapOp { func: script }),
            OperatorType::Filter { predicate } => Operator::Filter(filter::FilterOp { predicate }),
            OperatorType::FlatMap { func } => Operator::FlatMap(flat_map::FlatMapOp { func }),
            OperatorType::Join { on } => Operator::Join(JoinOp { on }),
            OperatorType::Reduce { reducer } => Operator::Reduce(reduce::ReduceOp { reducer }),
            OperatorType::ReduceByKey { key, op } => {
                Operator::ReduceByKey(reduce::ReduceByKeyOp { key, reducer: op })
            }
            OperatorType::Aggregate { aggregation } => Operator::Reduce(reduce::ReduceOp {
                reducer: aggregation,
            }),
            OperatorType::Identity => Operator::Map(map::MapOp {
                func: "identity".into(),
            }),
        };

        Ok(op)
    }

    pub fn try_from(value: OperatorType) -> Result<Self, anyhow::Error> {
        let temp_path = std::env::temp_dir().join("dpe-spill-default.bin");
        Self::from_type(value, 0, 1, usize::MAX, temp_path)
    }

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

    pub fn execute(&self, input: Vec<PartitionData>) -> OpResult {
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

impl ExecutableOp for JoinOp {
    fn execute(&self, _partitions: Vec<PartitionData>) -> OpResult {
        Err(anyhow!("JoinOp not implemented yet (on={})", self.on))
    }
}

impl ExecutableOp for ShuffleOp {
    fn execute(&self, _partitions: Vec<PartitionData>) -> OpResult {
        Err(anyhow!(
            "ShuffleOp not implemented yet (strategy={})",
            self.strategy
        ))
    }
}
