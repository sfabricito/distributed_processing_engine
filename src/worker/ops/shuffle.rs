use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

use anyhow::anyhow;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::{ExecutableOp, OpResult, PartitionData};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShuffleByKeyOp {
    pub key: String,
    pub total_partitions: usize,
}

#[derive(thiserror::Error, Debug)]
pub enum ShuffleError {
    #[error("shuffle: total_partitions must be > 0")]
    InvalidPartitions,
}

impl ExecutableOp for ShuffleByKeyOp {
    fn execute(&self, partitions: Vec<PartitionData>) -> OpResult {
        if self.total_partitions == 0 {
            return Err(anyhow!(ShuffleError::InvalidPartitions));
        }
        let mut buckets: Vec<Vec<Value>> = vec![Vec::new(); self.total_partitions];
        let limit = partitions
            .first()
            .map(|p| p.limit_bytes())
            .unwrap_or(usize::MAX);
        let spill_base = partitions
            .first()
            .map(|p| p.spill_path().parent().map(|p| p.to_path_buf()))
            .flatten()
            .unwrap_or(std::env::temp_dir());

        for p in partitions {
            let (_lim, _spill, records) = p.into_parts()?;
            for rec in records {
                let target = compute_bucket(&rec, &self.key, self.total_partitions);
                buckets[target].push(rec);
            }
        }

        let mut out = Vec::with_capacity(self.total_partitions);
        for (idx, records) in buckets.into_iter().enumerate() {
            let spill_path = spill_base.join(format!("bucket-{idx}.bin"));
            out.push(PartitionData::from_records(
                idx, limit, spill_path, records,
            )?);
        }

        Ok(out)
    }
}

fn compute_bucket(value: &Value, key: &str, total: usize) -> usize {
    let mut hasher = DefaultHasher::new();
    match value {
        Value::Object(map) => {
            if let Some(v) = map.get(key) {
                match v {
                    Value::String(s) => s.hash(&mut hasher),
                    Value::Number(n) => n.to_string().hash(&mut hasher),
                    Value::Bool(b) => b.hash(&mut hasher),
                    _ => 0.hash(&mut hasher),
                }
            } else {
                0.hash(&mut hasher);
            }
        }
        _ => 0.hash(&mut hasher),
    }
    (hasher.finish() as usize) % total
}

#[cfg(test)]
mod tests_shuffle {
    use super::super::shuffle::ShuffleByKeyOp;
    use super::super::{ExecutableOp, PartitionData};
    use serde_json::json;
    use uuid::Uuid;

    fn partition(records: Vec<serde_json::Value>) -> PartitionData {
        let spill = std::env::temp_dir().join(format!("dpe-shuffle-{}", Uuid::new_v4()));
        PartitionData::from_records(0, usize::MAX, spill, records).expect("partition")
    }

    #[test]
    fn shuffle_single_partition_groups_keys() {
        let op = ShuffleByKeyOp {
            key: "k".into(),
            total_partitions: 2,
        };
        let input = partition(vec![
            json!({"k": "a", "v": 1}),
            json!({"k": "b", "v": 2}),
            json!({"k": "a", "v": 3}),
        ]);
        let out = op.execute(vec![input]).expect("shuffle");
        assert_eq!(out.len(), 2);
        let mut counts = vec![0usize; 2];
        for p in out {
            let pid = p.partition_id;
            let (_, _, recs) = p.into_parts().unwrap();
            counts[pid] = recs.len();
        }
        assert_eq!(counts.iter().sum::<usize>(), 3);
    }

    #[test]
    fn shuffle_multiple_partitions_merges_keys() {
        let op = ShuffleByKeyOp {
            key: "k".into(),
            total_partitions: 3,
        };
        let p1 = partition(vec![json!({"k": "x", "v": 1})]);
        let p2 = partition(vec![json!({"k": "x", "v": 2})]);
        let out = op.execute(vec![p1, p2]).expect("shuffle");
        let mut total = 0;
        for p in out {
            let (_, _, recs) = p.into_parts().unwrap();
            total += recs.len();
        }
        assert_eq!(total, 2);
    }
}
