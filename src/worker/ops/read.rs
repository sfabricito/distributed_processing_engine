use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::Path;

use anyhow::{anyhow, Context};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::{ExecutableOp, OpResult, PartitionData, Record};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReadOp {
    pub path: String,
    pub partition_id: usize,
    pub total_partitions: usize,
}

impl ReadOp {
    pub fn new(path: String, partition_id: usize, total_partitions: usize) -> Self {
        Self {
            path,
            partition_id,
            total_partitions: total_partitions.max(1),
        }
    }

    fn is_jsonl(path: &Path) -> bool {
        matches!(
            path.extension()
                .and_then(|e| e.to_str())
                .map(|e| e.to_ascii_lowercase())
                .as_deref(),
            Some("jsonl") | Some("ndjson") | Some("json")
        )
    }

    fn parse_csv_line(line: &str) -> Record {
        let fields: Vec<Value> = line
            .split(',')
            .map(|s| Value::String(s.trim().to_string()))
            .collect();
        Value::Array(fields)
    }
}

impl ExecutableOp for ReadOp {
    fn execute(&self, _partition: PartitionData) -> OpResult {
        let path = Path::new(&self.path);
        let file = File::open(path).with_context(|| format!("opening input {}", self.path))?;
        let reader = BufReader::new(file);

        let mut records = Vec::new();
        let json_mode = Self::is_jsonl(path);

        for (idx, line) in reader.lines().enumerate() {
            if idx % self.total_partitions != self.partition_id {
                continue;
            }
            let line = line?;
            let record = if json_mode {
                serde_json::from_str::<Record>(&line)
                    .map_err(|e| anyhow!("failed to parse json line {}: {}", idx, e))?
            } else {
                Self::parse_csv_line(&line)
            };
            records.push(record);
        }

        Ok(PartitionData {
            records,
            partition_id: self.partition_id,
        })
    }
}
