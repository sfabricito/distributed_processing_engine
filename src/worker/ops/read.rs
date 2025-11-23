use std::fs::File;
use std::io::{BufRead, BufReader, Read};
use std::path::Path;

use anyhow::{anyhow, Context};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Number, Value};

use super::{ExecutableOp, OpResult, PartitionData, Record};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReadOp {
    pub path: String,
    pub format: String,
    pub partition_id: usize,
    pub total_partitions: usize,
}

impl ReadOp {
    pub fn new(path: String, format: String, partition_id: usize, total_partitions: usize) -> Self {
        Self {
            path,
            format,
            partition_id,
            total_partitions: total_partitions.max(1),
        }
    }
}

impl ExecutableOp for ReadOp {
    fn execute(&self, _partition: PartitionData) -> OpResult {
        let path = Path::new(&self.path);
        let file = File::open(path).with_context(|| format!("opening input {}", self.path))?;

        let mut records = Vec::new();
        match self.format.as_str() {
            "csv" => {
                let mut reader = BufReader::new(file);
                let mut header_line = String::new();
                reader
                    .read_line(&mut header_line)
                    .context("reading CSV header")?;
                if header_line.trim().is_empty() {
                    return Err(anyhow!("CSV header is empty"));
                }
                let headers: Vec<String> = header_line
                    .trim_end_matches(&['\r', '\n'][..])
                    .split(',')
                    .map(|h| h.trim().to_string())
                    .collect();

                let mut data_idx = 0usize;
                for line in reader.lines() {
                    let line = line?;
                    if line.trim().is_empty() {
                        continue;
                    }
                    if data_idx % self.total_partitions == self.partition_id {
                        let record = parse_csv_line_to_object(&headers, &line);
                        records.push(record);
                    }
                    data_idx += 1;
                }
            }
            "jsonl" | "ndjson" => {
                let reader = BufReader::new(file);
                for (idx, line) in reader.lines().enumerate() {
                    if idx % self.total_partitions != self.partition_id {
                        continue;
                    }
                    let line = line?;
                    if line.trim().is_empty() {
                        continue;
                    }
                    let value = serde_json::from_str::<Record>(&line)
                        .map_err(|e| anyhow!("failed to parse json line {}: {}", idx, e))?;
                    records.push(normalize_json_value(value));
                }
            }
            "json" => {
                let mut buf = String::new();
                BufReader::new(file).read_to_string(&mut buf)?;
                let first_non_ws = buf.chars().find(|c| !c.is_whitespace());
                if first_non_ws == Some('[') {
                    let arr: Value =
                        serde_json::from_str(&buf).context("parsing json array input")?;
                    let elements = arr
                        .as_array()
                        .cloned()
                        .ok_or_else(|| anyhow!("json input is not an array"))?;
                    for (idx, elem) in elements.into_iter().enumerate() {
                        if idx % self.total_partitions != self.partition_id {
                            continue;
                        }
                        records.push(normalize_json_value(elem));
                    }
                } else {
                    // Fallback to JSONL behavior.
                    for (idx, line) in buf.lines().enumerate() {
                        if idx % self.total_partitions != self.partition_id {
                            continue;
                        }
                        if line.trim().is_empty() {
                            continue;
                        }
                        let value = serde_json::from_str::<Record>(line)
                            .map_err(|e| anyhow!("failed to parse json line {}: {}", idx, e))?;
                        records.push(normalize_json_value(value));
                    }
                }
            }
            other => {
                return Err(anyhow!("unsupported format '{}'", other));
            }
        }

        Ok(PartitionData {
            records,
            partition_id: self.partition_id,
        })
    }
}

fn parse_csv_line_to_object(headers: &[String], row: &str) -> Value {
    let mut obj = Map::with_capacity(headers.len());
    for (idx, col) in row.split(',').enumerate() {
        let key = headers
            .get(idx)
            .cloned()
            .unwrap_or_else(|| format!("col{idx}"));
        let val = infer_number(col.trim());
        obj.insert(key, val);
    }
    Value::Object(obj)
}

fn infer_number(raw: &str) -> Value {
    if let Ok(i) = raw.parse::<i64>() {
        return Value::Number(Number::from(i));
    }
    if let Ok(f) = raw.parse::<f64>() {
        if let Some(n) = Number::from_f64(f) {
            return Value::Number(n);
        }
    }
    Value::String(raw.to_string())
}

fn normalize_json_value(value: Value) -> Value {
    match value {
        Value::Object(_) => value,
        Value::Array(_) | Value::String(_) | Value::Number(_) | Value::Bool(_) | Value::Null => {
            let mut obj = Map::new();
            obj.insert("value".into(), value);
            Value::Object(obj)
        }
    }
}
