use anyhow::anyhow;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

use super::{ExecutableOp, OpResult, PartitionData, Record};

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ReduceOp {
    pub reducer: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ReduceByKeyOp {
    pub key: String,
    pub reducer: String,
}

#[derive(thiserror::Error, Debug)]
pub enum ReduceError {
    #[error("reduce: unknown reducer '{0}'")]
    UnknownReducer(String),
    #[error("reduce: expected numeric field '{0}'")]
    ExpectedNumeric(String),
    #[error("reduce_by_key: missing key field '{0}'")]
    MissingKey(String),
    #[error("reduce_by_key: unsupported reducer '{0}'")]
    UnsupportedReducer(String),
}

#[derive(Debug, Clone)]
enum ReducerKind {
    Sum(Option<String>),
    Min(Option<String>),
    Max(Option<String>),
    Count,
    Collect,
}

impl ReducerKind {
    fn parse(raw: &str) -> Result<Self, anyhow::Error> {
        if raw == "count" {
            return Ok(Self::Count);
        }
        if raw == "collect" {
            return Ok(Self::Collect);
        }
        let mut parts = raw.splitn(2, ':');
        let op = parts.next().unwrap_or("");
        let field = parts.next().map(|s| s.to_string());
        match op {
            "sum" => Ok(Self::Sum(field)),
            "min" => Ok(Self::Min(field)),
            "max" => Ok(Self::Max(field)),
            _ => Err(anyhow!(ReduceError::UnknownReducer(raw.to_string()))),
        }
    }
}

impl ExecutableOp for ReduceOp {
    fn execute(&self, partitions: Vec<PartitionData>) -> OpResult {
        let reducer = ReducerKind::parse(&self.reducer)?;
        if partitions.is_empty() {
            return Ok(Vec::new());
        }
        let pid = partitions
            .first()
            .map(|p| p.partition_id)
            .unwrap_or_default();
        let (limit, spill) = {
            let first = partitions.first().unwrap();
            (first.limit_bytes(), first.spill_path())
        };
        let mut all_records = Vec::new();
        for p in partitions {
            let (_lim, _spill, recs) = p.into_parts()?;
            all_records.extend(recs);
        }

        let output_record = apply_reducer(&reducer, &all_records)?;
        let result = PartitionData::from_records(
            pid,
            limit,
            spill,
            vec![output_record.unwrap_or_default()],
        )?;
        Ok(vec![result])
    }
}

impl ExecutableOp for ReduceByKeyOp {
    fn execute(&self, partitions: Vec<PartitionData>) -> OpResult {
        if partitions.is_empty() {
            return Ok(Vec::new());
        }
        let reducer = ReducerKind::parse(&self.reducer)
            .map_err(|_| anyhow!(ReduceError::UnsupportedReducer(self.reducer.clone())))?;

        let pid = partitions
            .first()
            .map(|p| p.partition_id)
            .unwrap_or_default();
        let (limit, spill) = {
            let first = partitions.first().unwrap();
            (first.limit_bytes(), first.spill_path())
        };

        let mut grouped: std::collections::HashMap<String, Vec<Record>> =
            std::collections::HashMap::new();
        for p in partitions {
            let (_lim, _spill, recs) = p.into_parts()?;
            for r in recs {
                let key_val = r
                    .get(&self.key)
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string())
                    .ok_or_else(|| anyhow!(ReduceError::MissingKey(self.key.clone())))?;
                grouped.entry(key_val).or_default().push(r);
            }
        }

        let mut out_records = Vec::with_capacity(grouped.len());
        for (key, recs) in grouped.into_iter() {
            let agg = apply_reducer(&reducer, &recs)?;
            let mut obj = Map::new();
            obj.insert(self.key.clone(), Value::String(key));
            if let Some(Value::Object(inner)) = agg.clone() {
                for (k, v) in inner.into_iter() {
                    obj.insert(k, v);
                }
            } else if let Some(v) = agg {
                obj.insert(format!("{}", self.reducer), v);
            }
            out_records.push(Value::Object(obj));
        }

        let result = PartitionData::from_records(pid, limit, spill, out_records)?;
        Ok(vec![result])
    }
}

fn apply_reducer(
    reducer: &ReducerKind,
    records: &[Record],
) -> Result<Option<Value>, anyhow::Error> {
    match reducer {
        ReducerKind::Count => Ok(Some(Value::Object(single_kv(
            "count",
            Value::Number((records.len() as u64).into()),
        )))),
        ReducerKind::Collect => {
            let arr = Value::Array(records.to_vec());
            Ok(Some(Value::Object(single_kv("collect", arr))))
        }
        ReducerKind::Sum(field) => {
            let (sum, count) = fold_numeric(records, field.as_deref(), |acc, n| acc + n);
            let sum_val = if count == 0 { 0.0 } else { sum };
            let key = field
                .as_ref()
                .map(|f| format!("sum_{}", f))
                .unwrap_or_else(|| "sum".into());
            Ok(Some(Value::Object(single_kv(
                key,
                Value::Number(serde_json::Number::from_f64(sum_val).unwrap_or_else(|| 0.into())),
            ))))
        }
        ReducerKind::Min(field) => {
            let (min, count) = fold_numeric_opt(records, field.as_deref(), |a, b| a.min(b));
            let min_val = if count == 0 { 0.0 } else { min };
            let key = field
                .as_ref()
                .map(|f| format!("min_{}", f))
                .unwrap_or_else(|| "min".into());
            Ok(Some(Value::Object(single_kv(
                key,
                Value::Number(serde_json::Number::from_f64(min_val).unwrap_or_else(|| 0.into())),
            ))))
        }
        ReducerKind::Max(field) => {
            let (max, count) = fold_numeric_opt(records, field.as_deref(), |a, b| a.max(b));
            let max_val = if count == 0 { 0.0 } else { max };
            let key = field
                .as_ref()
                .map(|f| format!("max_{}", f))
                .unwrap_or_else(|| "max".into());
            Ok(Some(Value::Object(single_kv(
                key,
                Value::Number(serde_json::Number::from_f64(max_val).unwrap_or_else(|| 0.into())),
            ))))
        }
    }
}

fn fold_numeric<F>(records: &[Record], field: Option<&str>, mut f: F) -> (f64, usize)
where
    F: FnMut(f64, f64) -> f64,
{
    let mut acc = 0.0;
    let mut count = 0usize;
    for r in records {
        if let Some(n) = extract_number(r, field) {
            acc = if count == 0 { n } else { f(acc, n) };
            count += 1;
        }
    }
    (acc, count)
}

fn fold_numeric_opt<F>(records: &[Record], field: Option<&str>, mut f: F) -> (f64, usize)
where
    F: FnMut(f64, f64) -> f64,
{
    fold_numeric(records, field, |a, b| f(a, b))
}

fn extract_number(record: &Record, field: Option<&str>) -> Option<f64> {
    match (record, field) {
        (Value::Number(n), None) => n.as_f64(),
        (Value::Object(map), Some(f)) => map.get(f).and_then(|v| v.as_f64()),
        _ => None,
    }
}

fn single_kv(key: impl Into<String>, value: Value) -> Map<String, Value> {
    let mut map = Map::new();
    map.insert(key.into(), value);
    map
}
