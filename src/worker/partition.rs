use std::{
    fs::{self, File, OpenOptions},
    io::{BufRead, BufReader, Read, Write},
    path::{Path, PathBuf},
};

use anyhow::Result;
use serde_json::Value;
use tracing::debug;
use uuid::Uuid;

use crate::common::config::Config;

#[derive(Debug, Clone)]
pub struct PartitionCache {
    in_memory: Vec<Value>,
    spilled_file: Option<PathBuf>,
    in_memory_bytes: usize,
    spilled_bytes: usize,
    limit_bytes: usize,
    spill_path: PathBuf,
    record_count: usize,
}

impl PartitionCache {
    pub fn new(limit_bytes: usize, spill_path: PathBuf) -> Self {
        Self {
            in_memory: Vec::new(),
            spilled_file: None,
            in_memory_bytes: 0,
            spilled_bytes: 0,
            limit_bytes,
            spill_path,
            record_count: 0,
        }
    }

    pub fn push(&mut self, value: Value) -> Result<()> {
        let size = Self::estimate_size(&value)?;
        self.in_memory_bytes += size;
        self.record_count += 1;
        let total_bytes = self.total_size_bytes();
        debug!(target: "partition", bytes = total_bytes, "partition memory grew");
        self.in_memory.push(value);
        self.maybe_spill()?;
        Ok(())
    }

    pub fn push_batch(&mut self, values: Vec<Value>) -> Result<()> {
        for value in values {
            self.push(value)?;
        }
        Ok(())
    }

    fn maybe_spill(&mut self) -> Result<()> {
        if self.total_size_bytes() > self.limit_bytes {
            self.spill_to_disk()?;
        }
        Ok(())
    }

    fn spill_to_disk(&mut self) -> Result<()> {
        if self.in_memory.is_empty() {
            return Ok(());
        }

        let path = self.spill_path.clone();
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }

        let mut file = OpenOptions::new().create(true).append(true).open(&path)?;

        let mut bytes_written = 0usize;
        for value in self.in_memory.drain(..) {
            let line = serde_json::to_vec(&value)?;
            bytes_written += line.len();
            file.write_all(&line)?;
            file.write_all(b"\n")?;
            bytes_written += 1;
        }

        self.spilled_file = Some(path.clone());
        self.spilled_bytes += bytes_written;
        self.in_memory_bytes = 0;
        debug!(
            target: "partition",
            path = %path.display(),
            bytes = bytes_written,
            total_bytes = self.total_size_bytes(),
            "spilled partition data to disk"
        );
        Ok(())
    }

    pub fn drain_all(&mut self) -> Result<Vec<Value>> {
        let mut data = Vec::new();
        if let Some(path) = self.spilled_file.as_ref() {
            let file = File::open(path)?;
            let reader = BufReader::new(file);
            for line in reader.lines() {
                let line = line?;
                if line.trim().is_empty() {
                    continue;
                }
                let value = serde_json::from_str::<Value>(&line)?;
                data.push(value);
            }
            fs::remove_file(path).ok();
            self.spilled_file = None;
            self.spilled_bytes = 0;
        }

        data.extend(self.in_memory.drain(..));
        self.in_memory_bytes = 0;
        self.record_count = data.len();
        Ok(data)
    }

    pub fn record_count(&self) -> usize {
        self.record_count
    }

    pub fn total_size_bytes(&self) -> usize {
        self.spilled_bytes + self.in_memory_bytes
    }

    pub fn limit_bytes(&self) -> usize {
        self.limit_bytes
    }

    pub fn spill_path(&self) -> PathBuf {
        self.spill_path.clone()
    }

    pub fn has_spill(&self) -> bool {
        self.spilled_file.is_some()
    }

    fn estimate_size(value: &Value) -> Result<usize> {
        Ok(serde_json::to_vec(value)?.len())
    }
}

pub struct PartitionStore {
    root: PathBuf,
    config: Config,
}

impl PartitionStore {
    pub fn new(config: Config) -> Self {
        let root = config.data_dir.clone();
        let _ = fs::create_dir_all(&root);
        Self { root, config }
    }

    pub fn partition_path(&self, job_id: Uuid, stage_id: u64, partition: u64) -> PathBuf {
        self.root
            .join(format!("{job_id}/{stage_id}/part-{partition}.bin"))
    }

    pub fn spill_path(&self, job_id: Uuid, stage_id: u64, partition: u64) -> PathBuf {
        self.root
            .join(format!("{job_id}/{stage_id}/spill-{partition}.bin"))
    }

    pub fn spill_to_disk(&self, path: &Path, data: &[u8]) -> Result<()> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        let mut file = File::create(path)?;
        file.write_all(data)?;
        Ok(())
    }

    pub fn read_from_disk(&self, path: &Path) -> Result<Vec<u8>> {
        let mut file = File::open(path)?;
        let mut buf = Vec::new();
        file.read_to_end(&mut buf)?;
        Ok(buf)
    }

    pub fn write_placeholder(&self, path: &Path) -> Result<()> {
        let bytes = b"partition placeholder";
        debug!(target: "partition", path = %path.display(), "writing placeholder partition");
        self.spill_to_disk(path, bytes)
    }

    pub fn write_records(&self, path: &Path, records: &[Value]) -> Result<u64> {
        let mut buf = Vec::new();
        for rec in records {
            let mut line = serde_json::to_vec(rec)?;
            buf.append(&mut line);
            buf.push(b'\n');
        }
        let bytes = buf.len() as u64;
        debug!(target: "partition", path = %path.display(), bytes = bytes, "writing records to partition");
        self.spill_to_disk(path, &buf)?;
        Ok(bytes)
    }

    pub fn should_spill(&self, current_size_mb: usize) -> bool {
        current_size_mb >= self.config.spill_threshold_mb
            || current_size_mb >= self.config.max_memory_mb
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn writes_and_reads_partition() {
        let mut cfg = Config::from_env().unwrap();
        cfg.data_dir = std::env::temp_dir().join(format!("dpe-test-{}", Uuid::new_v4()));
        let store = PartitionStore::new(cfg);
        let path = store.partition_path(Uuid::new_v4(), 1, 0);
        store.write_placeholder(&path).unwrap();
        let data = store.read_from_disk(&path).unwrap();
        assert!(!data.is_empty());
    }

    #[test]
    fn spills_when_limit_exceeded() {
        let spill_path = std::env::temp_dir().join(format!("dpe-spill-{}", Uuid::new_v4()));
        let mut cache = PartitionCache::new(16, spill_path.clone());
        cache.push(Value::String("small".into())).unwrap();
        assert!(!cache.has_spill());
        cache
            .push(Value::String("this string should spill".into()))
            .unwrap();
        assert!(cache.has_spill());
        let data = cache.drain_all().unwrap();
        assert_eq!(data.len(), 2);
        assert!(!spill_path.exists());
    }
}
