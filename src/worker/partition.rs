use std::{
    fs::{self, File},
    io::{Read, Write},
    path::{Path, PathBuf},
};

use anyhow::Result;
use tracing::debug;
use uuid::Uuid;

use crate::common::config::Config;

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
}
