use anyhow::{Context, Result};
use std::{env, path::PathBuf, str::FromStr};

#[derive(Debug, Clone)]
pub struct Config {
    pub master_host: String,
    pub master_port: u16,
    pub worker_base_port: u16,
    pub num_workers: usize,
    pub worker_threads: usize,
    pub max_memory_mb: usize,
    pub spill_threshold_mb: usize,
    pub task_timeout_ms: u64,
    pub heartbeat_interval_ms: u64,
    pub data_dir: PathBuf,
    pub result_dir: PathBuf,
    pub logs_dir: PathBuf,
}

impl Config {
    pub fn from_env() -> Result<Self> {
        Ok(Self {
            master_host: env_or("MASTER_HOST", "127.0.0.1".to_string())?,
            master_port: env_or("MASTER_PORT", 8080u16)?,
            worker_base_port: env_or("WORKER_BASE_PORT", 9100u16)?,
            num_workers: env_or("NUM_WORKERS", 2usize)?,
            worker_threads: env_or("WORKER_THREADS", 4usize)?,
            max_memory_mb: env_or("MAX_MEMORY_MB", 512usize)?,
            spill_threshold_mb: env_or("SPILL_THRESHOLD_MB", 256usize)?,
            task_timeout_ms: env_or("TASK_TIMEOUT_MS", 60_000u64)?,
            heartbeat_interval_ms: env_or("HEARTBEAT_INTERVAL_MS", 2_000u64)?,
            data_dir: env_or::<String>("DATA_DIR", "./data".into()).map(PathBuf::from)?,
            result_dir: env_or::<String>("RESULT_DIR", "./results".into()).map(PathBuf::from)?,
            logs_dir: env_or::<String>("LOG_DIR", "./logs".into()).map(PathBuf::from)?,
        })
    }

    pub fn master_addr(&self) -> String {
        format!("{}:{}", self.master_host, self.master_port)
    }

    pub fn worker_listen_addr(&self, worker_index: usize) -> String {
        let port = self.worker_base_port + worker_index as u16;
        format!("{}:{}", self.master_host, port)
    }
}

fn env_or<T>(key: &str, default: T) -> Result<T>
where
    T: FromStr,
    <T as FromStr>::Err: std::error::Error + Send + Sync + 'static,
{
    match env::var(key) {
        Ok(val) => val
            .parse::<T>()
            .with_context(|| format!("failed to parse env var {key}")),
        Err(_) => Ok(default),
    }
}

#[cfg(test)]
mod tests {
    use super::Config;

    #[test]
    fn loads_defaults() {
        let cfg = Config::from_env().expect("config should load");
        assert_eq!(cfg.master_port, 8080);
        assert_eq!(cfg.worker_threads, 4);
        assert_eq!(cfg.heartbeat_interval_ms, 2_000);
    }
}
