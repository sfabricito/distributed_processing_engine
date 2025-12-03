use anyhow::Result;
use rusqlite::{params, Connection};
use serde_json;
use std::path::PathBuf;
use std::sync::Mutex;

use crate::common::dag::DagSpecification;
use crate::common::types::{JobId, JobStatus, TaskId, TaskResult, TaskStatus};

pub struct StateStore {
    conn: Mutex<Connection>,
}

impl StateStore {
    /// Open (or create) the SQLite file `state.db` inside `base_path` and ensure schema exists.
    pub fn new(base_path: PathBuf) -> Self {
        let _ = std::fs::create_dir_all(&base_path);
        let db_path = base_path.join("state.db");

        // Open connection; use busy timeout to reduce contention.
        let conn = Connection::open(db_path).expect("failed to open state.db");
        conn.pragma_update(None, "journal_mode", &"WAL").ok();
        conn.pragma_update(None, "synchronous", &"NORMAL").ok();
        conn.pragma_update(None, "busy_timeout", &3000).ok();

        // Create schema if not exists
        conn.execute_batch(
            "BEGIN;
            CREATE TABLE IF NOT EXISTS jobs (
                job_id TEXT PRIMARY KEY,
                status TEXT NOT NULL,
                dag_json TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS tasks (
                task_id TEXT PRIMARY KEY,
                job_id TEXT NOT NULL,
                status TEXT NOT NULL,
                result_json TEXT,
                FOREIGN KEY (job_id) REFERENCES jobs(job_id)
            );
            CREATE TABLE IF NOT EXISTS job_status (
                job_id TEXT PRIMARY KEY,
                state TEXT NOT NULL,
                progress REAL NOT NULL,
                metrics_json TEXT,
                error TEXT,
                outputs_json TEXT
            );
            CREATE TABLE IF NOT EXISTS stage_status (
                job_id TEXT NOT NULL,
                stage_id INTEGER NOT NULL,
                status TEXT NOT NULL,
                metrics_json TEXT,
                PRIMARY KEY (job_id, stage_id)
            );
            CREATE TABLE IF NOT EXISTS worker_metrics (
                worker_id TEXT PRIMARY KEY,
                last_heartbeat INTEGER NOT NULL,
                status TEXT NOT NULL,
                metrics_json TEXT
            );
            COMMIT;",
        )
        .expect("failed to create state tables");

        Self {
            conn: Mutex::new(conn),
        }
    }

    /// Insert or update a job row.
    pub fn persist_job(
        &self,
        job_id: JobId,
        dag: &DagSpecification,
        status: JobStatus,
    ) -> Result<()> {
        let dag_json = serde_json::to_string_pretty(dag)?;
        let status_json = serde_json::to_string(&status)?;

        let mut conn = self.conn.lock().unwrap();
        let tx = conn.transaction()?;
        tx.execute(
            "INSERT INTO jobs (job_id, status, dag_json) VALUES (?1, ?2, ?3)
             ON CONFLICT(job_id) DO UPDATE SET status=excluded.status, dag_json=excluded.dag_json;",
            params![job_id.to_string(), status_json, dag_json],
        )?;
        tx.commit()?;
        Ok(())
    }

    pub fn persist_job_status(
        &self,
        job_id: JobId,
        state: JobStatus,
        progress: f32,
        metrics: &crate::common::types::JobMetrics,
        error: Option<String>,
        outputs: &[String],
    ) -> Result<()> {
        let metrics_json = serde_json::to_string(metrics)?;
        let outputs_json = serde_json::to_string(outputs)?;
        let mut conn = self.conn.lock().unwrap();
        let tx = conn.transaction()?;
        tx.execute(
            "INSERT INTO job_status (job_id, state, progress, metrics_json, error, outputs_json)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)
             ON CONFLICT(job_id) DO UPDATE SET state=excluded.state, progress=excluded.progress, metrics_json=excluded.metrics_json, error=excluded.error, outputs_json=excluded.outputs_json;",
            params![
                job_id.to_string(),
                serde_json::to_string(&state)?,
                progress as f64,
                metrics_json,
                error.unwrap_or_default(),
                outputs_json
            ],
        )?;
        tx.commit()?;
        Ok(())
    }

    pub fn persist_stage_status(
        &self,
        job_id: JobId,
        stage_id: u64,
        status: JobStatus,
        metrics: &crate::common::types::StageMetrics,
    ) -> Result<()> {
        let metrics_json = serde_json::to_string(metrics)?;
        let mut conn = self.conn.lock().unwrap();
        let tx = conn.transaction()?;
        tx.execute(
            "INSERT INTO stage_status (job_id, stage_id, status, metrics_json)
             VALUES (?1, ?2, ?3, ?4)
             ON CONFLICT(job_id, stage_id) DO UPDATE SET status=excluded.status, metrics_json=excluded.metrics_json;",
            params![
                job_id.to_string(),
                stage_id as i64,
                serde_json::to_string(&status)?,
                metrics_json
            ],
        )?;
        tx.commit()?;
        Ok(())
    }

    pub fn persist_worker_metrics(
        &self,
        worker_id: uuid::Uuid,
        status: crate::common::types::WorkerStatus,
        metrics: &crate::common::types::WorkerMetrics,
    ) -> Result<()> {
        let metrics_json = serde_json::to_string(metrics)?;
        let mut conn = self.conn.lock().unwrap();
        let tx = conn.transaction()?;
        tx.execute(
            "INSERT INTO worker_metrics (worker_id, last_heartbeat, status, metrics_json)
             VALUES (?1, strftime('%s','now'), ?2, ?3)
             ON CONFLICT(worker_id) DO UPDATE SET last_heartbeat=strftime('%s','now'), status=excluded.status, metrics_json=excluded.metrics_json;",
            params![
                worker_id.to_string(),
                serde_json::to_string(&status)?,
                metrics_json
            ],
        )?;
        tx.commit()?;
        Ok(())
    }

    /// Insert or update a task status (without changing result_json).
    pub fn persist_task_status(
        &self,
        task_id: TaskId,
        job_id: JobId,
        status: TaskStatus,
    ) -> Result<()> {
        let status_json = serde_json::to_string(&status)?;
        let mut conn = self.conn.lock().unwrap();
        let tx = conn.transaction()?;
        tx.execute(
            "INSERT INTO tasks (task_id, job_id, status) VALUES (?1, ?2, ?3)
             ON CONFLICT(task_id) DO UPDATE SET status=excluded.status;",
            params![task_id.to_string(), job_id.to_string(), status_json],
        )?;
        tx.commit()?;
        Ok(())
    }

    /// Insert or update a task result and set status to Completed.
    pub fn persist_task_result(&self, result: &TaskResult) -> Result<()> {
        let result_json = serde_json::to_string_pretty(result)?;
        let status_json = serde_json::to_string(&result.status)?;
        let mut conn = self.conn.lock().unwrap();
        let tx = conn.transaction()?;
        tx.execute(
            "INSERT INTO tasks (task_id, job_id, status, result_json) VALUES (?1, ?2, ?3, ?4)
             ON CONFLICT(task_id) DO UPDATE SET status=excluded.status, result_json=excluded.result_json;",
            params![result.task_id.to_string(), result.job_id.to_string(), status_json, result_json],
        )?;
        tx.commit()?;
        Ok(())
    }

    /// Load all jobs from DB. Returns tuples (JobId, JobStatus, DagSpecification)
    pub fn load_all_jobs(&self) -> Result<Vec<(JobId, JobStatus, DagSpecification)>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare("SELECT job_id, status, dag_json FROM jobs")?;
        let rows = stmt
            .query_map([], |row| {
                let job_id_str: String = row.get(0)?;
                let status_str: String = row.get(1)?;
                let dag_json: String = row.get(2)?;
                Ok((job_id_str, status_str, dag_json))
            })?
            .collect::<Result<Vec<_>, rusqlite::Error>>()?;

        let mut out = Vec::new();
        for (job_id_str, status_str, dag_json) in rows {
            let job_id = job_id_str.parse::<uuid::Uuid>()?;
            let status: JobStatus = serde_json::from_str(&status_str)?;
            let dag: DagSpecification = serde_json::from_str(&dag_json)?;
            out.push((job_id, status, dag));
        }
        Ok(out)
    }

    /// Load all tasks. Returns tuples (TaskId, JobId, TaskStatus, Option<TaskResult>)
    pub fn load_all_tasks(&self) -> Result<Vec<(TaskId, JobId, TaskStatus, Option<TaskResult>)>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare("SELECT task_id, job_id, status, result_json FROM tasks")?;
        let rows = stmt
            .query_map([], |row| {
                let task_id_str: String = row.get(0)?;
                let job_id_str: String = row.get(1)?;
                let status_str: String = row.get(2)?;
                let result_json: Option<String> = row.get(3)?;
                Ok((task_id_str, job_id_str, status_str, result_json))
            })?
            .collect::<Result<Vec<_>, rusqlite::Error>>()?;

        let mut out = Vec::new();
        for (task_id_str, job_id_str, status_str, result_json) in rows {
            let task_id = task_id_str.parse::<uuid::Uuid>()?;
            let job_id = job_id_str.parse::<uuid::Uuid>()?;
            let status: TaskStatus = serde_json::from_str(&status_str)?;
            let result = match result_json {
                Some(js) => Some(serde_json::from_str(&js)?),
                None => None,
            };
            out.push((task_id, job_id, status, result));
        }

        Ok(out)
    }
}
