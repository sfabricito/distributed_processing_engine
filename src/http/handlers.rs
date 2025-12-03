use std::sync::Arc;

use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::IntoResponse,
    Json,
};
use tracing::warn;

use crate::common::{
    dag::DagSpecification,
    types::{EngineError, HeartbeatMessage, JobId, JobStatus, TaskResult, WorkerId, WorkerInfo},
};
use crate::master::Master;

#[derive(Clone)]
pub struct ApiState {
    pub master: Arc<Master>,
}

pub async fn submit_job(
    State(state): State<ApiState>,
    Json(dag): Json<DagSpecification>,
) -> Result<Json<JobSubmitResponse>, ApiError> {
    let job_id = state.master.submit_job(dag).await.map_err(ApiError::from)?;
    Ok(Json(JobSubmitResponse { job_id }))
}

pub async fn job_status(
    State(state): State<ApiState>,
    Path(job_id): Path<String>,
) -> Result<Json<JobStatusResponse>, ApiError> {
    let parsed = parse_job_id(&job_id)?;
    let status = state.master.get_job_status(parsed)?;
    Ok(Json(status))
}

pub async fn job_results(
    State(state): State<ApiState>,
    Path(job_id): Path<String>,
) -> Result<Json<Vec<TaskResult>>, ApiError> {
    let parsed = parse_job_id(&job_id)?;
    let results = state.master.get_job_results(parsed)?;
    Ok(Json(results))
}

pub async fn heartbeat(
    State(state): State<ApiState>,
    Json(payload): Json<HeartbeatMessage>,
) -> Result<StatusCode, ApiError> {
    state.master.handle_heartbeat(payload);
    Ok(StatusCode::OK)
}

pub async fn register_worker(
    State(state): State<ApiState>,
    Json(worker): Json<WorkerInfo>,
) -> Result<Json<RegisterResponse>, ApiError> {
    let worker_id = state.master.clone().register_worker(worker)?;
    Ok(Json(RegisterResponse { worker_id }))
}

pub async fn complete_task(
    State(state): State<ApiState>,
    Json(result): Json<TaskResult>,
) -> Result<StatusCode, ApiError> {
    state.master.complete_task(result)?;
    Ok(StatusCode::OK)
}

fn parse_job_id(raw: &str) -> Result<JobId, ApiError> {
    JobId::parse_str(raw).map_err(|_| ApiError::BadRequest("invalid job id".into()))
}

#[derive(Debug, serde::Serialize)]
pub struct JobSubmitResponse {
    pub job_id: JobId,
}

#[derive(Debug, serde::Serialize)]
pub struct JobStatusResponse {
    pub job_id: JobId,
    pub status: JobStatus,
    pub progress: f32,
    pub metrics: crate::common::types::JobMetrics,
    pub error: Option<String>,
    pub outputs: Vec<String>,
}

#[derive(Debug, serde::Serialize)]
pub struct RegisterResponse {
    pub worker_id: WorkerId,
}

#[derive(Debug)]
pub enum ApiError {
    Engine(EngineError),
    BadRequest(String),
    Internal(anyhow::Error),
}

impl From<EngineError> for ApiError {
    fn from(value: EngineError) -> Self {
        ApiError::Engine(value)
    }
}

impl From<anyhow::Error> for ApiError {
    fn from(err: anyhow::Error) -> Self {
        ApiError::Internal(err)
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> axum::response::Response {
        let (code, message) = match self {
            ApiError::Engine(EngineError::NotFound(msg)) => (StatusCode::NOT_FOUND, msg),
            ApiError::Engine(EngineError::Validation(msg)) => (StatusCode::BAD_REQUEST, msg),
            ApiError::Engine(other) => {
                warn!("engine error: {}", other);
                (StatusCode::INTERNAL_SERVER_ERROR, other.to_string())
            }
            ApiError::BadRequest(msg) => (StatusCode::BAD_REQUEST, msg),
            ApiError::Internal(err) => {
                warn!("internal error: {:?}", err);
                (StatusCode::INTERNAL_SERVER_ERROR, "internal error".into())
            }
        };

        (code, message).into_response()
    }
}
