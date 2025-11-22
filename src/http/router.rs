use std::sync::Arc;

use axum::routing::{get, post};
use axum::Router;

use crate::master::Master;

use super::handlers;
use super::handlers::ApiState;
use super::request::HttpRequest;

pub fn register_routes(base_path: &str, master: Arc<Master>) -> Router {
    let state = ApiState { master };
    let base = base_path.trim_end_matches('/');

    Router::new()
        .route(&format!("{}/jobs", base), post(handlers::submit_job))
        .route(&format!("{}/jobs/:id", base), get(handlers::job_status))
        .route(
            &format!("{}/jobs/:id/results", base),
            get(handlers::job_results),
        )
        .route(&format!("{}/heartbeat", base), post(handlers::heartbeat))
        .route(
            &format!("{}/register", base),
            post(handlers::register_worker),
        )
        .with_state(state)
}

pub fn handle_request(req: &HttpRequest, base_path: &str) -> String {
    let base = base_path.trim_end_matches('/');
    let health = format!("{}/health", base);
    match (req.method.as_str(), req.path.as_str()) {
        ("GET", path) if path == health => "ok".to_string(),
        _ => "not found".to_string(),
    }
}
