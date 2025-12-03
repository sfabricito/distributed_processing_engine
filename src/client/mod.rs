pub mod commands;
pub mod types;

use anyhow::Result;
use dotenvy::dotenv;
use reqwest::redirect;
use std::env;
use std::time::Duration;

/// Shared application context with configured HTTP client and master URL.
#[derive(Clone)]
pub struct AppContext {
    pub master: String,
    pub client: reqwest::Client,
}

impl AppContext {
    /// Build a new context using an explicit master URL or MASTER_URL env, defaulting to localhost.
    pub fn new(master: Option<&str>) -> Result<Self> {
        let _ = dotenv();
        let master_url = master
            .map(|s| s.to_string())
            .or_else(|| env::var("MASTER_URL").ok())
            .unwrap_or_else(|| "http://127.0.0.1:8080".to_string());

        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(10))
            .redirect(redirect::Policy::none())
            .build()?;

        Ok(Self {
            master: master_url.trim_end_matches('/').to_string(),
            client,
        })
    }

    pub fn url(&self, path: &str) -> String {
        format!("{}/{}", self.master, path.trim_start_matches('/'))
    }
}

/// Color helper for status values.
pub fn color_status(status: &str) -> colored::ColoredString {
    use colored::Colorize;
    match status.to_uppercase().as_str() {
        "SUCCEEDED" | "COMPLETED" | "DONE" => status.green(),
        "RUNNING" | "ACCEPTED" | "PENDING" => status.yellow(),
        "FAILED" | "ERROR" => status.red(),
        _ => status.normal(),
    }
}
