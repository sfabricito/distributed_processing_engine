pub mod dag;
pub mod metrics;
pub mod progress;
pub mod results;
pub mod status;
pub mod submit;
pub mod workers;

use crate::client::AppContext;
use colored::Colorize;
use reqwest::Response;
use std::time::Duration;
use tokio::time::sleep;

pub async fn fetch_json<T: serde::de::DeserializeOwned>(
    ctx: &AppContext,
    url: &str,
) -> Result<T, anyhow::Error> {
    let resp = ctx.client.get(url).send().await?;
    let resp = handle_error(resp).await?;
    Ok(resp.json::<T>().await?)
}

pub async fn post_json<T: serde::de::DeserializeOwned>(
    ctx: &AppContext,
    url: &str,
    body: &serde_json::Value,
) -> Result<T, anyhow::Error> {
    let resp = ctx.client.post(url).json(body).send().await?;
    let resp = handle_error(resp).await?;
    Ok(resp.json::<T>().await?)
}

async fn handle_error(resp: Response) -> Result<Response, anyhow::Error> {
    if resp.status().is_success() {
        return Ok(resp);
    }
    let status = resp.status();
    let text = resp.text().await.unwrap_or_default();
    Err(anyhow::anyhow!(
        "{} {} {}",
        "[ERROR]".red(),
        status.as_u16(),
        text
    ))
}

/// Poll until terminal state with small delay.
pub async fn watch_until_terminal<F, Fut>(mut check: F) -> Result<(), anyhow::Error>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<bool, anyhow::Error>>,
{
    loop {
        if check().await? {
            break;
        }
        sleep(Duration::from_secs(2)).await;
    }
    Ok(())
}

pub fn print_error(msg: &str, details: Option<&str>) {
    eprintln!("{} {}", "[FAILED]".red().bold(), msg);
    if let Some(d) = details {
        if !d.is_empty() {
            eprintln!("Reason: {}", d);
        }
    }
}
