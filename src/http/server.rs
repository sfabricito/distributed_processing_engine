use std::net::SocketAddr;
use std::sync::Arc;

use anyhow::Result;
use tokio::net::TcpListener;
use tracing::info;

use crate::master::Master;

use super::router::register_routes;

pub async fn start_http_server(base_path: &str, master: Arc<Master>) -> Result<()> {
    let addr: SocketAddr = master.config().master_addr().parse()?;
    let listener = TcpListener::bind(addr).await?;
    let app = register_routes(base_path, master);

    info!("HTTP server listening on {} with base {}", addr, base_path);
    axum::serve(listener, app.into_make_service()).await?;
    Ok(())
}
