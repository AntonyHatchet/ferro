mod init;
mod router;

use ls_asf::service::ServiceRegistry;
use std::net::SocketAddr;
use std::sync::Arc;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    init::ensure_dirs();

    let port: u16 = std::env::var("GATEWAY_LISTEN")
        .ok()
        .and_then(|s| s.split(':').last().and_then(|p| p.parse().ok()))
        .unwrap_or(4566);

    let mut registry = ServiceRegistry::new();
    registry.register(Arc::new(ls_sqs::SqsService::new()));
    registry.register(Arc::new(ls_sns::SnsService::new()));
    registry.register(Arc::new(ls_s3::S3Service::new()));

    let state = Arc::new(registry);

    tracing::info!("Initializing resources...");
    init::run_init_config(&state).await;

    let app = router::create_router(state.clone());

    let addr = SocketAddr::from(([0, 0, 0, 0], port));
    tracing::info!("Ferro listening on {addr}");
    tracing::info!("Ready. Services: sqs, sns, s3");

    init::run_ready_scripts();

    let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
    axum::serve(listener, app).await.unwrap();
}
