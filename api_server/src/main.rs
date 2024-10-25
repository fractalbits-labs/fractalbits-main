use std::net::SocketAddr;
use std::sync::Arc;

use api_server::{handler::any_handler, AppState};
use axum::{extract::Request, routing};
use tower_http::trace::TraceLayer;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

#[tokio::main]
async fn main() {
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| {
                format!("{}=warn,tower_http=warn", env!("CARGO_CRATE_NAME")).into()
            }),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();

    let app_state = match AppState::new("127.0.0.1:9224").await {
        Ok(state) => state,
        Err(e) => {
            tracing::error!("Failed to start rpc clients: {e}");
            return;
        }
    };

    let app = routing::any(any_handler)
        .layer(
            TraceLayer::new_for_http()
                .make_span_with(|req: &Request| {
                    let method = req.method();
                    let uri = req.uri();
                    let path = uri.path();

                    tracing::debug_span!("request", %method, %uri, path)
                })
                // By default `TraceLayer` will log 5xx responses but we're doing our specific
                // logging of errors so disable that
                .on_failure(()),
        )
        .with_state(Arc::new(app_state))
        .into_make_service_with_connect_info::<SocketAddr>();

    let addr = "0.0.0.0:3000";
    let listener = match tokio::net::TcpListener::bind(addr).await {
        Ok(listener) => listener,
        Err(e) => {
            tracing::error!("Failed to bind addr {addr}: {e}");
            return;
        }
    };

    tracing::info!("Server started");
    if let Err(e) = axum::serve(listener, app).tcp_nodelay(true).await {
        tracing::error!("Server stopped: {e}");
    }
}
