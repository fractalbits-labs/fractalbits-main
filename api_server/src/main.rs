use std::hash::{DefaultHasher, Hash, Hasher};
use std::net::SocketAddr;
use std::sync::Arc;

use axum::{
    extract::{ConnectInfo, MatchedPath, Path, Request, State},
    http::StatusCode,
    routing::get,
    Router,
};
use nss_rpc_client::rpc_client::RpcClient;
use tower_http::trace::TraceLayer;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

const MAX_NSS_CONNECTION: usize = 19;

struct AppState {
    rpc_clients: Vec<RpcClient>,
}

fn calculate_hash<T: Hash>(t: &T) -> usize {
    let mut s = DefaultHasher::new();
    t.hash(&mut s);
    s.finish() as usize
}

async fn get_obj(
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    State(state): State<Arc<AppState>>,
    Path(key): Path<String>,
) -> Result<String, (StatusCode, String)> {
    let hash = calculate_hash(&addr) % MAX_NSS_CONNECTION;
    let mut key = format!("/{key}");
    key.push('\0');
    let resp = nss_rpc_client::nss_get_inode(&state.rpc_clients[hash], key)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))
        .unwrap();
    match serde_json::to_string_pretty(&resp.result) {
        Ok(resp) => Ok(resp),
        Err(e) => Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("internal server error: {e}"),
        )),
    }
}

async fn put_obj(
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    State(state): State<Arc<AppState>>,
    Path(key): Path<String>,
    value: String,
) -> Result<String, (StatusCode, String)> {
    let hash = calculate_hash(&addr) % MAX_NSS_CONNECTION;
    let mut key = format!("/{key}");
    key.push('\0');
    let resp = nss_rpc_client::nss_put_inode(&state.rpc_clients[hash], key, value)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))
        .unwrap();
    match serde_json::to_string_pretty(&resp.result) {
        Ok(resp) => Ok(resp),
        Err(e) => Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("internal server error: {e}"),
        )),
    }
}

#[tokio::main(flavor = "multi_thread", worker_threads = 8)]
async fn main() {
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| {
                format!("{}=warn,tower_http=warn", env!("CARGO_CRATE_NAME")).into()
            }),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();

    let mut rpc_clients = Vec::with_capacity(MAX_NSS_CONNECTION);
    for _i in 0..MAX_NSS_CONNECTION {
        let rpc_client = match RpcClient::new("127.0.0.1:9224").await {
            Ok(rpc_client) => rpc_client,
            Err(e) => {
                tracing::error!("failed to start rpc client: {e}");
                return;
            }
        };
        rpc_clients.push(rpc_client);
    }
    let shared_state = Arc::new(AppState { rpc_clients });

    let app = Router::new()
        .route("/*key", get(get_obj).post(put_obj))
        .layer(
            TraceLayer::new_for_http()
                // Create our own span for the request and include the matched path. The matched
                // path is useful for figuring out which handler the request was routed to.
                .make_span_with(|req: &Request| {
                    let method = req.method();
                    let uri = req.uri();

                    // axum automatically adds this extension.
                    let matched_path = req
                        .extensions()
                        .get::<MatchedPath>()
                        .map(|matched_path| matched_path.as_str());

                    tracing::debug_span!("request", %method, %uri, matched_path)
                })
                // By default `TraceLayer` will log 5xx responses but we're doing our specific
                // logging of errors so disable that
                .on_failure(()),
        )
        .with_state(shared_state);

    let addr = "0.0.0.0:3000";
    let listener = match tokio::net::TcpListener::bind(addr).await {
        Ok(listener) => listener,
        Err(e) => {
            tracing::error!("failed to bind addr {addr}: {e}");
            return;
        }
    };

    tracing::info!("server started");
    if let Err(e) = axum::serve(
        listener,
        app.into_make_service_with_connect_info::<SocketAddr>(),
    )
    .await
    {
        tracing::error!("serer stopped: {e}");
    }
}
