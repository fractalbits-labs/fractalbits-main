use std::io;
use std::sync::Arc;
use std::{net::SocketAddr, path::PathBuf};

mod api_key_routes;
mod cache_mgmt;

use api_server::{handler::any_handler, AppState, Config};
use axum::{
    extract::Request,
    routing::{delete, get, post},
    serve::ListenerExt,
    Router,
};
use clap::Parser;
use tower_http::{services::ServeDir, trace::TraceLayer};
use tracing::{error, info};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, Layer};

#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

#[derive(Parser)]
#[clap(name = "api_server", about = "API server")]
struct Opt {
    #[clap(short = 'c', long = "config", long_help = "Config file path")]
    config_file: Option<PathBuf>,
}

#[tokio::main]
async fn main() {
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| {
                format!("{}=warn,tower_http=warn", env!("CARGO_CRATE_NAME")).into()
            }),
        )
        .with(
            tracing_subscriber::fmt::layer()
                .with_file(true)
                .with_line_number(true)
                .without_time()
                .with_writer(io::stderr)
                .with_filter(tracing_subscriber::filter::LevelFilter::ERROR),
        )
        .with(
            tracing_subscriber::fmt::layer()
                .without_time()
                .with_writer(io::stderr)
                .with_filter(tracing_subscriber::filter::filter_fn(|meta| {
                    *meta.level() != tracing::Level::ERROR
                })),
        )
        .init();

    eprintln!(
        "build info: {}",
        option_env!("BUILD_INFO").unwrap_or_default()
    );

    let opt = Opt::parse();
    let mut config = match opt.config_file {
        Some(config_file) => config::Config::builder()
            .add_source(config::File::from(config_file).required(true))
            .add_source(config::Environment::with_prefix("APP"))
            .build()
            .unwrap()
            .try_deserialize()
            .unwrap(),
        None => {
            // Check for APP_BLOB_STORAGE_BACKEND environment variable
            if let Ok(backend) = std::env::var("APP_BLOB_STORAGE_BACKEND") {
                info!("APP_BLOB_STORAGE_BACKEND: {backend}");
                match backend.as_str() {
                    "s3_express_multi_az" => Config::s3_express_multi_az_with_tracking(),
                    "s3_express_single_az" => Config::s3_express_single_az(),
                    "hybrid_single_az" => Config::hybrid_single_az(),
                    _ => {
                        error!("Invalid APP_BLOB_STORAGE_BACKEND value: {backend}");
                        std::process::exit(1);
                    }
                }
            } else {
                config::Config::builder()
                    .add_source(config::Environment::with_prefix("APP"))
                    .build()
                    .unwrap()
                    .try_deserialize()
                    .unwrap_or_else(|_| Config::default())
            }
        }
    };

    if config.with_metrics {
        #[cfg(feature = "metrics_statsd")]
        {
            use metrics_exporter_statsd::StatsdBuilder;
            // Initialize StatsD metrics exporter
            let recorder = StatsdBuilder::from("127.0.0.1", 8125)
                .with_buffer_size(1)
                .build(None)
                .expect("Could not build StatsD recorder");
            metrics::set_global_recorder(Box::new(recorder))
                .expect("Could not install StatsD exporter");
            info!("Metrics exporter for StatsD installed");
        }
        #[cfg(feature = "metrics_prometheus")]
        {
            use metrics_exporter_prometheus::PrometheusBuilder;
            // Initialize Prometheus metrics exporter
            PrometheusBuilder::new()
                .with_http_listener("0.0.0.0:8085".parse::<SocketAddr>().unwrap())
                .install()
                .expect("Could not build Prometheus recorder");
            info!("Metrics exporter for Prometheus installed");
        }
    }

    let api_key_routes = Router::new()
        .route("/", post(api_key_routes::create_api_key))
        .route("/", get(api_key_routes::list_api_keys))
        .route("/{key_id}", delete(api_key_routes::delete_api_key));

    // Cache management routes - internal use only
    let mgmt_routes = Router::new()
        .route("/health", get(cache_mgmt::mgmt_health))
        .route(
            "/cache/invalidate/bucket/{name}",
            post(cache_mgmt::invalidate_bucket),
        )
        .route(
            "/cache/invalidate/api_key/{id}",
            post(cache_mgmt::invalidate_api_key),
        )
        .route(
            "/cache/update/az_status/{id}",
            post(cache_mgmt::update_az_status),
        )
        .route("/cache/clear", post(cache_mgmt::clear_cache));

    // Main application router
    let main_router = if let Ok(web_root) = std::env::var("GUI_WEB_ROOT") {
        info!(%web_root, "serving ui");
        config.allow_missing_or_bad_signature = true;
        Router::new()
            .nest_service("/ui", ServeDir::new(web_root))
            .nest("/api_keys", api_key_routes)
            .fallback(any_handler)
    } else {
        Router::new().fallback(any_handler)
    };

    let port = config.port;
    let mgmt_port = config.mgmt_port;
    let app_state = Arc::new(AppState::new(Arc::new(config)).await);
    let main_app = main_router
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
        .with_state(app_state.clone())
        .into_make_service_with_connect_info::<SocketAddr>();

    // Start main server
    let main_addr = format!("0.0.0.0:{port}");
    let main_listener = tokio::net::TcpListener::bind(main_addr)
        .await
        .unwrap()
        .tap_io(|tcp_stream| {
            if let Err(err) = tcp_stream.set_nodelay(true) {
                tracing::warn!("failed to set TCP_NODELAY on incoming connection: {err:#}");
            }
        });

    info!("Main server started at port {port}");

    // Start management server
    let mgmt_app = Router::new()
        .nest("/mgmt", mgmt_routes)
        .layer(
            TraceLayer::new_for_http()
                .make_span_with(|req: &Request| {
                    let method = req.method();
                    let uri = req.uri();
                    let path = uri.path();

                    tracing::debug_span!("mgmt_request", %method, %uri, path)
                })
                .on_failure(()),
        )
        .with_state(app_state)
        .into_make_service_with_connect_info::<SocketAddr>();

    let mgmt_addr = format!("0.0.0.0:{mgmt_port}");
    let mgmt_listener = tokio::net::TcpListener::bind(mgmt_addr)
        .await
        .unwrap()
        .tap_io(|tcp_stream| {
            if let Err(err) = tcp_stream.set_nodelay(true) {
                tracing::warn!("failed to set TCP_NODELAY on mgmt connection: {err:#}");
            }
        });

    info!("Management server started at port {mgmt_port}");

    // Run both servers concurrently
    tokio::select! {
        result = axum::serve(main_listener, main_app) => {
            if let Err(e) = result {
                tracing::error!("Main server stopped: {e}");
            }
        }
        result = axum::serve(mgmt_listener, mgmt_app) => {
            if let Err(e) = result {
                tracing::error!("Management server stopped: {e}");
            }
        }
    }
}
