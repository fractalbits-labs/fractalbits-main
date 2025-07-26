use std::sync::Arc;
use std::{net::SocketAddr, path::PathBuf};

mod api_key_routes;

use api_server::{
    config::{self, ArcConfig},
    handler::any_handler,
    AppState,
};
use axum::{
    extract::Request,
    routing::{delete, get, post},
    serve::ListenerExt,
    Router,
};
use clap::Parser;
use tower_http::{services::ServeDir, trace::TraceLayer};
use tracing::info;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, Layer};

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

#[derive(Parser)]
#[clap(name = "api_server", about = "API server")]
struct Opt {
    #[clap(short = 'c', long = "config", long_help = "Config file path")]
    pub config_file: Option<PathBuf>,
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
                .with_filter(tracing_subscriber::filter::LevelFilter::ERROR),
        )
        .with(tracing_subscriber::fmt::layer().without_time().with_filter(
            tracing_subscriber::filter::filter_fn(|meta| *meta.level() != tracing::Level::ERROR),
        ))
        .init();

    eprintln!(
        "build info: {}",
        option_env!("BUILD_INFO").unwrap_or_default()
    );

    let opt = Opt::parse();
    let config = match opt.config_file {
        Some(config_file) => config::read_config(config_file),
        None => config::Config::default(),
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

    let port = config.port;
    let web_root = &config.web_root.clone();
    let app_state = AppState::new(ArcConfig(Arc::new(config))).await;

    let api_key_routes = Router::new()
        .route("/", post(api_key_routes::create_api_key))
        .route("/", get(api_key_routes::list_api_keys))
        .route("/{key_id}", delete(api_key_routes::delete_api_key));

    let app = Router::new()
        .nest_service("/ui", ServeDir::new(web_root))
        .nest("/api_keys", api_key_routes)
        .fallback(any_handler)
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

    let addr = format!("0.0.0.0:{}", port);
    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .unwrap()
        .tap_io(|tcp_stream| {
            if let Err(err) = tcp_stream.set_nodelay(true) {
                tracing::warn!("failed to set TCP_NODELAY on incoming connection: {err:#}");
            }
        });

    info!("Server started at port {port}");
    if let Err(e) = axum::serve(listener, app).await {
        tracing::error!("Server stopped: {e}");
    }
}
