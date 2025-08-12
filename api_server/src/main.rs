use std::sync::Arc;
use std::{net::SocketAddr, path::PathBuf};

mod api_key_routes;
mod cache_mgmt;

use actix_files::Files;
use actix_web::{middleware::Logger, web, App, HttpServer};
use api_server::{handler::any_handler, AppState, Config};
use clap::Parser;
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

#[actix_web::main]
async fn main() {
    // AWS SDK suppression filter
    let third_party_filter =
        "tower_http=warn,hyper_util=warn,aws_smithy=warn,aws_sdk=warn,actix_web=warn,actix_server=warn";
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .map(|filter| {
                    format!("{filter},{third_party_filter}")
                        .parse()
                        .unwrap_or(filter)
                })
                .unwrap_or_else(|_| format!("info,{third_party_filter}").into()),
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

    let port = config.port;
    let mgmt_port = config.mgmt_port;
    // Get web root from environment variable
    let web_root = match std::env::var("GUI_WEB_ROOT") {
        Ok(gui_web_root) => {
            config.allow_missing_or_bad_signature = true;
            Some(PathBuf::from(gui_web_root))
        }
        Err(_) => None,
    };
    let app_state = AppState::new(Arc::new(config)).await;
    let app_state_arc = Arc::new(app_state);

    // Start main server
    info!("Main server started at port {port}");
    let main_server = HttpServer::new({
        let app_state_arc = app_state_arc.clone();
        let web_root = web_root.clone();
        move || {
            let web_root = web_root.clone();
            let mut app = App::new()
                .app_data(web::Data::new(app_state_arc.clone()))
                // Configure payload size limits for S3 operations
                // S3 supports up to 5GB per object, but multipart uploads can be up to 5TB
                // Set a reasonable limit for testing and production use
                .app_data(web::PayloadConfig::default().limit(5_368_709_120)) // 5GB limit
                .wrap(Logger::default());

            if let Some(web_root) = web_root {
                app = app
                    .service(Files::new("/ui", web_root).index_file("index.html"))
                    .service(
                        web::scope("/api_keys")
                            .route("/", web::post().to(api_key_routes::create_api_key))
                            .route("/", web::get().to(api_key_routes::list_api_keys))
                            .route(
                                "/{key_id}",
                                web::delete().to(api_key_routes::delete_api_key),
                            ),
                    )
            }
            app.default_service(web::route().to(any_handler))
        }
    })
    .bind(format!("0.0.0.0:{port}"))
    .unwrap();

    // Start management server
    info!("Management server started at port {mgmt_port}");
    let mgmt_server = HttpServer::new({
        let app_state_arc = app_state_arc.clone();
        move || {
            App::new()
                .app_data(web::Data::new(app_state_arc.clone()))
                .wrap(Logger::default())
                .service(
                    web::scope("/mgmt")
                        .route("/health", web::get().to(cache_mgmt::mgmt_health))
                        .route(
                            "/cache/invalidate/bucket/{name}",
                            web::post().to(cache_mgmt::invalidate_bucket),
                        )
                        .route(
                            "/cache/invalidate/api_key/{id}",
                            web::post().to(cache_mgmt::invalidate_api_key),
                        )
                        .route(
                            "/cache/update/az_status/{id}",
                            web::post().to(cache_mgmt::update_az_status),
                        )
                        .route("/cache/clear", web::post().to(cache_mgmt::clear_cache)),
                )
        }
    })
    .bind(format!("0.0.0.0:{mgmt_port}"))
    .unwrap();

    // Run both servers concurrently
    let main_server_future = main_server.run();
    let mgmt_server_future = mgmt_server.run();

    tokio::select! {
        result = main_server_future => {
            if let Err(e) = result {
                tracing::error!("Main server stopped: {e}");
            }
        }
        result = mgmt_server_future => {
            if let Err(e) = result {
                tracing::error!("Management server stopped: {e}");
            }
        }
    }
}
