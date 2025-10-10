use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;

mod api_key_routes;
mod cache_mgmt;

use actix_web::{App, HttpServer, middleware::Logger, web};
use api_server::runtime::{listeners, per_core::PerCoreBuilder};
use api_server::uring::{config::UringConfig, reactor, ring::PerCoreRing};
use api_server::{AppState, Config, handler::any_handler};
use clap::Parser;
use tracing::{error, info};
use tracing_subscriber::{Layer, layer::SubscriberExt, util::SubscriberInitExt};

#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

#[derive(Parser)]
#[clap(name = "api_server", about = "API server")]
struct Opt {
    #[clap(short = 'c', long = "config", long_help = "Config file path")]
    config_file: Option<std::path::PathBuf>,
}

#[actix_web::main]
async fn main() {
    // AWS SDK suppression filter
    let third_party_filter = "tower_http=warn,hyper_util=warn,aws_smithy=warn,aws_sdk=warn,actix_web=warn,actix_server=warn,h2=warn";
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

    let main_build_info = option_env!("MAIN_BUILD_INFO").unwrap_or("unknown");
    let build_timestamp = option_env!("BUILD_TIMESTAMP").unwrap_or("unknown");
    let build_info = format!("{}, build time: {}", main_build_info, build_timestamp);
    eprintln!("build info: {}", build_info);

    let opt = Opt::parse();
    let config = match opt.config_file {
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
                    "s3_express_multi_az" => Config::s3_express_multi_az(),
                    "s3_hybrid_single_az" => Config::s3_hybrid_single_az(),
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

    let config = Arc::new(config);
    let port = config.port;
    let mgmt_port = config.mgmt_port;
    let worker_count = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(2);
    let uring_config = UringConfig::default();

    let per_core_rings: Arc<Vec<Arc<PerCoreRing>>> = Arc::new(
        (0..worker_count)
            .map(|idx| {
                Arc::new(
                    PerCoreRing::new(idx, &uring_config)
                        .expect("failed to create per-core io_uring ring"),
                )
            })
            .collect(),
    );

    let per_core_reactors: Arc<Vec<Arc<reactor::RpcReactorHandle>>> = Arc::new(
        per_core_rings
            .iter()
            .enumerate()
            .map(|(idx, ring)| reactor::spawn_rpc_reactor(idx, ring.clone()))
            .collect(),
    );

    let http_app_states = Arc::new(
        build_per_core_app_states(
            worker_count,
            config.clone(),
            per_core_rings.clone(),
            per_core_reactors.clone(),
        )
        .await,
    );
    let mgmt_app_states = Arc::new(
        build_per_core_app_states(
            worker_count,
            config.clone(),
            per_core_rings.clone(),
            per_core_reactors.clone(),
        )
        .await,
    );

    let http_per_core = PerCoreBuilder::new(per_core_rings.clone(), per_core_reactors.clone());
    let mgmt_per_core = PerCoreBuilder::new(per_core_rings.clone(), per_core_reactors.clone());

    let http_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), port);
    let mgmt_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), mgmt_port);

    let http_listeners = listeners::bind_reuseport(http_addr, worker_count).unwrap_or_else(|e| {
        error!("Failed to bind HTTP listeners on {http_addr}: {e}");
        std::process::exit(1);
    });
    let mgmt_listeners = listeners::bind_reuseport(mgmt_addr, worker_count).unwrap_or_else(|e| {
        error!("Failed to bind management listeners on {mgmt_addr}: {e}");
        std::process::exit(1);
    });

    info!(
        port,
        worker_count, "HTTP server started with reuseport listeners"
    );
    let mut http_server = HttpServer::new({
        let per_core_builder = http_per_core.clone();
        let app_states = http_app_states.clone();
        move || {
            let per_core_ctx = per_core_builder
                .build_context()
                .unwrap_or_else(|e| panic!("failed to init per-core context: {e}"));
            per_core_builder.pin_current_thread(per_core_ctx.worker_index());
            let worker_index = per_core_ctx.worker_index();
            let app_state = app_states
                .get(worker_index)
                .unwrap_or_else(|| panic!("missing app state for worker {worker_index}"))
                .clone();

            App::new()
                .app_data(web::Data::new(app_state))
                .app_data(web::Data::new(per_core_ctx))
                .app_data(web::PayloadConfig::default().limit(5_368_709_120))
                .wrap(Logger::default())
                .service(
                    web::scope("/api_keys")
                        .route("/", web::post().to(api_key_routes::create_api_key))
                        .route("/", web::get().to(api_key_routes::list_api_keys))
                        .route(
                            "/{key_id}",
                            web::delete().to(api_key_routes::delete_api_key),
                        ),
                )
                .default_service(web::route().to(any_handler))
        }
    });

    http_server = http_server.workers(worker_count);

    for listener in http_listeners {
        http_server = http_server.listen(listener).unwrap();
    }

    info!(
        mgmt_port,
        worker_count, "Management server started with reuseport listeners"
    );
    let mut mgmt_server = HttpServer::new({
        let per_core_builder = mgmt_per_core.clone();
        let app_states = mgmt_app_states.clone();
        move || {
            let per_core_ctx = per_core_builder
                .build_context()
                .unwrap_or_else(|e| panic!("failed to init per-core context: {e}"));
            per_core_builder.pin_current_thread(per_core_ctx.worker_index());
            let worker_index = per_core_ctx.worker_index();
            let app_state = app_states
                .get(worker_index)
                .unwrap_or_else(|| panic!("missing mgmt app state for worker {worker_index}"))
                .clone();

            App::new()
                .app_data(web::Data::new(app_state))
                .app_data(web::Data::new(per_core_ctx))
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
    });

    mgmt_server = mgmt_server.workers(worker_count);

    for listener in mgmt_listeners {
        mgmt_server = mgmt_server.listen(listener).unwrap();
    }

    let http_server_future = http_server.run();
    let mgmt_server_future = mgmt_server.run();

    tokio::select! {
        result = http_server_future => {
            if let Err(e) = result {
                tracing::error!("HTTP server stopped: {e}");
            }
        }
        result = mgmt_server_future => {
            if let Err(e) = result {
                tracing::error!("Management server stopped: {e}");
            }
        }
    }
}

async fn build_per_core_app_states(
    worker_count: usize,
    config: Arc<Config>,
    rings: Arc<Vec<Arc<PerCoreRing>>>,
    reactors: Arc<Vec<Arc<reactor::RpcReactorHandle>>>,
) -> Vec<Arc<AppState>> {
    let mut states = Vec::with_capacity(worker_count);
    for idx in 0..worker_count {
        let ring = rings
            .get(idx)
            .expect("missing per-core ring for app state")
            .clone();
        let reactor_handle = reactors
            .get(idx)
            .expect("missing per-core reactor for app state")
            .clone();
        let state = AppState::new_per_core(config.clone(), ring, reactor_handle).await;
        states.push(Arc::new(state));
    }
    states
}
