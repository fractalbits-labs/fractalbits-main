use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::PathBuf;
use std::sync::Arc;
use std::{fs::File, io::Read};

mod api_key_routes;
mod cache_mgmt;

use actix_files::Files;
use actix_web::{App, HttpServer, middleware::Logger, web};
use api_server::runtime::uring::{config::UringConfig, reactor, ring::PerCoreRing};
use api_server::runtime::{listeners, per_core::PerCoreBuilder};
use api_server::{AppState, CacheCoordinator, Config, handler::any_handler};
use clap::Parser;
use data_types::Versioned;
use openssl::{
    pkey::{PKey, Private},
    ssl::{SslAcceptor, SslMethod},
};
use tracing::{error, info};
use tracing_subscriber::{Layer, layer::SubscriberExt, util::SubscriberInitExt};

#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

#[derive(Parser)]
#[clap(name = "api_server", about = "API server")]
struct Opt {
    #[clap(short = 'c', long = "config", long_help = "Config file path")]
    config_file: Option<PathBuf>,
}

fn load_private_key(key_path: &PathBuf) -> Result<PKey<Private>, Box<dyn std::error::Error>> {
    let mut file = File::open(key_path)?;
    let mut buffer = Vec::new();
    file.read_to_end(&mut buffer)?;

    if let Ok(key) = PKey::private_key_from_pem_passphrase(&buffer, b"password") {
        return Ok(key);
    }

    let key = PKey::private_key_from_pem(&buffer)?;
    Ok(key)
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

    let gui_web_root = std::env::var("GUI_WEB_ROOT").ok().map(PathBuf::from);
    if gui_web_root.is_some() {
        config.allow_missing_or_bad_signature = true;
    }

    let config = Arc::new(config);
    let port = config.port;
    let mgmt_port = config.mgmt_port;
    let mut https_config = config.https.clone();
    if std::env::var("HTTPS_DISABLED")
        .map(|v| v == "1")
        .unwrap_or(false)
    {
        https_config.enabled = false;
    }
    let web_root = gui_web_root.clone();
    let worker_count = std::thread::available_parallelism()
        .map(|n| n.get().saturating_sub(1).max(1))
        .unwrap_or(1);
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

    let cache_coordinator: Arc<CacheCoordinator<Versioned<String>>> =
        Arc::new(CacheCoordinator::new());
    let az_status_coordinator: Arc<CacheCoordinator<String>> = Arc::new(CacheCoordinator::new());

    let per_core_builder = PerCoreBuilder::new(per_core_rings.clone(), per_core_reactors.clone());

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
        mgmt_port,
        worker_count,
        "Starting unified server with reuseport listeners (thread-per-core)"
    );

    let config_clone = config.clone();
    let cache_coordinator_clone = cache_coordinator.clone();
    let az_status_coordinator_clone = az_status_coordinator.clone();
    let web_root_clone = web_root.clone();

    let mut server = HttpServer::new(move || {
        let per_core_ctx = per_core_builder
            .build_context()
            .unwrap_or_else(|e| panic!("failed to init per-core context: {e}"));
        per_core_builder.pin_current_thread(per_core_ctx.worker_index());
        reactor::set_current_reactor(reactor::ReactorTransport::new(per_core_ctx.reactor()));
        let app_state = Arc::new(AppState::new_per_core_sync(
            config_clone.clone(),
            per_core_ctx.ring(),
            per_core_ctx.reactor(),
            cache_coordinator_clone.clone(),
            az_status_coordinator_clone.clone(),
        ));

        let mut app = App::new()
            .app_data(web::Data::new(app_state))
            .app_data(web::Data::new(per_core_ctx))
            .app_data(web::PayloadConfig::default().limit(5_368_709_120))
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
            .service(
                web::scope("/api_keys")
                    .route("/", web::post().to(api_key_routes::create_api_key))
                    .route("/", web::get().to(api_key_routes::list_api_keys))
                    .route(
                        "/{key_id}",
                        web::delete().to(api_key_routes::delete_api_key),
                    ),
            );

        if let Some(ref web_root) = web_root_clone {
            let static_dir = web_root.clone();
            app = app.service(Files::new("/ui", static_dir).index_file("index.html"));
        }

        app.default_service(web::route().to(any_handler))
    });

    server = server.workers(worker_count);

    for listener in http_listeners {
        server = server.listen(listener).unwrap();
    }

    for listener in mgmt_listeners {
        server = server.listen(listener).unwrap();
    }

    if https_config.enabled {
        let https_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), https_config.port);
        let https_listeners =
            listeners::bind_reuseport(https_addr, worker_count).unwrap_or_else(|e| {
                error!("Failed to bind HTTPS listeners on {https_addr}: {e}");
                std::process::exit(1);
            });

        info!(
            https_port = https_config.port,
            "HTTPS enabled on unified server"
        );

        let key_path = PathBuf::from(&https_config.key_file);
        let private_key = match load_private_key(&key_path) {
            Ok(private_key) => private_key,
            Err(e) => {
                error!(
                    "Failed to load private key from {}: {e}",
                    key_path.display()
                );
                error!("If using mkcert, ensure the key is unencrypted");
                std::process::exit(1);
            }
        };

        let cert_path = PathBuf::from(&https_config.cert_file);

        if https_config.force_http1_only {
            info!("Configuring HTTPS for HTTP/1.1 only");
        } else {
            info!("Configuring HTTPS for both HTTP/1.1 and HTTP/2");
        }

        for listener in https_listeners {
            let mut builder = SslAcceptor::mozilla_intermediate(SslMethod::tls()).unwrap();
            builder.set_private_key(&private_key).unwrap();
            if let Err(e) = builder.set_certificate_chain_file(&cert_path) {
                error!(
                    "Failed to load certificate chain from {}: {e}",
                    cert_path.display()
                );
                std::process::exit(1);
            }

            if https_config.force_http1_only {
                builder.set_alpn_protos(b"\x08http/1.1").unwrap();
            }

            server = server.listen_openssl(listener, builder).unwrap();
        }
    } else {
        info!("HTTPS is disabled");
    }

    server.run().await.unwrap()
}
