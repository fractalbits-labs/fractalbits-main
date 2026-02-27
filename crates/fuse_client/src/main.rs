mod backend;
mod cache;
mod config;
mod error;
mod fs;
mod inode;
mod object_layout;

use clap::Parser;
use fuse3::MountOptions;
use fuse3::raw::Session;
use std::io::IsTerminal;
use std::path::PathBuf;
use std::sync::Arc;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

use crate::config::Config;

#[derive(Parser)]
#[clap(name = "fuse_client", about = "FUSE client for FractalBits S3")]
struct Opt {
    #[clap(short = 'c', long = "config", help = "Config file path")]
    config_file: Option<PathBuf>,

    #[clap(short = 'b', long = "bucket", help = "Bucket name (overrides config)")]
    bucket: Option<String>,

    #[clap(short = 'm', long = "mount", help = "Mount point (overrides config)")]
    mount_point: Option<String>,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let third_party_filter = "hyper_util=warn,aws_smithy=warn,aws_sdk=warn,h2=warn";
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
        .with({
            let is_terminal = std::io::stdout().is_terminal();
            tracing_subscriber::fmt::layer()
                .without_time()
                .with_ansi(false)
                .with_level(is_terminal)
                .with_target(is_terminal)
        })
        .init();

    let opt = Opt::parse();

    let mut cfg: Config = match opt.config_file {
        Some(config_file) => ::config::Config::builder()
            .add_source(::config::File::from(config_file).required(true))
            .add_source(::config::Environment::with_prefix("FUSE"))
            .build()?
            .try_deserialize()?,
        None => ::config::Config::builder()
            .add_source(::config::Environment::with_prefix("FUSE"))
            .build()?
            .try_deserialize()
            .unwrap_or_default(),
    };

    // CLI overrides
    if let Some(bucket) = opt.bucket {
        cfg.bucket_name = bucket;
    }
    if let Some(mount_point) = opt.mount_point {
        cfg.mount_point = mount_point;
    }

    let mount_point = cfg.mount_point.clone();
    let allow_other = cfg.allow_other;
    let worker_threads = cfg.worker_threads;
    let cfg = Arc::new(cfg);

    tracing::info!(
        bucket = %cfg.bucket_name,
        mount_point = %mount_point,
        "Starting FUSE client"
    );

    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(worker_threads)
        .enable_all()
        .build()?;

    rt.block_on(async move {
        // Initialize storage backend
        let backend = Arc::new(
            backend::StorageBackend::new(cfg)
                .await
                .map_err(std::io::Error::other)?,
        );

        let inodes = Arc::new(inode::InodeTable::new());
        let fuse_fs = fs::FuseFs::new(backend, inodes);

        // Configure mount options
        let mut mount_options = MountOptions::default();
        mount_options.fs_name("fractalbits").read_only(true);

        if allow_other {
            mount_options.allow_other(true);
        }

        // Mount the filesystem
        let mount_handle = Session::new(mount_options)
            .mount_with_unprivileged(fuse_fs, &mount_point)
            .await?;

        // Wait for either the mount to end or a signal
        let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("Failed to register SIGTERM handler");
        let mut sigint = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt())
            .expect("Failed to register SIGINT handler");

        tokio::select! {
            res = mount_handle => {
                res?;
            }
            _ = sigterm.recv() => {
                tracing::info!("Received SIGTERM, unmounting");
            }
            _ = sigint.recv() => {
                tracing::info!("Received SIGINT, unmounting");
            }
        }

        tracing::info!("FUSE client exited");
        Ok::<(), std::io::Error>(())
    })?;

    Ok(())
}
