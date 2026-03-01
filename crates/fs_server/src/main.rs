mod backend;
mod cache;
mod config;
mod error;
mod fs;
mod inode;
mod rpc;
mod rpc_bss;
mod rpc_nss;
mod rpc_rss;

use clap::Parser;
use fractal_fuse::MountOptions;
use fractal_fuse::Session;
use std::io::IsTerminal;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

use crate::config::Config;

#[derive(Parser)]
#[clap(name = "fs_server", about = "FUSE file server for FractalBits S3")]
struct Opt {
    #[clap(short = 'c', long = "config", help = "Config file path")]
    config_file: Option<PathBuf>,

    #[clap(short = 'b', long = "bucket", env = "FUSE_BUCKET_NAME", help = "Bucket name (overrides config)")]
    bucket: Option<String>,

    #[clap(short = 'm', long = "mount", env = "FUSE_MOUNT_POINT", help = "Mount point (overrides config)")]
    mount_point: Option<String>,

    #[clap(short = 'r', long = "read-write", env = "FUSE_READ_WRITE", help = "Enable read-write mode")]
    read_write: bool,
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
    if opt.read_write {
        cfg.read_write = true;
    }

    let mount_point = cfg.mount_point.clone();
    let read_write = cfg.read_write;

    tracing::info!(
        bucket = %cfg.bucket_name,
        mount_point = %mount_point,
        read_write = read_write,
        "Starting FUSE client"
    );

    // Discover backend configuration (NSS address, DataVgInfo, bucket) via RSS.
    // This runs on a temporary compio runtime since we need async RPC.
    let backend_config = {
        let cfg_ref = &cfg;
        compio_runtime::Runtime::new()
            .expect("Failed to create compio runtime for discovery")
            .block_on(backend::BackendConfig::discover(cfg_ref))
            .map_err(|e| std::io::Error::other(format!("Backend discovery failed: {e}")))?
    };
    let backend_config = Arc::new(backend_config);

    let inodes = Arc::new(inode::InodeTable::new());
    let fuse_fs = fs::FuseFs::new(backend_config, inodes, read_write);

    // Configure mount options
    let mount_options = MountOptions::default()
        .fs_name("fractalbits")
        .read_only(!read_write)
        .allow_other(cfg.allow_other);

    // Mount and run the filesystem (blocks until shutdown)
    Session::new(mount_options).run(fuse_fs, Path::new(&mount_point))?;

    tracing::info!("FUSE client exited");
    Ok(())
}
