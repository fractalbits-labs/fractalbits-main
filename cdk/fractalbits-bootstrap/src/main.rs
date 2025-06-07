mod api_server;
mod bss_server;
mod common;
mod nss_server;
mod root_server;

use clap::Parser;
use cmd_lib::*;
use strum::{AsRefStr, EnumString};

#[derive(Parser, AsRefStr, EnumString, Clone)]
#[strum(serialize_all = "snake_case")]
#[command(rename_all = "snake_case")]
#[clap(
    name = "fractalbits-bootstrap",
    about = "Bootstrap for cloud ec2 instances"
)]
enum Service {
    #[clap(about = "Run on api_server instance to bootstrap fractalbits service(s)")]
    ApiServer {
        #[clap(long, long_help = "S3 bucket name for fractalbits service")]
        bucket: String,

        #[clap(long, long_help = "bss_server IP address")]
        bss_ip: String,

        #[clap(long, long_help = "primary nss_server IP address")]
        nss_ip: String,

        #[clap(long, long_help = "root_server IP address")]
        rss_ip: String,
    },

    #[clap(about = "Run on bss_server instance to bootstrap fractalbits service(s)")]
    BssServer,

    #[clap(about = "Run on nss_server instance to bootstrap fractalbits service(s)")]
    NssServer {
        #[clap(long, long_help = "S3 bucket name for fractalbits service")]
        bucket: String,

        #[clap(long, long_help = "Multi-attached EBS volume ID")]
        volume_id: String,
    },

    #[clap(about = "Run on root_server instance to bootstrap fractalbits service(s)")]
    RootServer {
        #[clap(long, long_help = "Primary nss_server ec2 instance ID")]
        primary_instance_id: String,

        #[clap(long, long_help = "Secondary nss_server ec2 instance ID")]
        secondary_instance_id: String,

        #[clap(long, long_help = "Multi-attached EBS volume ID")]
        volume_id: String,
    },
}

#[cmd_lib::main]
fn main() -> CmdResult {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info"))
        .format_target(false)
        .init();
    let args: Vec<String> = std::env::args().collect();
    info!("Bootstrapping: {args:?}");

    let service = Service::parse();
    match service {
        Service::ApiServer {
            bucket,
            bss_ip,
            nss_ip,
            rss_ip,
        } => api_server::bootstrap(&bucket, &bss_ip, &nss_ip, &rss_ip),
        Service::BssServer => bss_server::bootstrap(),
        Service::NssServer { bucket, volume_id } => nss_server::bootstrap(&bucket, &volume_id),
        Service::RootServer {
            primary_instance_id,
            secondary_instance_id,
            volume_id,
        } => root_server::bootstrap(&primary_instance_id, &secondary_instance_id, &volume_id),
    }
}
