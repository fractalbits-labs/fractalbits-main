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
    },

    #[clap(about = "Run on bss_server instance to bootstrap fractalbits service(s)")]
    BssServer,

    #[clap(about = "Run on nss_server instance to bootstrap fractalbits service(s)")]
    NssServer {
        #[clap(long, long_help = "S3 bucket name for fractalbits service")]
        bucket: String,

        #[clap(long, long_help = "As secondary instance")]
        secondary: bool,
    },

    #[clap(about = "Run on root_server instance to bootstrap fractalbits service(s)")]
    RootServer,
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
        Service::ApiServer { bucket } => api_server::bootstrap(&bucket),
        Service::BssServer => bss_server::bootstrap(),
        Service::NssServer { bucket, secondary } => nss_server::bootstrap(&bucket, secondary),
        Service::RootServer => root_server::bootstrap(),
    }
}
