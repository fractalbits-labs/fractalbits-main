mod api_server;
mod bench_client;
mod bench_server;
mod bss_server;
mod common;
mod gui_server;
mod nss_server;
mod root_server;

use clap::Parser;
use cmd_lib::*;
use common::*;
use std::io::Write;
use strum::AsRefStr;

#[derive(Parser)]
#[clap(
    name = "fractalbits-bootstrap",
    about = "Bootstrap for cloud ec2 instances"
)]
struct Opts {
    #[clap(flatten)]
    common: CommonOpts,

    #[command(subcommand)]
    command: Command,
}

#[derive(Parser)]
#[command(rename_all = "snake_case")]
struct CommonOpts {
    #[clap(long, default_value = "false", long_help = "For benchmarking")]
    for_bench: bool,
}

#[allow(clippy::enum_variant_names)]
#[derive(Parser, AsRefStr)]
#[command(rename_all = "snake_case")]
#[strum(serialize_all = "snake_case")]
enum Command {
    #[clap(about = "Run on api_server instance to bootstrap fractalbits service(s)")]
    ApiServer {
        #[clap(long, long_help = "S3 bucket name for fractalbits service")]
        bucket: String,

        #[clap(long, long_help = "primary nss_server IP address")]
        nss_ip: String,

        #[clap(long, long_help = "root_server IP address")]
        rss_ip: String,
    },

    #[clap(about = "Run on api_server instance to bootstrap fractalbits service(s)")]
    GuiServer {
        #[clap(long, long_help = "S3 bucket name for fractalbits service")]
        bucket: String,

        #[clap(long, long_help = "primary nss_server IP address")]
        nss_ip: String,

        #[clap(long, long_help = "root_server IP address")]
        rss_ip: String,
    },

    #[clap(about = "Run on bss_server instance to bootstrap fractalbits service(s)")]
    BssServer {
        #[clap(long, default_value = "false", long_help = "For meta stack testing")]
        meta_stack_testing: bool,
    },

    #[clap(about = "Run on nss_server instance to bootstrap fractalbits service(s)")]
    NssServer {
        #[clap(long, long_help = "S3 bucket name for fractalbits service")]
        bucket: String,

        #[clap(long, long_help = "Multi-attached EBS volume ID")]
        volume_id: String,

        #[clap(long, long_help = "EC2 IAM role")]
        iam_role: String,

        #[clap(long, default_value = "false", long_help = "For meta stack testing")]
        meta_stack_testing: bool,
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

    #[clap(
        about = "Run on nss_server instance to format itself when receiving ssm command from root_server"
    )]
    FormatNss {
        #[clap(
            long,
            default_value = "false",
            long_help = "Testing mode (create testing art tree)"
        )]
        testing_mode: bool,

        #[clap(long, long_help = "EBS device")]
        ebs_dev: String,
    },

    #[clap(about = "Run on bench_server instance to benchmark fractalbits service(s)")]
    BenchServer {
        #[clap(long, long_help = "Service endpoint for benchmark")]
        api_server_service_endpoint: Option<String>,

        #[clap(long, long_help = "Number of api servers")]
        api_server_num: Option<usize>,

        #[clap(long, long_help = "Number of bench clients")]
        bench_client_num: usize,
    },

    #[clap(about = "Run on bench_client instance to benchmark fractalbits service(s)")]
    BenchClient,
}

#[cmd_lib::main]
fn main() -> CmdResult {
    env_logger::Builder::new()
        .format(|buf, record| {
            let timestamp = chrono::Local::now().format("%b %d %H:%M:%S").to_string();
            let process_name = std::env::current_exe()
                .ok()
                .and_then(|path| {
                    path.file_name()
                        .map(|name| name.to_string_lossy().into_owned())
                })
                .unwrap_or_else(|| "fractalbits-bootstrap".to_string());
            let pid = std::process::id();
            writeln!(
                buf,
                "{} {}[{}]: {} {}",
                timestamp,
                process_name,
                pid,
                record.level(),
                record.args()
            )
        })
        .filter(None, log::LevelFilter::Info)
        .init();

    info!(
        "build info: {}",
        option_env!("BUILD_INFO").unwrap_or_default()
    );

    let opts = Opts::parse();
    let for_bench = opts.common.for_bench;
    let command = opts.command.as_ref().to_owned();
    common_setup()?;
    match opts.command {
        Command::ApiServer {
            bucket,
            nss_ip,
            rss_ip,
        } => api_server::bootstrap(&bucket, &nss_ip, &rss_ip, for_bench)?,
        Command::GuiServer {
            bucket,
            nss_ip,
            rss_ip,
        } => gui_server::bootstrap(&bucket, &nss_ip, &rss_ip)?,
        Command::BssServer { meta_stack_testing } => {
            bss_server::bootstrap(meta_stack_testing, for_bench)?
        }
        Command::NssServer {
            bucket,
            volume_id,
            meta_stack_testing,
            iam_role,
        } => nss_server::bootstrap(
            &bucket,
            &volume_id,
            meta_stack_testing,
            for_bench,
            &iam_role,
        )?,
        Command::RootServer {
            primary_instance_id,
            secondary_instance_id,
            volume_id,
        } => root_server::bootstrap(
            &primary_instance_id,
            &secondary_instance_id,
            &volume_id,
            for_bench,
        )?,
        Command::FormatNss {
            testing_mode,
            ebs_dev,
        } => nss_server::format_nss(ebs_dev, testing_mode)?,
        Command::BenchServer {
            api_server_service_endpoint,
            api_server_num,
            bench_client_num,
        } => bench_server::bootstrap(
            api_server_service_endpoint,
            api_server_num,
            bench_client_num,
        )?,
        Command::BenchClient => bench_client::bootstrap()?,
    }

    run_cmd! {
        touch $BOOTSTRAP_DONE_FILE;
        info "fractalbits-bootstrap $command is done";
    }?;
    Ok(())
}
