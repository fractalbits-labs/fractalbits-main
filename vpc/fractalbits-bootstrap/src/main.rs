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
        #[clap(
            long,
            long_help = "S3 bucket name for fractalbits service (for hybrid mode)"
        )]
        bucket: Option<String>,

        #[clap(long, long_help = "Remote AZ for S3 Express multi-AZ setup")]
        remote_az: Option<String>,

        #[clap(long, long_help = "primary nss_server endpoint")]
        nss_endpoint: String,

        #[clap(long, long_help = "root_server endpoint")]
        rss_endpoint: String,
    },

    #[clap(about = "Run on gui_server instance to bootstrap fractalbits service(s)")]
    GuiServer {
        #[clap(
            long,
            long_help = "S3 bucket name for fractalbits service (for hybrid mode)"
        )]
        bucket: Option<String>,

        #[clap(long, long_help = "Remote AZ for S3 Express multi-AZ setup")]
        remote_az: Option<String>,

        #[clap(long, long_help = "primary nss_server endpoint")]
        nss_endpoint: String,

        #[clap(long, long_help = "root_server endpoint")]
        rss_endpoint: String,
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

        #[clap(long, long_help = "Mirrord endpoint for NSS communication")]
        mirrord_endpoint: Option<String>,

        #[clap(long, long_help = "Root server (RSS) endpoint")]
        rss_endpoint: String,
    },

    #[clap(about = "Run on root_server instance to bootstrap fractalbits service(s)")]
    RootServer {
        #[clap(long, long_help = "primary nss_server endpoint")]
        nss_endpoint: String,

        #[clap(long, long_help = "Primary nss_server ec2 instance ID")]
        nss_a_id: String,

        #[clap(long, long_help = "Secondary nss_server ec2 instance ID")]
        nss_b_id: Option<String>,

        #[clap(long, long_help = "EBS volume ID for nss-A")]
        volume_a_id: String,

        #[clap(long, long_help = "EBS volume ID for nss-B")]
        volume_b_id: Option<String>,

        #[clap(long, long_help = "Follower instance ID for root server")]
        follower_id: Option<String>,

        #[clap(long, long_help = "Remote AZ for S3 Express multi-AZ setup")]
        remote_az: Option<String>,
    },

    #[clap(
        about = "Run on nss_server instance to format itself when receiving ssm command from root_server"
    )]
    FormatNss {
        #[clap(long, long_help = "EBS device")]
        ebs_dev: String,
    },

    #[clap(about = "Run on bench_server instance to benchmark fractalbits service(s)")]
    BenchServer {
        #[clap(long, long_help = "Service endpoint for benchmark")]
        api_server_endpoint: String,

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

    let main_build_info = option_env!("MAIN_BUILD_INFO").unwrap_or("unknown");
    let build_timestamp = option_env!("BUILD_TIMESTAMP").unwrap_or("unknown");
    let build_info = format!("{}, build time: {}", main_build_info, build_timestamp);
    eprintln!("build info: {}", build_info);

    let opts = Opts::parse();
    let for_bench = opts.common.for_bench;
    let command = opts.command.as_ref().to_owned();
    common_setup()?;
    match opts.command {
        Command::ApiServer {
            bucket,
            remote_az,
            nss_endpoint,
            rss_endpoint,
        } => api_server::bootstrap(
            bucket.as_deref(),
            &nss_endpoint,
            &rss_endpoint,
            remote_az.as_deref(),
            for_bench,
        )?,
        Command::GuiServer {
            bucket,
            remote_az,
            nss_endpoint,
            rss_endpoint,
        } => gui_server::bootstrap(
            bucket.as_deref(),
            &nss_endpoint,
            &rss_endpoint,
            remote_az.as_deref(),
        )?,
        Command::BssServer { meta_stack_testing } => {
            bss_server::bootstrap(meta_stack_testing, for_bench)?
        }
        Command::NssServer {
            bucket,
            volume_id,
            meta_stack_testing,
            iam_role,
            mirrord_endpoint,
            rss_endpoint,
        } => nss_server::bootstrap(
            &bucket,
            &volume_id,
            meta_stack_testing,
            for_bench,
            &iam_role,
            mirrord_endpoint.as_deref(),
            &rss_endpoint,
        )?,
        Command::RootServer {
            nss_endpoint,
            nss_a_id,
            nss_b_id,
            volume_a_id,
            volume_b_id,
            follower_id,
            remote_az,
        } => root_server::bootstrap(
            &nss_endpoint,
            &nss_a_id,
            nss_b_id.as_deref(),
            &volume_a_id,
            volume_b_id.as_deref(),
            follower_id.as_deref(),
            remote_az.as_deref(),
            for_bench,
        )?,
        Command::FormatNss { ebs_dev } => nss_server::format_nss(ebs_dev)?,
        Command::BenchServer {
            api_server_endpoint,
            bench_client_num,
        } => bench_server::bootstrap(api_server_endpoint, bench_client_num)?,
        Command::BenchClient => bench_client::bootstrap()?,
    }

    run_cmd! {
        touch $BOOTSTRAP_DONE_FILE;
        info "fractalbits-bootstrap $command is done";
    }?;
    Ok(())
}
