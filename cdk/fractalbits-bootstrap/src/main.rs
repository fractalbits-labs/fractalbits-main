mod api_server;
mod bench_client;
mod bench_server;
mod bss_server;
mod common;
mod nss_server;
mod root_server;

use clap::Parser;
use cmd_lib::*;
use common::*;

#[derive(Parser)]
#[clap(
    name = "fractalbits-bootstrap",
    about = "Bootstrap for cloud ec2 instances"
)]
struct Opts {
    #[clap(flatten)]
    common: CommonOpts,

    #[command(subcommand)]
    service: Service,
}

#[derive(Parser)]
#[command(rename_all = "snake_case")]
struct CommonOpts {
    #[clap(long, default_value = "false", long_help = "For benchmarking")]
    for_bench: bool,
}

#[allow(clippy::enum_variant_names)]
#[derive(Parser)]
#[command(rename_all = "snake_case")]
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

        #[clap(long, default_value = "false", long_help = "With bunch client running")]
        with_bench_client: bool,
    },

    #[clap(about = "Run on bss_server instance to bootstrap fractalbits service(s)")]
    BssServer {
        #[clap(long, long_help = "Number of NVME disks")]
        num_nvme_disks: usize,

        #[clap(long, default_value = "false", long_help = "For meta stack testing")]
        meta_stack_testing: bool,
    },

    #[clap(about = "Run on nss_server instance to bootstrap fractalbits service(s)")]
    NssServer {
        #[clap(long, long_help = "S3 bucket name for fractalbits service")]
        bucket: String,

        #[clap(long, long_help = "Multi-attached EBS volume ID")]
        volume_id: String,

        #[clap(long, long_help = "Number of NVME disks")]
        num_nvme_disks: usize,

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

    #[clap(about = "Run on bench_server instance to benchmark fractalbits service(s)")]
    BenchServer {
        #[clap(
            long,
            default_value = "local-service-endpoint",
            long_help = "Service endpoint for benchmark"
        )]
        service_endpoint: String,

        #[clap(
            long,
            value_delimiter = ',',
            long_help = "Comma separated list of client IPs"
        )]
        client_ips: Vec<String>,

        #[clap(
            long,
            value_delimiter = ',',
            long_help = "Comma separated list of api_server IPs"
        )]
        api_server_ips: Vec<String>,
    },

    #[clap(about = "Run on bench_client instance to benchmark fractalbits service(s)")]
    BenchClient {
        #[clap(long, long_help = "Api server pair ip address")]
        api_server_pair_ip: Option<String>,
    },
}

#[cmd_lib::main]
fn main() -> CmdResult {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info"))
        .format_target(false)
        .init();

    let opts = Opts::parse();
    let for_bench = opts.common.for_bench;
    match opts.service {
        Service::ApiServer {
            bucket,
            bss_ip,
            nss_ip,
            rss_ip,
            with_bench_client,
        } => api_server::bootstrap(
            &bucket,
            &bss_ip,
            &nss_ip,
            &rss_ip,
            with_bench_client,
            for_bench,
        )?,
        Service::BssServer {
            num_nvme_disks,
            meta_stack_testing,
        } => bss_server::bootstrap(num_nvme_disks, meta_stack_testing, for_bench)?,
        Service::NssServer {
            bucket,
            volume_id,
            num_nvme_disks,
            meta_stack_testing,
        } => nss_server::bootstrap(
            &bucket,
            &volume_id,
            num_nvme_disks,
            meta_stack_testing,
            for_bench,
        )?,
        Service::RootServer {
            primary_instance_id,
            secondary_instance_id,
            volume_id,
        } => root_server::bootstrap(
            &primary_instance_id,
            &secondary_instance_id,
            &volume_id,
            for_bench,
        )?,
        Service::BenchServer {
            service_endpoint,
            client_ips,
            api_server_ips,
        } => bench_server::bootstrap(service_endpoint, client_ips, api_server_ips)?,
        Service::BenchClient { api_server_pair_ip } => bench_client::bootstrap(api_server_pair_ip)?,
    }

    run_cmd! {
        touch $BOOTSTRAP_DONE_FILE;
        info "fractalbits-bootstrap is done";
    }?;
    Ok(())
}
