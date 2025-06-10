mod build;
mod cmd_bench;
mod cmd_deploy;
mod cmd_nightly;
mod cmd_precheckin;
pub mod cmd_service;
mod cmd_tool;

use build::BuildMode;
use clap::{ArgAction, Parser};
use cmd_lib::*;
use strum::{AsRefStr, EnumString};

pub const TEST_BUCKET_ROOT_BLOB_NAME: &str = "947ef2be-44b2-4ac2-969b-2574eb85662b";
pub const TS_FMT: &str = "%b %d %H:%M:%.S";

#[derive(Parser)]
#[command(rename_all = "snake_case")]
#[clap(name = "xtask", about = "Misc project related tasks")]
enum Cmd {
    #[clap(about = "Run benchmark for api_server/nss_rpc/bss_rpc")]
    Bench {
        #[clap(
            long,
            long_help = "Run with pre-defined workload (read/write)",
            default_value = "write"
        )]
        workload: BenchWorkload,

        #[clap(long, long_help = "Run with perf tool and generate flamegraph")]
        with_flame_graph: bool,

        #[clap(long, long_help = "Nss data on local disks (without s3)")]
        nss_data_on_local: bool,

        #[clap(long_help = "api_server/nss_rpc/bss_rpc")]
        service: BenchService,
    },

    #[clap(about = "Run nightly tests")]
    Nightly,

    #[clap(about = "Run precheckin tests")]
    Precheckin,

    #[clap(about = "Service stop/start/restart")]
    Service {
        #[clap(long_help = "stop/start/restart")]
        action: ServiceAction,
        #[clap(
            long_help = "all/api_server/nss/bss/ddb_local/minio",
            default_value = "all"
        )]
        service: ServiceName,
    },

    #[clap(about = "Run tool related commands (gen_uuids only for now)")]
    #[command(subcommand)]
    Tool(ToolKind),

    #[clap(about = "Deploy binaries to s3 builds bucket")]
    Deploy {
        #[clap(long, action=ArgAction::Set, default_value = "true", num_args = 0..=1)]
        use_s3_backend: bool,
    },
}

#[derive(Clone, AsRefStr, EnumString)]
#[strum(serialize_all = "snake_case")]
enum BenchWorkload {
    Read,
    Write,
}

#[derive(Clone, EnumString)]
#[strum(serialize_all = "snake_case")]
enum BenchService {
    ApiServer,
    NssRpc,
    BssRpc,
}

#[derive(Clone, EnumString, PartialEq)]
#[strum(serialize_all = "snake_case")]
pub enum ServiceAction {
    Stop,
    Start,
    Restart,
}

#[derive(AsRefStr, EnumString, Copy, Clone)]
#[strum(serialize_all = "snake_case")]
pub enum ServiceName {
    ApiServer,
    Bss,
    Nss,
    Rss,
    All,
    Minio,
    DdbLocal,
}

#[derive(Parser, Clone)]
#[clap(rename_all = "snake_case")]
enum ToolKind {
    GenUuids {
        #[clap(short = 'n', long_help = "Number of uuids", default_value = "1000000")]
        num: usize,

        #[clap(short = 'f', long_help = "File output", default_value = "uuids.data")]
        file: String,
    },
}

#[cmd_lib::main]
fn main() -> CmdResult {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info"))
        .format_target(false)
        .init();
    rlimit::increase_nofile_limit(1000000).unwrap();

    match Cmd::parse() {
        Cmd::Precheckin => cmd_precheckin::run_cmd_precheckin()?,
        Cmd::Nightly => cmd_nightly::run_cmd_nightly()?,
        Cmd::Bench {
            service,
            workload,
            with_flame_graph,
            nss_data_on_local,
        } => {
            let mut service_name = ServiceName::All;
            cmd_bench::prepare_bench(with_flame_graph)?;
            cmd_bench::run_cmd_bench(
                service,
                workload,
                with_flame_graph,
                nss_data_on_local,
                &mut service_name,
            )
            .inspect_err(|_| {
                cmd_service::run_cmd_service(BuildMode::Release, ServiceAction::Stop, service_name)
                    .unwrap();
            })?;
        }
        Cmd::Service { action, service } => {
            if action != ServiceAction::Stop {
                // In case they have never been built before
                build::build_bss_nss_server(BuildMode::Debug)?;
                build::build_rss_api_server(BuildMode::Debug)?;
            }
            cmd_service::run_cmd_service(BuildMode::Debug, action, service)?
        }
        Cmd::Tool(tool_kind) => cmd_tool::run_cmd_tool(tool_kind)?,
        Cmd::Deploy { use_s3_backend } => cmd_deploy::run_cmd_deploy(use_s3_backend)?,
    }
    Ok(())
}
