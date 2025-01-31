mod build;
mod cmd_bench;
mod cmd_nightly;
mod cmd_precheckin;
mod cmd_service;
mod cmd_tool;

use build::BuildMode;
use clap::Parser;
use cmd_lib::*;
use strum::{AsRefStr, EnumString};

#[derive(Parser)]
#[clap(name = "xtask", about = "Misc project related tasks")]
enum Cmd {
    #[clap(about = "Run benchmark for api_server/nss_rpc/bss_rpc")]
    Bench {
        #[clap(
            short = 'w',
            long = "workload",
            long_help = "Run with pre-defined workload (read/write)",
            default_value = "write"
        )]
        workload: BenchWorkload,

        #[clap(
            short = 'f',
            long = "with_flame_graph",
            long_help = "Run with perf tool and generate flamegraph"
        )]
        with_flame_graph: bool,

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
        #[clap(long_help = "api_server/nss/bss/all", default_value = "all")]
        service: ServiceName,
    },

    #[clap(about = "Run tool related commands (gen_uuids only for now)")]
    #[command(subcommand)]
    Tool(ToolKind),
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
enum ServiceAction {
    Stop,
    Start,
    Restart,
}

#[derive(AsRefStr, EnumString, Copy, Clone)]
#[strum(serialize_all = "snake_case")]
enum ServiceName {
    ApiServer,
    Bss,
    Nss,
    Rss,
    All,
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
        } => {
            let mut service_name = ServiceName::All;
            cmd_bench::prepare_bench(with_flame_graph)?;
            cmd_bench::run_cmd_bench(service, workload, with_flame_graph, &mut service_name)
                .inspect_err(|_| {
                    cmd_service::run_cmd_service(
                        BuildMode::Release,
                        ServiceAction::Stop,
                        service_name,
                    )
                    .unwrap();
                })?;
        }
        Cmd::Service { action, service } => {
            if action != ServiceAction::Stop {
                // In case they have never been built before
                build::build_bss_nss_server(BuildMode::Debug)?;
                build::build_api_server(BuildMode::Debug)?;
            }
            cmd_service::run_cmd_service(BuildMode::Debug, action, service)?
        }
        Cmd::Tool(tool_kind) => cmd_tool::run_cmd_tool(tool_kind)?,
    }
    Ok(())
}
