mod build;
mod cmd_bench;
mod cmd_precheckin;
mod cmd_service;
mod cmd_tool;

use build::BuildMode;
use cmd_lib::*;
use structopt::StructOpt;
use strum::EnumString;

#[derive(StructOpt)]
#[structopt(name = "xtask", about = "Misc project related tasks")]
enum Cmd {
    #[structopt(about = "Run benchmark for api_server/nss_rpc/bss_rpc")]
    Bench {
        #[structopt(
            short = "w",
            long = "workload",
            long_help = "Run with pre-defined workload (read/write)",
            default_value = "write"
        )]
        workload: String,

        #[structopt(
            short = "f",
            long = "with_flame_graph",
            long_help = "Run with perf tool and generate flamegraph"
        )]
        with_flame_graph: bool,

        #[structopt(long_help = "api_server/nss_rpc/bss_rpc")]
        server: String,
    },

    #[structopt(about = "Run precheckin tests")]
    Precheckin,

    #[structopt(about = "Service stop/start/restart")]
    Service {
        #[structopt(long_help = "stop/start/restart")]
        action: ServiceAction,
        #[structopt(long_help = "api_server/nss/bss/all", default_value = "all")]
        service: String,
    },

    #[structopt(about = "Run tool related commands (gen_uuids only for now)")]
    Tool(ToolKind),
}

#[derive(EnumString)]
#[strum(serialize_all = "snake_case")]
enum ServiceAction {
    Stop,
    Start,
    Restart,
}

#[derive(StructOpt)]
#[structopt(rename_all = "snake_case")]
enum ToolKind {
    GenUuids {
        #[structopt(short = "n", long_help = "Number of uuids", default_value = "1000000")]
        num: usize,

        #[structopt(short = "f", long_help = "File output", default_value = "uuids.data")]
        file: String,
    },
}

#[cmd_lib::main]
fn main() -> CmdResult {
    match Cmd::from_args() {
        Cmd::Precheckin => cmd_precheckin::run_cmd_precheckin()?,
        Cmd::Bench {
            workload,
            with_flame_graph,
            server,
        } => match server.as_str() {
            "api_server" | "nss_rpc" | "bss_rpc" => {
                cmd_bench::prepare_bench()?;
                cmd_bench::run_cmd_bench(workload, with_flame_graph, &server).inspect_err(
                    |_| {
                        cmd_service::run_cmd_service(
                            BuildMode::Release,
                            ServiceAction::Stop,
                            "all",
                        )
                        .unwrap();
                    },
                )?;
            }
            _ => print_help_and_exit(),
        },
        Cmd::Service { action, service } => {
            // In case they have never been built before
            build::build_bss_nss_server(BuildMode::Debug)?;
            build::build_api_server(BuildMode::Debug)?;
            cmd_service::run_cmd_service(BuildMode::Debug, action, &service)?
        }
        Cmd::Tool(tool_kind) => cmd_tool::run_cmd_tool(tool_kind)?,
    }
    Ok(())
}

fn print_help_and_exit() {
    Cmd::clap().print_help().unwrap();
    println!();
    std::process::exit(1);
}
