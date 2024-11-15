mod build;
mod cmd_bench;
mod cmd_precheckin;
mod cmd_service;

use cmd_lib::*;
use structopt::StructOpt;

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
    #[structopt(about = "Service stop/start/restart")]
    Service {
        #[structopt(long_help = "stop/start/restart")]
        action: String,
    },
    #[structopt(about = "Run precheckin tests")]
    Precheckin,
}

#[cmd_lib::main]
fn main() -> CmdResult {
    match Cmd::from_args() {
        Cmd::Precheckin => cmd_precheckin::run_precheckin()?,
        Cmd::Bench {
            workload,
            with_flame_graph,
            server,
        } => match server.as_str() {
            "api_server" | "nss_rpc" | "bss_rpc" => {
                cmd_bench::prepare_bench()?;
                cmd_bench::run_cmd_bench(workload, with_flame_graph, &server)?;
            }
            _ => print_help_and_exit(),
        },
        Cmd::Service { action } => match action.as_str() {
            "stop" | "start" | "restart" => cmd_service::run_cmd_service(&action)?,
            _ => print_help_and_exit(),
        },
    }
    Ok(())
}

fn print_help_and_exit() {
    Cmd::clap().print_help().unwrap();
    println!();
    std::process::exit(1);
}
