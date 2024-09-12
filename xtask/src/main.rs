use cmd_lib::*;
use std::path::Path;
use structopt::StructOpt;

#[derive(StructOpt)]
#[structopt(name = "xtask", about = "Misc project related tasks")]
enum Cmd {
    #[structopt(about = "Run benchmark")]
    Bench {
        #[structopt(
            short = "f",
            long,
            long_help = "Run with perf tool and generate flamegraph"
        )]
        with_flame_graph: bool,
    },
    #[structopt(about = "Service stop/start/restart")]
    Service { action: String },
}

#[cmd_lib::main]
fn main() -> CmdResult {
    match Cmd::from_args() {
        Cmd::Bench { with_flame_graph } => run_cmd_bench(with_flame_graph)?,
        Cmd::Service { action } => run_cmd_service(&action)?,
    }
    Ok(())
}

fn run_cmd_bench(with_flame_graph: bool) -> CmdResult {
    if !Path::new("./api_server").exists() {
        error!("Could not find `api_server` in current directory.");
        error!("You need to run the command (cargo xtask ...) in the root source direcotry.");
        std::process::exit(1);
    }

    run_cmd! {
        info "building nss server ...";
        zig build --release=safe;

        info "building api_server ...";
        cd api_server;
        cargo build --release;
    }?;

    run_cmd_service("stop")?;

    let nss_wait_secs = 10;
    run_cmd! {
        info "starting nss server ...";
        bash -c "nohup ./zig-out/bin/nss_server &> nss_server.log &";
        info "waiting ${nss_wait_secs}s for server up";
        sleep $nss_wait_secs;
    }?;
    let nss_pid = run_fun!(pidof nss_server)?;
    info!("nss server(pid={nss_pid}) started");

    let api_server_wait_secs = 5;
    run_cmd! {
        info "starting api server ...";
        bash -c "nohup ./target/release/api_server &> api_server.log &";
        info "waiting ${api_server_wait_secs}s for server up";
        sleep $api_server_wait_secs;
    }?;
    let api_server_pid = run_fun!(pidof api_server)?;
    info!("api server(pid={api_server_pid}) started");

    let perf_handle = if with_flame_graph {
        run_cmd! {
            info "start perf in the background ...";
            sudo bash -c "echo 0 > /proc/sys/kernel/kptr_restrict";
            sudo bash -c "echo -1 > /proc/sys/kernel/perf_event_paranoid";
        }?;
        Some(spawn!(perf record -a -g -F 99 sleep 35)?)
    } else {
        None
    };

    let uri = "http://127.0.0.1:3000";
    run_cmd! {
        info "starting benchmark ...";
        cd ./api_server/benches/rewrk;
        cargo run --release -- -t 24 -c 500 -d 30s -h $uri -m post --pct;
    }?;

    if let Some(mut handle) = perf_handle {
        handle.wait()?;
        let flamegraph_path = "/home/linuxbrew/.linuxbrew/Cellar/flamegraph/1.0_1/bin/";
        run_cmd! {
            info "post-processing perf data ...";
            perf script > out.perf;
            ${flamegraph_path}/stackcollapse-perf.pl out.perf > out.folded;
            ${flamegraph_path}/flamegraph.pl out.folded > out_perf.svg;
            info "flamegraph \"out_perf.svg\" is generated";
        }?;
    }

    Ok(())
}

fn run_cmd_service(_action: &str) -> CmdResult {
    run_cmd! {
        info "killing previous servers (if any) ...";
        ignore killall nss_server;
        ignore killall api_server;
    }
}
