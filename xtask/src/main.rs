use cmd_lib::*;
use structopt::StructOpt;

#[derive(StructOpt)]
#[structopt(name = "xtask", about = "Misc project related tasks")]
enum Cmd {
    #[structopt(about = "Run benchmark for sample_web_server/api_server/nss_rpc")]
    Bench {
        #[structopt(
            short = "f",
            long = "with_flame_graph",
            long_help = "Run with perf tool and generate flamegraph"
        )]
        with_flame_graph: bool,

        #[structopt(parse(from_str), long_help = "sample_web_server/api_server/nss_rpc")]
        server: String,
    },
    #[structopt(about = "Service stop/start/restart")]
    Service {
        #[structopt(parse(from_str), long_help = "stop/start/restart")]
        action: String,
    },
}

#[cmd_lib::main]
fn main() -> CmdResult {
    match Cmd::from_args() {
        Cmd::Bench {
            with_flame_graph,
            server,
        } => match server.as_str() {
            "sample_web_server" | "api_server" | "nss_rpc" => {
                prepare_bench()?;
                run_cmd_bench(with_flame_graph, &server)?;
            }
            _ => print_help_and_exit(),
        },
        Cmd::Service { action } => match action.as_str() {
            "stop" | "start" | "restart" => run_cmd_service(&action)?,
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

fn prepare_bench() -> CmdResult {
    if run_cmd!(bash -c "type addr2line" | grep -q .cargo).is_err() {
        // From https://github.com/iced-rs/iced/issues/2394
        run_cmd! {
            info "Try to install addr2line to make perf script work with rust binary ...";
            cargo install  addr2line --features="bin";
        }?;
    }
    Ok(())
}

fn run_cmd_bench(with_flame_graph: bool, server: &str) -> CmdResult {
    let uri;
    let bench_exe;
    let bench_opts;

    match server {
        "sample_web_server" => {
            run_cmd! {
                info "building sample_web_server ...";
                cd play/io_uring/iofthetiger;
                zig build --release=safe;
            }?;
            run_cmd! {
                info "building benchmark tool `rewrk` ...";
                cd ./api_server/benches/rewrk;
                cargo build --release;
            }?;
            run_cmd! {
                info "starting sample web server ...";
                bash -c "nohup play/io_uring/iofthetiger/zig-out/bin/sample_web_server &> sample_web_server.log &";
                info "sleep 5s for web server";
                sleep 5;
            }?;
            uri = "http://127.0.0.1:3000";
            bench_exe = "./target/release/rewrk";
            bench_opts = ["-t", "24", "-c", "500", "-m", "post"];
        }
        "api_server" => {
            run_cmd! {
                info "building nss server ...";
                zig build --release=safe;
            }?;
            run_cmd! {
                info "building api_server ...";
                cd api_server;
                cargo build --release;
            }?;
            run_cmd_service("restart")?;
            uri = "http://127.0.0.1:3000";
            bench_exe = "./target/release/rewrk";
            bench_opts = ["-t", "1", "-c", "8", "-m", "post"];
        }
        "nss_rpc" => {
            run_cmd! {
                info "building nss server ...";
                zig build --release=safe;
            }?;
            run_cmd! {
                info "building benchmark tool `rewrk_rpc` ...";
                cd ./api_server/benches/rewrk_rpc;
                cargo build --release;
            }?;
            start_nss_service()?;
            uri = "127.0.0.1:9224";
            bench_exe = "./target/release/rewrk_rpc";
            bench_opts = ["-t", "24", "-c", "500", "", ""];
        }
        _ => unreachable!(),
    }

    let perf_handle = if with_flame_graph {
        run_cmd! {
            info "start perf in the background ...";
            sudo bash -c "echo 0 > /proc/sys/kernel/kptr_restrict";
            sudo bash -c "echo -1 > /proc/sys/kernel/perf_event_paranoid";
        }?;
        // Some(spawn!(perf record -F 99 --call-graph dwarf -p $api_server_pid -g -- sleep 30)?)
        Some(spawn!(perf record -F 99 --call-graph dwarf -a -g -- sleep 30)?)
    } else {
        None
    };

    run_cmd! {
        info "starting benchmark ...";
        $bench_exe $[bench_opts] -d 30s -h $uri --pct;
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

    // stop service after benchmark to save cpu power
    run_cmd_service("stop")?;

    Ok(())
}

fn run_cmd_service(action: &str) -> CmdResult {
    match action {
        "stop" => stop_service(),
        "start" => start_service(),
        "restart" => {
            stop_service()?;
            start_service()
        }
        _ => unreachable!(),
    }
}

fn stop_service() -> CmdResult {
    run_cmd! {
        info "killing previous servers (if any) ...";
        ignore killall nss_server;
        ignore killall api_server;
        ignore killall sample_web_server;
    }
}

fn start_service() -> CmdResult {
    start_nss_service()?;

    let api_server_wait_secs = 5;
    run_cmd! {
        info "starting api server ...";
        bash -c "nohup ./target/release/api_server &> api_server.log &";
        info "waiting ${api_server_wait_secs}s for server up";
        sleep $api_server_wait_secs;
    }?;
    let api_server_pid = match run_fun!(pidof api_server) {
        Ok(pid) => pid,
        Err(e) => {
            run_cmd! {
                error "Could not find api_server service";
                info "Tailing api_server.log:";
                tail api_server.log;
            }?;
            return Err(e);
        }
    };
    info!("api server(pid={api_server_pid}) started");
    Ok(())
}

fn start_nss_service() -> CmdResult {
    let nss_wait_secs = 10;
    run_cmd! {
        info "starting nss server ...";
        bash -c "nohup ./zig-out/bin/nss_server &> nss_server.log &";
        info "waiting ${nss_wait_secs}s for server up";
        sleep $nss_wait_secs;
    }?;
    let nss_server_pid = run_fun!(pidof nss_server)?;
    info!("nss server(pid={nss_server_pid}) started");
    Ok(())
}
