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
    #[structopt(about = "Run precheckin tests")]
    Precheckin,
}

#[cmd_lib::main]
fn main() -> CmdResult {
    match Cmd::from_args() {
        Cmd::Precheckin => run_precheckin()?,
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
            cargo install addr2line --features="bin";
        }?;
    }
    Ok(())
}

fn run_precheckin() -> CmdResult {
    run_cmd! {
        info "Building ...";
        zig build;
    }?;

    run_cmd! {
        info "ulimit -n 1000000";
    }?;

    run_cmd! {
        info "Running zig unit tests ...";
        zig build test --summary all;
    }?;

    run_cmd! {
        info "Running cargo unit tests ...";
        cargo test;
    }?;

    let rand_log = "test_art_random.log";
    run_cmd! {
        info "Running art tests (random) with log $rand_log ...";
        ./zig-out/bin/test_art --tests random --size 400000 --ops 400000 -d 20 &> $rand_log;
    }
    .map_err(|e| {
        run_cmd!(tail $rand_log).unwrap();
        e
    })?;

    let fat_log = "test_art_fat.log";
    run_cmd! {
        info "Running art tests (fat) with log $fat_log ...";
        ./zig-out/bin/test_art --tests fat --ops 1000000 &> $fat_log;
    }
    .map_err(|e| {
        run_cmd!(tail $fat_log).unwrap();
        e
    })?;

    let async_art_log = "test_async_art.log";
    run_cmd! {
        info "Running async art tests with log $async_art_log ...";
        ./zig-out/bin/test_async_art --fresh -p 20 &> $async_art_log;
        ./zig-out/bin/test_async_art -p 20 &>> $async_art_log;
        ./zig-out/bin/test_async_art -p 20 &>> $async_art_log;
    }
    .map_err(|e| {
        run_cmd!(tail $async_art_log).unwrap();
        e
    })?;

    info!("Precheckin is OK");
    Ok(())
}

fn run_cmd_bench(with_flame_graph: bool, server: &str) -> CmdResult {
    let uri;
    let bench_exe;
    let bench_opts;

    match server {
        "sample_web_server" => {
            build_sample_web_server()?;
            build_rewrk()?;

            start_sample_web_server()?;
            uri = "http://127.0.0.1:3000";
            bench_exe = "./target/release/rewrk";
            bench_opts = ["-t", "24", "-c", "500", "-m", "put"];
        }
        "api_server" => {
            build_nss_server()?;
            build_api_server()?;
            build_rewrk()?;

            run_cmd_service("restart")?;
            uri = "http://mybucket.localhost:3000";
            bench_exe = "./target/release/rewrk";
            bench_opts = ["-t", "24", "-c", "500", "-m", "put"];
        }
        "nss_rpc" => {
            build_nss_server()?;
            build_rewrk_rpc()?;

            start_nss_service()?;
            uri = "127.0.0.1:9224";
            bench_exe = "./target/release/rewrk_rpc";
            bench_opts = ["-t", "24", "-c", "500", "-D", "1"];
        }
        _ => unreachable!(),
    }

    let duration_secs = 30;
    let perf_handle = if with_flame_graph {
        run_cmd! {
            info "Start perf in the background ...";
            sudo bash -c "echo 0 > /proc/sys/kernel/kptr_restrict";
            sudo bash -c "echo -1 > /proc/sys/kernel/perf_event_paranoid";
        }?;
        // Some(spawn!(perf record -F 99 --call-graph dwarf -p $api_server_pid -g -- sleep 30)?)
        Some(spawn!(perf record -F 99 --call-graph dwarf -a -g -- sleep $duration_secs)?)
    } else {
        None
    };

    run_cmd! {
        info "Starting benchmark ...";
        $bench_exe $[bench_opts] -d ${duration_secs}s -h $uri --pct;
    }?;

    if let Some(mut handle) = perf_handle {
        handle.wait()?;
        let flamegraph_path = run_fun!(brew --prefix flamegraph)?;
        run_cmd! {
            info "Post-processing perf data ...";
            perf script > out.perf;
            ${flamegraph_path}/bin/stackcollapse-perf.pl out.perf > out.folded;
            ${flamegraph_path}/bin/flamegraph.pl out.folded > out_perf.svg;
            info "Flamegraph \"out_perf.svg\" is generated";
        }?;
    }

    // stop service after benchmark to save cpu power
    run_cmd_service("stop")?;

    Ok(())
}

fn build_sample_web_server() -> CmdResult {
    run_cmd! {
        info "Building sample_web_server ...";
        cd play/io_uring/iofthetiger;
        zig build --release=safe;
    }
}

fn build_rewrk() -> CmdResult {
    run_cmd! {
        info "Building benchmark tool `rewrk` ...";
        cd ./api_server/benches/rewrk;
        cargo build --release;
    }
}

fn build_rewrk_rpc() -> CmdResult {
    run_cmd! {
        info "Building benchmark tool `rewrk_rpc` ...";
        cd ./api_server/benches/rewrk_rpc;
        cargo build --release;
    }
}

fn build_nss_server() -> CmdResult {
    run_cmd! {
        info "Building nss server ...";
        zig build --release=safe;
    }
}

fn build_api_server() -> CmdResult {
    run_cmd! {
        info "Building api_server ...";
        cd api_server;
        cargo build --release;
    }
}

fn run_cmd_service(action: &str) -> CmdResult {
    match action {
        "stop" => stop_services(),
        "start" => start_services(),
        "restart" => {
            stop_services()?;
            start_services()
        }
        _ => unreachable!(),
    }
}

fn stop_services() -> CmdResult {
    info!("Killing previous services (if any) ...");
    for service in ["nss_server", "api_server", "sample_web_server"] {
        run_cmd!(ignore killall $service &>/dev/null)?;
        if let Ok(pids) = run_fun!(pidof $service) {
            for pid in pids.split_whitespace() {
                run_cmd! {
                    info "kill -9 for $service (pid=$pid) since using killall failed";
                    kill -9 $pid;
                    sleep 3;
                }?;
            }
        }
        // make sure the process is really being killed
        if let Ok(pid) = run_fun!(pidof $service) {
            cmd_die!("failed to stop $service: service is still running (pid=$pid)");
        }
    }
    Ok(())
}

fn start_sample_web_server() -> CmdResult {
    let service_log = "sample_web_server.log";
    run_cmd! {
        info "Starting sample web server with log $service_log ...";
        bash -c "nohup play/io_uring/iofthetiger/zig-out/bin/sample_web_server &> $service_log &";
        info "Sleep 5s for web server";
        sleep 5;
    }
}

fn start_services() -> CmdResult {
    start_nss_service()?;
    start_api_service()?;
    Ok(())
}

fn start_nss_service() -> CmdResult {
    let service_log = "nss_server.log";
    let nss_wait_secs = 10;
    run_cmd! {
        info "Starting nss server with log $service_log ...";
        bash -c "nohup ./zig-out/bin/nss_server &> $service_log &";
        info "Waiting ${nss_wait_secs}s for server up";
        sleep $nss_wait_secs;
    }?;
    let nss_server_pid = run_fun!(pidof nss_server)?;
    info!("nss server (pid={nss_server_pid}) started");
    Ok(())
}

fn start_api_service() -> CmdResult {
    let service_log = "api_server.log";
    let api_server_wait_secs = 5;
    run_cmd! {
        info "Starting api server with log $service_log ...";
        bash -c "nohup ./target/release/api_server &> $service_log &";
        info "Waiting ${api_server_wait_secs}s for server up";
        sleep $api_server_wait_secs;
    }?;
    let api_server_pid = match run_fun!(pidof api_server) {
        Ok(pid) => pid,
        Err(e) => {
            run_cmd! {
                error "Could not find api_server service";
                info "Tailing $service_log:";
                tail $service_log;
            }?;
            return Err(e);
        }
    };
    info!("api server (pid={api_server_pid}) started");
    Ok(())
}
