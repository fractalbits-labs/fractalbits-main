use cmd_lib::*;
use std::path::Path;
use structopt::StructOpt;

#[derive(StructOpt)]
#[structopt(name = "xtask", about = "Misc project related tasks")]
struct Opt {
    #[structopt(short, default_value = "bench")]
    op: String,
}

#[cmd_lib::main]
fn main() -> CmdResult {
    let Opt { op } = Opt::from_args();
    let _ = op; // we are only doing benchmark for now

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

    run_cmd! {
        info "killing previous servers (if any) ...";
        ignore killall nss_server;
        ignore killall api_server;
    }?;

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

    let uri = "http://127.0.0.1:3000";
    run_cmd! {
        info "starting benchmark ...";
        cd ./api_server/benches/rewrk;
        cargo run --release -- -t 24 -c 1 -d 30s -h $uri -m post --pct;
    }?;

    Ok(())
}
