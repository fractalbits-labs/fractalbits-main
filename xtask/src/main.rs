use cmd_lib::*;
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

    run_cmd! {
        info "building nss server ...";
        cd ..;
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
        bash -c "nohup ../zig-out/bin/nss_server &> nss_server.log &";
        info "waiting ${nss_wait_secs}s for server up";
        sleep $nss_wait_secs;
    }?;

    let api_server_wait_secs = 5;
    run_cmd! {
        info "starting api server ...";
        bash -c "nohup ../api_server/target/release/api_server &> api_server.log &";
        info "waiting ${api_server_wait_secs}s for server up";
        sleep $api_server_wait_secs;
    }?;

    run_cmd! {
        info "tailing api_server logs (./api_server.log): ";
        tail api_server.log;
    }?;

    let uri = "http://127.0.0.1:3000";
    run_cmd! {
        info "starting benchmark ...";
        cd ../api_server/benches/rewrk;
        cargo run --release -- -t 24 -c 500 -d 30s -h $uri -m post --pct;
    }?;

    Ok(())
}
