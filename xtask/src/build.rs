use cmd_lib::*;

const ZIG_BUILD_OPTS: &'static str = "--release=safe"; // or "" for debugging

pub fn build_rewrk() -> CmdResult {
    run_cmd! {
        info "Building benchmark tool `rewrk` ...";
        cd ./api_server/benches/rewrk;
        cargo build --release;
    }
}

pub fn build_rewrk_rpc() -> CmdResult {
    run_cmd! {
        info "Building benchmark tool `rewrk_rpc` ...";
        cd ./api_server/benches/rewrk_rpc;
        cargo build --release;
    }
}

pub fn build_bss_nss_server() -> CmdResult {
    run_cmd! {
        info "Building bss and nss server ...";
        zig build $ZIG_BUILD_OPTS;
    }
}

pub fn build_api_server() -> CmdResult {
    run_cmd! {
        info "Building api_server ...";
        cd api_server;
        cargo build --release;
    }
}
