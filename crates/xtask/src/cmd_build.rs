use crate::*;
use std::path::Path;
use std::sync::OnceLock;
use strum::{AsRefStr, EnumString};

pub static BUILD_INFO: OnceLock<String> = OnceLock::new();
pub const ZIG_REPO_PATH: &str = "core";
pub const UI_REPO_PATH: &str = "ui";

#[derive(Copy, Clone, AsRefStr, EnumString)]
#[strum(serialize_all = "snake_case")]
pub enum BuildMode {
    Debug,
    Release,
}

pub fn build_mode(release: bool) -> BuildMode {
    match release {
        true => BuildMode::Release,
        false => BuildMode::Debug,
    }
}

pub fn build_info() -> String {
    let git_branch = run_fun!(git branch --show-current).unwrap();
    let git_rev = run_fun!(git rev-parse --short HEAD).unwrap();
    let build_timestamp = run_fun!(date "+%s").unwrap();
    let dirty = if run_cmd!(git diff-index --quiet HEAD).is_ok() {
        ""
    } else {
        "+"
    };
    format!("{git_branch}:{git_rev}{dirty}, build time: {build_timestamp}")
}

pub fn build_bench_rpc() -> CmdResult {
    let build_info = BUILD_INFO.get().unwrap();
    run_cmd! {
        info "Building benchmark tool `rewrk_rpc` ...";
        cd crates/bench_rpc;
        BUILD_INFO=$build_info cargo build --release;
    }
}

pub fn build_zig_servers(mode: BuildMode) -> CmdResult {
    if !Path::new(ZIG_REPO_PATH).exists() {
        return Ok(());
    }

    let build_info = BUILD_INFO.get().unwrap();
    let opts = match mode {
        BuildMode::Debug => "",
        BuildMode::Release => "--release=safe",
    };
    run_cmd! {
        info "Building zig-based servers ...";
        cd $ZIG_REPO_PATH;
        zig build -p ../$ZIG_DEBUG_OUT -Dbuild_info=$build_info $opts 2>&1;
        info "Building bss and nss server done";
    }
}

pub fn build_rust_servers(mode: BuildMode) -> CmdResult {
    let build_info = BUILD_INFO.get().unwrap();
    match mode {
        BuildMode::Debug => {
            run_cmd! {
                info "Building rust-based servers in debug mode ...";
                BUILD_INFO=$build_info cargo build --workspace
                    --exclude fractalbits-bootstrap
                    --exclude rewrk*;
            }
        }
        BuildMode::Release => {
            run_cmd! {
                info "Building rust-based servers in release mode ...";
                BUILD_INFO=$build_info cargo build --release;
            }
        }
    }
}

pub fn build_ui(region: &str) -> CmdResult {
    if !Path::new(UI_REPO_PATH).exists() {
        return Ok(());
    }

    run_cmd! {
        info "Building ui ...";
        cd $UI_REPO_PATH;
        npm install;
        VITE_AWS_REGION=$region npm run build;
    }
}

pub fn build_all(release: bool) -> CmdResult {
    let build_mode = build_mode(release);
    build_rust_servers(build_mode)?;
    build_zig_servers(build_mode)?;
    if release {
        build_bench_rpc()?;
    }
    build_ui(crate::UI_DEFAULT_REGION)?;
    Ok(())
}

pub fn run_zig_unit_tests() -> CmdResult {
    let working_dir = run_fun!(pwd)?;
    for bss_service in ServiceName::all_bss_services() {
        cmd_service::start_service(bss_service)?;
    }

    run_cmd! {
        info "Formatting nss_server";
        $working_dir/$ZIG_DEBUG_OUT/bin/nss_server format;
    }?;

    run_cmd! {
        info "Running zig unit tests";
        cd $ZIG_REPO_PATH;
        zig build -p ../$ZIG_DEBUG_OUT test --summary all 2>&1;
    }?;
    for bss_service in ServiceName::all_bss_services() {
        cmd_service::stop_service(bss_service)?;
    }

    info!("Zig unit tests completed successfully");
    Ok(())
}
