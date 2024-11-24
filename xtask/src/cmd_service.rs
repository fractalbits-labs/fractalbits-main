use super::build::BuildMode;
use super::ServiceAction;
use cmd_lib::*;

pub fn run_cmd_service(build_mode: BuildMode, action: ServiceAction, service: &str) -> CmdResult {
    match action {
        ServiceAction::Stop => stop_services(service),
        ServiceAction::Start => start_services(build_mode, service),
        ServiceAction::Restart => {
            stop_services(service)?;
            start_services(build_mode, service)
        }
    }
}

pub fn stop_services(service: &str) -> CmdResult {
    info!("Killing previous services (if any) ...");
    run_cmd!(sync)?;

    let services = if service == "all" {
        vec!["bss_server", "nss_server", "api_server"]
    } else {
        vec![service]
    };

    for service in services {
        for _ in 0..3 {
            if run_fun!(pidof $service).is_ok() {
                run_cmd! {
                    ignore killall $service &>/dev/null;
                    sleep 5;
                }?;
            }
        }

        // killall failed, try with `kill -9`
        if let Ok(pids) = run_fun!(pidof $service) {
            for pid in pids.split_whitespace() {
                run_cmd! {
                    info "Kill -9 for $service (pid=$pid) since using killall failed";
                    kill -9 $pid;
                    sleep 3;
                }?;
            }
        }

        // make sure the process is really being killed
        if let Ok(pid) = run_fun!(pidof $service) {
            cmd_die!("Failed to stop $service: service is still running (pid=$pid)");
        }
    }
    Ok(())
}

pub fn start_services(build_mode: BuildMode, service: &str) -> CmdResult {
    if service == "bss" || service == "all" {
        start_bss_service()?;
    }

    if service == "nss" || service == "all" {
        start_nss_service()?;
    }

    if service == "api_server" || service == "all" {
        start_api_server(build_mode)?;
    }
    Ok(())
}

pub fn start_bss_service() -> CmdResult {
    let service_log = "bss_server.log";
    let bss_wait_secs = 10;
    run_cmd! {
        info "Starting bss server with log $service_log ...";
        bash -c "nohup ./zig-out/bin/bss_server &> $service_log &";
        info "Waiting ${bss_wait_secs}s for server up";
        sleep $bss_wait_secs;
    }?;
    let bss_server_pid = run_fun!(pidof bss_server)?;
    check_pids("bss", &bss_server_pid)?;
    info!("bss server (pid={bss_server_pid}) started");
    Ok(())
}

pub fn start_nss_service() -> CmdResult {
    let service_log = "nss_server.log";
    let nss_wait_secs = 10;
    run_cmd! {
        info "Starting nss server with log $service_log ...";
        bash -c "nohup ./zig-out/bin/nss_server &> $service_log &";
        info "Waiting ${nss_wait_secs}s for server up";
        sleep $nss_wait_secs;
    }?;
    let nss_server_pid = run_fun!(pidof nss_server)?;
    check_pids("nss", &nss_server_pid)?;
    info!("nss server (pid={nss_server_pid}) started");
    Ok(())
}

pub fn start_api_server(mode: BuildMode) -> CmdResult {
    let service_log = "api_server.log";
    let api_server_wait_secs = 5;
    let (rust_log, rust_build) = match mode {
        BuildMode::Debug => ("debug", "debug"),
        BuildMode::Release => ("", "release"),
    };
    run_cmd! {
        info "Starting api server with log $service_log ...";
        bash -c "RUST_LOG=$rust_log nohup ./target/$rust_build/api_server &> $service_log &";
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
    check_pids("api_server", &api_server_pid)?;
    info!("api server (pid={api_server_pid}) started");
    Ok(())
}

fn check_pids(service: &str, pids: &str) -> CmdResult {
    if pids.split_whitespace().count() > 1 {
        error!("Multiple processes were found: {pids}, stopping services ...");
        stop_services(service)?;
        cmd_die!("Multiple processes were found: {pids}");
    }
    Ok(())
}
