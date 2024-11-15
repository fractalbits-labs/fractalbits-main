use super::build::*;
use cmd_lib::*;

pub fn run_cmd_service(action: &str) -> CmdResult {
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

pub fn stop_services() -> CmdResult {
    info!("Killing previous services (if any) ...");
    run_cmd!(sync)?;

    for service in ["bss_server", "nss_server", "api_server"] {
        for _ in 0..5 {
            if run_fun!(pidof $service).is_ok() {
                run_cmd! {
                    ignore killall $service &>/dev/null;
                    sleep 3;
                }?;
            }
        }

        // killall failed, try with `kill -9`
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

pub fn start_services() -> CmdResult {
    build_bss_nss_server()?;
    build_api_server()?;
    start_bss_service()?;
    start_nss_service()?;
    start_api_service()?;
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
    check_pids(&bss_server_pid)?;
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
    check_pids(&nss_server_pid)?;
    info!("nss server (pid={nss_server_pid}) started");
    Ok(())
}

pub fn start_api_service() -> CmdResult {
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
    check_pids(&api_server_pid)?;
    info!("api server (pid={api_server_pid}) started");
    Ok(())
}

fn check_pids(pids: &str) -> CmdResult {
    if pids.split_whitespace().count() > 1 {
        stop_services()?;
        cmd_die!("multiple processes were found: {pids}");
    }
    Ok(())
}
