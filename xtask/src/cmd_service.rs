use super::build::BuildMode;
use super::{ServiceAction, ServiceName};
use cmd_lib::*;

const ROOT_BLOB_ID: &str = "947ef2be-44b2-4ac2-969b-2574eb85662b";

pub fn run_cmd_service(
    build_mode: BuildMode,
    action: ServiceAction,
    service: ServiceName,
) -> CmdResult {
    match action {
        ServiceAction::Stop => stop_services(service),
        ServiceAction::Start => start_services(build_mode, service),
        ServiceAction::Restart => {
            stop_services(service)?;
            start_services(build_mode, service)
        }
    }
}

pub fn stop_services(service: ServiceName) -> CmdResult {
    info!("Killing previous services (if any) ...");
    run_cmd!(sync)?;

    let services: Vec<String> = match service {
        ServiceName::All => vec![
            ServiceName::ApiServer.as_ref().to_owned(),
            ServiceName::Nss.as_ref().to_owned(),
            ServiceName::Bss.as_ref().to_owned(),
        ],
        single_service => vec![single_service.as_ref().to_owned()],
    };

    for service in services {
        if run_cmd!(systemctl --user is-active --quiet $service.service).is_err() {
            continue;
        }

        run_cmd!(systemctl --user stop $service.service)?;

        // make sure the process is really killed
        if run_cmd!(systemctl --user is-active --quiet $service.service).is_ok() {
            cmd_die!("Failed to stop $service: service is still running");
        }
    }
    Ok(())
}

pub fn start_services(build_mode: BuildMode, service: ServiceName) -> CmdResult {
    match service {
        ServiceName::Bss => start_bss_service(build_mode)?,
        ServiceName::Nss => start_nss_service(build_mode)?,
        ServiceName::ApiServer => start_api_server(build_mode)?,
        ServiceName::All => {
            start_bss_service(build_mode)?;
            start_nss_service(build_mode)?;
            start_api_server(build_mode)?;
        }
    }
    Ok(())
}

pub fn start_bss_service(build_mode: BuildMode) -> CmdResult {
    create_systemd_unit_file(ServiceName::Bss, build_mode)?;

    let bss_wait_secs = 10;
    run_cmd! {
        mkdir -p data/bss;
        systemctl --user start bss.service;
        info "Waiting ${bss_wait_secs}s for bss server up";
        sleep $bss_wait_secs;
    }?;

    let bss_server_pid = run_fun!(pidof bss_server)?;
    check_pids(ServiceName::Bss, &bss_server_pid)?;
    info!("bss server (pid={bss_server_pid}) started");
    Ok(())
}

pub fn start_nss_service(build_mode: BuildMode) -> CmdResult {
    create_systemd_unit_file(ServiceName::Nss, build_mode)?;

    if run_cmd!(test -f ./data/current/$ROOT_BLOB_ID).is_err() {
        run_cmd! {
            info "Could not find root blob ($ROOT_BLOB_ID), formatting at first ...";
            bash -c "mkdir -p data/{current,pending}";
            ./zig-out/bin/mkfs;
        }?;
    }

    let nss_wait_secs = 10;
    run_cmd! {
        systemctl --user start nss.service;
        info "Waiting ${nss_wait_secs}s for nss server up";
        sleep $nss_wait_secs;
    }?;
    let nss_server_pid = run_fun!(pidof nss_server)?;
    check_pids(ServiceName::Nss, &nss_server_pid)?;
    info!("nss server (pid={nss_server_pid}) started");
    Ok(())
}

pub fn start_api_server(build_mode: BuildMode) -> CmdResult {
    create_systemd_unit_file(ServiceName::ApiServer, build_mode)?;

    let api_server_wait_secs = 5;
    run_cmd! {
        systemctl --user start api_server.service;
        info "Waiting ${api_server_wait_secs}s for api server up";
        sleep $api_server_wait_secs;
    }?;
    let api_server_pid = run_fun!(pidof api_server)?;
    check_pids(ServiceName::ApiServer, &api_server_pid)?;
    info!("api server (pid={api_server_pid}) started");
    Ok(())
}

fn create_systemd_unit_file(service: ServiceName, build_mode: BuildMode) -> CmdResult {
    let pwd = run_fun!(pwd)?;
    let build = build_mode.as_ref();
    let service_name = service.as_ref();
    let mut env_settings = "";
    let exec_start = match service {
        ServiceName::Bss => format!("{pwd}/zig-out/bin/bss_server"),
        ServiceName::Nss => format!("{pwd}/zig-out/bin/nss_server"),
        ServiceName::ApiServer => {
            if let BuildMode::Debug = build_mode {
                env_settings = "\nEnvironment=\"RUST_LOG=debug\"";
            }
            format!("{pwd}/target/{build}/api_server")
        }
        ServiceName::All => unreachable!(),
    };
    let systemd_unit_content = format!(
        r##"
[Unit]
Description={service_name} Service

[Service]
LimitNOFILE=1000000
LimitCORE=infinity
WorkingDirectory={pwd}{env_settings}
ExecStart={exec_start}

[Install]
WantedBy=multi-user.target
"##
    );
    let service_file = format!("{service_name}.service");

    run_cmd! {
        mkdir -p etc;
        echo $systemd_unit_content > etc/$service_file;
        info "Linking ./etc/$service_file into ~/.config/systemd/user";
        systemctl --user link ./etc/$service_file --force --quiet;
    }?;
    Ok(())
}

fn check_pids(service: ServiceName, pids: &str) -> CmdResult {
    if pids.split_whitespace().count() > 1 {
        error!("Multiple processes were found: {pids}, stopping services ...");
        stop_services(service)?;
        cmd_die!("Multiple processes were found: {pids}");
    }
    Ok(())
}
