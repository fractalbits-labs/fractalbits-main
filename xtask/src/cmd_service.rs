use crate::build::BuildMode;
use crate::TEST_BUCKET_ROOT_BLOB_NAME;
use crate::{ServiceAction, ServiceName};
use cmd_lib::*;

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
            ServiceName::Rss.as_ref().to_owned(),
            "etcd".to_owned(),
            "minio".to_owned(),
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
        ServiceName::Rss => start_rss_service(build_mode)?,
        ServiceName::ApiServer => start_api_server(build_mode)?,
        ServiceName::All => {
            start_rss_service(build_mode)?;
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

    if run_cmd!(test -f ./ebs/fbs.state).is_err() {
        run_cmd! {
            info "Could not find state log, formatting at first ...";
            ./zig-out/bin/mkfs;
            ./zig-out/bin/fbs --new_tree $TEST_BUCKET_ROOT_BLOB_NAME;
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

pub fn start_rss_service(build_mode: BuildMode) -> CmdResult {
    // Start etcd service at first if needed, since root server stores infomation in etcd
    if run_cmd!(systemctl --user is-active --quiet etcd.service).is_err() {
        start_etcd_service()?;
    }

    // Start minio to simulate local s3 service
    if run_cmd!(systemctl --user is-active --quiet minio.service).is_err() {
        start_minio_service()?;
    }

    // Initialize api key for testing
    run_cmd!(./target/debug/rss_admin api-key init-test)?;

    create_systemd_unit_file(ServiceName::Rss, build_mode)?;
    let rss_wait_secs = 10;
    run_cmd! {
        systemctl --user start rss.service;
        info "Waiting ${rss_wait_secs}s for root server up";
        sleep $rss_wait_secs;
    }?;
    let rss_server_pid = run_fun!(pidof root_server)?;
    check_pids(ServiceName::Nss, &rss_server_pid)?;
    info!("root server (pid={rss_server_pid}) started");
    Ok(())
}

fn start_etcd_service() -> CmdResult {
    let pwd = run_fun!(pwd)?;
    let service_file = "etc/etcd.service";
    let service_file_content = format!(
        r##"
[Unit]
Description=etcd for root_server

[Install]
WantedBy=default.target

[Service]
Type=simple
ExecStart="/home/linuxbrew/.linuxbrew/opt/etcd/bin/etcd"
Restart=always
WorkingDirectory={pwd}/ebs
"##
    );

    let etcd_wait_secs = 5;
    run_cmd! {
        mkdir -p etc;
        mkdir -p ebs;
        echo $service_file_content > $service_file;
        info "Linking $service_file into ~/.config/systemd/user";
        systemctl --user link $service_file --force --quiet;
        systemctl --user start etcd.service;
        info "Waiting ${etcd_wait_secs}s for etcd up";
        sleep $etcd_wait_secs;
        systemctl --user is-active --quiet etcd.service;
    }?;

    Ok(())
}

fn start_minio_service() -> CmdResult {
    let pwd = run_fun!(pwd)?;
    let service_file = "etc/minio.service";
    let service_file_content = format!(
        r##"
[Unit]
Description=Simulated s3 service (minio)

[Install]
WantedBy=default.target

[Service]
Type=simple
ExecStart=/home/linuxbrew/.linuxbrew/opt/minio/bin/minio server s3/
Restart=always
WorkingDirectory={pwd}
"##
    );
    let minio_wait_secs = 5;
    let minio_url = "http://localhost:9000";
    run_cmd! {
        mkdir -p etc;
        mkdir -p s3;
        echo $service_file_content > $service_file;
        info "Linking $service_file into ~/.config/systemd/user";
        systemctl --user link $service_file --force --quiet;
        systemctl --user start minio.service;
        info "Waiting ${minio_wait_secs}s for minio up";
        sleep $minio_wait_secs;
        info "Creating s3 bucket (\"mybucket\") in minio ...";
        AWS_ENDPOINT_URL_S3=$minio_url AWS_ACCESS_KEY_ID=minioadmin AWS_SECRET_ACCESS_KEY=minioadmin
            aws s3api create-bucket --bucket mybucket;
    }?;

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
        ServiceName::Rss => {
            if let BuildMode::Debug = build_mode {
                env_settings = "\nEnvironment=\"RUST_LOG=info\"";
            }
            format!("{pwd}/target/{build}/root_server")
        }
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
