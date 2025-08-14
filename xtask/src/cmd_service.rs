use crate::*;

pub fn run_cmd_service(
    service: ServiceName,
    action: ServiceAction,
    build_mode: BuildMode,
    for_gui: bool,
    data_blob_storage: DataBlobStorage,
) -> CmdResult {
    match action {
        ServiceAction::Init => init_service(service, build_mode),
        ServiceAction::Stop => stop_service(service),
        ServiceAction::Start => start_services(service, build_mode, for_gui, data_blob_storage),
        ServiceAction::Restart => {
            stop_service(service)?;
            start_services(service, build_mode, for_gui, data_blob_storage)
        }
    }
}

pub fn init_service(service: ServiceName, build_mode: BuildMode) -> CmdResult {
    stop_service(service)?;

    let init_ddb_local = || -> CmdResult {
        run_cmd! {
            rm -f data/rss/shared-local-instance.db;
            mkdir -p data/rss;
        }?;
        start_ddb_local_service()?;

        // Create main keys-and-buckets table
        const DDB_TABLE_NAME: &str = "fractalbits-keys-and-buckets";
        run_cmd! {
            info "Initializing table: $DDB_TABLE_NAME ...";
            AWS_DEFAULT_REGION=fakeRegion
            AWS_ACCESS_KEY_ID=fakeMyKeyId
            AWS_SECRET_ACCESS_KEY=fakeSecretAccessKey
            AWS_ENDPOINT_URL_DYNAMODB="http://localhost:8000"
            aws dynamodb create-table
                --table-name $DDB_TABLE_NAME
                --attribute-definitions AttributeName=id,AttributeType=S
                --key-schema AttributeName=id,KeyType=HASH
                --provisioned-throughput ReadCapacityUnits=1,WriteCapacityUnits=1 >/dev/null;
        }?;

        // Create leader election table for root server
        const LEADER_TABLE_NAME: &str = "fractalbits-leader-election";
        run_cmd! {
            info "Initializing leader election table: $LEADER_TABLE_NAME ...";
            AWS_DEFAULT_REGION=fakeRegion
            AWS_ACCESS_KEY_ID=fakeMyKeyId
            AWS_SECRET_ACCESS_KEY=fakeSecretAccessKey
            AWS_ENDPOINT_URL_DYNAMODB="http://localhost:8000"
            aws dynamodb create-table
                --table-name $LEADER_TABLE_NAME
                --attribute-definitions AttributeName=key,AttributeType=S
                --key-schema AttributeName=key,KeyType=HASH
                --provisioned-throughput ReadCapacityUnits=1,WriteCapacityUnits=1 >/dev/null;
        }?;

        Ok(())
    };
    let init_rss = || -> CmdResult {
        // Start ddb_local service at first if needed, since root server stores infomation in ddb_local
        if run_cmd!(systemctl --user is-active --quiet ddb_local.service).is_err() {
            init_ddb_local()?;
        }

        // Initialize api key for testing
        let build = build_mode.as_ref();
        run_cmd! {
            AWS_DEFAULT_REGION=fakeRegion
            AWS_ACCESS_KEY_ID=fakeMyKeyId
            AWS_SECRET_ACCESS_KEY=fakeSecretAccessKey
            AWS_ENDPOINT_URL_DYNAMODB="http://localhost:8000"
            ./target/${build}/rss_admin --region=fakeRegion api-key init-test;
        }?;
        stop_service(ServiceName::DdbLocal)?;
        Ok(())
    };
    let init_nss = || -> CmdResult {
        let pwd = run_fun!(pwd)?;
        let format_log = "data/logs/format.log";
        let fbs_log = "data/logs/fbs.log";
        create_dirs_for_nss_server()?;
        match build_mode {
            BuildMode::Debug => run_cmd! {
                info "formatting nss_server with default configs";
                ${pwd}/zig-out/bin/nss_server format
                    |& ts -m $TS_FMT >$format_log;
                ${pwd}/zig-out/bin/fbs --new_tree $TEST_BUCKET_ROOT_BLOB_NAME
                    |& ts -m $TS_FMT >$fbs_log;
            }?,
            BuildMode::Release => run_cmd! {
                info "formatting nss_server for benchmarking";
                ${pwd}/zig-out/bin/nss_server format -c ${pwd}/etc/$NSS_SERVER_BENCH_CONFIG
                    |& ts -m $TS_FMT >$format_log;
                ${pwd}/zig-out/bin/fbs --new_tree $TEST_BUCKET_ROOT_BLOB_NAME
                    -c ${pwd}/etc/$NSS_SERVER_BENCH_CONFIG |& ts -m $TS_FMT >$fbs_log;
            }?,
        }
        Ok(())
    };
    let init_nss_role_agent = || -> CmdResult { Ok(()) };
    let init_minio = || run_cmd!(mkdir -p data/s3);
    let init_bss = || create_dirs_for_bss_server();
    let init_mirrord = || run_cmd!(mkdir -p data/nss-standby);

    match service {
        ServiceName::ApiServer => {}
        ServiceName::DdbLocal => init_ddb_local()?,
        ServiceName::Minio => init_minio()?,
        ServiceName::Bss => init_bss()?,
        ServiceName::Rss => init_rss()?,
        ServiceName::Nss => init_nss()?,
        ServiceName::NssRoleAgentA => init_nss_role_agent()?,
        ServiceName::NssRoleAgentB => init_nss_role_agent()?,
        ServiceName::Mirrord => init_mirrord()?,
        ServiceName::DataBlobResyncServer => {},
        ServiceName::All => {
            init_rss()?;
            init_bss()?;
            init_nss()?;
            init_mirrord()?;
        }
    }
    Ok(())
}

pub fn stop_service(service: ServiceName) -> CmdResult {
    let services: Vec<String> = match service {
        ServiceName::All => vec![
            ServiceName::ApiServer.as_ref().to_owned(),
            ServiceName::NssRoleAgentA.as_ref().to_owned(),
            ServiceName::NssRoleAgentB.as_ref().to_owned(),
            ServiceName::Bss.as_ref().to_owned(),
            ServiceName::Rss.as_ref().to_owned(),
            ServiceName::Minio.as_ref().to_owned(),
            ServiceName::DdbLocal.as_ref().to_owned(),
            ServiceName::Mirrord.as_ref().to_owned(),
            ServiceName::Nss.as_ref().to_owned(),
            ServiceName::DataBlobResyncServer.as_ref().to_owned(),
        ],
        single_service => vec![single_service.as_ref().to_owned()],
    };

    info!("Killing previous service(s) (if any) ...");
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

pub fn start_services(
    service: ServiceName,
    build_mode: BuildMode,
    for_gui: bool,
    data_blob_storage: DataBlobStorage,
) -> CmdResult {
    match service {
        ServiceName::Bss => start_bss_service(build_mode, data_blob_storage)?,
        ServiceName::Nss => start_nss_service(build_mode, false)?,
        ServiceName::NssRoleAgentA => start_nss_role_agent_service(build_mode, "A")?,
        ServiceName::NssRoleAgentB => start_nss_role_agent_service(build_mode, "B")?,
        ServiceName::Rss => start_rss_service(build_mode)?,
        ServiceName::ApiServer => start_api_server(build_mode, for_gui)?,
        ServiceName::DataBlobResyncServer => {
            info!("DataBlobResyncServer is a standalone CLI tool, not a service. Use 'cargo run -p data_blob_resync_server' instead.");
        },
        ServiceName::All => start_all_services(build_mode, for_gui, data_blob_storage)?,
        ServiceName::Minio => start_minio_service(data_blob_storage)?,
        ServiceName::DdbLocal => start_ddb_local_service()?,
        ServiceName::Mirrord => start_mirrord_service(build_mode)?,
    }
    Ok(())
}

pub fn start_bss_service(build_mode: BuildMode, data_blob_storage: DataBlobStorage) -> CmdResult {
    match data_blob_storage {
        DataBlobStorage::S3Express => {
            info!("Skipping bss_server in s3_express mode");
            return Ok(());
        }
        DataBlobStorage::Hybrid => {
            // Continue with normal BSS server startup
        }
    }

    create_systemd_unit_file(ServiceName::Bss, build_mode, None)?;

    run_cmd!(systemctl --user start bss.service)?;
    wait_for_service_ready(ServiceName::Bss, 15)?;

    let bss_server_pid = run_fun!(pidof bss_server)?;
    check_pids(ServiceName::Bss, &bss_server_pid)?;
    info!("bss server (pid={bss_server_pid}) started");
    Ok(())
}

pub fn start_nss_service(build_mode: BuildMode, data_on_local: bool) -> CmdResult {
    if !data_on_local {
        // Start minio to simulate local s3 service
        if run_cmd!(systemctl --user is-active --quiet minio.service).is_err() {
            start_minio_service(DataBlobStorage::Hybrid)?;
        }
    }

    let pwd = run_fun!(pwd)?;
    let config_file = match build_mode {
        BuildMode::Debug => None,
        BuildMode::Release => Some(format!("{pwd}/etc/{NSS_SERVER_BENCH_CONFIG}")),
    };
    create_systemd_unit_file(ServiceName::Nss, build_mode, config_file)?;

    run_cmd!(systemctl --user start nss.service)?;
    wait_for_service_ready(ServiceName::Nss, 15)?;

    let nss_server_pid = run_fun!(pidof nss_server)?;
    check_pids(ServiceName::Nss, &nss_server_pid)?;
    info!("nss server (pid={nss_server_pid}) started");
    Ok(())
}

pub fn start_mirrord_service(build_mode: BuildMode) -> CmdResult {
    create_systemd_unit_file(ServiceName::Mirrord, build_mode, None)?;

    run_cmd!(systemctl --user start mirrord.service)?;
    wait_for_service_ready(ServiceName::Mirrord, 15)?;

    let mirrord_pid = run_fun!(pidof mirrord)?;
    check_pids(ServiceName::Mirrord, &mirrord_pid)?;
    info!("nss server (pid={mirrord_pid}) started");
    Ok(())
}

pub fn start_nss_role_agent_service(build_mode: BuildMode, agent_id: &str) -> CmdResult {
    let service_name = match agent_id {
        "A" => ServiceName::NssRoleAgentA,
        "B" => ServiceName::NssRoleAgentB,
        _ => panic!("Invalid agent_id: {agent_id}"),
    };

    create_systemd_unit_file(service_name, build_mode, None)?;

    let service_file = format!("nss_role_agent_{}.service", agent_id.to_lowercase());
    run_cmd!(systemctl --user start $service_file)?;
    wait_for_service_ready(service_name, 15)?;

    // For role agents, get the PID from systemd instead of pidof to avoid conflicts
    let pid_output = run_fun!(systemctl --user show --property=MainPID --value $service_file)?;
    let server_pid = pid_output.trim();
    info!(
        "nss_role_agent_{} server (pid={server_pid}) started",
        agent_id.to_lowercase()
    );
    Ok(())
}

pub fn start_rss_service(build_mode: BuildMode) -> CmdResult {
    // Start ddb_local service at first if needed, since root server stores infomation in ddb_local
    if run_cmd!(systemctl --user is-active --quiet ddb_local.service).is_err() {
        start_ddb_local_service()?;
    }

    create_systemd_unit_file(ServiceName::Rss, build_mode, None)?;
    run_cmd!(systemctl --user start rss.service)?;
    wait_for_service_ready(ServiceName::Rss, 15)?;

    let rss_server_pid = run_fun!(pidof root_server)?;
    check_pids(ServiceName::Rss, &rss_server_pid)?;
    info!("root server (pid={rss_server_pid}) started");
    Ok(())
}

fn start_ddb_local_service() -> CmdResult {
    let pwd = run_fun!(pwd)?;
    let service_file = "etc/ddb_local.service";
    let java = run_fun!(bash -c "command -v java")?;
    let java_lib = format!("{pwd}/dynamodb_local/DynamoDBLocal_lib");
    let working_dir = format!("{pwd}/data/rss");
    let service_file_content = format!(
        r##"[Unit]
Description=dynamodb local service for root_server

[Install]
WantedBy=default.target

[Service]
Type=simple
ExecStart={java} -Djava.library.path={java_lib} -jar {java_lib}/../DynamoDBLocal.jar -sharedDb -dbPath {working_dir}
Restart=on-failure
RestartSec=5
StandardOutput=journal
StandardError=journal
WorkingDirectory={working_dir}
"##
    );

    run_cmd! {
        mkdir -p $working_dir;
        mkdir -p etc;
        echo $service_file_content > $service_file;
        info "Linking $service_file into ~/.config/systemd/user";
        systemctl --user link $service_file --force --quiet;
        systemctl --user start ddb_local.service;
    }?;
    wait_for_service_ready(ServiceName::DdbLocal, 10)?;

    Ok(())
}

pub fn start_minio_service(data_blob_storage: DataBlobStorage) -> CmdResult {
    let pwd = run_fun!(pwd)?;
    let service_file = "etc/minio.service";
    let service_file_content = format!(
        r##"[Unit]
Description=Simulated s3 service (minio)

[Install]
WantedBy=default.target

[Service]
Type=simple
ExecStart=/home/linuxbrew/.linuxbrew/opt/minio/bin/minio server s3/
Restart=always
WorkingDirectory={pwd}/data
"##
    );
    let minio_url = "http://localhost:9000";
    let (local_bucket_name, remote_bucket_name) = match data_blob_storage {
        DataBlobStorage::Hybrid => ("fractalbits-bucket", "fractalbits-bucket"),
        DataBlobStorage::S3Express => (
            "fractalbits-local-az-data-bucket",
            "fractalbits-remote-az-data-bucket",
        ),
    };
    let local_bucket = format!("s3://{local_bucket_name}");
    let remote_bucket = format!("s3://{remote_bucket_name}");

    run_cmd! {
        mkdir -p etc;
        echo $service_file_content > $service_file;
        info "Linking $service_file into ~/.config/systemd/user";
        systemctl --user link $service_file --force --quiet;
        systemctl --user start minio.service;
    }?;
    wait_for_service_ready(ServiceName::Minio, 10)?;

    run_cmd! {
        info "Creating s3 buckets (\"$local_bucket_name\" and \"$remote_bucket_name\") in minio ...";
        ignore AWS_ENDPOINT_URL_S3=$minio_url AWS_ACCESS_KEY_ID=minioadmin AWS_SECRET_ACCESS_KEY=minioadmin
            aws s3 mb $local_bucket &>/dev/null;
        ignore AWS_ENDPOINT_URL_S3=$minio_url AWS_ACCESS_KEY_ID=minioadmin AWS_SECRET_ACCESS_KEY=minioadmin
            aws s3 mb $remote_bucket &>/dev/null;
    }?;

    let mut wait_new_buckets_secs = 0;
    const TIMEOUT_SECS: i32 = 5;
    loop {
        let local_ready = run_cmd! (
            AWS_ENDPOINT_URL_S3=$minio_url AWS_ACCESS_KEY_ID=minioadmin AWS_SECRET_ACCESS_KEY=minioadmin
            aws s3api head-bucket --bucket $local_bucket_name &>/dev/null
        ).is_ok();

        let remote_ready = run_cmd! (
            AWS_ENDPOINT_URL_S3=$minio_url AWS_ACCESS_KEY_ID=minioadmin AWS_SECRET_ACCESS_KEY=minioadmin
            aws s3api head-bucket --bucket $remote_bucket_name &>/dev/null
        ).is_ok();

        if local_ready && remote_ready {
            break;
        }

        wait_new_buckets_secs += 1;
        if wait_new_buckets_secs >= TIMEOUT_SECS {
            cmd_die!("timeout waiting for newly created buckets {local_bucket_name} and {remote_bucket_name}");
        }

        info!("waiting for newly created buckets {local_bucket_name} and {remote_bucket_name}: {wait_new_buckets_secs}s");
        std::thread::sleep(std::time::Duration::from_secs(1));
    }
    Ok(())
}

pub fn start_api_server(build_mode: BuildMode, for_gui: bool) -> CmdResult {
    let pwd = run_fun!(pwd)?;
    let config_file = match for_gui {
        false => None,
        true => Some(format!("{pwd}/etc/{API_SERVER_GUI_CONFIG}")),
    };
    create_systemd_unit_file(ServiceName::ApiServer, build_mode, config_file)?;

    run_cmd!(systemctl --user start api_server.service)?;
    wait_for_service_ready(ServiceName::ApiServer, 10)?;

    let api_server_pid = run_fun!(pidof api_server)?;
    check_pids(ServiceName::ApiServer, &api_server_pid)?;
    info!("api server (pid={api_server_pid}) started");
    Ok(())
}

pub fn start_all_services(
    build_mode: BuildMode,
    for_gui: bool,
    data_blob_storage: DataBlobStorage,
) -> CmdResult {
    info!("Starting all services with systemd dependency management");

    // Create all systemd unit files first
    let pwd = run_fun!(pwd)?;
    let api_config_file = match for_gui {
        false => None,
        true => Some(format!("{pwd}/etc/{API_SERVER_GUI_CONFIG}")),
    };

    create_systemd_unit_file(ServiceName::Rss, build_mode, None)?;

    // Only create BSS systemd unit file if we're in hybrid mode
    match data_blob_storage {
        DataBlobStorage::Hybrid => {
            create_systemd_unit_file(ServiceName::Bss, build_mode, None)?;
        }
        DataBlobStorage::S3Express => {
            info!("Skipping BSS systemd unit file creation in s3_express mode");
        }
    }

    create_systemd_unit_file(ServiceName::NssRoleAgentA, build_mode, None)?;
    create_systemd_unit_file(ServiceName::Nss, build_mode, None)?;

    create_systemd_unit_file(ServiceName::NssRoleAgentB, build_mode, None)?;
    create_systemd_unit_file(ServiceName::Mirrord, build_mode, None)?;

    create_systemd_unit_file(ServiceName::ApiServer, build_mode, api_config_file)?;

    // Start supporting services first
    info!("Starting supporting services (ddb_local, minio)");
    start_ddb_local_service()?;
    start_minio_service(data_blob_storage)?;

    wait_for_service_ready(ServiceName::DdbLocal, 10)?;

    // Start all main services - systemd dependencies will handle ordering
    match data_blob_storage {
        DataBlobStorage::Hybrid => {
            info!("Starting all main services (systemd will handle dependency ordering)");
            run_cmd!(systemctl --user start rss.service bss.service nss_role_agent_a.service nss_role_agent_b.service api_server.service)?;

            // Wait for all services to be ready in dependency order
            wait_for_service_ready(ServiceName::Rss, 15)?;
            wait_for_service_ready(ServiceName::Bss, 15)?;
            wait_for_service_ready(ServiceName::NssRoleAgentA, 15)?;
            wait_for_service_ready(ServiceName::NssRoleAgentB, 15)?;
            wait_for_service_ready(ServiceName::ApiServer, 15)?;
        }
        DataBlobStorage::S3Express => {
            info!("Starting main services (skipping BSS in s3_express mode)");
            run_cmd!(systemctl --user start rss.service nss_role_agent_a.service nss_role_agent_b.service api_server.service)?;

            // Wait for all services to be ready in dependency order (excluding BSS)
            wait_for_service_ready(ServiceName::Rss, 15)?;
            info!("Skipping BSS service readiness check in s3_express mode");
            wait_for_service_ready(ServiceName::NssRoleAgentA, 15)?;
            wait_for_service_ready(ServiceName::NssRoleAgentB, 15)?;
            wait_for_service_ready(ServiceName::ApiServer, 15)?;
        }
    }

    info!("All services started successfully!");
    Ok(())
}

fn create_systemd_unit_file(
    service: ServiceName,
    build_mode: BuildMode,
    config_file: Option<String>,
) -> CmdResult {
    let pwd = run_fun!(pwd)?;
    let build = build_mode.as_ref();
    let service_name = service.as_ref();
    let mut env_settings = String::new();
    let env_rust_log = |build_mode: BuildMode| -> &'static str {
        match build_mode {
            BuildMode::Debug => {
                r##"
Environment="RUST_LOG=debug""##
            }
            BuildMode::Release => {
                r##"
Environment="RUST_LOG=warn""##
            }
        }
    };
    let mut exec_start = match service {
        ServiceName::Bss => format!("{pwd}/zig-out/bin/bss_server"),
        ServiceName::Nss => match build_mode {
            BuildMode::Debug => format!("{pwd}/zig-out/bin/nss_server serve"),
            BuildMode::Release => {
                format!("{pwd}/zig-out/bin/nss_server serve -c {pwd}/etc/{NSS_SERVER_BENCH_CONFIG}")
            }
        },
        ServiceName::Mirrord => format!("{pwd}/zig-out/bin/mirrord"),
        ServiceName::NssRoleAgentA => {
            env_settings += env_rust_log(build_mode);
            env_settings += r##"
Environment="APP_AGENT_ID=A"
Environment="APP_SERVICE_TYPE=nss""##;
            format!("{pwd}/target/{build}/nss_role_agent")
        }
        ServiceName::NssRoleAgentB => {
            env_settings += env_rust_log(build_mode);
            env_settings += r##"
Environment="APP_AGENT_ID=B"
Environment="APP_SERVICE_TYPE=mirrord""##;
            format!("{pwd}/target/{build}/nss_role_agent")
        }
        ServiceName::Rss => {
            env_settings = r##"
Environment="AWS_DEFAULT_REGION=fakeRegion"
Environment="AWS_ACCESS_KEY_ID=fakeMyKeyId"
Environment="AWS_ACCESS_KEY_ID=fakeMyKeyId"
Environment="AWS_ENDPOINT_URL_DYNAMODB=http://localhost:8000""##
                .to_string();
            env_settings += env_rust_log(build_mode);
            format!("{pwd}/target/{build}/root_server -r fakeRegion")
        }
        ServiceName::ApiServer => {
            env_settings += env_rust_log(build_mode);
            format!("{pwd}/target/{build}/api_server")
        }
        _ => unreachable!(),
    };
    if let Some(config) = config_file {
        exec_start += &format!(" -c {config}");
    }
    let working_dir = run_fun!(realpath $pwd)?;

    // Add systemd dependencies based on service type
    let dependencies = match service {
        ServiceName::Rss => "After=ddb_local.service\nWants=ddb_local.service\n",
        ServiceName::Nss => "After=minio.service\nWants=minio.service\n",
        ServiceName::ApiServer => {
            "After=rss.service bss.service nss.service\nWants=rss.service bss.service nss.service\n"
        }
        _ => "",
    };

    let systemd_unit_content = format!(
        r##"[Unit]
Description={service_name} Service
{dependencies}
[Service]
LimitNOFILE=1000000
LimitCORE=infinity
WorkingDirectory={working_dir}{env_settings}
ExecStart={exec_start}

[Install]
WantedBy=multi-user.target
"##
    );
    let service_file = match service {
        ServiceName::NssRoleAgentA => "nss_role_agent_a.service".to_string(),
        ServiceName::NssRoleAgentB => "nss_role_agent_b.service".to_string(),
        _ => format!("{service_name}.service"),
    };

    run_cmd! {
        mkdir -p $pwd/data/logs;
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
        stop_service(service)?;
        cmd_die!("Multiple processes were found: {pids}");
    }
    Ok(())
}

fn create_dirs_for_nss_server() -> CmdResult {
    info!("Creating necessary directories for nss_server");
    run_cmd! {
        mkdir -p data/logs;
        mkdir -p data/nss-active/ebs;
        mkdir -p data/nss-active/local/stats;
        mkdir -p data/nss-active/local/meta_cache/blobs;
    }?;
    for i in 0..256 {
        run_cmd!(mkdir -p data/nss-active/local/meta_cache/blobs/dir$i)?;
    }

    Ok(())
}

fn create_dirs_for_bss_server() -> CmdResult {
    info!("Creating necessary directories for bss_server");
    run_cmd! {
        mkdir -p data/bss/local/stats;
        mkdir -p data/bss/local/blobs;
    }?;
    for i in 0..256 {
        run_cmd!(mkdir -p data/bss/local/blobs/dir$i)?;
    }

    Ok(())
}

fn wait_for_service_ready(service: ServiceName, timeout_secs: u32) -> CmdResult {
    use std::time::{Duration, Instant};

    let start = Instant::now();
    let timeout = Duration::from_secs(timeout_secs as u64);
    let service_name = service.as_ref();

    info!("Waiting for {service_name} to be ready (timeout: {timeout_secs}s)");

    while start.elapsed() < timeout {
        // Check if systemd reports service as active
        if run_cmd!(systemctl --user is-active --quiet $service_name.service).is_ok() {
            // For network services, also check port availability
            let port_ready = match service {
                ServiceName::DdbLocal => check_port_ready(8000),
                ServiceName::Minio => check_port_ready(9000),
                ServiceName::Rss => check_port_ready(8086),
                ServiceName::Bss => check_port_ready(8088),
                ServiceName::Nss => check_port_ready(8087),
                ServiceName::Mirrord => check_port_ready(9999),
                ServiceName::ApiServer => check_port_ready(8080),
                ServiceName::NssRoleAgentA => true, // No network port for this service
                ServiceName::NssRoleAgentB => true, // No network port for this service
                ServiceName::DataBlobResyncServer => true, // CLI tool, not a service
                ServiceName::All => unreachable!("Should not check readiness for All"),
            };

            if port_ready {
                info!("{service_name} is ready");
                return Ok(());
            }
        }

        std::thread::sleep(Duration::from_millis(500));
    }

    cmd_die!("Timeout waiting for ${service_name} to be ready after ${timeout_secs}s")
}

fn check_port_ready(port: u16) -> bool {
    run_cmd!(nc -z localhost $port).is_ok()
}
