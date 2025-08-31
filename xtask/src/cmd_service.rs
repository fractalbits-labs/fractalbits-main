use std::time::Duration;

use crate::*;
use colored::*;

pub fn init_service(
    service: ServiceName,
    build_mode: BuildMode,
    init_config: crate::InitConfig,
) -> CmdResult {
    stop_service(service)?;

    // Create systemd unit files for the services being initialized
    create_systemd_unit_files_for_init(
        service,
        build_mode,
        init_config.for_gui,
        init_config.data_blob_storage,
    )?;

    let init_ddb_local = || -> CmdResult {
        run_cmd! {
            rm -f data/rss/shared-local-instance.db;
            mkdir -p data/rss;
        }?;
        start_ddb_local_service()?;

        // Create main keys-and-buckets table
        const DDB_TABLE_NAME: &str = "fractalbits-api-keys-and-buckets";
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

        // Create service-discovery table for NSS role states
        const SERVICE_DISCOVERY_TABLE: &str = "fractalbits-service-discovery";
        run_cmd! {
            info "Creating service-discovery table: $SERVICE_DISCOVERY_TABLE ...";
            AWS_DEFAULT_REGION=fakeRegion
            AWS_ACCESS_KEY_ID=fakeMyKeyId
            AWS_SECRET_ACCESS_KEY=fakeSecretAccessKey
            AWS_ENDPOINT_URL_DYNAMODB="http://localhost:8000"
            aws dynamodb create-table
                --table-name $SERVICE_DISCOVERY_TABLE
                --attribute-definitions AttributeName=service_id,AttributeType=S
                --key-schema AttributeName=service_id,KeyType=HASH
                --provisioned-throughput ReadCapacityUnits=1,WriteCapacityUnits=1 >/dev/null;
        }?;

        // Initialize NSS role states in service-discovery table
        let nss_roles_item = match init_config.data_blob_storage {
            DataBlobStorage::HybridSingleAz | DataBlobStorage::S3ExpressSingleAz => {
                r#"{"service_id":{"S":"nss_roles"},"states":{"M":{"nss-A":{"S":"solo"},"nss-B":{"S":"dead"}}}}"#
            }
            DataBlobStorage::S3ExpressMultiAz => {
                r#"{"service_id":{"S":"nss_roles"},"states":{"M":{"nss-A":{"S":"active"},"nss-B":{"S":"standby"}}}}"#
            }
        };

        run_cmd! {
            info "Initializing NSS role states in service-discovery table ...";
            AWS_DEFAULT_REGION=fakeRegion
            AWS_ACCESS_KEY_ID=fakeMyKeyId
            AWS_SECRET_ACCESS_KEY=fakeSecretAccessKey
            AWS_ENDPOINT_URL_DYNAMODB="http://localhost:8000"
            aws dynamodb put-item
                --table-name $SERVICE_DISCOVERY_TABLE
                --item $nss_roles_item >/dev/null;
        }?;

        // Initialize AZ status in service-discovery table (using mock AZ names for local testing)
        let az_status_item = r#"{"service_id":{"S":"az_status"},"status":{"M":{"localdev-az1":{"S":"Normal"},"localdev-az2":{"S":"Normal"}}}}"#;

        run_cmd! {
            info "Initializing AZ status in service-discovery table ...";
            AWS_DEFAULT_REGION=fakeRegion
            AWS_ACCESS_KEY_ID=fakeMyKeyId
            AWS_SECRET_ACCESS_KEY=fakeSecretAccessKey
            AWS_ENDPOINT_URL_DYNAMODB="http://localhost:8000"
            aws dynamodb put-item
                --table-name $SERVICE_DISCOVERY_TABLE
                --item $az_status_item >/dev/null;
        }?;

        Ok(())
    };
    let init_rss = || -> CmdResult {
        // Start ddb_local service at first if needed, since root server stores infomation in ddb_local
        if run_cmd!(systemctl --user is-active --quiet ddb_local.service).is_err() {
            init_ddb_local()?;
        }

        // Start RSS service since admin now connects via RPC
        start_rss_service()?;

        // Initialize api key for testing using RSS RPC
        let build = build_mode.as_ref();
        run_cmd! {
            ./target/${build}/rss_admin --rss-addr=127.0.0.1:8086 api-key init-test;
        }?;

        // Stop services after initialization
        stop_service(ServiceName::Rss)?;
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
                ${pwd}/zig-out/bin/nss_server format
                    |& ts -m $TS_FMT >$format_log;
                ${pwd}/zig-out/bin/fbs --new_tree $TEST_BUCKET_ROOT_BLOB_NAME
                    |& ts -m $TS_FMT >$fbs_log;
            }?,
        }
        Ok(())
    };
    let init_nss_role_agent = || -> CmdResult { Ok(()) };
    let init_minio = || run_cmd!(mkdir -p data/s3);
    let init_minio_dev_az1 = || run_cmd!(mkdir -p data/s3-localdev-az1);
    let init_minio_dev_az2 = || run_cmd!(mkdir -p data/s3-localdev-az2);
    let init_bss = || create_dirs_for_bss_server();
    let init_mirrord = || -> CmdResult {
        let pwd = run_fun!(pwd)?;
        let format_log = "data/logs/format_mirrord.log";
        create_dirs_for_mirrord_server()?;
        match build_mode {
            BuildMode::Debug => run_cmd! {
                info "formatting mirrord with default configs";
                WORKING_DIR="./data/nss-B" ${pwd}/zig-out/bin/nss_server format
                    |& ts -m $TS_FMT >$format_log;
            }?,
            BuildMode::Release => run_cmd! {
                info "formatting mirrord for benchmarking";
                WORKING_DIR="./data/nss-B" ${pwd}/zig-out/bin/nss_server format
                    |& ts -m $TS_FMT >$format_log;
            }?,
        }
        Ok(())
    };

    match service {
        ServiceName::ApiServer | ServiceName::GuiServer => {
            generate_https_certificates()?;
        }
        ServiceName::DdbLocal => init_ddb_local()?,
        ServiceName::Minio => init_minio()?,
        ServiceName::MinioAz1 => init_minio_dev_az1()?,
        ServiceName::MinioAz2 => init_minio_dev_az2()?,
        ServiceName::Bss => init_bss()?,
        ServiceName::Rss => init_rss()?,
        ServiceName::Nss => init_nss()?,
        ServiceName::NssRoleAgentA => init_nss_role_agent()?,
        ServiceName::NssRoleAgentB => init_nss_role_agent()?,
        ServiceName::Mirrord => init_mirrord()?,
        ServiceName::DataBlobResyncServer => {}
        ServiceName::All => {
            generate_https_certificates()?;
            init_rss()?;
            init_bss()?;
            init_nss()?;
            init_mirrord()?;
            init_minio()?;
            init_minio_dev_az1()?;
            init_minio_dev_az2()?;
        }
    }
    Ok(())
}

pub fn stop_service(service: ServiceName) -> CmdResult {
    let services: Vec<String> = match service {
        ServiceName::All => vec![
            ServiceName::ApiServer.as_ref().to_owned(),
            ServiceName::NssRoleAgentA.as_ref().to_owned(),
            ServiceName::Nss.as_ref().to_owned(),
            ServiceName::NssRoleAgentB.as_ref().to_owned(),
            ServiceName::Mirrord.as_ref().to_owned(),
            ServiceName::Bss.as_ref().to_owned(),
            ServiceName::Rss.as_ref().to_owned(),
            ServiceName::DdbLocal.as_ref().to_owned(),
            // Stop minio services last
            ServiceName::Minio.as_ref().to_owned(),
            ServiceName::MinioAz1.as_ref().to_owned(),
            ServiceName::MinioAz2.as_ref().to_owned(),
        ],
        single_service => vec![single_service.as_ref().to_owned()],
    };

    info!("Killing previous service(s) (if any) ...");
    for service in services {
        if run_cmd!(systemctl --user is-active --quiet $service.service).is_err() {
            continue;
        }

        if service == ServiceName::Mirrord.as_ref() {
            while run_cmd!(systemctl --user is-active --quiet nss.service).is_ok() {
                // waiting for nss to stop at first, or it may crash nss due to journal mirroring failure
                std::thread::sleep(Duration::from_secs(1));
            }
        }
        run_cmd!(systemctl --user stop $service.service)?;

        // make sure the process is really killed
        if run_cmd!(systemctl --user is-active --quiet $service.service).is_ok() {
            cmd_die!("Failed to stop $service: service is still running");
        }
    }

    Ok(())
}

pub fn show_service_status(service: ServiceName) -> CmdResult {
    match service {
        ServiceName::All => {
            println!("Service Status:");
            println!("─────────────────────────────────────");

            let all_services = vec![
                ServiceName::ApiServer,
                ServiceName::Rss,
                ServiceName::Bss,
                ServiceName::Nss,
                ServiceName::NssRoleAgentA,
                ServiceName::NssRoleAgentB,
                ServiceName::Mirrord,
                ServiceName::DdbLocal,
                ServiceName::Minio,
                ServiceName::MinioAz1,
                ServiceName::MinioAz2,
            ];

            for svc in all_services {
                let display_name = match svc {
                    ServiceName::NssRoleAgentA => "nss_role_agent_a".to_string(),
                    ServiceName::NssRoleAgentB => "nss_role_agent_b".to_string(),
                    _ => svc.as_ref().to_string(),
                };

                let service_to_check = &display_name;

                let status = if run_cmd!(systemctl --user list-unit-files --quiet $service_to_check.service | grep -q $service_to_check).is_ok() {
                    // Service exists, get its status
                    match run_fun!(systemctl --user is-active $service_to_check.service 2>/dev/null) {
                        Ok(output) => match output.trim() {
                            "active" => "active".green().to_string(),
                            status => status.yellow().to_string(),
                        },
                        Err(_) => {
                            // Command failed, try to get the actual status
                            if run_cmd!(systemctl --user is-failed --quiet $service_to_check.service).is_ok() {
                                "failed".red().to_string()
                            } else {
                                "inactive (dead)".bright_black().to_string()
                            }
                        }
                    }
                } else {
                    "not installed".bright_black().to_string()
                };

                println!("{display_name:<16}: {status}");
            }
        }
        single_service => {
            // Show detailed status for a single service
            let service_name = match single_service {
                ServiceName::NssRoleAgentA => "nss_role_agent_a",
                ServiceName::NssRoleAgentB => "nss_role_agent_b",
                ServiceName::Nss => "nss",
                _ => single_service.as_ref(),
            };

            run_cmd!(systemctl --user status $service_name.service --no-pager)?;
        }
    }

    Ok(())
}

pub fn start_service(service: ServiceName) -> CmdResult {
    match service {
        ServiceName::Bss => start_bss_service()?,
        ServiceName::Nss => start_nss_service()?,
        ServiceName::NssRoleAgentA => start_nss_role_agent_service(ServiceName::NssRoleAgentA)?,
        ServiceName::NssRoleAgentB => start_nss_role_agent_service(ServiceName::NssRoleAgentB)?,
        ServiceName::Rss => start_rss_service()?,
        ServiceName::ApiServer => start_api_server()?,
        ServiceName::GuiServer => start_api_server()?,
        ServiceName::DataBlobResyncServer => {
            info!("DataBlobResyncServer is a standalone CLI tool, not a service. Use 'cargo run -p data_blob_resync_server' instead.");
        }
        ServiceName::All => start_all_services()?,
        ServiceName::Minio => start_minio_service()?,
        ServiceName::MinioAz1 => start_minio_az1_service()?,
        ServiceName::MinioAz2 => start_minio_az2_service()?,
        ServiceName::DdbLocal => start_ddb_local_service()?,
        ServiceName::Mirrord => start_mirrord_service()?,
    }
    Ok(())
}

pub fn start_bss_service() -> CmdResult {
    run_cmd!(systemctl --user start bss.service)?;
    wait_for_service_ready(ServiceName::Bss, 15)?;

    let bss_server_pid = run_fun!(pidof bss_server)?;
    check_pids(ServiceName::Bss, &bss_server_pid)?;
    info!("bss server (pid={bss_server_pid}) started");
    Ok(())
}

pub fn start_nss_service() -> CmdResult {
    // Start minio to simulate local s3 service if not running
    if run_cmd!(systemctl --user is-active --quiet minio.service).is_err() {
        start_minio_service()?;
    }

    run_cmd!(systemctl --user start nss.service)?;
    wait_for_service_ready(ServiceName::Nss, 15)?;

    let nss_server_pid = run_fun!(pidof nss_server)?;
    check_pids(ServiceName::Nss, &nss_server_pid)?;
    info!("nss server (pid={nss_server_pid}) started");
    Ok(())
}

pub fn start_mirrord_service() -> CmdResult {
    run_cmd!(systemctl --user start mirrord.service)?;
    wait_for_service_ready(ServiceName::Mirrord, 15)?;

    let mirrord_pid = run_fun!(pidof mirrord)?;
    check_pids(ServiceName::Mirrord, &mirrord_pid)?;
    info!("nss server (pid={mirrord_pid}) started");
    Ok(())
}

pub fn start_nss_role_agent_service(service_name: ServiceName) -> CmdResult {
    let service_file = match service_name {
        ServiceName::NssRoleAgentA => "nss_role_agent_a.service",
        ServiceName::NssRoleAgentB => "nss_role_agent_b.service",
        _ => panic!("Invalid service for nss_role_agent"),
    };

    run_cmd!(systemctl --user start $service_file)?;
    wait_for_service_ready(service_name, 30)?;

    // For role agents, get the PID from systemd instead of pidof to avoid conflicts
    let pid_output = run_fun!(systemctl --user show --property=MainPID --value $service_file)?;
    let server_pid = pid_output.trim();
    info!("{service_file} (pid={server_pid}) started");
    Ok(())
}

pub fn start_rss_service() -> CmdResult {
    // Start ddb_local service at first if needed, since root server stores information in ddb_local
    if run_cmd!(systemctl --user is-active --quiet ddb_local.service).is_err() {
        start_ddb_local_service()?;
    }

    run_cmd!(systemctl --user start rss.service)?;
    wait_for_service_ready(ServiceName::Rss, 30)?;

    let rss_server_pid = run_fun!(pidof root_server)?;
    check_pids(ServiceName::Rss, &rss_server_pid)?;
    info!("root server (pid={rss_server_pid}) started");
    Ok(())
}

pub fn start_ddb_local_service() -> CmdResult {
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

fn start_minio_service_common(
    service_enum: ServiceName,
    port: u16,
    data_dir: &str,
    bucket_name: &str,
) -> CmdResult {
    let pwd = run_fun!(pwd)?;
    let service_name = service_enum.as_ref();

    let service_file_content = format!(
        r##"[Unit]
Description={service_name}

[Install]
WantedBy=default.target

[Service]
Type=simple
Environment="MINIO_REGION=localdev"
ExecStart=/home/linuxbrew/.linuxbrew/opt/minio/bin/minio server --address :{port} {data_dir}/
Restart=always
WorkingDirectory={pwd}/data
"##
    );
    let minio_url = format!("http://localhost:{port}");

    let service_file = format!("{service_name}.service");
    run_cmd! {
        mkdir -p etc;
        echo $service_file_content > etc/$service_file;
        info "Linking etc/$service_file into ~/.config/systemd/user";
        systemctl --user link etc/$service_file --force --quiet;
        systemctl --user start $service_file;
    }?;
    wait_for_service_ready(service_enum, 10)?;

    let bucket = format!("s3://{bucket_name}");
    run_cmd! {
        info "Creating s3 bucket (\"$bucket_name\") in $service_name ...";
        ignore AWS_DEFAULT_REGION=localdev AWS_ENDPOINT_URL_S3=$minio_url AWS_ACCESS_KEY_ID=minioadmin AWS_SECRET_ACCESS_KEY=minioadmin
            aws s3 mb $bucket --region localdev &>/dev/null;
    }?;

    let mut wait_new_bucket_secs = 0;
    const TIMEOUT_SECS: i32 = 5;
    loop {
        let bucket_ready = run_cmd! (
            AWS_DEFAULT_REGION=localdev AWS_ENDPOINT_URL_S3=$minio_url AWS_ACCESS_KEY_ID=minioadmin AWS_SECRET_ACCESS_KEY=minioadmin
            aws s3api head-bucket --bucket $bucket_name --region localdev &>/dev/null
        ).is_ok();

        if bucket_ready {
            break;
        }

        wait_new_bucket_secs += 1;
        if wait_new_bucket_secs >= TIMEOUT_SECS {
            cmd_die!("timeout waiting for newly created bucket ${bucket_name}");
        }

        info!("waiting for newly created bucket {bucket_name}: {wait_new_bucket_secs}s");
        std::thread::sleep(std::time::Duration::from_secs(1));
    }
    Ok(())
}

pub fn start_minio_service() -> CmdResult {
    start_minio_service_common(ServiceName::Minio, 9000, "s3", "fractalbits-bucket")
}

pub fn start_minio_az1_service() -> CmdResult {
    start_minio_service_common(
        ServiceName::MinioAz1,
        9001,
        "s3-localdev-az1",
        "fractalbits-localdev-az1-data-bucket",
    )
}

pub fn start_minio_az2_service() -> CmdResult {
    start_minio_service_common(
        ServiceName::MinioAz2,
        9002,
        "s3-localdev-az2",
        "fractalbits-localdev-az2-data-bucket",
    )
}

pub fn start_api_server() -> CmdResult {
    run_cmd!(systemctl --user start api_server.service)?;
    wait_for_service_ready(ServiceName::ApiServer, 10)?;

    let api_server_pid = run_fun!(pidof api_server)?;
    check_pids(ServiceName::ApiServer, &api_server_pid)?;
    info!("api server (pid={api_server_pid}) started");

    // Register local api_server with service discovery
    register_local_api_server()?;

    Ok(())
}

fn start_all_services() -> CmdResult {
    info!("Starting all services with systemd dependency management");

    // Start supporting services first
    info!("Starting supporting services (ddb_local, minio instances)");
    start_ddb_local_service()?;
    start_minio_service()?; // Original minio for NSS metadata (port 9000)
    if run_cmd!(grep -q multi_az etc/api_server.service).is_ok() {
        start_minio_az1_service()?; // Local AZ data blobs (port 9001)
        start_minio_az2_service()?; // Remote AZ data blobs (port 9002)
        wait_for_service_ready(ServiceName::MinioAz1, 15)?;
        wait_for_service_ready(ServiceName::MinioAz2, 15)?;
    }
    wait_for_service_ready(ServiceName::DdbLocal, 15)?;
    wait_for_service_ready(ServiceName::Minio, 15)?;

    // Start all main services - systemd dependencies will handle ordering
    if run_cmd!(grep -q single_az etc/api_server.service).is_ok() {
        info!("Starting single_az services (systemd will handle dependency ordering)");
        run_cmd!(systemctl --user start rss.service bss.service nss_role_agent_a.service api_server.service)?;

        // Wait for all services to be ready in dependency order
        wait_for_service_ready(ServiceName::Rss, 30)?;
        wait_for_service_ready(ServiceName::Bss, 15)?;
        wait_for_service_ready(ServiceName::NssRoleAgentA, 30)?;
        wait_for_service_ready(ServiceName::ApiServer, 15)?;
    } else {
        info!("Starting multi_az services (skipping BSS)");
        start_nss_role_agent_service(ServiceName::NssRoleAgentB)?;
        run_cmd!(systemctl --user start rss.service nss_role_agent_a.service api_server.service)?;
        wait_for_service_ready(ServiceName::Rss, 30)?;
        wait_for_service_ready(ServiceName::NssRoleAgentA, 30)?;
        wait_for_service_ready(ServiceName::ApiServer, 15)?;
    }

    info!("All services started successfully!");
    Ok(())
}

fn create_systemd_unit_file(service: ServiceName, build_mode: BuildMode) -> CmdResult {
    create_systemd_unit_file_impl(service, build_mode, None)
}

fn create_systemd_unit_file_with_backend(
    service: ServiceName,
    build_mode: BuildMode,
    data_blob_storage: DataBlobStorage,
) -> CmdResult {
    create_systemd_unit_file_impl(service, build_mode, Some(data_blob_storage))
}

fn create_systemd_unit_files_for_init(
    service: ServiceName,
    build_mode: BuildMode,
    for_gui: bool,
    data_blob_storage: DataBlobStorage,
) -> CmdResult {
    match service {
        ServiceName::ApiServer => {
            if for_gui {
                create_systemd_unit_file_with_backend(
                    ServiceName::GuiServer,
                    build_mode,
                    data_blob_storage,
                )?;
            } else {
                create_systemd_unit_file_with_backend(
                    ServiceName::ApiServer,
                    build_mode,
                    data_blob_storage,
                )?;
            }
        }
        ServiceName::GuiServer => {
            create_systemd_unit_file_with_backend(
                ServiceName::GuiServer,
                build_mode,
                data_blob_storage,
            )?;
        }
        ServiceName::Bss => {
            create_systemd_unit_file(ServiceName::Bss, build_mode)?;
        }
        ServiceName::Nss => {
            create_systemd_unit_file(ServiceName::Nss, build_mode)?;
        }
        ServiceName::NssRoleAgentA => {
            create_systemd_unit_file(ServiceName::NssRoleAgentA, build_mode)?;
        }
        ServiceName::NssRoleAgentB => {
            create_systemd_unit_file(ServiceName::NssRoleAgentB, build_mode)?;
        }
        ServiceName::Mirrord => {
            create_systemd_unit_file(ServiceName::Mirrord, build_mode)?;
        }
        ServiceName::Rss => {
            create_systemd_unit_file(ServiceName::Rss, build_mode)?;
        }
        ServiceName::DdbLocal => {
            // DdbLocal creates its systemd unit file in start_ddb_local_service
        }
        ServiceName::Minio | ServiceName::MinioAz1 | ServiceName::MinioAz2 => {
            // Minio services create their systemd unit files in their respective start functions
        }
        ServiceName::DataBlobResyncServer => {
            // DataBlobResyncServer is a CLI tool, not a service
        }
        ServiceName::All => {
            create_systemd_unit_file(ServiceName::Rss, build_mode)?;

            // Only create BSS systemd unit file if we're in hybrid mode
            match data_blob_storage {
                DataBlobStorage::HybridSingleAz => {
                    create_systemd_unit_file(ServiceName::Bss, build_mode)?;
                }
                _ => {
                    info!(
                        "Skipping BSS systemd unit file creation in {} mode",
                        data_blob_storage
                    );
                }
            }

            create_systemd_unit_file(ServiceName::NssRoleAgentA, build_mode)?;
            create_systemd_unit_file(ServiceName::Nss, build_mode)?;
            create_systemd_unit_file(ServiceName::NssRoleAgentB, build_mode)?;
            create_systemd_unit_file(ServiceName::Mirrord, build_mode)?;

            if for_gui {
                create_systemd_unit_file_with_backend(
                    ServiceName::GuiServer,
                    build_mode,
                    data_blob_storage,
                )?;
            } else {
                create_systemd_unit_file_with_backend(
                    ServiceName::ApiServer,
                    build_mode,
                    data_blob_storage,
                )?;
            }
        }
    }
    Ok(())
}

fn create_systemd_unit_file_impl(
    service: ServiceName,
    build_mode: BuildMode,
    data_blob_storage: Option<DataBlobStorage>,
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
    let exec_start = match service {
        ServiceName::Bss => format!("{pwd}/zig-out/bin/bss_server"),
        ServiceName::Nss => match build_mode {
            BuildMode::Debug => format!("{pwd}/zig-out/bin/nss_server serve"),
            BuildMode::Release => {
                format!("{pwd}/zig-out/bin/nss_server serve")
            }
        },
        ServiceName::Mirrord => format!("{pwd}/zig-out/bin/mirrord"),
        ServiceName::NssRoleAgentA => {
            env_settings += env_rust_log(build_mode);
            env_settings += "\nEnvironment=\"INSTANCE_ID=nss-A\"";
            format!("{pwd}/target/{build}/nss_role_agent")
        }
        ServiceName::NssRoleAgentB => {
            env_settings += env_rust_log(build_mode);
            env_settings += "\nEnvironment=\"INSTANCE_ID=nss-B\"";
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
            format!("{pwd}/target/{build}/root_server")
        }
        ServiceName::ApiServer => {
            env_settings += env_rust_log(build_mode);

            // Add APP_BLOB_STORAGE_BACKEND environment variable if provided
            if let Some(backend) = data_blob_storage {
                env_settings += &format!(
                    "\nEnvironment=\"APP_BLOB_STORAGE_BACKEND={}\"",
                    backend.as_ref()
                );
            }
            format!("{pwd}/target/{build}/api_server")
        }
        ServiceName::GuiServer => {
            env_settings += env_rust_log(build_mode);

            // Add APP_BLOB_STORAGE_BACKEND environment variable if provided
            if let Some(backend) = data_blob_storage {
                env_settings += &format!(
                    "\nEnvironment=\"APP_BLOB_STORAGE_BACKEND={}\"",
                    backend.as_ref()
                );
            }

            env_settings += r##"
Environment="GUI_WEB_ROOT=ui/dist""##;
            format!("{pwd}/target/{build}/api_server")
        }
        _ => unreachable!(),
    };
    let working_dir = run_fun!(realpath $pwd)?;

    // Add systemd dependencies based on service type
    let dependencies = match service {
        ServiceName::NssRoleAgentA | ServiceName::NssRoleAgentB => {
            "After=rss.service\nWants=rss.service\n"
        }
        ServiceName::Rss => "After=ddb_local.service\nWants=ddb_local.service\n",
        ServiceName::Nss => "After=minio.service\nWants=minio.service\n",
        ServiceName::ApiServer | ServiceName::GuiServer => {
            "After=rss.service nss.service\nWants=rss.service nss.service\n"
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
        ServiceName::GuiServer => "api_server.service".to_string(),
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
        cmd_die!("Multiple processes were found: ${pids}");
    }
    Ok(())
}

fn create_dirs_for_nss_server() -> CmdResult {
    info!("Creating necessary directories for nss_server");
    create_nss_dirs("nss-A")
}

fn create_dirs_for_mirrord_server() -> CmdResult {
    info!("Creating necessary directories for mirrord");
    create_nss_dirs("nss-B")
}

fn create_nss_dirs(dir_name: &str) -> CmdResult {
    run_cmd! {
        mkdir -p data/logs;
        mkdir -p data/$dir_name/ebs;
        mkdir -p data/$dir_name/local/stats;
        mkdir -p data/$dir_name/local/meta_cache/blobs;
    }?;
    for i in 0..256 {
        run_cmd!(mkdir -p data/$dir_name/local/meta_cache/blobs/dir$i)?;
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

// Test instance management for leader election tests
pub fn start_test_root_server_instance(
    instance_id: &str,
    server_port: u16,
    health_port: u16,
    metrics_port: u16,
    table_name: &str,
    log_path: &str,
) -> Result<cmd_lib::CmdChildren, std::io::Error> {
    info!("Starting test root_server instance: {instance_id}");

    let proc = spawn! {
        RUST_LOG=info,root_server=debug
        AWS_ACCESS_KEY_ID=fakeMyKeyId
        AWS_SECRET_ACCESS_KEY=fakeSecretAccessKey
        INSTANCE_ID=$instance_id
        RSS_SERVER_PORT=$server_port
        RSS_HEALTH_PORT=$health_port
        RSS_METRICS_PORT=$metrics_port
        LEADER_TABLE_NAME=$table_name
        LEADER_KEY=test-leader
        LEADER_LEASE_DURATION=20
        ./target/debug/root_server |& ts -m "%b %d %H:%M:%.S" > $log_path
    }?;

    // Give the instance a moment to start
    std::thread::sleep(std::time::Duration::from_secs(2));

    Ok(proc)
}

pub fn cleanup_test_root_server_instances() -> CmdResult {
    run_cmd!(ignore pkill root_server)?;
    Ok(())
}

pub fn wait_for_service_ready(service: ServiceName, timeout_secs: u32) -> CmdResult {
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
                ServiceName::MinioAz1 => check_port_ready(9001),
                ServiceName::MinioAz2 => check_port_ready(9002),
                ServiceName::Rss => check_port_ready(8086),
                ServiceName::Bss => check_port_ready(8088),
                ServiceName::Nss => check_port_ready(8087),
                ServiceName::Mirrord => check_port_ready(9999),
                ServiceName::ApiServer | ServiceName::GuiServer => {
                    // Check both HTTP and HTTPS ports for API server
                    check_port_ready(8080) && check_port_ready(8443)
                }
                ServiceName::NssRoleAgentA => check_port_ready(8087), // Check managed nss_server
                ServiceName::NssRoleAgentB => check_port_ready(9999), // check managed mirrord
                ServiceName::DataBlobResyncServer => true,            // CLI tool, not a service
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

fn register_local_api_server() -> CmdResult {
    info!("Registering local api_server with service discovery");

    // Create the JSON item for DynamoDB
    let item_json = r#"{
        "service_id": {"S": "api-server"},
        "instances": {
            "M": {
                "local-dev": {"S": "127.0.0.1:8080"}
            }
        }
    }"#;

    // Try to update existing item first, if it doesn't exist, create it
    let key_json = "{\"service_id\": {\"S\": \"api-server\"}}";
    let attr_names = "{\"#instances\": \"instances\", \"#local\": \"local-dev\"}";
    let attr_values = "{\":ip\": {\"S\": \"127.0.0.1:8080\"}}";

    if run_cmd!(
        AWS_DEFAULT_REGION=fakeRegion
        AWS_ACCESS_KEY_ID=fakeMyKeyId
        AWS_SECRET_ACCESS_KEY=fakeSecretAccessKey
        AWS_ENDPOINT_URL_DYNAMODB="http://localhost:8000"
        aws dynamodb update-item
            --table-name fractalbits-service-discovery
            --key $key_json
            --update-expression "SET #instances.#local = :ip"
            --expression-attribute-names $attr_names
            --expression-attribute-values $attr_values
            --condition-expression "attribute_exists(service_id)" 2>/dev/null
    )
    .is_err()
    {
        // Item doesn't exist, create it
        run_cmd!(
            AWS_DEFAULT_REGION=fakeRegion
            AWS_ACCESS_KEY_ID=fakeMyKeyId
            AWS_SECRET_ACCESS_KEY=fakeSecretAccessKey
            AWS_ENDPOINT_URL_DYNAMODB="http://localhost:8000"
            aws dynamodb put-item
                --table-name fractalbits-service-discovery
                --item $item_json
        )?;
    }

    info!("Local api_server registered in service discovery");
    Ok(())
}

fn generate_https_certificates() -> CmdResult {
    info!("Generating HTTPS certificates for local development");

    // Check if certificates already exist
    if run_cmd!(test -f etc/cert.pem).is_ok() && run_cmd!(test -f etc/key.pem).is_ok() {
        info!("Certificates already exist, skipping generation");
        return Ok(());
    }

    run_cmd! {
        info "Running mkcert for trusted local certificates...";
        mkcert -install;
        mkdir -p etc;
        mkcert -key-file etc/key.pem -cert-file etc/cert.pem 127.0.0.1 localhost;
    }?;

    info!("HTTPS certificates generated successfully with mkcert:");
    info!("  Certificate: etc/cert.pem (trusted by system)");
    info!("  Private key: etc/key.pem (unencrypted)");
    Ok(())
}
