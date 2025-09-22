use std::path::Path;
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
        ensure_dynamodb_local()?;
        run_cmd! {
            rm -f data/rss/shared-local-instance.db;
            mkdir -p data/rss;
        }?;
        start_service(ServiceName::DdbLocal)?;

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
            DataBlobStorage::S3HybridSingleAz => {
                r#"{"service_id":{"S":"nss_roles"},"states":{"M":{"nss-A":{"S":"solo"}}}}"#
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

        // Initialize BSS data volume group configuration in service-discovery table
        let bss_data_vg_config_json = r#"{
            "volumes": [
                {
                    "volume_id": 0,
                    "bss_nodes": [
                        {"node_id": "bss0", "ip": "127.0.0.1", "port": 8088},
                        {"node_id": "bss1", "ip": "127.0.0.1", "port": 8089},
                        {"node_id": "bss2", "ip": "127.0.0.1", "port": 8090}
                    ]
                },
                {
                    "volume_id": 1,
                    "bss_nodes": [
                        {"node_id": "bss3", "ip": "127.0.0.1", "port": 8091},
                        {"node_id": "bss4", "ip": "127.0.0.1", "port": 8092},
                        {"node_id": "bss5", "ip": "127.0.0.1", "port": 8093}
                    ]
                }
            ],
            "quorum": {
                "n": 3,
                "r": 2,
                "w": 2
            }
        }"#;
        let bss_data_vg_config_item = format!(
            r#"{{"service_id":{{"S":"bss-data-vg-config"}},"value":{{"S":"{}"}}}}"#,
            bss_data_vg_config_json
                .replace('"', r#"\""#)
                .replace('\n', "")
        );

        run_cmd! {
            info "Initializing BSS data volume group configuration in service-discovery table ...";
            AWS_DEFAULT_REGION=fakeRegion
            AWS_ACCESS_KEY_ID=fakeMyKeyId
            AWS_SECRET_ACCESS_KEY=fakeSecretAccessKey
            AWS_ENDPOINT_URL_DYNAMODB="http://localhost:8000"
            aws dynamodb put-item
                --table-name $SERVICE_DISCOVERY_TABLE
                --item $bss_data_vg_config_item >/dev/null;
        }?;

        // Initialize BSS metadata volume group configuration in service-discovery table
        let bss_metadata_vg_config_json = r#"{
            "volumes": [
                {
                    "volume_id": 0,
                    "bss_nodes": [
                        {"node_id": "bss0", "ip": "127.0.0.1", "port": 8088},
                        {"node_id": "bss1", "ip": "127.0.0.1", "port": 8089},
                        {"node_id": "bss2", "ip": "127.0.0.1", "port": 8090},
                        {"node_id": "bss3", "ip": "127.0.0.1", "port": 8091},
                        {"node_id": "bss4", "ip": "127.0.0.1", "port": 8092},
                        {"node_id": "bss5", "ip": "127.0.0.1", "port": 8093}
                    ]
                }
            ],
            "quorum": {
                "n": 6,
                "r": 4,
                "w": 4
            }
        }"#;
        let bss_metadata_vg_config_item = format!(
            r#"{{"service_id":{{"S":"bss-metadata-vg-config"}},"value":{{"S":"{}"}}}}"#,
            bss_metadata_vg_config_json
                .replace('"', r#"\""#)
                .replace('\n', "")
        );

        run_cmd! {
            info "Initializing BSS metadata volume group configuration in service-discovery table ...";
            AWS_DEFAULT_REGION=fakeRegion
            AWS_ACCESS_KEY_ID=fakeMyKeyId
            AWS_SECRET_ACCESS_KEY=fakeSecretAccessKey
            AWS_ENDPOINT_URL_DYNAMODB="http://localhost:8000"
            aws dynamodb put-item
                --table-name $SERVICE_DISCOVERY_TABLE
                --item $bss_metadata_vg_config_item >/dev/null;
        }?;

        Ok(())
    };
    let init_rss = || -> CmdResult {
        // Start ddb_local service at first if needed, since root server stores infomation in ddb_local
        if run_cmd!(systemctl --user is-active --quiet ddb_local.service).is_err() {
            init_ddb_local()?;
        }

        // Start RSS service since admin now connects via RPC
        start_service(ServiceName::Rss)?;

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
        create_dirs_for_nss_server()?;
        match build_mode {
            BuildMode::Debug => run_cmd! {
                info "formatting nss_server with default configs";
                ${pwd}/$ZIG_DEBUG_OUT/bin/nss_server format --init_test_tree
                    |& ts -m $TS_FMT >$format_log;
            }?,
            BuildMode::Release => run_cmd! {
                info "formatting nss_server for benchmarking";
                ${pwd}/$ZIG_DEBUG_OUT/bin/nss_server format --init_test_tree
                    |& ts -m $TS_FMT >$format_log;
            }?,
        }
        Ok(())
    };
    let init_minio = || run_cmd!(mkdir -p data/s3);
    let init_minio_dev_az1 = || run_cmd!(mkdir -p data/s3-localdev-az1);
    let init_minio_dev_az2 = || run_cmd!(mkdir -p data/s3-localdev-az2);
    let init_bss = |id: u32| create_dirs_for_bss_server(id);
    let init_mirrord = || -> CmdResult {
        let pwd = run_fun!(pwd)?;
        let format_log = "data/logs/format_mirrord.log";
        create_dirs_for_mirrord_server()?;
        match build_mode {
            BuildMode::Debug => run_cmd! {
                info "formatting mirrord with default configs";
                WORKING_DIR="./data/nss-B" ${pwd}/$ZIG_DEBUG_OUT/bin/nss_server format
                    |& ts -m $TS_FMT >$format_log;
            }?,
            BuildMode::Release => run_cmd! {
                info "formatting mirrord for benchmarking";
                WORKING_DIR="./data/nss-B" ${pwd}/$ZIG_DEBUG_OUT/bin/nss_server format
                    |& ts -m $TS_FMT >$format_log;
            }?,
        }
        Ok(())
    };

    match service {
        ServiceName::ApiServer | ServiceName::GuiServer => {
            if init_config.with_https {
                generate_https_certificates()?;
            }
        }
        ServiceName::DdbLocal => init_ddb_local()?,
        ServiceName::Minio => init_minio()?,
        ServiceName::MinioAz1 => init_minio_dev_az1()?,
        ServiceName::MinioAz2 => init_minio_dev_az2()?,
        ServiceName::Bss0
        | ServiceName::Bss1
        | ServiceName::Bss2
        | ServiceName::Bss3
        | ServiceName::Bss4
        | ServiceName::Bss5 => {
            if let Some(id) = service.bss_id() {
                init_bss(id)?;
            }
        }
        ServiceName::Rss => init_rss()?,
        ServiceName::Nss => init_nss()?,
        ServiceName::NssRoleAgentA | ServiceName::NssRoleAgentB => {}
        ServiceName::Mirrord => init_mirrord()?,
        ServiceName::All => {
            if init_config.with_https {
                generate_https_certificates()?;
            }
            init_rss()?;
            for bss_service in ServiceName::all_bss_services() {
                if let Some(id) = bss_service.bss_id() {
                    init_bss(id)?;
                }
            }
            init_nss()?;
            init_mirrord()?;
            init_minio()?;
            init_minio_dev_az1()?;
            init_minio_dev_az2()?;
        }
    }

    run_cmd! {
        info "systemctl daemon-reload";
        systemctl --user daemon-reload;
        systemctl --user reset-failed;
    }?;

    Ok(())
}

fn ensure_dynamodb_local() -> CmdResult {
    let dynamodb_file = "dynamodb_local_latest.tar.gz";
    let dynamodb_path = format!("third_party/{dynamodb_file}");
    let dynamodb_dir = "third_party/dynamodb_local";

    // Check if DynamoDB Local is already extracted and ready
    if Path::new(&format!("{dynamodb_dir}/DynamoDBLocal.jar")).exists() {
        return Ok(());
    }

    let download_url = "https://d1ni2b6xgvw0s0.cloudfront.net/v2.x/dynamodb_local_latest.tar.gz";

    // Check if already downloaded
    if !Path::new(&dynamodb_path).exists() {
        run_cmd! {
            info "Downloading DynamoDB Local...";
            curl -sL -o $dynamodb_path $download_url;
        }?;
    }

    run_cmd! {
        cd third_party;
        info "Extracting DynamoDB Local...";
        mkdir -p dynamodb_local;
        cd dynamodb_local;
        tar -xzf ../$dynamodb_file;
    }?;

    Ok(())
}

pub fn stop_service(service: ServiceName) -> CmdResult {
    let services: Vec<ServiceName> = match service {
        ServiceName::All => all_services(get_data_blob_storage_setting()),
        single_service => vec![single_service],
    };

    info!(
        "Killing previous {} {} (if any) ...",
        if services.len() > 1 {
            "services"
        } else {
            "service"
        },
        service.as_ref()
    );
    for service in services {
        let service_name = service.as_ref();
        if run_cmd!(systemctl --user is-active --quiet $service_name.service).is_err() {
            continue;
        }

        if service == ServiceName::Mirrord {
            while run_cmd!(systemctl --user is-active --quiet nss.service).is_ok() {
                // waiting for nss to stop at first, or it may crash nss due to journal mirroring failure
                std::thread::sleep(Duration::from_secs(1));
            }
        }
        run_cmd!(systemctl --user stop $service_name.service)?;

        // make sure the process is really killed
        if run_cmd!(systemctl --user is-active --quiet $service_name.service).is_ok() {
            cmd_die!("Failed to stop $service_name: service is still running");
        }
    }

    Ok(())
}

fn all_services(data_blob_storage: DataBlobStorage) -> Vec<ServiceName> {
    match data_blob_storage {
        DataBlobStorage::S3HybridSingleAz => {
            let mut services = vec![
                ServiceName::ApiServer,
                ServiceName::NssRoleAgentA,
                ServiceName::Nss,
            ];
            services.extend(ServiceName::all_bss_services());
            services.extend(vec![
                ServiceName::Rss,
                ServiceName::DdbLocal,
                ServiceName::Minio,
            ]);
            services
        }
        DataBlobStorage::S3ExpressMultiAz => vec![
            ServiceName::ApiServer,
            ServiceName::NssRoleAgentA,
            ServiceName::Nss,
            ServiceName::NssRoleAgentB,
            ServiceName::Mirrord,
            ServiceName::Rss,
            ServiceName::DdbLocal,
            ServiceName::Minio,
            ServiceName::MinioAz1,
            ServiceName::MinioAz2,
        ],
    }
}

fn get_data_blob_storage_setting() -> DataBlobStorage {
    if run_cmd!(grep -q multi_az data/etc/api_server.service &>/dev/null).is_ok() {
        DataBlobStorage::S3ExpressMultiAz
    } else {
        DataBlobStorage::S3HybridSingleAz
    }
}

pub fn show_service_status(service: ServiceName) -> CmdResult {
    match service {
        ServiceName::All => {
            println!("Service Status:");
            println!("─────────────────────────────────────");

            for svc in all_services(get_data_blob_storage_setting()) {
                let service_name = svc.as_ref();
                let status = if run_cmd!(systemctl --user list-unit-files --quiet $service_name.service | grep -q $service_name).is_ok() {
                    // Service exists, get its status
                    match run_fun!(systemctl --user is-active $service_name.service 2>/dev/null) {
                        Ok(output) => match output.trim() {
                            "active" => "active".green().to_string(),
                            status => status.yellow().to_string(),
                        },
                        Err(_) => {
                            // Command failed, try to get the actual status
                            if run_cmd!(systemctl --user is-failed --quiet $service_name.service).is_ok() {
                                "failed".red().to_string()
                            } else {
                                "inactive (dead)".bright_black().to_string()
                            }
                        }
                    }
                } else {
                    "not installed".bright_black().to_string()
                };

                println!("{service_name:<16}: {status}");
            }
        }
        single_service => {
            // Show detailed status for a single service
            let service_name = single_service.as_ref();
            run_cmd!(systemctl --user status $service_name.service --no-pager)?;
        }
    }

    Ok(())
}

pub fn start_service(service: ServiceName) -> CmdResult {
    match service {
        ServiceName::All => start_all_services()?,
        _ => {
            // Start the systemd service
            let service_name = service.as_ref();
            run_cmd!(systemctl --user start $service_name.service)?;

            // Wait for service to be ready
            wait_for_service_ready(service, 30)?;

            // Post-start actions
            match service {
                ServiceName::Minio => create_minio_bucket(9000, "fractalbits-bucket")?,
                ServiceName::MinioAz1 => {
                    create_minio_bucket(9001, "fractalbits-localdev-az1-data-bucket")?
                }
                ServiceName::MinioAz2 => {
                    create_minio_bucket(9002, "fractalbits-localdev-az2-data-bucket")?
                }
                ServiceName::ApiServer | ServiceName::GuiServer => register_local_api_server()?,
                _ => {}
            }

            info!("{service_name} service started successfully");
        }
    }
    Ok(())
}

fn create_minio_bucket(port: u16, bucket_name: &str) -> CmdResult {
    let minio_url = format!("http://localhost:{port}");
    let bucket = format!("s3://{bucket_name}");

    run_cmd! {
        info "Creating s3 bucket (\"$bucket_name\") ...";
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

fn start_all_services() -> CmdResult {
    info!("Starting all services with systemd dependency management");

    // Start supporting services first
    info!("Starting supporting services (ddb_local, minio instances)");
    start_service(ServiceName::DdbLocal)?;
    start_service(ServiceName::Minio)?; // Original minio for NSS metadata (port 9000)
    if run_cmd!(grep -q multi_az data/etc/api_server.service &>/dev/null).is_ok() {
        start_service(ServiceName::MinioAz1)?; // Local AZ data blobs (port 9001)
        start_service(ServiceName::MinioAz2)?; // Remote AZ data blobs (port 9002)
    }

    // Start all main services - systemd dependencies will handle ordering
    if run_cmd!(grep -q single_az data/etc/api_server.service).is_ok() {
        info!("Starting single_az services");
        start_service(ServiceName::Rss)?;
        for bss_service in ServiceName::all_bss_services() {
            start_service(bss_service)?;
        }
        start_service(ServiceName::NssRoleAgentA)?;
        start_service(ServiceName::ApiServer)?;
    } else {
        info!("Starting multi_az services (skipping BSS)");
        start_service(ServiceName::NssRoleAgentB)?;
        start_service(ServiceName::Rss)?;
        start_service(ServiceName::NssRoleAgentA)?;
        start_service(ServiceName::ApiServer)?;
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
    let api_server_service = if for_gui {
        ServiceName::GuiServer
    } else {
        ServiceName::ApiServer
    };
    match service {
        ServiceName::ApiServer | ServiceName::GuiServer => {
            create_systemd_unit_file_with_backend(
                api_server_service,
                build_mode,
                data_blob_storage,
            )?;
        }
        ServiceName::Bss0
        | ServiceName::Bss1
        | ServiceName::Bss2
        | ServiceName::Bss3
        | ServiceName::Bss4
        | ServiceName::Bss5 => {
            create_systemd_unit_file(service, build_mode)?;
        }
        ServiceName::Nss
        | ServiceName::NssRoleAgentA
        | ServiceName::NssRoleAgentB
        | ServiceName::Mirrord
        | ServiceName::Rss
        | ServiceName::DdbLocal
        | ServiceName::Minio
        | ServiceName::MinioAz1
        | ServiceName::MinioAz2 => {
            create_systemd_unit_file(service, build_mode)?;
        }
        ServiceName::All => {
            create_systemd_unit_file(ServiceName::DdbLocal, build_mode)?;
            create_systemd_unit_file(ServiceName::Minio, build_mode)?;
            create_systemd_unit_file(ServiceName::MinioAz1, build_mode)?;
            create_systemd_unit_file(ServiceName::MinioAz2, build_mode)?;
            create_systemd_unit_file(ServiceName::Rss, build_mode)?;
            for bss_service in ServiceName::all_bss_services() {
                create_systemd_unit_file(bss_service, build_mode)?;
            }
            create_systemd_unit_file(ServiceName::NssRoleAgentA, build_mode)?;
            create_systemd_unit_file(ServiceName::Nss, build_mode)?;
            create_systemd_unit_file(ServiceName::NssRoleAgentB, build_mode)?;
            create_systemd_unit_file(ServiceName::Mirrord, build_mode)?;
            create_systemd_unit_file_with_backend(
                api_server_service,
                build_mode,
                data_blob_storage,
            )?;
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
    let minio_bin = run_fun!(bash -c "command -v minio")?;
    let exec_start = match service {
        ServiceName::Bss0
        | ServiceName::Bss1
        | ServiceName::Bss2
        | ServiceName::Bss3
        | ServiceName::Bss4
        | ServiceName::Bss5 => {
            if let Some(id) = service.bss_id() {
                let port = 8088 + id;
                env_settings += &format!(
                    r##"
Environment="BSS_WORKING_DIR=./data/bss{id}"
Environment="BSS_PORT={port}""##
                );
                format!("{pwd}/{ZIG_DEBUG_OUT}/bin/bss_server")
            } else {
                unreachable!("BSS service should have valid ID")
            }
        }
        ServiceName::Nss => match build_mode {
            BuildMode::Debug => format!("{pwd}/{ZIG_DEBUG_OUT}/bin/nss_server serve"),
            BuildMode::Release => {
                format!("{pwd}/{ZIG_DEBUG_OUT}/bin/nss_server serve")
            }
        },
        ServiceName::Mirrord => format!("{pwd}/{ZIG_DEBUG_OUT}/bin/mirrord"),
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
        ServiceName::DdbLocal => {
            let java = run_fun!(bash -c "command -v java")?;
            let java_lib = format!("{pwd}/third_party/dynamodb_local/DynamoDBLocal_lib");
            format!(
                "{java} -Djava.library.path={java_lib} -jar {java_lib}/../DynamoDBLocal.jar -sharedDb -dbPath ./rss"
            )
        }
        ServiceName::Minio => {
            env_settings = r##"
Environment="MINIO_REGION=localdev""##
                .to_string();
            format!("{minio_bin} server --address :9000 data/s3/")
        }
        ServiceName::MinioAz1 => {
            env_settings = r##"
Environment="MINIO_REGION=localdev""##
                .to_string();
            format!("{minio_bin} server --address :9001 data/s3-localdev-az1/")
        }
        ServiceName::MinioAz2 => {
            env_settings = r##"
Environment="MINIO_REGION=localdev""##
                .to_string();
            format!("{minio_bin} server --address :9002 data/s3-localdev-az2/")
        }
        _ => unreachable!(),
    };
    let working_dir = match service {
        ServiceName::DdbLocal => format!("{}/data", run_fun!(realpath $pwd)?),
        _ => run_fun!(realpath $pwd)?,
    };

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
        ServiceName::DdbLocal
        | ServiceName::Minio
        | ServiceName::MinioAz1
        | ServiceName::MinioAz2 => "",
        _ => "",
    };

    let systemd_unit_content = format!(
        r##"[Unit]
Description={service_name} Service
{dependencies}
StartLimitIntervalSec=600
StartLimitBurst=1
[Service]
LimitNOFILE=1000000
LimitCORE=infinity
WorkingDirectory={working_dir}{env_settings}
ExecStart={exec_start}
SuccessExitStatus=143

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
        mkdir -p data/etc;
        echo $systemd_unit_content > data/etc/$service_file;
        info "Linking ./data/etc/$service_file into ~/.config/systemd/user";
        systemctl --user link ./data/etc/$service_file --force --quiet;
    }?;
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

fn create_dirs_for_bss_server(bss_id: u32) -> CmdResult {
    info!("Creating necessary directories for bss{} server", bss_id);
    run_cmd! {
        mkdir -p data/bss$bss_id/local/stats;
        mkdir -p data/bss$bss_id/local/blobs;
    }?;

    // Data volume
    let data_volume_id = bss_id / 3;
    run_cmd!(mkdir -p data/bss$bss_id/local/blobs/data_volume$data_volume_id)?;
    for i in 0..256 {
        run_cmd!(mkdir -p data/bss$bss_id/local/blobs/data_volume$data_volume_id/dir$i)?;
    }

    // Metadata volume
    let metadata_volume_id = bss_id / 6;
    run_cmd!(mkdir -p data/bss$bss_id/local/blobs/metadata_volume$metadata_volume_id)?;
    for i in 0..256 {
        run_cmd!(mkdir -p data/bss$bss_id/local/blobs/metadata_volume$metadata_volume_id/dir$i)?;
    }

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
                ServiceName::Bss0
                | ServiceName::Bss1
                | ServiceName::Bss2
                | ServiceName::Bss3
                | ServiceName::Bss4
                | ServiceName::Bss5 => {
                    if let Some(id) = service.bss_id() {
                        check_port_ready((8088 + id) as u16)
                    } else {
                        false
                    }
                }
                ServiceName::Nss => check_port_ready(8087),
                ServiceName::Mirrord => check_port_ready(9999),
                ServiceName::ApiServer | ServiceName::GuiServer => check_port_ready(8080),
                ServiceName::NssRoleAgentA => check_port_ready(8087), // Check managed nss_server
                ServiceName::NssRoleAgentB => check_port_ready(9999), // check managed mirrord
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
    run_cmd!(nc -z localhost $port &>/dev/null).is_ok()
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
    if run_cmd!(test -f data/etc/cert.pem).is_ok() && run_cmd!(test -f data/etc/key.pem).is_ok() {
        info!("Certificates already exist, skipping generation");
        return Ok(());
    }

    run_cmd! {
        info "Running mkcert for trusted local certificates...";
        mkcert -install;
        mkdir -p data/etc;
        mkcert -key-file data/etc/key.pem -cert-file data/etc/cert.pem 127.0.0.1 localhost;
    }?;

    info!("HTTPS certificates generated successfully with mkcert:");
    info!("  Certificate: data/etc/cert.pem (trusted by system)");
    info!("  Private key: data/etc/key.pem (unencrypted)");
    Ok(())
}
