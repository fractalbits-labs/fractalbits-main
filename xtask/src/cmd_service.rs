use crate::*;

pub fn run_cmd_service(
    service: ServiceName,
    action: ServiceAction,
    build_mode: BuildMode,
    for_gui: bool,
) -> CmdResult {
    match action {
        ServiceAction::Init => init_service(service, build_mode),
        ServiceAction::Stop => stop_service(service),
        ServiceAction::Start => start_services(service, build_mode, for_gui),
        ServiceAction::Restart => {
            stop_service(service)?;
            start_services(service, build_mode, for_gui)
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
        }
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
        let format_log = "data/format.log";
        let fbs_log = "data/fbs.log";
        create_dirs_for_nss_server()?;
        match build_mode {
            BuildMode::Debug => run_cmd! {
                cd data;
                info "formatting nss_server with default configs";
                ${pwd}/zig-out/bin/nss_server format
                    |& ts -m $TS_FMT >$format_log;
                ${pwd}/zig-out/bin/fbs --new_tree $TEST_BUCKET_ROOT_BLOB_NAME
                    |& ts -m $TS_FMT >$fbs_log;
            }?,
            BuildMode::Release => run_cmd! {
                cd data;
                info "formatting nss_server for benchmarking";
                ${pwd}/zig-out/bin/nss_server format -c ${pwd}/etc/$NSS_SERVER_BENCH_CONFIG
                    |& ts -m $TS_FMT >$format_log;
                ${pwd}/zig-out/bin/fbs --new_tree $TEST_BUCKET_ROOT_BLOB_NAME
                    -c ${pwd}/etc/$NSS_SERVER_BENCH_CONFIG |& ts -m $TS_FMT >$fbs_log;
            }?,
        }
        Ok(())
    };
    let init_minio = || run_cmd!(mkdir -p data/s3);
    let init_bss = || create_dirs_for_bss_server();

    match service {
        ServiceName::ApiServer => {}
        ServiceName::DdbLocal => init_ddb_local()?,
        ServiceName::Minio => init_minio()?,
        ServiceName::Bss => init_bss()?,
        ServiceName::Rss => init_rss()?,
        ServiceName::Nss => init_nss()?,
        ServiceName::All => {
            init_rss()?;
            init_bss()?;
            init_nss()?;
        }
    }
    Ok(())
}

pub fn stop_service(service: ServiceName) -> CmdResult {
    let services: Vec<String> = match service {
        ServiceName::All => vec![
            ServiceName::ApiServer.as_ref().to_owned(),
            ServiceName::Nss.as_ref().to_owned(),
            ServiceName::Bss.as_ref().to_owned(),
            ServiceName::Rss.as_ref().to_owned(),
            ServiceName::Minio.as_ref().to_owned(),
            ServiceName::DdbLocal.as_ref().to_owned(),
        ],
        single_service => vec![single_service.as_ref().to_owned()],
    };

    run_cmd! {
        info "Running sync command";
        sync
    }?;

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

pub fn start_services(service: ServiceName, build_mode: BuildMode, for_gui: bool) -> CmdResult {
    match service {
        ServiceName::Bss => start_bss_service(build_mode)?,
        ServiceName::Nss => start_nss_service(build_mode, false)?,
        ServiceName::Rss => start_rss_service(build_mode)?,
        ServiceName::ApiServer => start_api_server(build_mode, for_gui)?,
        ServiceName::All => {
            start_rss_service(build_mode)?;
            start_bss_service(build_mode)?;
            start_nss_service(build_mode, false)?;
            start_api_server(build_mode, for_gui)?;
        }
        ServiceName::Minio => start_minio_service()?,
        ServiceName::DdbLocal => start_ddb_local_service()?,
    }
    Ok(())
}

pub fn start_bss_service(build_mode: BuildMode) -> CmdResult {
    create_systemd_unit_file(ServiceName::Bss, build_mode, None)?;

    let bss_wait_secs = 10;
    run_cmd! {
        systemctl --user start bss.service;
        info "Waiting ${bss_wait_secs}s for bss server up";
        sleep $bss_wait_secs;
    }?;

    let bss_server_pid = run_fun!(pidof bss_server)?;
    check_pids(ServiceName::Bss, &bss_server_pid)?;
    info!("bss server (pid={bss_server_pid}) started");
    Ok(())
}

pub fn start_nss_service(build_mode: BuildMode, data_on_local: bool) -> CmdResult {
    if !data_on_local {
        // Start minio to simulate local s3 service
        if run_cmd!(systemctl --user is-active --quiet minio.service).is_err() {
            start_minio_service()?;
        }
    }

    let pwd = run_fun!(pwd)?;
    let config_file = match build_mode {
        BuildMode::Debug => None,
        BuildMode::Release => Some(format!("{pwd}/etc/{NSS_SERVER_BENCH_CONFIG}")),
    };
    create_systemd_unit_file(ServiceName::Nss, build_mode, config_file)?;

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
    // Start ddb_local service at first if needed, since root server stores infomation in ddb_local
    if run_cmd!(systemctl --user is-active --quiet ddb_local.service).is_err() {
        start_ddb_local_service()?;
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

    create_systemd_unit_file(ServiceName::Rss, build_mode, None)?;
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

#[allow(dead_code)]
fn start_etcd_service() -> CmdResult {
    let pwd = run_fun!(pwd)?;
    let working_dir = format!("{pwd}/data/rss");
    let service_file = "etc/etcd.service";
    let service_file_content = format!(
        r##"[Unit]
Description=etcd for root_server

[Install]
WantedBy=default.target

[Service]
Type=simple
ExecStart="/home/linuxbrew/.linuxbrew/opt/etcd/bin/etcd"
Restart=always
WorkingDirectory={pwd}/data/rss
"##
    );

    let etcd_wait_secs = 5;
    run_cmd! {
        mkdir -p etc;
        mkdir -p $working_dir;
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

    let ddb_local_wait_secs = 5;
    run_cmd! {
        mkdir -p $working_dir;
        mkdir -p etc;
        echo $service_file_content > $service_file;
        info "Linking $service_file into ~/.config/systemd/user";
        systemctl --user link $service_file --force --quiet;
        systemctl --user start ddb_local.service;
        info "Waiting ${ddb_local_wait_secs}s for ddb_local up";
        sleep $ddb_local_wait_secs;
        systemctl --user is-active --quiet ddb_local.service;
    }?;

    Ok(())
}

pub fn start_minio_service() -> CmdResult {
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
    let minio_wait_secs = 5;
    let minio_url = "http://localhost:9000";
    let bucket_name = "fractalbits-bucket";
    let my_bucket = format!("s3://{bucket_name}");
    run_cmd! {
        mkdir -p etc;
        echo $service_file_content > $service_file;
        info "Linking $service_file into ~/.config/systemd/user";
        systemctl --user link $service_file --force --quiet;
        systemctl --user start minio.service;
        info "Waiting ${minio_wait_secs}s for minio up";
        sleep $minio_wait_secs;
        info "Creating s3 bucket (\"$bucket_name\") in minio ...";
        ignore AWS_ENDPOINT_URL_S3=$minio_url AWS_ACCESS_KEY_ID=minioadmin AWS_SECRET_ACCESS_KEY=minioadmin
            aws s3 mb $my_bucket &>/dev/null;
    }?;

    let mut wait_new_buckets_secs = 0;
    const TIMEOUT_SECS: i32 = 5;
    loop {
        if run_cmd! (
            AWS_ENDPOINT_URL_S3=$minio_url AWS_ACCESS_KEY_ID=minioadmin AWS_SECRET_ACCESS_KEY=minioadmin
            aws s3api head-bucket --bucket $bucket_name &>/dev/null
        ).is_ok() {
            break;
        }

        wait_new_buckets_secs += 1;
        if wait_new_buckets_secs >= TIMEOUT_SECS {
            cmd_die!("timeout waiting for newly created bucket ${bucket_name}");
        }

        info!("waiting for newly created bucket {bucket_name}: {wait_new_buckets_secs}s");
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
Environment="RUST_LOG=info""##
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
    let working_dir = run_fun!(realpath $pwd/data)?;
    let systemd_unit_content = format!(
        r##"[Unit]
Description={service_name} Service

[Service]
LimitNOFILE=1000000
LimitCORE=infinity
WorkingDirectory={working_dir}{env_settings}
ExecStart={exec_start}

[Install]
WantedBy=multi-user.target
"##
    );
    let service_file = format!("{service_name}.service");

    run_cmd! {
        mkdir -p $pwd/data;
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
        mkdir -p data/ebs;
        mkdir -p data/local/meta_cache/blobs;
    }?;
    for i in 0..256 {
        run_cmd!(mkdir -p data/local/meta_cache/blobs/dir$i)?;
    }

    Ok(())
}

fn create_dirs_for_bss_server() -> CmdResult {
    info!("Creating necessary directories for bss_server");
    run_cmd! {
        mkdir -p data/bss;
    }?;
    for i in 0..256 {
        run_cmd!(mkdir -p data/bss/dir$i)?;
    }

    Ok(())
}
