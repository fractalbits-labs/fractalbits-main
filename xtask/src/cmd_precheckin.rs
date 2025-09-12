use crate::*;

pub fn run_cmd_precheckin(
    s3_api_only: bool,
    zig_unit_tests_only: bool,
    debug_api_server: bool,
    with_art_tests: bool,
    data_blob_storage: DataBlobStorage,
) -> CmdResult {
    let working_dir = run_fun!(pwd)?;
    if debug_api_server {
        cmd_service::stop_service(ServiceName::ApiServer)?;
        run_cmd! {
            cargo build -p api_server;
        }?;
    } else {
        cmd_service::stop_service(ServiceName::All)?;
        cmd_build::build_rust_servers(BuildMode::Debug)?;
        cmd_build::build_zig_servers(BuildMode::Debug)?;
    }

    if s3_api_only {
        return run_s3_api_tests(debug_api_server, data_blob_storage);
    }

    if zig_unit_tests_only {
        return run_zig_unit_tests(data_blob_storage);
    }

    init_service_with_data_blob_storage(data_blob_storage)?;
    cmd_service::start_service(ServiceName::Minio)?;
    run_cmd! {
        info "Formatting nss_server";
        $working_dir/zig-out/bin/nss_server format;
    }?;
    run_cmd! {
        info "Running zig unit tests";
        cd ./core;
        zig build -p ../zig-out test --summary all 2>&1;
    }?;

    run_s3_api_tests(false, data_blob_storage)?;

    if with_art_tests {
        run_art_tests()?;
    }

    if let Ok(core_file) = run_fun!(ls data | grep ^core) {
        let core_files: Vec<&str> = core_file.split("\n").collect();
        cmd_die!("Found core file(s) in directory ./data: ${core_files:?}");
    }

    info!("Precheckin is OK");
    Ok(())
}

fn init_service_with_data_blob_storage(data_blob_storage: DataBlobStorage) -> CmdResult {
    cmd_service::init_service(
        ServiceName::All,
        BuildMode::Debug,
        InitConfig {
            for_gui: false,
            data_blob_storage,
        },
    )?;
    Ok(())
}

fn run_art_tests() -> CmdResult {
    let rand_log = "data/logs/test_art_random.log";
    let format_log = "data/logs/format.log";
    let ts = ["ts", "-m", TS_FMT];
    let working_dir = run_fun!(pwd)?;

    cmd_service::start_service(ServiceName::Minio)?;
    run_cmd! {
        mkdir -p data/logs;
        info "Running art tests (random) with log $rand_log";
        $working_dir/zig-out/bin/nss_server format |& $[ts] >$format_log;
        $working_dir/zig-out/bin/test_art --tests random
            --size 400000 --ops 1000000 --threads 20 |& $[ts] >$rand_log;
    }?;

    let fat_log = "data/logs/test_art_fat.log";
    run_cmd! {
        info "Running art tests (fat) with log $fat_log";
        $working_dir/zig-out/bin/nss_server format |& $[ts] >$format_log;
        $working_dir/zig-out/bin/test_art --tests fat --ops 1000000 |& $[ts] >$fat_log;
    }?;

    let async_art_log = "data/logs/test_async_art_rename.log";
    run_cmd! {
        info "Running async art rename tests with log $async_art_log";
        $working_dir/zig-out/bin/nss_server format --init_test_tree |& $[ts] >$format_log;
        $working_dir/zig-out/bin/test_async_art --prefill 100000 --tests rename
            --ops 10000 --parallelism 1000 --debug |& $[ts] >$async_art_log;
    }?;

    let async_art_log = "data/logs/test_async_art.log";
    run_cmd! {
        info "Running async art tests with log $async_art_log";
        $working_dir/zig-out/bin/nss_server format --init_test_tree |& $[ts] >$format_log;
        $working_dir/zig-out/bin/test_async_art -p 20 |& $[ts] >$async_art_log;
        $working_dir/zig-out/bin/test_async_art -p 20 |& $[ts] >>$async_art_log;
        $working_dir/zig-out/bin/test_async_art -p 20 |& $[ts] >>$async_art_log;
    }?;

    Ok(())
}

fn run_zig_unit_tests(data_blob_storage: DataBlobStorage) -> CmdResult {
    let working_dir = run_fun!(pwd)?;
    init_service_with_data_blob_storage(data_blob_storage)?;
    cmd_service::start_service(ServiceName::Minio)?;

    run_cmd! {
        info "Formatting nss_server";
        $working_dir/zig-out/bin/nss_server format;
    }?;

    run_cmd! {
        info "Running zig unit tests";
        cd ./core;
        zig build -p ../zig-out test --summary all 2>&1;
    }?;

    info!("Zig unit tests completed successfully");
    Ok(())
}

fn run_s3_api_tests(debug_api_server: bool, data_blob_storage: DataBlobStorage) -> CmdResult {
    if debug_api_server {
        cmd_service::start_service(ServiceName::ApiServer)?;
    } else {
        cmd_service::init_service(
            ServiceName::All,
            BuildMode::Debug,
            InitConfig {
                for_gui: false,
                data_blob_storage,
            },
        )?;
        cmd_service::start_service(ServiceName::All)?;
    }

    run_cmd! {
        info "Run cargo tests (s3 api tests)";
        cargo test --package api_server;
    }?;
    run_cmd! {
        info "Run cargo tests (s3 https api tests)";
        USE_HTTPS_ENDPOINT=true cargo test --package api_server;
    }?;

    if !debug_api_server {
        let _ = cmd_service::stop_service(ServiceName::All);
    }

    Ok(())
}
