use crate::*;

pub fn run_cmd_precheckin(
    data_blob_storage: DataBlobStorage,
    s3_api_only: bool,
    zig_unit_tests_only: bool,
    debug_api_server: bool,
    with_art_tests: bool,
    with_https: bool,
) -> CmdResult {
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
        return run_s3_api_tests(data_blob_storage, debug_api_server, with_https);
    }

    if zig_unit_tests_only {
        return run_zig_unit_tests(data_blob_storage, with_https);
    }

    init_service_with_data_blob_storage(data_blob_storage, with_https)?;
    cmd_build::run_zig_unit_tests()?;
    run_cmd! {
        info "Run cargo tests (except s3 api)";
        cargo test --workspace --exclude api_server;
    }?;

    run_s3_api_tests(data_blob_storage, false, with_https)?;

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

fn init_service_with_data_blob_storage(data_blob_storage: DataBlobStorage, with_https: bool) -> CmdResult {
    cmd_service::init_service(
        ServiceName::All,
        BuildMode::Debug,
        InitConfig {
            for_gui: false,
            data_blob_storage,
            with_https,
        },
    )?;
    Ok(())
}

fn run_art_tests() -> CmdResult {
    let rand_log = "data/logs/test_art_random.log";
    let format_log = "data/logs/format.log";
    let ts = ["ts", "-m", TS_FMT];
    let working_dir = run_fun!(pwd)?;
    let nss_server = format!("{working_dir}/{ZIG_DEBUG_OUT}/bin/nss_server");
    let test_art = format!("{working_dir}/{ZIG_DEBUG_OUT}/bin/test_art");
    let test_async_art = format!("{working_dir}/{ZIG_DEBUG_OUT}/bin/test_async_art");

    if !std::path::Path::new(&test_art).exists() {
        info!("Skipping art-tests");
        return Ok(());
    }

    for bss_service in ServiceName::all_bss_services() {
        cmd_service::start_service(bss_service)?;
    }
    run_cmd! {
        mkdir -p data/logs;
        info "Running art tests (random) with log $rand_log";
        $nss_server format |& $[ts] >$format_log;
        $test_art --tests random --size 400000 --ops 1000000 --threads 20 |& $[ts] >$rand_log;
    }?;

    let fat_log = "data/logs/test_art_fat.log";
    run_cmd! {
        info "Running art tests (fat) with log $fat_log";
        $nss_server format |& $[ts] >$format_log;
        $test_art --tests fat --ops 1000000 |& $[ts] >$fat_log;
    }?;

    let async_art_log = "data/logs/test_async_art_rename.log";
    run_cmd! {
        info "Running async art rename tests with log $async_art_log";
        $nss_server format --init_test_tree |& $[ts] >$format_log;
        $test_async_art --prefill 100000 --tests rename
            --ops 10000 --parallelism 1000 --debug |& $[ts] >$async_art_log;
    }?;

    let async_art_log = "data/logs/test_async_art.log";
    run_cmd! {
        info "Running async art tests with log $async_art_log";
        $nss_server format --init_test_tree |& $[ts] >$format_log;
        $test_async_art -p 20 |& $[ts] >$async_art_log;
        $test_async_art -p 20 |& $[ts] >>$async_art_log;
        $test_async_art -p 20 |& $[ts] >>$async_art_log;
    }?;

    for bss_service in ServiceName::all_bss_services() {
        cmd_service::stop_service(bss_service)?;
    }
    Ok(())
}

fn run_zig_unit_tests(data_blob_storage: DataBlobStorage, with_https: bool) -> CmdResult {
    init_service_with_data_blob_storage(data_blob_storage, with_https)?;
    cmd_build::run_zig_unit_tests()?;
    Ok(())
}

fn run_s3_api_tests(data_blob_storage: DataBlobStorage, debug_api_server: bool, with_https: bool) -> CmdResult {
    if debug_api_server {
        cmd_service::start_service(ServiceName::ApiServer)?;
    } else {
        cmd_service::init_service(
            ServiceName::All,
            BuildMode::Debug,
            InitConfig {
                for_gui: false,
                data_blob_storage,
                with_https,
            },
        )?;
        cmd_service::start_service(ServiceName::All)?;
    }

    run_cmd! {
        info "Run cargo tests (s3 api tests)";
        cargo test --package api_server;
    }?;

    if with_https {
        run_cmd! {
            info "Run cargo tests (s3 https api tests)";
            USE_HTTPS_ENDPOINT=true cargo test --package api_server;
        }?;
    }

    if !debug_api_server {
        let _ = cmd_service::stop_service(ServiceName::All);
    }

    Ok(())
}
