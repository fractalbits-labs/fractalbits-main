use crate::*;

pub fn run_cmd_precheckin(s3_api_only: bool) -> CmdResult {
    let working_dir = run_fun!(pwd)?;
    cmd_service::stop_service(ServiceName::All)?;
    cmd_build::build_rust_servers(BuildMode::Debug)?;
    cmd_build::build_zig_servers(BuildMode::Debug)?;

    if s3_api_only {
        return run_s3_api_tests();
    }

    cmd_service::init_service(ServiceName::All, BuildMode::Debug)?;
    cmd_service::start_minio_service()?;
    run_cmd! {
        info "Formatting nss_server";
        $working_dir/zig-out/bin/nss_server format;
    }?;
    run_cmd! {
        info "Running zig unit tests";
        zig build test --summary all 2>&1;
    }?;

    run_s3_api_tests()?;
    run_leader_election_tests()?;
    run_art_tests()?;

    if let Ok(core_file) = run_fun!(ls data | grep ^core) {
        let core_files: Vec<&str> = core_file.split("\n").collect();
        cmd_die!("Found core file(s) in directory ./data: ${core_files:?}");
    }

    info!("Precheckin is OK");
    Ok(())
}

fn run_art_tests() -> CmdResult {
    let rand_log = "data/logs/test_art_random.log";
    let format_log = "data/logs/format.log";
    let fbs_log = "data/logs/fbs.log";
    let ts = ["ts", "-m", TS_FMT];
    let working_dir = run_fun!(pwd)?;

    cmd_service::start_minio_service()?;
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
        $working_dir/zig-out/bin/nss_server format |& $[ts] >$format_log;
        $working_dir/zig-out/bin/fbs --new_tree $TEST_BUCKET_ROOT_BLOB_NAME |& $[ts] >$fbs_log;
        $working_dir/zig-out/bin/test_async_art --prefill 100000 --tests rename
            --ops 10000 --parallelism 1000 --debug |& $[ts] >$async_art_log;
    }?;

    let async_art_log = "data/logs/test_async_art.log";
    run_cmd! {
        info "Running async art tests with log $async_art_log";
        $working_dir/zig-out/bin/nss_server format |& $[ts] >$format_log;
        $working_dir/zig-out/bin/fbs --new_tree $TEST_BUCKET_ROOT_BLOB_NAME |& $[ts] >$fbs_log;
        $working_dir/zig-out/bin/test_async_art -p 20 |& $[ts] >$async_art_log;
        $working_dir/zig-out/bin/test_async_art -p 20 |& $[ts] >>$async_art_log;
        $working_dir/zig-out/bin/test_async_art -p 20 |& $[ts] >>$async_art_log;
    }?;

    Ok(())
}

fn run_s3_api_tests() -> CmdResult {
    cmd_service::init_service(ServiceName::All, BuildMode::Debug)?;
    cmd_service::start_services(
        ServiceName::All,
        BuildMode::Debug,
        false,
        Default::default(),
    )?;
    run_cmd! {
        info "Run cargo tests (s3 api tests)";
        cargo test --package api_server -- --test-threads 1 --skip multi_az_resilience;
    }?;
    let _ = cmd_service::stop_service(ServiceName::All);

    Ok(())
}

fn run_leader_election_tests() -> CmdResult {
    // Ensure DDB local is initialized with leader election table
    cmd_service::init_service(ServiceName::DdbLocal, BuildMode::Debug)?;
    cmd_service::start_services(
        ServiceName::DdbLocal,
        BuildMode::Debug,
        false,
        DataBlobStorage::HybridSingleAz,
    )?;

    run_cmd! {
        info "Running root_server leader election tests";
        cargo test --package root_server --test leader_election_test -- --test-threads 1;
    }?;

    let _ = cmd_service::stop_service(ServiceName::DdbLocal);
    Ok(())
}
