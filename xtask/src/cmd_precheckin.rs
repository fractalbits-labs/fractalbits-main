use crate::{cmd_service, BuildMode, ServiceName, TEST_BUCKET_ROOT_BLOB_NAME};
use cmd_lib::*;

pub fn run_cmd_precheckin() -> CmdResult {
    crate::cmd_service::stop_service(ServiceName::All)?;
    crate::cmd_service::start_minio_service()?;

    run_cmd! {
        info "Building ...";
        cargo build;
        zig build 2>&1;
    }?;

    cmd_service::create_dirs_for_nss_server()?;
    run_cmd! {
        cd data;
        info "Formatting nss_server";
        ../zig-out/bin/nss_server format;
        info "Running zig unit tests";
        zig build test --summary all 2>&1;
    }?;

    run_s3_api_tests()?;
    run_art_tests()?;

    info!("Precheckin is OK");
    Ok(())
}

fn run_art_tests() -> CmdResult {
    let rand_log = "test_art_random.log";
    run_cmd! {
        info "Running art tests (random) with log $rand_log";
        cd data;
        ../zig-out/bin/nss_server format;
        ../zig-out/bin/test_art --tests random --size 400000 --ops 1000000 --threads 20 &> $rand_log;
    }
    .map_err(|e| {
        run_cmd!(tail $rand_log).unwrap();
        e
    })?;

    let fat_log = "test_art_fat.log";
    run_cmd! {
        info "Running art tests (fat) with log $fat_log";
        cd data;
        ../zig-out/bin/nss_server format;
        ../zig-out/bin/test_art --tests fat --ops 1000000 &> $fat_log;
    }
    .map_err(|e| {
        run_cmd!(tail $fat_log).unwrap();
        e
    })?;

    let async_art_log = "test_async_art_rename.log";
    run_cmd! {
        info "Running async art rename tests with log $async_art_log";
        cd data;
        ../zig-out/bin/nss_server format;
        ../zig-out/bin/fbs --new_tree $TEST_BUCKET_ROOT_BLOB_NAME;
        ../zig-out/bin/test_async_art --prefill 100000 --tests rename --ops 10000 --parallelism 1000 --debug  &> $async_art_log;
    }
    .map_err(|e| {
        run_cmd!(tail $async_art_log).unwrap();
        e
    })?;

    let async_art_log = "test_async_art.log";
    run_cmd! {
        info "Running async art tests with log $async_art_log";
        cd data;
        ../zig-out/bin/nss_server format;
        ../zig-out/bin/fbs --new_tree $TEST_BUCKET_ROOT_BLOB_NAME;
        ../zig-out/bin/test_async_art -p 20 &> $async_art_log;
        ../zig-out/bin/test_async_art -p 20 &>> $async_art_log;
        ../zig-out/bin/test_async_art -p 20 &>> $async_art_log;
    }
    .map_err(|e| {
        run_cmd!(tail $async_art_log).unwrap();
        e
    })?;

    Ok(())
}

fn run_s3_api_tests() -> CmdResult {
    cmd_service::stop_service(ServiceName::All)?;
    run_cmd! {
        info "Removing previous buckets (ddb_local) data";
        cd data;
        rm -f rss/shared-local-instance.db;
        info "Formatting nss_server";
        ../zig-out/bin/nss_server format;
    }?;

    cmd_service::start_services(BuildMode::Debug, ServiceName::All)?;
    run_cmd! {
        info "Run cargo tests (s3 api tests)";
        cargo test;
    }?;
    let _ = cmd_service::stop_service(ServiceName::All);

    Ok(())
}
