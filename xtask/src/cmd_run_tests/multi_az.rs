use crate::cmd_service::{start_services, stop_service, wait_for_service_ready};
use crate::{BuildMode, CmdResult, DataBlobStorage, ServiceName};
use aws_sdk_s3::primitives::ByteStream;
use cmd_lib::*;
use colored::*;
use serde::{Deserialize, Serialize};
use std::time::Duration;
use test_common::*;
use tokio::time::sleep;

#[derive(Serialize, Deserialize, Debug)]
struct BlobInfo {
    blob_key: String,
    missing_from: String,
}

#[derive(Serialize, Deserialize, Debug)]
struct ResyncResult {
    blobs: Vec<BlobInfo>,
    total_processed: u64,
    successful: u64,
    errors: u64,
    total_time_ms: u64,
}

fn dump_single_copy_blobs_status() -> CmdResult {
    println!("  Checking single-copy blob status...");
    run_cmd!(./target/debug/data_blob_resync_server status)
}

fn dump_single_copy_blobs_list() -> Result<ResyncResult, std::io::Error> {
    println!("  Listing single-copy blobs...");
    let output = run_fun!(./target/debug/data_blob_resync_server resync --dry-run --json)?;

    // Parse JSON output
    serde_json::from_str::<ResyncResult>(&output)
        .map_err(|e| std::io::Error::other(format!("Failed to parse JSON output: {e}")))
}

pub async fn run_multi_az_tests() -> CmdResult {
    info!("Running multi-AZ resilience tests...");

    // Initialize and start all required services for multi-AZ testing
    info!("Initializing and starting services for multi-AZ testing...");
    crate::cmd_service::stop_service(ServiceName::All)?;
    crate::cmd_build::build_rust_servers(BuildMode::Debug)?;
    crate::cmd_service::init_service(ServiceName::All, BuildMode::Debug)?;
    crate::cmd_service::start_services(
        ServiceName::All,
        BuildMode::Debug,
        false,
        DataBlobStorage::S3ExpressMultiAz,
    )?;

    info!("All services started successfully");

    // Run all three test scenarios
    println!(
        "\n{}",
        "=== Test 1: Remote AZ Service Interruption and Recovery ===".bold()
    );
    if let Err(e) = test_remote_az_service_interruption_and_recovery().await {
        eprintln!("{}: {}", "Test 1 FAILED".red().bold(), e);
        return Err(e);
    }

    println!(
        "\n{}",
        "=== Test 2: Rapid Remote AZ Interruptions ===".bold()
    );
    if let Err(e) = test_rapid_remote_az_interruptions().await {
        eprintln!("{}: {}", "Test 2 FAILED".red().bold(), e);
        return Err(e);
    }

    println!("\n{}", "=== Test 3: Extended Remote AZ Outage ===".bold());
    if let Err(e) = test_extended_remote_az_outage().await {
        eprintln!("{}: {}", "Test 3 FAILED".red().bold(), e);
        return Err(e);
    }

    println!(
        "\n{}",
        "=== All Multi-AZ Resilience Tests PASSED ==="
            .green()
            .bold()
    );
    Ok(())
}

async fn test_remote_az_service_interruption_and_recovery() -> CmdResult {
    let ctx = context();

    // Create test bucket
    let bucket_name = ctx.create_bucket("test-multi-az-resilience").await;

    // Initial setup - upload a test object while both AZs are online
    let test_key = "test-object-1";
    let test_data = b"Hello, Multi-AZ World!";

    println!(" Step 1: Upload object with both AZs online");
    ctx.client
        .put_object()
        .bucket(&bucket_name)
        .key(test_key)
        .body(ByteStream::from_static(test_data))
        .send()
        .await
        .expect("Failed to upload object");

    // Verify object exists
    let response = ctx
        .client
        .get_object()
        .bucket(&bucket_name)
        .key(test_key)
        .send()
        .await
        .expect("Failed to get object");

    let body = response.body.collect().await.expect("Failed to read body");
    assert_eq!(body.into_bytes().as_ref(), test_data);
    println!("OK: Object uploaded and verified successfully");

    // Simulate remote AZ going down
    println!("  Step 2: Simulating remote AZ service interruption...");
    stop_service(ServiceName::MinioAz2)?;

    // Wait a moment for the service to fully stop
    sleep(Duration::from_secs(2)).await;

    // Verify remote AZ is down by checking port
    let remote_az_down = run_cmd!(nc -z localhost 9002).is_err();
    assert!(remote_az_down, "Remote AZ service should be down");
    println!("OK: Remote AZ service confirmed down");

    // Try to upload new objects while remote AZ is down (should work in degraded mode)
    println!(" Step 3: Testing uploads during remote AZ downtime...");
    let degraded_objects = vec![
        ("degraded-object-1", b"Data during outage 1"),
        ("degraded-object-2", b"Data during outage 2"),
        ("degraded-object-3", b"Data during outage 3"),
    ];

    for (key, data) in &degraded_objects {
        println!("  Uploading {key}");
        ctx.client
            .put_object()
            .bucket(&bucket_name)
            .key(*key)
            .body(ByteStream::from_static(*data))
            .send()
            .await
            .expect("Failed to upload object during degraded mode");

        // Verify we can read it back immediately
        let response = ctx
            .client
            .get_object()
            .bucket(&bucket_name)
            .key(*key)
            .send()
            .await
            .expect("Failed to get object during degraded mode");

        let body = response.body.collect().await.expect("Failed to read body");
        assert_eq!(body.into_bytes().as_ref(), *data);
        println!("  OK: {key} uploaded and verified");
    }

    // Verify single-copy blob tracking during outage
    println!(" Step 3.1: Verifying single-copy blob tracking...");
    if let Err(e) = dump_single_copy_blobs_status() {
        warn!("Could not check single-copy blob status: {e}");
    }

    // Simulate remote AZ coming back online
    println!(" Step 4: Bringing remote AZ back online...");
    start_services(
        ServiceName::MinioAz2,
        BuildMode::Debug,
        false,
        DataBlobStorage::default(),
    )?;

    // Wait for service to fully start and be ready
    wait_for_service_ready(ServiceName::MinioAz2, 30)?;
    println!("OK: Remote AZ service back online");

    // Test that we can still access all objects after recovery
    println!(" Step 5: Verifying data integrity after recovery...");

    // Check original object
    let response = ctx
        .client
        .get_object()
        .bucket(&bucket_name)
        .key(test_key)
        .send()
        .await
        .expect("Failed to get original object after recovery");

    let body = response.body.collect().await.expect("Failed to read body");
    assert_eq!(body.into_bytes().as_ref(), test_data);
    println!("  OK: Original object still accessible");

    // Check objects uploaded during outage
    for (key, expected_data) in &degraded_objects {
        let response = ctx
            .client
            .get_object()
            .bucket(&bucket_name)
            .key(*key)
            .send()
            .await
            .expect("Failed to get degraded object after recovery");

        let body = response.body.collect().await.expect("Failed to read body");
        assert_eq!(body.into_bytes().as_ref(), *expected_data);
        println!("  OK: Degraded object {key} still accessible");
    }

    // Test new uploads after recovery
    println!(" Step 6: Testing new uploads after recovery...");
    let post_recovery_key = "post-recovery-object";
    let post_recovery_data = b"Data after recovery";

    ctx.client
        .put_object()
        .bucket(&bucket_name)
        .key(post_recovery_key)
        .body(ByteStream::from_static(post_recovery_data))
        .send()
        .await
        .expect("Failed to upload object after recovery");

    let response = ctx
        .client
        .get_object()
        .bucket(&bucket_name)
        .key(post_recovery_key)
        .send()
        .await
        .expect("Failed to get object after recovery");

    let body = response.body.collect().await.expect("Failed to read body");
    assert_eq!(body.into_bytes().as_ref(), post_recovery_data);
    println!("OK: New object uploaded and verified after recovery");

    println!("SUCCESS: Multi-AZ resilience test completed successfully!");
    Ok(())
}

async fn test_rapid_remote_az_interruptions() -> CmdResult {
    let ctx = context();
    let bucket_name = ctx.create_bucket("test-rapid-interruptions").await;

    println!(" Testing rapid remote AZ interruptions...");

    // Perform multiple rapid interruption cycles
    for cycle in 1..=3 {
        println!("  Cycle {cycle}: Stopping remote AZ");
        stop_service(ServiceName::MinioAz2)?;
        sleep(Duration::from_secs(1)).await;

        // Upload during outage
        let outage_key = format!("rapid-outage-{cycle}");
        let outage_data = format!("Data during rapid outage {cycle}");

        ctx.client
            .put_object()
            .bucket(&bucket_name)
            .key(&outage_key)
            .body(ByteStream::from(outage_data.as_bytes().to_vec()))
            .send()
            .await
            .expect("Failed to upload during rapid outage");

        println!("  Cycle {cycle}: Restarting remote AZ");
        start_services(
            ServiceName::MinioAz2,
            BuildMode::Debug,
            false,
            DataBlobStorage::default(),
        )?;
        wait_for_service_ready(ServiceName::MinioAz2, 15)?;

        // Verify data after restart
        let response = ctx
            .client
            .get_object()
            .bucket(&bucket_name)
            .key(&outage_key)
            .send()
            .await
            .expect("Failed to get object after rapid restart");

        let body = response.body.collect().await.expect("Failed to read body");
        assert_eq!(body.into_bytes().as_ref(), outage_data.as_bytes());
        println!("  OK: Cycle {cycle} completed successfully");

        // Check single-copy blob status after each cycle
        if let Err(e) = dump_single_copy_blobs_status() {
            warn!("Could not check single-copy blob status for cycle {cycle}: {e}");
        }
    }

    println!("SUCCESS: Rapid interruption test completed successfully!");
    Ok(())
}

async fn test_extended_remote_az_outage() -> CmdResult {
    let ctx = context();
    let bucket_name = ctx.create_bucket("test-extended-outage").await;

    println!(" Testing extended remote AZ outage (10+ objects during downtime)...");

    // Stop remote AZ
    stop_service(ServiceName::MinioAz2)?;
    sleep(Duration::from_secs(2)).await;

    // Upload many objects during extended outage
    for i in 1..=12 {
        let key = format!("extended-outage-object-{i:02}");
        let data = format!(
            "Extended outage data item {i} with some additional content to make it more realistic"
        );

        println!("  Uploading {key} (during outage)");
        ctx.client
            .put_object()
            .bucket(&bucket_name)
            .key(&key)
            .body(ByteStream::from(data.as_bytes().to_vec()))
            .send()
            .await
            .expect("Failed to upload during extended outage");

        // Verify immediate readability
        let response = ctx
            .client
            .get_object()
            .bucket(&bucket_name)
            .key(&key)
            .send()
            .await
            .expect("Failed to get object during extended outage");

        let body = response.body.collect().await.expect("Failed to read body");
        assert_eq!(body.into_bytes().as_ref(), data.as_bytes());
    }

    println!("  OK: All objects uploaded during extended outage");

    // Verify all objects are tracked as single-copy
    println!(" Verifying single-copy blob tracking for extended outage...");
    if let Err(e) = dump_single_copy_blobs_status() {
        warn!("Could not check single-copy blob status: {e}");
    }
    let result = dump_single_copy_blobs_list()?;
    let blob_count = result.blobs.len();
    println!("  Found {blob_count} single-copy blobs in tracking system");

    // Expected: 3 (degraded) + 3 (rapid interruption) + 12 (extended outage) = 18 total
    assert_eq!(18, blob_count, "Single-copy blob count mismatch");
    // Bring remote AZ back online
    start_services(
        ServiceName::MinioAz2,
        BuildMode::Debug,
        false,
        DataBlobStorage::default(),
    )?;
    wait_for_service_ready(ServiceName::MinioAz2, 30)?;

    // Verify all objects are still accessible
    for i in 1..=12 {
        let key = format!("extended-outage-object-{i:02}");
        let expected_data = format!(
            "Extended outage data item {i} with some additional content to make it more realistic"
        );

        let response = ctx
            .client
            .get_object()
            .bucket(&bucket_name)
            .key(&key)
            .send()
            .await
            .expect("Failed to get object after extended outage recovery");

        let body = response.body.collect().await.expect("Failed to read body");
        assert_eq!(body.into_bytes().as_ref(), expected_data.as_bytes());
    }

    println!("SUCCESS: Extended outage test completed successfully!");
    Ok(())
}
