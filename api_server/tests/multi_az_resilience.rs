use aws_sdk_s3::primitives::ByteStream;
use cmd_lib::*;
use std::time::Duration;
use test_common::*;
use tokio::time::sleep;

#[tokio::test]
async fn test_remote_az_service_interruption_and_recovery() {
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
    stop_remote_az_service().expect("Failed to stop remote AZ service");

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

    // Simulate remote AZ coming back online
    println!(" Step 4: Bringing remote AZ back online...");
    start_remote_az_service().expect("Failed to start remote AZ service");

    // Wait for service to fully start and be ready
    wait_for_remote_az_ready(30)
        .await
        .expect("Remote AZ service failed to start");
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
}

#[tokio::test]
async fn test_rapid_remote_az_interruptions() {
    let ctx = context();
    let bucket_name = ctx.create_bucket("test-rapid-interruptions").await;

    println!(" Testing rapid remote AZ interruptions...");

    // Perform multiple rapid interruption cycles
    for cycle in 1..=3 {
        println!("  Cycle {cycle}: Stopping remote AZ");
        stop_remote_az_service().expect("Failed to stop remote AZ service");
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
        start_remote_az_service().expect("Failed to start remote AZ service");
        wait_for_remote_az_ready(15)
            .await
            .expect("Remote AZ failed to restart");

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
    }

    println!("SUCCESS: Rapid interruption test completed successfully!");
}

#[tokio::test]
async fn test_extended_remote_az_outage() {
    let ctx = context();
    let bucket_name = ctx.create_bucket("test-extended-outage").await;

    println!(" Testing extended remote AZ outage (10+ objects during downtime)...");

    // Stop remote AZ
    stop_remote_az_service().expect("Failed to stop remote AZ service");
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

    // Bring remote AZ back online
    start_remote_az_service().expect("Failed to start remote AZ service");
    wait_for_remote_az_ready(30)
        .await
        .expect("Remote AZ failed to start after extended outage");

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
}

// Helper functions for service management
fn stop_remote_az_service() -> Result<(), Box<dyn std::error::Error>> {
    run_cmd!(cd ..; cargo xtask service stop minio_az2)?;
    Ok(())
}

fn start_remote_az_service() -> Result<(), Box<dyn std::error::Error>> {
    run_cmd!(cd ..; cargo xtask service start minio_az2)?;
    Ok(())
}

async fn wait_for_remote_az_ready(timeout_secs: u32) -> Result<(), Box<dyn std::error::Error>> {
    use tokio::time::{timeout, Duration};

    let result = timeout(Duration::from_secs(timeout_secs as u64), async {
        loop {
            if run_cmd!(nc -z localhost 9002).is_ok() {
                // Give it a moment to be fully ready for requests
                sleep(Duration::from_secs(1)).await;
                return Ok(());
            }
            sleep(Duration::from_millis(500)).await;
        }
    })
    .await;

    match result {
        Ok(Ok(())) => Ok(()),
        Ok(Err(e)) => Err(e),
        Err(_) => {
            Err(format!("Timeout waiting for remote AZ to be ready after {timeout_secs}s").into())
        }
    }
}

// Note: Data blob resync server tests have been moved to /data_blob_resync_server/tests/
// This file now focuses on basic multi-AZ service interruption and recovery scenarios
