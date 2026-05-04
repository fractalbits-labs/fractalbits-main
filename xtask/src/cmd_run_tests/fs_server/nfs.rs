use crate::cmd_service;
use crate::{CmdResult, FsServerConfig, InitConfig, ServiceName};
use aws_sdk_s3::primitives::ByteStream;
use cmd_lib::*;
use colored::*;
use std::os::unix::fs::FileExt;
use std::path::Path;
use std::time::Duration;

use super::{cleanup_objects, generate_test_data, setup_test_bucket};

const NFS_MOUNT_POINT: &str = "/tmp/nfs_server_test";

fn mount_nfs(bucket: &str) -> CmdResult {
    mount_nfs_with_opts(bucket, false)
}

fn mount_nfs_rw(bucket: &str) -> CmdResult {
    mount_nfs_with_opts(bucket, true)
}

fn mount_nfs_with_opts(bucket: &str, read_write: bool) -> CmdResult {
    let mount_point = NFS_MOUNT_POINT;

    run_cmd!(mkdir -p $mount_point)?;

    let init_config = InitConfig {
        fs_server: FsServerConfig {
            bucket_name: bucket.to_string(),
            mount_point: mount_point.to_string(),
            mode: "nfs".to_string(),
            read_write,
            ..Default::default()
        },
        ..Default::default()
    };
    cmd_service::init_service(
        ServiceName::FsServer,
        crate::cmd_build::BuildMode::Debug,
        &init_config,
    )?;
    cmd_service::start_service(ServiceName::FsServer)?;

    // Wait for NFS server to start listening
    std::thread::sleep(Duration::from_secs(2));

    // Mount via NFS (port/mountport bypass rpcbind; our server handles both on one port)
    if let Err(e) = run_cmd!(
        sudo mount -t nfs -o vers=3,tcp,nolock,soft,timeo=50,port=2049,mountport=2049 "localhost:/" $mount_point
    ) {
        let _ = cmd_service::stop_service(ServiceName::FsServer);
        return Err(e);
    }

    // Verify mount
    for i in 0..10 {
        std::thread::sleep(Duration::from_millis(500));
        let status = std::process::Command::new("mountpoint")
            .arg("-q")
            .arg(mount_point)
            .status();
        if let Ok(s) = status
            && s.success()
        {
            println!(
                "    NFS mounted at {} (after {}ms)",
                mount_point,
                2000 + (i + 1) * 500
            );
            return Ok(());
        }
    }

    run_cmd! { ignore sudo umount $mount_point 2>/dev/null; }?;
    let _ = cmd_service::stop_service(ServiceName::FsServer);
    Err(std::io::Error::other(format!(
        "NFS mount at {} not ready after 7 seconds",
        mount_point
    )))
}

fn unmount_nfs() -> CmdResult {
    let mount_point = NFS_MOUNT_POINT;
    run_cmd! { ignore sudo umount $mount_point 2>/dev/null; }?;
    let _ = cmd_service::stop_service(ServiceName::FsServer);
    run_cmd! { ignore pkill -x fs_server 2>/dev/null; }?;
    std::thread::sleep(Duration::from_millis(500));
    Ok(())
}

pub async fn run_nfs_tests() -> CmdResult {
    info!("Running NFS integration tests...");

    println!("\n{}", "=== NFS Test: Basic File Read ===".bold());
    if let Err(e) = test_nfs_basic_file_read().await {
        eprintln!("{}: {}", "Test FAILED".red().bold(), e);
        return Err(e);
    }

    println!("\n{}", "=== NFS Test: Directory Listing ===".bold());
    if let Err(e) = test_nfs_directory_listing().await {
        eprintln!("{}: {}", "Test FAILED".red().bold(), e);
        return Err(e);
    }

    println!("\n{}", "=== NFS Test: Large File Read ===".bold());
    if let Err(e) = test_nfs_large_file_read().await {
        eprintln!("{}: {}", "Test FAILED".red().bold(), e);
        return Err(e);
    }

    println!("\n{}", "=== NFS Test: Create, Write, Read ===".bold());
    if let Err(e) = test_nfs_create_write_read().await {
        eprintln!("{}: {}", "Test FAILED".red().bold(), e);
        return Err(e);
    }

    println!("\n{}", "=== NFS Test: Large File Write ===".bold());
    if let Err(e) = test_nfs_large_file_write().await {
        eprintln!("{}: {}", "Test FAILED".red().bold(), e);
        return Err(e);
    }

    println!("\n{}", "=== NFS Test: Mkdir and Rmdir ===".bold());
    if let Err(e) = test_nfs_mkdir_rmdir().await {
        eprintln!("{}: {}", "Test FAILED".red().bold(), e);
        return Err(e);
    }

    println!("\n{}", "=== NFS Test: Unlink ===".bold());
    if let Err(e) = test_nfs_unlink().await {
        eprintln!("{}: {}", "Test FAILED".red().bold(), e);
        return Err(e);
    }

    println!("\n{}", "=== NFS Test: Rename ===".bold());
    if let Err(e) = test_nfs_rename().await {
        eprintln!("{}: {}", "Test FAILED".red().bold(), e);
        return Err(e);
    }

    println!(
        "\n{}",
        "=== NFS Test: Sparse Truncate Up (O(1) extend) ===".bold()
    );
    if let Err(e) = test_nfs_sparse_truncate_up().await {
        eprintln!("{}: {}", "Test FAILED".red().bold(), e);
        return Err(e);
    }

    println!(
        "\n{}",
        "=== NFS Test: Sparse Partial-Block Overwrite ===".bold()
    );
    if let Err(e) = test_nfs_sparse_partial_overwrite().await {
        eprintln!("{}: {}", "Test FAILED".red().bold(), e);
        return Err(e);
    }

    println!(
        "\n{}",
        "=== NFS Test: Sparse File Round Trip (truncate + write + holes) ===".bold()
    );
    if let Err(e) = test_nfs_sparse_round_trip().await {
        eprintln!("{}: {}", "Test FAILED".red().bold(), e);
        return Err(e);
    }

    println!(
        "\n{}",
        "=== NFS Test: Sparse Truncate-Then-Shrink ===".bold()
    );
    if let Err(e) = test_nfs_sparse_truncate_shrink().await {
        eprintln!("{}: {}", "Test FAILED".red().bold(), e);
        return Err(e);
    }

    println!("\n{}", "=== All NFS Tests PASSED ===".green().bold());
    Ok(())
}

async fn test_nfs_basic_file_read() -> CmdResult {
    let (ctx, bucket) = setup_test_bucket().await;

    println!("  Step 1: Upload test objects via S3 API");
    let test_files: Vec<(&str, Vec<u8>)> = vec![
        ("hello.txt", b"Hello, NFS!".to_vec()),
        ("numbers.dat", b"0123456789".to_vec()),
    ];

    for (key, data) in &test_files {
        ctx.client
            .put_object()
            .bucket(&bucket)
            .key(*key)
            .body(ByteStream::from(data.clone()))
            .send()
            .await
            .unwrap_or_else(|e| panic!("Failed to put {key}: {e}"));
        println!("    Uploaded: {} ({} bytes)", key, data.len());
    }

    println!("  Step 2: Mount NFS filesystem");
    mount_nfs(&bucket)?;

    println!("  Step 3: Read and verify files");
    for (key, expected_data) in &test_files {
        let nfs_path = format!("{}/{}", NFS_MOUNT_POINT, key);
        let actual_data =
            std::fs::read(&nfs_path).unwrap_or_else(|e| panic!("Failed to read {key}: {e}"));
        assert_eq!(actual_data, *expected_data, "{key}: data mismatch");
        println!("    {}: OK ({} bytes)", key, actual_data.len());
    }

    unmount_nfs()?;
    cleanup_objects(
        &ctx,
        &bucket,
        &test_files.iter().map(|(k, _)| *k).collect::<Vec<_>>(),
    )
    .await;

    println!("{}", "SUCCESS: NFS basic file read test passed".green());
    Ok(())
}

async fn test_nfs_directory_listing() -> CmdResult {
    let (ctx, bucket) = setup_test_bucket().await;

    println!("  Step 1: Upload objects with directory structure");
    let keys = vec![
        "top-level.txt",
        "docs/readme.md",
        "docs/guide.md",
        "src/main.rs",
    ];

    for key in &keys {
        let data = format!("content of {key}");
        ctx.client
            .put_object()
            .bucket(&bucket)
            .key(*key)
            .body(ByteStream::from(data.into_bytes()))
            .send()
            .await
            .unwrap_or_else(|e| panic!("Failed to put {key}: {e}"));
    }
    println!("    Uploaded {} objects", keys.len());

    println!("  Step 2: Mount NFS filesystem");
    mount_nfs(&bucket)?;

    println!("  Step 3: Verify root directory listing");
    let root_entries: Vec<String> = std::fs::read_dir(NFS_MOUNT_POINT)
        .expect("Failed to list root")
        .filter_map(|e| e.ok())
        .map(|e| e.file_name().to_string_lossy().to_string())
        .collect();

    println!("    Root entries: {:?}", root_entries);

    let expected_root = vec!["top-level.txt", "docs", "src"];
    for expected in &expected_root {
        assert!(
            root_entries.contains(&expected.to_string()),
            "Missing root entry: {expected}"
        );
        println!("    Found: {}", expected);
    }

    println!("  Step 4: Verify file content");
    let readme_path = format!("{}/docs/readme.md", NFS_MOUNT_POINT);
    let content = std::fs::read_to_string(&readme_path).expect("Failed to read docs/readme.md");
    assert_eq!(
        content, "content of docs/readme.md",
        "Content mismatch for docs/readme.md"
    );
    println!("    docs/readme.md content: OK");

    unmount_nfs()?;
    cleanup_objects(&ctx, &bucket, &keys.to_vec()).await;

    println!("{}", "SUCCESS: NFS directory listing test passed".green());
    Ok(())
}

async fn test_nfs_large_file_read() -> CmdResult {
    let (ctx, bucket) = setup_test_bucket().await;

    let sizes: Vec<(&str, usize)> = vec![
        ("small-4k", 4 * 1024),
        ("medium-512k", 512 * 1024),
        ("large-2mb", 2 * 1024 * 1024),
    ];

    println!("  Step 1: Upload large test objects");
    let mut upload_keys = Vec::new();
    for (label, size) in &sizes {
        let key = format!("nfs-large-{label}");
        let data = generate_test_data(&key, *size);
        ctx.client
            .put_object()
            .bucket(&bucket)
            .key(&key)
            .body(ByteStream::from(data))
            .send()
            .await
            .unwrap_or_else(|e| panic!("Failed to put {key}: {e}"));
        upload_keys.push(key);
        println!("    Uploaded: {} ({} bytes)", label, size);
    }

    println!("  Step 2: Mount NFS filesystem");
    mount_nfs(&bucket)?;

    println!("  Step 3: Read and verify large files");
    for (i, (label, size)) in sizes.iter().enumerate() {
        let key = &upload_keys[i];
        let expected_data = generate_test_data(key, *size);
        let nfs_path = format!("{}/{}", NFS_MOUNT_POINT, key);
        let actual_data =
            std::fs::read(&nfs_path).unwrap_or_else(|e| panic!("Failed to read {key}: {e}"));
        assert_eq!(actual_data, expected_data, "{label}: data mismatch");
        println!("    {}: OK ({} bytes)", label, actual_data.len());
    }

    unmount_nfs()?;
    let key_refs: Vec<&str> = upload_keys.iter().map(|k| k.as_str()).collect();
    cleanup_objects(&ctx, &bucket, &key_refs).await;

    println!("{}", "SUCCESS: NFS large file read test passed".green());
    Ok(())
}

async fn test_nfs_create_write_read() -> CmdResult {
    let (_ctx, bucket) = setup_test_bucket().await;

    println!("  Step 1: Mount NFS in read-write mode");
    mount_nfs_rw(&bucket)?;

    println!("  Step 2: Create and write files");
    let test_data = b"Hello from NFS write!";
    let nfs_path = format!("{}/nfs-write-test.txt", NFS_MOUNT_POINT);
    std::fs::write(&nfs_path, test_data).expect("Failed to write file");
    println!(
        "    Written: nfs-write-test.txt ({} bytes)",
        test_data.len()
    );

    println!("  Step 3: Read back and verify");
    let read_back = std::fs::read(&nfs_path).expect("Failed to read back");
    assert_eq!(read_back, test_data, "nfs-write-test.txt data mismatch");
    println!("    nfs-write-test.txt content: OK");

    println!("  Step 4: Write a larger file (64KB)");
    let large_data = generate_test_data("nfs-large-write", 64 * 1024);
    let large_path = format!("{}/nfs-large-write.bin", NFS_MOUNT_POINT);
    std::fs::write(&large_path, &large_data).expect("Failed to write large file");

    let large_read = std::fs::read(&large_path).expect("Failed to read back large file");
    assert_eq!(large_read, large_data, "nfs-large-write.bin data mismatch");
    println!("    nfs-large-write.bin (64KB): OK");

    println!("  Step 5: Verify files appear in listing");
    let entries: Vec<String> = std::fs::read_dir(NFS_MOUNT_POINT)
        .expect("Failed to list root")
        .filter_map(|e| e.ok())
        .map(|e| e.file_name().to_string_lossy().to_string())
        .collect();
    assert!(
        entries.contains(&"nfs-write-test.txt".to_string()),
        "nfs-write-test.txt not found in listing"
    );
    println!("    nfs-write-test.txt in listing: OK");

    unmount_nfs()?;
    println!("{}", "SUCCESS: NFS create/write/read test passed".green());
    Ok(())
}

async fn test_nfs_large_file_write() -> CmdResult {
    let (_ctx, bucket) = setup_test_bucket().await;

    let sizes: Vec<(&str, usize)> = vec![
        ("small-4k", 4 * 1024),
        ("medium-512k", 512 * 1024),
        ("large-2mb", 2 * 1024 * 1024),
    ];

    println!("  Step 1: Mount NFS in read-write mode");
    mount_nfs_rw(&bucket)?;

    println!("  Step 2: Write large files via NFS");
    let mut keys = Vec::new();
    for (label, size) in &sizes {
        let key = format!("nfs-write-{label}");
        let data = generate_test_data(&key, *size);
        let nfs_path = format!("{}/{}", NFS_MOUNT_POINT, key);
        std::fs::write(&nfs_path, &data).unwrap_or_else(|e| panic!("Failed to write {key}: {e}"));
        keys.push((key, data));
        println!("    Written: {} ({} bytes)", label, size);
    }

    println!("  Step 3: Read back and verify");
    for (i, (label, _)) in sizes.iter().enumerate() {
        let (key, expected_data) = &keys[i];
        let nfs_path = format!("{}/{}", NFS_MOUNT_POINT, key);
        let actual_data =
            std::fs::read(&nfs_path).unwrap_or_else(|e| panic!("Failed to read {key}: {e}"));
        assert_eq!(actual_data, *expected_data, "{label}: data mismatch");
        println!("    {}: OK ({} bytes)", label, actual_data.len());
    }

    unmount_nfs()?;

    println!("{}", "SUCCESS: NFS large file write test passed".green());
    Ok(())
}

async fn test_nfs_mkdir_rmdir() -> CmdResult {
    let (_ctx, bucket) = setup_test_bucket().await;

    println!("  Step 1: Mount NFS in read-write mode");
    mount_nfs_rw(&bucket)?;

    println!("  Step 2: Create directory");
    let dir_path = format!("{}/nfs-testdir", NFS_MOUNT_POINT);
    std::fs::create_dir(&dir_path).expect("Failed to mkdir");
    println!("    Created: nfs-testdir/");

    assert!(
        Path::new(&dir_path).is_dir(),
        "nfs-testdir/ is not a directory"
    );
    println!("    nfs-testdir/ is a directory: OK");

    println!("  Step 3: Create file in directory");
    let file_path = format!("{}/nfs-testdir/file.txt", NFS_MOUNT_POINT);
    std::fs::write(&file_path, b"content in dir").expect("Failed to write file in dir");
    println!("    Created: nfs-testdir/file.txt");

    println!("  Step 4: Remove file then directory");
    std::fs::remove_file(&file_path).expect("Failed to unlink file");
    println!("    Removed: nfs-testdir/file.txt");

    std::fs::remove_dir(&dir_path).expect("Failed to rmdir");
    println!("    Removed: nfs-testdir/");

    assert!(
        !Path::new(&dir_path).exists(),
        "nfs-testdir/ still exists after rmdir"
    );
    println!("    nfs-testdir/ gone: OK");

    unmount_nfs()?;
    println!("{}", "SUCCESS: NFS mkdir/rmdir test passed".green());
    Ok(())
}

async fn test_nfs_unlink() -> CmdResult {
    let (_ctx, bucket) = setup_test_bucket().await;

    println!("  Step 1: Mount NFS in read-write mode");
    mount_nfs_rw(&bucket)?;

    println!("  Step 2: Create file then unlink");
    let file_path = format!("{}/nfs-to-delete.txt", NFS_MOUNT_POINT);
    std::fs::write(&file_path, b"delete me via NFS").expect("Failed to write");
    println!("    Created: nfs-to-delete.txt");

    assert!(
        Path::new(&file_path).exists(),
        "nfs-to-delete.txt should exist"
    );

    std::fs::remove_file(&file_path).expect("Failed to unlink");
    println!("    Unlinked: nfs-to-delete.txt");

    assert!(
        !Path::new(&file_path).exists(),
        "nfs-to-delete.txt still exists after unlink"
    );
    println!("    nfs-to-delete.txt gone: OK");

    unmount_nfs()?;
    println!("{}", "SUCCESS: NFS unlink test passed".green());
    Ok(())
}

async fn test_nfs_rename() -> CmdResult {
    let (_ctx, bucket) = setup_test_bucket().await;

    println!("  Step 1: Mount NFS in read-write mode");
    mount_nfs_rw(&bucket)?;

    println!("  Step 2: Create file and rename");
    let src_path = format!("{}/nfs-original.txt", NFS_MOUNT_POINT);
    let dst_path = format!("{}/nfs-renamed.txt", NFS_MOUNT_POINT);
    let content = b"rename me via NFS";
    std::fs::write(&src_path, content).expect("Failed to write");
    println!("    Created: nfs-original.txt");

    std::fs::rename(&src_path, &dst_path).expect("Failed to rename");
    println!("    Renamed: nfs-original.txt -> nfs-renamed.txt");

    assert!(
        !Path::new(&src_path).exists(),
        "nfs-original.txt still exists after rename"
    );
    println!("    nfs-original.txt gone: OK");

    let read_back = std::fs::read(&dst_path).expect("Failed to read renamed file");
    assert_eq!(read_back, content, "nfs-renamed.txt content mismatch");
    println!("    nfs-renamed.txt content: OK");

    unmount_nfs()?;
    println!("{}", "SUCCESS: NFS rename test passed".green());
    Ok(())
}

// Exercises the sparse override path through NFS SETATTR(size=N) with
// N > current_size. The Linux NFS client maps `set_len` to a SETATTR
// RPC; before the V1 sparse work this was either unsupported (NFS
// adapter only honored size=0) or O(N) on the filesystem side.
async fn test_nfs_sparse_truncate_up() -> CmdResult {
    let (_ctx, bucket) = setup_test_bucket().await;

    println!("  Step 1: Mount NFS in read-write mode");
    mount_nfs_rw(&bucket)?;

    println!("  Step 2: Create a small file");
    let file_path = format!("{}/nfs-sparse-trunc-up.bin", NFS_MOUNT_POINT);
    std::fs::write(&file_path, b"hello").expect("seed write failed");

    println!("  Step 3: ftruncate up to 16MB (sparse extend, must be O(1))");
    let f = std::fs::OpenOptions::new()
        .write(true)
        .open(&file_path)
        .expect("open for set_len failed");
    f.set_len(16 * 1024 * 1024)
        .expect("set_len(16MB) failed -- NFS setattr did not honor non-zero size?");
    drop(f);

    println!("  Step 4: stat() reports the new size");
    let meta = std::fs::metadata(&file_path).expect("stat failed");
    assert_eq!(
        meta.len(),
        16 * 1024 * 1024,
        "stat after set_len should report 16MB"
    );

    println!("  Step 5: Original bytes are preserved");
    let read_handle = std::fs::File::open(&file_path).expect("open ro failed");
    let mut head = vec![0u8; 5];
    read_handle
        .read_exact_at(&mut head, 0)
        .expect("read head failed");
    assert_eq!(&head, b"hello", "Original bytes lost after sparse extend");

    println!("  Step 6: A 4KB read in the hole returns zeros");
    let mut hole = vec![0xffu8; 4096];
    read_handle
        .read_exact_at(&mut hole, 1024 * 1024)
        .expect("hole read failed");
    assert!(
        hole.iter().all(|&b| b == 0),
        "Hole region must read as zeros (got first non-zero at offset {})",
        hole.iter().position(|&b| b != 0).unwrap_or(usize::MAX),
    );
    drop(read_handle);

    unmount_nfs()?;
    println!("{}", "SUCCESS: NFS sparse truncate-up test passed".green());
    Ok(())
}

// A small NFS WRITE in the middle of an existing multi-block object
// must override only the touched block via the V1 flush path. The
// surrounding bytes (in adjacent blocks AND inside the same block)
// must survive the per-call open/write/flush/release cycle that NFS
// goes through for every WRITE RPC.
async fn test_nfs_sparse_partial_overwrite() -> CmdResult {
    let (ctx, bucket) = setup_test_bucket().await;

    println!("  Step 1: Upload a 64KB seed object via S3");
    let key = "nfs-sparse-partial.bin";
    let original = generate_test_data(key, 64 * 1024);
    ctx.client
        .put_object()
        .bucket(&bucket)
        .key(key)
        .body(ByteStream::from(original.clone()))
        .send()
        .await
        .expect("put failed");

    println!("  Step 2: Mount NFS in read-write mode");
    mount_nfs_rw(&bucket)?;

    println!("  Step 3: Patch 16 bytes at offset 32KB via NFS WRITE");
    let file_path = format!("{}/{}", NFS_MOUNT_POINT, key);
    let patch = b"NFS-V1-PARTIAL!!";
    {
        let f = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&file_path)
            .expect("open RDWR failed");
        f.write_all_at(patch, 32 * 1024)
            .expect("write_all_at failed");
        f.sync_all().expect("sync_all failed");
    }

    println!("  Step 4: Verify patch landed and surrounding bytes are intact");
    let actual = std::fs::read(&file_path).expect("read after partial write failed");
    assert_eq!(actual.len(), original.len(), "Length must match original");
    assert_eq!(
        &actual[..32 * 1024],
        &original[..32 * 1024],
        "Pre-patch region corrupted"
    );
    assert_eq!(
        &actual[32 * 1024..32 * 1024 + patch.len()],
        patch,
        "Patch did not land"
    );
    assert_eq!(
        &actual[32 * 1024 + patch.len()..],
        &original[32 * 1024 + patch.len()..],
        "Post-patch region corrupted"
    );

    unmount_nfs()?;
    cleanup_objects(&ctx, &bucket, &[key]).await;
    println!(
        "{}",
        "SUCCESS: NFS sparse partial-block overwrite test passed".green()
    );
    Ok(())
}

// Round-trip a sparse file end-to-end via NFS: write head, ftruncate
// up to 4MB, write a marker near 3MB, then close + reopen + read.
// Holes must read as zeros (BlockNotFound -> zeros) and the written
// bytes must be intact. This exercises the sparse extend + override-
// flush path across multiple NFS RPCs to the same inode.
async fn test_nfs_sparse_round_trip() -> CmdResult {
    let (_ctx, bucket) = setup_test_bucket().await;

    println!("  Step 1: Mount NFS rw and create a fresh file");
    mount_nfs_rw(&bucket)?;
    let file_path = format!("{}/nfs-sparse-roundtrip.bin", NFS_MOUNT_POINT);

    let marker = b"NFS-END-MARKER-32B-XXXXXXXXXXXXX";
    let marker_offset: u64 = 3 * 1024 * 1024;

    println!("  Step 2: Create + write head, ftruncate to 4MB, write marker near 3MB");
    {
        let f = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(&file_path)
            .expect("create failed");
        f.write_all_at(b"head!", 0).expect("write head failed");
        f.set_len(4 * 1024 * 1024).expect("set_len(4MB) failed");
        f.write_all_at(marker, marker_offset)
            .expect("write marker failed");
        f.sync_all().expect("sync_all failed");
    }

    println!("  Step 3: Reopen; verify size, head, marker, and hole reads zeros");
    let meta = std::fs::metadata(&file_path).expect("stat failed");
    assert_eq!(meta.len(), 4 * 1024 * 1024, "size mismatch after flush");

    let f = std::fs::OpenOptions::new()
        .read(true)
        .open(&file_path)
        .expect("open ro failed");

    let mut head = vec![0u8; 5];
    f.read_exact_at(&mut head, 0).expect("read head failed");
    assert_eq!(&head, b"head!", "head bytes lost");

    let mut hole = vec![0xffu8; 4096];
    f.read_exact_at(&mut hole, 1024 * 1024)
        .expect("read hole failed");
    assert!(
        hole.iter().all(|&b| b == 0),
        "hole did not read as zeros (got first non-zero at offset {})",
        hole.iter().position(|&b| b != 0).unwrap_or(usize::MAX),
    );

    let mut readback = vec![0u8; marker.len()];
    f.read_exact_at(&mut readback, marker_offset)
        .expect("read marker failed");
    assert_eq!(&readback, marker, "marker mismatch");

    drop(f);
    unmount_nfs()?;
    println!("{}", "SUCCESS: NFS sparse round-trip test passed".green());
    Ok(())
}

// Truncate down to a non-block-aligned size via NFS, then read past
// the new EOF on a fresh handle. POSIX shrink-destroys: the original
// bytes past the new EOF must be gone (file size reflects the shrink,
// reads past EOF return EOF). Exercises the V1 shrink path through
// NFS SETATTR(size=N) where N < current_size.
async fn test_nfs_sparse_truncate_shrink() -> CmdResult {
    let (ctx, bucket) = setup_test_bucket().await;

    println!("  Step 1: Upload a 64KB seed object via S3 with a recognizable pattern");
    let key = "nfs-sparse-shrink.bin";
    let original: Vec<u8> = (0..64 * 1024).map(|i| (i % 251 + 1) as u8).collect();
    ctx.client
        .put_object()
        .bucket(&bucket)
        .key(key)
        .body(ByteStream::from(original.clone()))
        .send()
        .await
        .expect("put failed");

    println!("  Step 2: Mount NFS in read-write mode");
    mount_nfs_rw(&bucket)?;
    let file_path = format!("{}/{}", NFS_MOUNT_POINT, key);

    println!("  Step 3: Shrink to a non-block-aligned size (5000 bytes)");
    let shrunk_size: u64 = 5000;
    {
        let f = std::fs::OpenOptions::new()
            .write(true)
            .open(&file_path)
            .expect("open for shrink failed");
        f.set_len(shrunk_size).expect("set_len(5000) failed");
        f.sync_all().expect("sync_all failed");
    }

    println!("  Step 4: stat() reports the shrunk size");
    let meta = std::fs::metadata(&file_path).expect("stat failed");
    assert_eq!(meta.len(), shrunk_size, "stat after shrink should be 5000");

    println!("  Step 5: Surviving prefix matches the original");
    let actual = std::fs::read(&file_path).expect("read after shrink failed");
    assert_eq!(
        actual.len() as u64,
        shrunk_size,
        "Read length must match new EOF"
    );
    assert_eq!(
        &actual[..],
        &original[..shrunk_size as usize],
        "Surviving prefix corrupted by shrink"
    );

    unmount_nfs()?;
    cleanup_objects(&ctx, &bucket, &[key]).await;
    println!(
        "{}",
        "SUCCESS: NFS sparse truncate-shrink test passed".green()
    );
    Ok(())
}
