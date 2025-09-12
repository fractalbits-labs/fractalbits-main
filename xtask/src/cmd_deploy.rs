use crate::*;
use std::path::Path;

pub fn run_cmd_deploy(
    use_s3_backend: bool,
    enable_dev_mode: bool,
    release_mode: bool,
    target_arm: bool,
    bss_use_i3: bool,
    deploy_mode: DeployMode,
) -> CmdResult {
    let build_info = BUILD_INFO.get().unwrap();
    let bucket_name = get_build_bucket_name()?;
    let bucket = format!("s3://{bucket_name}");

    // Check if the bucket exists; create if it doesn't
    let bucket_exists = run_cmd!(aws s3api head-bucket --bucket $bucket_name &>/dev/null).is_ok();
    if !bucket_exists {
        run_cmd! {
            info "Creating bucket $bucket";
            aws s3 mb $bucket
        }?;
    }

    let (zig_build_opt, rust_build_opt) = if release_mode {
        ("--release=safe", "--release")
    } else {
        ("", "")
    };
    let (rust_build_target, zig_build_target) = if target_arm {
        (
            "aarch64-unknown-linux-gnu",
            [
                "-Dtarget=aarch64-linux-gnu",
                "-Dcpu=neoverse_v1",
                "-Dbss_cpu=neoverse_v2",
            ],
        )
    } else {
        (
            "x86_64-unknown-linux-gnu",
            ["-Dtarget=x86_64-linux-gnu", "-Dcpu=cascadelake", ""],
        )
    };
    let arch = if target_arm { "aarch64" } else { "x86_64" };

    run_cmd!(mkdir -p data/deploy/$arch)?;
    if bss_use_i3 && arch != "x86_64" {
        run_cmd!(mkdir -p data/deploy/x86_64)?;
    }
    if deploy_mode == DeployMode::Bootstrap {
        // Build fractalbits-bootstrap binary only, in debug mode
        build_bootstrap_only(rust_build_target)?;
        run_cmd! {
            cp target/$rust_build_target/debug/fractalbits-bootstrap
                data/deploy/$arch/fractalbits-bootstrap;
            info "Syncing fractalbits-bootstrap to S3";
            aws s3 sync data/deploy $bucket;
        }?;
        return Ok(());
    }

    // Build and upload Rust projects if mode is rust or all
    if deploy_mode == DeployMode::Rust || deploy_mode == DeployMode::All {
        run_cmd! {
            info "Building Rust projects with zigbuild";
        }?;

        if target_arm {
            // Build for all workspaces, and the same workspace may get built again with other targets
            run_cmd! {
                info "Building Rust projects for Graviton3 (neoverse-v1)";
                RUSTFLAGS="-C target-cpu=neoverse-v1"
                BUILD_INFO=$build_info
                cargo zigbuild
                    --workspace --exclude api_server
                    --target $rust_build_target $rust_build_opt;

                info "Building api_server for Graviton4 (neoverse-v2)";
                RUSTFLAGS="-C target-cpu=neoverse-v2"
                BUILD_INFO=$build_info
                cargo zigbuild
                    -p api_server
                    --target $rust_build_target $rust_build_opt;
            }?;

            if !bss_use_i3 {
                // Build fractalbits-bootstrap separately with neoverse_n1
                run_cmd! {
                    info "Building fractalbits-bootstrap for Graviton2 (neoverse-n1)";
                    RUSTFLAGS="-C target-cpu=neoverse-n1"
                    BUILD_INFO=$build_info
                    cargo zigbuild -p fractalbits-bootstrap --target $rust_build_target $rust_build_opt;
                }?;
            } else {
                let target_cpu = "skylake"; // for i3en Intel Xeon Platinum 8175
                run_cmd! {
                    info "Building bss fractalbits-bootstrap & rewrk_rpc for x86_64";
                    RUSTFLAGS="-C target-cpu=$target_cpu"
                    BUILD_INFO=$build_info
                    cargo zigbuild -p fractalbits-bootstrap
                        --target x86_64-unknown-linux-gnu $rust_build_opt;
                    RUSTFLAGS="-C target-cpu=$target_cpu"
                    BUILD_INFO=$build_info
                    cargo zigbuild -p rewrk_rpc
                        --target x86_64-unknown-linux-gnu $rust_build_opt;
                }?;
            }
        } else {
            // Original behavior for x86
            run_cmd! {
                info "Building all Rust projects for x86_64";
                RUSTFLAGS="-C target-cpu=cascadelake"
                BUILD_INFO=$build_info
                cargo zigbuild --target $rust_build_target $rust_build_opt;
            }?;
        }

        // Copy Rust binaries to local deploy directory
        info!("Copying Rust-built binaries to data/deploy");
        let rust_bins = [
            "api_server",
            "root_server",
            "rss_admin",
            "fractalbits-bootstrap",
            "nss_role_agent",
            // "ebs-failover",
            "rewrk_rpc",
        ];
        let build_dir = if release_mode { "release" } else { "debug" };
        for bin in &rust_bins {
            run_cmd!(cp target/$rust_build_target/$build_dir/$bin data/deploy/$arch/$bin)?;
        }

        // Copy additional x86_64 binaries if bss_use_i3
        if bss_use_i3 {
            run_cmd!(cp target/x86_64-unknown-linux-gnu/$build_dir/fractalbits-bootstrap data/deploy/x86_64/fractalbits-bootstrap)?;
            run_cmd!(cp target/x86_64-unknown-linux-gnu/$build_dir/rewrk_rpc data/deploy/x86_64/rewrk_rpc)?;
        }
    }

    // Build and upload Zig projects if mode is zig or all
    if deploy_mode == DeployMode::Zig || deploy_mode == DeployMode::All {
        run_cmd! {
            info "Building Zig projects";
            cd ./core;
            zig build -p ../zig-out
                -Duse_s3_backend=$use_s3_backend
                -Denable_dev_mode=$enable_dev_mode
                -Dbuild_info=$build_info
                $[zig_build_target] $zig_build_opt 2>&1;
        }?;

        if bss_use_i3 {
            let target_cpu = "skylake"; // for i3en Intel Xeon Platinum 8175
            let zig_build_target = ["-Dtarget=x86_64-linux-gnu", &format!("-Dcpu={target_cpu}")];
            run_cmd! {
                info "Building bss_server for x86_64";
                cd ./core;
                zig build -p ../zig-out
                    -Duse_s3_backend=$use_s3_backend
                    -Denable_dev_mode=$enable_dev_mode
                    -Dbuild_info=$build_info
                    $[zig_build_target] $zig_build_opt bss_server 2>&1;
            }?;
        }

        // Copy Zig binaries to local deploy directory
        info!("Copying Zig-built binaries to data/deploy");
        let zig_bins = [
            "nss_server",
            "mirrord",
            "test_art", // to create test.data for benchmarking nss_rpc
            "s3_blob_client",
        ];
        for bin in &zig_bins {
            run_cmd!(cp zig-out/bin/$bin data/deploy/$arch/$bin)?;
        }

        // Copy bss_server
        if bss_use_i3 {
            run_cmd!(cp zig-out/bin/bss_server data/deploy/x86_64/bss_server)?;
        } else {
            run_cmd!(cp zig-out/bin/bss_server data/deploy/$arch/bss_server)?;
        }
    }

    // Build and copy UI if mode is ui or all
    if deploy_mode == DeployMode::Ui || deploy_mode == DeployMode::All {
        let region = run_fun!(aws configure list | grep region | awk r"{print $2}")?;
        cmd_build::build_ui(&region)?;
        run_cmd!(cp -r ui/dist data/deploy/ui)?;
    }

    // Deploy warp binary
    if deploy_mode == DeployMode::All {
        deploy_warp_binary(target_arm)?;
    }

    // Sync all binaries to S3 at once
    run_cmd! {
        info "Syncing all binaries to S3";
        aws s3 sync data/deploy $bucket;
        info "Syncing all binaries done";
    }?;

    Ok(())
}

fn deploy_warp_binary(target_arm: bool) -> CmdResult {
    let arch = if target_arm { "aarch64" } else { "x86_64" };
    let linux_arch = if target_arm { "arm64" } else { "x86_64" };

    let warp_version = "v1.3.0";
    let warp_file = format!("warp_Linux_{linux_arch}.tar.gz");
    let warp_path = format!("third_party/{warp_file}");

    let base_url = "https://github.com/minio/warp/releases/download";
    let download_url = format!("{base_url}/{warp_version}/{warp_file}");
    let checksums_url = format!("{base_url}/{warp_version}/checksums.txt");
    // Check if already downloaded
    if !Path::new(&warp_path).exists() {
        run_cmd! {
            info "Downloading warp binary for {linux_arch}";
            curl -sL -o $warp_path $download_url;
        }?;
    }
    run_cmd! {
        cd third_party;
        info "Verifying warp binary checksum";
        curl -sL -o warp_checksums.txt $checksums_url;
        grep $warp_file warp_checksums.txt | sha256sum -c --quiet;
        rm -f warp_checksums.txt;

        info "Extracting warp binary directly to data/deploy";
        tar -xzf $warp_file -C ../data/deploy/$arch warp;
    }?;

    Ok(())
}

fn build_bootstrap_only(rust_build_target: &str) -> CmdResult {
    let build_info = BUILD_INFO.get().unwrap();
    run_cmd! {
        info "Building fractalbits-bootstrap";
        BUILD_INFO=$build_info
        cargo zigbuild -p fractalbits-bootstrap --target $rust_build_target;
    }?;
    Ok(())
}

fn get_build_bucket_name() -> FunResult {
    let region = run_fun!(aws configure list | grep region | awk r"{print $2}")?;
    let account_id = run_fun!(aws sts get-caller-identity --query Account --output text)?;
    Ok(format!("fractalbits-builds-{region}-{account_id}"))
}

pub fn cleanup_builds_bucket() -> CmdResult {
    let bucket_name = get_build_bucket_name()?;
    let bucket = format!("s3://{bucket_name}");

    // Check if the bucket exists
    let bucket_exists = run_cmd!(aws s3api head-bucket --bucket $bucket_name &>/dev/null).is_ok();
    if !bucket_exists {
        info!("Bucket {bucket} does not exist, nothing to clean up");
        return Ok(());
    }

    // Empty the bucket first (delete all objects)
    run_cmd! {
        info "Emptying bucket $bucket";
        aws s3 rm $bucket --recursive;
    }?;

    // Delete the bucket
    run_cmd! {
        info "Deleting bucket $bucket";
        aws s3 rb $bucket;
    }?;

    info!("Successfully cleaned up builds bucket: {bucket}");
    Ok(())
}
