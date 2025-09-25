use crate::*;
use std::path::Path;

#[derive(Clone)]
struct CpuTarget {
    name: &'static str,
    arch: &'static str,
    rust_target: &'static str,
    rust_cpu: &'static str,
    zig_target: &'static str,
    zig_cpu: &'static str,
}

const CPU_TARGETS: &[CpuTarget] = &[
    CpuTarget {
        name: "i3",
        arch: "x86_64",
        rust_target: "x86_64-unknown-linux-gnu",
        rust_cpu: "skylake",
        zig_target: "x86_64-linux-gnu",
        zig_cpu: "skylake",
    },
    CpuTarget {
        name: "graviton3",
        arch: "aarch64",
        rust_target: "aarch64-unknown-linux-gnu",
        rust_cpu: "neoverse-v1",
        zig_target: "aarch64-linux-gnu",
        zig_cpu: "neoverse_v1",
    },
    CpuTarget {
        name: "graviton4",
        arch: "aarch64",
        rust_target: "aarch64-unknown-linux-gnu",
        rust_cpu: "neoverse-v2",
        zig_target: "aarch64-linux-gnu",
        zig_cpu: "neoverse_v2",
    },
];

const RUST_BINS: &[&str] = &[
    "fractalbits-bootstrap",
    "root_server",
    "api_server",
    "nss_role_agent",
    "rss_admin",
    "rewrk_rpc",
];

const ZIG_BINS: &[&str] = &["nss_server", "bss_server", "test_art"];

pub fn build(deploy_target: DeployTarget, release_mode: bool) -> CmdResult {
    let (zig_build_opt, rust_build_opt, build_dir) = if release_mode {
        ("--release=safe", "--release", "release")
    } else {
        ("", "", "debug")
    };

    // Create deploy directories for all CPU targets
    for target in CPU_TARGETS {
        let deploy_dir = format!("prebuilt/deploy/{}/{}", target.arch, target.name);
        run_cmd!(mkdir -p $deploy_dir)?;
    }

    // Build fractalbits-bootstrap separately for each architecture without CPU flags
    if deploy_target == DeployTarget::Bootstrap || deploy_target == DeployTarget::All {
        let build_envs = cmd_build::get_build_envs();
        for arch in ["x86_64", "aarch64"] {
            let rust_target = format!("{arch}-unknown-linux-gnu");
            run_cmd! {
                info "Building fractalbits-bootstrap for $arch";
                $[build_envs] cargo zigbuild
                    -p fractalbits-bootstrap --target $rust_target $rust_build_opt;
            }?;

            // Copy fractalbits-bootstrap to arch-level directory
            let src_path = format!("target/{}/{}/fractalbits-bootstrap", rust_target, build_dir);
            let dst_path = format!("prebuilt/deploy/{}/fractalbits-bootstrap", arch);
            run_cmd!(mkdir -p prebuilt/deploy/$arch)?;
            run_cmd!(cp $src_path $dst_path)?;
        }
    }

    // Build other Rust projects with CPU-specific optimizations
    if deploy_target == DeployTarget::Rust || deploy_target == DeployTarget::All {
        info!("Building Rust projects for all CPU targets");
        let build_envs = cmd_build::get_build_envs();

        for target in CPU_TARGETS {
            let rust_cpu = target.rust_cpu;
            let rust_target = target.rust_target;
            run_cmd! {
                info "Building Rust projects for $rust_target ($rust_cpu)";
                RUSTFLAGS="-C target-cpu=$rust_cpu"
                $[build_envs] cargo zigbuild
                    --target $rust_target $rust_build_opt;
            }?;

            // Copy Rust binaries to deploy directory (excluding fractalbits-bootstrap)
            let deploy_dir = format!("prebuilt/deploy/{}/{}", target.arch, target.name);
            for bin in RUST_BINS {
                if *bin != "fractalbits-bootstrap" {
                    let src_path = format!("target/{}/{}/{}", rust_target, build_dir, bin);
                    let dst_path = format!("{}/{}", deploy_dir, bin);
                    if Path::new(&src_path).exists() {
                        run_cmd!(cp $src_path $dst_path)?;
                    }
                }
            }
        }
    }

    // Build Zig projects for all CPU targets
    if (deploy_target == DeployTarget::Zig || deploy_target == DeployTarget::All)
        && Path::new(ZIG_REPO_PATH).exists()
    {
        info!("Building Zig projects for all CPU targets");
        let build_envs = cmd_build::get_build_envs();

        for target in CPU_TARGETS {
            let zig_out_dir = if target.arch == "aarch64" {
                format!("target/aarch64-unknown-linux-gnu/{build_dir}/zig-out")
            } else {
                format!("target/x86_64-unknown-linux-gnu/{build_dir}/zig-out")
            };

            let zig_target = target.zig_target;
            let zig_cpu = target.zig_cpu;
            run_cmd! {
                info "Building Zig projects for $zig_target ($zig_cpu)";
                cd $ZIG_REPO_PATH;
                $[build_envs] zig build
                    -p ../$zig_out_dir
                    -Dtarget=$zig_target -Dcpu=$zig_cpu $zig_build_opt 2>&1;
            }?;

            // Copy Zig binaries to deploy directory
            let deploy_dir = format!("prebuilt/deploy/{}/{}", target.arch, target.name);
            for bin in ZIG_BINS {
                let src_path = format!("{}/bin/{}", zig_out_dir, bin);
                let dst_path = format!("{}/{}", deploy_dir, bin);
                run_cmd!(cp $src_path $dst_path)?;
            }
        }
    }

    // Build and copy UI
    if (deploy_target == DeployTarget::Ui || deploy_target == DeployTarget::All)
        && Path::new(UI_REPO_PATH).exists()
    {
        let region = run_fun!(aws configure list | grep region | awk r"{print $2}")?;
        cmd_build::build_ui(&region)?;
        run_cmd! {
            rm -rf prebuilt/deploy/ui;
            cp -r ui/dist prebuilt/deploy/ui
        }?;
    }

    // Build (extract) warp binary for each architecture
    build_warp_binaries()?;

    info!("Deploy build is done");

    Ok(())
}

fn build_warp_binaries() -> CmdResult {
    for arch in ["x86_64", "aarch64"] {
        let linux_arch = if arch == "aarch64" { "arm64" } else { "x86_64" };

        let warp_version = "v1.3.0";
        let warp_file = format!("warp_Linux_{linux_arch}.tar.gz");
        let warp_path = format!("third_party/{warp_file}");

        let base_url = "https://github.com/minio/warp/releases/download";
        let download_url = format!("{base_url}/{warp_version}/{warp_file}");
        let checksums_url = format!("{base_url}/{warp_version}/checksums.txt");

        // Check if already downloaded
        if !Path::new(&warp_path).exists() {
            run_cmd! {
                info "Downloading warp binary for $linux_arch";
                curl -sL -o $warp_path $download_url;
            }?;
        }

        run_cmd! {
            cd third_party;
            info "Verifying warp binary checksum for $linux_arch";
            curl -sL -o warp_checksums.txt $checksums_url;
            grep $warp_file warp_checksums.txt | sha256sum -c --quiet;
            rm -f warp_checksums.txt;
        }?;

        // Extract warp to arch-level directory
        let deploy_dir = format!("prebuilt/deploy/{}", arch);
        run_cmd! {
            info "Extracting warp binary to $deploy_dir for $linux_arch";
            tar -xzf third_party/$warp_file -C $deploy_dir warp;
        }?;
    }

    Ok(())
}

pub fn upload() -> CmdResult {
    // Check/create S3 bucket and sync
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

    run_cmd! {
        info "Syncing all binaries to S3 bucket $bucket";
        aws s3 sync prebuilt/deploy $bucket;
        info "Syncing all binaries is done";
    }?;
    Ok(())
}

fn get_build_bucket_name() -> FunResult {
    let region = run_fun!(aws configure list | grep region | awk r"{print $2}")?;
    let account_id = run_fun!(aws sts get-caller-identity --query Account --output text)?;
    Ok(format!("fractalbits-builds-{region}-{account_id}"))
}

pub fn cleanup() -> CmdResult {
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
