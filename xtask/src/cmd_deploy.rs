use crate::*;

const TARGET_ACCOUNT_ID: &str = "ACCOUNT_ID_TARGET"; // TARGET_EMAIL@example.com
const BUCKET_OWNER_ACCOUNT_ID: &str = "ACCOUNT_ID_OWNER";

pub fn run_cmd_deploy(
    use_s3_backend: bool,
    enable_dev_mode: bool,
    release_mode: bool,
    target_arm: bool,
    bss_use_i3: bool,
    bootstrap_only: bool,
) -> CmdResult {
    let build_info = BUILD_INFO.get().unwrap();
    let bucket_name = get_build_bucket_name()?;
    let bucket = format!("s3://{bucket_name}");

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

    if bootstrap_only {
        // Build fractalbits-bootstrap binary only, in debug mode
        build_bootstrap_only(rust_build_target)?;
        run_cmd!(aws s3 cp target/$rust_build_target/debug/fractalbits-bootstrap $bucket/$arch/fractalbits-bootstrap)?;
        return Ok(());
    }

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

    run_cmd! {
        info "Building Zig projects";
        zig build
            -Duse_s3_backend=$use_s3_backend
            -Denable_dev_mode=$enable_dev_mode
            -Dbuild_info=$build_info
            $[zig_build_target] $zig_build_opt 2>&1;
    }?;

    if bss_use_i3 {
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

        let zig_build_target = ["-Dtarget=x86_64-linux-gnu", &format!("-Dcpu={target_cpu}")];
        run_cmd! {
            info "Building bss_server for x86_64";
            zig build
                -Duse_s3_backend=$use_s3_backend
                -Denable_dev_mode=$enable_dev_mode
                -Dbuild_info=$build_info
                $[zig_build_target] $zig_build_opt bss_server 2>&1;
        }?;
    }

    let region = run_fun!(aws configure list | grep region | awk r"{print $2}")?;
    cmd_build::build_ui(&region)?;

    info!("Uploading Rust-built binaries");
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
        run_cmd!(aws s3 cp target/$rust_build_target/$build_dir/$bin $bucket/$arch/$bin)?;
    }

    info!("Uploading Zig-built binaries");
    let zig_bins = [
        "nss_server",
        "mirrord",
        "fbs",      // to create test art tree for benchmarking nss_rpc
        "test_art", // to create test.data for benchmarking nss_rpc
        "s3_blob_client",
    ];
    for bin in &zig_bins {
        run_cmd!(aws s3 cp zig-out/bin/$bin $bucket/$arch/$bin)?;
    }

    run_cmd!(aws s3 cp ui/dist $bucket/ui --recursive)?;

    // Upload bss_server separately
    if bss_use_i3 {
        run_cmd!(aws s3 cp zig-out/bin/bss_server $bucket/x86_64/bss_server)?;
        run_cmd!(aws s3 cp target/x86_64-unknown-linux-gnu/$build_dir/fractalbits-bootstrap $bucket/x86_64/fractalbits-bootstrap)?;
        run_cmd!(aws s3 cp target/x86_64-unknown-linux-gnu/$build_dir/rewrk_rpc $bucket/x86_64/rewrk_rpc)?;
    } else {
        run_cmd!(aws s3 cp zig-out/bin/bss_server $bucket/$arch/bss_server)?;
    };

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
    Ok(format!("fractalbits-builds-{region}"))
}

pub fn update_builds_bucket_access_policy() -> CmdResult {
    let current_aws_account = run_fun!(aws sts get-caller-identity --query Account --output text)?;
    if current_aws_account != BUCKET_OWNER_ACCOUNT_ID {
        cmd_die!("Only bucket owner could update s3 build bucket policy!");
    }

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

    // Remove Block Public Access
    let new_public_access_conf = r##"{
"BlockPublicAcls": false,
"IgnorePublicAcls": false,
"BlockPublicPolicy": false,
"RestrictPublicBuckets": false
}"##;
    run_cmd! {
        aws s3api put-public-access-block
          --bucket $bucket_name
          --public-access-block-configuration $new_public_access_conf
    }?;

    // Allows s3:GetObject to anyone except anonymous users
    let new_policy = format!(
        r##"{{
"Version": "2012-10-17",
"Statement": [
  {{
    "Sid": "AllowGetObjectToAuthenticatedAWSAccounts",
    "Effect": "Allow",
    "Principal": "*",
    "Action": "s3:GetObject",
    "Resource": "arn:aws:s3:::{bucket_name}/*",
    "Condition": {{
      "StringNotEquals": {{
        "aws:PrincipalType": "Anonymous"
      }}
    }}
  }},
  {{
    "Sid": "DenyAnonymousAccess",
    "Effect": "Deny",
    "Principal": "*",
    "Action": "s3:GetObject",
    "Resource": "arn:aws:s3:::{bucket_name}/*",
    "Condition": {{
      "StringEquals": {{
        "aws:PrincipalType": "Anonymous"
      }}
    }}
  }},
  {{
    "Sid": "AllowPutObjectToTargetAccount",
    "Effect": "Allow",
    "Principal": {{
      "AWS": "arn:aws:iam::{TARGET_ACCOUNT_ID}:root"
    }},
    "Action": "s3:PutObject",
    "Resource": "arn:aws:s3:::{bucket_name}/*"
  }},
  {{
    "Sid": "AllowListBucketToTargetAccount",
    "Effect": "Allow",
    "Principal": {{
      "AWS": "arn:aws:iam::{TARGET_ACCOUNT_ID}:root"
    }},
    "Action": "s3:ListBucket",
    "Resource": "arn:aws:s3:::{bucket_name}"
  }}
]
}}"##
    );

    run_cmd! {
        info "Updating bucket policy for ${bucket_name}";
        aws s3api put-bucket-policy
            --bucket $bucket_name
            --policy $new_policy;
        info "Updating bucket policy for ${bucket_name} is done";
    }?;

    Ok(())
}
