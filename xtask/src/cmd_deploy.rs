use crate::*;

const TARGET_ACCOUNT_ID: &str = "ACCOUNT_ID_TARGET"; // TARGET_EMAIL@example.com
const BUCKET_OWNER_ACCOUNT_ID: &str = "ACCOUNT_ID_OWNER";

pub fn run_cmd_deploy(
    use_s3_backend: bool,
    enable_dev_mode: bool,
    release_mode: bool,
    target_arm: bool,
) -> CmdResult {
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
            ["-Dtarget=aarch64-linux-gnu", "-Dcpu=neoverse_v1"],
        )
    } else {
        (
            "x86_64-unknown-linux-gnu",
            ["-Dtarget=x86_64-linux-gnu", "-Dcpu=cascadelake"],
        )
    };

    run_cmd! {
        info "Building Rust projects with zigbuild";
        cargo zigbuild --target $rust_build_target $rust_build_opt;

        info "Building Zig projects";
        zig build -Duse_s3_backend=$use_s3_backend
            -Denable_dev_mode=$enable_dev_mode $[zig_build_target] $zig_build_opt 2>&1;
    }?;

    info!("Uploading Rust-built binaries");
    let rust_bins = [
        "api_server",
        "root_server",
        "rss_admin",
        "fractalbits-bootstrap",
        "ebs-failover",
        "format-nss",
        "rewrk_rpc",
        "xtask",
    ];
    let prefix = if target_arm { "aarch64" } else { "x86_64" };
    let build_dir = if release_mode { "release" } else { "debug" };
    for bin in &rust_bins {
        run_cmd!(aws s3 cp target/$rust_build_target/$build_dir/$bin $bucket/$prefix/$bin)?;
    }

    info!("Uploading Zig-built binaries");
    let zig_bins = [
        "bss_server",
        "nss_server",
        "fbs",      // to create test art tree for benchmarking nss_rpc
        "test_art", // to create test.data for benchmarking nss_rpc
        "s3_blob_client",
    ];
    for bin in &zig_bins {
        run_cmd!(aws s3 cp zig-out/bin/$bin $bucket/$prefix/$bin)?;
    }

    Ok(())
}

fn get_build_bucket_name() -> FunResult {
    let awk_opts = r#"{{print $2}}"#;
    let region = run_fun!(aws configure list | grep region | awk $awk_opts)?;
    Ok(format!("fractalbits-builds-{}", region))
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
