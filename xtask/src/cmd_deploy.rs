use cmd_lib::*;

pub fn run_cmd_deploy(use_s3_backend: bool) -> CmdResult {
    // Get AWS region
    let awk_opts = r#"{{print $2}}"#;
    let region = run_fun!(aws configure list | grep region | awk $awk_opts)?;
    let bucket_name = format!("fractalbits-builds-{}", region);
    let bucket = format!("s3://{bucket_name}");

    // Check if the bucket exists; create if it doesn't
    let bucket_exists = run_cmd!(aws s3api head-bucket --bucket $bucket_name &>/dev/null).is_ok();
    if !bucket_exists {
        run_cmd! {
            info "Creating bucket $bucket";
            aws s3 mb $bucket
        }?;
    }
    update_builds_bucket_access_policy(&bucket_name)?;

    const BUILD_MODE: &str = "release";
    run_cmd! {
        info "Building with zigbuild";
        cargo zigbuild --target x86_64-unknown-linux-gnu --$BUILD_MODE;
    }?;

    info!("Uploading Rust-built binaries");
    let rust_bins = [
        "api_server",
        "root_server",
        "rss_admin",
        "fractalbits-bootstrap",
        "ebs-failover",
        "format-ebs",
        "rewrk_rpc",
    ];
    for bin in &rust_bins {
        run_cmd!(aws s3 cp target/x86_64-unknown-linux-gnu/$BUILD_MODE/$bin $bucket)?;
    }

    run_cmd! {
        info "Building Zig project";
        zig build -Dcpu=x86_64_v3 -Duse_s3_backend=$use_s3_backend --release=safe 2>&1;
    }?;

    info!("Uploading Zig-built binaries");
    let zig_bins = [
        "bss_server",
        "nss_server",
        "mkfs",
        "fbs",      // to create test art tree for benchmarking nss_rpc
        "test_art", // to create test.data for benchmarking nss_rpc
    ];
    for bin in &zig_bins {
        run_cmd!(aws s3 cp zig-out/bin/$bin $bucket)?;
    }

    Ok(())
}

fn update_builds_bucket_access_policy(bucket_name: &str) -> CmdResult {
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
  }}
]
}}"##
    );

    run_cmd! {
          aws s3api put-bucket-policy
            --bucket $bucket_name
            --policy $new_policy
    }?;

    Ok(())
}
