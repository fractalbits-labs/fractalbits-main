use cmd_lib::*;

pub fn run_cmd_deploy() -> CmdResult {
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

    run_cmd! {
        info "Building with zigbuild";
        cargo zigbuild --target x86_64-unknown-linux-gnu;
    }?;

    info!("Uploading Rust-built binaries");
    let rust_bins = [
        "api_server",
        "root_server",
        "rss_admin",
        "fractalbits-bootstrap",
    ];
    for bin in &rust_bins {
        run_cmd!(aws s3 cp target/x86_64-unknown-linux-gnu/release/$bin $bucket)?;
    }

    run_cmd! {
        info "Building Zig project";
        zig build -Dcpu=x86_64_v3 2>&1;
    }?;

    info!("Uploading Zig-built binaries");
    let zig_bins = ["bss_server", "nss_server", "mkfs"];
    for bin in &zig_bins {
        run_cmd!(aws s3 cp zig-out/bin/$bin $bucket)?;
    }

    Ok(())
}
