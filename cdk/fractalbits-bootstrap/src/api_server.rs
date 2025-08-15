use crate::*;

pub fn bootstrap(bucket_name: &str, nss_ip: &str, rss_ip: &str, for_bench: bool) -> CmdResult {
    install_rpms(&["amazon-cloudwatch-agent", "nmap-ncat", "perf"])?;
    download_binaries(&["api_server"])?;

    // Check if we're using S3 Express One Zone by looking at bucket name
    let is_s3_express = bucket_name.ends_with("--x-s3");

    let bss_ip = if is_s3_express {
        info!("Using S3 Express One Zone storage, skipping BSS server");
        String::new()
    } else {
        info!("Waiting for bss");
        let bss_ip = get_service_ips("bss-server", 1)[0].clone();
        for (role, ip) in [("bss", bss_ip.as_str()), ("rss", rss_ip), ("nss", nss_ip)] {
            info!("Waiting for {role} node with ip {ip} to be ready");
            while run_cmd!(nc -z $ip 8088 &>/dev/null).is_err() {
                std::thread::sleep(std::time::Duration::from_secs(1));
            }
            info!("{role} node can be reached (`nc -z {ip} 8088` is ok)");
        }
        bss_ip
    };

    // For S3 Express, only wait for RSS and NSS
    if is_s3_express {
        for (role, ip) in [("rss", rss_ip), ("nss", nss_ip)] {
            info!("Waiting for {role} node with ip {ip} to be ready");
            while run_cmd!(nc -z $ip 8088 &>/dev/null).is_err() {
                std::thread::sleep(std::time::Duration::from_secs(1));
            }
            info!("{role} node can be reached (`nc -z {ip} 8088` is ok)");
        }
    }

    create_config(bucket_name, &bss_ip, nss_ip, rss_ip)?;

    if for_bench {
        // Try to download tools for micro-benchmarking
        download_binaries(&["rewrk_rpc", "fbs", "test_art"])?;
        // Testing data for bss-rpc
        xtask_tools::gen_uuids(1_000_000, "/data/uuids.data")?;
        // Testing data for bss-rpc
        run_cmd! {
            cd /data;
            info "Generating random 10_000_000 keys for nss-rpc";
            /opt/fractalbits/bin/test_art --gen --size 10000000;
        }?;
    }

    // setup_cloudwatch_agent()?;
    create_systemd_unit_file("api_server", true)?;
    create_ddb_register_and_deregister_service("api-server")?;

    Ok(())
}

pub fn create_config(bucket_name: &str, bss_ip: &str, nss_ip: &str, rss_ip: &str) -> CmdResult {
    let aws_region = get_current_aws_region()?;
    let aws_az = get_current_aws_az()?;
    let num_cores = run_fun!(nproc)?;
    let is_s3_express = bucket_name.ends_with("--x-s3");

    let config_content = if is_s3_express {
        // S3 Express configuration
        format!(
            r##"nss_addr = "{nss_ip}:8088"
rss_addr = "{rss_ip}:8088"
nss_conn_num = {num_cores}
rss_conn_num = 1
region = "{aws_region}"
port = 80
root_domain = ".localhost"
with_metrics = true
http_request_timeout_seconds = 5
rpc_timeout_seconds = 4
allow_missing_or_bad_signature = false

[blob_storage]
backend = "s3_express_single_az"

[blob_storage.s3_express_single_az]
s3_host = "https://s3.{aws_region}.amazonaws.com"
s3_port = 443
s3_region = "{aws_region}"
s3_bucket = "{bucket_name}"
az = "{aws_az}"
force_path_style = false
"##
        )
    } else {
        // Hybrid configuration
        format!(
            r##"bss_addr = "{bss_ip}:8088"
nss_addr = "{nss_ip}:8088"
rss_addr = "{rss_ip}:8088"
bss_conn_num = {num_cores}
nss_conn_num = {num_cores}
rss_conn_num = 1
region = "{aws_region}"
port = 80
root_domain = ".localhost"
with_metrics = false
http_request_timeout_seconds = 5
rpc_timeout_seconds = 4
allow_missing_or_bad_signature = false

[blob_storage]
backend = "hybrid_single_az"

[blob_storage.bss]
addr = "{bss_ip}:8088"
conn_num = {num_cores}

[blob_storage.s3_hybrid]
s3_host = "http://s3.{aws_region}.amazonaws.com"
s3_port = 80
s3_region = "{aws_region}"
s3_bucket = "{bucket_name}"
"##
        )
    };

    run_cmd! {
        mkdir -p $ETC_PATH;
        echo $config_content > $ETC_PATH/$API_SERVER_CONFIG
    }?;
    Ok(())
}
