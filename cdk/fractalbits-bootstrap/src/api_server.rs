use crate::*;

pub fn bootstrap(
    service_id: &str,
    bucket_name: &str,
    nss_ip: &str,
    rss_ip: &str,
    for_bench: bool,
) -> CmdResult {
    install_rpms(&["amazon-cloudwatch-agent", "nmap-ncat", "perf"])?;
    download_binaries(&["api_server"])?;

    let bss_service_name = "bss-server.fractalbits.local";
    info!("Waiting for bss with dns name: {bss_service_name}");
    let bss_ip = loop {
        match run_fun!(dig +short $bss_service_name) {
            Ok(ip) if !ip.is_empty() => break ip,
            _ => std::thread::sleep(std::time::Duration::from_secs(1)),
        }
    };
    for (role, ip) in [("bss", bss_ip.as_str()), ("rss", rss_ip), ("nss", nss_ip)] {
        info!("Waiting for {role} node with ip {ip} to be ready");
        while run_cmd!(nc -z $ip 8088 &>/dev/null).is_err() {
            std::thread::sleep(std::time::Duration::from_secs(1));
        }
        info!("{role} node can be reached (`nc -z {ip} 8088` is ok)");
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
    create_cloudmap_register_and_deregister_service(service_id)?;

    Ok(())
}

fn create_config(bucket_name: &str, bss_ip: &str, nss_ip: &str, rss_ip: &str) -> CmdResult {
    let aws_region = get_current_aws_region()?;
    let config_content = format!(
        r##"bss_addr = "{bss_ip}:8088"
nss_addr = "{nss_ip}:8088"
rss_addr = "{rss_ip}:8088"
bss_conn_num = 2
nss_conn_num = 2
rss_conn_num = 1
region = "{aws_region}"
port = 80
root_domain = ".localhost"
with_metrics = false
http_request_timeout_seconds = 5
rpc_timeout_seconds = 4
allow_missing_or_bad_signature = false
web_root = "{WEB_ROOT}"

[s3_cache]
s3_host = "http://s3.{aws_region}.amazonaws.com"
s3_port = 80
s3_region = "{aws_region}"
s3_bucket = "{bucket_name}"
"##
    );
    run_cmd! {
        mkdir -p $ETC_PATH;
        echo $config_content > $ETC_PATH/$API_SERVER_CONFIG
    }?;
    Ok(())
}
