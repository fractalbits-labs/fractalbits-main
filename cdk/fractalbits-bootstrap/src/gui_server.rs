use crate::*;

pub fn bootstrap(bucket_name: &str, nss_ip: &str, rss_ip: &str) -> CmdResult {
    install_rpms(&["amazon-cloudwatch-agent", "nmap-ncat", "perf"])?;
    download_binaries(&["api_server"])?;

    let builds_bucket = format!("s3://fractalbits-builds-{}", get_current_aws_region()?);
    run_cmd!(aws s3 cp --no-progress $builds_bucket/ui $WEB_ROOT --recursive)?;

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

    // setup_cloudwatch_agent()?;
    create_systemd_unit_file("api_server", true)?;

    Ok(())
}

fn create_config(bucket_name: &str, bss_ip: &str, nss_ip: &str, rss_ip: &str) -> CmdResult {
    let aws_region = get_current_aws_region()?;
    let config_content = format!(
        r##"bss_addr = "{bss_ip}:8088"
nss_addr = "{nss_ip}:8088"
rss_addr = "{rss_ip}:8088"
bss_conn_num = 4
nss_conn_num = 4
rss_conn_num = 1
region = "{aws_region}"
port = 80
root_domain = ".localhost"
with_metrics = false
request_timeout_seconds = 115
allow_missing_or_bad_signature = true
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
