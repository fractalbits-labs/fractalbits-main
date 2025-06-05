use cmd_lib::*;

pub const BIN_PATH: &str = "/opt/fractalbits/bin/";
pub const ETC_PATH: &str = "/opt/fractalbits/etc/";
pub const NSS_SERVER_CONFIG: &str = "nss_server_cloud_config.toml";
pub const API_SERVER_CONFIG: &str = "api_server_cloud_config.toml";

pub fn download_binary(file_name: &str) -> CmdResult {
    let builds_bucket = format!("s3://fractalbits-builds-{}", get_current_aws_region()?);
    run_cmd! {
        info "Downloading $file_name from $builds_bucket to $BIN_PATH";
        aws s3 cp --no-progress $builds_bucket/$file_name $BIN_PATH;
        chmod +x $BIN_PATH/$file_name
    }?;
    Ok(())
}

pub fn create_systemd_unit_file(service_name: &str) -> CmdResult {
    let mut requires = "";
    let exec_start = match service_name {
        "api_server" => format!("{BIN_PATH}{service_name} -c {ETC_PATH}{API_SERVER_CONFIG}"),
        "nss_server" => {
            requires = "var-data.mount";
            format!("{BIN_PATH}{service_name} -c {ETC_PATH}{NSS_SERVER_CONFIG}")
        }
        "bss_server" => format!("{BIN_PATH}{service_name}"),
        "root_server" => format!("{BIN_PATH}{service_name}"),
        _ => unreachable!(),
    };
    let systemd_unit_content = format!(
        r##"[Unit]
Description={service_name} Service
After=network-online.target {requires}
Requires={requires}

[Service]
LimitNOFILE=1000000
LimitCORE=infinity
WorkingDirectory=/var/data
ExecStart={exec_start}

[Install]
WantedBy=multi-user.target
"##
    );
    let service_file = format!("{service_name}.service");

    run_cmd! {
        mkdir -p /var/data;
        mkdir -p $ETC_PATH;
        echo $systemd_unit_content > ${ETC_PATH}${service_file};
        info "Linking ${ETC_PATH}${service_file} into /etc/systemd/system";
        systemctl link ${ETC_PATH}${service_file} --force --quiet;
        systemctl enable $service_name;
    }?;
    Ok(())
}

// TODO: use imds sdk
pub fn get_current_aws_region() -> FunResult {
    const HDR_TOKEN_TTL: &str = "X-aws-ec2-metadata-token-ttl-seconds";
    const HDR_TOKEN: &str = "X-aws-ec2-metadata-token";
    const IMDS_URL: &str = "http://169.254.169.254";
    const TOKEN_PATH: &str = "latest/api/token";
    const ID_PATH: &str = "latest/dynamic/instance-identity/document";

    let token = run_fun!(curl -sS -X PUT -H "$HDR_TOKEN_TTL: 21600" "$IMDS_URL/$TOKEN_PATH")?;
    run_fun!(curl -sS -H "$HDR_TOKEN: $token" "$IMDS_URL/$ID_PATH" | jq -r .region)
}
