use super::common::*;
use cmd_lib::*;

pub fn bootstrap() -> CmdResult {
    download_binary("rss_admin")?;
    run_cmd!($BIN_PATH/rss_admin api-key init-test)?;

    let service_name = "root_server";
    download_binary(service_name)?;
    create_systemd_unit_file(service_name)?;
    run_cmd! {
        info "Starting root_server.service";
        systemctl start root_server.service;
    }?;
    Ok(())
}
