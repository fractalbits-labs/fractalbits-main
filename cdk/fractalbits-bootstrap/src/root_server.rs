use super::common::*;
use cmd_lib::*;

pub fn bootstrap() -> CmdResult {
    download_binary("rss_admin")?;
    run_cmd!($BIN_PATH/rss_admin api-key init-test)?;

    let service = super::Service::RootServer;
    download_binary(service.as_ref())?;
    create_systemd_unit_file(service)?;
    run_cmd! {
        info "Starting root_server.service";
        systemctl start root_server.service;
    }?;
    Ok(())
}
