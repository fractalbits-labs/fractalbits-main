use super::common::*;
use cmd_lib::*;

pub fn bootstrap() -> CmdResult {
    let service_name = "bss_server";
    download_binary(service_name)?;
    create_systemd_unit_file(service_name)?;
    run_cmd! {
        info "Starting bss_server.service";
        systemctl start bss_server.service;
    }?;
    Ok(())
}
