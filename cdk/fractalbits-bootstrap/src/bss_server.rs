use super::common::*;
use cmd_lib::*;

pub fn bootstrap() -> CmdResult {
    let service = super::Service::BssServer;
    download_binary(service.as_ref())?;
    create_systemd_unit_file(service)?;
    run_cmd! {
        info "Starting bss_server.service";
        systemctl start bss_server.service;
    }?;
    Ok(())
}
