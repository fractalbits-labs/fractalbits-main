use clap::Parser;
use cmd_lib::*;

#[derive(Parser)]
#[clap(
    name = "fractalbits-bootstrap",
    about = "Bootstrap for cloud ec2 instances"
)]
enum Cmd {
    #[command(name = "api_server")]
    ApiServer,
    #[command(name = "bss_server")]
    BssServer,
    #[command(name = "nss_server")]
    NssServer,
    #[command(name = "root_server")]
    RootServer,
}

#[cmd_lib::main]
fn main() -> CmdResult {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info"))
        .format_target(false)
        .init();

    match Cmd::parse() {
        Cmd::ApiServer => bootstrap_api_server(),
        Cmd::BssServer => bootstrap_bss_server(),
        Cmd::NssServer => bootstrap_nss_server(),
        Cmd::RootServer => bootstrap_root_server(),
    }
}

fn bootstrap_api_server() -> CmdResult {
    info!("bootstrapping api_server ...");
    Ok(())
}

fn bootstrap_bss_server() -> CmdResult {
    info!("bootstrapping bss_server ...");
    Ok(())
}

fn bootstrap_nss_server() -> CmdResult {
    info!("bootstrapping nss_server ...");
    Ok(())
}

fn bootstrap_root_server() -> CmdResult {
    info!("bootstrapping root_server ...");
    Ok(())
}
