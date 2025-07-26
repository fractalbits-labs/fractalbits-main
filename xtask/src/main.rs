mod cmd_bench;
mod cmd_build;
mod cmd_deploy;
mod cmd_nightly;
mod cmd_precheckin;
mod cmd_service;
mod cmd_tool;

use clap::{ArgAction, Parser};
use cmd_build::{build_mode, BuildMode, BUILD_INFO};
use cmd_lib::*;
use strum::{AsRefStr, EnumString};

pub const TEST_BUCKET_ROOT_BLOB_NAME: &str = "947ef2be-44b2-4ac2-969b-2574eb85662b";
pub const TS_FMT: &str = "%b %d %H:%M:%.S";
pub const NSS_SERVER_BENCH_CONFIG: &str = "nss_server_bench_config.toml";
pub const API_SERVER_GUI_CONFIG: &str = "api_server_gui_config.toml";

#[derive(Parser)]
#[command(rename_all = "snake_case")]
#[clap(name = "xtask", about = "Misc project related tasks")]
enum Cmd {
    #[clap(about = "Run benchmark for api_server/nss_rpc/bss_rpc")]
    Bench {
        #[clap(
            long,
            long_help = "Run with pre-defined workload (read/write)",
            default_value = "write"
        )]
        workload: BenchWorkload,

        #[clap(long, long_help = "Run with perf tool and generate flamegraph")]
        with_flame_graph: bool,

        #[clap(long, long_help = "Nss data on local disks (without s3)")]
        nss_data_on_local: bool,

        #[clap(
            long,
            long_help = "set max number of keys for benchmark",
            default_value = "5000000"
        )]
        keys_limit: usize,

        #[clap(long_help = "api_server/nss_rpc/bss_rpc")]
        service: BenchService,
    },

    #[clap(about = "Run nightly tests")]
    Nightly,

    #[clap(about = "Run precheckin tests")]
    Precheckin {
        #[clap(long, long_help = "Run s3 api tests only")]
        api_only: bool,
    },

    #[clap(about = "Build the whole project")]
    Build {
        #[clap(long, long_help = "release build or not")]
        release: bool,
    },

    #[clap(about = "Service stop/init/start/restart")]
    Service {
        #[clap(long_help = "stop/int/start/restart")]
        action: ServiceAction,

        #[clap(
            long_help = "all/api_server/bss/nss/minio/ddb_local",
            default_value = "all"
        )]
        service: ServiceName,

        #[clap(long, long_help = "release build or not")]
        release: bool,

        #[clap(long, long_help = "start service for gui")]
        for_gui: bool,
    },

    #[clap(about = "Run tool related commands (gen_uuids only for now)")]
    #[command(subcommand)]
    Tool(ToolKind),

    #[clap(about = "Deploy binaries to s3 builds bucket")]
    Deploy {
        #[clap(long, action=ArgAction::Set, default_value = "true", num_args = 0..=1)]
        release: bool,

        #[clap(long, action=ArgAction::Set, default_value = "true", num_args = 0..=1)]
        target_arm: bool,

        #[clap(long, action=ArgAction::Set, default_value = "true", num_args = 0..=1)]
        use_s3_backend: bool,

        #[clap(long)]
        enable_dev_mode: bool,

        #[clap(long)]
        bss_use_i3: bool,
    },

    #[clap(about = "Grant S3 build bucket policy")]
    GrantBuildBucket,
}

#[derive(Clone, AsRefStr, EnumString)]
#[strum(serialize_all = "snake_case")]
enum BenchWorkload {
    Read,
    Write,
}

#[derive(Clone, EnumString)]
#[strum(serialize_all = "snake_case")]
enum BenchService {
    ApiServer,
    NssRpc,
    BssRpc,
}

#[derive(Parser, Clone, EnumString, PartialEq)]
#[strum(serialize_all = "snake_case")]
pub enum ServiceAction {
    Init,
    Stop,
    Start,
    Restart,
}

#[derive(AsRefStr, EnumString, Copy, Clone)]
#[strum(serialize_all = "snake_case")]
pub enum ServiceName {
    ApiServer,
    Bss,
    Nss,
    Rss,
    All,
    Minio,
    DdbLocal,
}

#[derive(Parser, Clone)]
#[clap(rename_all = "snake_case")]
enum ToolKind {
    GenUuids {
        #[clap(short = 'n', long_help = "Number of uuids", default_value = "1000000")]
        num: usize,

        #[clap(short = 'f', long_help = "File output", default_value = "uuids.data")]
        file: String,
    },
}

#[cmd_lib::main]
fn main() -> CmdResult {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info"))
        .format_target(false)
        .init();
    rlimit::increase_nofile_limit(1000000).unwrap();
    BUILD_INFO.get_or_init(cmd_build::build_info);

    match Cmd::parse() {
        Cmd::Build { release } => {
            let build_mode = build_mode(release);
            cmd_build::build_rss_api_server(build_mode)?;
            cmd_build::build_bss_nss_server(build_mode)?;
            cmd_build::build_rewrk_rpc()?;
            cmd_build::build_ui()?;
        }
        Cmd::Precheckin { api_only } => cmd_precheckin::run_cmd_precheckin(api_only)?,
        Cmd::Nightly => cmd_nightly::run_cmd_nightly()?,
        Cmd::Bench {
            service,
            workload,
            with_flame_graph,
            nss_data_on_local,
            keys_limit,
        } => {
            let mut service_name = ServiceName::All;
            cmd_bench::prepare_bench(with_flame_graph)?;
            cmd_bench::run_cmd_bench(
                service,
                workload,
                with_flame_graph,
                nss_data_on_local,
                keys_limit,
                &mut service_name,
            )
            .inspect_err(|_| {
                cmd_service::run_cmd_service(
                    service_name,
                    ServiceAction::Stop,
                    BuildMode::Release,
                    false,
                )
                .unwrap();
            })?;
        }
        Cmd::Service {
            action,
            service,
            release,
            for_gui,
        } => cmd_service::run_cmd_service(service, action, build_mode(release), for_gui)?,
        Cmd::Tool(tool_kind) => cmd_tool::run_cmd_tool(tool_kind)?,
        Cmd::Deploy {
            use_s3_backend,
            enable_dev_mode,
            release,
            target_arm,
            bss_use_i3,
        } => cmd_deploy::run_cmd_deploy(
            use_s3_backend,
            enable_dev_mode,
            release,
            target_arm,
            bss_use_i3,
        )?,
        Cmd::GrantBuildBucket => cmd_deploy::update_builds_bucket_access_policy()?,
    }
    Ok(())
}
