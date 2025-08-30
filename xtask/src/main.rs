mod cmd_bench;
mod cmd_build;
mod cmd_deploy;
mod cmd_nightly;
mod cmd_precheckin;
mod cmd_run_tests;
mod cmd_service;
mod cmd_tool;

use clap::{ArgAction, Parser};
use cmd_build::{BuildMode, BUILD_INFO};
use cmd_lib::*;
use strum::{AsRefStr, EnumString};

pub const TEST_BUCKET_ROOT_BLOB_NAME: &str = "947ef2be-44b2-4ac2-969b-2574eb85662b";
pub const TS_FMT: &str = "%b %d %H:%M:%.S";
// Need to match with api_server's default config to make authentication work
pub const UI_DEFAULT_REGION: &str = "localdev";

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
        s3_api_only: bool,

        #[clap(
            long,
            long_help = "Debug by recompiling and restarting api_server only"
        )]
        debug_api_server: bool,
    },

    #[clap(about = "Build the whole project")]
    Build {
        #[clap(long, long_help = "release build or not")]
        release: bool,
    },

    #[clap(about = "Service stop/init/start/restart")]
    #[command(subcommand)]
    Service(ServiceCommand),

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

        #[clap(long, default_value = "all", value_enum)]
        mode: DeployMode,
    },

    #[clap(about = "Grant S3 build bucket policy")]
    GrantBuildBucket,

    #[clap(about = "Run various test suites")]
    RunTests {
        #[clap(subcommand)]
        test_type: Option<TestType>,
    },
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
    Status,
}

#[derive(AsRefStr, EnumString, Copy, Clone, PartialEq)]
#[strum(serialize_all = "snake_case")]
pub enum ServiceName {
    GuiServer,
    ApiServer,
    Bss,
    NssRoleAgentA,
    Nss,
    NssRoleAgentB,
    Mirrord,
    Rss,
    DataBlobResyncServer,
    All,
    Minio,
    MinioAz1,
    MinioAz2,
    DdbLocal,
}

#[derive(AsRefStr, EnumString, Copy, Clone, Default)]
#[strum(serialize_all = "snake_case")]
pub enum DataBlobStorage {
    #[default]
    HybridSingleAz,
    S3ExpressMultiAz,
    S3ExpressSingleAz,
}

impl std::fmt::Display for DataBlobStorage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_ref())
    }
}

#[derive(AsRefStr, EnumString, Copy, Clone, Default, PartialEq, clap::ValueEnum)]
#[strum(serialize_all = "snake_case")]
pub enum DeployMode {
    #[default]
    All,
    Zig,
    Rust,
    Bootstrap,
    Ui,
}

impl std::fmt::Display for DeployMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_ref())
    }
}

#[derive(AsRefStr, EnumString, Copy, Clone, Default)]
#[strum(serialize_all = "snake_case")]
pub enum NssRole {
    #[default]
    Active,
    Solo,
}

impl std::fmt::Display for NssRole {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_ref())
    }
}

#[derive(Clone, Default)]
pub struct InitConfig {
    pub for_gui: bool,
    pub data_blob_storage: DataBlobStorage,
}

#[derive(Parser, Clone)]
#[clap(rename_all = "snake_case")]
pub enum ServiceCommand {
    Init {
        #[clap(
            long_help = "all/api_server/bss/nss/nss_role_agent_a/nss_role_agent_b/minio/ddb_local",
            default_value = "all"
        )]
        service: ServiceName,

        #[clap(long, long_help = "release build or not")]
        release: bool,

        #[clap(long, long_help = "start service for gui")]
        for_gui: bool,

        #[clap(
            long,
            long_help = "Data blob storage mode: hybrid_single_az, s3_express_single_az or s3_express_multi_az"
        )]
        #[arg(default_value_t)]
        data_blob_storage: DataBlobStorage,
    },
    Stop {
        #[clap(
            long_help = "all/api_server/bss/nss/nss_role_agent_a/nss_role_agent_b/minio/ddb_local",
            default_value = "all"
        )]
        service: ServiceName,
    },
    Start {
        #[clap(
            long_help = "all/api_server/bss/nss/nss_role_agent_a/nss_role_agent_b/minio/ddb_local",
            default_value = "all"
        )]
        service: ServiceName,
    },
    Restart {
        #[clap(
            long_help = "all/api_server/bss/nss/nss_role_agent_a/nss_role_agent_b/minio/ddb_local",
            default_value = "all"
        )]
        service: ServiceName,
    },
    Status {
        #[clap(
            long_help = "all/api_server/bss/nss/nss_role_agent_a/nss_role_agent_b/minio/ddb_local",
            default_value = "all"
        )]
        service: ServiceName,
    },
}

#[derive(Parser, Clone)]
#[clap(rename_all = "snake_case")]
pub enum TestType {
    All,
    MultiAz {
        #[clap(subcommand)]
        subcommand: MultiAzTestType,
    },
    LeaderElection,
}

#[derive(Parser, Clone, EnumString)]
#[strum(serialize_all = "snake_case")]
#[clap(rename_all = "snake_case")]
pub enum MultiAzTestType {
    All,
    DataBlobTracking,
    DataBlobResyncing,
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

#[tokio::main]
#[cmd_lib::main]
async fn main() -> CmdResult {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info"))
        .format_target(false)
        .init();
    rlimit::increase_nofile_limit(1000000).unwrap();
    BUILD_INFO.get_or_init(cmd_build::build_info);

    match Cmd::parse() {
        Cmd::Build { release } => {
            let build_mode = cmd_build::build_mode(release);
            cmd_build::build_rust_servers(build_mode)?;
            cmd_build::build_zig_servers(build_mode)?;
            if release {
                cmd_build::build_rewrk_rpc()?;
            }
            cmd_build::build_ui(UI_DEFAULT_REGION)?;
        }
        Cmd::Precheckin {
            s3_api_only,
            debug_api_server,
        } => cmd_precheckin::run_cmd_precheckin(s3_api_only, debug_api_server)?,
        Cmd::Nightly => cmd_nightly::run_cmd_nightly()?,
        Cmd::Bench {
            service,
            workload,
            with_flame_graph,
            keys_limit,
        } => {
            let mut service_name = ServiceName::All;
            cmd_bench::prepare_bench(with_flame_graph)?;
            cmd_bench::run_cmd_bench(
                service,
                workload,
                with_flame_graph,
                keys_limit,
                &mut service_name,
            )
            .inspect_err(|_| {
                cmd_service::stop_service(service_name).unwrap();
            })?;
        }
        Cmd::Service(service_cmd) => match service_cmd {
            ServiceCommand::Init {
                service,
                release,
                for_gui,
                data_blob_storage,
            } => {
                let init_config = InitConfig {
                    for_gui,
                    data_blob_storage,
                };
                cmd_service::init_service(service, cmd_build::build_mode(release), init_config)?;
            }
            ServiceCommand::Stop { service } => {
                cmd_service::stop_service(service)?;
            }
            ServiceCommand::Start { service } => {
                cmd_service::start_service(service)?;
            }
            ServiceCommand::Restart { service } => {
                cmd_service::stop_service(service)?;
                cmd_service::start_service(service)?;
            }
            ServiceCommand::Status { service } => {
                cmd_service::show_service_status(service)?;
            }
        },
        Cmd::Tool(tool_kind) => cmd_tool::run_cmd_tool(tool_kind)?,
        Cmd::Deploy {
            use_s3_backend,
            enable_dev_mode,
            release,
            target_arm,
            bss_use_i3,
            mode,
        } => cmd_deploy::run_cmd_deploy(
            use_s3_backend,
            enable_dev_mode,
            release,
            target_arm,
            bss_use_i3,
            mode,
        )?,
        Cmd::GrantBuildBucket => cmd_deploy::update_builds_bucket_access_policy()?,
        Cmd::RunTests { test_type } => {
            let test_type = test_type.unwrap_or(TestType::All);
            cmd_run_tests::run_tests(test_type).await?
        }
    }
    Ok(())
}
