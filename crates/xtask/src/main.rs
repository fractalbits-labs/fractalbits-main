mod cmd_bench;
mod cmd_build;
mod cmd_deploy;
mod cmd_git;
mod cmd_nightly;
mod cmd_precheckin;
mod cmd_run_tests;
mod cmd_service;
mod cmd_tool;

use clap::{ArgAction, Parser};
use cmd_build::{BUILD_INFO, BuildMode};
use cmd_lib::*;
use strum::{AsRefStr, EnumString};

pub const TS_FMT: &str = "%b %d %H:%M:%.S";
// Need to match with api_server's default config to make authentication work
pub const UI_DEFAULT_REGION: &str = "localdev";
pub const ZIG_DEBUG_OUT: &str = "target/debug/zig-out";
pub const ZIG_RELEASE_OUT_AARCH64: &str = "target/aarch64-unknown-linux-gnu/release/zig-out";
pub const ZIG_RELEASE_OUT_X86_64: &str = "target/x86_64-unknown-linux-gnu/release/zig-out";

#[derive(Parser)]
#[clap(name = "xtask", about = "Misc project related tasks")]
enum Cmd {
    #[clap(about = "Run benchmark")]
    Bench {
        #[clap(long, default_value = "write", value_enum)]
        workload: BenchWorkload,

        #[clap(long, long_help = "Run with perf tool and generate flamegraph")]
        with_flame_graph: bool,

        #[clap(
            long,
            long_help = "set max number of keys for benchmark",
            default_value = "5000000"
        )]
        keys_limit: usize,

        #[clap(value_enum)]
        service: BenchService,
    },

    #[clap(about = "Run nightly tests")]
    Nightly,

    #[clap(about = "Run precheckin tests")]
    Precheckin {
        #[clap(long, long_help = "Run s3 api tests only")]
        s3_api_only: bool,

        #[clap(long, long_help = "Run zig unit tests only")]
        zig_unit_tests_only: bool,

        #[clap(
            long,
            long_help = "Debug by recompiling and restarting api_server only"
        )]
        debug_api_server: bool,

        #[clap(long, long_help = "Run art tests in addition to other tests")]
        with_art_tests: bool,

        #[clap(long, value_enum)]
        #[arg(default_value_t)]
        data_blob_storage: DataBlobStorage,
    },

    #[clap(about = "Build the whole project")]
    Build {
        #[clap(subcommand)]
        command: Option<BuildCommand>,

        #[clap(long, long_help = "release build or not")]
        release: bool,
    },

    #[clap(about = "Service stop/init/start/restart")]
    #[command(subcommand)]
    Service(ServiceCommand),

    #[clap(about = "Run tool related commands")]
    #[command(subcommand)]
    Tools(ToolKind),

    #[clap(about = "Deploy binaries to s3 builds bucket")]
    Deploy {
        #[clap(subcommand)]
        command: Option<DeployCommand>,

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

    #[clap(about = "Run various test suites")]
    RunTests {
        #[clap(subcommand)]
        test_type: Option<TestType>,
    },

    #[clap(about = "Git repos management commands")]
    #[command(subcommand)]
    Git(GitCommand),
}

#[derive(Parser, Clone)]
pub enum BuildCommand {
    #[clap(about = "Build all components")]
    All,
    #[clap(about = "Build only zig components")]
    Zig {
        #[clap(subcommand)]
        command: Option<ZigCommand>,
    },
    #[clap(about = "Build only rust components")]
    Rust,
}

#[derive(Parser, Clone)]
pub enum ZigCommand {
    #[clap(about = "Run zig unit tests")]
    Test,
}

#[derive(Parser, Clone)]
pub enum DeployCommand {
    #[clap(about = "Cleanup builds bucket (empty and delete)")]
    Cleanup,
}

#[derive(Clone, AsRefStr, EnumString, clap::ValueEnum)]
enum BenchWorkload {
    Read,
    Write,
}

#[derive(Clone, EnumString, clap::ValueEnum)]
enum BenchService {
    NssRpc,
    BssRpc,
}

#[derive(Parser, Clone, EnumString, PartialEq)]
pub enum ServiceAction {
    Init,
    Stop,
    Start,
    Restart,
    Status,
}

#[derive(AsRefStr, EnumString, Copy, Clone, PartialEq, clap::ValueEnum)]
#[strum(serialize_all = "snake_case")]
#[clap(rename_all = "snake_case")]
pub enum ServiceName {
    GuiServer,
    ApiServer,
    Bss0,
    Bss1,
    Bss2,
    Bss3,
    Bss4,
    Bss5,
    NssRoleAgentA,
    Nss,
    NssRoleAgentB,
    Mirrord,
    Rss,
    All,
    Minio,
    MinioAz1,
    MinioAz2,
    DdbLocal,
}

impl ServiceName {
    pub fn bss_id(&self) -> Option<u32> {
        match self {
            ServiceName::Bss0 => Some(0),
            ServiceName::Bss1 => Some(1),
            ServiceName::Bss2 => Some(2),
            ServiceName::Bss3 => Some(3),
            ServiceName::Bss4 => Some(4),
            ServiceName::Bss5 => Some(5),
            _ => None,
        }
    }

    pub fn all_bss_services() -> Vec<ServiceName> {
        vec![
            ServiceName::Bss0,
            ServiceName::Bss1,
            ServiceName::Bss2,
            ServiceName::Bss3,
            ServiceName::Bss4,
            ServiceName::Bss5,
        ]
    }
}

#[derive(AsRefStr, EnumString, Copy, Clone, Default, clap::ValueEnum)]
#[strum(serialize_all = "snake_case")]
#[clap(rename_all = "snake_case")]
pub enum DataBlobStorage {
    #[default]
    S3HybridSingleAz,
    S3ExpressMultiAz,
}

#[derive(AsRefStr, EnumString, Copy, Clone, Default, PartialEq, clap::ValueEnum)]
pub enum DeployMode {
    #[default]
    All,
    Zig,
    Rust,
    Bootstrap,
    Ui,
}

#[derive(AsRefStr, EnumString, Copy, Clone, Default)]
pub enum NssRole {
    #[default]
    Active,
    Solo,
}

#[derive(Clone, Default)]
pub struct InitConfig {
    pub for_gui: bool,
    pub data_blob_storage: DataBlobStorage,
}

#[derive(Parser, Clone)]
pub enum ServiceCommand {
    Init {
        #[clap(default_value = "all", value_enum)]
        service: ServiceName,

        #[clap(long, long_help = "release build or not")]
        release: bool,

        #[clap(long, long_help = "start service for gui")]
        for_gui: bool,

        #[clap(long, value_enum)]
        #[arg(default_value_t)]
        data_blob_storage: DataBlobStorage,
    },
    Stop {
        #[clap(default_value = "all", value_enum)]
        service: ServiceName,
    },
    Start {
        #[clap(default_value = "all", value_enum)]
        service: ServiceName,
    },
    Restart {
        #[clap(default_value = "all", value_enum)]
        service: ServiceName,
    },
    Status {
        #[clap(default_value = "all", value_enum)]
        service: ServiceName,
    },
}

#[derive(Parser, Clone)]
pub enum TestType {
    All,
    MultiAz {
        #[clap(subcommand)]
        subcommand: MultiAzTestType,
    },
    LeaderElection,
}

#[derive(Parser, Clone, EnumString)]
pub enum MultiAzTestType {
    All,
    DataBlobTracking,
    DataBlobResyncing,
}

#[derive(Parser, Clone)]
pub enum GitCommand {
    #[clap(about = "List all configured git repos")]
    List,

    #[clap(about = "Show git repo status")]
    Status,

    #[clap(about = "Initialize all git repos")]
    Init,

    #[clap(about = "Run a command in each git repo")]
    Foreach {
        #[clap(required = true, num_args = 1.., value_name = "COMMAND", allow_hyphen_values = true)]
        command: Vec<String>,
    },
}

#[derive(Parser, Clone)]
enum ToolKind {
    GenUuids {
        #[clap(short = 'n', long_help = "Number of uuids", default_value = "1000000")]
        num: usize,

        #[clap(short = 'f', long_help = "File output", default_value = "uuids.data")]
        file: String,
    },
    DescribeStack {
        #[clap(
            long_help = "CloudFormation stack name",
            default_value = "FractalbitsVpcStack"
        )]
        stack_name: String,
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
        Cmd::Build { command, release } => match command {
            Some(build_cmd) => match build_cmd {
                BuildCommand::All => cmd_build::build_all(release)?,
                BuildCommand::Zig { command } => match command {
                    Some(ZigCommand::Test) => cmd_build::run_zig_unit_tests()?,
                    None => {
                        let build_mode = cmd_build::build_mode(release);
                        cmd_build::build_zig_servers(build_mode)?;
                    }
                },
                BuildCommand::Rust => {
                    let build_mode = cmd_build::build_mode(release);
                    cmd_build::build_rust_servers(build_mode)?;
                }
            },
            None => {
                // Default to building all components
                cmd_build::build_all(release)?;
            }
        },
        Cmd::Precheckin {
            s3_api_only,
            zig_unit_tests_only,
            debug_api_server,
            with_art_tests,
            data_blob_storage,
        } => cmd_precheckin::run_cmd_precheckin(
            s3_api_only,
            zig_unit_tests_only,
            debug_api_server,
            with_art_tests,
            data_blob_storage,
        )?,
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
        Cmd::Tools(tool_kind) => cmd_tool::run_cmd_tool(tool_kind)?,
        Cmd::Deploy {
            command,
            use_s3_backend,
            enable_dev_mode,
            release,
            target_arm,
            bss_use_i3,
            mode,
        } => match command {
            Some(DeployCommand::Cleanup) => cmd_deploy::cleanup_builds_bucket()?,
            None => cmd_deploy::run_cmd_deploy(
                use_s3_backend,
                enable_dev_mode,
                release,
                target_arm,
                bss_use_i3,
                mode,
            )?,
        },
        Cmd::RunTests { test_type } => {
            let test_type = test_type.unwrap_or(TestType::All);
            cmd_run_tests::run_tests(test_type).await?
        }
        Cmd::Git(git_cmd) => cmd_git::run_cmd_git(git_cmd)?,
    }
    Ok(())
}
