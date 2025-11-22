use cmd_lib::*;

const THIRD_PARTY_PROTOC: &str = "../../../../third_party/protoc/bin/protoc";

#[cmd_lib::main]
fn main() -> CmdResult {
    let mut prost_build = &mut prost_build::Config::new();
    if run_cmd!(bash -c "command -v protoc").is_err() {
        if std::path::Path::new(THIRD_PARTY_PROTOC).exists() {
            prost_build = prost_build.protoc_executable(THIRD_PARTY_PROTOC);
        }
    }

    prost_build
        .bytes(["."])
        .compile_protos(&["src/proto/nss_ops.proto"], &["src/proto/"])
}
