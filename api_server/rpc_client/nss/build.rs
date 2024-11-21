use std::io::Result;

fn main() -> Result<()> {
    let mut prost_build = prost_build::Config::new();
    prost_build
        .bytes(["."])
        .compile_protos(&["src/proto/nss_ops.proto"], &["proto/"])?;
    Ok(())
}
