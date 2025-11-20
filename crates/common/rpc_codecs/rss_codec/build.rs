fn main() {
    prost_build::Config::new()
        .protoc_executable("../../../../third_party/protoc/bin/protoc")
        .bytes(["."])
        .compile_protos(&["src/proto/rss_ops.proto"], &["src/proto/"])
        .unwrap();
}
