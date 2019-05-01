fn main() {
    tower_grpc_build::Config::new()
        .enable_server(true)
        .enable_client(false)
        .build(
            &["proto/arbiter.proto"],
            &["proto"],
        )
        .unwrap_or_else(|e| panic!("protobuf compilation failed: {}", e));
}