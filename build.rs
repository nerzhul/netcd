fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Build the etcdserverpb service proto + dependencies.
    // tonic-build handles the gRPC server/client traits; prost handles messages.
    tonic_build::configure()
        .build_server(true)
        .build_client(true)
        .compile_protos(
            &[
                "proto/etcdserverpb.proto",
                "proto/grpc/health/v1/health.proto",
            ],
            &["proto"],
        )?;
    println!("cargo:rerun-if-changed=proto/");
    println!("cargo:rerun-if-changed=build.rs");
    Ok(())
}
