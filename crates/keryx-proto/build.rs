fn main() -> Result<(), Box<dyn std::error::Error>> {
    let protoc = protoc_bin_vendored::protoc_bin_path()?;
    std::env::set_var("PROTOC", protoc);

    let proto_root = "../../proto";
    let files = [
        "../../proto/hermes/keryx/v1/common.proto",
        "../../proto/hermes/keryx/v1/agent.proto",
        "../../proto/hermes/keryx/v1/capability.proto",
        "../../proto/hermes/keryx/v1/task.proto",
        "../../proto/hermes/keryx/v1/event.proto",
        "../../proto/hermes/keryx/v1/policy.proto",
        "../../proto/hermes/keryx/v1/daemon.proto",
        "../../proto/hermes/keryx/v1/relay.proto",
    ];

    println!("cargo:rerun-if-changed={proto_root}");
    for file in &files {
        println!("cargo:rerun-if-changed={file}");
    }

    tonic_prost_build::configure()
        .build_client(true)
        .build_server(true)
        .compile_protos(&files, &[proto_root])?;

    Ok(())
}
