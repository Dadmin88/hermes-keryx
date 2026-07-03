use std::time::Duration;

use keryx_relay::config::RelayConfig;
use tokio::process::Command;
use tokio::time::sleep;

#[tokio::test]
async fn relay_binary_starts_with_default_config() {
    let dir = tempfile::tempdir().expect("tempdir");
    let config_path = dir.path().join("relay.json");
    let config = RelayConfig {
        listen_addresses: vec![
            "/ip4/127.0.0.1/tcp/0".into(),
            "/ip4/127.0.0.1/udp/0/quic-v1".into(),
        ],
        health_grpc_bind: String::new(),
        health_http_bind: String::new(),
        registry_grpc_bind: String::new(),
        ..Default::default()
    };
    std::fs::write(
        &config_path,
        serde_json::to_string_pretty(&config).expect("serialize config"),
    )
    .expect("write config");

    let mut child = Command::new(env!("CARGO_BIN_EXE_keryx-relay"))
        .env("HERMES_KERYX_RELAY_CONFIG", &config_path)
        .env("RUST_LOG", "warn")
        .spawn()
        .expect("spawn relay binary");

    sleep(Duration::from_secs(2)).await;
    assert!(
        child.try_wait().expect("try_wait").is_none(),
        "relay binary exited early"
    );
    child.kill().await.ok();
}
