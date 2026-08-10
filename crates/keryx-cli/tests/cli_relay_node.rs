use keryx_core::PeerId;
use keryx_daemon::{serve_daemon_rpc, KeryxDaemonConfig, KeryxDaemonRuntime};
use keryx_relay::{
    health_server::serve_grpc_health,
    registry::{SkillRegistry, StoredSkill},
    registry_server::{serve_registry_rpc, serve_registry_rpc_with_tls, RegistryRpcService},
    runtime::RelayRuntime,
};
use rcgen::{generate_simple_self_signed, CertifiedKey};
use std::sync::Arc;
use tempfile::tempdir;
use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Identity;

#[test]
fn cli_registry_reads_reject_remote_plaintext_endpoints() {
    let endpoint = "http://0.0.0.0:1";
    for args in [
        &["relay", "registry", "list"][..],
        &["node", "discover", "python"][..],
    ] {
        let output = run_keryx(args, &[("HERMES_KERYX_RELAY_REGISTRY_ENDPOINT", endpoint)]);
        assert!(!output.status.success());
        assert!(
            String::from_utf8_lossy(&output.stderr).contains("require TLS"),
            "stderr={}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn cli_registry_reads_accept_https_with_private_ca() {
    let CertifiedKey { cert, key_pair } =
        generate_simple_self_signed(vec!["localhost".to_string()]).unwrap();
    let cert_pem = cert.pem();
    let dir = tempdir().unwrap();
    let ca_path = dir.path().join("registry-ca.pem");
    std::fs::write(&ca_path, cert_pem.as_bytes()).unwrap();
    let registry = Arc::new(SkillRegistry::new());
    registry
        .register(
            PeerId::new("peer-tls-cli").unwrap(),
            vec![StoredSkill {
                skill_id: "python".into(),
                description: String::new(),
                tags: vec![],
            }],
            "TLS CLI".into(),
            String::new(),
            None,
        )
        .await;
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(serve_registry_rpc_with_tls(
        RegistryRpcService::new(registry),
        listener,
        Some(Identity::from_pem(
            cert_pem.as_bytes(),
            key_pair.serialize_pem().as_bytes(),
        )),
    ));
    let endpoint = format!("https://localhost:{}", addr.port());
    let ca_path = ca_path.to_string_lossy().to_string();

    for args in [
        &["relay", "registry", "list"][..],
        &["node", "discover", "python"][..],
    ] {
        let output = run_keryx(
            args,
            &[
                ("HERMES_KERYX_RELAY_REGISTRY_ENDPOINT", endpoint.as_str()),
                ("HERMES_KERYX_REGISTRY_CA_CERT", ca_path.as_str()),
            ],
        );
        assert!(
            output.status.success(),
            "stderr={}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
    server.abort();
}

fn run_keryx(args: &[&str], env: &[(&str, &str)]) -> std::process::Output {
    let mut command = std::process::Command::new(env!("CARGO_BIN_EXE_keryx"));
    command.args(args);
    for (key, value) in env {
        command.env(key, value);
    }
    command.output().unwrap()
}

#[tokio::test(flavor = "multi_thread")]
async fn cli_relay_status_uses_health_endpoint() {
    let runtime = RelayRuntime::new("relay-cli-test");
    runtime.mark_transport_listening();
    runtime.note_connection_established();

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);

    let rt = Arc::clone(&runtime);
    tokio::spawn(async move {
        let _ = serve_grpc_health(rt, None, addr).await;
    });
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    let endpoint = format!("http://{addr}");
    let output = tokio::task::spawn_blocking(move || {
        run_keryx(
            &["relay", "status"],
            &[("HERMES_KERYX_RELAY_HEALTH_ENDPOINT", endpoint.as_str())],
        )
    })
    .await
    .unwrap();

    assert!(
        output.status.success(),
        "stderr={}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(stdout.contains("keryx relay status: healthy"));
    assert!(stdout.contains("connected_peers: 1"));
}

#[tokio::test(flavor = "multi_thread")]
async fn cli_relay_registry_list_shows_registrations() {
    let registry = Arc::new(SkillRegistry::new());
    registry
        .register(
            PeerId::new("peer-list").unwrap(),
            vec![StoredSkill {
                skill_id: "python".into(),
                description: String::new(),
                tags: vec![],
            }],
            "Lister".into(),
            String::new(),
            None,
        )
        .await;

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let reg = Arc::clone(&registry);
    let service = RegistryRpcService::new(reg);
    tokio::spawn(serve_registry_rpc(service, listener));
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    let endpoint = format!("http://{addr}");
    let output = tokio::task::spawn_blocking(move || {
        run_keryx(
            &["relay", "registry", "list"],
            &[("HERMES_KERYX_RELAY_REGISTRY_ENDPOINT", endpoint.as_str())],
        )
    })
    .await
    .unwrap();

    assert!(
        output.status.success(),
        "stderr={}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(stdout.contains("keryx relay registry list: 1 registration(s)"));
    assert!(stdout.contains("peer_id=peer-list"));
    assert!(stdout.contains("skills=[python]"));
}

#[tokio::test(flavor = "multi_thread")]
async fn cli_node_status_uses_daemon_endpoint() {
    let dir = tempdir().unwrap();
    let runtime = KeryxDaemonRuntime::startup(
        KeryxDaemonConfig::new(dir.path().join("node-status-home"), 123)
            .with_daemon_rpc_token(Some("keryx-cli-test-daemon-token".to_string())),
    )
    .await
    .unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(serve_daemon_rpc(runtime, TcpListenerStream::new(listener)));

    let output = tokio::task::spawn_blocking(move || {
        run_keryx(
            &["node", "status"],
            &[("HERMES_KERYX_DAEMON_ENDPOINT", &format!("http://{addr}"))],
        )
    })
    .await
    .unwrap();

    server.abort();
    assert!(
        output.status.success(),
        "stderr={}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(stdout.contains("keryx node status: connected"));
    assert!(stdout.contains("daemon_ready: true"));
}

#[tokio::test(flavor = "multi_thread")]
async fn cli_node_discover_filters_by_skill() {
    let registry = Arc::new(SkillRegistry::new());
    registry
        .register(
            PeerId::new("peer-discover").unwrap(),
            vec![
                StoredSkill {
                    skill_id: "python".into(),
                    description: String::new(),
                    tags: vec![],
                },
                StoredSkill {
                    skill_id: "rust".into(),
                    description: String::new(),
                    tags: vec![],
                },
            ],
            "Discoverable".into(),
            String::new(),
            None,
        )
        .await;

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let reg = Arc::clone(&registry);
    let service = RegistryRpcService::new(reg);
    tokio::spawn(serve_registry_rpc(service, listener));
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    let endpoint = format!("http://{addr}");
    let output = tokio::task::spawn_blocking(move || {
        run_keryx(
            &["node", "discover", "python"],
            &[("HERMES_KERYX_RELAY_REGISTRY_ENDPOINT", endpoint.as_str())],
        )
    })
    .await
    .unwrap();

    assert!(
        output.status.success(),
        "stderr={}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(stdout.contains("keryx node discover: skill=python matches=1"));
    assert!(stdout.contains("peer_id=peer-discover"));
}

#[test]
fn cli_relay_and_node_subcommands_parse() {
    let help = run_keryx(&["--help"], &[]);
    assert!(help.status.success());
    let stdout = String::from_utf8(help.stdout).unwrap();
    assert!(stdout.contains("relay"));
    assert!(stdout.contains("node"));

    let relay_help = run_keryx(&["relay", "--help"], &[]);
    assert!(relay_help.status.success());
    let relay_stdout = String::from_utf8(relay_help.stdout).unwrap();
    assert!(relay_stdout.contains("start"));
    assert!(relay_stdout.contains("status"));
    assert!(relay_stdout.contains("registry"));

    let node_help = run_keryx(&["node", "--help"], &[]);
    assert!(node_help.status.success());
    let node_stdout = String::from_utf8(node_help.stdout).unwrap();
    assert!(node_stdout.contains("start"));
    assert!(node_stdout.contains("status"));
    assert!(node_stdout.contains("discover"));

    let invalid = run_keryx(&["relay", "not-a-command"], &[]);
    assert!(!invalid.status.success());
}
