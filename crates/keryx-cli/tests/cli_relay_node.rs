use keryx_core::PeerId;
use keryx_daemon::{serve_daemon_rpc, KeryxDaemonConfig, KeryxDaemonRuntime};
use keryx_relay::{
    health_server::serve_grpc_health,
    registry::{SkillRegistry, StoredSkill},
    registry_server::{serve_registry_rpc, RegistryRpcService},
    runtime::RelayRuntime,
};
use std::sync::Arc;
use tempfile::tempdir;
use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;

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
    tokio::spawn(serve_registry_rpc(
        service,
        TcpListenerStream::new(listener),
    ));
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
    let runtime = KeryxDaemonRuntime::startup(KeryxDaemonConfig::new(
        dir.path().join("node-status-home"),
        123,
    ))
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
    tokio::spawn(serve_registry_rpc(
        service,
        TcpListenerStream::new(listener),
    ));
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
