use keryx_daemon::{serve_daemon_rpc, KeryxDaemonConfig, KeryxDaemonRuntime};
use tempfile::tempdir;
use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;

fn run_keryx(command: &str, endpoint: String) -> std::process::Output {
    std::process::Command::new(env!("CARGO_BIN_EXE_keryx"))
        .arg(command)
        .env("HERMES_KERYX_DAEMON_ENDPOINT", endpoint)
        .output()
        .unwrap()
}

#[tokio::test(flavor = "multi_thread")]
async fn cli_status_uses_daemon_endpoint_when_configured() {
    let dir = tempdir().unwrap();
    let runtime = KeryxDaemonRuntime::startup(KeryxDaemonConfig::new(
        dir.path().join("cli-rpc-keryx-home"),
        123,
    ))
    .await
    .unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(serve_daemon_rpc(runtime, TcpListenerStream::new(listener)));

    let output = tokio::task::spawn_blocking(move || run_keryx("status", format!("http://{addr}")))
        .await
        .unwrap();

    server.abort();
    assert!(output.status.success());
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(stdout.contains("keryx status: ready"));
    assert!(stdout.contains("source: daemon http://"));
    assert!(stdout.contains("data_dir:"));
    assert!(stdout.contains("db_path:"));
    assert!(stdout.contains("store: ready sqlite schema_version=2 supported_schema_version=2"));
    assert!(stdout.contains(
        "startup_recovery: recovered_tasks=0 cleaned_terminal_leases=0 corruption_count=0 duration_ms="
    ));
}

#[tokio::test(flavor = "multi_thread")]
async fn cli_doctor_uses_daemon_endpoint_when_configured() {
    let dir = tempdir().unwrap();
    let runtime = KeryxDaemonRuntime::startup(KeryxDaemonConfig::new(
        dir.path().join("cli-doctor-rpc-keryx-home"),
        123,
    ))
    .await
    .unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(serve_daemon_rpc(runtime, TcpListenerStream::new(listener)));

    let output = tokio::task::spawn_blocking(move || run_keryx("doctor", format!("http://{addr}")))
        .await
        .unwrap();

    server.abort();
    assert!(output.status.success());
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(stdout.contains("keryx doctor: pass"));
    assert!(stdout.contains("source: daemon http://"));
    assert!(stdout.contains("sqlite_store"));
    assert!(stdout.contains("schema_version"));
}

#[tokio::test(flavor = "multi_thread")]
async fn cli_status_reports_unavailable_daemon_when_endpoint_cannot_connect() {
    let output =
        tokio::task::spawn_blocking(|| run_keryx("status", "http://127.0.0.1:1".to_string()))
            .await
            .unwrap();

    assert!(!output.status.success());
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(stderr.contains("keryx status: daemon unavailable at http://127.0.0.1:1"));
}

#[tokio::test(flavor = "multi_thread")]
async fn cli_doctor_reports_unavailable_daemon_when_endpoint_cannot_connect() {
    let output =
        tokio::task::spawn_blocking(|| run_keryx("doctor", "http://127.0.0.1:1".to_string()))
            .await
            .unwrap();

    assert!(!output.status.success());
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(stderr.contains("keryx doctor: daemon unavailable at http://127.0.0.1:1"));
}
