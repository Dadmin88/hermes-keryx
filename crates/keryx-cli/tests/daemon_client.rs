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

fn run_keryx_args(args: &[&str], endpoint: String) -> std::process::Output {
    std::process::Command::new(env!("CARGO_BIN_EXE_keryx"))
        .args(args)
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
    assert!(stdout.contains("store: ready sqlite schema_version=6 supported_schema_version=6"));
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

#[tokio::test(flavor = "multi_thread")]
async fn cli_artifact_commands_round_trip_file_content_against_daemon() {
    let dir = tempdir().unwrap();
    let runtime = KeryxDaemonRuntime::startup(KeryxDaemonConfig::new(
        dir.path().join("cli-artifact-rpc-keryx-home"),
        123,
    ))
    .await
    .unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(serve_daemon_rpc(runtime, TcpListenerStream::new(listener)));
    let endpoint = format!("http://{addr}");

    let put_file = dir.path().join("artifact.txt");
    std::fs::write(&put_file, "hello artifact cli\n").unwrap();

    let submit_output = tokio::task::spawn_blocking({
        let endpoint = endpoint.clone();
        move || run_keryx_args(&["task", "submit", "cli-artifact-task"], endpoint)
    })
    .await
    .unwrap();
    assert!(submit_output.status.success());

    let put_output = tokio::task::spawn_blocking({
        let endpoint = endpoint.clone();
        let put_file = put_file.clone();
        move || {
            run_keryx_args(
                &[
                    "artifact",
                    "put",
                    "cli-artifact-task",
                    put_file.to_str().unwrap(),
                    "--id",
                    "cli-artifact-1",
                    "--media-type",
                    "text/plain",
                ],
                endpoint,
            )
        }
    })
    .await
    .unwrap();
    assert!(put_output.status.success());
    let put_stdout = String::from_utf8(put_output.stdout).unwrap();
    assert!(put_stdout.contains("Stored artifact cli-artifact-1 for task cli-artifact-task"));

    let ls_output = tokio::task::spawn_blocking({
        let endpoint = endpoint.clone();
        move || run_keryx_args(&["artifact", "ls", "cli-artifact-task"], endpoint)
    })
    .await
    .unwrap();
    assert!(ls_output.status.success());
    let ls_stdout = String::from_utf8(ls_output.stdout).unwrap();
    assert!(ls_stdout.contains("ARTIFACT_ID\tDIGEST\tMEDIA_TYPE\tBYTE_LEN\tINLINE\tCREATED_AT"));
    assert!(ls_stdout.contains("cli-artifact-1"));

    let get_output_path = dir.path().join("artifact-out.txt");
    let get_output = tokio::task::spawn_blocking({
        let endpoint = endpoint.clone();
        let get_output_path = get_output_path.clone();
        move || {
            run_keryx_args(
                &[
                    "artifact",
                    "get",
                    "cli-artifact-1",
                    "--output",
                    get_output_path.to_str().unwrap(),
                ],
                endpoint,
            )
        }
    })
    .await
    .unwrap();
    assert!(get_output.status.success());
    assert_eq!(
        std::fs::read_to_string(&get_output_path).unwrap(),
        "hello artifact cli\n"
    );

    let rm_output = tokio::task::spawn_blocking({
        let endpoint = endpoint.clone();
        move || run_keryx_args(&["artifact", "rm", "cli-artifact-1"], endpoint)
    })
    .await
    .unwrap();
    assert!(rm_output.status.success());
    let rm_stdout = String::from_utf8(rm_output.stdout).unwrap();
    assert!(rm_stdout.contains("Deleted artifact cli-artifact-1"));

    server.abort();
}
