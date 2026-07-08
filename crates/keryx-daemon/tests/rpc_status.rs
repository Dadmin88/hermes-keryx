use keryx_daemon::{serve_daemon_rpc, KeryxDaemonConfig, KeryxDaemonRuntime};
use keryx_proto::v1::{keryx_daemon_client::KeryxDaemonClient, DoctorRequest, StatusRequest};
use keryx_store::CURRENT_SCHEMA_VERSION;
use tempfile::tempdir;
use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;

#[tokio::test]
async fn daemon_rpc_reports_runtime_status_and_doctor_readiness() {
    let dir = tempdir().unwrap();
    let data_dir = dir.path().join("rpc-keryx-home");
    let runtime = KeryxDaemonRuntime::startup(KeryxDaemonConfig::new(data_dir.clone(), 42))
        .await
        .unwrap();

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(serve_daemon_rpc(runtime, TcpListenerStream::new(listener)));

    let mut client = KeryxDaemonClient::connect(format!("http://{addr}"))
        .await
        .unwrap();
    let status = client.status(StatusRequest {}).await.unwrap().into_inner();
    assert_eq!(status.status, "ready");
    assert_eq!(status.data_dir, data_dir.display().to_string());
    assert_eq!(
        status.db_path,
        data_dir.join("keryx.db").display().to_string()
    );
    assert_eq!(status.schema_version, CURRENT_SCHEMA_VERSION);
    assert_eq!(status.supported_schema_version, CURRENT_SCHEMA_VERSION);
    assert_eq!(status.recovered_tasks, 0);
    assert_eq!(status.cleaned_terminal_leases, 0);
    assert_eq!(status.corruption_count, 0);
    assert!(status.startup_recovery_duration_ms <= 1_000);
    assert_eq!(status.store_kind, "sqlite");
    assert!(status.store_ready);
    assert_eq!(
        status.store_path,
        data_dir.join("keryx.db").display().to_string()
    );
    assert_eq!(status.tasks_submitted, 0);
    assert_eq!(status.tasks_claimed, 0);
    assert_eq!(status.tasks_completed, 0);
    assert_eq!(status.tasks_failed, 0);
    assert_eq!(status.heartbeats, 0);
    assert_eq!(status.leases_recovered, 0);
    assert_eq!(status.recovery_ticks, 0);
    assert_eq!(status.active_leases, 0);
    assert_eq!(status.dead_letters, 0);

    let doctor = client.doctor(DoctorRequest {}).await.unwrap().into_inner();
    assert_eq!(doctor.status, "pass");
    assert!(doctor
        .messages
        .iter()
        .any(|message| message.contains("schema_version")
            && message.contains(&format!(
                "supported_schema_version={CURRENT_SCHEMA_VERSION}"
            ))));

    server.abort();
}
