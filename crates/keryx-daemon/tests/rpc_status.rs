use keryx_daemon::{serve_daemon_rpc, KeryxDaemonConfig, KeryxDaemonRuntime};
use keryx_proto::v1::{keryx_daemon_client::KeryxDaemonClient, DoctorRequest, StatusRequest};
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

    let doctor = client.doctor(DoctorRequest {}).await.unwrap().into_inner();
    assert_eq!(doctor.status, "pass");
    assert!(doctor
        .messages
        .iter()
        .any(|message| message.contains("sqlite_store") && message.contains("schema_version=1")));

    server.abort();
}
