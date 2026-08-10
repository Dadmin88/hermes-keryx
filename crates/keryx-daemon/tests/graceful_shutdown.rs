use std::sync::Arc;
use std::time::Duration;

use keryx_daemon::{serve_daemon_rpc, KeryxDaemonConfig, KeryxDaemonRuntime};
use keryx_proto::v1::{
    keryx_daemon_client::KeryxDaemonClient, AckResultDeliveryRequest, FailResultDeliveryRequest,
    IngestRemoteResultRequest, StatusRequest,
};
use tempfile::tempdir;
use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::service::Interceptor;
use tonic::transport::Channel;
use tonic::{Code, Request};

const TEST_DAEMON_TOKEN: &str = "keryx-graceful-shutdown-daemon-token";

#[derive(Clone)]
struct TestDaemonTokenInterceptor;

impl Interceptor for TestDaemonTokenInterceptor {
    fn call(&mut self, mut request: Request<()>) -> Result<Request<()>, tonic::Status> {
        request.metadata_mut().insert(
            "authorization",
            format!("Bearer {TEST_DAEMON_TOKEN}")
                .parse()
                .expect("static graceful-shutdown token is valid metadata"),
        );
        Ok(request)
    }
}

type TestDaemonClient = KeryxDaemonClient<
    tonic::service::interceptor::InterceptedService<Channel, TestDaemonTokenInterceptor>,
>;

async fn authenticated_client(endpoint: String) -> TestDaemonClient {
    let channel = tonic::transport::Endpoint::from_shared(endpoint)
        .unwrap()
        .connect()
        .await
        .unwrap();
    KeryxDaemonClient::with_interceptor(channel, TestDaemonTokenInterceptor)
}

async fn spawn_rpc_server(
    runtime: Arc<KeryxDaemonRuntime>,
) -> (String, tokio::task::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let rpc_runtime = (*runtime).clone();
    let server = tokio::spawn(async move {
        serve_daemon_rpc(rpc_runtime, TcpListenerStream::new(listener))
            .await
            .unwrap();
    });
    (format!("http://{addr}"), server)
}

#[tokio::test]
async fn in_flight_rpc_completes_during_shutdown() {
    std::env::set_var("KERYX_TEST_RPC_DELAY_MS", "250");

    let dir = tempdir().unwrap();
    let data_dir = dir.path().join("graceful-inflight-home");
    let runtime = Arc::new(
        KeryxDaemonRuntime::startup(
            KeryxDaemonConfig::new(data_dir, 42)
                .with_daemon_rpc_token(Some(TEST_DAEMON_TOKEN.to_string())),
        )
        .await
        .unwrap(),
    );

    let (endpoint, server) = spawn_rpc_server(Arc::clone(&runtime)).await;
    let mut client = authenticated_client(endpoint).await;

    let status_task = tokio::spawn(async move { client.status(StatusRequest {}).await });

    tokio::time::sleep(Duration::from_millis(30)).await;
    runtime.initiate_shutdown();
    let status_result = status_task.await.unwrap();
    assert!(
        status_result.is_ok(),
        "in-flight status should complete: {status_result:?}"
    );

    Arc::clone(&runtime).shutdown().await.unwrap();
    server.await.unwrap();

    std::env::remove_var("KERYX_TEST_RPC_DELAY_MS");
}

#[tokio::test]
async fn new_rpc_rejected_when_daemon_is_shutting_down() {
    let dir = tempdir().unwrap();
    let data_dir = dir.path().join("graceful-reject-home");
    let runtime = Arc::new(
        KeryxDaemonRuntime::startup(
            KeryxDaemonConfig::new(data_dir, 42)
                .with_daemon_rpc_token(Some(TEST_DAEMON_TOKEN.to_string())),
        )
        .await
        .unwrap(),
    );

    let (endpoint, server) = spawn_rpc_server(Arc::clone(&runtime)).await;
    let mut client = authenticated_client(endpoint).await;

    runtime.mark_shutting_down();

    let err = client
        .status(StatusRequest {})
        .await
        .expect_err("status should be rejected during shutdown");
    assert_eq!(err.code(), Code::Unavailable);
    assert!(
        err.message().contains("shutting down"),
        "unexpected message: {}",
        err.message()
    );

    Arc::clone(&runtime).shutdown().await.unwrap();
    server.await.unwrap();
}

#[tokio::test]
async fn result_delivery_mutations_are_rejected_when_daemon_is_shutting_down() {
    let dir = tempdir().unwrap();
    let data_dir = dir.path().join("graceful-result-mutations-home");
    let runtime = Arc::new(
        KeryxDaemonRuntime::startup(
            KeryxDaemonConfig::new(data_dir, 42)
                .with_daemon_rpc_token(Some(TEST_DAEMON_TOKEN.to_string())),
        )
        .await
        .unwrap(),
    );

    let (endpoint, server) = spawn_rpc_server(Arc::clone(&runtime)).await;
    let mut client = authenticated_client(endpoint).await;

    runtime.mark_shutting_down();

    let ack_error = client
        .ack_result_delivery(AckResultDeliveryRequest::default())
        .await
        .expect_err("result delivery acknowledgement should be rejected during shutdown");
    assert_eq!(ack_error.code(), Code::Unavailable);

    let fail_error = client
        .fail_result_delivery(FailResultDeliveryRequest::default())
        .await
        .expect_err("result delivery failure should be rejected during shutdown");
    assert_eq!(fail_error.code(), Code::Unavailable);

    let ingest_error = client
        .ingest_remote_result(IngestRemoteResultRequest::default())
        .await
        .expect_err("remote result ingestion should be rejected during shutdown");
    assert_eq!(ingest_error.code(), Code::Unavailable);

    Arc::clone(&runtime).shutdown().await.unwrap();
    server.await.unwrap();
}
