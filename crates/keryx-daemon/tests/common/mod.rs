use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use keryx_core::PeerId;
use keryx_daemon::{
    serve_daemon_rpc, KeryxDaemonConfig, KeryxDaemonRuntime, RelayTaskPublisher, RoutingError,
};
use keryx_proto::v1::keryx_daemon_client::KeryxDaemonClient;
use keryx_proto::v1::TaskEnvelope;
use tempfile::TempDir;
use tokio::net::TcpListener;
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use tokio_stream::wrappers::TcpListenerStream;

#[allow(dead_code)]
pub struct RpcTestHarness {
    pub _dir: TempDir,
    pub runtime: Arc<KeryxDaemonRuntime>,
    pub client: KeryxDaemonClient<tonic::transport::Channel>,
    server: JoinHandle<Result<(), tonic::transport::Error>>,
}

#[allow(dead_code)]
impl RpcTestHarness {
    pub async fn start() -> Self {
        let dir = tempfile::tempdir().unwrap();
        let data_dir = dir.path().join("rpc-keryx-home");
        Self::start_with_data_dir_and_dir(data_dir, dir).await
    }

    pub async fn start_with_data_dir(data_dir: std::path::PathBuf) -> Self {
        let dir = tempfile::tempdir().unwrap();
        Self::start_with_data_dir_and_dir(data_dir, dir).await
    }

    pub async fn start_with_config(config: KeryxDaemonConfig) -> Self {
        let dir = tempfile::tempdir().unwrap();
        let runtime = Arc::new(KeryxDaemonRuntime::startup(config).await.unwrap());
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(serve_daemon_rpc(
            runtime.as_ref().clone(),
            TcpListenerStream::new(listener),
        ));
        let client = KeryxDaemonClient::connect(format!("http://{addr}"))
            .await
            .unwrap();
        Self {
            _dir: dir,
            runtime,
            client,
            server,
        }
    }

    async fn start_with_data_dir_and_dir(data_dir: std::path::PathBuf, dir: TempDir) -> Self {
        let config = KeryxDaemonConfig::new(data_dir.clone(), 42)
            .with_fail_retry_policy(keryx_core::RetryPolicy::no_retries());
        let runtime = Arc::new(KeryxDaemonRuntime::startup(config).await.unwrap());

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(serve_daemon_rpc(
            runtime.as_ref().clone(),
            TcpListenerStream::new(listener),
        ));

        let client = KeryxDaemonClient::connect(format!("http://{addr}"))
            .await
            .unwrap();

        Self {
            _dir: dir,
            runtime,
            client,
            server,
        }
    }
}

impl Drop for RpcTestHarness {
    fn drop(&mut self) {
        self.server.abort();
    }
}

#[allow(dead_code)]
pub struct MockRelayPublisher {
    delay: Duration,
    deliveries: Mutex<Vec<(String, String)>>,
}

#[allow(dead_code)]
impl MockRelayPublisher {
    pub fn new() -> Self {
        Self {
            delay: Duration::ZERO,
            deliveries: Mutex::new(Vec::new()),
        }
    }

    pub fn with_delay(mut self, delay: Duration) -> Self {
        self.delay = delay;
        self
    }

    pub async fn deliveries(&self) -> Vec<(String, String)> {
        self.deliveries.lock().await.clone()
    }
}

#[async_trait]
impl RelayTaskPublisher for MockRelayPublisher {
    async fn deliver_task(
        &self,
        target_peer_id: &PeerId,
        envelope: TaskEnvelope,
        _timeout: Duration,
    ) -> Result<(), RoutingError> {
        if self.delay > Duration::ZERO {
            tokio::time::sleep(self.delay).await;
        }
        let task_id = envelope
            .task_id
            .as_ref()
            .map(|id| id.value.clone())
            .unwrap_or_default();
        self.deliveries
            .lock()
            .await
            .push((target_peer_id.as_str().to_string(), task_id));
        Ok(())
    }
}
