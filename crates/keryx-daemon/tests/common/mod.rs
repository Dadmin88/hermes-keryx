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
use tonic::metadata::MetadataValue;
use tonic::service::Interceptor;
use tonic::transport::Channel;
use tonic::{Request, Status};

#[derive(Clone)]
pub struct ArtifactTokenInterceptor {
    token: MetadataValue<tonic::metadata::Ascii>,
}

impl ArtifactTokenInterceptor {
    pub fn new(token: &str) -> Self {
        Self {
            token: MetadataValue::try_from(token).unwrap(),
        }
    }
}

impl Interceptor for ArtifactTokenInterceptor {
    fn call(&mut self, mut request: Request<()>) -> Result<Request<()>, Status> {
        request
            .metadata_mut()
            .insert("x-keryx-artifact-token", self.token.clone());
        Ok(request)
    }
}

#[allow(dead_code)]
pub struct RpcTestHarness {
    pub _dir: TempDir,
    pub runtime: Arc<KeryxDaemonRuntime>,
    pub client: KeryxDaemonClient<
        tonic::service::interceptor::InterceptedService<Channel, ArtifactTokenInterceptor>,
    >,
    pub endpoint: String,
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
        let channel = Channel::from_shared(format!("http://{addr}"))
            .unwrap()
            .connect()
            .await
            .unwrap();
        let client = KeryxDaemonClient::with_interceptor(
            channel,
            ArtifactTokenInterceptor::new(runtime.config().artifact_rpc_token()),
        );
        Self {
            _dir: dir,
            runtime,
            client,
            endpoint: format!("http://{addr}"),
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

        let channel = Channel::from_shared(format!("http://{addr}"))
            .unwrap()
            .connect()
            .await
            .unwrap();
        let client = KeryxDaemonClient::with_interceptor(
            channel,
            ArtifactTokenInterceptor::new(runtime.config().artifact_rpc_token()),
        );

        Self {
            _dir: dir,
            runtime,
            client,
            endpoint: format!("http://{addr}"),
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
