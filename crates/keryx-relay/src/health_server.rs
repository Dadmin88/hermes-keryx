//! gRPC health endpoint for the relay.

use std::net::SocketAddr;
use std::sync::Arc;

use keryx_proto::v1::keryx_relay_server::{KeryxRelay, KeryxRelayServer};
use keryx_proto::v1::{HealthRequest, HealthResponse};
use tokio::net::TcpListener;
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::{Request, Response, Status};

use crate::health::RelayHealthReport;
use crate::registry::SkillRegistry;
use crate::runtime::RelayRuntime;

pub struct RelayHealthService {
    runtime: Arc<RelayRuntime>,
    registry: Option<Arc<SkillRegistry>>,
}

impl RelayHealthService {
    #[must_use]
    pub fn new(runtime: Arc<RelayRuntime>) -> Self {
        Self {
            runtime,
            registry: None,
        }
    }

    #[must_use]
    pub fn with_registry(runtime: Arc<RelayRuntime>, registry: Arc<SkillRegistry>) -> Self {
        Self {
            runtime,
            registry: Some(registry),
        }
    }

    async fn refresh_registry_metric(&self) {
        let Some(registry) = &self.registry else {
            return;
        };
        let count = registry.registration_count().await as u64;
        self.runtime.metrics().set_registry_size(count);
    }
}

#[tonic::async_trait]
impl KeryxRelay for RelayHealthService {
    type ConnectNodeStream = ReceiverStream<Result<keryx_proto::v1::RelayFrame, Status>>;

    async fn connect_node(
        &self,
        _request: Request<tonic::Streaming<keryx_proto::v1::NodeFrame>>,
    ) -> Result<Response<Self::ConnectNodeStream>, Status> {
        Err(Status::unimplemented(
            "ConnectNode arrives in a later phase",
        ))
    }

    async fn register_node(
        &self,
        _request: Request<keryx_proto::v1::RegisterNodeRequest>,
    ) -> Result<Response<keryx_proto::v1::RegisterNodeResponse>, Status> {
        Err(Status::unimplemented(
            "RegisterNode arrives in a later phase",
        ))
    }

    async fn publish_task(
        &self,
        _request: Request<keryx_proto::v1::PublishTaskRequest>,
    ) -> Result<Response<keryx_proto::v1::PublishTaskResponse>, Status> {
        Err(Status::unimplemented(
            "PublishTask arrives in a later phase",
        ))
    }

    async fn ack_task(
        &self,
        _request: Request<keryx_proto::v1::AckTaskRequest>,
    ) -> Result<Response<keryx_proto::v1::AckTaskResponse>, Status> {
        Err(Status::unimplemented("AckTask arrives in a later phase"))
    }

    async fn health(
        &self,
        _request: Request<HealthRequest>,
    ) -> Result<Response<HealthResponse>, Status> {
        self.refresh_registry_metric().await;
        let report = RelayHealthReport::from_runtime(&self.runtime);
        Ok(Response::new(HealthResponse {
            healthy: report.healthy,
            connected_peers: report.connected_peers,
            registry_size: report.registry_size,
            uptime_seconds: report.uptime_seconds,
            transport_status: report.transport_status,
            tasks_routed: report.tasks_routed,
            local_peer_id: report.local_peer_id,
        }))
    }
}

/// Bind and serve the relay gRPC surface (health RPC today; other RPCs stubbed).
pub async fn serve_grpc_health(
    runtime: Arc<RelayRuntime>,
    registry: Option<Arc<SkillRegistry>>,
    addr: SocketAddr,
) -> Result<(), tonic::transport::Error> {
    let listener = TcpListener::bind(addr).await.expect("bind health grpc");
    let incoming = TcpListenerStream::new(listener);
    let service = match registry {
        Some(registry) => RelayHealthService::with_registry(runtime, registry),
        None => RelayHealthService::new(runtime),
    };
    tonic::transport::Server::builder()
        .add_service(KeryxRelayServer::new(service))
        .serve_with_incoming(incoming)
        .await
}

/// Accept HTTP `GET /health` on `addr` until `shutdown` completes.
pub async fn serve_http_health(
    runtime: Arc<RelayRuntime>,
    addr: SocketAddr,
    mut shutdown: tokio::sync::broadcast::Receiver<()>,
) {
    let listener = match TcpListener::bind(addr).await {
        Ok(listener) => listener,
        Err(err) => {
            tracing::error!(%addr, error = %err, "failed to bind HTTP health listener");
            return;
        }
    };
    loop {
        tokio::select! {
            _ = shutdown.recv() => break,
            accept = listener.accept() => {
                match accept {
                    Ok((stream, _)) => {
                        let rt = Arc::clone(&runtime);
                        tokio::spawn(async move {
                            if let Err(err) = crate::health::serve_http_health_once(rt, stream).await {
                                tracing::debug!(error = %err, "HTTP health connection ended");
                            }
                        });
                    }
                    Err(err) => tracing::warn!(error = %err, "HTTP health accept failed"),
                }
            }
        }
    }
}
