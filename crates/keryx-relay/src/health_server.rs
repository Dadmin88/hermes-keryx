//! gRPC health endpoint for the relay.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use futures::StreamExt;
use keryx_proto::v1::keryx_relay_server::{KeryxRelay, KeryxRelayServer};
use keryx_proto::v1::{
    AckFrameRequest, AckFrameResponse, AckTaskRequest, AckTaskResponse, HealthRequest,
    HealthResponse, NodeFrame, PublishResultRequest, PublishResultResponse, PublishTaskRequest,
    PublishTaskResponse, RegisterNodeRequest, RegisterNodeResponse, RelayFrame, TaskEnvelope,
};

use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::{Identity, ServerTlsConfig};
use tonic::{Request, Response, Status};
use uuid::Uuid;

use crate::health::RelayHealthReport;
use crate::registry::SkillRegistry;
use crate::runtime::{
    FrameAcknowledgement, PublishedTaskDelivery, PublishedTaskReceipt, RelayRuntime,
    MAX_TRACKED_FRAMES,
};
use crate::security::NodeTokenAuth;
use keryx_core::{PeerId, RESULT_ARTIFACT_FRAME_MAX_BYTES};

/// gRPC metadata key used by `ConnectNode` to identify the streaming node.
pub const NODE_ID_METADATA_KEY: &str = "x-keryx-node-id";
pub const NODE_TOKEN_METADATA_KEY: &str = "x-keryx-node-token";

const RELAY_STREAM_BUFFER: usize = MAX_TRACKED_FRAMES;
const TARGET_NODE_METADATA_KEYS: &[&str] = &[
    "target_node_id",
    "target_node",
    "recipient_node_id",
    "recipient_node",
    "destination_node_id",
    "destination_node",
    "node_id",
    "keryx.target_node_id",
];

const ABSOLUTE_DEADLINES_FEATURE: &str = "absolute_deadlines_v1";
const RESULT_ARTIFACT_BYTES_FEATURE: &str = "result_artifact_bytes_v1";
const AUTHENTICATED_SOURCE_FEATURES_METADATA_KEY: &str =
    "keryx.authenticated_source_protocol_features";

pub struct RelayHealthService {
    runtime: Arc<RelayRuntime>,
    registry: Option<Arc<SkillRegistry>>,
    node_auth: Option<Arc<NodeTokenAuth>>,
}

impl RelayHealthService {
    async fn require_destination_feature(
        &self,
        destination_node_id: &str,
        feature: &str,
    ) -> Result<(), Status> {
        let registry = self.registry.as_ref().ok_or_else(|| {
            Status::failed_precondition(format!(
                "destination protocol feature {feature} is unavailable without registry state"
            ))
        })?;
        let peer_id = parse_registry_peer_id(destination_node_id)?;
        if !registry.supports_protocol_feature(&peer_id, feature).await {
            return Err(Status::failed_precondition(format!(
                "destination {destination_node_id} does not advertise protocol feature {feature}"
            )));
        }
        Ok(())
    }

    #[must_use]
    pub fn new(runtime: Arc<RelayRuntime>) -> Self {
        Self {
            runtime,
            registry: None,
            node_auth: None,
        }
    }

    #[must_use]
    pub fn with_registry(runtime: Arc<RelayRuntime>, registry: Arc<SkillRegistry>) -> Self {
        Self {
            runtime,
            registry: Some(registry),
            node_auth: None,
        }
    }

    #[must_use]
    pub fn with_registry_and_auth(
        runtime: Arc<RelayRuntime>,
        registry: Arc<SkillRegistry>,
        node_auth: Arc<NodeTokenAuth>,
    ) -> Self {
        Self {
            runtime,
            registry: Some(registry),
            node_auth: Some(node_auth),
        }
    }

    fn authenticate_request<T>(
        &self,
        request: &Request<T>,
        claimed_node_id: &str,
    ) -> Result<String, Status> {
        let claimed_node_id = claimed_node_id.trim();
        if claimed_node_id.is_empty() {
            return Err(Status::invalid_argument("source node id is required"));
        }
        let auth = self
            .node_auth
            .as_ref()
            .filter(|auth| auth.is_configured())
            .ok_or_else(|| {
                Status::unauthenticated("node authentication is not configured for mutations")
            })?;
        let metadata_node_id = request
            .metadata()
            .get(NODE_ID_METADATA_KEY)
            .and_then(|value| value.to_str().ok())
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .ok_or_else(|| Status::unauthenticated("node id metadata is required"))?;
        if metadata_node_id != claimed_node_id {
            return Err(Status::permission_denied(
                "claimed source node does not match authenticated node metadata",
            ));
        }
        let token = request
            .metadata()
            .get(NODE_TOKEN_METADATA_KEY)
            .and_then(|value| value.to_str().ok());
        let node_id = metadata_node_id
            .parse()
            .map_err(|error| Status::invalid_argument(format!("invalid node id: {error}")))?;
        auth.authenticate_optional(&node_id, token)
            .map_err(|failure| {
                Status::unauthenticated(format!("node authentication failed: {}", failure.reason()))
            })?;
        Ok(metadata_node_id.to_string())
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
        request: Request<tonic::Streaming<NodeFrame>>,
    ) -> Result<Response<Self::ConnectNodeStream>, Status> {
        let node_id = node_id_from_metadata(&request)?;
        let node_id = self.authenticate_request(&request, &node_id)?;
        let mut inbound = request.into_inner();
        let (tx, rx) = mpsc::channel(RELAY_STREAM_BUFFER);

        let pending_count = self.runtime.connect_node(node_id.clone(), tx.clone());

        let runtime = Arc::clone(&self.runtime);
        let source_node_id = node_id.clone();
        tokio::spawn(async move {
            if let Some(next) = inbound.next().await {
                match next {
                    Ok(_) => {
                        let error = Status::failed_precondition(
                            "ConnectNode is receive-only; publish through authenticated PublishTask or PublishResult",
                        );
                        tracing::warn!(
                            source_node_id = %source_node_id,
                            "rejecting mutation frame on receive-only node stream"
                        );
                        let _ = tx.send(Err(error)).await;
                    }
                    Err(err) => {
                        tracing::debug!(
                            source_node_id = %source_node_id,
                            error = %err,
                            "node relay stream ended with error"
                        );
                    }
                }
            }
            runtime.disconnect_node(&source_node_id);
        });

        tracing::debug!(%node_id, pending_count, "node connected to relay stream");
        Ok(Response::new(ReceiverStream::new(rx)))
    }

    async fn register_node(
        &self,
        request: Request<RegisterNodeRequest>,
    ) -> Result<Response<RegisterNodeResponse>, Status> {
        let inner = request.into_inner();
        let node_id = inner
            .node_id
            .as_ref()
            .map(|node_id| node_id.value.trim())
            .filter(|value| !value.is_empty())
            .ok_or_else(|| Status::invalid_argument("RegisterNode requires node_id"))?;
        let auth = self
            .node_auth
            .as_ref()
            .filter(|auth| auth.is_configured())
            .ok_or_else(|| {
                Status::unauthenticated("node authentication is not configured for mutations")
            })?;
        let parsed = node_id
            .parse()
            .map_err(|error| Status::invalid_argument(format!("invalid node id: {error}")))?;
        auth.authenticate(&parsed, inner.token.trim())
            .map_err(|failure| {
                Status::unauthenticated(format!("node authentication failed: {}", failure.reason()))
            })?;
        self.runtime.register_node(node_id.to_string());
        if let Some(registry) = &self.registry {
            let peer_id = parse_registry_peer_id(node_id)?;
            registry
                .upsert_node(peer_id, node_id.to_string(), String::new(), None)
                .await;
            self.refresh_registry_metric().await;
        }
        Ok(Response::new(RegisterNodeResponse { accepted: true }))
    }

    async fn publish_task(
        &self,
        request: Request<PublishTaskRequest>,
    ) -> Result<Response<PublishTaskResponse>, Status> {
        let claimed_source = request.get_ref().source_node_id.clone();
        let authenticated_source = self.authenticate_request(&request, &claimed_source)?;
        let inner = request.into_inner();
        let task = inner
            .task
            .ok_or_else(|| Status::invalid_argument("PublishTask requires task"))?;
        let target_node_id = if inner.target_node_id.trim().is_empty() {
            target_node_id_from_task(&task)?
        } else {
            inner.target_node_id.trim().to_string()
        };
        let source_node_id = authenticated_source;
        if task.deadline_ms > 0 {
            self.require_destination_feature(&target_node_id, ABSOLUTE_DEADLINES_FEATURE)
                .await?;
        }

        let task_id = task
            .task_id
            .clone()
            .ok_or_else(|| Status::invalid_argument("PublishTask requires task.task_id"))?;
        if task_id.value.trim().is_empty() {
            return Err(Status::invalid_argument(
                "PublishTask requires non-empty task.task_id",
            ));
        }

        let proposed_receipt = PublishedTaskReceipt {
            frame_id: new_relay_frame_id(),
            accepted_at_ms: unix_ms_now(),
        };
        let source_peer_id = PeerId::new(source_node_id.clone())
            .map_err(|error| Status::unauthenticated(error.to_string()))?;
        let source_features = if let Some(registry) = self.registry.as_ref() {
            registry.protocol_features(&source_peer_id).await
        } else {
            Vec::new()
        };
        let mut delivered_task = task.clone();
        delivered_task.metadata.insert(
            AUTHENTICATED_SOURCE_FEATURES_METADATA_KEY.to_string(),
            serde_json::to_string(&source_features)
                .map_err(|error| Status::internal(error.to_string()))?,
        );
        let frame = RelayFrame {
            frame_id: proposed_receipt.frame_id.clone(),
            task: Some(delivered_task),
            result: None,
            authenticated_source_node_id: source_node_id.clone(),
            destination_node_id: target_node_id.clone(),
        };
        let receipt = match self.runtime.publish_task_frame(
            &source_node_id,
            &target_node_id,
            task_id.value.trim(),
            &task,
            frame,
            proposed_receipt,
        ) {
            PublishedTaskDelivery::New { receipt, .. } | PublishedTaskDelivery::Retry(receipt) => {
                receipt
            }
            PublishedTaskDelivery::Conflict => {
                return Err(Status::already_exists(
                    "task identity was already accepted with a different envelope",
                ));
            }
            PublishedTaskDelivery::RejectedCapacity => {
                return Err(Status::resource_exhausted(
                    "relay task or frame identity table is at capacity",
                ));
            }
        };
        Ok(Response::new(PublishTaskResponse {
            task_id: Some(task_id),
            frame_id: receipt.frame_id,
            authenticated_source_peer_id: source_node_id,
            accepted_destination_peer_id: target_node_id,
            accepted_route: "relay".to_string(),
            accepted_at_ms: receipt.accepted_at_ms,
        }))
    }

    async fn publish_result(
        &self,
        request: Request<PublishResultRequest>,
    ) -> Result<Response<PublishResultResponse>, Status> {
        if self.node_auth.is_none() {
            return Err(Status::unauthenticated(
                "terminal results require configured node authentication",
            ));
        }
        let claimed_source = request.get_ref().source_node_id.clone();
        let authenticated_source = self.authenticate_request(&request, &claimed_source)?;
        let inner = request.into_inner();
        let result = inner
            .result
            .ok_or_else(|| Status::invalid_argument("PublishResult requires result"))?;
        let target_node_id = required_node_value(&inner.target_node_id, "target_node_id")?;
        let source_node_id = authenticated_source;
        if result
            .output_artifacts
            .iter()
            .any(|artifact| artifact.content_present || !artifact.content.is_empty())
        {
            self.require_destination_feature(&target_node_id, RESULT_ARTIFACT_BYTES_FEATURE)
                .await?;
        }
        result
            .task_id
            .as_ref()
            .map(|value| value.value.trim())
            .filter(|value| !value.is_empty())
            .ok_or_else(|| Status::invalid_argument("PublishResult requires result.task_id"))?;
        let frame_id = new_relay_frame_id();
        ensure_frame_routed(self.runtime.route_frame(
            target_node_id.clone(),
            RelayFrame {
                frame_id: frame_id.clone(),
                task: None,
                result: Some(result),
                authenticated_source_node_id: source_node_id,
                destination_node_id: target_node_id,
            },
        ))?;
        Ok(Response::new(PublishResultResponse {
            accepted: true,
            frame_id,
        }))
    }

    async fn ack_frame(
        &self,
        request: Request<AckFrameRequest>,
    ) -> Result<Response<AckFrameResponse>, Status> {
        let node_id = node_id_from_metadata(&request)?;
        let authenticated_node_id = self.authenticate_request(&request, &node_id)?;
        let frame_id = request.into_inner().frame_id;
        match self.runtime.ack_frame(&authenticated_node_id, &frame_id) {
            FrameAcknowledgement::Accepted => {
                Ok(Response::new(AckFrameResponse { accepted: true }))
            }
            FrameAcknowledgement::WrongDestination => Err(Status::permission_denied(
                "authenticated node does not own the relay frame",
            )),
            FrameAcknowledgement::UnknownFrame => {
                Ok(Response::new(AckFrameResponse { accepted: false }))
            }
        }
    }

    async fn ack_task(
        &self,
        request: Request<AckTaskRequest>,
    ) -> Result<Response<AckTaskResponse>, Status> {
        let node_id = node_id_from_metadata(&request)?;
        self.authenticate_request(&request, &node_id)?;
        Err(Status::failed_precondition(
            "legacy task acknowledgement cannot prove relay frame ownership; use AckFrame",
        ))
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

fn node_id_from_metadata<T>(request: &Request<T>) -> Result<String, Status> {
    request
        .metadata()
        .get(NODE_ID_METADATA_KEY)
        .ok_or_else(|| {
            Status::unauthenticated(format!(
                "ConnectNode requires {NODE_ID_METADATA_KEY} metadata"
            ))
        })?
        .to_str()
        .map(str::trim)
        .map(str::to_string)
        .map_err(|_| Status::invalid_argument("ConnectNode node metadata must be ASCII"))
        .and_then(|value| {
            if value.is_empty() {
                Err(Status::invalid_argument(
                    "ConnectNode node id cannot be empty",
                ))
            } else {
                Ok(value)
            }
        })
}

fn required_node_value(value: &str, field: &str) -> Result<String, Status> {
    let value = value.trim();
    if value.is_empty() {
        Err(Status::invalid_argument(format!("{field} is required")))
    } else {
        Ok(value.to_string())
    }
}

fn parse_registry_peer_id(value: &str) -> Result<PeerId, Status> {
    PeerId::new(value.trim()).map_err(|error| {
        Status::invalid_argument(format!("node id is not a valid registry peer id: {error}"))
    })
}

fn ensure_frame_routed(delivery: crate::runtime::FrameDelivery) -> Result<(), Status> {
    match delivery {
        crate::runtime::FrameDelivery::Delivered | crate::runtime::FrameDelivery::Mailboxed => {
            Ok(())
        }
        crate::runtime::FrameDelivery::RejectedDuplicate => Err(Status::already_exists(
            "relay frame identity already exists",
        )),
        crate::runtime::FrameDelivery::RejectedCapacity => {
            Err(Status::resource_exhausted("relay frame capacity reached"))
        }
    }
}

fn target_node_id_from_task(task: &TaskEnvelope) -> Result<String, Status> {
    TARGET_NODE_METADATA_KEYS
        .iter()
        .find_map(|key| task.metadata.get(*key))
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .ok_or_else(|| {
            Status::invalid_argument(format!(
                "task metadata must include one of: {}",
                TARGET_NODE_METADATA_KEYS.join(", ")
            ))
        })
}

fn new_relay_frame_id() -> String {
    format!("relay-frame-{}", Uuid::new_v4())
}

fn unix_ms_now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(i64::MAX as u128) as i64
}

/// Bind and serve the relay gRPC surface (health RPC today; other RPCs stubbed).
pub async fn serve_grpc_health(
    runtime: Arc<RelayRuntime>,
    registry: Option<Arc<SkillRegistry>>,
    addr: SocketAddr,
) -> anyhow::Result<()> {
    serve_grpc_health_with_tls(runtime, registry, addr, None).await
}

pub async fn serve_grpc_health_with_tls(
    runtime: Arc<RelayRuntime>,
    registry: Option<Arc<SkillRegistry>>,
    addr: SocketAddr,
    tls_identity: Option<Identity>,
) -> anyhow::Result<()> {
    if tls_identity.is_none() && !addr.ip().is_loopback() {
        anyhow::bail!("non-loopback relay control listeners require TLS");
    }
    let listener = TcpListener::bind(addr).await?;
    let incoming = TcpListenerStream::new(listener);
    let service = match registry {
        Some(registry) => RelayHealthService::with_registry(runtime, registry),
        None => RelayHealthService::new(runtime),
    };
    let mut server = tonic::transport::Server::builder();
    if let Some(identity) = tls_identity {
        server = server.tls_config(ServerTlsConfig::new().identity(identity))?;
    }
    server
        .add_service(
            KeryxRelayServer::new(service)
                .max_decoding_message_size(RESULT_ARTIFACT_FRAME_MAX_BYTES)
                .max_encoding_message_size(RESULT_ARTIFACT_FRAME_MAX_BYTES),
        )
        .serve_with_incoming(incoming)
        .await?;
    Ok(())
}

pub async fn serve_grpc_health_with_auth(
    runtime: Arc<RelayRuntime>,
    registry: Arc<SkillRegistry>,
    node_auth: Arc<NodeTokenAuth>,
    addr: SocketAddr,
) -> anyhow::Result<()> {
    serve_grpc_health_with_auth_and_tls(runtime, registry, node_auth, addr, None).await
}

pub async fn serve_grpc_health_with_auth_and_tls(
    runtime: Arc<RelayRuntime>,
    registry: Arc<SkillRegistry>,
    node_auth: Arc<NodeTokenAuth>,
    addr: SocketAddr,
    tls_identity: Option<Identity>,
) -> anyhow::Result<()> {
    if tls_identity.is_none() && !addr.ip().is_loopback() {
        anyhow::bail!("non-loopback authenticated relay control listeners require TLS");
    }
    let listener = TcpListener::bind(addr).await?;
    let incoming = TcpListenerStream::new(listener);
    let service = RelayHealthService::with_registry_and_auth(runtime, registry, node_auth);
    let mut server = tonic::transport::Server::builder();
    if let Some(identity) = tls_identity {
        server = server.tls_config(ServerTlsConfig::new().identity(identity))?;
    }
    server
        .add_service(
            KeryxRelayServer::new(service)
                .max_decoding_message_size(RESULT_ARTIFACT_FRAME_MAX_BYTES)
                .max_encoding_message_size(RESULT_ARTIFACT_FRAME_MAX_BYTES),
        )
        .serve_with_incoming(incoming)
        .await?;
    Ok(())
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
