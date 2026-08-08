//! gRPC health endpoint for the relay.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

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

struct PendingFrameOwnership {
    runtime: Arc<RelayRuntime>,
    destination_node_id: String,
    frame_id: String,
    armed: bool,
}

impl PendingFrameOwnership {
    fn new(runtime: Arc<RelayRuntime>, destination_node_id: String, frame_id: String) -> Self {
        Self {
            runtime,
            destination_node_id,
            frame_id,
            armed: true,
        }
    }

    fn abandon(&mut self) {
        if std::mem::take(&mut self.armed) {
            self.runtime
                .abandon_frame(&self.destination_node_id, &self.frame_id);
        }
    }

    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for PendingFrameOwnership {
    fn drop(&mut self) {
        self.abandon();
    }
}

async fn await_frame_acknowledgement(
    acknowledgement: tokio::sync::oneshot::Receiver<()>,
    pending_frame: &mut PendingFrameOwnership,
    timeout: Duration,
) -> Result<(), Status> {
    match tokio::time::timeout(timeout, acknowledgement).await {
        Ok(Ok(())) => {
            pending_frame.disarm();
            Ok(())
        }
        Ok(Err(_)) => {
            pending_frame.abandon();
            Err(Status::unavailable(
                "relay restarted before destination acknowledged result frame",
            ))
        }
        Err(_) => {
            pending_frame.abandon();
            Err(Status::deadline_exceeded(
                "destination did not acknowledge result frame before retry deadline",
            ))
        }
    }
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

        let (pending_count, connection_generation) =
            self.runtime.connect_node_fenced(node_id.clone(), tx);

        let runtime = Arc::clone(&self.runtime);
        let source_node_id = node_id.clone();
        tokio::spawn(async move {
            if let Some(next) = inbound.next().await {
                match next {
                    Ok(_) => {
                        tracing::warn!(
                            source_node_id = %source_node_id,
                            "rejecting mutation frame on receive-only node stream"
                        );
                        runtime.disconnect_node_with_status_if_current(
                            &source_node_id,
                            connection_generation,
                            Status::failed_precondition(
                                "ConnectNode is receive-only; publish through authenticated PublishTask or PublishResult",
                            ),
                        );
                        return;
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
            runtime.disconnect_node_if_current(&source_node_id, connection_generation);
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
        let (delivery, acknowledgement) = self.runtime.route_frame_waiting_for_ack(
            target_node_id.clone(),
            RelayFrame {
                frame_id: frame_id.clone(),
                task: None,
                result: Some(result),
                authenticated_source_node_id: source_node_id,
                destination_node_id: target_node_id.clone(),
            },
        );
        ensure_frame_routed(delivery)?;
        let mut pending_frame = PendingFrameOwnership::new(
            Arc::clone(&self.runtime),
            target_node_id.clone(),
            frame_id.clone(),
        );
        let acknowledgement = acknowledgement
            .ok_or_else(|| Status::internal("accepted result frame lacks acknowledgement state"))?;
        await_frame_acknowledgement(acknowledgement, &mut pending_frame, Duration::from_secs(25))
            .await?;
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
        crate::runtime::FrameDelivery::RejectedInvalid => {
            Err(Status::invalid_argument("relay frame identity is required"))
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

#[cfg(test)]
mod tests {
    use std::collections::{HashMap, HashSet};

    use keryx_proto::v1::{TaskId, TaskResultEnvelope, TerminalOutcome};

    use super::*;

    const SOURCE_NODE_ID: &str = "executor-node";
    const DESTINATION_NODE_ID: &str = "origin-node";
    const SOURCE_TOKEN: &str = "test-token";

    fn test_service(runtime: Arc<RelayRuntime>) -> Arc<RelayHealthService> {
        let source_node_id = SOURCE_NODE_ID.parse().expect("valid source node id");
        let auth = Arc::new(NodeTokenAuth::new(
            HashMap::from([(source_node_id, SOURCE_TOKEN.to_string())]),
            HashSet::new(),
        ));
        Arc::new(RelayHealthService::with_registry_and_auth(
            runtime,
            Arc::new(SkillRegistry::new()),
            auth,
        ))
    }

    fn publish_result_request(task_id: impl Into<String>) -> Request<PublishResultRequest> {
        let mut request = Request::new(PublishResultRequest {
            result: Some(TaskResultEnvelope {
                protocol_version: 1,
                task_id: Some(TaskId {
                    value: task_id.into(),
                }),
                correlation_id: None,
                outcome: TerminalOutcome::Completed as i32,
                executor_peer_id: SOURCE_NODE_ID.to_string(),
                duration_ms: 1,
                completed_at_ms: 1,
                error_reason: String::new(),
                result_metadata: HashMap::new(),
                output_artifacts: Vec::new(),
            }),
            target_node_id: DESTINATION_NODE_ID.to_string(),
            source_node_id: SOURCE_NODE_ID.to_string(),
            frame_id: String::new(),
        });
        request
            .metadata_mut()
            .insert(NODE_ID_METADATA_KEY, SOURCE_NODE_ID.parse().unwrap());
        request
            .metadata_mut()
            .insert(NODE_TOKEN_METADATA_KEY, SOURCE_TOKEN.parse().unwrap());
        request
    }

    fn unrelated_frame() -> RelayFrame {
        RelayFrame {
            frame_id: "unrelated-frame".to_string(),
            task: None,
            result: None,
            authenticated_source_node_id: "unrelated-source".to_string(),
            destination_node_id: DESTINATION_NODE_ID.to_string(),
        }
    }

    fn spawn_publish_result(
        service: Arc<RelayHealthService>,
        task_id: impl Into<String>,
    ) -> tokio::task::JoinHandle<Result<Response<PublishResultResponse>, Status>> {
        let request = publish_result_request(task_id);
        tokio::spawn(async move { KeryxRelay::publish_result(service.as_ref(), request).await })
    }

    async fn wait_for_single_pending_result_frame(runtime: &RelayRuntime) -> String {
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let frame_ids = runtime.test_pending_result_frame_ids(DESTINATION_NODE_ID);
                if let [frame_id] = frame_ids.as_slice() {
                    return frame_id.clone();
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("result frame must reach the ACK-wait state")
    }

    #[tokio::test]
    async fn dropping_publish_result_future_abandons_only_its_exact_frame() {
        let runtime = RelayRuntime::new("publish-result-drop-test");
        let service = test_service(Arc::clone(&runtime));
        assert_eq!(
            runtime.route_frame(DESTINATION_NODE_ID, unrelated_frame()),
            crate::runtime::FrameDelivery::Mailboxed
        );
        let baseline = runtime.test_pending_frame_counts(DESTINATION_NODE_ID);
        assert_eq!(baseline, (0, 1, 1));

        let publish = spawn_publish_result(Arc::clone(&service), "cancelled-result");
        let frame_id = wait_for_single_pending_result_frame(&runtime).await;
        assert_eq!(
            runtime.test_exact_frame_state(DESTINATION_NODE_ID, &frame_id),
            (true, true, true)
        );
        assert_eq!(
            runtime.test_pending_frame_counts(DESTINATION_NODE_ID),
            (1, 2, 2)
        );

        publish.abort();
        assert!(publish.await.unwrap_err().is_cancelled());

        assert_eq!(
            runtime.test_pending_frame_counts(DESTINATION_NODE_ID),
            baseline
        );
        assert_eq!(
            runtime.test_exact_frame_state(DESTINATION_NODE_ID, &frame_id),
            (false, false, false)
        );
        assert_eq!(
            runtime.ack_frame(DESTINATION_NODE_ID, &frame_id),
            FrameAcknowledgement::UnknownFrame
        );
        assert_eq!(
            runtime.test_exact_frame_state(DESTINATION_NODE_ID, "unrelated-frame"),
            (false, true, true)
        );

        let later_publish = spawn_publish_result(service, "later-result");
        let later_frame_id = wait_for_single_pending_result_frame(&runtime).await;
        assert_eq!(
            runtime.ack_frame(DESTINATION_NODE_ID, &later_frame_id),
            FrameAcknowledgement::Accepted
        );
        assert!(later_publish.await.unwrap().unwrap().into_inner().accepted);
        assert_eq!(
            runtime.test_pending_frame_counts(DESTINATION_NODE_ID),
            baseline
        );
    }

    #[tokio::test]
    async fn repeated_publish_result_cancellation_recovers_frame_capacity() {
        const CANCELLATION_ATTEMPTS: usize = 64;

        let runtime = RelayRuntime::new("publish-result-capacity-recovery-test");
        let service = test_service(Arc::clone(&runtime));
        let baseline = runtime.test_pending_frame_counts(DESTINATION_NODE_ID);
        assert_eq!(baseline, (0, 0, 0));

        for attempt in 0..CANCELLATION_ATTEMPTS {
            let publish =
                spawn_publish_result(Arc::clone(&service), format!("cancelled-{attempt}"));
            let frame_id = wait_for_single_pending_result_frame(&runtime).await;
            assert_eq!(
                runtime.test_exact_frame_state(DESTINATION_NODE_ID, &frame_id),
                (true, true, true)
            );
            publish.abort();
            assert!(publish.await.unwrap_err().is_cancelled());
            assert_eq!(
                runtime.test_pending_frame_counts(DESTINATION_NODE_ID),
                baseline,
                "cancellation attempt {attempt} leaked bounded frame capacity"
            );
        }

        let later_publish = spawn_publish_result(service, "accepted-after-cancellations");
        let later_frame_id = wait_for_single_pending_result_frame(&runtime).await;
        assert_eq!(
            runtime.ack_frame(DESTINATION_NODE_ID, &later_frame_id),
            FrameAcknowledgement::Accepted
        );
        assert!(later_publish.await.unwrap().unwrap().into_inner().accepted);
        assert_eq!(
            runtime.test_pending_frame_counts(DESTINATION_NODE_ID),
            baseline
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn cancellation_after_ack_is_recorded_keeps_ack_idempotent_and_leak_free() {
        let runtime = RelayRuntime::new("publish-result-cancel-after-ack-test");
        let service = test_service(Arc::clone(&runtime));
        let publish = spawn_publish_result(service, "cancel-after-ack");
        let frame_id = wait_for_single_pending_result_frame(&runtime).await;

        assert_eq!(
            runtime.ack_frame(DESTINATION_NODE_ID, &frame_id),
            FrameAcknowledgement::Accepted
        );
        assert!(
            !publish.is_finished(),
            "the handler must still be awaiting its next poll before cancellation"
        );
        publish.abort();
        assert!(publish.await.unwrap_err().is_cancelled());

        assert_eq!(
            runtime.ack_frame(DESTINATION_NODE_ID, &frame_id),
            FrameAcknowledgement::Accepted
        );
        assert_eq!(
            runtime.test_exact_frame_state(DESTINATION_NODE_ID, &frame_id),
            (false, false, false)
        );
        assert_eq!(
            runtime.test_pending_frame_counts(DESTINATION_NODE_ID),
            (0, 0, 0)
        );
    }

    #[tokio::test]
    async fn concurrent_ack_and_cancellation_never_leak_or_panic() {
        let runtime = RelayRuntime::new("publish-result-concurrent-ack-cancel-test");
        let service = test_service(Arc::clone(&runtime));
        assert_eq!(
            runtime.route_frame(DESTINATION_NODE_ID, unrelated_frame()),
            crate::runtime::FrameDelivery::Mailboxed
        );
        let baseline = runtime.test_pending_frame_counts(DESTINATION_NODE_ID);
        let publish = spawn_publish_result(service, "concurrent-ack-cancel");
        let frame_id = wait_for_single_pending_result_frame(&runtime).await;
        let barrier = Arc::new(tokio::sync::Barrier::new(2));
        let ack_runtime = Arc::clone(&runtime);
        let ack_frame_id = frame_id.clone();
        let ack_barrier = Arc::clone(&barrier);
        let ack = tokio::spawn(async move {
            ack_barrier.wait().await;
            ack_runtime.ack_frame(DESTINATION_NODE_ID, &ack_frame_id)
        });

        barrier.wait().await;
        publish.abort();
        let publish_result = publish.await;
        let ack_result = ack.await.unwrap();
        match publish_result {
            Err(error) => assert!(error.is_cancelled()),
            Ok(Ok(response)) => assert!(response.into_inner().accepted),
            Ok(Err(status)) => panic!("racing authenticated ACK failed: {status}"),
        }
        assert!(matches!(
            ack_result,
            FrameAcknowledgement::Accepted | FrameAcknowledgement::UnknownFrame
        ));
        if ack_result == FrameAcknowledgement::Accepted {
            assert_eq!(
                runtime.ack_frame(DESTINATION_NODE_ID, &frame_id),
                FrameAcknowledgement::Accepted
            );
        }
        assert_eq!(
            runtime.test_exact_frame_state(DESTINATION_NODE_ID, &frame_id),
            (false, false, false)
        );
        assert_eq!(
            runtime.test_pending_frame_counts(DESTINATION_NODE_ID),
            baseline
        );
        assert_eq!(
            runtime.test_exact_frame_state(DESTINATION_NODE_ID, "unrelated-frame"),
            (false, true, true)
        );
    }

    #[tokio::test]
    async fn aborting_server_request_task_abandons_waiting_result_frame() {
        let runtime = RelayRuntime::new("publish-result-server-request-shutdown-test");
        let service = test_service(Arc::clone(&runtime));
        let server_request_task = spawn_publish_result(service, "server-request-shutdown");
        let frame_id = wait_for_single_pending_result_frame(&runtime).await;

        server_request_task.abort();
        assert!(server_request_task.await.unwrap_err().is_cancelled());

        assert_eq!(
            runtime.test_exact_frame_state(DESTINATION_NODE_ID, &frame_id),
            (false, false, false)
        );
        assert_eq!(
            runtime.test_pending_frame_counts(DESTINATION_NODE_ID),
            (0, 0, 0)
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn ack_receiver_failure_abandons_only_the_exact_result_frame() {
        let runtime = RelayRuntime::new("publish-result-receiver-failure-test");
        let service = test_service(Arc::clone(&runtime));
        assert_eq!(
            runtime.route_frame(DESTINATION_NODE_ID, unrelated_frame()),
            crate::runtime::FrameDelivery::Mailboxed
        );
        let baseline = runtime.test_pending_frame_counts(DESTINATION_NODE_ID);
        let publish = spawn_publish_result(service, "receiver-failure");
        let frame_id = wait_for_single_pending_result_frame(&runtime).await;

        assert!(runtime.test_drop_frame_ack_waiter(DESTINATION_NODE_ID, &frame_id));
        assert_eq!(
            runtime.test_exact_frame_state(DESTINATION_NODE_ID, &frame_id),
            (false, true, true)
        );
        let error = publish.await.unwrap().unwrap_err();
        assert_eq!(error.code(), tonic::Code::Unavailable);
        assert_eq!(
            runtime.test_pending_frame_counts(DESTINATION_NODE_ID),
            baseline
        );
        assert_eq!(
            runtime.test_exact_frame_state(DESTINATION_NODE_ID, "unrelated-frame"),
            (false, true, true)
        );
    }

    #[tokio::test]
    async fn acknowledgement_timeout_abandons_only_the_exact_result_frame() {
        let runtime = RelayRuntime::new("publish-result-timeout-test");
        assert_eq!(
            runtime.route_frame(DESTINATION_NODE_ID, unrelated_frame()),
            crate::runtime::FrameDelivery::Mailboxed
        );
        let baseline = runtime.test_pending_frame_counts(DESTINATION_NODE_ID);
        let frame_id = "timed-out-frame".to_string();
        let (delivery, acknowledgement) = runtime.route_frame_waiting_for_ack(
            DESTINATION_NODE_ID,
            RelayFrame {
                frame_id: frame_id.clone(),
                task: None,
                result: None,
                authenticated_source_node_id: SOURCE_NODE_ID.to_string(),
                destination_node_id: DESTINATION_NODE_ID.to_string(),
            },
        );
        assert_eq!(delivery, crate::runtime::FrameDelivery::Mailboxed);
        let mut pending_frame = PendingFrameOwnership::new(
            Arc::clone(&runtime),
            DESTINATION_NODE_ID.to_string(),
            frame_id.clone(),
        );

        let error = await_frame_acknowledgement(
            acknowledgement.unwrap(),
            &mut pending_frame,
            Duration::from_millis(1),
        )
        .await
        .unwrap_err();

        assert_eq!(error.code(), tonic::Code::DeadlineExceeded);
        assert_eq!(
            runtime.test_pending_frame_counts(DESTINATION_NODE_ID),
            baseline
        );
        assert_eq!(
            runtime.test_exact_frame_state(DESTINATION_NODE_ID, &frame_id),
            (false, false, false)
        );
        assert_eq!(
            runtime.test_exact_frame_state(DESTINATION_NODE_ID, "unrelated-frame"),
            (false, true, true)
        );
    }
}
