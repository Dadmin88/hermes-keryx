//! gRPC health endpoint for the relay.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use futures::StreamExt;
use keryx_proto::v1::keryx_relay_server::{KeryxRelay, KeryxRelayServer};
use keryx_proto::v1::{
    AckFrameRequest, AckFrameResponse, AckTaskRequest, AckTaskResponse,
    CompleteNodescaleIdentityBindRequest, CompleteNodescaleIdentityBindResponse,
    CompleteNodescaleIdentityChallengeRequest, CompleteNodescaleIdentityChallengeResponse,
    HealthRequest, HealthResponse, NodeFrame, NodescaleIdentityBindDisposition,
    NodescaleIdentityBindResult, NodescaleIdentityBindV1, NodescaleIdentityChallengeDisposition,
    NodescaleIdentityChallengeResult, NodescaleIdentityChallengeV1,
    PublishNodescaleIdentityBindRequest, PublishNodescaleIdentityBindResponse,
    PublishNodescaleIdentityChallengeRequest, PublishNodescaleIdentityChallengeResponse,
    PublishResultRequest, PublishResultResponse, PublishTaskRequest, PublishTaskResponse,
    RegisterNodeRequest, RegisterNodeResponse, RelayFrame, TaskEnvelope,
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
const NODESCALE_IDENTITY_BIND_FEATURE: &str = "nodescale_identity_bind_v1";
const NODESCALE_IDENTITY_CHALLENGE_FEATURE: &str = "nodescale.identity.challenge.v1";
const MAX_DIRECT_CONTROL_ID_BYTES: usize = 256;
const MAX_DIRECT_CONTROL_NONCE_BYTES: usize = 512;
const MAX_DIRECT_CONTROL_SECRET_BYTES: usize = 512;
const MAX_DIRECT_CONTROL_REASON_BYTES: usize = 512;
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

async fn await_nodescale_identity_bind_completion(
    completion: tokio::sync::oneshot::Receiver<NodescaleIdentityBindResult>,
    pending_frame: &mut PendingFrameOwnership,
    timeout: Duration,
) -> Result<NodescaleIdentityBindResult, Status> {
    match tokio::time::timeout(timeout, completion).await {
        Ok(Ok(result)) => {
            pending_frame.disarm();
            Ok(result)
        }
        Ok(Err(_)) => {
            pending_frame.abandon();
            Err(Status::unavailable(
                "relay restarted before destination completed direct control",
            ))
        }
        Err(_) => {
            pending_frame.abandon();
            Err(Status::deadline_exceeded(
                "destination did not complete direct control before retry deadline",
            ))
        }
    }
}

async fn await_nodescale_identity_challenge_completion(
    completion: tokio::sync::oneshot::Receiver<NodescaleIdentityChallengeResult>,
    pending_frame: &mut PendingFrameOwnership,
    timeout: Duration,
) -> Result<NodescaleIdentityChallengeResult, Status> {
    match tokio::time::timeout(timeout, completion).await {
        Ok(Ok(result)) => {
            pending_frame.disarm();
            Ok(result)
        }
        Ok(Err(_)) => {
            pending_frame.abandon();
            Err(Status::unavailable(
                "relay restarted before destination completed challenge control",
            ))
        }
        Err(_) => {
            pending_frame.abandon();
            Err(Status::deadline_exceeded(
                "destination did not complete challenge control before retry deadline",
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

    fn authenticate_metadata_only<T>(&self, request: &Request<T>) -> Result<String, Status> {
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

    fn authenticate_request<T>(
        &self,
        request: &Request<T>,
        claimed_node_id: &str,
    ) -> Result<String, Status> {
        let claimed_node_id = claimed_node_id.trim();
        if claimed_node_id.is_empty() {
            return Err(Status::invalid_argument("source node id is required"));
        }
        let metadata_node_id = self.authenticate_metadata_only(request)?;
        if metadata_node_id != claimed_node_id {
            return Err(Status::permission_denied(
                "claimed source node does not match authenticated node metadata",
            ));
        }
        Ok(metadata_node_id)
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
        let identity_task = canonical_task_publication_identity(&task);
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
            task: Some(delivered_task.clone()),
            result: None,
            authenticated_source_node_id: source_node_id.clone(),
            destination_node_id: target_node_id.clone(),
            nodescale_identity_bind_v1: None,
            nodescale_identity_challenge_v1: None,
        };
        let receipt = match self.runtime.publish_task_frame(
            &source_node_id,
            &target_node_id,
            task_id.value.trim(),
            (&identity_task, &delivered_task),
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
            PublishedTaskDelivery::RejectedInvalid => {
                return Err(Status::invalid_argument(
                    "PublishTask frame must not contain typed direct control",
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
                nodescale_identity_bind_v1: None,
                nodescale_identity_challenge_v1: None,
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

    async fn publish_nodescale_identity_bind(
        &self,
        request: Request<PublishNodescaleIdentityBindRequest>,
    ) -> Result<Response<PublishNodescaleIdentityBindResponse>, Status> {
        let authenticated_source = self.authenticate_metadata_only(&request)?;
        let inner = request.into_inner();
        let target_node_id = required_node_value(&inner.target_node_id, "target_node_id")?;
        let operation = inner
            .operation
            .ok_or_else(|| Status::invalid_argument("operation is required"))?;
        validate_nodescale_identity_bind(&operation)?;
        self.require_destination_feature(&target_node_id, NODESCALE_IDENTITY_BIND_FEATURE)
            .await?;

        let frame_id = new_relay_frame_id();
        let (delivery, completion) = self
            .runtime
            .route_nodescale_identity_bind_waiting_for_completion(
                target_node_id.clone(),
                RelayFrame {
                    frame_id: frame_id.clone(),
                    task: None,
                    result: None,
                    authenticated_source_node_id: authenticated_source,
                    destination_node_id: target_node_id.clone(),
                    nodescale_identity_bind_v1: Some(operation),
                    nodescale_identity_challenge_v1: None,
                },
            );
        ensure_frame_routed(delivery)?;
        let mut pending_frame = PendingFrameOwnership::new(
            Arc::clone(&self.runtime),
            target_node_id.clone(),
            frame_id.clone(),
        );
        let completion = completion.ok_or_else(|| {
            Status::internal("accepted direct control frame lacks completion state")
        })?;
        let result = await_nodescale_identity_bind_completion(
            completion,
            &mut pending_frame,
            Duration::from_secs(25),
        )
        .await?;
        Ok(Response::new(PublishNodescaleIdentityBindResponse {
            frame_id,
            destination_node_id: target_node_id,
            result: Some(result),
        }))
    }

    async fn complete_nodescale_identity_bind(
        &self,
        request: Request<CompleteNodescaleIdentityBindRequest>,
    ) -> Result<Response<CompleteNodescaleIdentityBindResponse>, Status> {
        let authenticated_destination = self.authenticate_metadata_only(&request)?;
        let inner = request.into_inner();
        let frame_id = required_node_value(&inner.frame_id, "frame_id")?;
        let result = inner
            .result
            .ok_or_else(|| Status::invalid_argument("result is required"))?;
        validate_nodescale_identity_bind_result(&result)?;
        match self.runtime.complete_nodescale_identity_bind(
            &authenticated_destination,
            &frame_id,
            result,
        ) {
            crate::runtime::DirectControlCompletion::Accepted => {
                Ok(Response::new(CompleteNodescaleIdentityBindResponse {
                    accepted: true,
                }))
            }
            crate::runtime::DirectControlCompletion::WrongDestination => {
                Err(Status::permission_denied(
                    "authenticated node does not own the direct control frame",
                ))
            }
            crate::runtime::DirectControlCompletion::UnknownFrame => Err(
                Status::failed_precondition("direct control frame is unknown or already completed"),
            ),
        }
    }

    async fn publish_nodescale_identity_challenge(
        &self,
        request: Request<PublishNodescaleIdentityChallengeRequest>,
    ) -> Result<Response<PublishNodescaleIdentityChallengeResponse>, Status> {
        let authenticated_source = self.authenticate_metadata_only(&request)?;
        let inner = request.into_inner();
        let target_node_id = required_node_value(&inner.target_node_id, "target_node_id")?;
        let operation = inner
            .operation
            .ok_or_else(|| Status::invalid_argument("operation is required"))?;
        validate_nodescale_identity_challenge(&operation)?;
        self.require_destination_feature(&target_node_id, NODESCALE_IDENTITY_CHALLENGE_FEATURE)
            .await?;

        let frame_id = new_relay_frame_id();
        let (delivery, completion) = self
            .runtime
            .route_nodescale_identity_challenge_waiting_for_completion(
                target_node_id.clone(),
                RelayFrame {
                    frame_id: frame_id.clone(),
                    task: None,
                    result: None,
                    authenticated_source_node_id: authenticated_source,
                    destination_node_id: target_node_id.clone(),
                    nodescale_identity_bind_v1: None,
                    nodescale_identity_challenge_v1: Some(operation),
                },
            );
        ensure_frame_routed(delivery)?;
        let mut pending_frame = PendingFrameOwnership::new(
            Arc::clone(&self.runtime),
            target_node_id.clone(),
            frame_id.clone(),
        );
        let completion = completion.ok_or_else(|| {
            Status::internal("accepted challenge control frame lacks completion state")
        })?;
        let result = await_nodescale_identity_challenge_completion(
            completion,
            &mut pending_frame,
            Duration::from_secs(25),
        )
        .await?;
        Ok(Response::new(PublishNodescaleIdentityChallengeResponse {
            frame_id,
            destination_node_id: target_node_id,
            result: Some(result),
        }))
    }

    async fn complete_nodescale_identity_challenge(
        &self,
        request: Request<CompleteNodescaleIdentityChallengeRequest>,
    ) -> Result<Response<CompleteNodescaleIdentityChallengeResponse>, Status> {
        let authenticated_destination = self.authenticate_metadata_only(&request)?;
        let inner = request.into_inner();
        let frame_id = required_node_value(&inner.frame_id, "frame_id")?;
        let result = inner
            .result
            .ok_or_else(|| Status::invalid_argument("result is required"))?;
        validate_nodescale_identity_challenge_result(&result)?;
        match self.runtime.complete_nodescale_identity_challenge(
            &authenticated_destination,
            &frame_id,
            result,
        ) {
            crate::runtime::DirectControlCompletion::Accepted => {
                Ok(Response::new(CompleteNodescaleIdentityChallengeResponse {
                    accepted: true,
                }))
            }
            crate::runtime::DirectControlCompletion::WrongDestination => {
                Err(Status::permission_denied(
                    "authenticated node does not own the challenge control frame",
                ))
            }
            crate::runtime::DirectControlCompletion::UnknownFrame => {
                Err(Status::failed_precondition(
                    "challenge control frame is unknown or already completed",
                ))
            }
        }
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

fn canonical_task_publication_identity(task: &TaskEnvelope) -> TaskEnvelope {
    let mut identity = task.clone();
    identity
        .metadata
        .remove(AUTHENTICATED_SOURCE_FEATURES_METADATA_KEY);
    identity
}

fn parse_registry_peer_id(value: &str) -> Result<PeerId, Status> {
    PeerId::new(value.trim()).map_err(|error| {
        Status::invalid_argument(format!("node id is not a valid registry peer id: {error}"))
    })
}

fn validate_nodescale_identity_challenge(
    operation: &NodescaleIdentityChallengeV1,
) -> Result<(), Status> {
    for (name, value) in [
        ("operation_id", operation.operation_id.as_str()),
        ("network_id", operation.network_id.as_str()),
        ("device_id", operation.device_id.as_str()),
        ("join_session_id", operation.join_session_id.as_str()),
        ("agent_version", operation.agent_version.as_str()),
    ] {
        if value.trim().is_empty() || value.len() > MAX_DIRECT_CONTROL_ID_BYTES {
            return Err(Status::invalid_argument(format!(
                "{name} must be non-empty and at most {MAX_DIRECT_CONTROL_ID_BYTES} bytes"
            )));
        }
    }
    Ok(())
}

fn validate_nodescale_identity_challenge_result(
    result: &NodescaleIdentityChallengeResult,
) -> Result<(), Status> {
    let disposition =
        NodescaleIdentityChallengeDisposition::try_from(result.disposition).map_err(|_| {
            Status::invalid_argument("invalid nodescale identity challenge disposition")
        })?;
    if disposition == NodescaleIdentityChallengeDisposition::Unspecified {
        return Err(Status::invalid_argument(
            "nodescale identity challenge disposition is required",
        ));
    }
    for (name, value, maximum) in [
        (
            "challenge_id",
            result.challenge_id.as_str(),
            MAX_DIRECT_CONTROL_ID_BYTES,
        ),
        (
            "challenge_secret",
            result.challenge_secret.as_str(),
            MAX_DIRECT_CONTROL_SECRET_BYTES,
        ),
        (
            "reason",
            result.reason.as_str(),
            MAX_DIRECT_CONTROL_REASON_BYTES,
        ),
        (
            "code",
            result.code.as_str(),
            MAX_DIRECT_CONTROL_REASON_BYTES,
        ),
    ] {
        if value.len() > maximum {
            return Err(Status::invalid_argument(format!(
                "{name} exceeds {maximum} bytes"
            )));
        }
    }
    match disposition {
        NodescaleIdentityChallengeDisposition::Issued => {
            if !result.accepted
                || result.challenge_id.trim().is_empty()
                || result.challenge_secret.trim().is_empty()
                || result.binding_generation == 0
                || result.expires_at_unix_ms == 0
                || !result.reason.is_empty()
                || !result.code.is_empty()
            {
                return Err(Status::invalid_argument(
                    "issued challenge requires secret, identity, positive fences, and no rejection details",
                ));
            }
        }
        NodescaleIdentityChallengeDisposition::Rejected => {
            if result.accepted
                || !result.challenge_id.is_empty()
                || !result.challenge_secret.is_empty()
                || result.binding_generation != 0
                || result.expires_at_unix_ms != 0
            {
                return Err(Status::invalid_argument(
                    "rejected challenge must not claim active challenge material",
                ));
            }
        }
        NodescaleIdentityChallengeDisposition::Unspecified => unreachable!(),
    }
    Ok(())
}

fn validate_nodescale_identity_bind(operation: &NodescaleIdentityBindV1) -> Result<(), Status> {
    for (name, value, maximum) in [
        (
            "operation_id",
            operation.operation_id.as_str(),
            MAX_DIRECT_CONTROL_ID_BYTES,
        ),
        (
            "network_id",
            operation.network_id.as_str(),
            MAX_DIRECT_CONTROL_ID_BYTES,
        ),
        (
            "device_id",
            operation.device_id.as_str(),
            MAX_DIRECT_CONTROL_ID_BYTES,
        ),
        (
            "join_session_id",
            operation.join_session_id.as_str(),
            MAX_DIRECT_CONTROL_ID_BYTES,
        ),
        (
            "binding_nonce",
            operation.binding_nonce.as_str(),
            MAX_DIRECT_CONTROL_NONCE_BYTES,
        ),
        (
            "agent_version",
            operation.agent_version.as_str(),
            MAX_DIRECT_CONTROL_ID_BYTES,
        ),
    ] {
        let trimmed = value.trim();
        if trimmed.is_empty() || value.len() > maximum {
            return Err(Status::invalid_argument(format!(
                "{name} must be non-empty and at most {maximum} bytes"
            )));
        }
    }
    if operation.binding_generation == 0 {
        return Err(Status::invalid_argument(
            "binding_generation must be positive",
        ));
    }
    Ok(())
}

fn validate_nodescale_identity_bind_result(
    result: &NodescaleIdentityBindResult,
) -> Result<(), Status> {
    let disposition = NodescaleIdentityBindDisposition::try_from(result.disposition)
        .map_err(|_| Status::invalid_argument("invalid nodescale identity bind disposition"))?;
    if disposition == NodescaleIdentityBindDisposition::Unspecified {
        return Err(Status::invalid_argument(
            "nodescale identity bind disposition is required",
        ));
    }
    for (name, value) in [
        ("binding_id", result.binding_id.as_str()),
        ("reason", result.reason.as_str()),
        ("code", result.code.as_str()),
    ] {
        if value.len() > MAX_DIRECT_CONTROL_REASON_BYTES {
            return Err(Status::invalid_argument(format!(
                "{name} exceeds {MAX_DIRECT_CONTROL_REASON_BYTES} bytes"
            )));
        }
    }
    match disposition {
        NodescaleIdentityBindDisposition::Active
        | NodescaleIdentityBindDisposition::AlreadyConfirmed => {
            if !result.accepted
                || result.binding_id.trim().is_empty()
                || result.generation == 0
                || result.revision == 0
                || !result.reason.is_empty()
                || !result.code.is_empty()
            {
                return Err(Status::invalid_argument(
                    "accepted direct control result requires binding identity, positive generation/revision, and no rejection details",
                ));
            }
        }
        NodescaleIdentityBindDisposition::Rejected => {
            if result.accepted
                || !result.binding_id.is_empty()
                || result.generation != 0
                || result.revision != 0
            {
                return Err(Status::invalid_argument(
                    "rejected direct control result must not claim an active binding",
                ));
            }
        }
        NodescaleIdentityBindDisposition::Unspecified => {
            return Err(Status::invalid_argument(
                "nodescale identity bind disposition is required",
            ));
        }
    }
    Ok(())
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

    use std::sync::Mutex;

    use crate::node::{
        dispatch_relay_typed_control_for_local, AuthenticatedDirectContext, DirectControlHandlers,
        LocalTypedControlDispatch, NodescaleIdentityChallengeHandler,
    };
    use keryx_proto::v1::{
        NodescaleIdentityBindDisposition, NodescaleIdentityBindResult, NodescaleIdentityBindV1,
        NodescaleIdentityChallengeDisposition, NodescaleIdentityChallengeResult,
        NodescaleIdentityChallengeV1, PublishNodescaleIdentityBindRequest,
        PublishNodescaleIdentityChallengeRequest, TaskId, TaskResultEnvelope, TerminalOutcome,
    };

    use super::*;

    const SOURCE_NODE_ID: &str = "executor-node";
    const DESTINATION_NODE_ID: &str = "origin-node";
    const SOURCE_TOKEN: &str = "test-token";

    #[test]
    fn task_publication_identity_excludes_only_relay_owned_feature_projection() {
        let mut first = TaskEnvelope::default();
        first.metadata.insert(
            AUTHENTICATED_SOURCE_FEATURES_METADATA_KEY.to_string(),
            "[\"forged-a\"]".to_string(),
        );
        first
            .metadata
            .insert("caller-owned".to_string(), "stable".to_string());
        let mut second = first.clone();
        second.metadata.insert(
            AUTHENTICATED_SOURCE_FEATURES_METADATA_KEY.to_string(),
            "[\"forged-b\"]".to_string(),
        );
        assert_eq!(
            canonical_task_publication_identity(&first),
            canonical_task_publication_identity(&second)
        );

        second
            .metadata
            .insert("caller-owned".to_string(), "changed".to_string());
        assert_ne!(
            canonical_task_publication_identity(&first),
            canonical_task_publication_identity(&second)
        );
    }

    #[test]
    fn typed_nodescale_challenge_result_requires_delivery_only_secret_on_issued_and_none_on_rejected(
    ) {
        use keryx_proto::v1::{
            NodescaleIdentityChallengeDisposition, NodescaleIdentityChallengeResult,
            NodescaleIdentityChallengeV1,
        };

        let request = NodescaleIdentityChallengeV1 {
            operation_id: "challenge-operation".to_string(),
            network_id: "network".to_string(),
            device_id: "device".to_string(),
            join_session_id: "session".to_string(),
            agent_version: "v1".to_string(),
        };
        assert_eq!(
            NODESCALE_IDENTITY_CHALLENGE_FEATURE,
            "nodescale.identity.challenge.v1"
        );
        assert_eq!(request.operation_id, "challenge-operation");

        let issued = NodescaleIdentityChallengeResult {
            disposition: NodescaleIdentityChallengeDisposition::Issued as i32,
            accepted: true,
            challenge_id: "challenge-id".to_string(),
            challenge_secret: "challenge-secret-sentinel".to_string(),
            binding_generation: 1,
            expires_at_unix_ms: 1,
            reason: String::new(),
            code: String::new(),
        };
        assert!(validate_nodescale_identity_challenge_result(&issued).is_ok());

        let rejected = NodescaleIdentityChallengeResult {
            disposition: NodescaleIdentityChallengeDisposition::Rejected as i32,
            accepted: false,
            challenge_id: String::new(),
            challenge_secret: String::new(),
            binding_generation: 0,
            expires_at_unix_ms: 0,
            reason: "safe rejection".to_string(),
            code: "rejected".to_string(),
        };
        assert!(validate_nodescale_identity_challenge_result(&rejected).is_ok());

        let mut contradictory = rejected;
        contradictory.challenge_secret = "challenge-secret-sentinel".to_string();
        let error = validate_nodescale_identity_challenge_result(&contradictory).unwrap_err();
        assert!(!error.message().contains("challenge-secret-sentinel"));
        for source in [
            include_str!("runtime.rs"),
            include_str!("node.rs"),
            include_str!("../../keryx-daemon/src/incoming.rs"),
        ] {
            let production_source = source.split("#[cfg(test)]").next().unwrap();
            assert!(
                !production_source.contains("challenge_secret"),
                "challenge secret must not enter relay runtime, edge, or daemon persistence paths"
            );
        }
    }

    #[test]
    fn typed_nodescale_publish_request_cannot_claim_a_source_identity() {
        let request = PublishNodescaleIdentityBindRequest {
            operation: Some(NodescaleIdentityBindV1 {
                operation_id: "bind-operation".to_string(),
                network_id: "network".to_string(),
                device_id: "device".to_string(),
                join_session_id: "session".to_string(),
                binding_nonce: "secret-nonce".to_string(),
                binding_generation: 1,
                agent_version: "v1".to_string(),
            }),
            target_node_id: DESTINATION_NODE_ID.to_string(),
        };
        assert_eq!(request.target_node_id, DESTINATION_NODE_ID);
        assert!(request.operation.is_some());
    }

    fn valid_nodescale_identity_bind_result(
        disposition: NodescaleIdentityBindDisposition,
    ) -> NodescaleIdentityBindResult {
        NodescaleIdentityBindResult {
            disposition: disposition as i32,
            accepted: disposition != NodescaleIdentityBindDisposition::Rejected,
            binding_id: if disposition == NodescaleIdentityBindDisposition::Rejected {
                String::new()
            } else {
                "binding".to_string()
            },
            generation: if disposition == NodescaleIdentityBindDisposition::Rejected {
                0
            } else {
                1
            },
            revision: if disposition == NodescaleIdentityBindDisposition::Rejected {
                0
            } else {
                1
            },
            reason: String::new(),
            code: String::new(),
        }
    }

    #[test]
    fn typed_direct_control_result_rejects_contradictory_semantics() {
        for disposition in [
            NodescaleIdentityBindDisposition::Active,
            NodescaleIdentityBindDisposition::AlreadyConfirmed,
        ] {
            assert!(validate_nodescale_identity_bind_result(
                &valid_nodescale_identity_bind_result(disposition)
            )
            .is_ok());

            let mut rejected = valid_nodescale_identity_bind_result(disposition);
            rejected.accepted = false;
            assert!(validate_nodescale_identity_bind_result(&rejected).is_err());

            let mut missing_binding = valid_nodescale_identity_bind_result(disposition);
            missing_binding.binding_id.clear();
            assert!(validate_nodescale_identity_bind_result(&missing_binding).is_err());

            let mut zero_generation = valid_nodescale_identity_bind_result(disposition);
            zero_generation.generation = 0;
            assert!(validate_nodescale_identity_bind_result(&zero_generation).is_err());

            let mut zero_revision = valid_nodescale_identity_bind_result(disposition);
            zero_revision.revision = 0;
            assert!(validate_nodescale_identity_bind_result(&zero_revision).is_err());

            let mut rejection_detail = valid_nodescale_identity_bind_result(disposition);
            rejection_detail.reason = "safe rejection detail".to_string();
            assert!(validate_nodescale_identity_bind_result(&rejection_detail).is_err());
        }

        let rejected =
            valid_nodescale_identity_bind_result(NodescaleIdentityBindDisposition::Rejected);
        assert!(validate_nodescale_identity_bind_result(&rejected).is_ok());
        let mut false_rejection = rejected.clone();
        false_rejection.accepted = true;
        assert!(validate_nodescale_identity_bind_result(&false_rejection).is_err());
        let mut claimed_binding = rejected.clone();
        claimed_binding.binding_id = "binding".to_string();
        assert!(validate_nodescale_identity_bind_result(&claimed_binding).is_err());
        let mut claimed_generation = rejected.clone();
        claimed_generation.generation = 1;
        assert!(validate_nodescale_identity_bind_result(&claimed_generation).is_err());
        let mut claimed_revision = rejected;
        claimed_revision.revision = 1;
        assert!(validate_nodescale_identity_bind_result(&claimed_revision).is_err());

        assert!(
            validate_nodescale_identity_bind_result(&NodescaleIdentityBindResult {
                disposition: NodescaleIdentityBindDisposition::Unspecified as i32,
                ..valid_nodescale_identity_bind_result(NodescaleIdentityBindDisposition::Rejected)
            })
            .is_err()
        );
        assert!(
            validate_nodescale_identity_bind_result(&NodescaleIdentityBindResult {
                disposition: 999,
                ..valid_nodescale_identity_bind_result(NodescaleIdentityBindDisposition::Rejected)
            })
            .is_err()
        );
    }

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

    fn direct_operation() -> NodescaleIdentityBindV1 {
        NodescaleIdentityBindV1 {
            operation_id: "bind-operation".to_string(),
            network_id: "network".to_string(),
            device_id: "device".to_string(),
            join_session_id: "session".to_string(),
            binding_nonce: "nonce-not-for-diagnostics".to_string(),
            binding_generation: 1,
            agent_version: "v1".to_string(),
        }
    }

    fn direct_publish_request(token: Option<&str>) -> Request<PublishNodescaleIdentityBindRequest> {
        let mut request = Request::new(PublishNodescaleIdentityBindRequest {
            operation: Some(direct_operation()),
            target_node_id: DESTINATION_NODE_ID.to_string(),
        });
        request
            .metadata_mut()
            .insert(NODE_ID_METADATA_KEY, SOURCE_NODE_ID.parse().unwrap());
        if let Some(token) = token {
            request
                .metadata_mut()
                .insert(NODE_TOKEN_METADATA_KEY, token.parse().unwrap());
        }
        request
    }

    fn challenge_operation() -> NodescaleIdentityChallengeV1 {
        NodescaleIdentityChallengeV1 {
            operation_id: "challenge-operation".to_string(),
            network_id: "network".to_string(),
            device_id: "device".to_string(),
            join_session_id: "session".to_string(),
            agent_version: "v1".to_string(),
        }
    }

    fn challenge_publish_request(
        token: Option<&str>,
    ) -> Request<PublishNodescaleIdentityChallengeRequest> {
        let mut request = Request::new(PublishNodescaleIdentityChallengeRequest {
            operation: Some(challenge_operation()),
            target_node_id: DESTINATION_NODE_ID.to_string(),
        });
        request
            .metadata_mut()
            .insert(NODE_ID_METADATA_KEY, SOURCE_NODE_ID.parse().unwrap());
        if let Some(token) = token {
            request
                .metadata_mut()
                .insert(NODE_TOKEN_METADATA_KEY, token.parse().unwrap());
        }
        request
    }

    #[derive(Default)]
    struct IdempotentChallengeHarness {
        issued_keys: Mutex<HashSet<(String, String)>>,
    }

    #[tonic::async_trait]
    impl NodescaleIdentityChallengeHandler for IdempotentChallengeHarness {
        async fn handle_nodescale_identity_challenge(
            &self,
            context: AuthenticatedDirectContext,
            operation: NodescaleIdentityChallengeV1,
        ) -> anyhow::Result<NodescaleIdentityChallengeResult> {
            let key = (
                context.authenticated_source_node_id().to_string(),
                operation.operation_id,
            );
            if !self.issued_keys.lock().unwrap().insert(key) {
                return Ok(NodescaleIdentityChallengeResult {
                    disposition: NodescaleIdentityChallengeDisposition::Rejected as i32,
                    accepted: false,
                    challenge_id: String::new(),
                    challenge_secret: String::new(),
                    binding_generation: 0,
                    expires_at_unix_ms: 0,
                    reason: "duplicate operation".to_string(),
                    code: "duplicate_operation".to_string(),
                });
            }
            Ok(NodescaleIdentityChallengeResult {
                disposition: NodescaleIdentityChallengeDisposition::Issued as i32,
                accepted: true,
                challenge_id: "issued-challenge".to_string(),
                challenge_secret: "issued-secret-delivery-only".to_string(),
                binding_generation: 1,
                expires_at_unix_ms: 1,
                reason: String::new(),
                code: String::new(),
            })
        }
    }

    impl IdempotentChallengeHarness {
        fn issued_count(&self) -> usize {
            self.issued_keys.lock().unwrap().len()
        }
    }

    fn direct_service(
        runtime: Arc<RelayRuntime>,
        registry: Arc<SkillRegistry>,
        revoked: HashSet<keryx_core::NodeId>,
    ) -> Arc<RelayHealthService> {
        let source_node_id = SOURCE_NODE_ID.parse().expect("valid source node id");
        let destination_node_id = DESTINATION_NODE_ID
            .parse()
            .expect("valid destination node id");
        Arc::new(RelayHealthService::with_registry_and_auth(
            runtime,
            registry,
            Arc::new(NodeTokenAuth::new(
                HashMap::from([
                    (source_node_id, SOURCE_TOKEN.to_string()),
                    (destination_node_id, "destination-token".to_string()),
                ]),
                revoked,
            )),
        ))
    }

    async fn wait_for_single_pending_direct_control_frame(runtime: &RelayRuntime) -> String {
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let frame_ids = runtime.test_pending_direct_control_frame_ids(DESTINATION_NODE_ID);
                if let [frame_id] = frame_ids.as_slice() {
                    return frame_id.clone();
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("direct-control frame must reach completion-wait state")
    }

    #[tokio::test]
    async fn typed_direct_control_authentication_failures_have_no_routing_side_effects() {
        for (token, revoked) in [
            (None, HashSet::new()),
            (Some("wrong-token"), HashSet::new()),
            (
                Some(SOURCE_TOKEN),
                HashSet::from([SOURCE_NODE_ID.parse().expect("valid source node id")]),
            ),
        ] {
            let runtime = RelayRuntime::new("typed-direct-auth-test");
            let service = direct_service(
                Arc::clone(&runtime),
                Arc::new(SkillRegistry::new()),
                revoked,
            );
            let error = KeryxRelay::publish_nodescale_identity_bind(
                service.as_ref(),
                direct_publish_request(token),
            )
            .await
            .unwrap_err();
            assert_eq!(error.code(), tonic::Code::Unauthenticated);
            assert_eq!(
                runtime.test_pending_direct_control_state(DESTINATION_NODE_ID),
                (0, 0, 0)
            );
        }

        let runtime = RelayRuntime::new("typed-direct-auth-mismatch-test");
        let service = direct_service(
            Arc::clone(&runtime),
            Arc::new(SkillRegistry::new()),
            HashSet::new(),
        );
        let mut mismatched_metadata = direct_publish_request(Some(SOURCE_TOKEN));
        mismatched_metadata
            .metadata_mut()
            .insert(NODE_ID_METADATA_KEY, DESTINATION_NODE_ID.parse().unwrap());
        let error =
            KeryxRelay::publish_nodescale_identity_bind(service.as_ref(), mismatched_metadata)
                .await
                .unwrap_err();
        assert_eq!(error.code(), tonic::Code::Unauthenticated);
        assert_eq!(
            runtime.test_pending_direct_control_state(DESTINATION_NODE_ID),
            (0, 0, 0)
        );
    }

    #[tokio::test]
    async fn typed_direct_control_rejects_invalid_bounded_request_without_nonce_echo_or_route() {
        let runtime = RelayRuntime::new("typed-direct-bounds-test");
        let service = direct_service(
            Arc::clone(&runtime),
            Arc::new(SkillRegistry::new()),
            HashSet::new(),
        );
        for mutate in [
            |operation: &mut NodescaleIdentityBindV1| operation.binding_nonce.clear(),
            |operation: &mut NodescaleIdentityBindV1| {
                operation.operation_id = "x".repeat(MAX_DIRECT_CONTROL_ID_BYTES + 1)
            },
        ] {
            let mut request = direct_publish_request(Some(SOURCE_TOKEN));
            mutate(request.get_mut().operation.as_mut().unwrap());
            let error = KeryxRelay::publish_nodescale_identity_bind(service.as_ref(), request)
                .await
                .unwrap_err();
            assert_eq!(error.code(), tonic::Code::InvalidArgument);
            assert!(!error.message().contains("nonce-not-for-diagnostics"));
            assert_eq!(
                runtime.test_pending_direct_control_state(DESTINATION_NODE_ID),
                (0, 0, 0)
            );
        }
    }

    #[tokio::test]
    async fn typed_direct_control_requires_exact_destination_feature_before_admission() {
        let runtime = RelayRuntime::new("typed-direct-feature-test");
        let registry = Arc::new(SkillRegistry::new());
        let service = direct_service(Arc::clone(&runtime), Arc::clone(&registry), HashSet::new());

        let error = KeryxRelay::publish_nodescale_identity_bind(
            service.as_ref(),
            direct_publish_request(Some(SOURCE_TOKEN)),
        )
        .await
        .unwrap_err();
        assert_eq!(error.code(), tonic::Code::FailedPrecondition);
        assert_eq!(
            runtime.test_pending_direct_control_state(DESTINATION_NODE_ID),
            (0, 0, 0)
        );

        registry
            .register_with_features(
                DESTINATION_NODE_ID.parse().unwrap(),
                Vec::new(),
                String::new(),
                String::new(),
                vec![NODESCALE_IDENTITY_BIND_FEATURE.to_string()],
                None,
            )
            .await;
        let publish = tokio::spawn(async move {
            KeryxRelay::publish_nodescale_identity_bind(
                service.as_ref(),
                direct_publish_request(Some(SOURCE_TOKEN)),
            )
            .await
        });
        let frame_id = wait_for_single_pending_direct_control_frame(&runtime).await;
        assert_eq!(
            runtime.test_pending_direct_control_state(DESTINATION_NODE_ID),
            (1, 1, 1)
        );
        publish.abort();
        assert!(publish.await.unwrap_err().is_cancelled());
        assert_eq!(
            runtime.test_pending_direct_control_state(DESTINATION_NODE_ID),
            (0, 0, 0)
        );
        assert_eq!(
            runtime.ack_frame(DESTINATION_NODE_ID, &frame_id),
            FrameAcknowledgement::UnknownFrame
        );
    }

    #[tokio::test]
    async fn typed_direct_control_projects_metadata_source_and_settles_once_at_destination() {
        let runtime = RelayRuntime::new("typed-direct-projection-test");
        let registry = Arc::new(SkillRegistry::new());
        registry
            .register_with_features(
                DESTINATION_NODE_ID.parse().unwrap(),
                Vec::new(),
                String::new(),
                String::new(),
                vec![NODESCALE_IDENTITY_BIND_FEATURE.to_string()],
                None,
            )
            .await;
        let service = direct_service(Arc::clone(&runtime), registry, HashSet::new());
        let (sender, mut receiver) = mpsc::channel(1);
        assert_eq!(runtime.connect_node(DESTINATION_NODE_ID, sender), 0);
        let publish_service = Arc::clone(&service);
        let publish = tokio::spawn(async move {
            KeryxRelay::publish_nodescale_identity_bind(
                publish_service.as_ref(),
                direct_publish_request(Some(SOURCE_TOKEN)),
            )
            .await
        });
        let frame = tokio::time::timeout(Duration::from_secs(2), receiver.recv())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(frame.authenticated_source_node_id, SOURCE_NODE_ID);
        assert_eq!(frame.destination_node_id, DESTINATION_NODE_ID);
        assert_eq!(frame.nodescale_identity_bind_v1, Some(direct_operation()));
        assert!(frame.task.is_none() && frame.result.is_none());

        let mut wrong_destination = Request::new(CompleteNodescaleIdentityBindRequest {
            frame_id: frame.frame_id.clone(),
            result: Some(valid_nodescale_identity_bind_result(
                NodescaleIdentityBindDisposition::Active,
            )),
        });
        wrong_destination
            .metadata_mut()
            .insert(NODE_ID_METADATA_KEY, SOURCE_NODE_ID.parse().unwrap());
        wrong_destination
            .metadata_mut()
            .insert(NODE_TOKEN_METADATA_KEY, SOURCE_TOKEN.parse().unwrap());
        assert_eq!(
            KeryxRelay::complete_nodescale_identity_bind(service.as_ref(), wrong_destination)
                .await
                .unwrap_err()
                .code(),
            tonic::Code::PermissionDenied
        );

        let mut completion = Request::new(CompleteNodescaleIdentityBindRequest {
            frame_id: frame.frame_id.clone(),
            result: Some(valid_nodescale_identity_bind_result(
                NodescaleIdentityBindDisposition::Active,
            )),
        });
        completion
            .metadata_mut()
            .insert(NODE_ID_METADATA_KEY, DESTINATION_NODE_ID.parse().unwrap());
        completion.metadata_mut().insert(
            NODE_TOKEN_METADATA_KEY,
            "destination-token".parse().unwrap(),
        );
        assert!(
            KeryxRelay::complete_nodescale_identity_bind(service.as_ref(), completion)
                .await
                .unwrap()
                .into_inner()
                .accepted
        );
        let published = publish.await.unwrap().unwrap().into_inner();
        assert_eq!(published.frame_id, frame.frame_id);
        assert_eq!(published.destination_node_id, DESTINATION_NODE_ID);
        assert_eq!(published.result.unwrap().binding_id, "binding");

        for frame_id in [frame.frame_id.as_str(), "unknown-direct-control-frame"] {
            let mut duplicate = Request::new(CompleteNodescaleIdentityBindRequest {
                frame_id: frame_id.to_string(),
                result: Some(valid_nodescale_identity_bind_result(
                    NodescaleIdentityBindDisposition::Active,
                )),
            });
            duplicate
                .metadata_mut()
                .insert(NODE_ID_METADATA_KEY, DESTINATION_NODE_ID.parse().unwrap());
            duplicate.metadata_mut().insert(
                NODE_TOKEN_METADATA_KEY,
                "destination-token".parse().unwrap(),
            );
            assert_eq!(
                KeryxRelay::complete_nodescale_identity_bind(service.as_ref(), duplicate)
                    .await
                    .unwrap_err()
                    .code(),
                tonic::Code::FailedPrecondition
            );
        }
    }

    #[tokio::test]
    async fn authenticated_challenge_rpc_projects_frame_completes_at_destination_and_delegates_dedupe(
    ) {
        let runtime = RelayRuntime::new("challenge-rpc-integration-test");
        let registry = Arc::new(SkillRegistry::new());
        let service = direct_service(Arc::clone(&runtime), Arc::clone(&registry), HashSet::new());
        let before = runtime.metrics().snapshot().tasks_routed;

        registry
            .register_with_features(
                DESTINATION_NODE_ID.parse().unwrap(),
                Vec::new(),
                String::new(),
                String::new(),
                vec!["nodescale_identity_challenge_v1".to_string()],
                None,
            )
            .await;
        let error = KeryxRelay::publish_nodescale_identity_challenge(
            service.as_ref(),
            challenge_publish_request(Some(SOURCE_TOKEN)),
        )
        .await
        .unwrap_err();
        assert_eq!(error.code(), tonic::Code::FailedPrecondition);
        assert_eq!(runtime.mailbox_depth(DESTINATION_NODE_ID), 0);
        assert_eq!(runtime.metrics().snapshot().tasks_routed, before);

        registry
            .register_with_features(
                DESTINATION_NODE_ID.parse().unwrap(),
                Vec::new(),
                String::new(),
                String::new(),
                vec![NODESCALE_IDENTITY_CHALLENGE_FEATURE.to_string()],
                None,
            )
            .await;
        let (sender, mut receiver) = mpsc::channel(2);
        assert_eq!(runtime.connect_node(DESTINATION_NODE_ID, sender), 0);
        let harness = Arc::new(IdempotentChallengeHarness::default());
        let handlers = DirectControlHandlers {
            nodescale_identity_bind_v1: None,
            nodescale_identity_challenge_v1: Some(harness.clone()),
        };

        let publish_service = Arc::clone(&service);
        let publish = tokio::spawn(async move {
            KeryxRelay::publish_nodescale_identity_challenge(
                publish_service.as_ref(),
                challenge_publish_request(Some(SOURCE_TOKEN)),
            )
            .await
        });
        let frame = tokio::time::timeout(Duration::from_secs(2), receiver.recv())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(frame.authenticated_source_node_id, SOURCE_NODE_ID);
        assert_eq!(frame.destination_node_id, DESTINATION_NODE_ID);
        assert_eq!(
            frame.nodescale_identity_challenge_v1,
            Some(challenge_operation())
        );
        assert!(frame.task.is_none() && frame.result.is_none());
        assert!(frame.nodescale_identity_bind_v1.is_none());
        let frame_id = frame.frame_id.clone();
        let issued =
            match dispatch_relay_typed_control_for_local(&handlers, DESTINATION_NODE_ID, &frame)
                .await
                .unwrap()
            {
                Some(LocalTypedControlDispatch::Challenge(result)) => result,
                _ => panic!("authenticated challenge frame must dispatch to the challenge handler"),
            };
        assert_eq!(harness.issued_count(), 1);

        let mut wrong_destination = Request::new(CompleteNodescaleIdentityChallengeRequest {
            frame_id: frame_id.clone(),
            result: Some(issued.clone()),
        });
        wrong_destination
            .metadata_mut()
            .insert(NODE_ID_METADATA_KEY, SOURCE_NODE_ID.parse().unwrap());
        wrong_destination
            .metadata_mut()
            .insert(NODE_TOKEN_METADATA_KEY, SOURCE_TOKEN.parse().unwrap());
        assert_eq!(
            KeryxRelay::complete_nodescale_identity_challenge(service.as_ref(), wrong_destination)
                .await
                .unwrap_err()
                .code(),
            tonic::Code::PermissionDenied
        );
        assert_eq!(runtime.metrics().snapshot().tasks_routed, before);

        let mut completion = Request::new(CompleteNodescaleIdentityChallengeRequest {
            frame_id: frame_id.clone(),
            result: Some(issued),
        });
        completion
            .metadata_mut()
            .insert(NODE_ID_METADATA_KEY, DESTINATION_NODE_ID.parse().unwrap());
        completion.metadata_mut().insert(
            NODE_TOKEN_METADATA_KEY,
            "destination-token".parse().unwrap(),
        );
        assert!(
            KeryxRelay::complete_nodescale_identity_challenge(service.as_ref(), completion)
                .await
                .unwrap()
                .into_inner()
                .accepted
        );
        let published = publish.await.unwrap().unwrap().into_inner();
        assert_eq!(published.frame_id, frame_id);
        assert_eq!(published.destination_node_id, DESTINATION_NODE_ID);
        assert_eq!(
            published.result.unwrap().challenge_secret,
            "issued-secret-delivery-only"
        );
        assert_eq!(runtime.metrics().snapshot().tasks_routed, before);

        let duplicate_service = Arc::clone(&service);
        let duplicate_publish = tokio::spawn(async move {
            KeryxRelay::publish_nodescale_identity_challenge(
                duplicate_service.as_ref(),
                challenge_publish_request(Some(SOURCE_TOKEN)),
            )
            .await
        });
        let duplicate_frame = tokio::time::timeout(Duration::from_secs(2), receiver.recv())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        let duplicate_frame_id = duplicate_frame.frame_id.clone();
        let duplicate_result = match dispatch_relay_typed_control_for_local(
            &handlers,
            DESTINATION_NODE_ID,
            &duplicate_frame,
        )
        .await
        .unwrap()
        {
            Some(LocalTypedControlDispatch::Challenge(result)) => result,
            _ => panic!("authenticated challenge frame must dispatch to the challenge handler"),
        };
        assert_eq!(harness.issued_count(), 1);
        assert_eq!(
            duplicate_result.disposition,
            NodescaleIdentityChallengeDisposition::Rejected as i32
        );
        assert!(!duplicate_result.accepted);
        assert!(duplicate_result.challenge_secret.is_empty());

        let mut duplicate_completion = Request::new(CompleteNodescaleIdentityChallengeRequest {
            frame_id: duplicate_frame_id,
            result: Some(duplicate_result),
        });
        duplicate_completion
            .metadata_mut()
            .insert(NODE_ID_METADATA_KEY, DESTINATION_NODE_ID.parse().unwrap());
        duplicate_completion.metadata_mut().insert(
            NODE_TOKEN_METADATA_KEY,
            "destination-token".parse().unwrap(),
        );
        assert!(
            KeryxRelay::complete_nodescale_identity_challenge(
                service.as_ref(),
                duplicate_completion,
            )
            .await
            .unwrap()
            .into_inner()
            .accepted
        );
        let duplicate = duplicate_publish.await.unwrap().unwrap().into_inner();
        let duplicate_result = duplicate.result.unwrap();
        assert_eq!(
            duplicate_result.disposition,
            NodescaleIdentityChallengeDisposition::Rejected as i32
        );
        assert!(duplicate_result.challenge_secret.is_empty());
        assert_eq!(runtime.metrics().snapshot().tasks_routed, before);
    }

    #[tokio::test]
    async fn challenge_timeout_and_publisher_cancellation_release_delivery_only_ownership() {
        let runtime = RelayRuntime::new("challenge-timeout-cleanup-test");
        let frame_id = "timed-out-challenge".to_string();
        let (delivery, completion) = runtime
            .route_nodescale_identity_challenge_waiting_for_completion(
                DESTINATION_NODE_ID,
                RelayFrame {
                    frame_id: frame_id.clone(),
                    task: None,
                    result: None,
                    authenticated_source_node_id: SOURCE_NODE_ID.to_string(),
                    destination_node_id: DESTINATION_NODE_ID.to_string(),
                    nodescale_identity_bind_v1: None,
                    nodescale_identity_challenge_v1: Some(challenge_operation()),
                },
            );
        assert_eq!(delivery, crate::runtime::FrameDelivery::Mailboxed);
        let mut pending_frame = PendingFrameOwnership::new(
            Arc::clone(&runtime),
            DESTINATION_NODE_ID.to_string(),
            frame_id.clone(),
        );
        let error = await_nodescale_identity_challenge_completion(
            completion.unwrap(),
            &mut pending_frame,
            Duration::from_millis(1),
        )
        .await
        .unwrap_err();
        assert_eq!(error.code(), tonic::Code::DeadlineExceeded);
        assert!(!error.message().contains("issued-secret-delivery-only"));
        assert_eq!(runtime.mailbox_depth(DESTINATION_NODE_ID), 0);
        assert_eq!(
            runtime.complete_nodescale_identity_challenge(
                DESTINATION_NODE_ID,
                &frame_id,
                NodescaleIdentityChallengeResult {
                    disposition: NodescaleIdentityChallengeDisposition::Rejected as i32,
                    accepted: false,
                    challenge_id: String::new(),
                    challenge_secret: String::new(),
                    binding_generation: 0,
                    expires_at_unix_ms: 0,
                    reason: "timed out".to_string(),
                    code: "timeout".to_string(),
                },
            ),
            crate::runtime::DirectControlCompletion::UnknownFrame
        );

        let runtime = RelayRuntime::new("challenge-cancel-cleanup-test");
        let registry = Arc::new(SkillRegistry::new());
        registry
            .register_with_features(
                DESTINATION_NODE_ID.parse().unwrap(),
                Vec::new(),
                String::new(),
                String::new(),
                vec![NODESCALE_IDENTITY_CHALLENGE_FEATURE.to_string()],
                None,
            )
            .await;
        let service = direct_service(Arc::clone(&runtime), registry, HashSet::new());
        let (sender, mut receiver) = mpsc::channel(1);
        assert_eq!(runtime.connect_node(DESTINATION_NODE_ID, sender), 0);
        let publish_service = Arc::clone(&service);
        let publish = tokio::spawn(async move {
            KeryxRelay::publish_nodescale_identity_challenge(
                publish_service.as_ref(),
                challenge_publish_request(Some(SOURCE_TOKEN)),
            )
            .await
        });
        let frame_id = tokio::time::timeout(Duration::from_secs(2), receiver.recv())
            .await
            .unwrap()
            .unwrap()
            .unwrap()
            .frame_id;
        publish.abort();
        assert!(publish.await.unwrap_err().is_cancelled());
        assert_eq!(runtime.mailbox_depth(DESTINATION_NODE_ID), 0);
        assert_eq!(
            runtime.ack_frame(DESTINATION_NODE_ID, &frame_id),
            FrameAcknowledgement::UnknownFrame
        );
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
            task: Some(TaskEnvelope::default()),
            result: None,
            authenticated_source_node_id: "unrelated-source".to_string(),
            destination_node_id: DESTINATION_NODE_ID.to_string(),
            nodescale_identity_bind_v1: None,
            nodescale_identity_challenge_v1: None,
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
                task: Some(TaskEnvelope::default()),
                result: None,
                authenticated_source_node_id: SOURCE_NODE_ID.to_string(),
                destination_node_id: DESTINATION_NODE_ID.to_string(),
                nodescale_identity_bind_v1: None,
                nodescale_identity_challenge_v1: None,
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

    #[tokio::test]
    async fn direct_control_timeout_cleans_waiter_ownership_and_mailbox_for_reuse() {
        let runtime = RelayRuntime::new("direct-control-timeout-test");
        let frame_id = "timed-out-direct-control".to_string();
        let (delivery, completion) = runtime.route_nodescale_identity_bind_waiting_for_completion(
            DESTINATION_NODE_ID,
            RelayFrame {
                frame_id: frame_id.clone(),
                task: None,
                result: None,
                authenticated_source_node_id: SOURCE_NODE_ID.to_string(),
                destination_node_id: DESTINATION_NODE_ID.to_string(),
                nodescale_identity_bind_v1: Some(direct_operation()),
                nodescale_identity_challenge_v1: None,
            },
        );
        assert_eq!(delivery, crate::runtime::FrameDelivery::Mailboxed);
        let mut pending_frame = PendingFrameOwnership::new(
            Arc::clone(&runtime),
            DESTINATION_NODE_ID.to_string(),
            frame_id.clone(),
        );
        let error = await_nodescale_identity_bind_completion(
            completion.unwrap(),
            &mut pending_frame,
            Duration::from_millis(1),
        )
        .await
        .unwrap_err();
        assert_eq!(error.code(), tonic::Code::DeadlineExceeded);
        assert_eq!(
            runtime.test_pending_direct_control_state(DESTINATION_NODE_ID),
            (0, 0, 0)
        );
        assert_eq!(
            runtime.ack_frame(DESTINATION_NODE_ID, &frame_id),
            FrameAcknowledgement::UnknownFrame
        );

        let (reuse_delivery, reuse_completion) = runtime
            .route_nodescale_identity_bind_waiting_for_completion(
                DESTINATION_NODE_ID,
                RelayFrame {
                    frame_id: "reused-direct-control-capacity".to_string(),
                    task: None,
                    result: None,
                    authenticated_source_node_id: SOURCE_NODE_ID.to_string(),
                    destination_node_id: DESTINATION_NODE_ID.to_string(),
                    nodescale_identity_bind_v1: Some(direct_operation()),
                    nodescale_identity_challenge_v1: None,
                },
            );
        assert_eq!(reuse_delivery, crate::runtime::FrameDelivery::Mailboxed);
        drop(reuse_completion);
        runtime.abandon_frame(DESTINATION_NODE_ID, "reused-direct-control-capacity");
        assert_eq!(
            runtime.test_pending_direct_control_state(DESTINATION_NODE_ID),
            (0, 0, 0)
        );
    }
}
