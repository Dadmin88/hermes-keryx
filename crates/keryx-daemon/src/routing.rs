//! Task routing between local store and relay-connected peers.

use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use keryx_core::{IdempotencyKey, PeerId, TaskId, TaskStatus};
use keryx_proto::v1::{keryx_relay_client::KeryxRelayClient, PublishTaskRequest, TaskEnvelope};
use keryx_store::{
    SqliteStore, StoreError, TaskEnvelopeRecord, TaskRecord, TaskTransportContextRecord,
};
use prost::Message;
use thiserror::Error;
use tokio::sync::RwLock;
use tonic::transport::Channel;
use tonic::Request;
use tonic::Status;
use tracing::{info, instrument, warn};

use crate::grpc_transport::{ca_cert_path_from_env, secure_grpc_endpoint};

/// Default outbound delivery timeout when callers omit `timeout_ms`.
pub const DEFAULT_SEND_TASK_TIMEOUT_MS: u64 = 30_000;

/// How a task was delivered after [`TaskRouter::send_task`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeliveryRoute {
    Local,
    Relay,
    AwaitingApproval,
}

impl DeliveryRoute {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Local => "local",
            Self::Relay => "relay",
            Self::AwaitingApproval => "awaiting_approval",
        }
    }
}

/// Outcome of a routed task submission.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SendTaskOutcome {
    pub task_id: TaskId,
    pub status: String,
    pub routed_to: PeerId,
    pub route: DeliveryRoute,
}

/// Connected peer snapshot returned by [`PeerDirectory::list_peers`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PeerInfo {
    pub peer_id: PeerId,
    pub connected: bool,
    pub local: bool,
}

#[derive(Debug, Error)]
pub enum RoutingError {
    #[error("target peer is unknown: {peer_id}")]
    UnknownPeer { peer_id: String },
    #[error("relay delivery timed out for peer {peer_id}")]
    Timeout { peer_id: String },
    #[error("relay is not configured")]
    RelayUnavailable,
    #[error("relay delivery failed for peer {peer_id}: {reason}")]
    RelayFailed { peer_id: String, reason: String },
    #[error("routing policy denied task: {reason}")]
    PolicyDenied { reason: String },
    #[error("invalid task envelope: {reason}")]
    InvalidEnvelope { reason: String },
    #[error(transparent)]
    Store(#[from] StoreError),
    #[error(transparent)]
    Validation(#[from] keryx_core::ValidationError),
}

/// Publishes a task envelope to a remote peer via the relay control plane.
#[async_trait]
pub trait RelayTaskPublisher: Send + Sync {
    fn is_configured(&self) -> bool {
        true
    }

    async fn deliver_task(
        &self,
        target_peer_id: &PeerId,
        envelope: TaskEnvelope,
        timeout: Duration,
    ) -> Result<(), RoutingError>;
}

/// No-op publisher used when the daemon has no relay session.
#[derive(Debug, Default)]
pub struct NoopRelayPublisher;

#[async_trait]
impl RelayTaskPublisher for NoopRelayPublisher {
    fn is_configured(&self) -> bool {
        false
    }

    async fn deliver_task(
        &self,
        _target_peer_id: &PeerId,
        _envelope: TaskEnvelope,
        _timeout: Duration,
    ) -> Result<(), RoutingError> {
        Err(RoutingError::RelayUnavailable)
    }
}

/// gRPC relay publisher used when the daemon has a relay control-plane endpoint.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GrpcRelayTaskPublisher {
    endpoint: String,
    source_peer_id: PeerId,
    node_token: Option<String>,
    ca_cert_path: Option<PathBuf>,
}

impl GrpcRelayTaskPublisher {
    #[must_use]
    pub fn new(endpoint: impl Into<String>, source_peer_id: PeerId) -> Self {
        Self {
            endpoint: endpoint.into(),
            source_peer_id,
            node_token: std::env::var("HERMES_KERYX_NODE_TOKEN")
                .ok()
                .map(|value| value.trim().to_string())
                .filter(|value| !value.is_empty()),
            ca_cert_path: ca_cert_path_from_env(),
        }
    }

    async fn connect(
        &self,
        target_peer_id: &PeerId,
    ) -> Result<KeryxRelayClient<Channel>, RoutingError> {
        let endpoint = secure_grpc_endpoint(&self.endpoint, self.ca_cert_path.as_deref()).map_err(
            |error| RoutingError::RelayFailed {
                peer_id: target_peer_id.to_string(),
                reason: error.to_string(),
            },
        )?;
        endpoint
            .connect()
            .await
            .map(KeryxRelayClient::new)
            .map_err(|error| RoutingError::RelayFailed {
                peer_id: target_peer_id.to_string(),
                reason: format!("failed to connect to relay at {}: {error}", self.endpoint),
            })
    }
}

#[async_trait]
impl RelayTaskPublisher for GrpcRelayTaskPublisher {
    async fn deliver_task(
        &self,
        target_peer_id: &PeerId,
        mut envelope: TaskEnvelope,
        _timeout: Duration,
    ) -> Result<(), RoutingError> {
        envelope.metadata.insert(
            "target_node_id".to_string(),
            target_peer_id.as_str().to_string(),
        );
        let mut client = self.connect(target_peer_id).await?;
        let mut request = Request::new(PublishTaskRequest {
            task: Some(envelope),
            target_node_id: target_peer_id.as_str().to_string(),
            source_node_id: self.source_peer_id.as_str().to_string(),
        });
        request.metadata_mut().insert(
            "x-keryx-node-id",
            self.source_peer_id
                .as_str()
                .parse()
                .map_err(|error| RoutingError::RelayFailed {
                    peer_id: target_peer_id.to_string(),
                    reason: format!("invalid source peer metadata: {error}"),
                })?,
        );
        if let Some(token) = &self.node_token {
            request.metadata_mut().insert(
                "x-keryx-node-token",
                token.parse().map_err(|error| RoutingError::RelayFailed {
                    peer_id: target_peer_id.to_string(),
                    reason: format!("invalid node token metadata: {error}"),
                })?,
            );
        }
        client
            .publish_task(request)
            .await
            .map(|_| ())
            .map_err(|status| RoutingError::RelayFailed {
                peer_id: target_peer_id.to_string(),
                reason: format!(
                    "relay PublishTask returned {}: {}",
                    status.code(),
                    status.message()
                ),
            })
    }
}

/// Tracks the local peer id and relay-connected remote peers.
#[derive(Debug)]
pub struct PeerDirectory {
    local_peer_id: PeerId,
    connected_peers: RwLock<HashSet<String>>,
    relay_routable_peers: RwLock<HashSet<String>>,
}

impl PeerDirectory {
    #[must_use]
    pub fn new(local_peer_id: PeerId) -> Self {
        Self {
            local_peer_id,
            connected_peers: RwLock::new(HashSet::new()),
            relay_routable_peers: RwLock::new(HashSet::new()),
        }
    }

    #[must_use]
    pub fn local_peer_id(&self) -> &PeerId {
        &self.local_peer_id
    }

    pub async fn set_connected_peer(&self, peer_id: &PeerId, connected: bool) {
        let mut peers = self.connected_peers.write().await;
        if connected {
            peers.insert(peer_id.as_str().to_string());
        } else {
            peers.remove(peer_id.as_str());
        }
    }

    pub async fn set_routable_peer(&self, peer_id: &PeerId, routable: bool) {
        let mut peers = self.relay_routable_peers.write().await;
        if routable {
            peers.insert(peer_id.as_str().to_string());
        } else {
            peers.remove(peer_id.as_str());
        }
    }

    pub async fn is_connected(&self, peer_id: &PeerId) -> bool {
        if peer_id == &self.local_peer_id {
            return true;
        }
        self.connected_peers.read().await.contains(peer_id.as_str())
    }

    pub async fn is_routable(&self, peer_id: &PeerId) -> bool {
        if self.is_connected(peer_id).await {
            return true;
        }
        self.relay_routable_peers
            .read()
            .await
            .contains(peer_id.as_str())
    }

    pub async fn list_peers(&self) -> Vec<PeerInfo> {
        let connected = self.connected_peers.read().await.clone();
        let routable = self.relay_routable_peers.read().await.clone();
        let mut seen = HashSet::new();
        let mut peers: Vec<PeerInfo> = Vec::new();

        for value in connected {
            if let Ok(peer_id) = PeerId::new(value.clone()) {
                seen.insert(value);
                peers.push(PeerInfo {
                    peer_id,
                    connected: true,
                    local: false,
                });
            }
        }

        for value in routable {
            if seen.contains(&value) {
                continue;
            }
            if let Ok(peer_id) = PeerId::new(value) {
                peers.push(PeerInfo {
                    peer_id,
                    connected: false,
                    local: false,
                });
            }
        }

        peers.push(PeerInfo {
            peer_id: self.local_peer_id.clone(),
            connected: true,
            local: true,
        });
        peers.sort_by(|left, right| left.peer_id.as_str().cmp(right.peer_id.as_str()));
        peers
    }
}

/// Permission assigned by routing policy for a requested capability.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RoutingPolicyPermission {
    Allow,
    Deny,
    ApprovalRequired,
}

impl RoutingPolicyPermission {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Allow => "allow",
            Self::Deny => "deny",
            Self::ApprovalRequired => "approval_required",
        }
    }
}

/// Policy decision returned before the router touches the store or relay.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RoutingPolicyDecision {
    pub permission: RoutingPolicyPermission,
    pub capability_id: Option<String>,
    pub reason: String,
}

impl RoutingPolicyDecision {
    #[must_use]
    pub fn allow(capability_id: Option<String>, reason: impl Into<String>) -> Self {
        Self {
            permission: RoutingPolicyPermission::Allow,
            capability_id,
            reason: reason.into(),
        }
    }

    #[must_use]
    pub fn deny(capability_id: Option<String>, reason: impl Into<String>) -> Self {
        Self {
            permission: RoutingPolicyPermission::Deny,
            capability_id,
            reason: reason.into(),
        }
    }

    #[must_use]
    pub fn awaiting_approval(capability_id: Option<String>, reason: impl Into<String>) -> Self {
        Self {
            permission: RoutingPolicyPermission::ApprovalRequired,
            capability_id,
            reason: reason.into(),
        }
    }
}

/// Minimal capability policy used by the daemon routing gate.
#[derive(Debug, Clone)]
pub struct RoutingPolicy {
    default_permission: RoutingPolicyPermission,
    capability_permissions: HashMap<String, RoutingPolicyPermission>,
}

impl Default for RoutingPolicy {
    fn default() -> Self {
        Self {
            default_permission: RoutingPolicyPermission::Allow,
            capability_permissions: HashMap::new(),
        }
    }
}

impl RoutingPolicy {
    #[must_use]
    pub fn new(default_permission: RoutingPolicyPermission) -> Self {
        Self {
            default_permission,
            capability_permissions: HashMap::new(),
        }
    }

    pub fn set_permission(
        &mut self,
        capability_id: impl Into<String>,
        permission: RoutingPolicyPermission,
    ) -> Option<RoutingPolicyPermission> {
        self.capability_permissions
            .insert(capability_id.into(), permission)
    }

    #[must_use]
    pub fn evaluate(&self, envelope: &TaskEnvelope) -> RoutingPolicyDecision {
        let capability_id = envelope_capability_id(envelope);
        let permission = capability_id
            .as_ref()
            .and_then(|capability_id| self.capability_permissions.get(capability_id))
            .copied()
            .unwrap_or(self.default_permission);

        match permission {
            RoutingPolicyPermission::Allow => {
                RoutingPolicyDecision::allow(capability_id, "capability allowed by routing policy")
            }
            RoutingPolicyPermission::Deny => RoutingPolicyDecision::deny(
                capability_id.clone(),
                format!(
                    "capability {} denied by routing policy",
                    capability_id.as_deref().unwrap_or("<unspecified>")
                ),
            ),
            RoutingPolicyPermission::ApprovalRequired => RoutingPolicyDecision::awaiting_approval(
                capability_id.clone(),
                format!(
                    "capability {} requires approval",
                    capability_id.as_deref().unwrap_or("<unspecified>")
                ),
            ),
        }
    }
}

/// Security audit event emitted by routing policy decisions.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RoutingAuditEvent {
    pub task_id: TaskId,
    pub target_peer_id: PeerId,
    pub capability_id: Option<String>,
    pub decision: RoutingPolicyPermission,
    pub reason: String,
}

impl RoutingAuditEvent {
    #[must_use]
    pub fn new(task_id: TaskId, target_peer_id: PeerId, decision: &RoutingPolicyDecision) -> Self {
        Self {
            task_id,
            target_peer_id,
            capability_id: decision
                .capability_id
                .as_ref()
                .map(|value| redact_secrets(value)),
            decision: decision.permission,
            reason: redact_secrets(&decision.reason),
        }
    }
}

/// Routes task envelopes to the local store or relay publisher.
pub struct TaskRouter {
    peers: Arc<PeerDirectory>,
    publisher: Arc<RwLock<Arc<dyn RelayTaskPublisher>>>,
    policy: Arc<RwLock<RoutingPolicy>>,
    audit_events: Arc<RwLock<Vec<RoutingAuditEvent>>>,
    default_timeout_ms: u64,
}

impl std::fmt::Debug for TaskRouter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TaskRouter")
            .field("default_timeout_ms", &self.default_timeout_ms)
            .finish_non_exhaustive()
    }
}

impl TaskRouter {
    #[must_use]
    pub fn new(
        peers: Arc<PeerDirectory>,
        publisher: Arc<dyn RelayTaskPublisher>,
        default_timeout_ms: u64,
    ) -> Self {
        Self {
            peers,
            publisher: Arc::new(RwLock::new(publisher)),
            policy: Arc::new(RwLock::new(RoutingPolicy::default())),
            audit_events: Arc::new(RwLock::new(Vec::new())),
            default_timeout_ms,
        }
    }

    pub async fn set_publisher(&self, publisher: Arc<dyn RelayTaskPublisher>) {
        *self.publisher.write().await = publisher;
    }

    pub async fn set_policy(&self, policy: RoutingPolicy) {
        *self.policy.write().await = policy;
    }

    pub async fn audit_events(&self) -> Vec<RoutingAuditEvent> {
        self.audit_events.read().await.clone()
    }

    async fn record_audit_event(&self, event: RoutingAuditEvent) {
        self.audit_events.write().await.push(event);
    }

    #[instrument(
        name = "keryx::routing::route_task",
        skip(self, store, envelope),
        fields(
            target_peer_id = %target_peer_id.as_str()
        )
    )]
    pub async fn route_task(
        &self,
        store: &SqliteStore,
        target_peer_id: PeerId,
        envelope: TaskEnvelope,
        timeout_ms: i64,
    ) -> Result<SendTaskOutcome, RoutingError> {
        self.send_task(store, target_peer_id, envelope, timeout_ms)
            .await
    }

    #[instrument(
        name = "keryx::routing::send_task",
        skip(self, store, envelope),
        fields(
            target_peer_id = %target_peer_id.as_str(),
            task_id = tracing::field::Empty
        )
    )]
    pub async fn send_task(
        &self,
        store: &SqliteStore,
        target_peer_id: PeerId,
        envelope: TaskEnvelope,
        timeout_ms: i64,
    ) -> Result<SendTaskOutcome, RoutingError> {
        let task_id = parse_envelope_task_id(&envelope)?;
        tracing::Span::current().record("task_id", tracing::field::display(task_id.as_str()));
        let deadline_ms = deadline_from_envelope(&envelope)?;

        let policy_decision = self.policy.read().await.evaluate(&envelope);
        let audit_event =
            RoutingAuditEvent::new(task_id.clone(), target_peer_id.clone(), &policy_decision);
        match policy_decision.permission {
            RoutingPolicyPermission::Allow => {
                info!(
                    target: "keryx.security",
                    audit_event = "routing_policy",
                    decision = policy_decision.permission.as_str(),
                    capability_id = audit_event.capability_id.as_deref().unwrap_or("<unspecified>"),
                    task_id = %task_id,
                    target_peer_id = %target_peer_id,
                    "routing policy allowed task"
                );
                self.record_audit_event(audit_event).await;
            }
            RoutingPolicyPermission::Deny => {
                let reason = audit_event.reason.clone();
                warn!(
                    target: "keryx.security",
                    audit_event = "routing_policy",
                    decision = policy_decision.permission.as_str(),
                    capability_id = audit_event.capability_id.as_deref().unwrap_or("<unspecified>"),
                    task_id = %task_id,
                    target_peer_id = %target_peer_id,
                    reason = %reason,
                    "routing policy denied task"
                );
                self.record_audit_event(audit_event).await;
                return Err(RoutingError::PolicyDenied { reason });
            }
            RoutingPolicyPermission::ApprovalRequired => {
                let reason = audit_event.reason.clone();
                info!(
                    target: "keryx.security",
                    audit_event = "routing_policy",
                    decision = policy_decision.permission.as_str(),
                    capability_id = audit_event.capability_id.as_deref().unwrap_or("<unspecified>"),
                    task_id = %task_id,
                    target_peer_id = %target_peer_id,
                    reason = %reason,
                    "routing policy placed task in AwaitingApproval"
                );
                self.record_audit_event(audit_event).await;
                return Ok(SendTaskOutcome {
                    task_id,
                    status: "awaiting_approval".to_string(),
                    routed_to: target_peer_id,
                    route: DeliveryRoute::AwaitingApproval,
                });
            }
        }

        if target_peer_id == *self.peers.local_peer_id() {
            let outcome = accept_local_task(store, envelope).await?;
            return Ok(SendTaskOutcome {
                task_id: outcome.task_id,
                status: outcome.status,
                routed_to: target_peer_id,
                route: DeliveryRoute::Local,
            });
        }

        let publisher = Arc::clone(&*self.publisher.read().await);
        if !self.peers.is_routable(&target_peer_id).await && !publisher.is_configured() {
            return Err(RoutingError::UnknownPeer {
                peer_id: target_peer_id.to_string(),
            });
        }

        let encoded_envelope = envelope.encode_to_vec();
        let idempotency_key = parse_envelope_idempotency_key(&envelope)?;
        let mut record = TaskRecord::new(task_id.clone(), TaskStatus::Pending, idempotency_key);
        record.deadline_ms = deadline_ms;
        let now_ms = unix_ms_now();
        let envelope_record = TaskEnvelopeRecord::new(task_id.clone(), encoded_envelope, now_ms);
        let context = TaskTransportContextRecord {
            task_id: task_id.clone(),
            authenticated_sender_peer_id: None,
            expected_executor_peer_id: Some(target_peer_id.clone()),
            destination_peer_id: self.peers.local_peer_id().clone(),
            relay_frame_id: Some(format!("relay-{}", task_id.as_str())),
            received_at_ms: now_ms,
        };
        store
            .accept_task_with_envelope_and_context(record, envelope_record, context)
            .await?;

        let timeout = normalize_timeout(timeout_ms, self.default_timeout_ms);
        let delivery = tokio::time::timeout(
            timeout,
            publisher.deliver_task(&target_peer_id, envelope, timeout),
        )
        .await;

        match delivery {
            Ok(Ok(())) => Ok(SendTaskOutcome {
                task_id,
                status: "delivered".to_string(),
                routed_to: target_peer_id,
                route: DeliveryRoute::Relay,
            }),
            Ok(Err(error)) => Err(error),
            Err(_elapsed) => Err(RoutingError::Timeout {
                peer_id: target_peer_id.to_string(),
            }),
        }
    }

    pub async fn set_peer_connected(&self, peer_id: &PeerId, connected: bool) {
        self.peers.set_connected_peer(peer_id, connected).await;
    }

    pub async fn set_peer_routable(&self, peer_id: &PeerId, routable: bool) {
        self.peers.set_routable_peer(peer_id, routable).await;
    }

    pub async fn list_peers(&self) -> Vec<PeerInfo> {
        self.peers.list_peers().await
    }
}

struct LocalAcceptOutcome {
    task_id: TaskId,
    status: String,
}

async fn accept_local_task(
    store: &SqliteStore,
    envelope: TaskEnvelope,
) -> Result<LocalAcceptOutcome, RoutingError> {
    let task_id = parse_envelope_task_id(&envelope)?;
    let idempotency_key = parse_envelope_idempotency_key(&envelope)?;
    let mut record = TaskRecord::new(task_id.clone(), TaskStatus::Pending, idempotency_key);
    record.deadline_ms = deadline_from_envelope(&envelope)?;
    let accepted = store.accept_task(record).await?;
    Ok(LocalAcceptOutcome {
        task_id,
        status: task_status_label(accepted.status).to_string(),
    })
}

fn envelope_capability_id(envelope: &TaskEnvelope) -> Option<String> {
    [
        "keryx.capability_id",
        "capability_id",
        "capability",
        "skill_id",
        "skill",
    ]
    .iter()
    .filter_map(|key| envelope.metadata.get(*key))
    .map(|value| value.trim())
    .find(|value| !value.is_empty())
    .map(ToOwned::to_owned)
}

fn parse_envelope_task_id(envelope: &TaskEnvelope) -> Result<TaskId, RoutingError> {
    let value = envelope
        .task_id
        .as_ref()
        .map(|id| id.value.trim())
        .filter(|value| !value.is_empty())
        .ok_or_else(|| RoutingError::RelayFailed {
            peer_id: String::new(),
            reason: "envelope.task_id is required".to_string(),
        })?;
    TaskId::new(value).map_err(RoutingError::from)
}

fn parse_envelope_idempotency_key(
    envelope: &TaskEnvelope,
) -> Result<Option<IdempotencyKey>, StoreError> {
    let Some(key) = envelope.idempotency_key.as_ref() else {
        return Ok(None);
    };
    let trimmed = key.value.trim();
    if trimmed.is_empty() {
        return Ok(None);
    }
    IdempotencyKey::new(trimmed)
        .map(Some)
        .map_err(StoreError::Validation)
}

fn normalize_timeout(timeout_ms: i64, default_timeout_ms: u64) -> Duration {
    if timeout_ms <= 0 {
        Duration::from_millis(default_timeout_ms)
    } else {
        Duration::from_millis(timeout_ms as u64)
    }
}

fn task_status_label(status: TaskStatus) -> &'static str {
    match status {
        TaskStatus::Pending => "pending",
        TaskStatus::Running => "running",
        TaskStatus::Completed => "completed",
        TaskStatus::Failed => "failed",
    }
}

fn redact_secrets(input: &str) -> String {
    let mut output = Vec::new();
    for segment in input.split_whitespace() {
        let lower = segment.to_ascii_lowercase();
        if lower.contains("token")
            || lower.contains("secret")
            || lower.contains("password")
            || lower.contains("authorization")
            || lower.contains("bearer")
        {
            if let Some((key, _value)) = segment.split_once('=') {
                output.push(format!("{key}=<redacted>"));
            } else if let Some((key, _value)) = segment.split_once(':') {
                output.push(format!("{key}:<redacted>"));
            } else {
                output.push("<redacted>".to_string());
            }
        } else {
            output.push(segment.to_string());
        }
    }
    output.join(" ")
}

pub fn routing_error_to_status(error: RoutingError) -> Status {
    match error {
        RoutingError::UnknownPeer { peer_id } => {
            Status::not_found(format!("unknown peer: {peer_id}"))
        }
        RoutingError::Timeout { peer_id } => {
            Status::deadline_exceeded(format!("delivery timed out for peer {peer_id}"))
        }
        RoutingError::RelayUnavailable => Status::unavailable("relay is not connected"),
        RoutingError::RelayFailed { peer_id, reason } => {
            let reason = redact_secrets(&reason);
            if peer_id.is_empty() {
                Status::invalid_argument(reason)
            } else {
                Status::unavailable(format!("relay delivery failed for {peer_id}: {reason}"))
            }
        }
        RoutingError::PolicyDenied { reason } => Status::permission_denied(redact_secrets(&reason)),
        RoutingError::InvalidEnvelope { reason } => Status::invalid_argument(reason),
        RoutingError::Store(store_error) => super::store_error_to_status(store_error),
        RoutingError::Validation(validation_error) => {
            Status::invalid_argument(validation_error.to_string())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use keryx_proto::v1::TaskId as ProtoTaskId;

    #[tokio::test]
    async fn relay_publisher_rejects_remote_plaintext_endpoint() {
        let publisher = GrpcRelayTaskPublisher::new(
            "http://192.0.2.1:50052",
            PeerId::new("source-peer").unwrap(),
        );
        let target = PeerId::new("target-peer").unwrap();

        let error = publisher.connect(&target).await.unwrap_err();
        assert!(error.to_string().contains("require TLS"));
    }

    #[test]
    fn routing_policy_requires_approval_for_matching_capability() {
        let mut envelope = TaskEnvelope {
            task_id: Some(ProtoTaskId {
                value: "task:deploy".to_string(),
            }),
            deadline_ms: 0,
            ..TaskEnvelope::default()
        };
        envelope
            .metadata
            .insert("capability_id".to_string(), "cap:deploy".to_string());

        let mut policy = RoutingPolicy::default();
        policy.set_permission("cap:deploy", RoutingPolicyPermission::ApprovalRequired);
        let decision = policy.evaluate(&envelope);

        assert_eq!(
            decision.permission,
            RoutingPolicyPermission::ApprovalRequired
        );
        assert_eq!(decision.capability_id.as_deref(), Some("cap:deploy"));
    }

    #[tokio::test]
    async fn send_task_rejects_negative_deadline_before_approval_policy() {
        let dir = tempfile::tempdir().unwrap();
        let store = SqliteStore::connect(&dir.path().join("keryx.sqlite3"))
            .await
            .unwrap();
        store.migrate().await.unwrap();
        let local_peer = PeerId::new("node-policy-deadline").unwrap();
        let router = TaskRouter::new(
            Arc::new(PeerDirectory::new(local_peer.clone())),
            Arc::new(NoopRelayPublisher),
            DEFAULT_SEND_TASK_TIMEOUT_MS,
        );
        let mut policy = RoutingPolicy::default();
        policy.set_permission(
            "cap:approval-required",
            RoutingPolicyPermission::ApprovalRequired,
        );
        router.set_policy(policy).await;
        let mut envelope = TaskEnvelope {
            task_id: Some(ProtoTaskId {
                value: "route-invalid-before-policy".to_string(),
            }),
            deadline_ms: -1,
            ..TaskEnvelope::default()
        };
        envelope.metadata.insert(
            "capability_id".to_string(),
            "cap:approval-required".to_string(),
        );

        let error = router
            .send_task(&store, local_peer, envelope, 0)
            .await
            .unwrap_err();

        assert!(matches!(error, RoutingError::InvalidEnvelope { .. }));
        let task_id = TaskId::new("route-invalid-before-policy").unwrap();
        assert!(store.get_task(&task_id).await.is_err());
        assert!(router.audit_events().await.is_empty());
    }
}

#[cfg(test)]
#[allow(dead_code)]
pub mod test_support {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;
    use tokio::sync::Mutex;

    #[derive(Debug, Default)]
    pub struct MockRelayPublisher {
        delay: Duration,
        fail: bool,
        deliveries: Mutex<Vec<(String, String)>>,
        call_count: AtomicUsize,
    }

    impl MockRelayPublisher {
        #[must_use]
        pub fn new() -> Self {
            Self::default()
        }

        #[must_use]
        pub fn with_delay(mut self, delay: Duration) -> Self {
            self.delay = delay;
            self
        }

        #[must_use]
        pub fn failing(mut self) -> Self {
            self.fail = true;
            self
        }

        pub async fn deliveries(&self) -> Vec<(String, String)> {
            self.deliveries.lock().await.clone()
        }

        pub fn call_count(&self) -> usize {
            self.call_count.load(Ordering::SeqCst)
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
            self.call_count.fetch_add(1, Ordering::SeqCst);
            if self.delay > Duration::ZERO {
                tokio::time::sleep(self.delay).await;
            }
            if self.fail {
                return Err(RoutingError::RelayFailed {
                    peer_id: target_peer_id.to_string(),
                    reason: "mock relay failure".to_string(),
                });
            }
            let task_id = envelope
                .task_id
                .as_ref()
                .map(|id| id.value.clone())
                .unwrap_or_default();
            self.deliveries
                .lock()
                .await
                .push((target_peer_id.to_string(), task_id));
            Ok(())
        }
    }
}

fn deadline_from_envelope(envelope: &TaskEnvelope) -> Result<Option<i64>, RoutingError> {
    if envelope.deadline_ms < 0 {
        return Err(RoutingError::InvalidEnvelope {
            reason: "deadline_ms must be zero or a positive Unix epoch timestamp".to_string(),
        });
    }
    Ok((envelope.deadline_ms > 0).then_some(envelope.deadline_ms))
}

fn unix_ms_now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}
