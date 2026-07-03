//! Task routing between local store and relay-connected peers.

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use keryx_core::{IdempotencyKey, PeerId, TaskId, TaskStatus};
use keryx_proto::v1::TaskEnvelope;
use keryx_store::{SqliteStore, StoreError, StoreResult, TaskRecord};
use thiserror::Error;
use tokio::sync::RwLock;
use tonic::Status;
use tracing::instrument;

/// Default outbound delivery timeout when callers omit `timeout_ms`.
pub const DEFAULT_SEND_TASK_TIMEOUT_MS: u64 = 30_000;

/// How a task was delivered after [`TaskRouter::send_task`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeliveryRoute {
    Local,
    Relay,
}

impl DeliveryRoute {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Local => "local",
            Self::Relay => "relay",
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
    #[error(transparent)]
    Store(#[from] StoreError),
    #[error(transparent)]
    Validation(#[from] keryx_core::ValidationError),
}

/// Publishes a task envelope to a remote peer via the relay control plane.
#[async_trait]
pub trait RelayTaskPublisher: Send + Sync {
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
    async fn deliver_task(
        &self,
        _target_peer_id: &PeerId,
        _envelope: TaskEnvelope,
        _timeout: Duration,
    ) -> Result<(), RoutingError> {
        Err(RoutingError::RelayUnavailable)
    }
}

/// Tracks the local peer id and relay-connected remote peers.
#[derive(Debug)]
pub struct PeerDirectory {
    local_peer_id: PeerId,
    connected_peers: RwLock<HashSet<String>>,
}

impl PeerDirectory {
    #[must_use]
    pub fn new(local_peer_id: PeerId) -> Self {
        Self {
            local_peer_id,
            connected_peers: RwLock::new(HashSet::new()),
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

    pub async fn is_connected(&self, peer_id: &PeerId) -> bool {
        if peer_id == &self.local_peer_id {
            return true;
        }
        self.connected_peers.read().await.contains(peer_id.as_str())
    }

    pub async fn list_peers(&self) -> Vec<PeerInfo> {
        let connected = self.connected_peers.read().await.clone();
        let mut peers: Vec<PeerInfo> = connected
            .into_iter()
            .filter_map(|value| {
                PeerId::new(value).ok().map(|peer_id| PeerInfo {
                    peer_id,
                    connected: true,
                    local: false,
                })
            })
            .collect();
        peers.push(PeerInfo {
            peer_id: self.local_peer_id.clone(),
            connected: true,
            local: true,
        });
        peers.sort_by(|left, right| left.peer_id.as_str().cmp(right.peer_id.as_str()));
        peers
    }
}

/// Routes task envelopes to the local store or relay publisher.
pub struct TaskRouter {
    peers: Arc<PeerDirectory>,
    publisher: Arc<RwLock<Arc<dyn RelayTaskPublisher>>>,
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
            default_timeout_ms,
        }
    }

    pub async fn set_publisher(&self, publisher: Arc<dyn RelayTaskPublisher>) {
        *self.publisher.write().await = publisher;
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

        if target_peer_id == *self.peers.local_peer_id() {
            let outcome = accept_local_task(store, envelope).await?;
            return Ok(SendTaskOutcome {
                task_id: outcome.task_id,
                status: outcome.status,
                routed_to: target_peer_id,
                route: DeliveryRoute::Local,
            });
        }

        if !self.peers.is_connected(&target_peer_id).await {
            return Err(RoutingError::UnknownPeer {
                peer_id: target_peer_id.to_string(),
            });
        }

        let timeout = normalize_timeout(timeout_ms, self.default_timeout_ms);
        let publisher = Arc::clone(&*self.publisher.read().await);
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
) -> StoreResult<LocalAcceptOutcome> {
    let task_id = parse_envelope_task_id(&envelope).map_err(|error| {
        StoreError::Validation(keryx_core::ValidationError::InvalidIdValue {
            kind: "TaskId",
            value: error.to_string(),
        })
    })?;
    let idempotency_key = parse_envelope_idempotency_key(&envelope)?;
    let record = TaskRecord::new(task_id.clone(), TaskStatus::Pending, idempotency_key);
    let accepted = store.accept_task(record).await?;
    Ok(LocalAcceptOutcome {
        task_id,
        status: task_status_label(accepted.status).to_string(),
    })
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
            if peer_id.is_empty() {
                Status::invalid_argument(reason)
            } else {
                Status::unavailable(format!("relay delivery failed for {peer_id}: {reason}"))
            }
        }
        RoutingError::Store(store_error) => super::store_error_to_status(store_error),
        RoutingError::Validation(validation_error) => {
            Status::invalid_argument(validation_error.to_string())
        }
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
