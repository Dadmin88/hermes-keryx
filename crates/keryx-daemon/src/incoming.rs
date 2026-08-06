//! Accept relay-delivered tasks into the local store and optionally dispatch to a worker.

use std::collections::HashSet;
use std::sync::Arc;

use keryx_core::{AgentId, IdempotencyKey, LeaseId, TaskId, TaskStatus};
use keryx_proto::v1::{RelayFrame, TaskEnvelope};
use keryx_store::{LeaseRecord, StoreError, StoreResult, TaskEnvelopeRecord, TaskRecord};
use prost::Message;
use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;
use tracing::{info, instrument, warn};

use crate::KeryxDaemonRuntime;

/// Task frame received from the relay control plane (gRPC stream or test channel).
#[derive(Debug, Clone, PartialEq)]
pub struct IncomingRelayTask {
    pub frame_id: String,
    pub sender_node_id: String,
    pub envelope: TaskEnvelope,
}

impl IncomingRelayTask {
    #[must_use]
    pub fn new(
        frame_id: impl Into<String>,
        sender_node_id: impl Into<String>,
        envelope: TaskEnvelope,
    ) -> Self {
        Self {
            frame_id: frame_id.into(),
            sender_node_id: sender_node_id.into(),
            envelope,
        }
    }

    /// Build from a [`RelayFrame`] when the relay attaches the originating node id separately.
    #[must_use]
    pub fn from_relay_frame(sender_node_id: impl Into<String>, frame: RelayFrame) -> Self {
        Self {
            frame_id: frame.frame_id,
            sender_node_id: sender_node_id.into(),
            envelope: frame.task.unwrap_or(TaskEnvelope {
                task_id: None,
                correlation_id: None,
                idempotency_key: None,
                status: 0,
                messages: vec![],
                metadata: Default::default(),
                deadline_ms: 0,
            }),
        }
    }
}

/// Relay security gate: allowlist check for remote senders (see keryx-relay `security` in Phase 9C).
pub trait SenderAllowlist: Send + Sync {
    fn is_allowed(&self, sender_node_id: &str) -> bool;
}

/// Static allowlist used by the daemon and integration tests until relay security is wired in.
#[derive(Debug, Clone, Default)]
pub struct StaticSenderAllowlist {
    allowed: HashSet<String>,
}

impl StaticSenderAllowlist {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    #[must_use]
    pub fn with_nodes(mut self, nodes: impl IntoIterator<Item = impl Into<String>>) -> Self {
        self.allowed.extend(nodes.into_iter().map(Into::into));
        self
    }

    pub fn allow(&mut self, sender_node_id: impl Into<String>) {
        self.allowed.insert(sender_node_id.into());
    }
}

impl SenderAllowlist for StaticSenderAllowlist {
    fn is_allowed(&self, sender_node_id: &str) -> bool {
        self.allowed.contains(sender_node_id)
    }
}

/// Optional auto-claim after accept so a local worker can execute without a separate ClaimTask RPC.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct IncomingDispatchConfig {
    pub local_worker_id: Option<AgentId>,
    pub lease_duration_ms: i64,
}

impl IncomingDispatchConfig {
    #[must_use]
    pub fn auto_dispatch(worker_id: AgentId, lease_duration_ms: i64) -> Self {
        Self {
            local_worker_id: Some(worker_id),
            lease_duration_ms,
        }
    }
}

/// Outcome of handling one incoming relay task.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IncomingHandleResult {
    Accepted {
        task_id: TaskId,
        dispatched: bool,
        lease_id: Option<LeaseId>,
    },
    RejectedSender {
        sender_node_id: String,
    },
    InvalidEnvelope(String),
    Store(StoreError),
}

/// Validate sender, persist via `accept_task`, and optionally lease to the configured local worker.
#[instrument(
    name = "keryx::daemon::incoming_task",
    skip(runtime, allowlist, dispatch, incoming),
    fields(
        frame_id = %incoming.frame_id,
        sender_node_id = %incoming.sender_node_id,
        task_id = tracing::field::Empty
    )
)]
pub async fn handle_incoming_task(
    runtime: &KeryxDaemonRuntime,
    allowlist: &dyn SenderAllowlist,
    dispatch: &IncomingDispatchConfig,
    incoming: IncomingRelayTask,
) -> IncomingHandleResult {
    if runtime.shutdown_is_active() {
        return IncomingHandleResult::Store(StoreError::Database(
            "daemon is shutting down".to_string(),
        ));
    }

    if !allowlist.is_allowed(&incoming.sender_node_id) {
        warn!(
            component = "incoming_handler",
            sender_node_id = %incoming.sender_node_id,
            frame_id = %incoming.frame_id,
            "rejected incoming task from non-allowed sender"
        );
        return IncomingHandleResult::RejectedSender {
            sender_node_id: incoming.sender_node_id,
        };
    }

    let task_id = match parse_envelope_task_id(&incoming.envelope) {
        Ok(id) => id,
        Err(message) => return IncomingHandleResult::InvalidEnvelope(message),
    };
    tracing::Span::current().record("task_id", tracing::field::display(task_id.as_str()));

    let idempotency_key = match parse_envelope_idempotency_key(&incoming.envelope) {
        Ok(key) => key,
        Err(message) => return IncomingHandleResult::InvalidEnvelope(message),
    };
    let deadline_ms = match deadline_from_envelope(&incoming.envelope) {
        Ok(deadline_ms) => deadline_ms,
        Err(message) => return IncomingHandleResult::InvalidEnvelope(message),
    };
    let encoded_envelope = incoming.envelope.encode_to_vec();

    let mut record = TaskRecord::new(task_id.clone(), TaskStatus::Pending, idempotency_key);
    record.deadline_ms = deadline_ms;
    let envelope_record = TaskEnvelopeRecord::new(task_id.clone(), encoded_envelope, unix_ms_now());
    let accepted = match runtime
        .accept_pending_task_with_envelope_backpressure(record, envelope_record)
        .await
    {
        Ok(task) => task,
        Err(error) => return IncomingHandleResult::Store(error),
    };
    runtime.metrics().increment_tasks_submitted();

    let mut dispatched = false;
    let mut lease_id = None;
    if let Some(worker_id) = dispatch.local_worker_id.as_ref() {
        match dispatch_to_local_worker(
            runtime,
            accepted.task_id(),
            worker_id,
            dispatch.lease_duration_ms,
            runtime.config().lease_default_ttl_ms(),
        )
        .await
        {
            Ok(lease) => {
                dispatched = true;
                lease_id = Some(lease);
                runtime.metrics().increment_tasks_claimed();
            }
            Err(StoreError::TaskDeadlineExpired { .. }) => {}
            Err(error) => return IncomingHandleResult::Store(error),
        }
    }

    info!(
        component = "incoming_handler",
        task_id = %accepted.task_id().as_str(),
        dispatched,
        frame_id = %incoming.frame_id,
        sender_node_id = %incoming.sender_node_id,
        "accepted incoming relay task"
    );

    IncomingHandleResult::Accepted {
        task_id: accepted.task_id().clone(),
        dispatched,
        lease_id,
    }
}

async fn dispatch_to_local_worker(
    runtime: &KeryxDaemonRuntime,
    task_id: &TaskId,
    worker_id: &AgentId,
    lease_duration_ms: i64,
    default_lease_duration_ms: i64,
) -> StoreResult<LeaseId> {
    let duration_ms = if lease_duration_ms <= 0 {
        default_lease_duration_ms
    } else {
        lease_duration_ms
    };
    let leased_at_ms = unix_ms_now();
    let expires_at_ms = leased_at_ms.saturating_add(duration_ms);
    let lease_id = new_lease_id(task_id, leased_at_ms);
    let lease = LeaseRecord::new(
        lease_id.clone(),
        task_id.clone(),
        worker_id.clone(),
        leased_at_ms,
        expires_at_ms,
    );
    runtime
        .store()
        .lease_task_for_peer(task_id, lease, runtime.config().local_peer_id())
        .await?;
    Ok(lease_id)
}

fn parse_envelope_task_id(envelope: &TaskEnvelope) -> Result<TaskId, String> {
    let value = envelope
        .task_id
        .as_ref()
        .map(|id| id.value.trim())
        .filter(|value| !value.is_empty())
        .ok_or_else(|| "envelope.task_id is required".to_string())?;
    TaskId::new(value).map_err(|error| error.to_string())
}

fn parse_envelope_idempotency_key(
    envelope: &TaskEnvelope,
) -> Result<Option<IdempotencyKey>, String> {
    let Some(key) = envelope.idempotency_key.as_ref() else {
        return Ok(None);
    };
    let trimmed = key.value.trim();
    if trimmed.is_empty() {
        return Ok(None);
    }
    IdempotencyKey::new(trimmed)
        .map(Some)
        .map_err(|error| error.to_string())
}

fn deadline_from_envelope(envelope: &TaskEnvelope) -> Result<Option<i64>, String> {
    if envelope.deadline_ms < 0 {
        return Err("deadline_ms must be zero or a positive Unix epoch timestamp".to_string());
    }
    Ok((envelope.deadline_ms > 0).then_some(envelope.deadline_ms))
}

fn new_lease_id(task_id: &TaskId, leased_at_ms: i64) -> LeaseId {
    LeaseId::new(format!("lease-{}-{leased_at_ms}", task_id.as_str()))
        .expect("daemon-generated lease id is valid")
}

fn unix_ms_now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(i64::MAX as u128) as i64
}

/// Handle returned by [`IncomingTaskLoop::spawn`].
pub struct IncomingTaskLoopHandle {
    stop_tx: watch::Sender<bool>,
    join: JoinHandle<()>,
}

impl IncomingTaskLoopHandle {
    pub async fn shutdown(self) {
        let _ = self.stop_tx.send(true);
        let _ = self.join.await;
    }
}

/// Background loop that reads relay frames from a channel (production: fed by relay gRPC stream).
pub struct IncomingTaskLoop;

impl IncomingTaskLoop {
    #[must_use]
    pub fn spawn(
        runtime: Arc<KeryxDaemonRuntime>,
        allowlist: Arc<dyn SenderAllowlist>,
        dispatch: IncomingDispatchConfig,
        mut source: mpsc::Receiver<IncomingRelayTask>,
    ) -> IncomingTaskLoopHandle {
        let (stop_tx, mut stop_rx) = watch::channel(false);
        let join = tokio::spawn(async move {
            loop {
                tokio::select! {
                    item = source.recv() => {
                        match item {
                            Some(incoming) => {
                                let _ = handle_incoming_task(
                                    runtime.as_ref(),
                                    allowlist.as_ref(),
                                    &dispatch,
                                    incoming,
                                )
                                .await;
                            }
                            None => {
                                info!(component = "incoming_task_loop", "relay source closed");
                                break;
                            }
                        }
                    }
                    changed = stop_rx.changed() => {
                        if changed.is_err() || *stop_rx.borrow() {
                            info!(component = "incoming_task_loop", "stopping");
                            break;
                        }
                    }
                }
            }
        });
        IncomingTaskLoopHandle { stop_tx, join }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use keryx_proto::v1::TaskId as ProtoTaskId;

    fn envelope(task_id: &str) -> TaskEnvelope {
        TaskEnvelope {
            task_id: Some(ProtoTaskId {
                value: task_id.to_string(),
            }),
            correlation_id: None,
            idempotency_key: None,
            status: 0,
            messages: vec![],
            metadata: Default::default(),
            deadline_ms: 0,
        }
    }

    #[test]
    fn static_allowlist_rejects_unknown_sender() {
        let list = StaticSenderAllowlist::new().with_nodes(["node-a"]);
        assert!(list.is_allowed("node-a"));
        assert!(!list.is_allowed("node-b"));
    }

    #[test]
    fn from_relay_frame_preserves_frame_id() {
        let task = IncomingRelayTask::from_relay_frame(
            "node-remote",
            RelayFrame {
                frame_id: "frame-1".to_string(),
                task: Some(envelope("task-1")),
                result: None,
                authenticated_source_node_id: "node-remote".to_string(),
                destination_node_id: "node-local".to_string(),
            },
        );
        assert_eq!(task.frame_id, "frame-1");
        assert_eq!(task.sender_node_id, "node-remote");
    }
}
