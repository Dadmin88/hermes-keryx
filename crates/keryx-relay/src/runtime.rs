//! Shared relay process state for health, telemetry, and relay delivery.

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use keryx_observe::RelayMetrics;
use keryx_proto::v1::RelayFrame;
use tokio::sync::mpsc;
use tonic::Status;

/// Sender used by the gRPC relay stream to push frames to a connected node.
pub type RelayFrameSender = mpsc::Sender<Result<RelayFrame, Status>>;

/// Result of routing a frame to a target node.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FrameDelivery {
    /// The target node had an active stream and the frame was queued to it.
    Delivered,
    /// The target node is not currently reachable, so the frame is held in its mailbox.
    Mailboxed,
}

/// Snapshot of a node identity tracked by the relay runtime.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PeerIdentity {
    pub node_id: String,
    pub connected: bool,
    pub mailbox_depth: usize,
}

#[derive(Debug, Default)]
struct PeerState {
    registered: HashSet<String>,
    connected_nodes: HashMap<String, RelayFrameSender>,
    libp2p_connected_peers: HashSet<String>,
    mailboxes: HashMap<String, VecDeque<RelayFrame>>,
    acked_frame_ids: HashSet<String>,
}

/// Live operational state surfaced by health checks and metrics.
#[derive(Debug)]
pub struct RelayRuntime {
    metrics: Arc<RelayMetrics>,
    started_at: Instant,
    transport_listening: AtomicBool,
    local_peer_id: String,
    peers: Mutex<PeerState>,
}

impl RelayRuntime {
    #[must_use]
    pub fn new(local_peer_id: impl Into<String>) -> Arc<Self> {
        Arc::new(Self {
            metrics: Arc::new(RelayMetrics::new()),
            started_at: Instant::now(),
            transport_listening: AtomicBool::new(false),
            local_peer_id: local_peer_id.into(),
            peers: Mutex::new(PeerState::default()),
        })
    }

    #[must_use]
    pub fn metrics(&self) -> &Arc<RelayMetrics> {
        &self.metrics
    }

    #[must_use]
    pub fn local_peer_id(&self) -> &str {
        &self.local_peer_id
    }

    pub fn mark_transport_listening(&self) {
        self.transport_listening.store(true, Ordering::Relaxed);
    }

    /// Compatibility helper for tests and callers that only track aggregate connections.
    pub fn note_connection_established(&self) {
        self.metrics.increment_connected_peers();
    }

    /// Compatibility helper for tests and callers that only track aggregate connections.
    pub fn note_connection_closed(&self) {
        self.metrics.decrement_connected_peers();
    }

    /// Track a concrete libp2p peer connection and update the connected peer metric idempotently.
    pub fn note_peer_connected(&self, peer_id: impl Into<String>) {
        let mut guard = self.lock_peers();
        guard.libp2p_connected_peers.insert(peer_id.into());
        self.sync_connected_peer_metric(&guard);
    }

    /// Track a concrete libp2p peer disconnect and update the connected peer metric idempotently.
    pub fn note_peer_disconnected(&self, peer_id: &str) {
        let mut guard = self.lock_peers();
        guard.libp2p_connected_peers.remove(peer_id);
        self.sync_connected_peer_metric(&guard);
    }

    /// Register a node identity with the relay control plane.
    pub fn register_node(&self, node_id: impl Into<String>) {
        let node_id = node_id.into();
        let mut guard = self.lock_peers();
        guard.registered.insert(node_id.clone());
        guard.mailboxes.entry(node_id).or_default();
    }

    /// Attach a node's gRPC relay stream and return any frames stored while it was offline.
    pub fn connect_node(
        &self,
        node_id: impl Into<String>,
        sender: RelayFrameSender,
    ) -> Vec<RelayFrame> {
        let node_id = node_id.into();
        let mut guard = self.lock_peers();
        guard.registered.insert(node_id.clone());
        guard.connected_nodes.insert(node_id.clone(), sender);
        let pending = guard
            .mailboxes
            .remove(&node_id)
            .unwrap_or_default()
            .into_iter()
            .filter(|frame| !is_acked(frame, &guard.acked_frame_ids))
            .collect();
        self.sync_connected_peer_metric(&guard);
        pending
    }

    /// Mark a node stream disconnected. A reconnect with the same node id replaces this state.
    pub fn disconnect_node(&self, node_id: &str) {
        let mut guard = self.lock_peers();
        guard.connected_nodes.remove(node_id);
        self.sync_connected_peer_metric(&guard);
    }

    /// Route a frame to a target node, storing it in the offline mailbox when needed.
    pub fn route_frame(
        &self,
        target_node_id: impl Into<String>,
        frame: RelayFrame,
    ) -> FrameDelivery {
        let target_node_id = target_node_id.into();
        let mut guard = self.lock_peers();
        guard.registered.insert(target_node_id.clone());

        if is_acked(&frame, &guard.acked_frame_ids) {
            return FrameDelivery::Delivered;
        }

        if let Some(sender) = guard.connected_nodes.get(&target_node_id).cloned() {
            match sender.try_send(Ok(frame.clone())) {
                Ok(()) => {
                    self.metrics.increment_tasks_routed();
                    return FrameDelivery::Delivered;
                }
                Err(tokio::sync::mpsc::error::TrySendError::Full(Ok(frame)))
                | Err(tokio::sync::mpsc::error::TrySendError::Closed(Ok(frame))) => {
                    guard.connected_nodes.remove(&target_node_id);
                    guard
                        .mailboxes
                        .entry(target_node_id)
                        .or_default()
                        .push_back(frame);
                    self.sync_connected_peer_metric(&guard);
                    self.metrics.increment_tasks_routed();
                    return FrameDelivery::Mailboxed;
                }
                Err(tokio::sync::mpsc::error::TrySendError::Full(Err(_)))
                | Err(tokio::sync::mpsc::error::TrySendError::Closed(Err(_))) => {
                    guard.connected_nodes.remove(&target_node_id);
                    self.sync_connected_peer_metric(&guard);
                    return FrameDelivery::Mailboxed;
                }
            }
        }

        guard
            .mailboxes
            .entry(target_node_id)
            .or_default()
            .push_back(frame);
        self.metrics.increment_tasks_routed();
        FrameDelivery::Mailboxed
    }

    /// Acknowledge a frame and remove any undelivered mailbox copies.
    pub fn ack_frame(&self, frame_id: &str) -> bool {
        let frame_id = frame_id.trim();
        if frame_id.is_empty() {
            return false;
        }
        let mut guard = self.lock_peers();
        guard.acked_frame_ids.insert(frame_id.to_string());
        for mailbox in guard.mailboxes.values_mut() {
            mailbox.retain(|frame| frame.frame_id.trim() != frame_id);
        }
        true
    }

    /// Compatibility acknowledgement for older task-id callers.
    pub fn ack_task(&self, task_id: &str) -> bool {
        let task_id = task_id.trim();
        if task_id.is_empty() {
            return false;
        }
        let mut guard = self.lock_peers();
        let mut matched = false;
        for mailbox in guard.mailboxes.values_mut() {
            mailbox.retain(|frame| {
                let remove = frame_task_id(frame).as_deref() == Some(task_id);
                matched |= remove;
                !remove
            });
        }
        matched || true
    }

    #[must_use]
    pub fn mailbox_depth(&self, node_id: &str) -> usize {
        self.lock_peers()
            .mailboxes
            .get(node_id)
            .map_or(0, VecDeque::len)
    }

    #[must_use]
    pub fn peer_identity(&self, node_id: &str) -> Option<PeerIdentity> {
        let guard = self.lock_peers();
        if !guard.registered.contains(node_id) && !guard.connected_nodes.contains_key(node_id) {
            return None;
        }
        Some(PeerIdentity {
            node_id: node_id.to_string(),
            connected: guard.connected_nodes.contains_key(node_id),
            mailbox_depth: guard.mailboxes.get(node_id).map_or(0, VecDeque::len),
        })
    }

    #[must_use]
    pub fn uptime_seconds(&self) -> u64 {
        self.started_at.elapsed().as_secs()
    }

    #[must_use]
    pub fn transport_status(&self) -> &'static str {
        if self.transport_listening.load(Ordering::Relaxed) {
            "listening"
        } else {
            "starting"
        }
    }

    #[must_use]
    pub fn is_healthy(&self) -> bool {
        self.transport_listening.load(Ordering::Relaxed)
    }

    fn lock_peers(&self) -> std::sync::MutexGuard<'_, PeerState> {
        self.peers
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    fn sync_connected_peer_metric(&self, guard: &PeerState) {
        let count = guard.connected_nodes.len() + guard.libp2p_connected_peers.len();
        self.metrics.set_connected_peers(count as u64);
    }
}

fn is_acked(frame: &RelayFrame, acked: &HashSet<String>) -> bool {
    !frame.frame_id.trim().is_empty() && acked.contains(frame.frame_id.trim())
}

fn frame_task_id(frame: &RelayFrame) -> Option<String> {
    frame
        .task
        .as_ref()
        .and_then(|task| task.task_id.as_ref())
        .map(|task_id| task_id.value.trim().to_string())
        .filter(|value| !value.is_empty())
}
