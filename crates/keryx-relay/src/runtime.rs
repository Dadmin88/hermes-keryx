//! Shared relay process state for health, telemetry, and relay delivery.

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use keryx_observe::RelayMetrics;
use keryx_proto::v1::{RelayFrame, TaskEnvelope};
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
    /// A still-active or recently acknowledged frame already owns this identity.
    RejectedDuplicate,
    /// The bounded ownership table cannot accept another unacknowledged frame.
    RejectedCapacity,
}

/// Result of acknowledging a relay frame as an authenticated destination.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FrameAcknowledgement {
    /// The destination owns the frame and the acknowledgement was recorded.
    Accepted,
    /// The frame identifier is known, but not for the authenticated destination.
    WrongDestination,
    /// The relay has no record of this frame identifier.
    UnknownFrame,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PublishedTaskIdentity {
    New,
    Retry,
    Conflict,
    RejectedCapacity,
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
    frame_destinations: HashSet<(String, String)>,
    acknowledged_frames: HashSet<(String, String)>,
    acknowledged_frame_order: VecDeque<(String, String)>,
    frame_accepted_at_ms: HashMap<(String, String), i64>,
    published_task_envelopes: HashMap<(String, String), TaskEnvelope>,
}

const MAX_TRACKED_FRAMES: usize = 8_192;
const MAX_RECENT_ACKNOWLEDGEMENTS: usize = 8_192;

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
            .filter(|frame| !is_acked(&node_id, frame, &guard.acknowledged_frames))
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

        let frame_id = frame.frame_id.trim();
        if !frame_id.is_empty() {
            let key = (target_node_id.clone(), frame_id.to_string());
            if guard.frame_destinations.contains(&key) || guard.acknowledged_frames.contains(&key) {
                return FrameDelivery::RejectedDuplicate;
            }
            if guard.frame_destinations.len() >= MAX_TRACKED_FRAMES {
                return FrameDelivery::RejectedCapacity;
            }
            guard.frame_destinations.insert(key);
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

    /// Acknowledge a destination-owned frame and remove only its mailbox copy.
    pub fn ack_frame(&self, destination_node_id: &str, frame_id: &str) -> FrameAcknowledgement {
        let destination_node_id = destination_node_id.trim();
        let frame_id = frame_id.trim();
        if destination_node_id.is_empty() || frame_id.is_empty() {
            return FrameAcknowledgement::UnknownFrame;
        }
        let mut guard = self.lock_peers();
        let key = (destination_node_id.to_string(), frame_id.to_string());
        if guard.acknowledged_frames.contains(&key) {
            return FrameAcknowledgement::Accepted;
        }
        if !guard.frame_destinations.contains(&key) {
            return if guard
                .frame_destinations
                .iter()
                .any(|(_, known_frame_id)| known_frame_id == frame_id)
            {
                FrameAcknowledgement::WrongDestination
            } else {
                FrameAcknowledgement::UnknownFrame
            };
        }
        guard.frame_destinations.remove(&key);
        if let Some(mailbox) = guard.mailboxes.get_mut(destination_node_id) {
            if let Some(position) = mailbox
                .iter()
                .position(|frame| frame.frame_id.trim() == frame_id)
            {
                mailbox.remove(position);
            }
        }
        remember_acknowledgement(&mut guard, key);
        FrameAcknowledgement::Accepted
    }

    #[must_use]
    pub fn mailbox_depth(&self, node_id: &str) -> usize {
        self.lock_peers()
            .mailboxes
            .get(node_id)
            .map_or(0, VecDeque::len)
    }

    /// Record the immutable envelope associated with a relay-issued task frame identity.
    pub fn classify_published_task(
        &self,
        destination_node_id: &str,
        frame_id: &str,
        task: &TaskEnvelope,
    ) -> PublishedTaskIdentity {
        let key = (destination_node_id.to_string(), frame_id.to_string());
        let mut guard = self.lock_peers();
        if let Some(existing) = guard.published_task_envelopes.get(&key) {
            return if existing == task {
                PublishedTaskIdentity::Retry
            } else {
                PublishedTaskIdentity::Conflict
            };
        }
        if guard.published_task_envelopes.len() >= MAX_TRACKED_FRAMES {
            return PublishedTaskIdentity::RejectedCapacity;
        }
        guard.published_task_envelopes.insert(key, task.clone());
        PublishedTaskIdentity::New
    }

    pub fn forget_published_task(&self, destination_node_id: &str, frame_id: &str) {
        self.lock_peers()
            .published_task_envelopes
            .remove(&(destination_node_id.to_string(), frame_id.to_string()));
    }

    /// Return the stable relay-acceptance time for an active or recently acknowledged frame.
    pub fn frame_accepted_at_ms(
        &self,
        destination_node_id: &str,
        frame_id: &str,
        candidate_ms: i64,
    ) -> Option<i64> {
        let mut guard = self.lock_peers();
        let key = (destination_node_id.to_string(), frame_id.to_string());
        if !guard.frame_destinations.contains(&key) && !guard.acknowledged_frames.contains(&key) {
            return None;
        }
        Some(
            *guard
                .frame_accepted_at_ms
                .entry(key)
                .or_insert(candidate_ms),
        )
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

fn remember_acknowledgement(state: &mut PeerState, key: (String, String)) {
    if state.acknowledged_frames.insert(key.clone()) {
        state.acknowledged_frame_order.push_back(key);
    }
    while state.acknowledged_frame_order.len() > MAX_RECENT_ACKNOWLEDGEMENTS {
        if let Some(expired) = state.acknowledged_frame_order.pop_front() {
            state.acknowledged_frames.remove(&expired);
            state.frame_accepted_at_ms.remove(&expired);
            state.published_task_envelopes.remove(&expired);
        }
    }
}

fn is_acked(
    destination_node_id: &str,
    frame: &RelayFrame,
    acknowledged: &HashSet<(String, String)>,
) -> bool {
    let frame_id = frame.frame_id.trim();
    !frame_id.is_empty()
        && acknowledged.contains(&(destination_node_id.to_string(), frame_id.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn frame(frame_id: impl Into<String>) -> RelayFrame {
        RelayFrame {
            frame_id: frame_id.into(),
            task: None,
            result: None,
            authenticated_source_node_id: "source".to_string(),
            destination_node_id: "destination".to_string(),
        }
    }

    #[test]
    fn duplicate_frame_identity_is_rejected_without_replacing_mailbox_entry() {
        let runtime = RelayRuntime::new("relay");
        assert_eq!(
            runtime.route_frame("destination", frame("duplicate")),
            FrameDelivery::Mailboxed
        );
        assert_eq!(
            runtime.route_frame("destination", frame("duplicate")),
            FrameDelivery::RejectedDuplicate
        );
        assert_eq!(runtime.mailbox_depth("destination"), 1);
    }

    #[test]
    fn frame_acknowledgement_retention_is_bounded_and_recent_duplicates_are_stable() {
        let runtime = RelayRuntime::new("relay");
        for index in 0..=MAX_RECENT_ACKNOWLEDGEMENTS {
            let frame_id = format!("frame-{index}");
            assert_eq!(
                runtime.route_frame("destination", frame(&frame_id)),
                FrameDelivery::Mailboxed
            );
            assert_eq!(
                runtime.ack_frame("destination", &frame_id),
                FrameAcknowledgement::Accepted
            );
        }
        assert_eq!(
            runtime.ack_frame("destination", "frame-0"),
            FrameAcknowledgement::UnknownFrame
        );
        assert_eq!(
            runtime.ack_frame(
                "destination",
                &format!("frame-{MAX_RECENT_ACKNOWLEDGEMENTS}")
            ),
            FrameAcknowledgement::Accepted
        );
        let guard = runtime.lock_peers();
        assert_eq!(guard.acknowledged_frames.len(), MAX_RECENT_ACKNOWLEDGEMENTS);
        assert_eq!(
            guard.acknowledged_frame_order.len(),
            MAX_RECENT_ACKNOWLEDGEMENTS
        );
    }

    #[test]
    fn unacknowledged_frame_ownership_is_bounded() {
        let runtime = RelayRuntime::new("relay");
        for index in 0..MAX_TRACKED_FRAMES {
            assert_eq!(
                runtime.route_frame("destination", frame(format!("frame-{index}"))),
                FrameDelivery::Mailboxed
            );
        }
        assert_eq!(
            runtime.route_frame("destination", frame("over-capacity")),
            FrameDelivery::RejectedCapacity
        );
        assert_eq!(runtime.mailbox_depth("destination"), MAX_TRACKED_FRAMES);
    }
}
