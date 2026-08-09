//! Shared relay process state for health, telemetry, and relay delivery.

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use keryx_observe::RelayMetrics;
use keryx_proto::v1::{
    NodescaleIdentityBindResult, NodescaleIdentityChallengeResult, RelayFrame, TaskEnvelope,
};
use tokio::sync::{mpsc, oneshot};
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
    /// The frame lacks the non-empty relay identity required for bounded ownership.
    RejectedInvalid,
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

/// Result of settling a typed direct-control frame.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DirectControlCompletion {
    Accepted,
    WrongDestination,
    UnknownFrame,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PublishedTaskReceipt {
    pub frame_id: String,
    pub accepted_at_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PublishedTaskIdentity {
    New(PublishedTaskReceipt),
    Retry(PublishedTaskReceipt),
    Conflict,
    RejectedCapacity,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PublishedTaskDelivery {
    New {
        receipt: PublishedTaskReceipt,
        delivery: FrameDelivery,
    },
    Retry(PublishedTaskReceipt),
    Conflict,
    RejectedCapacity,
    RejectedInvalid,
}

type FrameKey = (String, String);
type TaskPublishKey = (String, String, String);

#[derive(Debug, Clone)]
struct PublishedTaskRecord {
    envelope: TaskEnvelope,
    receipt: PublishedTaskReceipt,
    acknowledged: bool,
}

/// Snapshot of a node identity tracked by the relay runtime.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PeerIdentity {
    pub node_id: String,
    pub connected: bool,
    pub mailbox_depth: usize,
}

#[derive(Debug)]
struct ConnectedNode {
    generation: u64,
    sender: RelayFrameSender,
}

#[derive(Debug, Default)]
struct PeerState {
    registered: HashSet<String>,
    connected_nodes: HashMap<String, ConnectedNode>,
    next_connection_generation: u64,
    libp2p_connected_peers: HashSet<String>,
    mailboxes: HashMap<String, VecDeque<RelayFrame>>,
    frame_destinations: HashSet<FrameKey>,
    acknowledged_frames: HashSet<FrameKey>,
    acknowledged_frame_order: VecDeque<FrameKey>,
    published_tasks: HashMap<TaskPublishKey, PublishedTaskRecord>,
    published_frame_index: HashMap<FrameKey, TaskPublishKey>,
    acknowledged_published_task_order: VecDeque<TaskPublishKey>,
    frame_ack_waiters: HashMap<FrameKey, oneshot::Sender<()>>,
    direct_control_waiters: HashMap<FrameKey, oneshot::Sender<NodescaleIdentityBindResult>>,
    challenge_control_waiters: HashMap<FrameKey, oneshot::Sender<NodescaleIdentityChallengeResult>>,
}

pub const MAX_TRACKED_FRAMES: usize = 8_192;
const MAX_RECENT_ACKNOWLEDGEMENTS: usize = 8_192;
const MAX_RETAINED_PUBLISHED_TASKS: usize = 8_192;

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
    pub fn connect_node(&self, node_id: impl Into<String>, sender: RelayFrameSender) -> usize {
        self.connect_node_fenced(node_id, sender).0
    }

    /// Attach a node stream and return its replay count plus an opaque connection generation.
    pub fn connect_node_fenced(
        &self,
        node_id: impl Into<String>,
        sender: RelayFrameSender,
    ) -> (usize, u64) {
        let node_id = node_id.into();
        let mut guard = self.lock_peers();
        guard.next_connection_generation = guard.next_connection_generation.wrapping_add(1).max(1);
        let generation = guard.next_connection_generation;
        guard.registered.insert(node_id.clone());
        let pending = guard
            .mailboxes
            .get(&node_id)
            .into_iter()
            .flat_map(|mailbox| mailbox.iter())
            .filter(|frame| !is_acked(&node_id, frame, &guard.acknowledged_frames))
            .cloned()
            .collect::<Vec<_>>();
        let mut stream_open = true;
        let mut replayed = 0;
        for frame in &pending {
            match sender.try_send(Ok(frame.clone())) {
                Ok(()) => replayed += 1,
                Err(mpsc::error::TrySendError::Full(_)) => {
                    stream_open = false;
                    break;
                }
                Err(mpsc::error::TrySendError::Closed(_)) => {
                    stream_open = false;
                    break;
                }
            }
        }
        if stream_open {
            guard
                .connected_nodes
                .insert(node_id.clone(), ConnectedNode { generation, sender });
        }
        self.sync_connected_peer_metric(&guard);
        (replayed, generation)
    }

    /// Mark a node stream disconnected. A reconnect with the same node id replaces this state.
    pub fn disconnect_node(&self, node_id: &str) {
        let mut guard = self.lock_peers();
        guard.connected_nodes.remove(node_id);
        self.sync_connected_peer_metric(&guard);
    }

    /// Disconnect only when the caller still owns the current stream generation.
    pub fn disconnect_node_if_current(&self, node_id: &str, generation: u64) {
        let mut guard = self.lock_peers();
        if guard
            .connected_nodes
            .get(node_id)
            .is_some_and(|connected| connected.generation == generation)
        {
            guard.connected_nodes.remove(node_id);
        }
        self.sync_connected_peer_metric(&guard);
    }

    /// Disconnect a node stream and best-effort deliver a terminal stream status first.
    pub fn disconnect_node_with_status(&self, node_id: &str, status: Status) {
        let connected = {
            let mut guard = self.lock_peers();
            let connected = guard.connected_nodes.remove(node_id);
            self.sync_connected_peer_metric(&guard);
            connected
        };
        if let Some(connected) = connected {
            let _ = connected.sender.try_send(Err(status));
        }
    }

    /// Disconnect the current generation with a terminal stream status.
    pub fn disconnect_node_with_status_if_current(
        &self,
        node_id: &str,
        generation: u64,
        status: Status,
    ) {
        let connected = {
            let mut guard = self.lock_peers();
            let connected = match guard.connected_nodes.get(node_id) {
                Some(connected) if connected.generation == generation => {
                    guard.connected_nodes.remove(node_id)
                }
                _ => None,
            };
            self.sync_connected_peer_metric(&guard);
            connected
        };
        if let Some(connected) = connected {
            let _ = connected.sender.try_send(Err(status));
        }
    }

    /// Route a frame to a target node, storing it in the offline mailbox when needed.
    pub fn route_frame(
        &self,
        target_node_id: impl Into<String>,
        frame: RelayFrame,
    ) -> FrameDelivery {
        if !has_exactly_one_generic_payload(&frame) {
            return FrameDelivery::RejectedInvalid;
        }
        let target_node_id = target_node_id.into();
        let mut guard = self.lock_peers();
        let delivery = route_frame_locked(&mut guard, target_node_id, frame);
        self.sync_connected_peer_metric(&guard);
        if matches!(
            delivery,
            FrameDelivery::Delivered | FrameDelivery::Mailboxed
        ) {
            self.metrics.increment_tasks_routed();
        }
        delivery
    }

    /// Route a result frame and return a waiter that resolves only after the authenticated
    /// destination acknowledges the exact relay-issued frame.
    pub fn route_frame_waiting_for_ack(
        &self,
        target_node_id: impl Into<String>,
        frame: RelayFrame,
    ) -> (FrameDelivery, Option<oneshot::Receiver<()>>) {
        if !has_exactly_one_generic_payload(&frame) {
            return (FrameDelivery::RejectedInvalid, None);
        }
        let target_node_id = target_node_id.into();
        let frame_id = frame.frame_id.trim().to_string();
        let mut guard = self.lock_peers();
        // Keep the peer-state lock across both live enqueue and waiter insertion. A destination
        // may receive the frame immediately, but its AckFrame cannot acquire this lock until the
        // waiter is installed, so an early authenticated ACK cannot be lost.
        let delivery = route_frame_locked(&mut guard, target_node_id.clone(), frame);
        let receiver = if matches!(
            delivery,
            FrameDelivery::Delivered | FrameDelivery::Mailboxed
        ) {
            let (sender, receiver) = oneshot::channel();
            guard
                .frame_ack_waiters
                .insert((target_node_id, frame_id), sender);
            Some(receiver)
        } else {
            None
        };
        self.sync_connected_peer_metric(&guard);
        if receiver.is_some() {
            self.metrics.increment_tasks_routed();
        }
        (delivery, receiver)
    }

    /// Atomically route a typed direct-control frame and install its destination-only result waiter.
    pub(crate) fn route_nodescale_identity_bind_waiting_for_completion(
        &self,
        target_node_id: impl Into<String>,
        frame: RelayFrame,
    ) -> (
        FrameDelivery,
        Option<oneshot::Receiver<NodescaleIdentityBindResult>>,
    ) {
        let target_node_id = target_node_id.into();
        if !typed_direct_control_target_matches_frame_destination(&target_node_id, &frame) {
            return (FrameDelivery::RejectedInvalid, None);
        }
        let frame_id = frame.frame_id.trim().to_string();
        if frame.nodescale_identity_bind_v1.is_none()
            || frame.nodescale_identity_challenge_v1.is_some()
            || frame.task.is_some()
            || frame.result.is_some()
        {
            return (FrameDelivery::RejectedInvalid, None);
        }
        let mut guard = self.lock_peers();
        let delivery = route_frame_locked(&mut guard, target_node_id.clone(), frame);
        let receiver = if matches!(
            delivery,
            FrameDelivery::Delivered | FrameDelivery::Mailboxed
        ) {
            let (sender, receiver) = oneshot::channel();
            guard
                .direct_control_waiters
                .insert((target_node_id, frame_id), sender);
            Some(receiver)
        } else {
            None
        };
        self.sync_connected_peer_metric(&guard);
        (delivery, receiver)
    }

    /// Complete a typed control frame exactly once. Only the exact authenticated destination can
    /// settle the publisher's waiter; duplicate/unknown completion is deliberately not idempotent.
    pub(crate) fn complete_nodescale_identity_bind(
        &self,
        destination_node_id: &str,
        frame_id: &str,
        result: NodescaleIdentityBindResult,
    ) -> DirectControlCompletion {
        let destination_node_id = destination_node_id.trim();
        let frame_id = frame_id.trim();
        if destination_node_id.is_empty() || frame_id.is_empty() {
            return DirectControlCompletion::UnknownFrame;
        }
        let key = (destination_node_id.to_string(), frame_id.to_string());
        let mut guard = self.lock_peers();
        let Some(waiter) = guard.direct_control_waiters.remove(&key) else {
            return if guard
                .direct_control_waiters
                .keys()
                .any(|(_, known_frame_id)| known_frame_id == frame_id)
            {
                DirectControlCompletion::WrongDestination
            } else {
                DirectControlCompletion::UnknownFrame
            };
        };
        if !guard.frame_destinations.remove(&key) {
            return DirectControlCompletion::UnknownFrame;
        }
        if let Some(mailbox) = guard.mailboxes.get_mut(destination_node_id) {
            mailbox.retain(|frame| frame.frame_id.trim() != frame_id);
        }
        let _ = waiter.send(result);
        DirectControlCompletion::Accepted
    }

    /// Atomically route a typed challenge-control frame and install its destination-only result waiter.
    pub(crate) fn route_nodescale_identity_challenge_waiting_for_completion(
        &self,
        target_node_id: impl Into<String>,
        frame: RelayFrame,
    ) -> (
        FrameDelivery,
        Option<oneshot::Receiver<NodescaleIdentityChallengeResult>>,
    ) {
        let target_node_id = target_node_id.into();
        if !typed_direct_control_target_matches_frame_destination(&target_node_id, &frame) {
            return (FrameDelivery::RejectedInvalid, None);
        }
        let frame_id = frame.frame_id.trim().to_string();
        if frame.nodescale_identity_challenge_v1.is_none()
            || frame.nodescale_identity_bind_v1.is_some()
            || frame.task.is_some()
            || frame.result.is_some()
        {
            return (FrameDelivery::RejectedInvalid, None);
        }
        let mut guard = self.lock_peers();
        let delivery = route_frame_locked(&mut guard, target_node_id.clone(), frame);
        let receiver = if matches!(
            delivery,
            FrameDelivery::Delivered | FrameDelivery::Mailboxed
        ) {
            let (sender, receiver) = oneshot::channel();
            guard
                .challenge_control_waiters
                .insert((target_node_id, frame_id), sender);
            Some(receiver)
        } else {
            None
        };
        self.sync_connected_peer_metric(&guard);
        (delivery, receiver)
    }

    /// Complete a typed challenge control frame exactly once. Only the exact authenticated
    /// destination can settle the publisher's waiter; duplicate/unknown completion is not idempotent.
    pub(crate) fn complete_nodescale_identity_challenge(
        &self,
        destination_node_id: &str,
        frame_id: &str,
        result: NodescaleIdentityChallengeResult,
    ) -> DirectControlCompletion {
        let destination_node_id = destination_node_id.trim();
        let frame_id = frame_id.trim();
        if destination_node_id.is_empty() || frame_id.is_empty() {
            return DirectControlCompletion::UnknownFrame;
        }
        let key = (destination_node_id.to_string(), frame_id.to_string());
        let mut guard = self.lock_peers();
        let Some(waiter) = guard.challenge_control_waiters.remove(&key) else {
            return if guard
                .challenge_control_waiters
                .keys()
                .any(|(_, known_frame_id)| known_frame_id == frame_id)
            {
                DirectControlCompletion::WrongDestination
            } else {
                DirectControlCompletion::UnknownFrame
            };
        };
        if !guard.frame_destinations.remove(&key) {
            return DirectControlCompletion::UnknownFrame;
        }
        if let Some(mailbox) = guard.mailboxes.get_mut(destination_node_id) {
            mailbox.retain(|frame| frame.frame_id.trim() != frame_id);
        }
        let _ = waiter.send(result);
        DirectControlCompletion::Accepted
    }

    /// Abandon a timed-out result frame generation while leaving any already-delivered copy
    /// harmlessly idempotent at the destination.
    pub fn abandon_frame(&self, destination_node_id: &str, frame_id: &str) {
        let key = (destination_node_id.to_string(), frame_id.to_string());
        let mut guard = self.lock_peers();
        guard.frame_ack_waiters.remove(&key);
        guard.direct_control_waiters.remove(&key);
        guard.challenge_control_waiters.remove(&key);
        guard.frame_destinations.remove(&key);
        if let Some(mailbox) = guard.mailboxes.get_mut(destination_node_id) {
            mailbox.retain(|frame| frame.frame_id.trim() != frame_id);
        }
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
        // Typed direct-control frames have a destination-only semantic completion channel.
        // Generic ACKs must never consume their ownership or settle their waiters.
        if guard.direct_control_waiters.contains_key(&key)
            || guard.challenge_control_waiters.contains_key(&key)
        {
            return FrameAcknowledgement::UnknownFrame;
        }
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
        if let Some(waiter) = guard
            .frame_ack_waiters
            .remove(&(destination_node_id.to_string(), frame_id.to_string()))
        {
            let _ = waiter.send(());
        }
        mark_published_task_acknowledged(&mut guard, destination_node_id, frame_id);
        FrameAcknowledgement::Accepted
    }

    #[must_use]
    pub fn mailbox_depth(&self, node_id: &str) -> usize {
        self.lock_peers()
            .mailboxes
            .get(node_id)
            .map_or(0, VecDeque::len)
    }

    #[cfg(test)]
    pub(crate) fn test_pending_frame_counts(&self, node_id: &str) -> (usize, usize, usize) {
        let guard = self.lock_peers();
        (
            guard.frame_ack_waiters.len(),
            guard.frame_destinations.len(),
            guard.mailboxes.get(node_id).map_or(0, VecDeque::len),
        )
    }

    #[cfg(test)]
    pub(crate) fn test_pending_direct_control_state(&self, node_id: &str) -> (usize, usize, usize) {
        let guard = self.lock_peers();
        (
            guard
                .direct_control_waiters
                .keys()
                .filter(|(destination_node_id, _)| destination_node_id == node_id)
                .count(),
            guard
                .frame_destinations
                .iter()
                .filter(|(destination_node_id, _)| destination_node_id == node_id)
                .count(),
            guard.mailboxes.get(node_id).map_or(0, VecDeque::len),
        )
    }

    #[cfg(test)]
    pub(crate) fn test_pending_direct_control_frame_ids(&self, node_id: &str) -> Vec<String> {
        let guard = self.lock_peers();
        let mut frame_ids = guard
            .direct_control_waiters
            .keys()
            .filter(|(destination_node_id, _)| destination_node_id == node_id)
            .map(|(_, frame_id)| frame_id.clone())
            .collect::<Vec<_>>();
        frame_ids.sort();
        frame_ids
    }

    #[cfg(test)]
    pub(crate) fn test_pending_result_frame_ids(&self, node_id: &str) -> Vec<String> {
        let guard = self.lock_peers();
        let mut frame_ids = guard
            .frame_ack_waiters
            .keys()
            .filter(|(destination_node_id, _)| destination_node_id == node_id)
            .map(|(_, frame_id)| frame_id.clone())
            .collect::<Vec<_>>();
        frame_ids.sort();
        frame_ids
    }

    #[cfg(test)]
    pub(crate) fn test_drop_frame_ack_waiter(&self, node_id: &str, frame_id: &str) -> bool {
        self.lock_peers()
            .frame_ack_waiters
            .remove(&(node_id.to_string(), frame_id.to_string()))
            .is_some()
    }

    #[cfg(test)]
    pub(crate) fn test_exact_frame_state(
        &self,
        node_id: &str,
        frame_id: &str,
    ) -> (bool, bool, bool) {
        let guard = self.lock_peers();
        let key = (node_id.to_string(), frame_id.to_string());
        (
            guard.frame_ack_waiters.contains_key(&key),
            guard.frame_destinations.contains(&key),
            guard
                .mailboxes
                .get(node_id)
                .is_some_and(|mailbox| mailbox.iter().any(|frame| frame.frame_id == frame_id)),
        )
    }

    /// Atomically admit a stable caller task identity and acquire bounded frame ownership.
    ///
    /// `identity_task` excludes relay-projected metadata and remains stable across retries.
    /// `routed_task` is the exact task payload carried by `frame` for this delivery attempt.
    pub fn publish_task_frame(
        &self,
        source_node_id: &str,
        destination_node_id: &str,
        task_id: &str,
        tasks: (&TaskEnvelope, &TaskEnvelope),
        frame: RelayFrame,
        proposed_receipt: PublishedTaskReceipt,
    ) -> PublishedTaskDelivery {
        let (identity_task, routed_task) = tasks;
        if !is_exact_task_frame(&frame, routed_task) {
            return PublishedTaskDelivery::RejectedInvalid;
        }
        let task_key = (
            source_node_id.to_string(),
            destination_node_id.to_string(),
            task_id.to_string(),
        );
        let mut guard = self.lock_peers();
        if let Some(existing) = guard.published_tasks.get(&task_key) {
            return if existing.envelope == *identity_task {
                PublishedTaskDelivery::Retry(existing.receipt.clone())
            } else {
                PublishedTaskDelivery::Conflict
            };
        }

        let frame_key = (
            destination_node_id.to_string(),
            proposed_receipt.frame_id.clone(),
        );
        if guard.frame_destinations.contains(&frame_key)
            || guard.acknowledged_frames.contains(&frame_key)
            || guard.frame_destinations.len() >= MAX_TRACKED_FRAMES
            || proposed_receipt.frame_id.trim().is_empty()
            || guard
                .mailboxes
                .get(destination_node_id)
                .is_some_and(|mailbox| mailbox.len() >= MAX_TRACKED_FRAMES)
        {
            return PublishedTaskDelivery::RejectedCapacity;
        }
        while guard.published_tasks.len() >= MAX_RETAINED_PUBLISHED_TASKS {
            let Some(expired_key) = guard.acknowledged_published_task_order.pop_front() else {
                return PublishedTaskDelivery::RejectedCapacity;
            };
            if let Some(expired) = guard.published_tasks.remove(&expired_key) {
                guard
                    .published_frame_index
                    .remove(&(expired_key.1.clone(), expired.receipt.frame_id));
            }
        }
        guard
            .published_frame_index
            .insert(frame_key, task_key.clone());
        guard.published_tasks.insert(
            task_key,
            PublishedTaskRecord {
                envelope: identity_task.clone(),
                receipt: proposed_receipt.clone(),
                acknowledged: false,
            },
        );
        let delivery = route_frame_locked(&mut guard, destination_node_id.to_string(), frame);
        debug_assert!(matches!(
            delivery,
            FrameDelivery::Delivered | FrameDelivery::Mailboxed
        ));
        self.sync_connected_peer_metric(&guard);
        self.metrics.increment_tasks_routed();
        PublishedTaskDelivery::New {
            receipt: proposed_receipt,
            delivery,
        }
    }

    /// Retain an immutable envelope and relay-issued receipt for a bounded task identity history.
    pub fn classify_published_task(
        &self,
        source_node_id: &str,
        destination_node_id: &str,
        task_id: &str,
        task: &TaskEnvelope,
        proposed_receipt: PublishedTaskReceipt,
    ) -> PublishedTaskIdentity {
        let task_key = (
            source_node_id.to_string(),
            destination_node_id.to_string(),
            task_id.to_string(),
        );
        let mut guard = self.lock_peers();
        if let Some(existing) = guard.published_tasks.get(&task_key) {
            return if existing.envelope == *task {
                PublishedTaskIdentity::Retry(existing.receipt.clone())
            } else {
                PublishedTaskIdentity::Conflict
            };
        }

        while guard.published_tasks.len() >= MAX_RETAINED_PUBLISHED_TASKS {
            let Some(expired_key) = guard.acknowledged_published_task_order.pop_front() else {
                return PublishedTaskIdentity::RejectedCapacity;
            };
            if let Some(expired) = guard.published_tasks.remove(&expired_key) {
                guard
                    .published_frame_index
                    .remove(&(expired_key.1.clone(), expired.receipt.frame_id));
            }
        }

        guard.published_frame_index.insert(
            (
                destination_node_id.to_string(),
                proposed_receipt.frame_id.clone(),
            ),
            task_key.clone(),
        );
        guard.published_tasks.insert(
            task_key,
            PublishedTaskRecord {
                envelope: task.clone(),
                receipt: proposed_receipt.clone(),
                acknowledged: false,
            },
        );
        PublishedTaskIdentity::New(proposed_receipt)
    }

    /// Roll back only a just-admitted publication that never acquired active frame ownership.
    pub fn forget_published_task(
        &self,
        source_node_id: &str,
        destination_node_id: &str,
        task_id: &str,
        frame_id: &str,
    ) {
        let task_key = (
            source_node_id.to_string(),
            destination_node_id.to_string(),
            task_id.to_string(),
        );
        let frame_key = (destination_node_id.to_string(), frame_id.to_string());
        let mut guard = self.lock_peers();
        let removable = guard.published_tasks.get(&task_key).is_some_and(|record| {
            record.receipt.frame_id == frame_id
                && !record.acknowledged
                && !guard.frame_destinations.contains(&frame_key)
        });
        if removable {
            guard.published_tasks.remove(&task_key);
            guard.published_frame_index.remove(&frame_key);
        }
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

fn remember_acknowledgement(state: &mut PeerState, key: FrameKey) {
    if state.acknowledged_frames.insert(key.clone()) {
        state.acknowledged_frame_order.push_back(key);
    }
    while state.acknowledged_frame_order.len() > MAX_RECENT_ACKNOWLEDGEMENTS {
        if let Some(expired) = state.acknowledged_frame_order.pop_front() {
            state.acknowledged_frames.remove(&expired);
        }
    }
}

fn mark_published_task_acknowledged(
    state: &mut PeerState,
    destination_node_id: &str,
    frame_id: &str,
) {
    let frame_key = (destination_node_id.to_string(), frame_id.to_string());
    let Some(task_key) = state.published_frame_index.get(&frame_key).cloned() else {
        return;
    };
    let Some(record) = state.published_tasks.get_mut(&task_key) else {
        return;
    };
    if !record.acknowledged {
        record.acknowledged = true;
        state.acknowledged_published_task_order.push_back(task_key);
    }
}

fn route_frame_locked(
    state: &mut PeerState,
    target_node_id: String,
    frame: RelayFrame,
) -> FrameDelivery {
    if !has_exactly_one_relay_payload(&frame) {
        return FrameDelivery::RejectedInvalid;
    }
    let frame_id = frame.frame_id.trim();
    if frame_id.is_empty() {
        return FrameDelivery::RejectedInvalid;
    }
    let key = (target_node_id.clone(), frame_id.to_string());
    if state.frame_destinations.contains(&key) || state.acknowledged_frames.contains(&key) {
        return FrameDelivery::RejectedDuplicate;
    }
    if state.frame_destinations.len() >= MAX_TRACKED_FRAMES
        || state
            .mailboxes
            .get(&target_node_id)
            .is_some_and(|mailbox| mailbox.len() >= MAX_TRACKED_FRAMES)
    {
        return FrameDelivery::RejectedCapacity;
    }
    state.frame_destinations.insert(key);
    state.registered.insert(target_node_id.clone());
    state
        .mailboxes
        .entry(target_node_id.clone())
        .or_default()
        .push_back(frame.clone());
    if let Some(sender) = state
        .connected_nodes
        .get(&target_node_id)
        .map(|connected| connected.sender.clone())
    {
        match sender.try_send(Ok(frame.clone())) {
            Ok(()) => return FrameDelivery::Delivered,
            Err(tokio::sync::mpsc::error::TrySendError::Full(Ok(_))) => {
                return FrameDelivery::Mailboxed;
            }
            Err(tokio::sync::mpsc::error::TrySendError::Closed(Ok(_))) => {
                state.connected_nodes.remove(&target_node_id);
                return FrameDelivery::Mailboxed;
            }
            Err(tokio::sync::mpsc::error::TrySendError::Full(Err(_))) => {
                return FrameDelivery::Mailboxed;
            }
            Err(tokio::sync::mpsc::error::TrySendError::Closed(Err(_))) => {
                state.connected_nodes.remove(&target_node_id);
                return FrameDelivery::Mailboxed;
            }
        }
    }
    FrameDelivery::Mailboxed
}

fn has_exactly_one_relay_payload(frame: &RelayFrame) -> bool {
    let payload_count = usize::from(frame.task.is_some())
        + usize::from(frame.result.is_some())
        + usize::from(frame.nodescale_identity_bind_v1.is_some())
        + usize::from(frame.nodescale_identity_challenge_v1.is_some());
    payload_count == 1
}

fn has_exactly_one_generic_payload(frame: &RelayFrame) -> bool {
    has_exactly_one_relay_payload(frame) && (frame.task.is_some() || frame.result.is_some())
}

fn is_exact_task_frame(frame: &RelayFrame, task: &TaskEnvelope) -> bool {
    has_exactly_one_relay_payload(frame) && frame.task.as_ref() == Some(task)
}

fn typed_direct_control_target_matches_frame_destination(
    target_node_id: &str,
    frame: &RelayFrame,
) -> bool {
    !target_node_id.trim().is_empty()
        && (frame.destination_node_id.trim().is_empty()
            || target_node_id == frame.destination_node_id)
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
    use keryx_proto::v1::TaskId;

    fn frame(frame_id: impl Into<String>) -> RelayFrame {
        RelayFrame {
            frame_id: frame_id.into(),
            task: Some(TaskEnvelope::default()),
            result: None,
            authenticated_source_node_id: "source".to_string(),
            destination_node_id: "destination".to_string(),
            nodescale_identity_bind_v1: None,
            nodescale_identity_challenge_v1: None,
        }
    }

    fn direct_control_frame(frame_id: impl Into<String>) -> RelayFrame {
        RelayFrame {
            frame_id: frame_id.into(),
            task: None,
            result: None,
            authenticated_source_node_id: "source".to_string(),
            destination_node_id: "destination".to_string(),
            nodescale_identity_bind_v1: Some(keryx_proto::v1::NodescaleIdentityBindV1 {
                operation_id: "operation".to_string(),
                network_id: "network".to_string(),
                device_id: "device".to_string(),
                join_session_id: "session".to_string(),
                binding_nonce: "nonce".to_string(),
                binding_generation: 1,
                agent_version: "v1".to_string(),
            }),
            nodescale_identity_challenge_v1: None,
        }
    }

    fn challenge_control_frame(frame_id: impl Into<String>) -> RelayFrame {
        RelayFrame {
            frame_id: frame_id.into(),
            task: None,
            result: None,
            authenticated_source_node_id: "source".to_string(),
            destination_node_id: "destination".to_string(),
            nodescale_identity_bind_v1: None,
            nodescale_identity_challenge_v1: Some(keryx_proto::v1::NodescaleIdentityChallengeV1 {
                operation_id: "challenge-operation".to_string(),
                network_id: "network".to_string(),
                device_id: "device".to_string(),
                join_session_id: "session".to_string(),
                agent_version: "v1".to_string(),
            }),
        }
    }

    fn challenge_result() -> NodescaleIdentityChallengeResult {
        NodescaleIdentityChallengeResult {
            disposition: keryx_proto::v1::NodescaleIdentityChallengeDisposition::Issued as i32,
            accepted: true,
            challenge_id: "challenge".to_string(),
            challenge_secret: "delivery-only-test-secret".to_string(),
            binding_generation: 1,
            expires_at_unix_ms: 1,
            reason: String::new(),
            code: String::new(),
        }
    }

    #[test]
    fn generic_route_rejects_typed_direct_controls_without_acquiring_state_or_metrics() {
        let runtime = RelayRuntime::new("relay");
        let before = runtime.metrics().snapshot().tasks_routed;

        let mut bind_with_task = direct_control_frame("bind-with-task");
        bind_with_task.task = Some(task("mixed-bind-task"));
        let mut bind_with_result = direct_control_frame("bind-with-result");
        bind_with_result.result = Some(keryx_proto::v1::TaskResultEnvelope::default());
        let mut challenge_with_task = challenge_control_frame("challenge-with-task");
        challenge_with_task.task = Some(task("mixed-challenge-task"));
        let mut challenge_with_result = challenge_control_frame("challenge-with-result");
        challenge_with_result.result = Some(keryx_proto::v1::TaskResultEnvelope::default());
        let mut bind_with_challenge = direct_control_frame("bind-with-challenge");
        bind_with_challenge.nodescale_identity_challenge_v1 =
            challenge_control_frame("unused").nodescale_identity_challenge_v1;

        for frame in [
            direct_control_frame("bind-only"),
            challenge_control_frame("challenge-only"),
            bind_with_task,
            bind_with_result,
            challenge_with_task,
            challenge_with_result,
            bind_with_challenge,
        ] {
            assert_eq!(
                runtime.route_frame("destination", frame),
                FrameDelivery::RejectedInvalid
            );
            assert_eq!(runtime.mailbox_depth("destination"), 0);
            assert_eq!(runtime.metrics().snapshot().tasks_routed, before);
            let guard = runtime.lock_peers();
            assert!(guard.frame_destinations.is_empty());
            assert!(guard.frame_ack_waiters.is_empty());
            assert!(guard.direct_control_waiters.is_empty());
            assert!(guard.challenge_control_waiters.is_empty());
        }
    }

    #[test]
    fn generic_ack_waiter_route_rejects_typed_direct_controls_without_acquiring_state() {
        let runtime = RelayRuntime::new("relay");
        let before = runtime.metrics().snapshot().tasks_routed;

        for frame in [
            direct_control_frame("bind-generic-ack-waiter"),
            challenge_control_frame("challenge-generic-ack-waiter"),
        ] {
            let (delivery, acknowledgement) =
                runtime.route_frame_waiting_for_ack("destination", frame);
            assert_eq!(delivery, FrameDelivery::RejectedInvalid);
            assert!(acknowledgement.is_none());
            assert_eq!(runtime.mailbox_depth("destination"), 0);
            assert_eq!(runtime.metrics().snapshot().tasks_routed, before);
            let guard = runtime.lock_peers();
            assert!(guard.frame_destinations.is_empty());
            assert!(guard.frame_ack_waiters.is_empty());
            assert!(guard.direct_control_waiters.is_empty());
            assert!(guard.challenge_control_waiters.is_empty());
        }
    }

    #[test]
    fn generic_routes_reject_zero_and_multi_payload_frames_before_mailbox_waiters_or_metrics() {
        let runtime = RelayRuntime::new("relay");
        let before = runtime.metrics().snapshot().tasks_routed;

        let mut task_and_result = frame("task-and-result");
        task_and_result.result = Some(keryx_proto::v1::TaskResultEnvelope::default());
        let zero_payload = RelayFrame {
            frame_id: "zero-payload".to_string(),
            task: None,
            result: None,
            authenticated_source_node_id: "source".to_string(),
            destination_node_id: "destination".to_string(),
            nodescale_identity_bind_v1: None,
            nodescale_identity_challenge_v1: None,
        };

        for frame in [task_and_result, zero_payload] {
            assert_eq!(
                runtime.route_frame("destination", frame.clone()),
                FrameDelivery::RejectedInvalid
            );
            let (delivery, acknowledgement) =
                runtime.route_frame_waiting_for_ack("destination", frame);
            assert_eq!(delivery, FrameDelivery::RejectedInvalid);
            assert!(acknowledgement.is_none());
            assert_eq!(runtime.mailbox_depth("destination"), 0);
            assert_eq!(runtime.metrics().snapshot().tasks_routed, before);
            let guard = runtime.lock_peers();
            assert!(guard.frame_destinations.is_empty());
            assert!(guard.frame_ack_waiters.is_empty());
        }
    }

    #[test]
    fn task_publish_route_rejects_typed_direct_controls_without_acquiring_state() {
        let runtime = RelayRuntime::new("relay");
        let before = runtime.metrics().snapshot().tasks_routed;

        for (index, mut frame) in [
            direct_control_frame("published-bind-control"),
            challenge_control_frame("published-challenge-control"),
        ]
        .into_iter()
        .enumerate()
        {
            let task = task(format!("published-task-{index}"));
            frame.task = Some(task.clone());
            assert!(!matches!(
                runtime.publish_task_frame(
                    "source",
                    "destination",
                    task.task_id.as_ref().unwrap().value.as_str(),
                    (&task, &task),
                    frame,
                    PublishedTaskReceipt {
                        frame_id: format!("published-control-{index}"),
                        accepted_at_ms: index as i64,
                    },
                ),
                PublishedTaskDelivery::New { .. }
            ));
            assert_eq!(runtime.mailbox_depth("destination"), 0);
            assert_eq!(runtime.metrics().snapshot().tasks_routed, before);
            let guard = runtime.lock_peers();
            assert!(guard.published_tasks.is_empty());
            assert!(guard.published_frame_index.is_empty());
            assert!(guard.frame_destinations.is_empty());
            assert!(guard.direct_control_waiters.is_empty());
            assert!(guard.challenge_control_waiters.is_empty());
        }
    }

    #[test]
    fn task_publish_requires_one_unchanged_task_payload_before_ownership_or_metrics() {
        let runtime = RelayRuntime::new("relay");
        let before = runtime.metrics().snapshot().tasks_routed;
        let published_task = task("published-task");
        let mut task_and_result = frame("published-task-and-result");
        task_and_result.result = Some(keryx_proto::v1::TaskResultEnvelope::default());
        let result_only = RelayFrame {
            frame_id: "published-result-only".to_string(),
            task: None,
            result: Some(keryx_proto::v1::TaskResultEnvelope::default()),
            authenticated_source_node_id: "source".to_string(),
            destination_node_id: "destination".to_string(),
            nodescale_identity_bind_v1: None,
            nodescale_identity_challenge_v1: None,
        };
        let mismatched_envelope = RelayFrame {
            frame_id: "published-mismatched-task".to_string(),
            task: Some(task("mismatched-published-task")),
            result: None,
            authenticated_source_node_id: "source".to_string(),
            destination_node_id: "destination".to_string(),
            nodescale_identity_bind_v1: None,
            nodescale_identity_challenge_v1: None,
        };

        for (index, frame) in [task_and_result, result_only, mismatched_envelope]
            .into_iter()
            .enumerate()
        {
            assert_eq!(
                runtime.publish_task_frame(
                    "source",
                    "destination",
                    "published-task",
                    (&published_task, &published_task),
                    frame,
                    PublishedTaskReceipt {
                        frame_id: format!("rejected-published-frame-{index}"),
                        accepted_at_ms: index as i64,
                    },
                ),
                PublishedTaskDelivery::RejectedInvalid
            );
            assert_eq!(runtime.mailbox_depth("destination"), 0);
            assert_eq!(runtime.metrics().snapshot().tasks_routed, before);
            let guard = runtime.lock_peers();
            assert!(guard.published_tasks.is_empty());
            assert!(guard.published_frame_index.is_empty());
            assert!(guard.frame_destinations.is_empty());
        }
    }

    fn task(task_id: impl Into<String>) -> TaskEnvelope {
        TaskEnvelope {
            task_id: Some(TaskId {
                value: task_id.into(),
            }),
            ..TaskEnvelope::default()
        }
    }

    fn receipt(index: usize) -> PublishedTaskReceipt {
        PublishedTaskReceipt {
            frame_id: format!("task-frame-{index}"),
            accepted_at_ms: index as i64 + 1,
        }
    }

    #[test]
    fn task_retry_identity_is_stable_when_relay_projected_metadata_changes() {
        let runtime = RelayRuntime::new("relay");
        let identity_task = task("stable-task");
        let mut first_delivery = identity_task.clone();
        first_delivery.metadata.insert(
            "keryx.authenticated_source_protocol_features".to_string(),
            "[\"feature-a\"]".to_string(),
        );
        let mut second_delivery = first_delivery.clone();
        second_delivery.metadata.insert(
            "keryx.authenticated_source_protocol_features".to_string(),
            "[\"feature-b\"]".to_string(),
        );
        let original_receipt = PublishedTaskReceipt {
            frame_id: "stable-task-frame".to_string(),
            accepted_at_ms: 17,
        };

        let first = runtime.publish_task_frame(
            "source",
            "destination",
            "stable-task",
            (&identity_task, &first_delivery),
            RelayFrame {
                frame_id: original_receipt.frame_id.clone(),
                task: Some(first_delivery.clone()),
                result: None,
                authenticated_source_node_id: "source".to_string(),
                destination_node_id: "destination".to_string(),
                nodescale_identity_bind_v1: None,
                nodescale_identity_challenge_v1: None,
            },
            original_receipt.clone(),
        );
        assert!(matches!(first, PublishedTaskDelivery::New { .. }));

        let retry = runtime.publish_task_frame(
            "source",
            "destination",
            "stable-task",
            (&identity_task, &second_delivery),
            RelayFrame {
                frame_id: "ignored-retry-frame".to_string(),
                task: Some(second_delivery.clone()),
                result: None,
                authenticated_source_node_id: "source".to_string(),
                destination_node_id: "destination".to_string(),
                nodescale_identity_bind_v1: None,
                nodescale_identity_challenge_v1: None,
            },
            PublishedTaskReceipt {
                frame_id: "ignored-retry-frame".to_string(),
                accepted_at_ms: 18,
            },
        );
        assert_eq!(retry, PublishedTaskDelivery::Retry(original_receipt));
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

    #[tokio::test]
    async fn stale_stream_cleanup_cannot_disconnect_newer_generation() {
        let runtime = RelayRuntime::new("relay");
        let (old_sender, mut old_receiver) = mpsc::channel(1);
        let (_, old_generation) = runtime.connect_node_fenced("destination", old_sender);
        let (new_sender, mut new_receiver) = mpsc::channel(1);
        let (_, new_generation) = runtime.connect_node_fenced("destination", new_sender);
        assert_ne!(old_generation, new_generation);
        assert!(old_receiver.recv().await.is_none());

        runtime.disconnect_node_if_current("destination", old_generation);
        runtime.disconnect_node_with_status_if_current(
            "destination",
            old_generation,
            Status::unavailable("stale stream ended"),
        );
        assert!(runtime.peer_identity("destination").unwrap().connected);
        assert_eq!(
            runtime.route_frame("destination", frame("new-generation-frame")),
            FrameDelivery::Delivered
        );
        assert_eq!(
            new_receiver.recv().await.unwrap().unwrap().frame_id,
            "new-generation-frame"
        );
    }

    #[test]
    fn reconnect_snapshot_preserves_pending_mailbox_until_destination_acknowledges() {
        let runtime = RelayRuntime::new("relay");
        for index in 0..129 {
            assert_eq!(
                runtime.route_frame("destination", frame(format!("pending-{index}"))),
                FrameDelivery::Mailboxed
            );
        }
        let (sender, mut receiver) = mpsc::channel(MAX_TRACKED_FRAMES);
        let pending_count = runtime.connect_node("destination", sender);

        assert_eq!(pending_count, 129);
        for index in 0..129 {
            let delivered = receiver.try_recv().unwrap().unwrap();
            assert_eq!(delivered.frame_id, format!("pending-{index}"));
        }
        assert_eq!(runtime.mailbox_depth("destination"), 129);
        assert_eq!(
            runtime.ack_frame("destination", "pending-0"),
            FrameAcknowledgement::Accepted
        );
        assert_eq!(runtime.mailbox_depth("destination"), 128);
    }

    #[test]
    fn live_delivery_remains_replayable_until_ack_and_survives_backpressure() {
        let runtime = RelayRuntime::new("relay");
        let (sender, mut receiver) = mpsc::channel(MAX_TRACKED_FRAMES);
        assert_eq!(runtime.connect_node("destination", sender), 0);

        for index in 0..130 {
            assert_eq!(
                runtime.route_frame("destination", frame(format!("live-{index}"))),
                FrameDelivery::Delivered
            );
        }
        assert_eq!(runtime.mailbox_depth("destination"), 130);
        for index in 0..130 {
            let delivered = receiver.try_recv().unwrap().unwrap();
            assert_eq!(delivered.frame_id, format!("live-{index}"));
        }
        assert_eq!(
            runtime.route_frame("destination", frame("after-drain")),
            FrameDelivery::Delivered
        );
        assert_eq!(
            receiver.try_recv().unwrap().unwrap().frame_id,
            "after-drain"
        );

        runtime.disconnect_node("destination");
        let (reconnect_sender, mut reconnect_receiver) = mpsc::channel(MAX_TRACKED_FRAMES);
        assert_eq!(runtime.connect_node("destination", reconnect_sender), 131);
        assert_eq!(
            reconnect_receiver.try_recv().unwrap().unwrap().frame_id,
            "live-0"
        );
        assert_eq!(
            runtime.ack_frame("destination", "live-0"),
            FrameAcknowledgement::Accepted
        );
        assert_eq!(runtime.mailbox_depth("destination"), 130);
    }

    #[test]
    fn acknowledged_task_receipt_history_is_bounded_live_and_stale_ack_safe() {
        let runtime = RelayRuntime::new("relay");
        for index in 0..MAX_RETAINED_PUBLISHED_TASKS {
            let task = task(format!("task-{index}"));
            let receipt = receipt(index);
            assert_eq!(
                runtime.classify_published_task(
                    "source",
                    "destination",
                    task.task_id.as_ref().unwrap().value.as_str(),
                    &task,
                    receipt.clone(),
                ),
                PublishedTaskIdentity::New(receipt.clone())
            );
            assert_eq!(
                runtime.route_frame(
                    "destination",
                    RelayFrame {
                        frame_id: receipt.frame_id.clone(),
                        task: Some(task),
                        result: None,
                        authenticated_source_node_id: "source".to_string(),
                        destination_node_id: "destination".to_string(),
                        nodescale_identity_bind_v1: None,
                        nodescale_identity_challenge_v1: None,
                    },
                ),
                FrameDelivery::Mailboxed
            );
            assert_eq!(
                runtime.ack_frame("destination", &receipt.frame_id),
                FrameAcknowledgement::Accepted
            );
        }

        let retained_task = task(format!("task-{}", MAX_RETAINED_PUBLISHED_TASKS - 1));
        assert_eq!(
            runtime.classify_published_task(
                "source",
                "destination",
                retained_task.task_id.as_ref().unwrap().value.as_str(),
                &retained_task,
                PublishedTaskReceipt {
                    frame_id: "discarded-retry-candidate".to_string(),
                    accepted_at_ms: i64::MAX,
                },
            ),
            PublishedTaskIdentity::Retry(receipt(MAX_RETAINED_PUBLISHED_TASKS - 1))
        );
        let mut changed = retained_task.clone();
        changed.metadata.insert("changed".into(), "true".into());
        assert_eq!(
            runtime.classify_published_task(
                "source",
                "destination",
                retained_task.task_id.as_ref().unwrap().value.as_str(),
                &changed,
                receipt(MAX_RETAINED_PUBLISHED_TASKS + 10),
            ),
            PublishedTaskIdentity::Conflict
        );

        let overflow_task = task("task-overflow");
        let overflow_receipt = receipt(MAX_RETAINED_PUBLISHED_TASKS);
        assert!(matches!(
            runtime.classify_published_task(
                "source",
                "destination",
                "task-overflow",
                &overflow_task,
                overflow_receipt.clone(),
            ),
            PublishedTaskIdentity::New(_)
        ));
        assert_eq!(
            runtime.route_frame(
                "destination",
                RelayFrame {
                    frame_id: overflow_receipt.frame_id.clone(),
                    task: Some(overflow_task),
                    result: None,
                    authenticated_source_node_id: "source".to_string(),
                    destination_node_id: "destination".to_string(),
                    nodescale_identity_bind_v1: None,
                    nodescale_identity_challenge_v1: None,
                },
            ),
            FrameDelivery::Mailboxed
        );
        assert_eq!(
            runtime.ack_frame("destination", &overflow_receipt.frame_id),
            FrameAcknowledgement::Accepted
        );

        let first_task = task("task-0");
        let fresh_receipt = PublishedTaskReceipt {
            frame_id: "fresh-task-frame".to_string(),
            accepted_at_ms: 99_999,
        };
        assert_eq!(
            runtime.classify_published_task(
                "source",
                "destination",
                "task-0",
                &first_task,
                fresh_receipt.clone(),
            ),
            PublishedTaskIdentity::New(fresh_receipt.clone())
        );
        assert_ne!(fresh_receipt.frame_id, receipt(0).frame_id);
        assert_eq!(
            runtime.route_frame(
                "destination",
                RelayFrame {
                    frame_id: fresh_receipt.frame_id.clone(),
                    task: Some(first_task),
                    result: None,
                    authenticated_source_node_id: "source".to_string(),
                    destination_node_id: "destination".to_string(),
                    nodescale_identity_bind_v1: None,
                    nodescale_identity_challenge_v1: None,
                },
            ),
            FrameDelivery::Mailboxed
        );
        assert_eq!(
            runtime.ack_frame("destination", &receipt(0).frame_id),
            FrameAcknowledgement::UnknownFrame
        );
        assert_eq!(runtime.mailbox_depth("destination"), 1);
        assert_eq!(
            runtime.ack_frame("destination", &fresh_receipt.frame_id),
            FrameAcknowledgement::Accepted
        );
        assert_eq!(runtime.mailbox_depth("destination"), 0);

        let guard = runtime.lock_peers();
        assert_eq!(guard.published_tasks.len(), MAX_RETAINED_PUBLISHED_TASKS);
        assert_eq!(
            guard.published_frame_index.len(),
            MAX_RETAINED_PUBLISHED_TASKS
        );
    }

    #[test]
    fn full_frame_capacity_rejects_new_task_without_evicting_retry_history() {
        let runtime = RelayRuntime::new("relay");
        for index in 0..MAX_RETAINED_PUBLISHED_TASKS {
            let task = task(format!("retained-{index}"));
            let receipt = receipt(index);
            assert_eq!(
                runtime.classify_published_task(
                    "source",
                    "destination",
                    task.task_id.as_ref().unwrap().value.as_str(),
                    &task,
                    receipt.clone(),
                ),
                PublishedTaskIdentity::New(receipt.clone())
            );
            assert_eq!(
                runtime.route_frame(
                    "destination",
                    RelayFrame {
                        frame_id: receipt.frame_id.clone(),
                        task: Some(task),
                        result: None,
                        authenticated_source_node_id: "source".into(),
                        destination_node_id: "destination".into(),
                        nodescale_identity_bind_v1: None,
                        nodescale_identity_challenge_v1: None,
                    },
                ),
                FrameDelivery::Mailboxed
            );
            assert_eq!(
                runtime.ack_frame("destination", &receipt.frame_id),
                FrameAcknowledgement::Accepted
            );
        }
        for index in 0..MAX_TRACKED_FRAMES {
            assert_eq!(
                runtime.route_frame("other-destination", frame(format!("active-{index}"))),
                FrameDelivery::Mailboxed
            );
        }

        let overflow_task = task("capacity-overflow");
        let overflow_receipt = PublishedTaskReceipt {
            frame_id: "capacity-overflow-frame".into(),
            accepted_at_ms: 9_999,
        };
        assert_eq!(
            runtime.publish_task_frame(
                "source",
                "destination",
                "capacity-overflow",
                (&overflow_task, &overflow_task),
                RelayFrame {
                    frame_id: overflow_receipt.frame_id.clone(),
                    task: Some(overflow_task.clone()),
                    result: None,
                    authenticated_source_node_id: "source".into(),
                    destination_node_id: "destination".into(),
                    nodescale_identity_bind_v1: None,
                    nodescale_identity_challenge_v1: None,
                },
                overflow_receipt,
            ),
            PublishedTaskDelivery::RejectedCapacity
        );
        let oldest = task("retained-0");
        assert_eq!(
            runtime.classify_published_task(
                "source",
                "destination",
                "retained-0",
                &oldest,
                PublishedTaskReceipt {
                    frame_id: "discarded-candidate".into(),
                    accepted_at_ms: i64::MAX,
                },
            ),
            PublishedTaskIdentity::Retry(receipt(0))
        );
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

    #[test]
    fn blank_frame_identity_is_rejected_without_consuming_mailbox_capacity() {
        let runtime = RelayRuntime::new("relay");
        assert_eq!(
            runtime.route_frame("destination", frame("   ")),
            FrameDelivery::RejectedInvalid
        );
        assert_eq!(runtime.mailbox_depth("destination"), 0);
    }

    #[test]
    fn reconnect_backpressure_never_panics_or_discards_authoritative_mailbox() {
        let runtime = RelayRuntime::new("relay");
        assert_eq!(
            runtime.route_frame("destination", frame("pending")),
            FrameDelivery::Mailboxed
        );
        let (sender, _receiver) = mpsc::channel(1);
        sender.try_send(Ok(frame("already-buffered"))).unwrap();
        assert_eq!(runtime.connect_node("destination", sender), 0);
        assert_eq!(runtime.mailbox_depth("destination"), 1);
        assert!(!runtime.peer_identity("destination").unwrap().connected);
    }

    #[tokio::test]
    async fn result_delivery_waiter_resolves_only_for_authenticated_destination_ack() {
        let runtime = RelayRuntime::new("relay");
        let (delivery, acknowledgement) =
            runtime.route_frame_waiting_for_ack("destination", frame("result-frame"));
        assert_eq!(delivery, FrameDelivery::Mailboxed);
        let acknowledgement = acknowledgement.unwrap();
        assert_eq!(
            runtime.ack_frame("other-destination", "result-frame"),
            FrameAcknowledgement::WrongDestination
        );
        assert_eq!(
            runtime.ack_frame("destination", "result-frame"),
            FrameAcknowledgement::Accepted
        );
        acknowledgement.await.unwrap();
    }

    #[test]
    fn typed_direct_control_does_not_increment_generic_task_routing_metric() {
        let runtime = RelayRuntime::new("relay");
        let before = runtime.metrics().snapshot().tasks_routed;

        let (delivery, _completion) = runtime.route_nodescale_identity_bind_waiting_for_completion(
            "destination",
            direct_control_frame("direct-control-frame"),
        );

        assert_eq!(delivery, FrameDelivery::Mailboxed);
        assert_eq!(runtime.metrics().snapshot().tasks_routed, before);
    }

    #[tokio::test]
    async fn generic_ack_cannot_settle_or_orphan_a_typed_direct_control_frame() {
        let runtime = RelayRuntime::new("relay");
        let (delivery, completion) = runtime.route_nodescale_identity_bind_waiting_for_completion(
            "destination",
            direct_control_frame("direct-control-frame"),
        );
        assert_eq!(delivery, FrameDelivery::Mailboxed);
        let completion = completion.unwrap();

        assert_eq!(
            runtime.ack_frame("destination", "direct-control-frame"),
            FrameAcknowledgement::UnknownFrame
        );
        assert_eq!(
            runtime.complete_nodescale_identity_bind(
                "destination",
                "direct-control-frame",
                keryx_proto::v1::NodescaleIdentityBindResult {
                    disposition: keryx_proto::v1::NodescaleIdentityBindDisposition::Active as i32,
                    accepted: true,
                    binding_id: "binding".to_string(),
                    generation: 1,
                    revision: 1,
                    reason: String::new(),
                    code: String::new(),
                },
            ),
            DirectControlCompletion::Accepted
        );
        assert_eq!(completion.await.unwrap().binding_id, "binding");
    }

    #[tokio::test]
    async fn challenge_control_keeps_kind_specific_waiters_and_rejects_generic_ack() {
        let runtime = RelayRuntime::new("relay");
        let before = runtime.metrics().snapshot().tasks_routed;
        let (bind_delivery, bind_completion) = runtime
            .route_nodescale_identity_bind_waiting_for_completion(
                "destination",
                direct_control_frame("bind-frame"),
            );
        let (challenge_delivery, challenge_completion) = runtime
            .route_nodescale_identity_challenge_waiting_for_completion(
                "destination",
                challenge_control_frame("challenge-frame"),
            );
        assert_eq!(bind_delivery, FrameDelivery::Mailboxed);
        assert_eq!(challenge_delivery, FrameDelivery::Mailboxed);
        assert_eq!(runtime.metrics().snapshot().tasks_routed, before);

        assert_eq!(
            runtime.ack_frame("destination", "challenge-frame"),
            FrameAcknowledgement::UnknownFrame
        );
        assert_eq!(
            runtime.complete_nodescale_identity_bind(
                "destination",
                "challenge-frame",
                keryx_proto::v1::NodescaleIdentityBindResult {
                    disposition: keryx_proto::v1::NodescaleIdentityBindDisposition::Active as i32,
                    accepted: true,
                    binding_id: "binding".to_string(),
                    generation: 1,
                    revision: 1,
                    reason: String::new(),
                    code: String::new(),
                },
            ),
            DirectControlCompletion::UnknownFrame
        );
        assert_eq!(
            runtime.complete_nodescale_identity_challenge(
                "other-destination",
                "challenge-frame",
                challenge_result(),
            ),
            DirectControlCompletion::WrongDestination
        );
        assert_eq!(
            runtime.complete_nodescale_identity_challenge(
                "destination",
                "challenge-frame",
                challenge_result(),
            ),
            DirectControlCompletion::Accepted
        );
        assert_eq!(
            challenge_completion
                .unwrap()
                .await
                .unwrap()
                .challenge_secret,
            "delivery-only-test-secret"
        );
        assert_eq!(
            runtime.complete_nodescale_identity_challenge(
                "destination",
                "challenge-frame",
                challenge_result(),
            ),
            DirectControlCompletion::UnknownFrame
        );
        assert_eq!(
            runtime.complete_nodescale_identity_bind(
                "destination",
                "bind-frame",
                keryx_proto::v1::NodescaleIdentityBindResult {
                    disposition: keryx_proto::v1::NodescaleIdentityBindDisposition::Active as i32,
                    accepted: true,
                    binding_id: "binding".to_string(),
                    generation: 1,
                    revision: 1,
                    reason: String::new(),
                    code: String::new(),
                },
            ),
            DirectControlCompletion::Accepted
        );
        assert_eq!(
            bind_completion.unwrap().await.unwrap().binding_id,
            "binding"
        );
    }

    #[test]
    fn typed_direct_control_routes_reject_empty_or_mismatched_targets_before_ownership() {
        let runtime = RelayRuntime::new("relay");
        let before = runtime.metrics().snapshot().tasks_routed;

        for (target_node_id, frame) in [
            ("", direct_control_frame("empty-bind-target")),
            (
                "other-destination",
                direct_control_frame("mismatched-bind-target"),
            ),
            ("", challenge_control_frame("empty-challenge-target")),
            (
                "other-destination",
                challenge_control_frame("mismatched-challenge-target"),
            ),
        ] {
            let (delivery, completion_present) = if frame.nodescale_identity_bind_v1.is_some() {
                let (delivery, completion) = runtime
                    .route_nodescale_identity_bind_waiting_for_completion(target_node_id, frame);
                (delivery, completion.is_some())
            } else {
                let (delivery, completion) = runtime
                    .route_nodescale_identity_challenge_waiting_for_completion(
                        target_node_id,
                        frame,
                    );
                (delivery, completion.is_some())
            };
            assert_eq!(delivery, FrameDelivery::RejectedInvalid);
            assert!(!completion_present);
            assert_eq!(runtime.mailbox_depth("destination"), 0);
            assert_eq!(runtime.metrics().snapshot().tasks_routed, before);
            let guard = runtime.lock_peers();
            assert!(guard.frame_destinations.is_empty());
            assert!(guard.direct_control_waiters.is_empty());
            assert!(guard.challenge_control_waiters.is_empty());
        }
    }

    #[test]
    fn typed_control_routes_reject_task_or_result_mixed_frames_without_ownership() {
        let runtime = RelayRuntime::new("relay");
        let before = runtime.metrics().snapshot().tasks_routed;

        let mut bind_with_task = direct_control_frame("bind-with-task");
        bind_with_task.task = Some(task("mixed-bind-task"));
        let (delivery, receiver) = runtime
            .route_nodescale_identity_bind_waiting_for_completion("destination", bind_with_task);
        assert_eq!(delivery, FrameDelivery::RejectedInvalid);
        assert!(receiver.is_none());

        let mut bind_with_result = direct_control_frame("bind-with-result");
        bind_with_result.result = Some(keryx_proto::v1::TaskResultEnvelope::default());
        let (delivery, receiver) = runtime
            .route_nodescale_identity_bind_waiting_for_completion("destination", bind_with_result);
        assert_eq!(delivery, FrameDelivery::RejectedInvalid);
        assert!(receiver.is_none());

        let mut challenge_with_task = challenge_control_frame("challenge-with-task");
        challenge_with_task.task = Some(task("mixed-challenge-task"));
        let (delivery, receiver) = runtime
            .route_nodescale_identity_challenge_waiting_for_completion(
                "destination",
                challenge_with_task,
            );
        assert_eq!(delivery, FrameDelivery::RejectedInvalid);
        assert!(receiver.is_none());

        let mut challenge_with_result = challenge_control_frame("challenge-with-result");
        challenge_with_result.result = Some(keryx_proto::v1::TaskResultEnvelope::default());
        let (delivery, receiver) = runtime
            .route_nodescale_identity_challenge_waiting_for_completion(
                "destination",
                challenge_with_result,
            );
        assert_eq!(delivery, FrameDelivery::RejectedInvalid);
        assert!(receiver.is_none());

        assert_eq!(runtime.mailbox_depth("destination"), 0);
        assert_eq!(runtime.metrics().snapshot().tasks_routed, before);
        let guard = runtime.lock_peers();
        assert!(guard.direct_control_waiters.is_empty());
        assert!(guard.challenge_control_waiters.is_empty());
        assert!(guard.frame_destinations.is_empty());
    }

    #[tokio::test]
    async fn malformed_mixed_control_frames_and_challenge_restart_cleanup_are_fail_closed() {
        let runtime = RelayRuntime::new("relay");
        let mut malformed = direct_control_frame("mixed-frame");
        malformed.nodescale_identity_challenge_v1 =
            challenge_control_frame("unused").nodescale_identity_challenge_v1;
        assert_eq!(
            runtime
                .route_nodescale_identity_bind_waiting_for_completion(
                    "destination",
                    malformed.clone()
                )
                .0,
            FrameDelivery::RejectedInvalid
        );
        assert_eq!(
            runtime
                .route_nodescale_identity_challenge_waiting_for_completion("destination", malformed)
                .0,
            FrameDelivery::RejectedInvalid
        );
        assert_eq!(runtime.mailbox_depth("destination"), 0);

        let (delivery, completion) = runtime
            .route_nodescale_identity_challenge_waiting_for_completion(
                "destination",
                challenge_control_frame("restart-challenge-frame"),
            );
        assert_eq!(delivery, FrameDelivery::Mailboxed);
        drop(runtime);
        assert!(completion.unwrap().await.is_err());
    }

    #[test]
    fn abandoning_a_challenge_frame_is_scoped_and_releases_capacity() {
        let runtime = RelayRuntime::new("relay");
        let (delivery, completion) = runtime
            .route_nodescale_identity_challenge_waiting_for_completion(
                "destination",
                challenge_control_frame("abandoned-challenge-frame"),
            );
        assert_eq!(delivery, FrameDelivery::Mailboxed);
        runtime.abandon_frame("destination", "abandoned-challenge-frame");
        assert_eq!(runtime.mailbox_depth("destination"), 0);
        assert_eq!(
            runtime.ack_frame("destination", "abandoned-challenge-frame"),
            FrameAcknowledgement::UnknownFrame
        );
        assert!(completion.unwrap().try_recv().is_err());
    }

    #[test]
    fn abandoning_frame_is_idempotent_and_scoped_to_exact_destination_and_frame() {
        let runtime = RelayRuntime::new("relay");
        let (first_delivery, _first_acknowledgement) =
            runtime.route_frame_waiting_for_ack("destination-a", frame("shared-frame"));
        let (second_delivery, _second_acknowledgement) =
            runtime.route_frame_waiting_for_ack("destination-b", frame("shared-frame"));
        assert_eq!(first_delivery, FrameDelivery::Mailboxed);
        assert_eq!(second_delivery, FrameDelivery::Mailboxed);

        runtime.abandon_frame("destination-a", "shared-frame");
        runtime.abandon_frame("destination-a", "shared-frame");

        assert_eq!(runtime.mailbox_depth("destination-a"), 0);
        assert_eq!(runtime.mailbox_depth("destination-b"), 1);
        assert_eq!(
            runtime.ack_frame("destination-a", "shared-frame"),
            FrameAcknowledgement::WrongDestination
        );
        assert_eq!(
            runtime.ack_frame("destination-b", "shared-frame"),
            FrameAcknowledgement::Accepted
        );
    }

    #[tokio::test]
    async fn relay_restart_drops_unacknowledged_result_waiter_instead_of_settling_it() {
        let runtime = RelayRuntime::new("relay");
        let (delivery, acknowledgement) =
            runtime.route_frame_waiting_for_ack("destination", frame("result-frame"));
        assert_eq!(delivery, FrameDelivery::Mailboxed);
        drop(runtime);
        assert!(acknowledgement.unwrap().await.is_err());
    }
}
