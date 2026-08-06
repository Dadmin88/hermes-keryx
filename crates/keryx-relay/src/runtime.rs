//! Shared relay process state for health, telemetry, and relay delivery.

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use keryx_observe::RelayMetrics;
use keryx_proto::v1::{RelayFrame, TaskEnvelope};
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
        let target_node_id = target_node_id.into();
        let frame_id = frame.frame_id.trim().to_string();
        let mut guard = self.lock_peers();
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

    /// Abandon a timed-out result frame generation while leaving any already-delivered copy
    /// harmlessly idempotent at the destination.
    pub fn abandon_frame(&self, destination_node_id: &str, frame_id: &str) {
        let key = (destination_node_id.to_string(), frame_id.to_string());
        let mut guard = self.lock_peers();
        guard.frame_ack_waiters.remove(&key);
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

    /// Atomically admit a task identity and acquire bounded frame ownership before delivery.
    pub fn publish_task_frame(
        &self,
        source_node_id: &str,
        destination_node_id: &str,
        task_id: &str,
        task: &TaskEnvelope,
        frame: RelayFrame,
        proposed_receipt: PublishedTaskReceipt,
    ) -> PublishedTaskDelivery {
        let task_key = (
            source_node_id.to_string(),
            destination_node_id.to_string(),
            task_id.to_string(),
        );
        let mut guard = self.lock_peers();
        if let Some(existing) = guard.published_tasks.get(&task_key) {
            return if existing.envelope == *task {
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
                envelope: task.clone(),
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
            task: None,
            result: None,
            authenticated_source_node_id: "source".to_string(),
            destination_node_id: "destination".to_string(),
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
                &overflow_task,
                RelayFrame {
                    frame_id: overflow_receipt.frame_id.clone(),
                    task: Some(overflow_task.clone()),
                    result: None,
                    authenticated_source_node_id: "source".into(),
                    destination_node_id: "destination".into(),
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
