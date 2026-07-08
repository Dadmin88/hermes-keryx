//! Relay-server counters and gauges (peers, registry, routing).

use std::sync::atomic::{AtomicU64, Ordering};

/// Point-in-time relay telemetry.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct RelayMetricsSnapshot {
    pub connected_peers: u64,
    pub registry_size: u64,
    pub tasks_routed: u64,
}

/// Thread-safe relay metrics updated by the relay process and gRPC handlers.
#[derive(Debug, Default)]
pub struct RelayMetrics {
    connected_peers: AtomicU64,
    registry_size: AtomicU64,
    tasks_routed: AtomicU64,
}

impl RelayMetrics {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    pub fn set_connected_peers(&self, count: u64) {
        self.connected_peers.store(count, Ordering::Relaxed);
    }

    pub fn increment_connected_peers(&self) {
        self.connected_peers.fetch_add(1, Ordering::Relaxed);
    }

    pub fn decrement_connected_peers(&self) {
        self.connected_peers
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                current.checked_sub(1)
            })
            .ok();
    }

    pub fn set_registry_size(&self, count: u64) {
        self.registry_size.store(count, Ordering::Relaxed);
    }

    pub fn increment_registry_size(&self) {
        self.registry_size.fetch_add(1, Ordering::Relaxed);
    }

    pub fn decrement_registry_size(&self) {
        self.registry_size
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                current.checked_sub(1)
            })
            .ok();
    }

    pub fn increment_tasks_routed(&self) {
        self.tasks_routed.fetch_add(1, Ordering::Relaxed);
    }

    #[must_use]
    pub fn snapshot(&self) -> RelayMetricsSnapshot {
        RelayMetricsSnapshot {
            connected_peers: self.connected_peers.load(Ordering::Relaxed),
            registry_size: self.registry_size.load(Ordering::Relaxed),
            tasks_routed: self.tasks_routed.load(Ordering::Relaxed),
        }
    }
}
