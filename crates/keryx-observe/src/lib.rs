//! In-process metrics for the Hermes Keryx runtime.

mod relay_metrics;

pub use relay_metrics::{RelayMetrics, RelayMetricsSnapshot};

use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};

/// Point-in-time view of daemon counters and gauges.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct MetricsSnapshot {
    pub tasks_submitted: u64,
    pub tasks_claimed: u64,
    pub tasks_completed: u64,
    pub tasks_failed: u64,
    pub heartbeats: u64,
    pub leases_recovered: u64,
    pub recovery_ticks: u64,
    pub dead_letters: u64,
    pub active_leases: i64,
}

/// Thread-safe counters and gauges for task lifecycle and recovery.
#[derive(Debug, Default)]
pub struct KeryxMetrics {
    tasks_submitted: AtomicU64,
    tasks_claimed: AtomicU64,
    tasks_completed: AtomicU64,
    tasks_failed: AtomicU64,
    heartbeats: AtomicU64,
    leases_recovered: AtomicU64,
    recovery_ticks: AtomicU64,
    dead_letters: AtomicU64,
    active_leases: AtomicI64,
}

impl KeryxMetrics {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    pub fn increment_tasks_submitted(&self) {
        self.tasks_submitted.fetch_add(1, Ordering::Relaxed);
    }

    pub fn increment_tasks_claimed(&self) {
        self.tasks_claimed.fetch_add(1, Ordering::Relaxed);
        self.active_leases.fetch_add(1, Ordering::Relaxed);
    }

    pub fn increment_tasks_completed(&self) {
        self.tasks_completed.fetch_add(1, Ordering::Relaxed);
        self.decrement_active_leases();
    }

    pub fn increment_tasks_failed(&self) {
        self.tasks_failed.fetch_add(1, Ordering::Relaxed);
        self.decrement_active_leases();
    }

    pub fn increment_heartbeats(&self) {
        self.heartbeats.fetch_add(1, Ordering::Relaxed);
    }

    pub fn increment_leases_recovered(&self) {
        self.leases_recovered.fetch_add(1, Ordering::Relaxed);
        self.decrement_active_leases();
    }

    pub fn increment_recovery_ticks(&self) {
        self.recovery_ticks.fetch_add(1, Ordering::Relaxed);
    }

    pub fn increment_dead_letters(&self) {
        self.dead_letters.fetch_add(1, Ordering::Relaxed);
    }

    pub fn decrement_active_leases(&self) {
        self.active_leases.fetch_sub(1, Ordering::Relaxed);
    }

    #[must_use]
    pub fn snapshot(&self) -> MetricsSnapshot {
        MetricsSnapshot {
            tasks_submitted: self.tasks_submitted.load(Ordering::Relaxed),
            tasks_claimed: self.tasks_claimed.load(Ordering::Relaxed),
            tasks_completed: self.tasks_completed.load(Ordering::Relaxed),
            tasks_failed: self.tasks_failed.load(Ordering::Relaxed),
            heartbeats: self.heartbeats.load(Ordering::Relaxed),
            leases_recovered: self.leases_recovered.load(Ordering::Relaxed),
            recovery_ticks: self.recovery_ticks.load(Ordering::Relaxed),
            dead_letters: self.dead_letters.load(Ordering::Relaxed),
            active_leases: self.active_leases.load(Ordering::Relaxed),
        }
    }
}
