//! Runtime cancellation and deadline-enforcement counters for the local Keryx daemon.

use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};

/// Point-in-time view of cancellation/deadline enforcement state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct CancellationSnapshot {
    pub cancel_requests: u64,
    pub tasks_canceled: u64,
    pub deadline_ticks: u64,
    pub deadline_failures: u64,
    pub last_deadline_scan_ms: i64,
    pub last_deadline_failures: u64,
}

/// Thread-safe counters for explicit cancellation and deadline scans.
#[derive(Debug, Default)]
pub struct CancellationState {
    cancel_requests: AtomicU64,
    tasks_canceled: AtomicU64,
    deadline_ticks: AtomicU64,
    deadline_failures: AtomicU64,
    last_deadline_scan_ms: AtomicI64,
    last_deadline_failures: AtomicU64,
}

impl CancellationState {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    pub fn increment_cancel_requests(&self) {
        self.cancel_requests.fetch_add(1, Ordering::Relaxed);
    }

    pub fn increment_tasks_canceled(&self) {
        self.tasks_canceled.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_deadline_scan(&self, now_ms: i64, failures: u64) {
        self.deadline_ticks.fetch_add(1, Ordering::Relaxed);
        self.deadline_failures
            .fetch_add(failures, Ordering::Relaxed);
        self.last_deadline_scan_ms.store(now_ms, Ordering::Relaxed);
        self.last_deadline_failures
            .store(failures, Ordering::Relaxed);
    }

    #[must_use]
    pub fn snapshot(&self) -> CancellationSnapshot {
        CancellationSnapshot {
            cancel_requests: self.cancel_requests.load(Ordering::Relaxed),
            tasks_canceled: self.tasks_canceled.load(Ordering::Relaxed),
            deadline_ticks: self.deadline_ticks.load(Ordering::Relaxed),
            deadline_failures: self.deadline_failures.load(Ordering::Relaxed),
            last_deadline_scan_ms: self.last_deadline_scan_ms.load(Ordering::Relaxed),
            last_deadline_failures: self.last_deadline_failures.load(Ordering::Relaxed),
        }
    }
}
