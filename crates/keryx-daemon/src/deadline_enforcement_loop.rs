//! Background deadline enforcement for the local Keryx daemon.
//!
//! Phase 11C wires the daemon into the store-level deadline API by periodically
//! calling the Phase 11B `fail_expired_deadlines` implementation.

use std::sync::Arc;
use std::time::Duration;

use keryx_store::SqliteStore;
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tracing::{info, instrument, warn};

use crate::{CancellationState, KeryxDaemonRuntime};

/// Handle for a running [`DeadlineEnforcementLoop`]; call [`shutdown`](Self::shutdown) to stop.
pub struct DeadlineEnforcementLoopHandle {
    stop_tx: watch::Sender<bool>,
    join: JoinHandle<()>,
}

impl DeadlineEnforcementLoopHandle {
    /// Signal the loop to exit and wait until it has finished its current tick.
    pub async fn shutdown(self) {
        let _ = self.stop_tx.send(true);
        let _ = self.join.await;
    }
}

/// Periodically calls `fail_expired_deadlines` so timed-out work is terminalized.
pub struct DeadlineEnforcementLoop;

impl DeadlineEnforcementLoop {
    /// Spawn the loop using the runtime store and configured interval.
    #[must_use]
    pub fn spawn(runtime: Arc<KeryxDaemonRuntime>) -> DeadlineEnforcementLoopHandle {
        let interval_ms = runtime.config().deadline_enforcement_interval_ms();
        let store = runtime.store().clone();
        let cancellation = Arc::clone(runtime.cancellation());
        Self::spawn_with_store(store, interval_ms, cancellation)
    }

    #[must_use]
    pub(crate) fn spawn_with_store(
        store: SqliteStore,
        interval_ms: u64,
        cancellation: Arc<CancellationState>,
    ) -> DeadlineEnforcementLoopHandle {
        let (stop_tx, stop_rx) = watch::channel(false);
        let join = tokio::spawn(async move {
            Self::run(store, interval_ms, cancellation, stop_rx).await;
        });
        DeadlineEnforcementLoopHandle { stop_tx, join }
    }

    async fn run(
        store: SqliteStore,
        interval_ms: u64,
        cancellation: Arc<CancellationState>,
        mut stop_rx: watch::Receiver<bool>,
    ) {
        let mut interval = tokio::time::interval(Duration::from_millis(interval_ms.max(1)));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            tokio::select! {
                _ = interval.tick() => {
                    Self::tick(&store, cancellation.as_ref()).await;
                }
                changed = stop_rx.changed() => {
                    if changed.is_err() || *stop_rx.borrow() {
                        info!(component = "deadline_enforcement_loop", "stopping");
                        break;
                    }
                }
            }
        }
    }

    #[instrument(
        name = "keryx::daemon::deadline_enforcement_tick",
        skip(store, cancellation),
        fields(duration_ms = tracing::field::Empty, failed_tasks = tracing::field::Empty)
    )]
    async fn tick(store: &SqliteStore, cancellation: &CancellationState) {
        let started = std::time::Instant::now();
        let now_ms = unix_ms_now();
        let mut failed_tasks = 0_u64;
        match store.fail_expired_deadlines(now_ms, None).await {
            Ok(expired_tasks) => {
                failed_tasks = expired_tasks.len().min(u64::MAX as usize) as u64;
                cancellation.record_deadline_scan(now_ms, failed_tasks);
                if failed_tasks > 0 {
                    info!(
                        component = "deadline_enforcement_loop",
                        now_ms, failed_tasks, "failed expired task deadlines"
                    );
                }
            }
            Err(error) => {
                cancellation.record_deadline_scan(now_ms, 0);
                warn!(
                    component = "deadline_enforcement_loop",
                    error = %error,
                    "deadline enforcement failed"
                );
            }
        }
        let duration_ms = started.elapsed().as_millis().min(u128::from(u64::MAX)) as u64;
        tracing::Span::current().record("duration_ms", duration_ms);
        tracing::Span::current().record("failed_tasks", failed_tasks);
        info!(
            component = "deadline_enforcement_loop",
            failed_tasks, duration_ms, "deadline enforcement tick"
        );
    }
}

fn unix_ms_now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(i64::MAX as u128) as i64
}
