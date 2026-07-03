//! Background stale-lease recovery for the local Keryx daemon.

use std::sync::Arc;
use std::time::Duration;

use keryx_observe::KeryxMetrics;
use keryx_store::{SqliteStore, StoreError};
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tracing::{error, info, instrument, warn};

use crate::KeryxDaemonRuntime;

/// Handle for a running [`LeaseRecoveryLoop`]; call [`shutdown`](Self::shutdown) to stop gracefully.
pub struct LeaseRecoveryLoopHandle {
    stop_tx: watch::Sender<bool>,
    join: JoinHandle<()>,
}

impl LeaseRecoveryLoopHandle {
    /// Signal the loop to exit and wait until it has finished its current tick.
    pub async fn shutdown(self) {
        let _ = self.stop_tx.send(true);
        let _ = self.join.await;
    }
}

/// Periodically calls `SqliteStore::recover_stale_leases` so crashed workers lose claims.
pub struct LeaseRecoveryLoop;

impl LeaseRecoveryLoop {
    /// Spawn the loop using the runtime store and configured interval.
    #[must_use]
    pub fn spawn(runtime: Arc<KeryxDaemonRuntime>) -> LeaseRecoveryLoopHandle {
        let interval_ms = runtime.config().lease_recovery_interval_ms();
        let store = runtime.store().clone();
        let metrics = Arc::clone(runtime.metrics());
        Self::spawn_with_store(store, interval_ms, metrics)
    }

    #[must_use]
    pub(crate) fn spawn_with_store(
        store: SqliteStore,
        interval_ms: u64,
        metrics: Arc<KeryxMetrics>,
    ) -> LeaseRecoveryLoopHandle {
        let (stop_tx, stop_rx) = watch::channel(false);
        let join = tokio::spawn(async move {
            Self::run(store, interval_ms, metrics, stop_rx).await;
        });
        LeaseRecoveryLoopHandle { stop_tx, join }
    }

    async fn run(
        store: SqliteStore,
        interval_ms: u64,
        metrics: Arc<KeryxMetrics>,
        mut stop_rx: watch::Receiver<bool>,
    ) {
        let mut interval = tokio::time::interval(Duration::from_millis(interval_ms.max(1)));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            tokio::select! {
                _ = interval.tick() => {
                    Self::tick(&store, &metrics).await;
                }
                changed = stop_rx.changed() => {
                    if changed.is_err() || *stop_rx.borrow() {
                        info!(component = "lease_recovery_loop", "stopping");
                        break;
                    }
                }
            }
        }
    }

    #[instrument(
        name = "keryx::daemon::lease_recovery_tick",
        skip(store),
        fields(
            duration_ms = tracing::field::Empty,
            tasks_recovered = tracing::field::Empty,
            leases_cleaned = tracing::field::Empty
        )
    )]
    async fn tick(store: &SqliteStore, metrics: &KeryxMetrics) {
        metrics.increment_recovery_ticks();
        let started = std::time::Instant::now();
        let now_ms = unix_ms_now();
        let mut tasks_recovered = 0usize;
        let mut leases_cleaned = 0usize;
        match store.recover_stale_leases(now_ms, None).await {
            Ok(report) => {
                tasks_recovered = report.recovered_task_count();
                leases_cleaned = report.cleaned_terminal_leases;
                for _ in 0..tasks_recovered {
                    metrics.increment_leases_recovered();
                }
                let corrupt = report.corruption_count();
                if tasks_recovered > 0 || leases_cleaned > 0 {
                    info!(
                        component = "lease_recovery_loop",
                        now_ms, tasks_recovered, leases_cleaned, "recovered stale leases"
                    );
                    for task in &report.recovered_tasks {
                        info!(
                            component = "lease_recovery_loop",
                            task_id = %task.task_id().as_str(),
                            to_status = "pending",
                            "stale lease recovery"
                        );
                    }
                }
                if corrupt > 0 {
                    error!(
                        component = "lease_recovery_loop",
                        corruption_count = corrupt,
                        corrupted_tasks = ?report.corrupted_tasks,
                        "recovery found corrupt task snapshots"
                    );
                }
            }
            Err(StoreError::UnrepairedCorruption { corrupted_tasks }) => {
                error!(
                    component = "lease_recovery_loop",
                    corruption_count = corrupted_tasks.len(),
                    ?corrupted_tasks,
                    "unrepaired corruption during background recovery"
                );
            }
            Err(error) => {
                warn!(
                    component = "lease_recovery_loop",
                    error = %error,
                    "background lease recovery failed"
                );
            }
        }
        let duration_ms = started.elapsed().as_millis().min(u128::from(u64::MAX)) as u64;
        tracing::Span::current().record("duration_ms", duration_ms);
        tracing::Span::current().record("tasks_recovered", tasks_recovered);
        tracing::Span::current().record("leases_cleaned", leases_cleaned);
        info!(
            component = "lease_recovery_loop",
            tasks_recovered, leases_cleaned, duration_ms, "lease recovery tick"
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
