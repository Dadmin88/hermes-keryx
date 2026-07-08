//! Periodic store health probes that keep daemon readiness current.

use std::sync::Arc;
use std::time::Duration;

use keryx_store::{SqliteStore, StoreError, CURRENT_SCHEMA_VERSION};
use tokio::sync::{watch, RwLock};
use tokio::task::JoinHandle;
use tracing::{info, instrument, warn};

use crate::{DynamicReadiness, KeryxDaemonRuntime};

/// Handle for a running [`HealthLoop`]; call [`shutdown`](Self::shutdown) to stop gracefully.
pub struct HealthLoopHandle {
    stop_tx: watch::Sender<bool>,
    join: JoinHandle<()>,
}

impl HealthLoopHandle {
    /// Signal the loop to exit and wait until it has finished its current tick.
    pub async fn shutdown(self) {
        let _ = self.stop_tx.send(true);
        let _ = self.join.await;
    }
}

/// Periodically probes the SQLite store and updates runtime readiness.
pub struct HealthLoop;

impl HealthLoop {
    /// Spawn the loop using the runtime store and configured interval.
    #[must_use]
    pub fn spawn(runtime: Arc<KeryxDaemonRuntime>) -> HealthLoopHandle {
        let interval_ms = runtime.config().health_check_interval_ms();
        let store = runtime.store().clone();
        let readiness = Arc::clone(runtime.readiness_handle());
        Self::spawn_with_store(store, readiness, interval_ms)
    }

    #[must_use]
    pub(crate) fn spawn_with_store(
        store: SqliteStore,
        readiness: Arc<RwLock<DynamicReadiness>>,
        interval_ms: u64,
    ) -> HealthLoopHandle {
        let (stop_tx, stop_rx) = watch::channel(false);
        let join = tokio::spawn(async move {
            Self::run(store, readiness, interval_ms, stop_rx).await;
        });
        HealthLoopHandle { stop_tx, join }
    }

    async fn run(
        store: SqliteStore,
        readiness: Arc<RwLock<DynamicReadiness>>,
        interval_ms: u64,
        mut stop_rx: watch::Receiver<bool>,
    ) {
        let mut interval = tokio::time::interval(Duration::from_millis(interval_ms.max(1)));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            tokio::select! {
                _ = interval.tick() => {
                    Self::tick(&store, &readiness).await;
                }
                changed = stop_rx.changed() => {
                    if changed.is_err() || *stop_rx.borrow() {
                        info!(component = "health_loop", "stopping");
                        break;
                    }
                }
            }
        }
    }

    #[instrument(
        name = "keryx::daemon::health_tick",
        skip(store, readiness),
        fields(ready = tracing::field::Empty, reason_count = tracing::field::Empty)
    )]
    pub(crate) async fn tick(store: &SqliteStore, readiness: &RwLock<DynamicReadiness>) {
        let snapshot = probe_store_readiness(store).await;
        let ready = snapshot.ready;
        let reason_count = snapshot.not_ready_reasons.len();
        tracing::Span::current().record("ready", ready);
        tracing::Span::current().record("reason_count", reason_count);

        if !ready {
            warn!(
                component = "health_loop",
                ?snapshot.not_ready_reasons,
                "daemon readiness degraded"
            );
        }

        *readiness.write().await = snapshot;
    }
}

/// Evaluate current store health for readiness reporting.
pub async fn probe_store_readiness(store: &SqliteStore) -> DynamicReadiness {
    let mut not_ready_reasons = Vec::new();

    match store.schema_version().await {
        Ok(version) if version == CURRENT_SCHEMA_VERSION => {}
        Ok(version) => {
            not_ready_reasons.push(format!(
                "schema_version mismatch: found={version} supported={CURRENT_SCHEMA_VERSION}"
            ));
        }
        Err(error) => {
            not_ready_reasons.push(format!("store connectivity failed: {error}"));
        }
    }

    let now_ms = unix_ms_now();
    match store.recover_stale_leases(now_ms, None).await {
        Ok(report) => {
            let corrupt = report.corruption_count();
            if corrupt > 0 {
                not_ready_reasons.push(format!(
                    "corruption_count={corrupt} corrupted_tasks={:?}",
                    report.corrupted_tasks
                ));
            }
        }
        Err(StoreError::UnrepairedCorruption { corrupted_tasks }) => {
            not_ready_reasons.push(format!(
                "unrepaired_corruption: corrupted_tasks={corrupted_tasks:?}"
            ));
        }
        Err(error) => {
            not_ready_reasons.push(format!("store health probe failed: {error}"));
        }
    }

    DynamicReadiness {
        ready: not_ready_reasons.is_empty(),
        not_ready_reasons,
    }
}

fn unix_ms_now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(i64::MAX as u128) as i64
}
