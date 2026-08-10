//! Local daemon startup/runtime foundation for Hermes Keryx.

mod cancellation;
mod deadline_enforcement_loop;
mod discovery;
mod health_loop;
mod incoming;
mod lease_recovery_loop;
mod routing;

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use keryx_core::{
    should_inline, AgentId, ArtifactId, ArtifactMeta, Digest, IdempotencyKey, LeaseId,
    LimitExceeded, LimitsConfig, MediaType, PeerId, RetryPolicy, TaskId, TaskStatus,
    ValidationError, MAX_BLOB_BYTES,
};
use keryx_observe::{KeryxMetrics, MetricsSnapshot};
use keryx_proto::v1::{
    keryx_daemon_server::{KeryxDaemon, KeryxDaemonServer},
    AgentId as ProtoAgentId, ArtifactId as ProtoArtifactId, ArtifactSummary, CancelTaskRequest,
    CancelTaskResponse, ClaimTaskRequest, ClaimTaskResponse, CompleteTaskRequest,
    CompleteTaskResponse, DeleteArtifactRequest, DeleteArtifactResponse, DiscoverSkillsRequest,
    DiscoverSkillsResponse, DoctorRequest, DoctorResponse, FailTaskRequest, FailTaskResponse,
    GetArtifactRequest, GetArtifactResponse, HeartbeatRequest, HeartbeatResponse,
    IdempotencyKey as ProtoIdempotencyKey, LeaseId as ProtoLeaseId, ListArtifactsRequest,
    ListArtifactsResponse, ListPeersRequest, ListPeersResponse, LivenessRequest, LivenessResponse,
    PeerDescriptor, PutArtifactRequest, PutArtifactResponse, ReadinessRequest, ReadinessResponse,
    SendTaskRequest, SendTaskResponse, StatusRequest, StatusResponse, SubmitTaskRequest,
    SubmitTaskResponse, TaskId as ProtoTaskId,
};
use prost::Message;
use tokio::sync::watch;
use tokio::sync::{Mutex, RwLock};
use tokio_stream::wrappers::TcpListenerStream;
use tonic::{Code, Request, Response, Status};
use tracing::{error, instrument, warn};

use keryx_store::{
    LeaseRecord, RecoveryReport, SqliteStore, StoreError, StoreResult, TaskRecord,
    CURRENT_SCHEMA_VERSION,
};

pub use cancellation::{CancellationSnapshot, CancellationState};
pub use deadline_enforcement_loop::{DeadlineEnforcementLoop, DeadlineEnforcementLoopHandle};
pub use discovery::{
    ConfiguredSkill, DiscoveryHandle, DiscoverySettings, RegistrationSettings,
    DEFAULT_REFRESH_LEAD_SECONDS, DEFAULT_REGISTRATION_TTL_SECONDS,
};
pub use health_loop::{probe_store_readiness, HealthLoop, HealthLoopHandle};
pub use incoming::{
    handle_incoming_task, IncomingDispatchConfig, IncomingHandleResult, IncomingRelayTask,
    IncomingTaskLoop, IncomingTaskLoopHandle, SenderAllowlist, StaticSenderAllowlist,
};
pub use lease_recovery_loop::{LeaseRecoveryLoop, LeaseRecoveryLoopHandle};
pub use routing::{
    routing_error_to_status, DeliveryRoute, GrpcRelayTaskPublisher, NoopRelayPublisher,
    PeerDirectory, PeerInfo, RelayTaskPublisher, RoutingError, SendTaskOutcome, TaskRouter,
    DEFAULT_SEND_TASK_TIMEOUT_MS,
};

/// Default background health probe interval.
const DEFAULT_HEALTH_CHECK_INTERVAL_MS: u64 = 60_000;

/// Default background stale-lease scan interval.
const DEFAULT_LEASE_RECOVERY_INTERVAL_MS: u64 = 30_000;

/// Default background task-deadline scan interval.
const DEFAULT_DEADLINE_ENFORCEMENT_INTERVAL_MS: u64 = 30_000;

/// Default worker lease TTL when callers omit `lease_duration_ms`.
const DEFAULT_LEASE_DEFAULT_TTL_MS: i64 = 300_000;

/// Default time to wait for in-flight RPCs during graceful shutdown.
const DEFAULT_SHUTDOWN_TIMEOUT_MS: u64 = 30_000;
const ARTIFACT_RPC_MAX_BYTES: usize = MAX_BLOB_BYTES + (1024 * 1024);

#[derive(Debug)]
struct ShutdownState {
    shutting_down: AtomicBool,
    in_flight_rpcs: AtomicUsize,
    grpc_shutdown_tx: watch::Sender<bool>,
    grpc_shutdown_rx: watch::Receiver<bool>,
}

impl ShutdownState {
    fn new() -> Self {
        let (grpc_shutdown_tx, grpc_shutdown_rx) = watch::channel(false);
        Self {
            shutting_down: AtomicBool::new(false),
            in_flight_rpcs: AtomicUsize::new(0),
            grpc_shutdown_tx,
            grpc_shutdown_rx,
        }
    }

    fn mark_shutting_down(&self) {
        self.shutting_down.store(true, Ordering::SeqCst);
    }

    fn signal_grpc_stop(&self) {
        let _ = self.grpc_shutdown_tx.send(true);
    }

    fn initiate(&self) {
        self.mark_shutting_down();
        self.signal_grpc_stop();
    }

    fn is_shutting_down(&self) -> bool {
        self.shutting_down.load(Ordering::SeqCst)
    }

    fn in_flight(&self) -> usize {
        self.in_flight_rpcs.load(Ordering::SeqCst)
    }

    fn grpc_shutdown_wait(&self) -> impl std::future::Future<Output = ()> + Send {
        let mut rx = self.grpc_shutdown_rx.clone();
        async move {
            let _ = rx.wait_for(|active| *active).await;
        }
    }
}

struct RpcInFlightGuard {
    state: Arc<ShutdownState>,
}

impl RpcInFlightGuard {
    fn enter(runtime: &KeryxDaemonRuntime) -> Result<Self, Status> {
        if runtime.shutdown.is_shutting_down() {
            return Err(Status::unavailable("daemon is shutting down"));
        }
        runtime
            .shutdown
            .in_flight_rpcs
            .fetch_add(1, Ordering::SeqCst);
        Ok(Self {
            state: Arc::clone(&runtime.shutdown),
        })
    }
}

impl Drop for RpcInFlightGuard {
    fn drop(&mut self) {
        self.state.in_flight_rpcs.fetch_sub(1, Ordering::SeqCst);
    }
}

async fn test_rpc_delay() {
    let delay_ms = std::env::var("KERYX_TEST_RPC_DELAY_MS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(0);
    if delay_ms > 0 {
        tokio::time::sleep(Duration::from_millis(delay_ms)).await;
    }
}

/// Default local peer id when callers do not override config.
const DEFAULT_LOCAL_PEER_ID: &str = "node-local";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KeryxDaemonConfig {
    data_dir: PathBuf,
    blob_dir: PathBuf,
    startup_recovery_now_ms: i64,
    lease_recovery_interval_ms: u64,
    deadline_enforcement_interval_ms: u64,
    lease_default_ttl_ms: i64,
    health_check_interval_ms: u64,
    shutdown_timeout_ms: u64,
    fail_retry_policy: RetryPolicy,
    limits: LimitsConfig,
    local_peer_id: PeerId,
    send_task_timeout_ms: u64,
    discovery: Option<DiscoverySettings>,
    relay_endpoint: Option<String>,
}

impl KeryxDaemonConfig {
    #[must_use]
    pub fn new(data_dir: impl Into<PathBuf>, startup_recovery_now_ms: i64) -> Self {
        let data_dir = data_dir.into();
        Self {
            blob_dir: data_dir.join("blobs"),
            data_dir,
            startup_recovery_now_ms,
            lease_recovery_interval_ms: DEFAULT_LEASE_RECOVERY_INTERVAL_MS,
            deadline_enforcement_interval_ms: DEFAULT_DEADLINE_ENFORCEMENT_INTERVAL_MS,
            lease_default_ttl_ms: DEFAULT_LEASE_DEFAULT_TTL_MS,
            health_check_interval_ms: DEFAULT_HEALTH_CHECK_INTERVAL_MS,
            shutdown_timeout_ms: DEFAULT_SHUTDOWN_TIMEOUT_MS,
            fail_retry_policy: RetryPolicy::default(),
            limits: LimitsConfig::default(),
            local_peer_id: PeerId::new(DEFAULT_LOCAL_PEER_ID).expect("static local peer id"),
            send_task_timeout_ms: DEFAULT_SEND_TASK_TIMEOUT_MS,
            discovery: None,
            relay_endpoint: None,
        }
    }

    #[must_use]
    pub fn with_lease_recovery_interval_ms(mut self, lease_recovery_interval_ms: u64) -> Self {
        self.lease_recovery_interval_ms = lease_recovery_interval_ms;
        self
    }

    #[must_use]
    pub fn with_lease_default_ttl_ms(mut self, lease_default_ttl_ms: i64) -> Self {
        self.lease_default_ttl_ms = lease_default_ttl_ms;
        self
    }

    #[must_use]
    pub fn data_dir(&self) -> &Path {
        &self.data_dir
    }

    #[must_use]
    pub fn db_path(&self) -> PathBuf {
        self.data_dir.join("keryx.db")
    }

    #[must_use]
    pub fn blob_dir(&self) -> &Path {
        &self.blob_dir
    }

    #[must_use]
    pub const fn startup_recovery_now_ms(&self) -> i64 {
        self.startup_recovery_now_ms
    }

    #[must_use]
    pub const fn lease_recovery_interval_ms(&self) -> u64 {
        self.lease_recovery_interval_ms
    }

    #[must_use]
    pub fn with_deadline_enforcement_interval_ms(
        mut self,
        deadline_enforcement_interval_ms: u64,
    ) -> Self {
        self.deadline_enforcement_interval_ms = deadline_enforcement_interval_ms;
        self
    }

    #[must_use]
    pub const fn deadline_enforcement_interval_ms(&self) -> u64 {
        self.deadline_enforcement_interval_ms
    }

    #[must_use]
    pub fn with_health_check_interval_ms(mut self, health_check_interval_ms: u64) -> Self {
        self.health_check_interval_ms = health_check_interval_ms;
        self
    }

    #[must_use]
    pub const fn health_check_interval_ms(&self) -> u64 {
        self.health_check_interval_ms
    }

    #[must_use]
    pub fn with_shutdown_timeout_ms(mut self, shutdown_timeout_ms: u64) -> Self {
        self.shutdown_timeout_ms = shutdown_timeout_ms;
        self
    }

    #[must_use]
    pub const fn shutdown_timeout_ms(&self) -> u64 {
        self.shutdown_timeout_ms
    }

    #[must_use]
    pub fn with_fail_retry_policy(mut self, fail_retry_policy: RetryPolicy) -> Self {
        self.fail_retry_policy = fail_retry_policy;
        self
    }

    #[must_use]
    pub const fn fail_retry_policy(&self) -> RetryPolicy {
        self.fail_retry_policy
    }

    #[must_use]
    pub fn with_limits(mut self, limits: LimitsConfig) -> Self {
        self.limits = limits;
        self
    }

    #[must_use]
    pub const fn limits(&self) -> &LimitsConfig {
        &self.limits
    }

    #[must_use]
    pub const fn lease_default_ttl_ms(&self) -> i64 {
        self.lease_default_ttl_ms
    }

    #[must_use]
    pub fn with_local_peer_id(mut self, local_peer_id: PeerId) -> Self {
        self.local_peer_id = local_peer_id;
        self
    }

    #[must_use]
    pub fn local_peer_id(&self) -> &PeerId {
        &self.local_peer_id
    }

    #[must_use]
    pub fn with_send_task_timeout_ms(mut self, send_task_timeout_ms: u64) -> Self {
        self.send_task_timeout_ms = send_task_timeout_ms;
        self
    }

    #[must_use]
    pub const fn send_task_timeout_ms(&self) -> u64 {
        self.send_task_timeout_ms
    }

    #[must_use]
    pub fn with_discovery(mut self, discovery: Option<DiscoverySettings>) -> Self {
        self.discovery = discovery;
        self
    }

    #[must_use]
    pub fn discovery(&self) -> Option<&DiscoverySettings> {
        self.discovery.as_ref()
    }

    #[must_use]
    pub fn with_relay_endpoint(mut self, relay_endpoint: Option<String>) -> Self {
        self.relay_endpoint = relay_endpoint
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty());
        self
    }

    #[must_use]
    pub fn relay_endpoint(&self) -> Option<&str> {
        self.relay_endpoint.as_deref()
    }
}

const RELAY_ENDPOINT_ENV: &str = "HERMES_KERYX_RELAY_ENDPOINT";
const RELAY_HEALTH_ENDPOINT_ENV: &str = "HERMES_KERYX_RELAY_HEALTH_ENDPOINT";
const RELAY_REGISTRY_ENDPOINT_ENV: &str = "HERMES_KERYX_RELAY_REGISTRY_ENDPOINT";
const DAEMON_SKILLS_ENV: &str = "HERMES_KERYX_DAEMON_SKILLS";
const DAEMON_NAME_ENV: &str = "HERMES_KERYX_DAEMON_NAME";
const DAEMON_DESCRIPTION_ENV: &str = "HERMES_KERYX_DAEMON_DESCRIPTION";
const DAEMON_REGISTRATION_TTL_ENV: &str = "HERMES_KERYX_DAEMON_REGISTRATION_TTL_SECONDS";

/// Build relay task publishing endpoint from environment when configured.
#[must_use]
pub fn relay_endpoint_from_env() -> Option<String> {
    std::env::var(RELAY_ENDPOINT_ENV)
        .or_else(|_| std::env::var(RELAY_HEALTH_ENDPOINT_ENV))
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

/// Build discovery settings from environment when a relay registry endpoint is configured.
#[must_use]
pub fn discovery_settings_from_env() -> Option<DiscoverySettings> {
    let registry_endpoint = std::env::var(RELAY_REGISTRY_ENDPOINT_ENV)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())?;
    let skills = configured_skills_from_env();
    let ttl_seconds = std::env::var(DAEMON_REGISTRATION_TTL_ENV)
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(DEFAULT_REGISTRATION_TTL_SECONDS);
    let registration = if skills.is_empty() {
        None
    } else {
        Some(RegistrationSettings {
            skills,
            name: std::env::var(DAEMON_NAME_ENV).unwrap_or_else(|_| "keryx-daemon".into()),
            description: std::env::var(DAEMON_DESCRIPTION_ENV).unwrap_or_default(),
            ttl_seconds,
            refresh_interval: RegistrationSettings::refresh_interval_for_ttl(ttl_seconds),
        })
    };
    Some(DiscoverySettings {
        registry_endpoint,
        registration,
    })
}

fn configured_skills_from_env() -> Vec<ConfiguredSkill> {
    let Ok(raw) = std::env::var(DAEMON_SKILLS_ENV) else {
        return Vec::new();
    };
    raw.split(',')
        .map(str::trim)
        .filter(|part| !part.is_empty())
        .map(|skill_id| ConfiguredSkill {
            skill_id: skill_id.to_string(),
            description: String::new(),
            tags: Vec::new(),
        })
        .collect()
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoreReadinessReport {
    pub kind: &'static str,
    pub path: PathBuf,
    pub ready: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KeryxStatusReport {
    pub daemon_ready: bool,
    pub data_dir: PathBuf,
    pub db_path: PathBuf,
    pub schema_version: i64,
    pub supported_schema_version: i64,
    pub recovered_tasks: usize,
    pub cleaned_terminal_leases: usize,
    pub corruption_count: usize,
    pub startup_recovery_duration_ms: u128,
    pub store: StoreReadinessReport,
    pub max_pending_tasks: u64,
    pub max_envelope_bytes: u64,
    pub current_pending_tasks: Option<u64>,
    pub deadline_enforcement_interval_ms: u64,
    pub cancellation: CancellationSnapshot,
    pub warnings: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DoctorCheck {
    pub name: &'static str,
    pub ready: bool,
    pub detail: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KeryxDoctorReport {
    pub healthy: bool,
    pub status: KeryxStatusReport,
    pub checks: Vec<DoctorCheck>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DynamicReadiness {
    pub ready: bool,
    pub not_ready_reasons: Vec<String>,
}

impl DynamicReadiness {
    #[must_use]
    pub const fn ready() -> Self {
        Self {
            ready: true,
            not_ready_reasons: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StartupReport {
    pub schema_version: i64,
    pub supported_schema_version: i64,
    pub db_path: PathBuf,
    pub recovery: RecoveryReport,
    pub startup_recovery_duration_ms: u128,
}

#[derive(Debug, Clone)]
pub struct KeryxDaemonRuntime {
    config: KeryxDaemonConfig,
    store: SqliteStore,
    report: StartupReport,
    metrics: Arc<KeryxMetrics>,
    readiness: Arc<RwLock<DynamicReadiness>>,
    shutdown: Arc<ShutdownState>,
    router: Arc<TaskRouter>,
    discovery: Arc<RwLock<Option<Arc<DiscoveryHandle>>>>,
    submit_backpressure_lock: Arc<Mutex<()>>,
    cancellation: Arc<CancellationState>,
}

impl KeryxDaemonRuntime {
    /// Start the local daemon runtime enough to prove crash-recovery semantics:
    /// create/open the SQLite store, run migrations, recover stale leases, then
    /// expose a readiness report. Real RPC/task loops will layer on top of this.
    pub async fn startup(config: KeryxDaemonConfig) -> StoreResult<Self> {
        let startup_recovery_started_at = std::time::Instant::now();
        std::fs::create_dir_all(config.data_dir())?;
        let db_path = config.db_path();
        let store = SqliteStore::connect(&db_path).await?;
        store.migrate().await?;
        let schema_version = store.schema_version().await?;
        let recovery = store
            .recover_stale_leases(config.startup_recovery_now_ms(), None)
            .await?;
        if recovery.corruption_count() > 0 {
            return Err(StoreError::UnrepairedCorruption {
                corrupted_tasks: recovery.corrupted_tasks.clone(),
            });
        }
        let report = StartupReport {
            schema_version,
            supported_schema_version: CURRENT_SCHEMA_VERSION,
            db_path,
            recovery,
            startup_recovery_duration_ms: startup_recovery_started_at.elapsed().as_millis(),
        };
        let peer_directory = Arc::new(PeerDirectory::new(config.local_peer_id().clone()));
        let publisher: Arc<dyn RelayTaskPublisher> = config.relay_endpoint().map_or_else(
            || Arc::new(NoopRelayPublisher) as Arc<dyn RelayTaskPublisher>,
            |endpoint| {
                Arc::new(GrpcRelayTaskPublisher::new(endpoint)) as Arc<dyn RelayTaskPublisher>
            },
        );
        let router = Arc::new(TaskRouter::new(
            peer_directory,
            publisher,
            config.send_task_timeout_ms(),
        ));
        let discovery = Arc::new(RwLock::new(None));
        let runtime = Self {
            config,
            store,
            report,
            metrics: Arc::new(KeryxMetrics::new()),
            readiness: Arc::new(RwLock::new(DynamicReadiness::ready())),
            shutdown: Arc::new(ShutdownState::new()),
            router,
            discovery,
            submit_backpressure_lock: Arc::new(Mutex::new(())),
            cancellation: Arc::new(CancellationState::new()),
        };
        if let Some(settings) = runtime.config.discovery.clone() {
            runtime
                .attach_discovery(settings)
                .await
                .map_err(|status| StoreError::Database(status.message().to_string()))?;
        }
        Ok(runtime)
    }

    /// Connect to the relay registry, start TTL refresh registration, and enable discovery RPC.
    pub async fn attach_discovery(&self, settings: DiscoverySettings) -> Result<(), Status> {
        let handle =
            DiscoveryHandle::connect(&settings, self.config.local_peer_id().clone()).await?;
        handle.start_registration_loop().await?;
        *self.discovery.write().await = Some(Arc::new(handle));
        Ok(())
    }

    pub async fn discover_skills(
        &self,
        request: DiscoverSkillsRequest,
    ) -> Result<DiscoverSkillsResponse, Status> {
        let guard = self.discovery.read().await;
        let Some(handle) = guard.as_ref() else {
            return Err(Status::unavailable(
                "relay skill registry is not configured",
            ));
        };
        let response = handle.discover(request).await?;
        if self.config.relay_endpoint().is_some() {
            for registration in &response.registrations {
                if let Ok(peer_id) = PeerId::new(registration.peer_id.trim()) {
                    self.router.set_peer_routable(&peer_id, true).await;
                }
            }
        }
        Ok(response)
    }

    /// Reject new RPC handlers while keeping the listener up (used by integration tests).
    pub fn mark_shutting_down(&self) {
        self.shutdown.mark_shutting_down();
    }

    /// Mark the daemon as shutting down and signal the gRPC server to stop accepting
    /// new connections. Does not close the store; pair with [`shutdown`](Self::shutdown).
    pub fn initiate_shutdown(&self) {
        tracing::info!(component = "keryxd", "graceful shutdown initiated");
        self.shutdown.initiate();
    }

    /// Drain in-flight RPCs (bounded by config timeout), close the store, and log completion.
    pub async fn shutdown(self: Arc<Self>) -> StoreResult<()> {
        let started = Instant::now();
        self.initiate_shutdown();

        let timeout = Duration::from_millis(self.config.shutdown_timeout_ms());
        let deadline = started + timeout;
        loop {
            let in_flight = self.shutdown.in_flight();
            if in_flight == 0 {
                break;
            }
            if Instant::now() >= deadline {
                warn!(
                    component = "keryxd",
                    in_flight_rpcs = in_flight,
                    shutdown_timeout_ms = self.config.shutdown_timeout_ms(),
                    "shutdown drain timed out with in-flight RPCs remaining"
                );
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        if let Some(handle) = self.discovery.write().await.take() {
            handle.shutdown().await;
        }

        self.store.close().await;
        tracing::info!(
            component = "keryxd",
            duration_ms = started.elapsed().as_millis() as u64,
            in_flight_rpcs_at_close = self.shutdown.in_flight(),
            "daemon shutdown complete"
        );
        Ok(())
    }

    #[must_use]
    pub fn readiness_handle(&self) -> &Arc<RwLock<DynamicReadiness>> {
        &self.readiness
    }

    pub async fn readiness_snapshot(&self) -> DynamicReadiness {
        self.readiness.read().await.clone()
    }

    /// Re-run store health probes and refresh the cached readiness snapshot.
    pub async fn refresh_readiness(&self) {
        let snapshot = probe_store_readiness(&self.store).await;
        *self.readiness.write().await = snapshot;
    }

    pub async fn status_report(&self) -> StoreResult<KeryxStatusReport> {
        let mut warnings = Vec::new();
        let current_pending_tasks = match self
            .store
            .count_tasks_by_status(TaskStatus::Pending)
            .await
        {
            Ok(count) => Some(count),
            Err(error) => {
                let warning = format!("pending task count unavailable: {error}");
                warn!(component = "keryxd", error = %error, "status report continuing without pending task count");
                warnings.push(warning);
                None
            }
        };
        Ok(KeryxStatusReport {
            daemon_ready: true,
            data_dir: self.config.data_dir().to_path_buf(),
            db_path: self.report.db_path.clone(),
            schema_version: self.report.schema_version,
            supported_schema_version: self.report.supported_schema_version,
            recovered_tasks: self.report.recovery.recovered_task_count(),
            cleaned_terminal_leases: self.report.recovery.cleaned_terminal_leases,
            corruption_count: self.report.recovery.corruption_count(),
            startup_recovery_duration_ms: self.report.startup_recovery_duration_ms,
            store: StoreReadinessReport {
                kind: "sqlite",
                path: self.report.db_path.clone(),
                ready: true,
            },
            max_pending_tasks: self.config.limits().max_pending_tasks,
            max_envelope_bytes: self.config.limits().max_envelope_bytes,
            current_pending_tasks,
            deadline_enforcement_interval_ms: self.config.deadline_enforcement_interval_ms(),
            cancellation: self.cancellation.snapshot(),
            warnings,
        })
    }

    pub async fn doctor_report(&self) -> StoreResult<KeryxDoctorReport> {
        let status = self.status_report().await?;
        let limits_ready = status.current_pending_tasks.is_some_and(|pending| {
            status.max_pending_tasks == 0 || pending < status.max_pending_tasks
        });
        let mut checks = vec![
            DoctorCheck {
                name: "data_dir",
                ready: status.data_dir.is_dir(),
                detail: format!("data_dir={}", status.data_dir.display()),
            },
            DoctorCheck {
                name: "sqlite_store",
                ready: status.store.ready && status.db_path.is_file(),
                detail: format!(
                    "kind={} path={}",
                    status.store.kind,
                    status.store.path.display(),
                ),
            },
            DoctorCheck {
                name: "schema_version",
                ready: status.schema_version == status.supported_schema_version,
                detail: format!(
                    "schema_version={} supported_schema_version={}",
                    status.schema_version, status.supported_schema_version
                ),
            },
            DoctorCheck {
                name: "startup_recovery",
                ready: status.corruption_count == 0,
                detail: format!(
                    "recovered_tasks={} cleaned_terminal_leases={} corruption_count={} duration_ms={}",
                    status.recovered_tasks,
                    status.cleaned_terminal_leases,
                    status.corruption_count,
                    status.startup_recovery_duration_ms
                ),
            },
        ];
        checks.push(DoctorCheck {
            name: "limits",
            ready: limits_ready,
            detail: limits_detail(&status),
        });
        checks.push(DoctorCheck {
            name: "cancellation",
            ready: true,
            detail: cancellation_detail(&status),
        });
        let healthy = status.daemon_ready && checks.iter().all(|check| check.ready);
        Ok(KeryxDoctorReport {
            healthy,
            status,
            checks,
        })
    }

    #[must_use]
    pub const fn config(&self) -> &KeryxDaemonConfig {
        &self.config
    }

    #[must_use]
    pub const fn store(&self) -> &SqliteStore {
        &self.store
    }

    pub async fn accept_pending_task_with_backpressure(
        &self,
        record: TaskRecord,
        envelope_bytes: u64,
    ) -> StoreResult<TaskRecord> {
        self.config
            .limits()
            .check_envelope_bytes(envelope_bytes)
            .map_err(|error| StoreError::Validation(error.into()))?;
        let _submit_backpressure_guard = self.submit_backpressure_lock.lock().await;
        let pending_count = self
            .store
            .count_tasks_by_status(TaskStatus::Pending)
            .await?;
        self.config
            .limits()
            .check_pending_tasks(pending_count)
            .map_err(|error| StoreError::Validation(error.into()))?;
        self.store.accept_task(record).await
    }

    #[must_use]
    pub const fn report(&self) -> &StartupReport {
        &self.report
    }

    #[must_use]
    pub fn metrics(&self) -> &Arc<KeryxMetrics> {
        &self.metrics
    }

    #[must_use]
    pub fn metrics_snapshot(&self) -> MetricsSnapshot {
        self.metrics.snapshot()
    }

    #[must_use]
    pub fn cancellation(&self) -> &Arc<CancellationState> {
        &self.cancellation
    }

    #[must_use]
    pub fn cancellation_snapshot(&self) -> CancellationSnapshot {
        self.cancellation.snapshot()
    }

    /// Spawn the background stale-lease recovery loop (see [`LeaseRecoveryLoop`]).
    #[must_use]
    pub fn spawn_lease_recovery_loop(self: &Arc<Self>) -> LeaseRecoveryLoopHandle {
        LeaseRecoveryLoop::spawn(Arc::clone(self))
    }

    /// Spawn the background deadline enforcement loop (see [`DeadlineEnforcementLoop`]).
    #[must_use]
    pub fn spawn_deadline_enforcement_loop(self: &Arc<Self>) -> DeadlineEnforcementLoopHandle {
        DeadlineEnforcementLoop::spawn(Arc::clone(self))
    }

    /// Spawn the background store health probe loop (see [`HealthLoop`]).
    #[must_use]
    pub fn spawn_health_loop(self: &Arc<Self>) -> HealthLoopHandle {
        HealthLoop::spawn(Arc::clone(self))
    }

    /// Spawn the relay incoming-task loop (see [`IncomingTaskLoop`]).
    #[must_use]
    pub fn spawn_incoming_task_loop(
        self: &Arc<Self>,
        allowlist: Arc<dyn SenderAllowlist>,
        dispatch: IncomingDispatchConfig,
        source: tokio::sync::mpsc::Receiver<IncomingRelayTask>,
    ) -> IncomingTaskLoopHandle {
        IncomingTaskLoop::spawn(Arc::clone(self), allowlist, dispatch, source)
    }

    #[must_use]
    pub fn router(&self) -> &Arc<TaskRouter> {
        &self.router
    }

    #[must_use]
    pub fn shutdown_is_active(&self) -> bool {
        self.shutdown.is_shutting_down()
    }
}

#[derive(Debug, Clone)]
pub struct KeryxDaemonRpcService {
    runtime: Arc<KeryxDaemonRuntime>,
}

impl KeryxDaemonRpcService {
    #[must_use]
    pub fn new(runtime: KeryxDaemonRuntime) -> Self {
        Self {
            runtime: Arc::new(runtime),
        }
    }
}

/// Serve the minimal local daemon RPC surface used by the CLI readiness client.
pub async fn serve_daemon_rpc(
    runtime: KeryxDaemonRuntime,
    incoming: TcpListenerStream,
) -> Result<(), tonic::transport::Error> {
    let shutdown_signal = runtime.shutdown.grpc_shutdown_wait();
    tonic::transport::Server::builder()
        .add_service(
            KeryxDaemonServer::new(KeryxDaemonRpcService::new(runtime))
                .max_decoding_message_size(ARTIFACT_RPC_MAX_BYTES)
                .max_encoding_message_size(ARTIFACT_RPC_MAX_BYTES),
        )
        .serve_with_incoming_shutdown(incoming, shutdown_signal)
        .await
}

#[tonic::async_trait]
impl KeryxDaemon for KeryxDaemonRpcService {
    #[instrument(name = "keryx::rpc::status", skip(self))]
    async fn status(
        &self,
        _request: Request<StatusRequest>,
    ) -> Result<Response<StatusResponse>, Status> {
        let _rpc = RpcInFlightGuard::enter(&self.runtime)?;
        test_rpc_delay().await;
        let report = self
            .runtime
            .status_report()
            .await
            .map_err(store_error_to_status)?;
        let metrics = self.runtime.metrics_snapshot();
        let status = if report.daemon_ready {
            "ready"
        } else {
            "not-ready"
        };
        Ok(Response::new(StatusResponse {
            status: status.to_string(),
            data_dir: report.data_dir.display().to_string(),
            db_path: report.db_path.display().to_string(),
            schema_version: report.schema_version,
            supported_schema_version: report.supported_schema_version,
            recovered_tasks: report.recovered_tasks as u64,
            cleaned_terminal_leases: report.cleaned_terminal_leases as u64,
            corruption_count: report.corruption_count as u64,
            startup_recovery_duration_ms: report.startup_recovery_duration_ms as u64,
            store_kind: report.store.kind.to_string(),
            store_ready: report.store.ready,
            store_path: report.store.path.display().to_string(),
            tasks_submitted: metrics.tasks_submitted,
            tasks_claimed: metrics.tasks_claimed,
            tasks_completed: metrics.tasks_completed,
            tasks_failed: metrics.tasks_failed,
            heartbeats: metrics.heartbeats,
            leases_recovered: metrics.leases_recovered,
            recovery_ticks: metrics.recovery_ticks,
            active_leases: metrics.active_leases,
            dead_letters: metrics.dead_letters,
            max_pending_tasks: report.max_pending_tasks,
            max_envelope_bytes: report.max_envelope_bytes,
            current_pending_tasks: report.current_pending_tasks,
            warnings: report.warnings,
            cancel_requests: report.cancellation.cancel_requests,
            tasks_canceled: report.cancellation.tasks_canceled,
            deadline_ticks: report.cancellation.deadline_ticks,
            deadline_failures: report.cancellation.deadline_failures,
            last_deadline_scan_ms: report.cancellation.last_deadline_scan_ms,
            last_deadline_failures: report.cancellation.last_deadline_failures,
            deadline_enforcement_interval_ms: report.deadline_enforcement_interval_ms,
        }))
    }

    #[instrument(name = "keryx::rpc::doctor", skip(self))]
    async fn doctor(
        &self,
        _request: Request<DoctorRequest>,
    ) -> Result<Response<DoctorResponse>, Status> {
        let _rpc = RpcInFlightGuard::enter(&self.runtime)?;
        let report = self
            .runtime
            .doctor_report()
            .await
            .map_err(store_error_to_status)?;
        let status = if report.healthy { "pass" } else { "fail" };
        let messages = report
            .checks
            .iter()
            .map(|check| {
                let marker = if check.ready { "ok" } else { "fail" };
                format!("[{marker}] {} - {}", check.name, check.detail)
            })
            .collect();
        Ok(Response::new(DoctorResponse {
            status: status.to_string(),
            messages,
        }))
    }

    #[instrument(name = "keryx::rpc::liveness", skip(self))]
    async fn liveness(
        &self,
        _request: Request<LivenessRequest>,
    ) -> Result<Response<LivenessResponse>, Status> {
        let _rpc = RpcInFlightGuard::enter(&self.runtime)?;
        Ok(Response::new(LivenessResponse { alive: true }))
    }

    #[instrument(name = "keryx::rpc::readiness", skip(self))]
    async fn readiness(
        &self,
        _request: Request<ReadinessRequest>,
    ) -> Result<Response<ReadinessResponse>, Status> {
        let _rpc = RpcInFlightGuard::enter(&self.runtime)?;
        let snapshot = self.runtime.readiness_snapshot().await;
        Ok(Response::new(ReadinessResponse {
            ready: snapshot.ready,
            not_ready_reasons: snapshot.not_ready_reasons,
        }))
    }

    #[instrument(
        name = "keryx::rpc::submit_task",
        skip(self, request),
        fields(task_id = tracing::field::Empty)
    )]
    async fn submit_task(
        &self,
        request: Request<SubmitTaskRequest>,
    ) -> Result<Response<SubmitTaskResponse>, Status> {
        let _rpc = RpcInFlightGuard::enter(&self.runtime)?;
        let envelope = request
            .into_inner()
            .envelope
            .ok_or_else(|| Status::invalid_argument("envelope is required"))?;
        let task_id = parse_required_task_id(envelope.task_id.as_ref())?;
        tracing::Span::current().record("task_id", tracing::field::display(task_id.as_str()));
        let idempotency_key = parse_optional_idempotency_key(envelope.idempotency_key.as_ref())?;
        let envelope_bytes = envelope.encoded_len() as u64;
        self.runtime
            .config()
            .limits()
            .check_envelope_bytes(envelope_bytes)
            .map_err(limit_exceeded_to_status)?;
        let record = TaskRecord::new(task_id.clone(), TaskStatus::Pending, idempotency_key);
        let _submit_backpressure_guard = self.runtime.submit_backpressure_lock.lock().await;
        let pending_count = self
            .runtime
            .store()
            .count_tasks_by_status(TaskStatus::Pending)
            .await
            .map_err(store_error_to_status)?;
        self.runtime
            .config()
            .limits()
            .check_pending_tasks(pending_count)
            .map_err(limit_exceeded_to_status)?;
        let accepted = self
            .runtime
            .store()
            .accept_task(record)
            .await
            .map_err(store_error_to_status)?;
        self.runtime.metrics().increment_tasks_submitted();
        Ok(Response::new(SubmitTaskResponse {
            task_id: Some(proto_task_id(accepted.task_id())),
            status: task_status_label(accepted.status).to_string(),
        }))
    }

    #[instrument(
        name = "keryx::rpc::claim_task",
        skip(self, request),
        fields(task_id = tracing::field::Empty, worker_id = tracing::field::Empty)
    )]
    async fn claim_task(
        &self,
        request: Request<ClaimTaskRequest>,
    ) -> Result<Response<ClaimTaskResponse>, Status> {
        let _rpc = RpcInFlightGuard::enter(&self.runtime)?;
        let inner = request.into_inner();
        let task_id = parse_required_task_id(inner.task_id.as_ref())?;
        let worker_id = parse_required_agent_id(inner.worker_id.as_ref())?;
        tracing::Span::current().record("task_id", tracing::field::display(task_id.as_str()));
        tracing::Span::current().record("worker_id", tracing::field::display(worker_id.as_str()));
        let lease_duration_ms =
            normalize_lease_duration_ms(inner.lease_duration_ms, self.runtime.config());
        let leased_at_ms = unix_ms_now();
        let expires_at_ms = leased_at_ms.saturating_add(lease_duration_ms);
        let lease_id = new_lease_id(&task_id, leased_at_ms);
        let lease = LeaseRecord::new(
            lease_id.clone(),
            task_id.clone(),
            worker_id.clone(),
            leased_at_ms,
            expires_at_ms,
        );
        let task = self
            .runtime
            .store()
            .lease_task(&task_id, lease)
            .await
            .map_err(store_error_to_status)?;
        self.runtime.metrics().increment_tasks_claimed();
        Ok(Response::new(ClaimTaskResponse {
            task_id: Some(proto_task_id(task.task_id())),
            lease_id: Some(proto_lease_id(&lease_id)),
            worker_id: Some(proto_agent_id(&worker_id)),
            leased_at_ms,
            expires_at_ms,
            status: task_status_label(task.status).to_string(),
            retry_count: task.retry_count,
            dead_lettered: task.dead_lettered,
        }))
    }

    #[instrument(
        name = "keryx::rpc::heartbeat",
        skip(self, request),
        fields(
            task_id = tracing::field::Empty,
            lease_id = tracing::field::Empty,
            worker_id = tracing::field::Empty
        )
    )]
    async fn heartbeat(
        &self,
        request: Request<HeartbeatRequest>,
    ) -> Result<Response<HeartbeatResponse>, Status> {
        let _rpc = RpcInFlightGuard::enter(&self.runtime)?;
        let inner = request.into_inner();
        let task_id = parse_required_task_id(inner.task_id.as_ref())?;
        let lease_id = parse_required_lease_id(inner.lease_id.as_ref())?;
        let worker_id = parse_required_agent_id(inner.worker_id.as_ref())?;
        tracing::Span::current().record("task_id", tracing::field::display(task_id.as_str()));
        tracing::Span::current().record("lease_id", tracing::field::display(lease_id.as_str()));
        tracing::Span::current().record("worker_id", tracing::field::display(worker_id.as_str()));
        let lease_duration_ms =
            normalize_lease_duration_ms(inner.lease_duration_ms, self.runtime.config());
        let now_ms = unix_ms_now();
        let new_expires_at_ms = now_ms.saturating_add(lease_duration_ms);
        let renewed = self
            .runtime
            .store()
            .renew_lease(&task_id, &lease_id, &worker_id, now_ms, new_expires_at_ms)
            .await
            .map_err(store_error_to_status)?;
        self.runtime.metrics().increment_heartbeats();
        Ok(Response::new(HeartbeatResponse {
            lease_id: Some(proto_lease_id(&renewed.lease_id)),
            expires_at_ms: renewed.expires_at_ms,
        }))
    }

    #[instrument(
        name = "keryx::rpc::complete_task",
        skip(self, request),
        fields(
            task_id = tracing::field::Empty,
            lease_id = tracing::field::Empty,
            worker_id = tracing::field::Empty
        )
    )]
    async fn complete_task(
        &self,
        request: Request<CompleteTaskRequest>,
    ) -> Result<Response<CompleteTaskResponse>, Status> {
        let _rpc = RpcInFlightGuard::enter(&self.runtime)?;
        let inner = request.into_inner();
        let task_id = parse_required_task_id(inner.task_id.as_ref())?;
        let lease_id = parse_required_lease_id(inner.lease_id.as_ref())?;
        let worker_id = parse_required_agent_id(inner.worker_id.as_ref())?;
        tracing::Span::current().record("task_id", tracing::field::display(task_id.as_str()));
        tracing::Span::current().record("lease_id", tracing::field::display(lease_id.as_str()));
        tracing::Span::current().record("worker_id", tracing::field::display(worker_id.as_str()));
        let task = self
            .runtime
            .store()
            .complete_task(&task_id, &lease_id, &worker_id)
            .await
            .map_err(store_error_to_status)?;
        self.runtime.metrics().increment_tasks_completed();
        Ok(Response::new(CompleteTaskResponse {
            task_id: Some(proto_task_id(task.task_id())),
            status: task_status_label(task.status).to_string(),
            duration_ms: inner.duration_ms,
            result_metadata: inner.result_metadata,
            output_artifacts: inner.output_artifacts,
        }))
    }

    #[instrument(
        name = "keryx::rpc::fail_task",
        skip(self, request),
        fields(
            task_id = tracing::field::Empty,
            lease_id = tracing::field::Empty,
            worker_id = tracing::field::Empty,
            error_reason = tracing::field::Empty
        )
    )]
    async fn fail_task(
        &self,
        request: Request<FailTaskRequest>,
    ) -> Result<Response<FailTaskResponse>, Status> {
        let _rpc = RpcInFlightGuard::enter(&self.runtime)?;
        let inner = request.into_inner();
        let task_id = parse_required_task_id(inner.task_id.as_ref())?;
        let lease_id = parse_required_lease_id(inner.lease_id.as_ref())?;
        let worker_id = parse_required_agent_id(inner.worker_id.as_ref())?;
        let error_reason = inner.error_reason.clone();
        tracing::Span::current().record("task_id", tracing::field::display(task_id.as_str()));
        tracing::Span::current().record("lease_id", tracing::field::display(lease_id.as_str()));
        tracing::Span::current().record("worker_id", tracing::field::display(worker_id.as_str()));
        tracing::Span::current().record("error_reason", tracing::field::display(&error_reason));
        let policy = self.runtime.config().fail_retry_policy();
        let task = self
            .runtime
            .store()
            .fail_task(&task_id, &lease_id, &worker_id, &error_reason, &policy)
            .await
            .map_err(store_error_to_status)?;
        self.runtime.metrics().increment_tasks_failed();
        if task.dead_lettered {
            self.runtime.metrics().increment_dead_letters();
        }
        Ok(Response::new(FailTaskResponse {
            task_id: Some(proto_task_id(task.task_id())),
            status: task_status_label(task.status).to_string(),
            duration_ms: inner.duration_ms,
            error_reason,
            failure_metadata: inner.failure_metadata,
            retry_count: task.retry_count,
            dead_lettered: task.dead_lettered,
        }))
    }

    #[instrument(
        name = "keryx::rpc::cancel_task",
        skip(self, request),
        fields(task_id = tracing::field::Empty, lease_id = tracing::field::Empty, worker_id = tracing::field::Empty, reason = tracing::field::Empty)
    )]
    async fn cancel_task(
        &self,
        request: Request<CancelTaskRequest>,
    ) -> Result<Response<CancelTaskResponse>, Status> {
        let _rpc = RpcInFlightGuard::enter(&self.runtime)?;
        let inner = request.into_inner();
        let task_id = parse_required_task_id(inner.task_id.as_ref())?;
        let reason = normalized_cancel_reason(&inner.reason);
        let lease_id = parse_optional_lease_id(inner.lease_id.as_ref())?;
        let worker_id = parse_optional_agent_id(inner.worker_id.as_ref())?;
        tracing::Span::current().record("task_id", tracing::field::display(task_id.as_str()));
        if let Some(lease_id) = lease_id.as_ref() {
            tracing::Span::current().record("lease_id", tracing::field::display(lease_id.as_str()));
        }
        if let Some(worker_id) = worker_id.as_ref() {
            tracing::Span::current()
                .record("worker_id", tracing::field::display(worker_id.as_str()));
        }
        tracing::Span::current().record("reason", tracing::field::display(&reason));
        self.runtime.cancellation().increment_cancel_requests();
        let task = self
            .runtime
            .store()
            .cancel_task(
                &task_id,
                lease_id.as_ref(),
                worker_id.as_ref(),
                &reason,
                unix_ms_now(),
            )
            .await
            .map_err(store_error_to_status)?;
        self.runtime.cancellation().increment_tasks_canceled();
        self.runtime.metrics().increment_tasks_failed();
        Ok(Response::new(CancelTaskResponse {
            task_id: Some(proto_task_id(task.task_id())),
            status: task_status_label(task.status).to_string(),
            reason,
            canceled: true,
        }))
    }

    #[instrument(
        name = "keryx::rpc::put_artifact",
        skip(self, request),
        fields(task_id = tracing::field::Empty, artifact_id = tracing::field::Empty)
    )]
    async fn put_artifact(
        &self,
        request: Request<PutArtifactRequest>,
    ) -> Result<Response<PutArtifactResponse>, Status> {
        let _rpc = RpcInFlightGuard::enter(&self.runtime)?;
        let inner = request.into_inner();
        let task_id = parse_required_task_id(inner.task_id.as_ref())?;
        tracing::Span::current().record("task_id", tracing::field::display(task_id.as_str()));
        let artifact_id = parse_or_generate_artifact_id(inner.artifact_id.as_ref())?;
        tracing::Span::current()
            .record("artifact_id", tracing::field::display(artifact_id.as_str()));
        let digest = Digest::compute(&inner.content);
        let media_type = MediaType::new(inner.media_type);
        let byte_len = inner.content.len() as u64;
        let meta = ArtifactMeta {
            artifact_id,
            task_id,
            digest,
            media_type,
            byte_len,
            inline: should_inline(byte_len),
            created_at: unix_ms_now().to_string(),
        };
        let record = self
            .runtime
            .store()
            .put_artifact(&meta, &inner.content, self.runtime.config().blob_dir())
            .await
            .map_err(store_error_to_status)?;
        Ok(Response::new(PutArtifactResponse {
            artifact_id: Some(proto_artifact_id(&record.artifact_id)),
            task_id: Some(proto_task_id(&record.task_id)),
            digest: record.digest.as_str().to_string(),
            media_type: record.media_type.as_str().to_string(),
            byte_len: record.byte_len,
            inline: record.inline,
            created_at: record.created_at,
        }))
    }

    #[instrument(
        name = "keryx::rpc::get_artifact",
        skip(self, request),
        fields(artifact_id = tracing::field::Empty)
    )]
    async fn get_artifact(
        &self,
        request: Request<GetArtifactRequest>,
    ) -> Result<Response<GetArtifactResponse>, Status> {
        let _rpc = RpcInFlightGuard::enter(&self.runtime)?;
        let inner = request.into_inner();
        let artifact_id = parse_required_artifact_id(inner.artifact_id.as_ref())?;
        tracing::Span::current()
            .record("artifact_id", tracing::field::display(artifact_id.as_str()));
        let (record, content) = self
            .runtime
            .store()
            .get_artifact(&artifact_id, self.runtime.config().blob_dir())
            .await
            .map_err(store_error_to_status)?;
        Ok(Response::new(GetArtifactResponse {
            artifact_id: Some(proto_artifact_id(&record.artifact_id)),
            task_id: Some(proto_task_id(&record.task_id)),
            digest: record.digest.as_str().to_string(),
            media_type: record.media_type.as_str().to_string(),
            byte_len: record.byte_len,
            inline: record.inline,
            created_at: record.created_at,
            content: if inner.metadata_only {
                Vec::new()
            } else {
                content
            },
        }))
    }

    #[instrument(
        name = "keryx::rpc::list_artifacts",
        skip(self, request),
        fields(task_id = tracing::field::Empty)
    )]
    async fn list_artifacts(
        &self,
        request: Request<ListArtifactsRequest>,
    ) -> Result<Response<ListArtifactsResponse>, Status> {
        let _rpc = RpcInFlightGuard::enter(&self.runtime)?;
        let task_id = parse_required_task_id(request.into_inner().task_id.as_ref())?;
        tracing::Span::current().record("task_id", tracing::field::display(task_id.as_str()));
        let artifacts = self
            .runtime
            .store()
            .list_artifacts_for_task(&task_id)
            .await
            .map_err(store_error_to_status)?
            .into_iter()
            .map(|record| proto_artifact_summary(&record))
            .collect();
        Ok(Response::new(ListArtifactsResponse { artifacts }))
    }

    #[instrument(
        name = "keryx::rpc::delete_artifact",
        skip(self, request),
        fields(artifact_id = tracing::field::Empty)
    )]
    async fn delete_artifact(
        &self,
        request: Request<DeleteArtifactRequest>,
    ) -> Result<Response<DeleteArtifactResponse>, Status> {
        let _rpc = RpcInFlightGuard::enter(&self.runtime)?;
        let artifact_id = parse_required_artifact_id(request.into_inner().artifact_id.as_ref())?;
        tracing::Span::current()
            .record("artifact_id", tracing::field::display(artifact_id.as_str()));
        match self
            .runtime
            .store()
            .delete_artifact(&artifact_id, self.runtime.config().blob_dir())
            .await
        {
            Ok(()) => Ok(Response::new(DeleteArtifactResponse { deleted: true })),
            Err(StoreError::ArtifactNotFound(_)) => {
                Ok(Response::new(DeleteArtifactResponse { deleted: false }))
            }
            Err(error) => Err(store_error_to_status(error)),
        }
    }

    #[instrument(name = "keryx::rpc::send_task", skip(self, request))]
    async fn send_task(
        &self,
        request: Request<SendTaskRequest>,
    ) -> Result<Response<SendTaskResponse>, Status> {
        let _rpc = RpcInFlightGuard::enter(&self.runtime)?;
        test_rpc_delay().await;
        let inner = request.into_inner();
        let envelope = inner
            .envelope
            .ok_or_else(|| Status::invalid_argument("envelope is required"))?;
        let trimmed = inner.target_peer_id.trim();
        if trimmed.is_empty() {
            return Err(Status::invalid_argument("target_peer_id is required"));
        }
        let target_peer_id =
            PeerId::new(trimmed).map_err(|error| Status::invalid_argument(error.to_string()))?;
        self.runtime
            .config()
            .limits()
            .check_envelope_bytes(envelope.encoded_len() as u64)
            .map_err(limit_exceeded_to_status)?;
        let _submit_backpressure_guard = if target_peer_id == *self.runtime.config().local_peer_id()
        {
            let guard = self.runtime.submit_backpressure_lock.lock().await;
            let pending_count = self
                .runtime
                .store()
                .count_tasks_by_status(TaskStatus::Pending)
                .await
                .map_err(store_error_to_status)?;
            self.runtime
                .config()
                .limits()
                .check_pending_tasks(pending_count)
                .map_err(limit_exceeded_to_status)?;
            Some(guard)
        } else {
            None
        };
        let outcome = self
            .runtime
            .router()
            .send_task(
                self.runtime.store(),
                target_peer_id,
                envelope,
                inner.timeout_ms,
            )
            .await
            .map_err(routing_error_to_status)?;
        if outcome.route == DeliveryRoute::Local {
            self.runtime.metrics().increment_tasks_submitted();
        }
        Ok(Response::new(SendTaskResponse {
            task_id: Some(proto_task_id(&outcome.task_id)),
            status: outcome.status,
            routed_to: outcome.routed_to.to_string(),
            delivery_route: outcome.route.as_str().to_string(),
        }))
    }

    #[instrument(name = "keryx::rpc::list_peers", skip(self))]
    async fn list_peers(
        &self,
        _request: Request<ListPeersRequest>,
    ) -> Result<Response<ListPeersResponse>, Status> {
        let _rpc = RpcInFlightGuard::enter(&self.runtime)?;
        test_rpc_delay().await;
        let peers = self.runtime.router().list_peers().await;
        let peers = peers
            .into_iter()
            .map(|peer| PeerDescriptor {
                peer_id: peer.peer_id.to_string(),
                connected: peer.connected,
                local: peer.local,
            })
            .collect();
        Ok(Response::new(ListPeersResponse { peers }))
    }

    #[instrument(name = "keryx::rpc::discover_skills", skip(self, request))]
    async fn discover_skills(
        &self,
        request: Request<DiscoverSkillsRequest>,
    ) -> Result<Response<DiscoverSkillsResponse>, Status> {
        let _rpc = RpcInFlightGuard::enter(&self.runtime)?;
        test_rpc_delay().await;
        let response = self.runtime.discover_skills(request.into_inner()).await?;
        Ok(Response::new(response))
    }
}

fn unix_ms_now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

fn normalize_lease_duration_ms(duration_ms: i64, config: &KeryxDaemonConfig) -> i64 {
    if duration_ms <= 0 {
        config.lease_default_ttl_ms()
    } else {
        duration_ms
    }
}

fn new_lease_id(task_id: &TaskId, leased_at_ms: i64) -> LeaseId {
    LeaseId::new(format!("lease-{}-{}", task_id.as_str(), leased_at_ms))
        .expect("daemon-generated lease id is valid")
}

fn task_status_label(status: TaskStatus) -> &'static str {
    match status {
        TaskStatus::Pending => "pending",
        TaskStatus::Running => "running",
        TaskStatus::Completed => "completed",
        TaskStatus::Failed => "failed",
    }
}

fn normalized_cancel_reason(reason: &str) -> String {
    let trimmed = reason.trim();
    if trimmed.is_empty() {
        "canceled by request".to_string()
    } else {
        trimmed.to_string()
    }
}

fn limit_label(limit: u64) -> String {
    if limit == 0 {
        "unlimited".to_string()
    } else {
        limit.to_string()
    }
}

fn pending_tasks_label(count: Option<u64>) -> String {
    count.map_or_else(|| "unknown".to_string(), |value| value.to_string())
}

fn limits_detail(status: &KeryxStatusReport) -> String {
    let mut detail = format!(
        "pending_tasks={}/{} envelope_bytes_limit={}",
        pending_tasks_label(status.current_pending_tasks),
        limit_label(status.max_pending_tasks),
        limit_label(status.max_envelope_bytes)
    );
    if !status.warnings.is_empty() {
        detail.push_str(" warnings=");
        detail.push_str(&status.warnings.join("; "));
    }
    detail
}

fn cancellation_detail(status: &KeryxStatusReport) -> String {
    let cancellation = status.cancellation;
    format!(
        "cancel_requests={} tasks_canceled={} deadline_ticks={} deadline_failures={} last_deadline_scan_ms={} last_deadline_failures={} deadline_interval_ms={}",
        cancellation.cancel_requests,
        cancellation.tasks_canceled,
        cancellation.deadline_ticks,
        cancellation.deadline_failures,
        cancellation.last_deadline_scan_ms,
        cancellation.last_deadline_failures,
        status.deadline_enforcement_interval_ms
    )
}

fn parse_required_task_id(id: Option<&ProtoTaskId>) -> Result<TaskId, Status> {
    let value = id
        .and_then(|id| {
            let trimmed = id.value.trim();
            if trimmed.is_empty() {
                None
            } else {
                Some(trimmed)
            }
        })
        .ok_or_else(|| Status::invalid_argument("task_id is required"))?;
    TaskId::new(value).map_err(|error| Status::invalid_argument(error.to_string()))
}

fn parse_required_agent_id(id: Option<&ProtoAgentId>) -> Result<AgentId, Status> {
    let value = id
        .and_then(|id| {
            let trimmed = id.value.trim();
            if trimmed.is_empty() {
                None
            } else {
                Some(trimmed)
            }
        })
        .ok_or_else(|| Status::invalid_argument("worker_id is required"))?;
    AgentId::new(value).map_err(|error| Status::invalid_argument(error.to_string()))
}

fn parse_required_lease_id(id: Option<&ProtoLeaseId>) -> Result<LeaseId, Status> {
    let value = id
        .and_then(|id| {
            let trimmed = id.value.trim();
            if trimmed.is_empty() {
                None
            } else {
                Some(trimmed)
            }
        })
        .ok_or_else(|| Status::invalid_argument("lease_id is required"))?;
    LeaseId::new(value).map_err(|error| Status::invalid_argument(error.to_string()))
}

fn parse_optional_agent_id(id: Option<&ProtoAgentId>) -> Result<Option<AgentId>, Status> {
    id.map(|id| parse_required_agent_id(Some(id))).transpose()
}

fn parse_optional_lease_id(id: Option<&ProtoLeaseId>) -> Result<Option<LeaseId>, Status> {
    id.map(|id| parse_required_lease_id(Some(id))).transpose()
}

fn parse_optional_idempotency_key(
    key: Option<&ProtoIdempotencyKey>,
) -> Result<Option<IdempotencyKey>, Status> {
    let Some(key) = key else {
        return Ok(None);
    };
    let trimmed = key.value.trim();
    if trimmed.is_empty() {
        return Ok(None);
    }
    IdempotencyKey::new(trimmed)
        .map(Some)
        .map_err(|error| Status::invalid_argument(error.to_string()))
}

fn proto_task_id(task_id: &TaskId) -> ProtoTaskId {
    ProtoTaskId {
        value: task_id.as_str().to_string(),
    }
}

fn proto_agent_id(worker_id: &AgentId) -> ProtoAgentId {
    ProtoAgentId {
        value: worker_id.as_str().to_string(),
    }
}

fn proto_lease_id(lease_id: &LeaseId) -> ProtoLeaseId {
    ProtoLeaseId {
        value: lease_id.as_str().to_string(),
    }
}

fn proto_artifact_id(artifact_id: &ArtifactId) -> ProtoArtifactId {
    ProtoArtifactId {
        value: artifact_id.as_str().to_string(),
    }
}

fn proto_artifact_summary(record: &keryx_store::ArtifactRecord) -> ArtifactSummary {
    ArtifactSummary {
        artifact_id: Some(proto_artifact_id(&record.artifact_id)),
        task_id: Some(proto_task_id(&record.task_id)),
        digest: record.digest.as_str().to_string(),
        media_type: record.media_type.as_str().to_string(),
        byte_len: record.byte_len,
        inline: record.inline,
        created_at: record.created_at.clone(),
    }
}

fn parse_required_artifact_id(id: Option<&ProtoArtifactId>) -> Result<ArtifactId, Status> {
    let value = id
        .and_then(|id| {
            let trimmed = id.value.trim();
            if trimmed.is_empty() {
                None
            } else {
                Some(trimmed)
            }
        })
        .ok_or_else(|| Status::invalid_argument("artifact_id is required"))?;
    ArtifactId::new(value).map_err(|error| Status::invalid_argument(error.to_string()))
}

fn parse_or_generate_artifact_id(id: Option<&ProtoArtifactId>) -> Result<ArtifactId, Status> {
    match id {
        Some(id) if !id.value.trim().is_empty() => parse_required_artifact_id(Some(id)),
        _ => ArtifactId::new(uuid::Uuid::new_v4().to_string())
            .map_err(|error| Status::internal(error.to_string())),
    }
}

pub(crate) fn store_error_to_status(error: StoreError) -> Status {
    let error_detail = error.to_string();
    let status = match error {
        StoreError::TaskNotFound(task_id) => {
            Status::not_found(format!("task not found: {task_id}"))
        }
        StoreError::TaskAlreadyExists(task_id) => {
            Status::already_exists(format!("task already exists: {task_id}"))
        }
        StoreError::ArtifactNotFound(artifact_id) => {
            Status::not_found(format!("artifact not found: {artifact_id}"))
        }
        StoreError::IdempotencyConflict {
            key,
            existing_task_id,
        } => Status::already_exists(format!(
            "idempotency key {} already belongs to task {}",
            key.as_str(),
            existing_task_id.as_str()
        )),
        StoreError::Validation(ValidationError::LimitExceeded { .. }) => {
            Status::resource_exhausted(error_detail.clone())
        }
        StoreError::Validation(error) => Status::failed_precondition(error.to_string()),
        StoreError::CorruptEventStream(task_id) => {
            Status::data_loss(format!("corrupt event stream for task {task_id}"))
        }
        StoreError::LeaseNotFound(task_id) => {
            Status::not_found(format!("lease not found for task {task_id}"))
        }
        StoreError::LeaseConflict { task_id } => {
            Status::aborted(format!("task {task_id} already has an active lease"))
        }
        StoreError::LeaseMismatch { task_id, lease_id } => Status::permission_denied(format!(
            "lease {} does not own task {}",
            lease_id.as_str(),
            task_id.as_str()
        )),
        StoreError::LeaseOwnerMismatch { task_id, worker_id } => {
            Status::permission_denied(format!(
                "worker {} does not own active lease for task {}",
                worker_id.as_str(),
                task_id.as_str()
            ))
        }
        StoreError::LeaseOwnerMissing { task_id, lease_id } => {
            Status::failed_precondition(format!(
                "lease {} for task {} is missing a worker owner",
                lease_id.as_str(),
                task_id.as_str()
            ))
        }
        StoreError::ArtifactTooLarge { .. } => Status::resource_exhausted(error_detail.clone()),
        StoreError::DigestMismatch { .. } => Status::data_loss(error_detail.clone()),
        StoreError::InvalidLeaseExpiry { .. } => Status::invalid_argument(error_detail.clone()),
        StoreError::UnsupportedSchema { .. }
        | StoreError::MigrationFailed(_)
        | StoreError::BlobDir(_)
        | StoreError::UnrepairedCorruption { .. } => Status::internal(error_detail.clone()),
        StoreError::LockPoisoned | StoreError::Database(_) => {
            Status::internal(error_detail.clone())
        }
    };
    match status.code() {
        Code::Internal | Code::DataLoss | Code::Unknown => {
            error!(
                error = %error_detail,
                grpc_code = ?status.code(),
                "rpc store error"
            );
        }
        _ => {
            warn!(
                error = %error_detail,
                grpc_code = ?status.code(),
                "rpc store error"
            );
        }
    }
    status
}

fn limit_exceeded_to_status(error: LimitExceeded) -> Status {
    store_error_to_status(StoreError::Validation(error.into()))
}
