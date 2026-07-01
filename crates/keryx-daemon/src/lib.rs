//! Local daemon startup/runtime foundation for Hermes Keryx.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use keryx_proto::v1::{
    keryx_daemon_server::{KeryxDaemon, KeryxDaemonServer},
    DoctorRequest, DoctorResponse, StatusRequest, StatusResponse,
};
use tokio_stream::wrappers::TcpListenerStream;
use tonic::{Request, Response, Status};

use keryx_store::{RecoveryReport, SqliteStore, StoreResult};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KeryxDaemonConfig {
    data_dir: PathBuf,
    startup_recovery_now_ms: i64,
}

impl KeryxDaemonConfig {
    #[must_use]
    pub fn new(data_dir: impl Into<PathBuf>, startup_recovery_now_ms: i64) -> Self {
        Self {
            data_dir: data_dir.into(),
            startup_recovery_now_ms,
        }
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
    pub const fn startup_recovery_now_ms(&self) -> i64 {
        self.startup_recovery_now_ms
    }
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
    pub recovered_tasks: usize,
    pub cleaned_terminal_leases: usize,
    pub corruption_count: usize,
    pub store: StoreReadinessReport,
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
pub struct StartupReport {
    pub schema_version: i64,
    pub db_path: PathBuf,
    pub recovery: RecoveryReport,
}

#[derive(Debug, Clone)]
pub struct KeryxDaemonRuntime {
    config: KeryxDaemonConfig,
    store: SqliteStore,
    report: StartupReport,
}

impl KeryxDaemonRuntime {
    /// Start the local daemon runtime enough to prove crash-recovery semantics:
    /// create/open the SQLite store, run migrations, recover stale leases, then
    /// expose a readiness report. Real RPC/task loops will layer on top of this.
    pub async fn startup(config: KeryxDaemonConfig) -> StoreResult<Self> {
        std::fs::create_dir_all(config.data_dir())?;
        let db_path = config.db_path();
        let store = SqliteStore::connect(&db_path).await?;
        store.migrate().await?;
        let schema_version = store.schema_version().await?;
        let recovery = store
            .recover_stale_leases(config.startup_recovery_now_ms(), None)
            .await?;
        let report = StartupReport {
            schema_version,
            db_path,
            recovery,
        };
        Ok(Self {
            config,
            store,
            report,
        })
    }

    #[must_use]
    pub fn status_report(&self) -> KeryxStatusReport {
        KeryxStatusReport {
            daemon_ready: true,
            data_dir: self.config.data_dir().to_path_buf(),
            db_path: self.report.db_path.clone(),
            schema_version: self.report.schema_version,
            recovered_tasks: self.report.recovery.recovered_task_count(),
            cleaned_terminal_leases: self.report.recovery.cleaned_terminal_leases,
            corruption_count: self.report.recovery.corruption_count(),
            store: StoreReadinessReport {
                kind: "sqlite",
                path: self.report.db_path.clone(),
                ready: true,
            },
        }
    }

    #[must_use]
    pub fn doctor_report(&self) -> KeryxDoctorReport {
        let status = self.status_report();
        let checks = vec![
            DoctorCheck {
                name: "data_dir",
                ready: status.data_dir.is_dir(),
                detail: format!("data_dir={}", status.data_dir.display()),
            },
            DoctorCheck {
                name: "sqlite_store",
                ready: status.store.ready && status.db_path.is_file() && status.schema_version > 0,
                detail: format!(
                    "kind={} path={} schema_version={}",
                    status.store.kind,
                    status.store.path.display(),
                    status.schema_version
                ),
            },
            DoctorCheck {
                name: "startup_recovery",
                ready: status.corruption_count == 0,
                detail: format!(
                    "recovered_tasks={} cleaned_terminal_leases={} corruption_count={}",
                    status.recovered_tasks, status.cleaned_terminal_leases, status.corruption_count
                ),
            },
        ];
        let healthy = status.daemon_ready && checks.iter().all(|check| check.ready);
        KeryxDoctorReport {
            healthy,
            status,
            checks,
        }
    }

    #[must_use]
    pub const fn config(&self) -> &KeryxDaemonConfig {
        &self.config
    }

    #[must_use]
    pub const fn store(&self) -> &SqliteStore {
        &self.store
    }

    #[must_use]
    pub const fn report(&self) -> &StartupReport {
        &self.report
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
    tonic::transport::Server::builder()
        .add_service(KeryxDaemonServer::new(KeryxDaemonRpcService::new(runtime)))
        .serve_with_incoming(incoming)
        .await
}

#[tonic::async_trait]
impl KeryxDaemon for KeryxDaemonRpcService {
    async fn status(
        &self,
        _request: Request<StatusRequest>,
    ) -> Result<Response<StatusResponse>, Status> {
        let report = self.runtime.status_report();
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
            recovered_tasks: report.recovered_tasks as u64,
            cleaned_terminal_leases: report.cleaned_terminal_leases as u64,
            corruption_count: report.corruption_count as u64,
            store_kind: report.store.kind.to_string(),
            store_ready: report.store.ready,
            store_path: report.store.path.display().to_string(),
        }))
    }

    async fn doctor(
        &self,
        _request: Request<DoctorRequest>,
    ) -> Result<Response<DoctorResponse>, Status> {
        let report = self.runtime.doctor_report();
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
}
