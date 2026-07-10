mod artifact;
mod node;
mod relay;

use anyhow::{Context, Result};
use artifact::ArtifactCommand;
use clap::{Parser, Subcommand};
use keryx_core::MAX_BLOB_BYTES;
use keryx_daemon::{KeryxDaemonConfig, KeryxDaemonRuntime, KeryxDoctorReport, KeryxStatusReport};
use keryx_proto::v1::{
    keryx_daemon_client::KeryxDaemonClient, AgentId, ClaimTaskRequest, CompleteTaskRequest,
    DoctorRequest, DoctorResponse, FailTaskRequest, HeartbeatRequest, LeaseId, StatusRequest,
    StatusResponse, SubmitTaskRequest, TaskEnvelope, TaskId,
};
use node::NodeCommand;
use relay::RelayCommand;

const DAEMON_ENDPOINT_ENV: &str = "HERMES_KERYX_DAEMON_ENDPOINT";
const ARTIFACT_RPC_MAX_BYTES: usize = MAX_BLOB_BYTES + (1024 * 1024);

#[derive(Debug, Parser)]
#[command(name = "keryx", about = "Hermes Keryx operator CLI")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Report readiness, querying HERMES_KERYX_DAEMON_ENDPOINT when set.
    Status,
    /// Run readiness checks, querying HERMES_KERYX_DAEMON_ENDPOINT when set.
    Doctor,
    /// Worker task lifecycle against the daemon RPC API.
    Task {
        #[command(subcommand)]
        command: TaskCommand,
    },
    /// Artifact storage operations against the daemon RPC API.
    Artifact {
        #[command(subcommand)]
        command: ArtifactCommand,
    },
    /// Relay server and registry operations.
    Relay {
        #[command(subcommand)]
        command: RelayCommand,
    },
    /// Edge node lifecycle and discovery.
    Node {
        #[command(subcommand)]
        command: NodeCommand,
    },
}

#[derive(Debug, Subcommand)]
enum TaskCommand {
    /// Enqueue a pending task by id.
    Submit { task_id: String },
    /// Claim a pending task and receive a lease.
    Claim {
        task_id: String,
        #[arg(long)]
        worker: String,
        #[arg(long)]
        lease_duration_ms: Option<i64>,
    },
    /// Mark a leased task completed.
    Complete {
        task_id: String,
        #[arg(long)]
        lease: String,
        #[arg(long)]
        worker: String,
        #[arg(long, default_value_t = 0)]
        duration_ms: i64,
    },
    /// Mark a leased task failed.
    Fail {
        task_id: String,
        #[arg(long)]
        lease: String,
        #[arg(long)]
        worker: String,
        #[arg(long)]
        reason: String,
        #[arg(long, default_value_t = 0)]
        duration_ms: i64,
    },
    /// Renew an active lease.
    Heartbeat {
        task_id: String,
        #[arg(long)]
        lease: String,
        #[arg(long)]
        worker: String,
        #[arg(long)]
        lease_duration_ms: Option<i64>,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    match Cli::parse().command {
        Command::Status => run_status().await?,
        Command::Doctor => run_doctor().await?,
        Command::Task { command } => run_task(command).await?,
        Command::Artifact { command } => artifact::run(command).await?,
        Command::Relay { command } => relay::run(command).await?,
        Command::Node { command } => node::run(command).await?,
    }
    Ok(())
}

async fn run_task(command: TaskCommand) -> Result<()> {
    let endpoint = require_daemon_endpoint()?;
    let mut client = connect_daemon(&endpoint, "keryx task").await?;
    match command {
        TaskCommand::Submit { task_id } => {
            let response = client
                .submit_task(SubmitTaskRequest {
                    envelope: Some(TaskEnvelope {
                        task_id: Some(TaskId {
                            value: task_id.clone(),
                        }),
                        correlation_id: None,
                        idempotency_key: None,
                        status: 0,
                        messages: vec![],
                        metadata: Default::default(),
                    }),
                })
                .await
                .with_context(|| format!("keryx task submit: RPC failed for task {task_id}"))?
                .into_inner();
            println!(
                "keryx task submit: task_id={} status={}",
                response.task_id.unwrap_or(TaskId { value: task_id }).value,
                response.status
            );
        }
        TaskCommand::Claim {
            task_id,
            worker,
            lease_duration_ms,
        } => {
            let response = client
                .claim_task(ClaimTaskRequest {
                    task_id: Some(TaskId {
                        value: task_id.clone(),
                    }),
                    worker_id: Some(AgentId {
                        value: worker.clone(),
                    }),
                    lease_duration_ms: lease_duration_ms.unwrap_or(0),
                })
                .await
                .with_context(|| format!("keryx task claim: RPC failed for task {task_id}"))?
                .into_inner();
            println!(
                "keryx task claim: task_id={} lease_id={} worker_id={} status={} expires_at_ms={}",
                response.task_id.unwrap().value,
                response.lease_id.unwrap().value,
                response.worker_id.unwrap().value,
                response.status,
                response.expires_at_ms
            );
        }
        TaskCommand::Complete {
            task_id,
            lease,
            worker,
            duration_ms,
        } => {
            let response = client
                .complete_task(CompleteTaskRequest {
                    task_id: Some(TaskId {
                        value: task_id.clone(),
                    }),
                    lease_id: Some(LeaseId { value: lease }),
                    worker_id: Some(AgentId {
                        value: worker.clone(),
                    }),
                    duration_ms,
                    result_metadata: Default::default(),
                    output_artifacts: vec![],
                })
                .await
                .with_context(|| format!("keryx task complete: RPC failed for task {task_id}"))?
                .into_inner();
            println!(
                "keryx task complete: task_id={} status={} duration_ms={}",
                response.task_id.unwrap().value,
                response.status,
                response.duration_ms
            );
        }
        TaskCommand::Fail {
            task_id,
            lease,
            worker,
            reason,
            duration_ms,
        } => {
            let response = client
                .fail_task(FailTaskRequest {
                    task_id: Some(TaskId {
                        value: task_id.clone(),
                    }),
                    lease_id: Some(LeaseId { value: lease }),
                    worker_id: Some(AgentId {
                        value: worker.clone(),
                    }),
                    duration_ms,
                    error_reason: reason.clone(),
                    failure_metadata: Default::default(),
                })
                .await
                .with_context(|| format!("keryx task fail: RPC failed for task {task_id}"))?
                .into_inner();
            println!(
                "keryx task fail: task_id={} status={} error_reason={} duration_ms={}",
                response.task_id.unwrap().value,
                response.status,
                response.error_reason,
                response.duration_ms
            );
        }
        TaskCommand::Heartbeat {
            task_id,
            lease,
            worker,
            lease_duration_ms,
        } => {
            let response = client
                .heartbeat(HeartbeatRequest {
                    task_id: Some(TaskId {
                        value: task_id.clone(),
                    }),
                    lease_id: Some(LeaseId { value: lease }),
                    worker_id: Some(AgentId {
                        value: worker.clone(),
                    }),
                    lease_duration_ms: lease_duration_ms.unwrap_or(0),
                })
                .await
                .with_context(|| format!("keryx task heartbeat: RPC failed for task {task_id}"))?
                .into_inner();
            println!(
                "keryx task heartbeat: lease_id={} expires_at_ms={}",
                response.lease_id.unwrap().value,
                response.expires_at_ms
            );
        }
    }
    Ok(())
}

fn require_daemon_endpoint() -> Result<String> {
    daemon_endpoint().ok_or_else(|| {
        anyhow::anyhow!(
            "keryx task: {DAEMON_ENDPOINT_ENV} must be set (e.g. http://127.0.0.1:50051)"
        )
    })
}

async fn connect_daemon(
    endpoint: &str,
    operation: &str,
) -> Result<KeryxDaemonClient<tonic::transport::Channel>> {
    let endpoint_url = endpoint.to_string();
    let endpoint = tonic::transport::Endpoint::from_shared(endpoint_url.clone())
        .with_context(|| format!("{operation}: invalid daemon endpoint {endpoint_url}"))?;
    let channel = endpoint
        .connect()
        .await
        .with_context(|| format!("{operation}: daemon unavailable at {endpoint_url}"))?;
    Ok(KeryxDaemonClient::new(channel)
        .max_decoding_message_size(ARTIFACT_RPC_MAX_BYTES)
        .max_encoding_message_size(ARTIFACT_RPC_MAX_BYTES))
}

async fn run_status() -> Result<()> {
    if let Some(endpoint) = daemon_endpoint() {
        let status = daemon_status(&endpoint).await?;
        print_daemon_status(&endpoint, &status);
    } else {
        let runtime = KeryxDaemonRuntime::startup(default_config()).await?;
        print_status(&runtime.status_report());
    }
    Ok(())
}

async fn run_doctor() -> Result<()> {
    if let Some(endpoint) = daemon_endpoint() {
        let doctor = daemon_doctor(&endpoint).await?;
        print_daemon_doctor(&endpoint, &doctor);
    } else {
        let runtime = KeryxDaemonRuntime::startup(default_config()).await?;
        print_doctor(&runtime.doctor_report());
    }
    Ok(())
}

fn daemon_endpoint() -> Option<String> {
    std::env::var(DAEMON_ENDPOINT_ENV)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

async fn daemon_status(endpoint: &str) -> Result<StatusResponse> {
    let mut client = connect_daemon(endpoint, "keryx status").await?;
    Ok(client
        .status(StatusRequest {})
        .await
        .with_context(|| format!("keryx status: daemon request failed at {endpoint}"))?
        .into_inner())
}

async fn daemon_doctor(endpoint: &str) -> Result<DoctorResponse> {
    let mut client = connect_daemon(endpoint, "keryx doctor").await?;
    Ok(client
        .doctor(DoctorRequest {})
        .await
        .with_context(|| format!("keryx doctor: daemon request failed at {endpoint}"))?
        .into_inner())
}

fn default_config() -> KeryxDaemonConfig {
    let data_dir = std::env::var_os("HERMES_KERYX_DATA_DIR")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|| std::path::PathBuf::from(".keryx"));
    KeryxDaemonConfig::new(data_dir, now_ms())
}

fn print_daemon_status(endpoint: &str, status: &StatusResponse) {
    println!("keryx status: {}", status.status);
    println!("source: daemon {endpoint}");
}

fn print_daemon_doctor(endpoint: &str, doctor: &DoctorResponse) {
    println!("keryx doctor: {}", doctor.status);
    println!("source: daemon {endpoint}");
    for message in &doctor.messages {
        println!("{message}");
    }
}

fn print_status(status: &KeryxStatusReport) {
    let readiness = if status.daemon_ready {
        "ready"
    } else {
        "not-ready"
    };
    let store_readiness = if status.store.ready {
        "ready"
    } else {
        "not-ready"
    };
    println!("keryx status: {readiness}");
    println!("source: local-runtime");
    println!("data_dir: {}", status.data_dir.display());
    println!("db_path: {}", status.db_path.display());
    println!(
        "store: {store_readiness} {} schema_version={} supported_schema_version={}",
        status.store.kind, status.schema_version, status.supported_schema_version
    );
    println!(
        "startup_recovery: recovered_tasks={} cleaned_terminal_leases={} corruption_count={} duration_ms={}",
        status.recovered_tasks,
        status.cleaned_terminal_leases,
        status.corruption_count,
        status.startup_recovery_duration_ms
    );
}

fn print_doctor(doctor: &KeryxDoctorReport) {
    let verdict = if doctor.healthy { "pass" } else { "fail" };
    println!("keryx doctor: {verdict}");
    println!("source: local-runtime");
    for check in &doctor.checks {
        let marker = if check.ready { "ok" } else { "fail" };
        println!("[{marker}] {} - {}", check.name, check.detail);
    }
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(i64::MAX as u128) as i64
}
