use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use keryx_daemon::{KeryxDaemonConfig, KeryxDaemonRuntime, KeryxDoctorReport, KeryxStatusReport};
use keryx_proto::v1::{
    keryx_daemon_client::KeryxDaemonClient, DoctorRequest, DoctorResponse, StatusRequest,
    StatusResponse,
};

const DAEMON_ENDPOINT_ENV: &str = "HERMES_KERYX_DAEMON_ENDPOINT";

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
}

#[tokio::main]
async fn main() -> Result<()> {
    match Cli::parse().command {
        Command::Status => run_status().await?,
        Command::Doctor => run_doctor().await?,
    }
    Ok(())
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
    let mut client = KeryxDaemonClient::connect(endpoint.to_string())
        .await
        .with_context(|| format!("keryx status: daemon unavailable at {endpoint}"))?;
    Ok(client
        .status(StatusRequest {})
        .await
        .with_context(|| format!("keryx status: daemon request failed at {endpoint}"))?
        .into_inner())
}

async fn daemon_doctor(endpoint: &str) -> Result<DoctorResponse> {
    let mut client = KeryxDaemonClient::connect(endpoint.to_string())
        .await
        .with_context(|| format!("keryx doctor: daemon unavailable at {endpoint}"))?;
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
    println!("data_dir: {}", status.data_dir);
    println!("db_path: {}", status.db_path);
    let store_readiness = if status.store_ready {
        "ready"
    } else {
        "not-ready"
    };
    println!(
        "store: {store_readiness} {} schema_version={}",
        status.store_kind, status.schema_version
    );
    println!(
        "startup_recovery: recovered_tasks={} cleaned_terminal_leases={} corruption_count={}",
        status.recovered_tasks, status.cleaned_terminal_leases, status.corruption_count
    );
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
        "store: {store_readiness} {} schema_version={}",
        status.store.kind, status.schema_version
    );
    println!(
        "startup_recovery: recovered_tasks={} cleaned_terminal_leases={} corruption_count={}",
        status.recovered_tasks, status.cleaned_terminal_leases, status.corruption_count
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
