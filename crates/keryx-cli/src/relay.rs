use anyhow::{Context, Result};
use clap::Subcommand;
use keryx_proto::v1::keryx_relay_client::KeryxRelayClient;
use keryx_proto::v1::registry_service_client::RegistryServiceClient;
use keryx_proto::v1::{DiscoverBySkillRequest, HealthRequest};
use std::path::PathBuf;
use std::process::Stdio;

const RELAY_CONFIG_ENV: &str = "HERMES_KERYX_RELAY_CONFIG";
const RELAY_BIN_ENV: &str = "HERMES_KERYX_RELAY_BIN";
const RELAY_HEALTH_ENDPOINT_ENV: &str = "HERMES_KERYX_RELAY_HEALTH_ENDPOINT";
const RELAY_REGISTRY_ENDPOINT_ENV: &str = "HERMES_KERYX_RELAY_REGISTRY_ENDPOINT";

#[derive(Debug, Subcommand)]
pub enum RelayCommand {
    /// Start the relay server (foreground; runs until interrupted).
    Start {
        /// Relay config path (JSON or TOML). Defaults to `relay.json` or `HERMES_KERYX_RELAY_CONFIG`.
        #[arg(long)]
        config: Option<PathBuf>,
    },
    /// Query relay health and connected peer count.
    Status,
    /// Registry operations against the relay skill registry.
    Registry {
        #[command(subcommand)]
        command: RelayRegistryCommand,
    },
}

#[derive(Debug, Subcommand)]
pub enum RelayRegistryCommand {
    /// List registered skills (all active registrations).
    List,
}

pub async fn run(command: RelayCommand) -> Result<()> {
    match command {
        RelayCommand::Start { config } => run_start(config).await,
        RelayCommand::Status => run_status().await,
        RelayCommand::Registry { command } => run_registry(command).await,
    }
}

async fn run_start(config: Option<PathBuf>) -> Result<()> {
    let config_path = config
        .or_else(|| std::env::var_os(RELAY_CONFIG_ENV).map(PathBuf::from))
        .unwrap_or_else(|| PathBuf::from("relay.json"));

    let binary = resolve_sibling_binary("keryx-relay", RELAY_BIN_ENV);
    let status = tokio::process::Command::new(&binary)
        .env(RELAY_CONFIG_ENV, &config_path)
        .stdin(Stdio::inherit())
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .status()
        .await
        .with_context(|| format!("keryx relay start: failed to execute {}", binary.display()))?;
    if !status.success() {
        anyhow::bail!(
            "keryx relay start: {} exited with {}",
            binary.display(),
            status
        );
    }
    Ok(())
}

async fn run_status() -> Result<()> {
    let endpoint = require_relay_health_endpoint()?;
    let mut client = connect_relay_health(&endpoint).await?;
    let health = client
        .health(HealthRequest {})
        .await
        .with_context(|| format!("keryx relay status: health RPC failed at {endpoint}"))?
        .into_inner();

    let readiness = if health.healthy {
        "healthy"
    } else {
        "unhealthy"
    };
    println!("keryx relay status: {readiness}");
    println!("source: relay {endpoint}");
    println!("local_peer_id: {}", health.local_peer_id);
    println!("connected_peers: {}", health.connected_peers);
    println!("registry_size: {}", health.registry_size);
    println!("uptime_seconds: {}", health.uptime_seconds);
    println!("transport_status: {}", health.transport_status);
    println!("tasks_routed: {}", health.tasks_routed);
    Ok(())
}

async fn run_registry(command: RelayRegistryCommand) -> Result<()> {
    match command {
        RelayRegistryCommand::List => run_registry_list().await,
    }
}

async fn run_registry_list() -> Result<()> {
    let endpoint = require_registry_endpoint()?;
    let mut client = connect_registry(&endpoint).await?;
    let response = client
        .discover_by_skill(DiscoverBySkillRequest {
            skill_id: String::new(),
            tags: vec![],
            limit: 0,
        })
        .await
        .with_context(|| format!("keryx relay registry list: RPC failed at {endpoint}"))?
        .into_inner();

    println!(
        "keryx relay registry list: {} registration(s)",
        response.registrations.len()
    );
    println!("source: registry {endpoint}");
    for reg in &response.registrations {
        let skills: Vec<String> = reg.skills.iter().map(|s| s.skill_id.clone()).collect();
        println!(
            "peer_id={} name={} skills=[{}]",
            reg.peer_id,
            reg.name,
            skills.join(", ")
        );
    }
    Ok(())
}

fn require_relay_health_endpoint() -> Result<String> {
    relay_health_endpoint().ok_or_else(|| {
        anyhow::anyhow!(
            "keryx relay status: {RELAY_HEALTH_ENDPOINT_ENV} must be set (e.g. http://127.0.0.1:50052)"
        )
    })
}

fn require_registry_endpoint() -> Result<String> {
    registry_endpoint().ok_or_else(|| {
        anyhow::anyhow!(
            "keryx relay registry: {RELAY_REGISTRY_ENDPOINT_ENV} must be set (e.g. http://127.0.0.1:50053)"
        )
    })
}

fn relay_health_endpoint() -> Option<String> {
    std::env::var(RELAY_HEALTH_ENDPOINT_ENV)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .or_else(|| Some("http://127.0.0.1:50052".to_string()))
}

fn registry_endpoint() -> Option<String> {
    std::env::var(RELAY_REGISTRY_ENDPOINT_ENV)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .or_else(|| Some("http://127.0.0.1:50053".to_string()))
}

async fn connect_relay_health(
    endpoint: &str,
) -> Result<KeryxRelayClient<tonic::transport::Channel>> {
    KeryxRelayClient::connect(endpoint.to_string())
        .await
        .with_context(|| format!("keryx relay status: relay unavailable at {endpoint}"))
}

async fn connect_registry(
    endpoint: &str,
) -> Result<RegistryServiceClient<tonic::transport::Channel>> {
    RegistryServiceClient::connect(endpoint.to_string())
        .await
        .with_context(|| format!("keryx relay registry: registry unavailable at {endpoint}"))
}

pub fn resolve_sibling_binary(default_name: &str, override_env: &str) -> PathBuf {
    if let Ok(path) = std::env::var(override_env) {
        let trimmed = path.trim();
        if !trimmed.is_empty() {
            return PathBuf::from(trimmed);
        }
    }
    if let Ok(current) = std::env::current_exe() {
        if let Some(parent) = current.parent() {
            let sibling = parent.join(default_name);
            if sibling.exists() {
                return sibling;
            }
        }
    }
    PathBuf::from(default_name)
}
