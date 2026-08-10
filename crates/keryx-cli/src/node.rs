use anyhow::{Context, Result};
use clap::Subcommand;
use keryx_daemon::grpc_transport::{ca_cert_path_from_env, secure_grpc_endpoint};
use keryx_proto::v1::keryx_daemon_client::KeryxDaemonClient;
use keryx_proto::v1::registry_service_client::RegistryServiceClient;
use keryx_proto::v1::{DiscoverBySkillRequest, ListPeersRequest, ReadinessRequest};
use std::process::Stdio;

use crate::relay::resolve_sibling_binary;

const DAEMON_ENDPOINT_ENV: &str = "HERMES_KERYX_DAEMON_ENDPOINT";
const NODE_BIN_ENV: &str = "HERMES_KERYX_NODE_BIN";
const RELAY_REGISTRY_ENDPOINT_ENV: &str = "HERMES_KERYX_RELAY_REGISTRY_ENDPOINT";

#[derive(Debug, Subcommand)]
pub enum NodeCommand {
    /// Start the SDK edge node (foreground; connects to daemon when configured).
    Start,
    /// Report node/daemon connection status via the daemon peer directory.
    Status,
    /// Discover agents offering a skill via the relay registry.
    Discover {
        skill: String,
        #[arg(long, default_value_t = 10)]
        limit: u32,
    },
}

pub async fn run(command: NodeCommand) -> Result<()> {
    match command {
        NodeCommand::Start => run_start().await,
        NodeCommand::Status => run_status().await,
        NodeCommand::Discover { skill, limit } => run_discover(&skill, limit).await,
    }
}

async fn run_start() -> Result<()> {
    let binary = resolve_sibling_binary("keryx-node", NODE_BIN_ENV);
    let status = tokio::process::Command::new(&binary)
        .stdin(Stdio::inherit())
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .status()
        .await
        .with_context(|| format!("keryx node start: failed to execute {}", binary.display()))?;
    if !status.success() {
        anyhow::bail!(
            "keryx node start: {} exited with {}",
            binary.display(),
            status
        );
    }
    Ok(())
}

async fn run_status() -> Result<()> {
    let endpoint = require_daemon_endpoint()?;
    let mut client = connect_daemon(&endpoint).await?;

    let readiness = client
        .readiness(ReadinessRequest {})
        .await
        .with_context(|| format!("keryx node status: readiness RPC failed at {endpoint}"))?
        .into_inner();

    let peers = client
        .list_peers(ListPeersRequest {})
        .await
        .with_context(|| format!("keryx node status: list_peers RPC failed at {endpoint}"))?
        .into_inner();

    let connection = if readiness.ready {
        "connected"
    } else {
        "degraded"
    };
    println!("keryx node status: {connection}");
    println!("source: daemon {endpoint}");
    println!("daemon_ready: {}", readiness.ready);
    if !readiness.not_ready_reasons.is_empty() {
        println!(
            "not_ready_reasons: {}",
            readiness.not_ready_reasons.join("; ")
        );
    }
    println!("peers: {}", peers.peers.len());
    for peer in peers.peers {
        println!(
            "peer_id={} connected={} local={}",
            peer.peer_id, peer.connected, peer.local
        );
    }
    Ok(())
}

async fn run_discover(skill: &str, limit: u32) -> Result<()> {
    let endpoint = require_registry_endpoint()?;
    let channel = secure_grpc_endpoint(&endpoint, ca_cert_path_from_env().as_deref())?
        .connect()
        .await
        .with_context(|| format!("keryx node discover: registry unavailable at {endpoint}"))?;
    let mut client = RegistryServiceClient::new(channel);
    let response = client
        .discover_by_skill(DiscoverBySkillRequest {
            skill_id: skill.to_string(),
            tags: vec![],
            limit,
        })
        .await
        .with_context(|| {
            format!("keryx node discover: discover RPC failed for skill {skill} at {endpoint}")
        })?
        .into_inner();

    println!(
        "keryx node discover: skill={skill} matches={}",
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

fn require_daemon_endpoint() -> Result<String> {
    daemon_endpoint().ok_or_else(|| {
        anyhow::anyhow!(
            "keryx node status: {DAEMON_ENDPOINT_ENV} must be set (e.g. http://127.0.0.1:50051)"
        )
    })
}

fn require_registry_endpoint() -> Result<String> {
    registry_endpoint().ok_or_else(|| {
        anyhow::anyhow!(
            "keryx node discover: {RELAY_REGISTRY_ENDPOINT_ENV} must be set (e.g. http://127.0.0.1:50053)"
        )
    })
}

fn daemon_endpoint() -> Option<String> {
    std::env::var(DAEMON_ENDPOINT_ENV)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

fn registry_endpoint() -> Option<String> {
    std::env::var(RELAY_REGISTRY_ENDPOINT_ENV)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .or_else(|| Some("http://127.0.0.1:50053".to_string()))
}

async fn connect_daemon(endpoint: &str) -> Result<KeryxDaemonClient<tonic::transport::Channel>> {
    KeryxDaemonClient::connect(endpoint.to_string())
        .await
        .with_context(|| format!("keryx node status: daemon unavailable at {endpoint}"))
}
