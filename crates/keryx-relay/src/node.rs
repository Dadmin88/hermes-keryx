//! Edge node runtime: libp2p relay client with optional registry registration.

use std::path::PathBuf;
use std::time::Duration;

use anyhow::{Context, Result};
use futures::StreamExt;
use keryx_proto::v1::registry_service_client::RegistryServiceClient;
use keryx_proto::v1::{RegisterSkillsRequest, SkillInfo};
use libp2p::swarm::SwarmEvent;
use libp2p::Multiaddr;
use tokio::signal;
use tracing::info;

use crate::bootstrap::{dial_bootstrap_peers, wait_for_listen_addr};
use crate::config::RelayConfig;
use crate::transport::{
    build_relay_client_swarm, load_or_generate_keypair, NodeSwarmOptions, RelayClientBehaviourEvent,
};

const NODE_PEER_ID_ENV: &str = "HERMES_KERYX_NODE_PEER_ID";
const NODE_KEYPAIR_ENV: &str = "HERMES_KERYX_NODE_KEYPAIR_PATH";
const NODE_BOOTSTRAP_ENV: &str = "HERMES_KERYX_NODE_BOOTSTRAP_PEERS";
const NODE_SKILLS_ENV: &str = "HERMES_KERYX_NODE_SKILLS";
const NODE_REGISTRY_ENDPOINT_ENV: &str = "HERMES_KERYX_RELAY_REGISTRY_ENDPOINT";
const DAEMON_ENDPOINT_ENV: &str = "HERMES_KERYX_DAEMON_ENDPOINT";

/// Run an edge node until SIGINT: listen, dial bootstrap peers, optionally register skills.
pub async fn run_edge_node() -> Result<()> {
    tracing_subscriber::fmt().with_env_filter("info").init();

    let keypair_path = std::env::var_os(NODE_KEYPAIR_ENV).map(PathBuf::from);
    let keypair = load_or_generate_keypair(keypair_path.as_deref())?;
    let libp2p_peer_id = keypair.public().to_peer_id();
    let registry_peer_id = std::env::var(NODE_PEER_ID_ENV)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| libp2p_peer_id.to_string());

    if let Some(endpoint) = daemon_endpoint() {
        verify_daemon_reachable(&endpoint).await?;
        info!(%endpoint, registry_peer_id = %registry_peer_id, "daemon reachable");
    } else {
        info!(
            registry_peer_id = %registry_peer_id,
            "HERMES_KERYX_DAEMON_ENDPOINT unset; node will not verify daemon connectivity"
        );
    }

    let mut swarm = build_relay_client_swarm(keypair, &NodeSwarmOptions::default())?;
    swarm.listen_on("/ip4/0.0.0.0/tcp/0".parse()?)?;
    swarm.listen_on("/ip4/0.0.0.0/udp/0/quic-v1".parse()?)?;
    let _ = wait_for_listen_addr(&mut swarm, Duration::from_secs(10)).await;

    let bootstrap = bootstrap_peers_from_env()?;
    dial_bootstrap_peers(&mut swarm, &bootstrap);

    register_node_skills(&registry_peer_id).await?;

    info!(
        component = "keryx-node",
        %libp2p_peer_id,
        registry_peer_id = %registry_peer_id,
        "Hermes Keryx edge node started"
    );

    loop {
        tokio::select! {
            _ = signal::ctrl_c() => {
                info!(component = "keryx-node", "shutdown signal received");
                break;
            }
            event = swarm.select_next_some() => {
                handle_node_swarm_event(event);
            }
        }
    }
    Ok(())
}

fn handle_node_swarm_event(event: SwarmEvent<RelayClientBehaviourEvent>) {
    match event {
        SwarmEvent::NewListenAddr { address, .. } => {
            info!(%address, "node listening");
        }
        SwarmEvent::ConnectionEstablished { peer_id, .. } => {
            info!(%peer_id, "peer connected");
        }
        SwarmEvent::ConnectionClosed { peer_id, .. } => {
            info!(%peer_id, "peer disconnected");
        }
        SwarmEvent::OutgoingConnectionError { peer_id, error, .. } => {
            tracing::debug!(?peer_id, ?error, "outgoing connection error");
        }
        _ => {}
    }
}

fn daemon_endpoint() -> Option<String> {
    std::env::var(DAEMON_ENDPOINT_ENV)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

async fn verify_daemon_reachable(endpoint: &str) -> Result<()> {
    use keryx_proto::v1::keryx_daemon_client::KeryxDaemonClient;
    use keryx_proto::v1::ReadinessRequest;

    let mut client = KeryxDaemonClient::connect(endpoint.to_string())
        .await
        .with_context(|| format!("keryx node start: daemon unavailable at {endpoint}"))?;
    let response = client
        .readiness(ReadinessRequest {})
        .await
        .with_context(|| format!("keryx node start: daemon readiness failed at {endpoint}"))?
        .into_inner();
    anyhow::ensure!(
        response.ready,
        "keryx node start: daemon not ready at {endpoint}: {:?}",
        response.not_ready_reasons
    );
    Ok(())
}

fn bootstrap_peers_from_env() -> Result<Vec<Multiaddr>> {
    if let Ok(raw) = std::env::var(NODE_BOOTSTRAP_ENV) {
        let peers: Vec<Multiaddr> = raw
            .split(',')
            .map(str::trim)
            .filter(|part| !part.is_empty())
            .map(|part| {
                part.parse()
                    .with_context(|| format!("parse bootstrap multiaddr {part}"))
            })
            .collect::<Result<Vec<_>>>()?;
        if !peers.is_empty() {
            return Ok(peers);
        }
    }

    let config_path = std::env::var_os("HERMES_KERYX_RELAY_CONFIG").map(PathBuf::from);
    if let Some(path) = config_path {
        let relay = RelayConfig::load(&path)?;
        return relay.parsed_bootstrap_peers();
    }
    Ok(Vec::new())
}

async fn register_node_skills(registry_peer_id: &str) -> Result<()> {
    let Some(endpoint) = registry_endpoint() else {
        return Ok(());
    };
    let skills = skills_from_env();
    if skills.is_empty() {
        return Ok(());
    }

    let mut client = RegistryServiceClient::connect(endpoint.clone())
        .await
        .with_context(|| format!("keryx node start: registry unavailable at {endpoint}"))?;
    client
        .register_skills(RegisterSkillsRequest {
            peer_id: registry_peer_id.to_string(),
            skills,
            name: std::env::var("HERMES_KERYX_NODE_NAME").unwrap_or_default(),
            description: std::env::var("HERMES_KERYX_NODE_DESCRIPTION").unwrap_or_default(),
            ttl_seconds: std::env::var("HERMES_KERYX_NODE_TTL_SECONDS")
                .ok()
                .and_then(|value| value.parse().ok())
                .unwrap_or(300),
        })
        .await
        .context("keryx node start: register_skills RPC failed")?;
    info!(registry_peer_id = %registry_peer_id, "registered node skills with relay registry");
    Ok(())
}

fn registry_endpoint() -> Option<String> {
    std::env::var(NODE_REGISTRY_ENDPOINT_ENV)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

fn skills_from_env() -> Vec<SkillInfo> {
    let Ok(raw) = std::env::var(NODE_SKILLS_ENV) else {
        return Vec::new();
    };
    raw.split(',')
        .map(str::trim)
        .filter(|part| !part.is_empty())
        .map(|skill_id| SkillInfo {
            skill_id: skill_id.to_string(),
            description: String::new(),
            tags: vec![],
        })
        .collect()
}
