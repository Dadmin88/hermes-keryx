//! Edge node runtime: libp2p relay client with optional registry registration.

use std::path::PathBuf;
use std::time::Duration;

use anyhow::{Context, Result};
use futures::StreamExt;
use keryx_core::RESULT_ARTIFACT_FRAME_MAX_BYTES;
use keryx_proto::v1::keryx_daemon_client::KeryxDaemonClient;
use keryx_proto::v1::keryx_relay_client::KeryxRelayClient;
use keryx_proto::v1::registry_service_client::RegistryServiceClient;
use keryx_proto::v1::{
    AckFrameRequest, AckResultDeliveryRequest, ClaimNextResultDeliveryRequest,
    FailResultDeliveryRequest, IngestRemoteResultRequest, NodeFrame, PublishResultRequest,
    SubmitRemoteTaskRequest,
};
use keryx_proto::v1::{RegisterSkillsRequest, SkillInfo};
use libp2p::swarm::SwarmEvent;
use libp2p::Multiaddr;
use tokio::signal;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::Request;
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
const NODE_RELAY_ENDPOINT_ENV: &str = "HERMES_KERYX_RELAY_ENDPOINT";
const NODE_RELAY_HEALTH_ENDPOINT_ENV: &str = "HERMES_KERYX_RELAY_HEALTH_ENDPOINT";
const NODE_TOKEN_ENV: &str = "HERMES_KERYX_NODE_TOKEN";
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

    let relay_stream_task = match (relay_endpoint(), daemon_endpoint()) {
        (Some(relay_endpoint), Some(daemon_endpoint)) => {
            let registry_peer_id = registry_peer_id.clone();
            Some(tokio::spawn(async move {
                if let Err(error) = run_relay_stream(
                    relay_endpoint,
                    registry_peer_id,
                    node_token(),
                    daemon_endpoint,
                )
                .await
                {
                    tracing::warn!(error = %error, "relay stream task exited");
                }
            }))
        }
        (Some(_), None) => {
            info!(
                registry_peer_id = %registry_peer_id,
                "HERMES_KERYX_DAEMON_ENDPOINT unset; node will not consume relay frames"
            );
            None
        }
        _ => None,
    };

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
                if let Some(task) = &relay_stream_task {
                    task.abort();
                }
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

fn node_token() -> Option<String> {
    std::env::var(NODE_TOKEN_ENV)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

fn add_node_auth_metadata<T>(
    request: &mut Request<T>,
    node_id: &str,
    token: Option<&str>,
) -> Result<()> {
    request
        .metadata_mut()
        .insert(crate::health_server::NODE_ID_METADATA_KEY, node_id.parse()?);
    if let Some(token) = token {
        request.metadata_mut().insert(
            crate::health_server::NODE_TOKEN_METADATA_KEY,
            token.parse()?,
        );
    }
    Ok(())
}

fn daemon_endpoint() -> Option<String> {
    std::env::var(DAEMON_ENDPOINT_ENV)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

fn relay_endpoint() -> Option<String> {
    std::env::var(NODE_RELAY_ENDPOINT_ENV)
        .or_else(|_| std::env::var(NODE_RELAY_HEALTH_ENDPOINT_ENV))
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

async fn run_relay_stream(
    relay_endpoint: String,
    registry_peer_id: String,
    node_token: Option<String>,
    daemon_endpoint: String,
) -> Result<()> {
    let mut relay = KeryxRelayClient::connect(relay_endpoint.clone())
        .await
        .with_context(|| format!("keryx node stream: relay unavailable at {relay_endpoint}"))?
        .max_encoding_message_size(RESULT_ARTIFACT_FRAME_MAX_BYTES)
        .max_decoding_message_size(RESULT_ARTIFACT_FRAME_MAX_BYTES);
    let (_tx, rx) = mpsc::channel::<NodeFrame>(8);
    let mut request = Request::new(ReceiverStream::new(rx));
    add_node_auth_metadata(&mut request, &registry_peer_id, node_token.as_deref())?;
    let mut stream = relay
        .connect_node(request)
        .await
        .context("keryx node stream: ConnectNode RPC failed")?
        .into_inner();
    info!(registry_peer_id = %registry_peer_id, relay_endpoint = %relay_endpoint, "relay stream connected");

    let delivery_worker = format!("edge-{registry_peer_id}");
    let mut delivery_tick = tokio::time::interval(Duration::from_millis(250));
    loop {
        tokio::select! {
            next = stream.next() => {
                let Some(frame) = next else { break; };
                let frame = frame.context("keryx node stream: relay frame failed")?;
                let mut daemon = KeryxDaemonClient::connect(daemon_endpoint.clone())
                    .await
                    .with_context(|| format!("keryx node stream: daemon unavailable at {daemon_endpoint}"))?
                    .max_encoding_message_size(RESULT_ARTIFACT_FRAME_MAX_BYTES)
                    .max_decoding_message_size(RESULT_ARTIFACT_FRAME_MAX_BYTES);
                if let Some(task) = frame.task {
                    daemon
                        .submit_remote_task(SubmitRemoteTaskRequest {
                            envelope: Some(task),
                            authenticated_sender_peer_id: frame.authenticated_source_node_id.clone(),
                            destination_peer_id: frame.destination_node_id.clone(),
                            relay_frame_id: frame.frame_id.clone(),
                        })
                        .await
                        .context("keryx node stream: daemon SubmitRemoteTask failed")?;
                } else if let Some(result) = frame.result {
                    daemon
                        .ingest_remote_result(IngestRemoteResultRequest {
                            result: Some(result),
                            authenticated_executor_peer_id: frame.authenticated_source_node_id.clone(),
                            destination_peer_id: frame.destination_node_id.clone(),
                            relay_frame_id: frame.frame_id.clone(),
                        })
                        .await
                        .context("keryx node stream: daemon IngestRemoteResult failed")?;
                } else {
                    tracing::warn!(frame_id = %frame.frame_id, "dropping empty relay frame");
                    continue;
                }
                let mut ack_client = KeryxRelayClient::connect(relay_endpoint.clone()).await?;
                let mut ack_request = Request::new(AckFrameRequest { frame_id: frame.frame_id });
                add_node_auth_metadata(
                    &mut ack_request,
                    &registry_peer_id,
                    node_token.as_deref(),
                )?;
                ack_client
                    .ack_frame(ack_request)
                    .await
                    .context("keryx node stream: relay AckFrame failed")?;
            }
            _ = delivery_tick.tick() => {
                let mut daemon = KeryxDaemonClient::connect(daemon_endpoint.clone())
                    .await?
                    .max_encoding_message_size(RESULT_ARTIFACT_FRAME_MAX_BYTES)
                    .max_decoding_message_size(RESULT_ARTIFACT_FRAME_MAX_BYTES);
                let delivery = daemon
                    .claim_next_result_delivery(ClaimNextResultDeliveryRequest {
                        worker_id: delivery_worker.clone(),
                        lease_duration_ms: 30_000,
                    })
                    .await?
                    .into_inner();
                if !delivery.has_delivery {
                    continue;
                }
                let mut publish_request = Request::new(PublishResultRequest {
                    result: delivery.result,
                    target_node_id: delivery.target_peer_id,
                    source_node_id: registry_peer_id.clone(),
                    frame_id: delivery.delivery_id.clone(),
                });
                add_node_auth_metadata(
                    &mut publish_request,
                    &registry_peer_id,
                    node_token.as_deref(),
                )?;
                let publish = relay.publish_result(publish_request).await;
                match publish {
                    Ok(_) => {
                        daemon
                            .ack_result_delivery(AckResultDeliveryRequest {
                                delivery_id: delivery.delivery_id,
                                worker_id: delivery_worker.clone(),
                            })
                            .await?;
                    }
                    Err(error) => {
                        daemon
                            .fail_result_delivery(FailResultDeliveryRequest {
                                delivery_id: delivery.delivery_id,
                                worker_id: delivery_worker.clone(),
                                error_reason: error.message().to_string(),
                                retry_delay_ms: 1_000,
                                dead_letter: false,
                            })
                            .await?;
                    }
                }
            }
        }
    }
    Ok(())
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
