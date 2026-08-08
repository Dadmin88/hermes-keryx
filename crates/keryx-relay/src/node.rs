//! Edge node runtime: libp2p relay client with optional registry registration.

use std::net::IpAddr;
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
use tokio::sync::{mpsc, watch};
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::{Certificate, ClientTlsConfig, Endpoint};
use tonic::{Code, Request};
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
const REGISTRY_CA_CERT_ENV: &str = "HERMES_KERYX_REGISTRY_CA_CERT";
const NODE_REGISTRY_ENDPOINT_ENV: &str = "HERMES_KERYX_RELAY_REGISTRY_ENDPOINT";
const NODE_RELAY_ENDPOINT_ENV: &str = "HERMES_KERYX_RELAY_ENDPOINT";
const NODE_RELAY_HEALTH_ENDPOINT_ENV: &str = "HERMES_KERYX_RELAY_HEALTH_ENDPOINT";
const NODE_TOKEN_ENV: &str = "HERMES_KERYX_NODE_TOKEN";
const DAEMON_ENDPOINT_ENV: &str = "HERMES_KERYX_DAEMON_ENDPOINT";
const RELAY_RECONNECT_INITIAL_DELAY: Duration = Duration::from_millis(250);
const RELAY_RECONNECT_MAX_DELAY: Duration = Duration::from_secs(5);

#[derive(Debug, Clone, Copy)]
struct RelayReconnectPolicy {
    initial_delay: Duration,
    max_delay: Duration,
    jitter_seed: u64,
}

impl RelayReconnectPolicy {
    const fn new(initial_delay: Duration, max_delay: Duration) -> Self {
        Self {
            initial_delay,
            max_delay,
            jitter_seed: 0,
        }
    }

    const fn with_jitter_seed(mut self, jitter_seed: u64) -> Self {
        self.jitter_seed = jitter_seed;
        self
    }
}

impl Default for RelayReconnectPolicy {
    fn default() -> Self {
        Self::new(RELAY_RECONNECT_INITIAL_DELAY, RELAY_RECONNECT_MAX_DELAY)
    }
}

fn next_reconnect_delay(current: Duration, maximum: Duration) -> Duration {
    current.saturating_mul(2).min(maximum)
}

fn stable_jitter_seed(value: &str) -> u64 {
    value
        .as_bytes()
        .iter()
        .fold(0xcbf29ce484222325, |hash, byte| {
            (hash ^ u64::from(*byte)).wrapping_mul(0x100000001b3)
        })
}

fn jittered_delay(base: Duration, maximum: Duration, seed: u64, attempt: u32) -> Duration {
    let mixed = seed
        .wrapping_add(u64::from(attempt).wrapping_mul(0x9e3779b97f4a7c15))
        .wrapping_mul(0xbf58476d1ce4e5b9);
    let percent = 80 + (mixed % 41);
    base.saturating_mul(percent as u32)
        .checked_div(100)
        .unwrap_or(base)
        .min(maximum)
}

async fn wait_for_shutdown(shutdown: &mut watch::Receiver<bool>) {
    if *shutdown.borrow() {
        return;
    }
    while shutdown.changed().await.is_ok() {
        if *shutdown.borrow_and_update() {
            return;
        }
    }
}

async fn supervise_relay_stream<F, Fut>(
    mut run_once: F,
    mut shutdown: watch::Receiver<bool>,
    policy: RelayReconnectPolicy,
) where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<()>>,
{
    let mut delay = policy.initial_delay.min(policy.max_delay);
    let mut retry_attempt = 0_u32;
    loop {
        if *shutdown.borrow() {
            break;
        }
        let outcome = tokio::select! {
            _ = wait_for_shutdown(&mut shutdown) => break,
            outcome = run_once() => outcome,
        };
        match outcome {
            Ok(()) => tracing::warn!("relay stream closed cleanly; reconnecting"),
            Err(error) => tracing::warn!(error = %error, "relay stream failed; reconnecting"),
        }
        let sleep_delay =
            jittered_delay(delay, policy.max_delay, policy.jitter_seed, retry_attempt);
        tokio::select! {
            _ = wait_for_shutdown(&mut shutdown) => break,
            _ = tokio::time::sleep(sleep_delay) => {}
        }
        retry_attempt = retry_attempt.saturating_add(1);
        delay = next_reconnect_delay(delay, policy.max_delay);
    }
}

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

    let mut relay_stream_task = match (relay_endpoint(), daemon_endpoint()) {
        (Some(relay_endpoint), Some(daemon_endpoint)) => {
            let registry_peer_id = registry_peer_id.clone();
            let node_token = node_token();
            let (shutdown_tx, shutdown_rx) = watch::channel(false);
            let stream_shutdown = shutdown_rx.clone();
            let stream_relay_endpoint = relay_endpoint.clone();
            let stream_registry_peer_id = registry_peer_id.clone();
            let stream_node_token = node_token.clone();
            let stream_daemon_endpoint = daemon_endpoint.clone();
            let stream_jitter_seed = stable_jitter_seed(&format!("stream:{registry_peer_id}"));
            let delivery_jitter_seed =
                stable_jitter_seed(&format!("result-delivery:{registry_peer_id}"));
            let task = tokio::spawn(async move {
                tokio::join!(
                    supervise_relay_stream(
                        move || {
                            run_relay_stream(
                                stream_relay_endpoint.clone(),
                                stream_registry_peer_id.clone(),
                                stream_node_token.clone(),
                                stream_daemon_endpoint.clone(),
                            )
                        },
                        stream_shutdown,
                        RelayReconnectPolicy::default().with_jitter_seed(stream_jitter_seed),
                    ),
                    supervise_relay_stream(
                        move || {
                            run_result_delivery_worker(
                                relay_endpoint.clone(),
                                registry_peer_id.clone(),
                                node_token.clone(),
                                daemon_endpoint.clone(),
                            )
                        },
                        shutdown_rx,
                        RelayReconnectPolicy::default().with_jitter_seed(delivery_jitter_seed),
                    )
                );
            });
            Some((shutdown_tx, task))
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
                if let Some((shutdown_tx, mut task)) = relay_stream_task.take() {
                    let _ = shutdown_tx.send(true);
                    if tokio::time::timeout(Duration::from_secs(5), &mut task)
                        .await
                        .is_err()
                    {
                        tracing::warn!("relay stream supervisor did not stop before timeout; aborting");
                        task.abort();
                        let _ = task.await;
                    }
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

const MAX_RESULT_DELIVERY_ATTEMPTS: u32 = 10;

fn publish_result_failure_is_permanent(code: Code) -> bool {
    matches!(
        code,
        Code::InvalidArgument
            | Code::Unauthenticated
            | Code::PermissionDenied
            | Code::NotFound
            | Code::AlreadyExists
            | Code::FailedPrecondition
            | Code::OutOfRange
            | Code::Unimplemented
            | Code::DataLoss
    )
}

/// `attempt_count` is the number of previously failed publication attempts stored in the outbox.
/// The failure currently being handled is therefore attempt `attempt_count + 1`.
fn result_delivery_failure_should_dead_letter(code: Code, attempt_count: u32) -> bool {
    publish_result_failure_is_permanent(code)
        || attempt_count.saturating_add(1) >= MAX_RESULT_DELIVERY_ATTEMPTS
}

fn result_delivery_retry_delay_ms(delivery_id: &str, attempt_count: u32) -> i64 {
    const MAX_DELAY_MS: i64 = 60_000;
    let multiplier = 1_i64 << attempt_count.min(6);
    let base_ms = 1_000_i64.saturating_mul(multiplier).min(MAX_DELAY_MS);
    jittered_delay(
        Duration::from_millis(base_ms as u64),
        Duration::from_millis(MAX_DELAY_MS as u64),
        stable_jitter_seed(delivery_id),
        attempt_count,
    )
    .as_millis() as i64
}

async fn run_result_delivery_worker(
    relay_endpoint: String,
    registry_peer_id: String,
    node_token: Option<String>,
    daemon_endpoint: String,
) -> Result<()> {
    run_result_delivery_worker_with_retry_delay(
        relay_endpoint,
        registry_peer_id,
        node_token,
        daemon_endpoint,
        result_delivery_retry_delay_ms,
    )
    .await
}

async fn run_result_delivery_worker_with_retry_delay<F>(
    relay_endpoint: String,
    registry_peer_id: String,
    node_token: Option<String>,
    daemon_endpoint: String,
    retry_delay_ms: F,
) -> Result<()>
where
    F: Fn(&str, u32) -> i64 + Send + Sync,
{
    let channel = secure_endpoint_builder(&relay_endpoint)?.connect().await?;
    let mut relay = KeryxRelayClient::new(channel)
        .max_encoding_message_size(RESULT_ARTIFACT_FRAME_MAX_BYTES)
        .max_decoding_message_size(RESULT_ARTIFACT_FRAME_MAX_BYTES);
    let mut daemon = KeryxDaemonClient::connect(daemon_endpoint)
        .await?
        .max_encoding_message_size(RESULT_ARTIFACT_FRAME_MAX_BYTES)
        .max_decoding_message_size(RESULT_ARTIFACT_FRAME_MAX_BYTES);
    let delivery_worker = format!("edge-{registry_peer_id}");
    let mut delivery_tick = tokio::time::interval(Duration::from_millis(250));
    loop {
        delivery_tick.tick().await;
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
        match relay.publish_result(publish_request).await {
            Ok(_) => {
                daemon
                    .ack_result_delivery(AckResultDeliveryRequest {
                        delivery_id: delivery.delivery_id,
                        worker_id: delivery_worker.clone(),
                        lease_expires_at_ms: delivery.lease_expires_at_ms,
                    })
                    .await?;
            }
            Err(error) => {
                daemon
                    .fail_result_delivery(FailResultDeliveryRequest {
                        delivery_id: delivery.delivery_id.clone(),
                        worker_id: delivery_worker.clone(),
                        error_reason: error.message().to_string(),
                        retry_delay_ms: retry_delay_ms(
                            &delivery.delivery_id,
                            delivery.attempt_count,
                        ),
                        dead_letter: result_delivery_failure_should_dead_letter(
                            error.code(),
                            delivery.attempt_count,
                        ),
                        lease_expires_at_ms: delivery.lease_expires_at_ms,
                    })
                    .await?;
            }
        }
    }
}

async fn run_relay_stream(
    relay_endpoint: String,
    registry_peer_id: String,
    node_token: Option<String>,
    daemon_endpoint: String,
) -> Result<()> {
    let channel = secure_endpoint_builder(&relay_endpoint)?
        .connect()
        .await
        .with_context(|| format!("keryx node stream: relay unavailable at {relay_endpoint}"))?;
    let mut relay = KeryxRelayClient::new(channel)
        .max_encoding_message_size(RESULT_ARTIFACT_FRAME_MAX_BYTES)
        .max_decoding_message_size(RESULT_ARTIFACT_FRAME_MAX_BYTES);
    let (request_sender, rx) = mpsc::channel::<NodeFrame>(8);
    let mut request = Request::new(ReceiverStream::new(rx));
    add_node_auth_metadata(&mut request, &registry_peer_id, node_token.as_deref())?;
    let mut stream = relay
        .connect_node(request)
        .await
        .context("keryx node stream: ConnectNode RPC failed")?
        .into_inner();
    info!(registry_peer_id = %registry_peer_id, relay_endpoint = %relay_endpoint, "relay stream connected");

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
                let ack_channel = secure_endpoint_builder(&relay_endpoint)?.connect().await?;
                let mut ack_client = KeryxRelayClient::new(ack_channel);
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

        }
    }
    drop(request_sender);
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

    let mut client = connect_registry_client(&endpoint).await?;
    let mut request = Request::new(RegisterSkillsRequest {
        peer_id: registry_peer_id.to_string(),
        skills,
        name: std::env::var("HERMES_KERYX_NODE_NAME").unwrap_or_default(),
        description: std::env::var("HERMES_KERYX_NODE_DESCRIPTION").unwrap_or_default(),
        ttl_seconds: std::env::var("HERMES_KERYX_NODE_TTL_SECONDS")
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(300),
        protocol_features: vec![
            "absolute_deadlines_v1".to_string(),
            "result_artifact_bytes_v1".to_string(),
        ],
    });
    add_node_auth_metadata(&mut request, registry_peer_id, node_token().as_deref())?;
    client
        .register_skills(request)
        .await
        .context("keryx node start: register_skills RPC failed")?;
    info!(registry_peer_id = %registry_peer_id, "registered node skills with relay registry");
    Ok(())
}

async fn connect_registry_client(
    endpoint: &str,
) -> Result<RegistryServiceClient<tonic::transport::Channel>> {
    let channel = secure_endpoint_builder(endpoint)?
        .connect()
        .await
        .with_context(|| format!("keryx node start: registry unavailable at {endpoint}"))?;
    Ok(RegistryServiceClient::new(channel))
}

fn secure_endpoint_builder(endpoint: &str) -> Result<Endpoint> {
    let mut endpoint_builder = Endpoint::from_shared(endpoint.to_string())
        .with_context(|| format!("invalid Keryx endpoint {endpoint}"))?;
    let uri = endpoint_builder.uri();
    let host = uri.host().context("Keryx endpoint must include a host")?;
    let loopback = host.eq_ignore_ascii_case("localhost")
        || host.parse::<IpAddr>().is_ok_and(|ip| ip.is_loopback());
    let secure = uri.scheme_str() == Some("https");
    if !secure && !loopback {
        anyhow::bail!("remote Keryx gRPC endpoints require TLS (https://)");
    }
    if secure {
        let mut tls = ClientTlsConfig::new().with_native_roots();
        if let Some(path) = std::env::var_os(REGISTRY_CA_CERT_ENV).map(PathBuf::from) {
            let pem = std::fs::read(&path)
                .with_context(|| format!("read Keryx CA certificate {}", path.display()))?;
            tls = tls.ca_certificate(Certificate::from_pem(pem));
        }
        endpoint_builder = endpoint_builder
            .tls_config(tls)
            .context("configure Keryx gRPC TLS")?;
    }
    Ok(endpoint_builder)
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

#[cfg(test)]
mod tests;
