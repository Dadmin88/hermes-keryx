use anyhow::{Context, Result};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt;
use libp2p::{gossipsub, swarm::SwarmEvent};
use tokio::net::TcpListener;
use tokio::signal;

use tonic::transport::Identity;
use tracing::info;

use keryx_relay::{
    autonat::map_autonat_status,
    bootstrap::dial_bootstrap_peers,
    config::RelayConfig,
    health_server::{
        serve_grpc_health_with_auth_and_tls, serve_grpc_health_with_tls, serve_http_health,
    },
    registry::{SkillRegistry, DEFAULT_CLEANUP_INTERVAL, REGISTRY_GOSSIP_TOPIC},
    registry_server::{serve_registry_rpc_with_tls, RegistryRpcService},
    runtime::RelayRuntime,
    security::{new_shared_allowlist, sync_allowlist_to_swarm, RelayTomlConfig, SharedAllowlist},
    transport::{
        build_relay_server_swarm, load_or_generate_keypair, RelayServerBehaviourEvent,
        RelayServerOptions,
    },
};

struct ProcessConfig {
    relay: RelayConfig,
    toml: Option<RelayTomlConfig>,
    allowlist: Option<keryx_relay::Allowlist>,
}

fn load_grpc_tls_identity(config_path: &Path, relay: &RelayConfig) -> Result<Option<Identity>> {
    let (Some(cert_path), Some(key_path)) = (
        relay.registry_tls_cert_path.as_ref(),
        relay.registry_tls_key_path.as_ref(),
    ) else {
        return Ok(None);
    };
    let resolve = |path: &Path| {
        if path.is_absolute() {
            path.to_path_buf()
        } else {
            config_path
                .parent()
                .unwrap_or_else(|| Path::new("."))
                .join(path)
        }
    };
    let cert_path = resolve(cert_path);
    let key_path = resolve(key_path);
    let cert = std::fs::read(&cert_path)
        .with_context(|| format!("read Keryx gRPC TLS certificate {}", cert_path.display()))?;
    let key = std::fs::read(&key_path)
        .with_context(|| format!("read Keryx gRPC TLS private key {}", key_path.display()))?;
    Ok(Some(Identity::from_pem(cert, key)))
}

fn load_process_config(path: &Path) -> Result<ProcessConfig> {
    let is_toml = path
        .extension()
        .and_then(|ext| ext.to_str())
        .is_some_and(|ext| ext.eq_ignore_ascii_case("toml"));

    if is_toml {
        let toml = RelayTomlConfig::load(path)?;
        let allowlist = toml.load_allowlist(path)?;
        Ok(ProcessConfig {
            relay: toml.to_relay_config(),
            toml: Some(toml),
            allowlist: Some(allowlist),
        })
    } else {
        Ok(ProcessConfig {
            relay: RelayConfig::load(path)?,
            toml: None,
            allowlist: None,
        })
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt().with_env_filter("info").init();

    let config_path = std::env::var_os("HERMES_KERYX_RELAY_CONFIG")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("relay.json"));
    let process = load_process_config(&config_path)?;

    let keypair = load_or_generate_keypair(process.relay.keypair_path.as_deref())?;
    let local_peer_id = keypair.public().to_peer_id();

    let runtime = RelayRuntime::new(local_peer_id.to_string());
    let registry_ttl = process
        .toml
        .as_ref()
        .map(|t| Duration::from_secs(t.registry.ttl_seconds))
        .unwrap_or(keryx_relay::DEFAULT_REGISTRATION_TTL);
    let registry = Arc::new(SkillRegistry::with_default_ttl(registry_ttl));
    let _registry_cleanup = registry.spawn_cleanup(DEFAULT_CLEANUP_INTERVAL);

    let node_auth = process
        .toml
        .as_ref()
        .map(|config| config.load_node_token_auth(&config_path))
        .transpose()?
        .unwrap_or_default();
    let node_auth_configured = node_auth.is_configured();
    let node_auth = Arc::new(node_auth);
    let grpc_tls_identity = load_grpc_tls_identity(&config_path, &process.relay)?;

    let shared_allowlist: Option<SharedAllowlist> = process
        .allowlist
        .as_ref()
        .map(|list| new_shared_allowlist(list.clone()));

    let (health_shutdown_tx, _) = tokio::sync::broadcast::channel::<()>(1);

    if let Some(addr) = process.relay.parse_health_http_bind()? {
        let rt = Arc::clone(&runtime);
        let shutdown_rx = health_shutdown_tx.subscribe();
        tokio::spawn(async move {
            serve_http_health(rt, addr, shutdown_rx).await;
        });
        info!(%addr, "HTTP health listening at /health");
    }

    if let Some(addr) = process.relay.parse_health_grpc_bind()? {
        let rt = Arc::clone(&runtime);
        let reg = Arc::clone(&registry);
        let auth = Arc::clone(&node_auth);
        let tls_identity = grpc_tls_identity.clone();
        tokio::spawn(async move {
            let result = if node_auth_configured {
                serve_grpc_health_with_auth_and_tls(rt, reg, auth, addr, tls_identity).await
            } else {
                serve_grpc_health_with_tls(rt, Some(reg), addr, tls_identity).await
            };
            if let Err(err) = result {
                tracing::error!(%addr, error = %err, "gRPC health server exited");
            }
        });
        info!(%addr, "gRPC relay health listening");
    }

    if let Some(addr) = process.relay.parse_registry_grpc_bind()? {
        let tls_identity = grpc_tls_identity.clone();
        let listener = TcpListener::bind(addr).await?;
        let service = RegistryRpcService::with_metrics_and_auth(
            Arc::clone(&registry),
            Arc::clone(runtime.metrics()),
            Arc::clone(&node_auth),
        );
        tokio::spawn(async move {
            if let Err(err) = serve_registry_rpc_with_tls(service, listener, tls_identity).await {
                tracing::error!(%addr, error = %err, "registry gRPC server exited");
            }
        });
        info!(%addr, tls = process.relay.registry_tls_cert_path.is_some(), "registry gRPC listening");
    }

    let mut swarm = build_relay_server_swarm(
        keypair,
        &RelayServerOptions {
            config: process.relay.clone(),
            allowlist: process.allowlist.clone(),
        },
    )?;

    for addr in process.relay.resolved_listen_addresses()? {
        swarm.listen_on(addr.clone())?;
        info!(%addr, %local_peer_id, "Hermes Keryx relay listening");
    }

    let bootstrap = process.relay.parsed_bootstrap_peers()?;
    dial_bootstrap_peers(&mut swarm, &bootstrap);

    info!(component = "keryx-relay", %local_peer_id, "Hermes Keryx relay started");

    #[cfg(unix)]
    run_relay_loop_unix(
        &mut swarm,
        &runtime,
        &registry,
        registry.subscribe_gossip(),
        &process,
        &config_path,
        &shared_allowlist,
        health_shutdown_tx,
    )
    .await?;

    #[cfg(not(unix))]
    run_relay_loop(
        &mut swarm,
        &runtime,
        &registry,
        registry.subscribe_gossip(),
        health_shutdown_tx,
    )
    .await?;

    Ok(())
}

#[cfg(not(unix))]
async fn run_relay_loop(
    swarm: &mut libp2p::Swarm<keryx_relay::RelayServerBehaviour>,
    runtime: &RelayRuntime,
    registry: &Arc<SkillRegistry>,
    mut registry_gossip_rx: tokio::sync::broadcast::Receiver<Vec<u8>>,
    health_shutdown_tx: tokio::sync::broadcast::Sender<()>,
) -> Result<()> {
    loop {
        tokio::select! {
            _ = signal::ctrl_c() => {
                info!(component = "keryx-relay", "shutdown signal received");
                let _ = health_shutdown_tx.send(());
                break;
            }
            gossip = registry_gossip_rx.recv() => {
                if let Ok(payload) = gossip {
                    publish_registry_gossip(swarm, payload);
                }
            }
            event = swarm.select_next_some() => {
                handle_swarm_event(swarm, runtime, registry, event).await;
            }
        }
    }
    Ok(())
}

#[cfg(unix)]
#[allow(clippy::too_many_arguments)]
async fn run_relay_loop_unix(
    swarm: &mut libp2p::Swarm<keryx_relay::RelayServerBehaviour>,
    runtime: &RelayRuntime,
    registry: &Arc<SkillRegistry>,
    mut registry_gossip_rx: tokio::sync::broadcast::Receiver<Vec<u8>>,
    process: &ProcessConfig,
    config_path: &Path,
    shared_allowlist: &Option<SharedAllowlist>,
    health_shutdown_tx: tokio::sync::broadcast::Sender<()>,
) -> Result<()> {
    use tokio::signal::unix::{signal, SignalKind};
    let mut sighup = signal(SignalKind::hangup()).context("register SIGHUP handler")?;

    loop {
        tokio::select! {
            _ = signal::ctrl_c() => {
                info!(component = "keryx-relay", "shutdown signal received");
                let _ = health_shutdown_tx.send(());
                break;
            }
            gossip = registry_gossip_rx.recv() => {
                if let Ok(payload) = gossip {
                    publish_registry_gossip(swarm, payload);
                }
            }
            _ = sighup.recv() => {
                if let (Some(toml), Some(shared)) = (&process.toml, shared_allowlist) {
                    if let Some(path) = toml.resolved_allowlist_path(config_path).ok().flatten() {
                        match shared.write() {
                            Ok(mut guard) => {
                                if let Err(err) = guard.reload(&path) {
                                    tracing::warn!(path = %path.display(), error = %err, "allowlist reload failed");
                                } else {
                                    let snapshot = guard.clone();
                                    drop(guard);
                                    sync_allowlist_to_swarm(swarm, &snapshot);
                                    info!(path = %path.display(), "allowlist reloaded from disk");
                                }
                            }
                            Err(err) => tracing::warn!(error = %err, "allowlist lock poisoned"),
                        }
                    } else {
                        tracing::warn!("SIGHUP received but security.allowlist_path is not set");
                    }
                }
            }
            event = swarm.select_next_some() => {
                handle_swarm_event(swarm, runtime, registry, event).await;
            }
        }
    }
    Ok(())
}

async fn handle_swarm_event(
    swarm: &mut libp2p::Swarm<keryx_relay::RelayServerBehaviour>,
    runtime: &RelayRuntime,
    registry: &Arc<SkillRegistry>,
    event: SwarmEvent<RelayServerBehaviourEvent>,
) {
    match event {
        SwarmEvent::IncomingConnectionError { error, .. } => {
            tracing::debug!(?error, "incoming connection rejected");
        }
        SwarmEvent::ConnectionEstablished { peer_id, .. } => {
            runtime.note_peer_connected(peer_id.to_string());
            let snapshot = registry.gossip_snapshot_bytes().await;
            publish_registry_gossip(swarm, snapshot);
            tracing::debug!(%peer_id, "peer connected");
        }
        SwarmEvent::ConnectionClosed { peer_id, .. } => {
            runtime.note_peer_disconnected(&peer_id.to_string());
            tracing::debug!(%peer_id, "peer disconnected");
        }
        SwarmEvent::Behaviour(RelayServerBehaviourEvent::Autonat(
            libp2p::autonat::Event::StatusChanged { new, .. },
        )) => {
            info!(status = ?map_autonat_status(new), "autonat status update");
        }
        SwarmEvent::Behaviour(RelayServerBehaviourEvent::Identify(
            libp2p::identify::Event::Received { info, .. },
        )) => {
            swarm.add_external_address(info.observed_addr.clone());
        }
        SwarmEvent::NewListenAddr { address, .. } => {
            runtime.mark_transport_listening();
            info!(%address, "relay ready on address");
        }
        SwarmEvent::Behaviour(RelayServerBehaviourEvent::RegistryGossip(
            gossipsub::Event::Message { message, .. },
        )) => {
            if let Err(err) = registry.apply_gossip_bytes(&message.data).await {
                tracing::debug!(error = %err, "ignored invalid registry gossip payload");
            } else {
                runtime
                    .metrics()
                    .set_registry_size(registry.registration_count().await as u64);
            }
        }
        SwarmEvent::Behaviour(RelayServerBehaviourEvent::Relay(event)) => {
            runtime.metrics().increment_tasks_routed();
            info!(?event, "relay reservation/circuit activity");
        }
        SwarmEvent::Behaviour(other) => {
            info!(?other, "relay behaviour event");
        }
        other => {
            info!(?other, "relay swarm event");
        }
    }
}

fn publish_registry_gossip(
    swarm: &mut libp2p::Swarm<keryx_relay::RelayServerBehaviour>,
    payload: Vec<u8>,
) {
    if payload.is_empty() {
        return;
    }
    let topic = gossipsub::IdentTopic::new(REGISTRY_GOSSIP_TOPIC);
    if let Err(err) = swarm
        .behaviour_mut()
        .registry_gossip
        .publish(topic, payload)
    {
        tracing::debug!(error = %err, "registry gossip publish skipped");
    }
}
