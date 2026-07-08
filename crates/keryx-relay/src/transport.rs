//! libp2p swarm construction for relay servers and edge nodes.

use std::fs;
use std::path::Path;
use std::time::Duration;

use anyhow::{Context, Result};
use libp2p::swarm::NetworkBehaviour;
use libp2p::{
    autonat, gossipsub, identify, identity, mdns, noise, ping, relay, tcp, yamux, Multiaddr,
    PeerId, Swarm,
};

use crate::config::{RelayConfig, KERYX_IDENTIFY_PROTOCOL};
use crate::relay::{new_relay_behaviour, RelayLimits};

/// Options for constructing a relay-server swarm.
#[derive(Debug, Clone, Default)]
pub struct RelayServerOptions {
    pub config: RelayConfig,
    pub allowlist: Option<crate::security::Allowlist>,
}

/// Options for constructing an edge node swarm (relay client + AutoNAT).
#[derive(Debug, Clone)]
pub struct NodeSwarmOptions {
    pub enable_mdns: bool,
    pub enable_relay_client: bool,
}

impl Default for NodeSwarmOptions {
    fn default() -> Self {
        Self {
            enable_mdns: false,
            enable_relay_client: true,
        }
    }
}

#[derive(NetworkBehaviour)]
pub struct RelayServerBehaviour {
    pub relay: relay::Behaviour,
    pub identify: identify::Behaviour,
    pub ping: ping::Behaviour,
    pub registry_gossip: gossipsub::Behaviour,
    pub autonat: autonat::Behaviour,
    pub mdns: libp2p::swarm::behaviour::toggle::Toggle<mdns::tokio::Behaviour>,
    pub allowed_peers: libp2p::swarm::behaviour::toggle::Toggle<
        libp2p::allow_block_list::Behaviour<libp2p::allow_block_list::AllowedPeers>,
    >,
}

#[derive(NetworkBehaviour)]
pub struct RelayClientBehaviour {
    pub relay_client: relay::client::Behaviour,
    pub identify: identify::Behaviour,
    pub ping: ping::Behaviour,
    pub autonat: autonat::Behaviour,
    pub mdns: libp2p::swarm::behaviour::toggle::Toggle<mdns::tokio::Behaviour>,
}

#[derive(NetworkBehaviour)]
pub struct PingNodeBehaviour {
    pub identify: identify::Behaviour,
    pub ping: ping::Behaviour,
}

/// Load or generate the local Ed25519 keypair.
pub fn load_or_generate_keypair(path: Option<&Path>) -> Result<identity::Keypair> {
    if let Some(path) = path {
        let bytes = fs::read(path).with_context(|| format!("read keypair {}", path.display()))?;
        anyhow::ensure!(
            bytes.len() == 32,
            "keypair file must contain exactly 32 bytes, found {}",
            bytes.len()
        );
        let mut key_bytes = [0u8; 32];
        key_bytes.copy_from_slice(&bytes);
        return Ok(identity::Keypair::ed25519_from_bytes(key_bytes)?);
    }
    Ok(identity::Keypair::generate_ed25519())
}

/// Deterministic keypair for integration tests.
pub fn test_keypair(seed: u8) -> identity::Keypair {
    let mut bytes = [0u8; 32];
    bytes[0] = seed;
    identity::Keypair::ed25519_from_bytes(bytes).expect("valid ed25519 seed")
}

fn mdns_toggle(
    enable: bool,
    local_peer_id: PeerId,
) -> Result<libp2p::swarm::behaviour::toggle::Toggle<mdns::tokio::Behaviour>> {
    if enable {
        Ok(libp2p::swarm::behaviour::toggle::Toggle::from(Some(
            mdns::tokio::Behaviour::new(mdns::Config::default(), local_peer_id)?,
        )))
    } else {
        Ok(libp2p::swarm::behaviour::toggle::Toggle::from(None))
    }
}

pub fn build_relay_server_swarm(
    keypair: identity::Keypair,
    options: &RelayServerOptions,
) -> Result<Swarm<RelayServerBehaviour>> {
    let limits = RelayLimits::from_config(&options.config);
    let local_peer_id = keypair.public().to_peer_id();
    let mut registry_gossip = gossipsub::Behaviour::new(
        gossipsub::MessageAuthenticity::Signed(keypair.clone()),
        gossipsub::ConfigBuilder::default()
            .validation_mode(gossipsub::ValidationMode::Strict)
            .build()
            .map_err(|err| anyhow::anyhow!("build registry gossipsub config: {err}"))?,
    )
    .map_err(|err| anyhow::anyhow!("build registry gossipsub behaviour: {err}"))?;
    registry_gossip
        .subscribe(&gossipsub::IdentTopic::new(
            crate::registry::REGISTRY_GOSSIP_TOPIC,
        ))
        .context("subscribe registry gossip topic")?;

    let behaviour = RelayServerBehaviour {
        relay: new_relay_behaviour(local_peer_id, limits),
        identify: identify::Behaviour::new(identify::Config::new(
            KERYX_IDENTIFY_PROTOCOL.to_string(),
            keypair.public(),
        )),
        ping: ping::Behaviour::new(ping::Config::new()),
        registry_gossip,
        autonat: crate::autonat::new_autonat_server_behaviour(keypair.public()),
        mdns: mdns_toggle(options.config.enable_mdns, local_peer_id)?,
        allowed_peers: options
            .allowlist
            .as_ref()
            .map(crate::security::allowlist_behaviour_toggle)
            .unwrap_or_else(|| libp2p::swarm::behaviour::toggle::Toggle::from(None)),
    };

    let timeout = options.config.connection_timeout();
    let swarm = libp2p::SwarmBuilder::with_existing_identity(keypair)
        .with_tokio()
        .with_tcp(
            tcp::Config::default(),
            noise::Config::new,
            yamux::Config::default,
        )?
        .with_quic()
        .with_behaviour(|_| behaviour)?
        .with_swarm_config(|cfg| cfg.with_idle_connection_timeout(timeout))
        .build();

    Ok(swarm)
}

pub fn build_relay_client_swarm(
    keypair: identity::Keypair,
    options: &NodeSwarmOptions,
) -> Result<Swarm<RelayClientBehaviour>> {
    let local_peer_id = keypair.public().to_peer_id();
    let mdns = mdns_toggle(options.enable_mdns, local_peer_id)?;

    let swarm = libp2p::SwarmBuilder::with_existing_identity(keypair)
        .with_tokio()
        .with_tcp(
            tcp::Config::default().nodelay(true),
            noise::Config::new,
            yamux::Config::default,
        )?
        .with_quic()
        .with_relay_client(noise::Config::new, yamux::Config::default)?
        .with_behaviour(|keypair, relay_client| RelayClientBehaviour {
            relay_client,
            identify: identify::Behaviour::new(identify::Config::new(
                KERYX_IDENTIFY_PROTOCOL.to_string(),
                keypair.public(),
            )),
            ping: ping::Behaviour::new(ping::Config::new()),
            autonat: crate::autonat::new_autonat_client_behaviour(keypair.public()),
            mdns,
        })?
        .build();

    Ok(swarm)
}

pub fn build_ping_node_swarm(keypair: identity::Keypair) -> Result<Swarm<PingNodeBehaviour>> {
    let behaviour = PingNodeBehaviour {
        identify: identify::Behaviour::new(identify::Config::new(
            KERYX_IDENTIFY_PROTOCOL.to_string(),
            keypair.public(),
        )),
        ping: ping::Behaviour::new(ping::Config::new()),
    };

    let swarm = libp2p::SwarmBuilder::with_existing_identity(keypair)
        .with_tokio()
        .with_tcp(
            tcp::Config::default().nodelay(true),
            noise::Config::new,
            yamux::Config::default,
        )?
        .with_quic()
        .with_behaviour(|_| behaviour)?
        .build();

    Ok(swarm)
}

pub async fn listen_on_ephemeral_tcp<B>(swarm: &mut Swarm<B>) -> Result<Multiaddr>
where
    B: NetworkBehaviour,
{
    swarm.listen_on("/ip4/127.0.0.1/tcp/0".parse()?)?;
    crate::bootstrap::wait_for_listen_addr(swarm, Duration::from_secs(5))
        .await
        .then_some(())
        .ok_or_else(|| anyhow::anyhow!("timed out waiting for listen address"))?;
    swarm
        .listeners()
        .next()
        .cloned()
        .ok_or_else(|| anyhow::anyhow!("no listen address registered"))
}
