//! AutoNAT client/server helpers and reachability reporting.

use std::sync::{Arc, RwLock};
use std::time::Duration;

use libp2p::autonat;
use libp2p::identity::PublicKey;
use libp2p::PeerId;

/// Coarse NAT reachability classification exposed to operators and tests.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, serde::Serialize, serde::Deserialize)]
pub enum NatReachability {
    #[default]
    Unknown,
    Public,
    Private,
}

/// Latest AutoNAT-derived status for a swarm.
#[derive(Debug, Clone, Default)]
pub struct NatStatus {
    inner: Arc<RwLock<NatReachability>>,
}

impl NatStatus {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn set(&self, reachability: NatReachability) {
        if let Ok(mut guard) = self.inner.write() {
            *guard = reachability;
        }
    }

    pub fn get(&self) -> NatReachability {
        self.inner
            .read()
            .map(|g| *g)
            .unwrap_or(NatReachability::Unknown)
    }
}

/// AutoNAT server behaviour (used by the relay binary).
pub fn new_autonat_server_behaviour(local_public_key: PublicKey) -> autonat::Behaviour {
    autonat::Behaviour::new(
        local_public_key.to_peer_id(),
        autonat::Config {
            only_global_ips: false,
            ..Default::default()
        },
    )
}

/// AutoNAT client behaviour with periodic probes suitable for edge nodes.
pub fn new_autonat_client_behaviour(local_public_key: PublicKey) -> autonat::Behaviour {
    autonat::Behaviour::new(
        local_public_key.to_peer_id(),
        autonat::Config {
            retry_interval: Duration::from_secs(10),
            refresh_interval: Duration::from_secs(30),
            boot_delay: Duration::from_secs(2),
            throttle_server_period: Duration::ZERO,
            only_global_ips: false,
            ..Default::default()
        },
    )
}

/// Register a known AutoNAT server on a client behaviour.
pub fn add_autonat_server(
    behaviour: &mut autonat::Behaviour,
    server_peer_id: PeerId,
    server_address: Option<libp2p::Multiaddr>,
) {
    behaviour.add_server(server_peer_id, server_address);
}

/// Map a libp2p AutoNAT v1 status event into our coarse enum.
pub fn map_autonat_status(status: autonat::NatStatus) -> NatReachability {
    match status {
        autonat::NatStatus::Public(_) => NatReachability::Public,
        autonat::NatStatus::Private => NatReachability::Private,
        autonat::NatStatus::Unknown => NatReachability::Unknown,
    }
}
