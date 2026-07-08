//! Circuit Relay v2 server behaviour and resource limits.

use libp2p::relay;
use libp2p::PeerId;

/// Resource limits applied to the relay server.
#[derive(Debug, Clone, Copy)]
pub struct RelayLimits {
    pub max_circuits: usize,
    pub max_reservations: usize,
    pub max_reservations_per_peer: usize,
}

impl RelayLimits {
    pub fn from_config(config: &crate::config::RelayConfig) -> Self {
        Self {
            max_circuits: config.max_circuits,
            max_reservations: config.max_reservations,
            max_reservations_per_peer: config.max_reservations_per_peer,
        }
    }
}

/// Build a relay v2 server behaviour with reservation / circuit caps.
pub fn new_relay_behaviour(local_peer_id: PeerId, limits: RelayLimits) -> relay::Behaviour {
    let relay_config = relay::Config {
        max_circuits: limits.max_circuits,
        max_reservations: limits.max_reservations,
        max_reservations_per_peer: limits.max_reservations_per_peer,
        reservation_rate_limiters: vec![],
        ..Default::default()
    };
    relay::Behaviour::new(local_peer_id, relay_config)
}
