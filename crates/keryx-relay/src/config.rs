//! Relay process configuration.

use std::net::{Ipv4Addr, Ipv6Addr};
use std::path::PathBuf;
use std::time::Duration;

use libp2p::core::multiaddr::Protocol;
use libp2p::Multiaddr;
use serde::{Deserialize, Serialize};

/// Default TCP listen port when none is configured.
pub const DEFAULT_TCP_PORT: u16 = 4001;

/// Default QUIC listen port when none is configured.
pub const DEFAULT_QUIC_PORT: u16 = 4001;

/// Application protocol string advertised via identify.
pub const KERYX_IDENTIFY_PROTOCOL: &str = "/hermes/keryx/relay/0.1.0";

/// Resource and network settings for the relay binary.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RelayConfig {
    /// Multiaddrs or `host:port` strings to listen on. Empty uses all interfaces on default ports.
    #[serde(default)]
    pub listen_addresses: Vec<String>,
    /// Bootstrap peer multiaddrs (may include `/p2p/<peer_id>`).
    #[serde(default)]
    pub bootstrap_peers: Vec<String>,
    /// Enable mDNS peer discovery on the local network.
    #[serde(default)]
    pub enable_mdns: bool,
    /// Path to a 32-byte Ed25519 secret key file. When absent, an ephemeral key is generated.
    #[serde(default)]
    pub keypair_path: Option<PathBuf>,
    /// Maximum concurrent relay circuits.
    #[serde(default = "default_max_circuits")]
    pub max_circuits: usize,
    /// Maximum relay reservations across all peers.
    #[serde(default = "default_max_reservations")]
    pub max_reservations: usize,
    /// Maximum relay reservations per peer.
    #[serde(default = "default_max_reservations_per_peer")]
    pub max_reservations_per_peer: usize,
    /// Outbound dial / handshake timeout.
    #[serde(default = "default_connection_timeout_ms")]
    pub connection_timeout_ms: u64,
    /// Prefer IPv6 for default listen addresses.
    #[serde(default)]
    pub use_ipv6: bool,
    /// gRPC bind address for relay APIs (health today). Empty disables gRPC health.
    #[serde(default = "default_health_grpc_bind")]
    pub health_grpc_bind: String,
    /// HTTP bind address for `GET /health`. Empty disables HTTP health.
    #[serde(default = "default_health_http_bind")]
    pub health_http_bind: String,
    /// gRPC bind address for the skill registry API. Empty disables registry gRPC.
    #[serde(default = "default_registry_grpc_bind")]
    pub registry_grpc_bind: String,
}

pub fn default_max_circuits() -> usize {
    256
}

pub fn default_max_reservations() -> usize {
    128
}

pub fn default_max_reservations_per_peer() -> usize {
    4
}

pub fn default_connection_timeout_ms() -> u64 {
    30_000
}

pub fn default_health_grpc_bind() -> String {
    "127.0.0.1:50052".to_string()
}

pub fn default_health_http_bind() -> String {
    "127.0.0.1:8081".to_string()
}

pub fn default_registry_grpc_bind() -> String {
    "127.0.0.1:50053".to_string()
}

impl Default for RelayConfig {
    fn default() -> Self {
        Self {
            listen_addresses: Vec::new(),
            bootstrap_peers: Vec::new(),
            enable_mdns: false,
            keypair_path: None,
            max_circuits: default_max_circuits(),
            max_reservations: default_max_reservations(),
            max_reservations_per_peer: default_max_reservations_per_peer(),
            connection_timeout_ms: default_connection_timeout_ms(),
            use_ipv6: false,
            health_grpc_bind: default_health_grpc_bind(),
            health_http_bind: default_health_http_bind(),
            registry_grpc_bind: default_registry_grpc_bind(),
        }
    }
}

impl RelayConfig {
    /// Load JSON config from disk, or return defaults when the path is missing.
    pub fn load(path: &std::path::Path) -> anyhow::Result<Self> {
        if !path.exists() {
            return Ok(Self::default());
        }
        let raw = std::fs::read_to_string(path)?;
        Ok(serde_json::from_str(&raw)?)
    }

    pub fn parse_health_grpc_bind(&self) -> anyhow::Result<Option<std::net::SocketAddr>> {
        parse_optional_socket_addr(&self.health_grpc_bind)
    }

    pub fn parse_health_http_bind(&self) -> anyhow::Result<Option<std::net::SocketAddr>> {
        parse_optional_socket_addr(&self.health_http_bind)
    }

    pub fn parse_registry_grpc_bind(&self) -> anyhow::Result<Option<std::net::SocketAddr>> {
        parse_optional_socket_addr(&self.registry_grpc_bind)
    }

    pub fn connection_timeout(&self) -> Duration {
        Duration::from_millis(self.connection_timeout_ms)
    }

    /// Resolved listen multiaddrs, synthesizing defaults when the list is empty.
    pub fn resolved_listen_addresses(&self) -> anyhow::Result<Vec<Multiaddr>> {
        if self.listen_addresses.is_empty() {
            return Ok(default_listen_addresses(self.use_ipv6));
        }
        self.listen_addresses
            .iter()
            .map(|raw| parse_listen_address(raw, self.use_ipv6))
            .collect()
    }

    pub fn parsed_bootstrap_peers(&self) -> anyhow::Result<Vec<Multiaddr>> {
        self.bootstrap_peers
            .iter()
            .map(|raw| {
                raw.parse::<Multiaddr>()
                    .map_err(|e| anyhow::anyhow!("invalid bootstrap multiaddr {raw}: {e}"))
            })
            .collect()
    }
}

fn default_listen_addresses(use_ipv6: bool) -> Vec<Multiaddr> {
    let ip = if use_ipv6 {
        Protocol::from(Ipv6Addr::UNSPECIFIED)
    } else {
        Protocol::from(Ipv4Addr::UNSPECIFIED)
    };
    vec![
        Multiaddr::empty()
            .with(ip.clone())
            .with(Protocol::Tcp(DEFAULT_TCP_PORT)),
        Multiaddr::empty()
            .with(ip)
            .with(Protocol::Udp(DEFAULT_QUIC_PORT))
            .with(Protocol::QuicV1),
    ]
}

fn parse_listen_address(raw: &str, use_ipv6: bool) -> anyhow::Result<Multiaddr> {
    if raw.contains('/') {
        return raw
            .parse::<Multiaddr>()
            .map_err(|e| anyhow::anyhow!("invalid listen multiaddr {raw}: {e}"));
    }
    // Shorthand `tcp:4001` or `4001`.
    let port: u16 = raw
        .trim_start_matches("tcp:")
        .parse()
        .map_err(|_| anyhow::anyhow!("invalid listen address {raw}"))?;
    let ip = if use_ipv6 {
        Protocol::from(Ipv6Addr::UNSPECIFIED)
    } else {
        Protocol::from(Ipv4Addr::UNSPECIFIED)
    };
    Ok(Multiaddr::empty().with(ip).with(Protocol::Tcp(port)))
}

fn parse_optional_socket_addr(raw: &str) -> anyhow::Result<Option<std::net::SocketAddr>> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Ok(None);
    }
    Ok(Some(trimmed.parse().map_err(|e| {
        anyhow::anyhow!("invalid socket address {trimmed}: {e}")
    })?))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_listen_addresses_include_tcp_and_quic() {
        let addrs = default_listen_addresses(false);
        assert_eq!(addrs.len(), 2);
        assert!(addrs[0].to_string().contains("/tcp/"));
        assert!(addrs[1].to_string().contains("/quic-v1"));
    }

    #[test]
    fn parses_shorthand_port() {
        let addr = parse_listen_address("4100", false).unwrap();
        assert_eq!(addr.to_string(), "/ip4/0.0.0.0/tcp/4100");
    }
}
