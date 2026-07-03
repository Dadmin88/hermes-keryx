//! Peer allowlist and connection gate for the relay server.

use std::collections::HashSet;
use std::path::Path;
use std::sync::{Arc, RwLock};

use anyhow::{Context, Result};
use libp2p::allow_block_list::{AllowedPeers, Behaviour as AllowBlockBehaviour};
use libp2p::identity;
use libp2p::swarm::behaviour::toggle::Toggle;
use libp2p::{PeerId, Swarm};
use serde::Deserialize;
use tracing::{info, warn};

use crate::transport::RelayServerBehaviour;

/// Policy when the allowlist file contains no peers.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum EmptyAllowlistPolicy {
    /// Reject all inbound peers (default).
    #[default]
    Deny,
    /// Do not enforce an allowlist (permit any peer).
    Allow,
}

/// In-memory set of authorized peers plus enforcement policy.
#[derive(Debug, Clone)]
pub struct Allowlist {
    peers: HashSet<PeerId>,
    empty_policy: EmptyAllowlistPolicy,
}

impl Allowlist {
    pub fn new(peers: HashSet<PeerId>, empty_policy: EmptyAllowlistPolicy) -> Self {
        Self {
            peers,
            empty_policy,
        }
    }

    pub fn empty_policy(&self) -> EmptyAllowlistPolicy {
        self.empty_policy
    }

    pub fn peers(&self) -> &HashSet<PeerId> {
        &self.peers
    }

    pub fn is_allowed(&self, peer_id: &PeerId) -> bool {
        match self.enforcement_mode() {
            EnforcementMode::AllowAll => true,
            EnforcementMode::DenyAll => false,
            EnforcementMode::AllowSet => self.peers.contains(peer_id),
        }
    }

    pub fn enforcement_mode(&self) -> EnforcementMode {
        if self.empty_policy == EmptyAllowlistPolicy::Allow && self.peers.is_empty() {
            return EnforcementMode::AllowAll;
        }
        if self.peers.is_empty() {
            return EnforcementMode::DenyAll;
        }
        EnforcementMode::AllowSet
    }

    /// Load peer IDs and optional Ed25519 public keys from a TOML allowlist file.
    pub fn load(path: &Path, empty_policy: EmptyAllowlistPolicy) -> Result<Self> {
        if !path.exists() {
            return Ok(Self::new(HashSet::new(), empty_policy));
        }
        let raw = std::fs::read_to_string(path)
            .with_context(|| format!("read allowlist {}", path.display()))?;
        let file: AllowlistFile =
            toml::from_str(&raw).with_context(|| format!("parse allowlist {}", path.display()))?;
        let mut peers = HashSet::new();
        for entry in file.allowed {
            if let Some(peer_id) = entry.peer_id {
                let parsed = peer_id
                    .parse::<PeerId>()
                    .with_context(|| format!("invalid peer_id {peer_id}"))?;
                peers.insert(parsed);
                continue;
            }
            if let Some(key_b64) = entry.ed25519_public_key_b64 {
                let bytes = base64_decode_32(&key_b64)?;
                let keypair = identity::Keypair::ed25519_from_bytes(bytes)
                    .context("invalid ed25519 public key bytes")?;
                peers.insert(keypair.public().to_peer_id());
                continue;
            }
            anyhow::bail!("allowlist entry must set peer_id or ed25519_public_key_b64");
        }
        Ok(Self::new(peers, empty_policy))
    }

    /// Replace allowlist contents from disk and return the new snapshot.
    pub fn reload(&mut self, path: &Path) -> Result<()> {
        let fresh = Self::load(path, self.empty_policy)?;
        self.peers = fresh.peers;
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EnforcementMode {
    AllowAll,
    DenyAll,
    AllowSet,
}

#[derive(Debug, Deserialize)]
struct AllowlistFile {
    #[serde(default)]
    allowed: Vec<AllowlistEntry>,
}

#[derive(Debug, Deserialize)]
struct AllowlistEntry {
    peer_id: Option<String>,
    ed25519_public_key_b64: Option<String>,
}

/// Security-related settings from the relay TOML config.
#[derive(Debug, Clone, Deserialize, Default)]
pub struct SecurityConfig {
    #[serde(default)]
    pub allowlist_path: Option<std::path::PathBuf>,
    #[serde(default)]
    pub empty_allowlist_policy: EmptyAllowlistPolicy,
}

/// Registry section from relay TOML (reserved for skill registry phase).
#[derive(Debug, Clone, Deserialize)]
pub struct RegistryConfig {
    #[serde(default = "default_registry_ttl_seconds")]
    pub ttl_seconds: u64,
    #[serde(default = "default_max_skills_per_peer")]
    pub max_skills_per_peer: usize,
}

fn default_registry_ttl_seconds() -> u64 {
    300
}

fn default_max_skills_per_peer() -> usize {
    64
}

impl Default for RegistryConfig {
    fn default() -> Self {
        Self {
            ttl_seconds: default_registry_ttl_seconds(),
            max_skills_per_peer: default_max_skills_per_peer(),
        }
    }
}

/// Top-level relay process TOML configuration.
#[derive(Debug, Clone, Deserialize)]
pub struct RelayTomlConfig {
    #[serde(default)]
    pub relay: RelayTomlRelaySection,
    #[serde(default)]
    pub security: SecurityConfig,
    #[serde(default)]
    pub registry: RegistryConfig,
}

#[derive(Debug, Clone, Deserialize)]
pub struct RelayTomlRelaySection {
    #[serde(default)]
    pub listen_addresses: Vec<String>,
    #[serde(default)]
    pub bootstrap_peers: Vec<String>,
    #[serde(default)]
    pub enable_mdns: bool,
    #[serde(default)]
    pub keypair_path: Option<std::path::PathBuf>,
    #[serde(default = "crate::config::default_max_circuits")]
    pub max_circuits: usize,
    #[serde(default)]
    pub max_connections: Option<usize>,
    #[serde(default = "crate::config::default_max_reservations")]
    pub max_reservations: usize,
    #[serde(default = "crate::config::default_max_reservations_per_peer")]
    pub max_reservations_per_peer: usize,
    #[serde(default = "crate::config::default_connection_timeout_ms")]
    pub connection_timeout_ms: u64,
    #[serde(default)]
    pub use_ipv6: bool,
}

impl Default for RelayTomlRelaySection {
    fn default() -> Self {
        Self {
            listen_addresses: Vec::new(),
            bootstrap_peers: Vec::new(),
            enable_mdns: false,
            keypair_path: None,
            max_circuits: crate::config::default_max_circuits(),
            max_connections: None,
            max_reservations: crate::config::default_max_reservations(),
            max_reservations_per_peer: crate::config::default_max_reservations_per_peer(),
            connection_timeout_ms: crate::config::default_connection_timeout_ms(),
            use_ipv6: false,
        }
    }
}

impl RelayTomlConfig {
    pub fn load(path: &Path) -> Result<Self> {
        let raw = std::fs::read_to_string(path)
            .with_context(|| format!("read relay config {}", path.display()))?;
        toml::from_str(&raw).with_context(|| format!("parse relay config {}", path.display()))
    }

    pub fn to_relay_config(&self) -> crate::config::RelayConfig {
        let max_circuits = self
            .relay
            .max_connections
            .unwrap_or(self.relay.max_circuits);
        crate::config::RelayConfig {
            listen_addresses: self.relay.listen_addresses.clone(),
            bootstrap_peers: self.relay.bootstrap_peers.clone(),
            enable_mdns: self.relay.enable_mdns,
            keypair_path: self.relay.keypair_path.clone(),
            max_circuits,
            max_reservations: self.relay.max_reservations,
            max_reservations_per_peer: self.relay.max_reservations_per_peer,
            connection_timeout_ms: self.relay.connection_timeout_ms,
            use_ipv6: self.relay.use_ipv6,
            health_grpc_bind: crate::config::default_health_grpc_bind(),
            health_http_bind: crate::config::default_health_http_bind(),
            registry_grpc_bind: crate::config::default_registry_grpc_bind(),
        }
    }

    pub fn resolved_allowlist_path(
        &self,
        config_path: &Path,
    ) -> Result<Option<std::path::PathBuf>> {
        self.security
            .allowlist_path
            .as_ref()
            .map(|p| resolve_path(config_path, p))
            .transpose()
    }

    pub fn load_allowlist(&self, config_path: &Path) -> Result<Allowlist> {
        let policy = self.security.empty_allowlist_policy;
        let Some(path) = self.resolved_allowlist_path(config_path)? else {
            return Ok(Allowlist::new(HashSet::new(), policy));
        };
        Allowlist::load(&path, policy)
    }
}

pub type SharedAllowlist = Arc<RwLock<Allowlist>>;

pub fn new_shared_allowlist(allowlist: Allowlist) -> SharedAllowlist {
    Arc::new(RwLock::new(allowlist))
}

/// Build the libp2p allow-list behaviour toggle from an [`Allowlist`] snapshot.
pub fn allowlist_behaviour_toggle(
    allowlist: &Allowlist,
) -> Toggle<AllowBlockBehaviour<AllowedPeers>> {
    match allowlist.enforcement_mode() {
        EnforcementMode::AllowAll => Toggle::from(None),
        EnforcementMode::DenyAll | EnforcementMode::AllowSet => {
            let mut behaviour = AllowBlockBehaviour::<AllowedPeers>::default();
            for peer in &allowlist.peers {
                behaviour.allow_peer(*peer);
            }
            Toggle::from(Some(behaviour))
        }
    }
}

/// Apply the current allowlist to a running relay-server swarm (used after SIGHUP reload).
pub fn sync_allowlist_to_swarm(swarm: &mut Swarm<RelayServerBehaviour>, allowlist: &Allowlist) {
    let mode = allowlist.enforcement_mode();
    swarm.behaviour_mut().allowed_peers = allowlist_behaviour_toggle(allowlist);
    match mode {
        EnforcementMode::AllowAll => {
            info!("allowlist enforcement disabled (empty allowlist policy=allow)");
        }
        EnforcementMode::DenyAll => {
            warn!("allowlist empty with deny policy — rejecting all peers");
        }
        EnforcementMode::AllowSet => {
            info!(count = allowlist.peers.len(), "allowlist synced to swarm");
        }
    }
}

fn resolve_path(base: &Path, relative: &Path) -> Result<std::path::PathBuf> {
    if relative.is_absolute() {
        return Ok(relative.to_path_buf());
    }
    let parent = base
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    Ok(parent.join(relative))
}

fn base64_decode_32(input: &str) -> Result<[u8; 32]> {
    use base64::Engine;
    let bytes = base64::engine::general_purpose::STANDARD
        .decode(input.trim())
        .context("invalid base64 in ed25519_public_key_b64")?;
    anyhow::ensure!(
        bytes.len() == 32,
        "ed25519 public key must be 32 bytes, found {}",
        bytes.len()
    );
    let mut out = [0u8; 32];
    out.copy_from_slice(&bytes);
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transport::test_keypair;

    #[test]
    fn empty_allowlist_deny_denies_everyone() {
        let list = Allowlist::new(HashSet::new(), EmptyAllowlistPolicy::Deny);
        let peer = test_keypair(1).public().to_peer_id();
        assert!(!list.is_allowed(&peer));
        assert_eq!(list.enforcement_mode(), EnforcementMode::DenyAll);
    }

    #[test]
    fn empty_allowlist_allow_permits_everyone() {
        let list = Allowlist::new(HashSet::new(), EmptyAllowlistPolicy::Allow);
        let peer = test_keypair(1).public().to_peer_id();
        assert!(list.is_allowed(&peer));
        assert_eq!(list.enforcement_mode(), EnforcementMode::AllowAll);
    }

    #[test]
    fn loads_peer_ids_from_toml() {
        let peer = test_keypair(9).public().to_peer_id();
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("allow.toml");
        std::fs::write(&path, format!("[[allowed]]\npeer_id = \"{}\"\n", peer)).unwrap();
        let list = Allowlist::load(&path, EmptyAllowlistPolicy::Deny).unwrap();
        assert!(list.is_allowed(&peer));
        assert!(!list.is_allowed(&test_keypair(10).public().to_peer_id()));
    }

    #[test]
    fn reload_picks_up_new_keys() {
        let peer_a = test_keypair(11).public().to_peer_id();
        let peer_b = test_keypair(12).public().to_peer_id();
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("allow.toml");
        std::fs::write(&path, format!("[[allowed]]\npeer_id = \"{peer_a}\"\n")).unwrap();
        let mut list = Allowlist::load(&path, EmptyAllowlistPolicy::Deny).unwrap();
        assert!(list.is_allowed(&peer_a));
        assert!(!list.is_allowed(&peer_b));
        std::fs::write(
            &path,
            format!("[[allowed]]\npeer_id = \"{peer_a}\"\n\n[[allowed]]\npeer_id = \"{peer_b}\"\n"),
        )
        .unwrap();
        list.reload(&path).unwrap();
        assert!(list.is_allowed(&peer_b));
    }
}
