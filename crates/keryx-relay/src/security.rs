//! Peer allowlist and connection gate for the relay server.

use std::collections::{HashMap, HashSet};
use std::fmt;
use std::path::Path;
use std::sync::{Arc, RwLock};

use anyhow::{Context, Result};
use keryx_core::NodeId;
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

/// Node-token registry used to authenticate relay control-plane callers.
#[derive(Clone, Default)]
pub struct NodeTokenAuth {
    tokens: HashMap<NodeId, String>,
    revoked_nodes: HashSet<NodeId>,
}

impl fmt::Debug for NodeTokenAuth {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("NodeTokenAuth")
            .field("tokens", &format_args!("{} configured", self.tokens.len()))
            .field("revoked_nodes", &self.revoked_nodes)
            .finish()
    }
}

impl NodeTokenAuth {
    #[must_use]
    pub fn new(tokens: HashMap<NodeId, String>, revoked_nodes: HashSet<NodeId>) -> Self {
        Self {
            tokens,
            revoked_nodes,
        }
    }

    #[must_use]
    pub fn is_configured(&self) -> bool {
        !self.tokens.is_empty()
    }

    #[must_use]
    pub fn is_revoked(&self, node_id: &NodeId) -> bool {
        self.revoked_nodes.contains(node_id)
    }

    pub fn authenticate(
        &self,
        node_id: &NodeId,
        presented_token: &str,
    ) -> std::result::Result<NodeAuthSuccess, NodeAuthFailure> {
        self.authenticate_optional(node_id, Some(presented_token))
    }

    pub fn authenticate_optional(
        &self,
        node_id: &NodeId,
        presented_token: Option<&str>,
    ) -> std::result::Result<NodeAuthSuccess, NodeAuthFailure> {
        if self.is_revoked(node_id) {
            let failure = NodeAuthFailure::RevokedNode {
                node_id: node_id.to_string(),
            };
            audit_node_auth_failure(&failure);
            return Err(failure);
        }

        let Some(expected) = self.tokens.get(node_id) else {
            let failure = NodeAuthFailure::UnknownNode {
                node_id: node_id.to_string(),
            };
            audit_node_auth_failure(&failure);
            return Err(failure);
        };

        let Some(presented_token) = presented_token else {
            let failure = NodeAuthFailure::MissingToken {
                node_id: node_id.to_string(),
            };
            audit_node_auth_failure(&failure);
            return Err(failure);
        };

        if constant_time_eq(expected.as_bytes(), presented_token.as_bytes()) {
            audit_node_auth_success(node_id);
            Ok(NodeAuthSuccess {
                node_id: node_id.clone(),
            })
        } else {
            let failure = NodeAuthFailure::InvalidToken {
                node_id: node_id.to_string(),
            };
            audit_node_auth_failure(&failure);
            Err(failure)
        }
    }

    pub fn load(path: &Path) -> Result<Self> {
        let raw = std::fs::read_to_string(path)
            .with_context(|| format!("read node token auth {}", path.display()))?;
        let file: NodeTokenFile = toml::from_str(&raw)
            .with_context(|| format!("parse node token auth {}", path.display()))?;
        Self::from_entries(file.tokens, file.revoked_nodes)
    }

    fn from_entries(entries: Vec<NodeTokenEntry>, revoked_nodes: Vec<String>) -> Result<Self> {
        let mut tokens = HashMap::new();
        for entry in entries {
            let node_id = entry
                .node_id
                .parse::<NodeId>()
                .with_context(|| format!("invalid node_id {}", entry.node_id))?;
            anyhow::ensure!(
                entry.token.trim().len() >= 16,
                "node token for {node_id} must be at least 16 bytes"
            );
            tokens.insert(node_id, entry.token.trim().to_string());
        }

        let revoked_nodes = revoked_nodes
            .into_iter()
            .map(|value| {
                value
                    .parse::<NodeId>()
                    .with_context(|| format!("invalid revoked node_id {value}"))
            })
            .collect::<Result<HashSet<_>>>()?;

        Ok(Self::new(tokens, revoked_nodes))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeAuthSuccess {
    pub node_id: NodeId,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NodeAuthFailure {
    UnknownNode { node_id: String },
    MissingToken { node_id: String },
    InvalidToken { node_id: String },
    RevokedNode { node_id: String },
}

impl NodeAuthFailure {
    #[must_use]
    pub fn node_id(&self) -> &str {
        match self {
            Self::UnknownNode { node_id }
            | Self::MissingToken { node_id }
            | Self::InvalidToken { node_id }
            | Self::RevokedNode { node_id } => node_id,
        }
    }

    #[must_use]
    pub const fn reason(&self) -> &'static str {
        match self {
            Self::UnknownNode { .. } => "unknown_node",
            Self::MissingToken { .. } => "missing_token",
            Self::InvalidToken { .. } => "invalid_token",
            Self::RevokedNode { .. } => "revoked_node",
        }
    }
}

#[derive(Debug, Deserialize)]
struct NodeTokenFile {
    #[serde(default)]
    tokens: Vec<NodeTokenEntry>,
    #[serde(default)]
    revoked_nodes: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct NodeTokenEntry {
    node_id: String,
    token: String,
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
#[derive(Clone, Deserialize, Default)]
pub struct SecurityConfig {
    #[serde(default)]
    pub allowlist_path: Option<std::path::PathBuf>,
    #[serde(default)]
    pub empty_allowlist_policy: EmptyAllowlistPolicy,
    #[serde(default)]
    pub node_tokens_path: Option<std::path::PathBuf>,
    #[serde(default)]
    pub node_tokens: Vec<NodeTokenConfig>,
    #[serde(default)]
    pub revoked_nodes: Vec<String>,
}

impl fmt::Debug for SecurityConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SecurityConfig")
            .field("allowlist_path", &self.allowlist_path)
            .field("empty_allowlist_policy", &self.empty_allowlist_policy)
            .field("node_tokens_path", &self.node_tokens_path)
            .field(
                "node_tokens",
                &format_args!("{} configured", self.node_tokens.len()),
            )
            .field("revoked_nodes", &self.revoked_nodes)
            .finish()
    }
}

#[derive(Clone, Deserialize)]
pub struct NodeTokenConfig {
    pub node_id: String,
    pub token: String,
}

impl fmt::Debug for NodeTokenConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("NodeTokenConfig")
            .field("node_id", &self.node_id)
            .field("token", &"[REDACTED]")
            .finish()
    }
}

impl From<NodeTokenConfig> for NodeTokenEntry {
    fn from(value: NodeTokenConfig) -> Self {
        Self {
            node_id: value.node_id,
            token: value.token,
        }
    }
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

    pub fn resolved_node_tokens_path(
        &self,
        config_path: &Path,
    ) -> Result<Option<std::path::PathBuf>> {
        self.security
            .node_tokens_path
            .as_ref()
            .map(|p| resolve_path(config_path, p))
            .transpose()
    }

    pub fn load_node_token_auth(&self, config_path: &Path) -> Result<NodeTokenAuth> {
        let mut auth = if let Some(path) = self.resolved_node_tokens_path(config_path)? {
            NodeTokenAuth::load(&path)?
        } else {
            NodeTokenAuth::default()
        };

        let inline_entries = self
            .security
            .node_tokens
            .clone()
            .into_iter()
            .map(NodeTokenEntry::from)
            .collect::<Vec<_>>();
        let inline =
            NodeTokenAuth::from_entries(inline_entries, self.security.revoked_nodes.clone())?;
        auth.tokens.extend(inline.tokens);
        auth.revoked_nodes.extend(inline.revoked_nodes);
        Ok(auth)
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

fn audit_node_auth_success(node_id: &NodeId) {
    info!(
        target: "keryx.security",
        audit_event = "node_token_auth",
        decision = "allow",
        node_id = %node_id,
        "node token authentication accepted"
    );
}

fn audit_node_auth_failure(failure: &NodeAuthFailure) {
    warn!(
        target: "keryx.security",
        audit_event = "node_token_auth",
        decision = "deny",
        reason = failure.reason(),
        node_id = %failure.node_id(),
        "node token authentication denied"
    );
}

fn constant_time_eq(left: &[u8], right: &[u8]) -> bool {
    let max_len = left.len().max(right.len());
    let mut diff = left.len() ^ right.len();
    for index in 0..max_len {
        let left_byte = left.get(index).copied().unwrap_or(0);
        let right_byte = right.get(index).copied().unwrap_or(0);
        diff |= usize::from(left_byte ^ right_byte);
    }
    diff == 0
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

    #[test]
    fn node_token_auth_accepts_valid_token_and_rejects_bad_or_revoked_nodes() {
        let node_a: NodeId = "node:a".parse().unwrap();
        let node_b: NodeId = "node:b".parse().unwrap();
        let mut tokens = HashMap::new();
        tokens.insert(node_a.clone(), "token-a".to_string());
        tokens.insert(node_b.clone(), "token-b".to_string());
        let revoked = HashSet::from([node_b.clone()]);
        let auth = NodeTokenAuth::new(tokens, revoked);

        assert_eq!(
            auth.authenticate(&node_a, "token-a").unwrap().node_id,
            node_a
        );
        assert!(matches!(
            auth.authenticate(&node_a, "wrong"),
            Err(NodeAuthFailure::InvalidToken { .. })
        ));
        assert!(matches!(
            auth.authenticate(&node_b, "token-b"),
            Err(NodeAuthFailure::RevokedNode { .. })
        ));
    }

    #[test]
    fn relay_config_loads_inline_node_tokens() {
        let raw = r#"
            [relay]
            listen_multiaddr = "/ip4/127.0.0.1/tcp/0"

            [[security.node_tokens]]
            node_id = "node:worker"
            token = "node-token-secure-1234567890"
        "#;
        let config: RelayTomlConfig = toml::from_str(raw).unwrap();
        let auth = config
            .load_node_token_auth(Path::new("relay.toml"))
            .unwrap();
        let node_id: NodeId = "node:worker".parse().unwrap();
        assert!(auth.is_configured());
        assert!(auth
            .authenticate(&node_id, "node-token-secure-1234567890")
            .is_ok());
    }
}
