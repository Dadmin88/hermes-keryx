//! Keryx security policy, node identity, task approval, and redaction helpers.
//!
//! Phase 14A intentionally keeps the policy crate self-contained so daemon and
//! relay code can make deterministic security decisions without depending on a
//! process-global service. Secrets are never exposed through `Debug`/`Display`;
//! use `expose_secret` only at the cryptographic/authentication boundary.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::str::FromStr;

pub use keryx_core::{
    validate_transition, AgentId, CapabilityId, KeryxEventType, PeerId, TaskId, TaskStatus,
    TaskTransition, ValidationError,
};

const NODE_KEY_BYTES: usize = 32;
const NODE_KEY_PREFIX: &str = "keryx-node-key-v1:";

/// Canonical location for a node's long-lived identity key.
#[must_use]
pub fn default_node_key_path() -> PathBuf {
    let home = std::env::var_os("HOME")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("."));
    home.join(".hermes/keryx/identity/node.key")
}

/// A long-lived node identity key stored on disk as private material.
#[derive(Clone, PartialEq, Eq)]
pub struct NodeKey {
    secret: [u8; NODE_KEY_BYTES],
}

impl NodeKey {
    /// Generate a fresh node key from the operating system CSPRNG.
    pub fn generate() -> std::io::Result<Self> {
        let mut secret = [0u8; NODE_KEY_BYTES];
        File::open("/dev/urandom")?.read_exact(&mut secret)?;
        Ok(Self { secret })
    }

    /// Load an existing key or create one at the default path.
    pub fn load_or_generate_default() -> std::io::Result<Self> {
        Self::load_or_generate(default_node_key_path())
    }

    /// Load an existing key or atomically create and persist a new one.
    pub fn load_or_generate(path: impl AsRef<Path>) -> std::io::Result<Self> {
        let path = path.as_ref();
        if path.exists() {
            return Self::load(path);
        }
        let key = Self::generate()?;
        key.store(path)?;
        Ok(key)
    }

    /// Load a key from disk. Accepts the current prefixed format and legacy raw hex.
    pub fn load(path: impl AsRef<Path>) -> std::io::Result<Self> {
        let raw = fs::read_to_string(path)?;
        Self::from_persisted_str(raw.trim()).map_err(|message| {
            std::io::Error::new(std::io::ErrorKind::InvalidData, message.to_string())
        })
    }

    /// Persist the key with 0700 parent directory and 0600 key-file permissions on Unix.
    pub fn store(&self, path: impl AsRef<Path>) -> std::io::Result<()> {
        let path = path.as_ref();
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
            set_private_dir_permissions(parent)?;
        }
        let mut options = OpenOptions::new();
        options.write(true).create(true).truncate(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let mut file = options.open(path)?;
        file.write_all(self.to_persisted_string().as_bytes())?;
        file.write_all(b"\n")?;
        file.sync_all()?;
        set_private_file_permissions(path)?;
        Ok(())
    }

    /// Stable node id derived from the private key without exposing the key itself.
    #[must_use]
    pub fn node_id(&self) -> NodeId {
        NodeId(format!("node-{}", hex_encode(&self.secret[..16])))
    }

    /// Returns the private key only for authentication/crypto boundaries.
    #[must_use]
    pub const fn expose_secret(&self) -> &[u8; NODE_KEY_BYTES] {
        &self.secret
    }

    #[must_use]
    pub fn to_persisted_string(&self) -> String {
        format!("{NODE_KEY_PREFIX}{}", hex_encode(&self.secret))
    }

    pub fn from_persisted_str(input: &str) -> Result<Self, &'static str> {
        let trimmed = input.trim();
        let hex = trimmed.strip_prefix(NODE_KEY_PREFIX).unwrap_or(trimmed);
        let bytes = hex_decode_exact::<NODE_KEY_BYTES>(hex)?;
        Ok(Self { secret: bytes })
    }
}

impl fmt::Debug for NodeKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("NodeKey")
            .field("node_id", &self.node_id())
            .field("secret", &Redacted)
            .finish()
    }
}

/// A stable identifier for a Keryx node.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct NodeId(String);

impl NodeId {
    pub fn new(value: impl AsRef<str>) -> Result<Self, IdentityError> {
        validate_component("NodeId", value.as_ref()).map(Self)
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for NodeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("NodeId").field(&self.0).finish()
    }
}

impl fmt::Display for NodeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl FromStr for NodeId {
    type Err = IdentityError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::new(value)
    }
}

/// Agent id scoped beneath the node that owns/runs the agent.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ScopedAgentIdentity {
    node_id: NodeId,
    agent_id: AgentId,
}

impl ScopedAgentIdentity {
    #[must_use]
    pub fn new(node_id: NodeId, agent_id: AgentId) -> Self {
        Self { node_id, agent_id }
    }

    pub fn parse(value: &str) -> Result<Self, IdentityError> {
        let (node, agent) = value
            .split_once('/')
            .ok_or(IdentityError::MalformedScopedAgent)?;
        Ok(Self {
            node_id: NodeId::new(node)?,
            agent_id: AgentId::new(agent)?,
        })
    }

    #[must_use]
    pub fn node_id(&self) -> &NodeId {
        &self.node_id
    }

    #[must_use]
    pub fn agent_id(&self) -> &AgentId {
        &self.agent_id
    }

    #[must_use]
    pub fn is_under_node(&self, node_id: &NodeId) -> bool {
        &self.node_id == node_id
    }
}

impl fmt::Debug for ScopedAgentIdentity {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ScopedAgentIdentity")
            .field("node_id", &self.node_id)
            .field("agent_id", &self.agent_id.as_str())
            .finish()
    }
}

impl fmt::Display for ScopedAgentIdentity {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}/{}", self.node_id, self.agent_id.as_str())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IdentityError {
    Empty { kind: &'static str },
    InvalidCharacter { kind: &'static str, value: String },
    MalformedScopedAgent,
    Validation(ValidationError),
}

impl fmt::Display for IdentityError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Empty { kind } => write!(f, "{kind} must not be empty"),
            Self::InvalidCharacter { kind, value } => {
                write!(f, "{kind} contains invalid characters: {value}")
            }
            Self::MalformedScopedAgent => f.write_str("scoped agent identity must be node/agent"),
            Self::Validation(err) => write!(f, "{err}"),
        }
    }
}

impl std::error::Error for IdentityError {}

impl From<ValidationError> for IdentityError {
    fn from(value: ValidationError) -> Self {
        Self::Validation(value)
    }
}

/// A bearer token used by a node when authenticating to a relay.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct NodeToken(String);

impl NodeToken {
    pub fn new(value: impl Into<String>) -> Result<Self, AuthError> {
        let value = value.into();
        let trimmed = value.trim();
        if trimmed.len() < 16 {
            return Err(AuthError::WeakToken);
        }
        Ok(Self(trimmed.to_string()))
    }

    pub fn generate() -> std::io::Result<Self> {
        let mut bytes = [0u8; 32];
        File::open("/dev/urandom")?.read_exact(&mut bytes)?;
        Ok(Self(hex_encode(&bytes)))
    }

    #[must_use]
    pub fn expose_secret(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for NodeToken {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("NodeToken([REDACTED])")
    }
}

impl fmt::Display for NodeToken {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("[REDACTED]")
    }
}

/// In-memory token verifier keyed by node id.
#[derive(Debug, Clone, Default)]
pub struct NodeTokenAuthenticator {
    tokens: BTreeMap<NodeId, NodeToken>,
    revoked: BTreeSet<NodeId>,
}

impl NodeTokenAuthenticator {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    pub fn allow_node(mut self, node_id: NodeId, token: NodeToken) -> Self {
        self.tokens.insert(node_id.clone(), token);
        self.revoked.remove(&node_id);
        self
    }

    pub fn insert(&mut self, node_id: NodeId, token: NodeToken) {
        self.tokens.insert(node_id.clone(), token);
        self.revoked.remove(&node_id);
    }

    pub fn revoke(&mut self, node_id: &NodeId) {
        self.revoked.insert(node_id.clone());
    }

    #[must_use]
    pub fn authenticate(&self, node_id: &NodeId, presented_token: &str) -> AuthDecision {
        if self.revoked.contains(node_id) {
            return AuthDecision::Denied(AuthError::RevokedNode);
        }
        let Some(expected) = self.tokens.get(node_id) else {
            return AuthDecision::Denied(AuthError::UnknownNode);
        };
        if constant_time_eq(
            expected.expose_secret().as_bytes(),
            presented_token.as_bytes(),
        ) {
            AuthDecision::Allowed
        } else {
            AuthDecision::Denied(AuthError::InvalidToken)
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AuthDecision {
    Allowed,
    Denied(AuthError),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AuthError {
    UnknownNode,
    InvalidToken,
    RevokedNode,
    MissingToken,
    WeakToken,
}

impl fmt::Display for AuthError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::UnknownNode => "unknown node",
            Self::InvalidToken => "invalid node token",
            Self::RevokedNode => "node is revoked",
            Self::MissingToken => "node token is required",
            Self::WeakToken => "node token must be at least 16 bytes",
        })
    }
}

impl std::error::Error for AuthError {}

/// Capability-level route decision.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum CapabilityPermission {
    #[default]
    Allow,
    Deny,
    ApprovalRequired,
}

impl CapabilityPermission {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Allow => "allow",
            Self::Deny => "deny",
            Self::ApprovalRequired => "approval_required",
        }
    }
}

impl FromStr for CapabilityPermission {
    type Err = PermissionParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match normalize_permission(value).as_str() {
            "allow" | "allowed" => Ok(Self::Allow),
            "deny" | "denied" => Ok(Self::Deny),
            "approvalrequired" | "approval_required" | "approval-required" | "requireapproval"
            | "requiresapproval" => Ok(Self::ApprovalRequired),
            _ => Err(PermissionParseError),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PermissionParseError;

impl fmt::Display for PermissionParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("unknown capability permission")
    }
}

impl std::error::Error for PermissionParseError {}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PolicySubject {
    pub node_id: NodeId,
    pub agent: Option<ScopedAgentIdentity>,
}

impl PolicySubject {
    #[must_use]
    pub fn node(node_id: NodeId) -> Self {
        Self {
            node_id,
            agent: None,
        }
    }

    #[must_use]
    pub fn agent(agent: ScopedAgentIdentity) -> Self {
        Self {
            node_id: agent.node_id().clone(),
            agent: Some(agent),
        }
    }
}

/// Rule set for capability dispatch. Specific node/agent rules override global rules.
#[derive(Debug, Clone)]
pub struct CapabilityPolicy {
    default_permission: CapabilityPermission,
    global_rules: BTreeMap<String, CapabilityPermission>,
    node_rules: BTreeMap<(NodeId, String), CapabilityPermission>,
    agent_rules: BTreeMap<(ScopedAgentIdentity, String), CapabilityPermission>,
}

impl Default for CapabilityPolicy {
    fn default() -> Self {
        Self::new(CapabilityPermission::Allow)
    }
}

impl CapabilityPolicy {
    #[must_use]
    pub fn new(default_permission: CapabilityPermission) -> Self {
        Self {
            default_permission,
            global_rules: BTreeMap::new(),
            node_rules: BTreeMap::new(),
            agent_rules: BTreeMap::new(),
        }
    }

    pub fn set_global(&mut self, capability: impl AsRef<str>, permission: CapabilityPermission) {
        self.global_rules
            .insert(capability.as_ref().to_string(), permission);
    }

    pub fn set_node(
        &mut self,
        node_id: NodeId,
        capability: impl AsRef<str>,
        permission: CapabilityPermission,
    ) {
        self.node_rules
            .insert((node_id, capability.as_ref().to_string()), permission);
    }

    pub fn set_agent(
        &mut self,
        agent: ScopedAgentIdentity,
        capability: impl AsRef<str>,
        permission: CapabilityPermission,
    ) {
        self.agent_rules
            .insert((agent, capability.as_ref().to_string()), permission);
    }

    #[must_use]
    pub fn evaluate(&self, subject: &PolicySubject, capability: &str) -> PolicyDecision {
        let permission = subject
            .agent
            .as_ref()
            .and_then(|agent| {
                self.agent_rules
                    .get(&(agent.clone(), capability.to_string()))
            })
            .copied()
            .or_else(|| {
                self.node_rules
                    .get(&(subject.node_id.clone(), capability.to_string()))
                    .copied()
            })
            .or_else(|| self.global_rules.get(capability).copied())
            .unwrap_or(self.default_permission);
        PolicyDecision::from_permission(permission, capability)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PolicyDecision {
    Allow { capability: String },
    Deny { capability: String, reason: String },
    ApprovalRequired { capability: String, reason: String },
}

impl PolicyDecision {
    #[must_use]
    pub fn from_permission(permission: CapabilityPermission, capability: &str) -> Self {
        match permission {
            CapabilityPermission::Allow => Self::Allow {
                capability: capability.to_string(),
            },
            CapabilityPermission::Deny => Self::Deny {
                capability: capability.to_string(),
                reason: "capability denied by policy".to_string(),
            },
            CapabilityPermission::ApprovalRequired => Self::ApprovalRequired {
                capability: capability.to_string(),
                reason: "operator approval required".to_string(),
            },
        }
    }

    #[must_use]
    pub const fn is_allowed(&self) -> bool {
        matches!(self, Self::Allow { .. })
    }

    #[must_use]
    pub const fn requires_approval(&self) -> bool {
        matches!(self, Self::ApprovalRequired { .. })
    }
}

/// Routing state for a task after policy evaluation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskApprovalState {
    Ready,
    AwaitingApproval,
    Denied,
}

/// Security-relevant audit event with metadata already redacted for safe output.
#[derive(Clone, PartialEq, Eq)]
pub struct SecurityAuditEvent {
    pub event_type: SecurityAuditEventType,
    pub node_id: Option<NodeId>,
    pub agent_id: Option<String>,
    pub capability: Option<String>,
    pub decision: String,
    pub reason: String,
    pub metadata: BTreeMap<String, String>,
}

impl SecurityAuditEvent {
    #[must_use]
    pub fn policy_decision(
        subject: Option<&PolicySubject>,
        capability: impl Into<String>,
        decision: &PolicyDecision,
    ) -> Self {
        let capability = capability.into();
        let (decision_label, reason) = match decision {
            PolicyDecision::Allow { .. } => ("allow", "capability allowed"),
            PolicyDecision::Deny { reason, .. } => ("deny", reason.as_str()),
            PolicyDecision::ApprovalRequired { reason, .. } => {
                ("approval_required", reason.as_str())
            }
        };
        Self {
            event_type: SecurityAuditEventType::CapabilityDecision,
            node_id: subject.map(|s| s.node_id.clone()),
            agent_id: subject.and_then(|s| s.agent.as_ref().map(ToString::to_string)),
            capability: Some(capability),
            decision: decision_label.to_string(),
            reason: redact_secrets(reason),
            metadata: BTreeMap::new(),
        }
    }

    #[must_use]
    pub fn auth_decision(node_id: Option<NodeId>, decision: &AuthDecision) -> Self {
        let decision_label = match decision {
            AuthDecision::Allowed => "allow",
            AuthDecision::Denied(_) => "deny",
        };
        let reason = match decision {
            AuthDecision::Allowed => "node token accepted".to_string(),
            AuthDecision::Denied(error) => error.to_string(),
        };
        Self {
            event_type: SecurityAuditEventType::NodeAuthentication,
            node_id,
            agent_id: None,
            capability: None,
            decision: decision_label.to_string(),
            reason: redact_secrets(&reason),
            metadata: BTreeMap::new(),
        }
    }

    #[must_use]
    pub fn with_metadata(mut self, key: impl Into<String>, value: impl AsRef<str>) -> Self {
        let key = key.into();
        let value = if is_secret_key(&key) {
            "[REDACTED]".to_string()
        } else {
            redact_secrets(value.as_ref())
        };
        self.metadata.insert(key, value);
        self
    }
}

impl fmt::Debug for SecurityAuditEvent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SecurityAuditEvent")
            .field("event_type", &self.event_type)
            .field("node_id", &self.node_id)
            .field("agent_id", &self.agent_id)
            .field("capability", &self.capability)
            .field("decision", &self.decision)
            .field("reason", &self.reason)
            .field("metadata", &self.metadata)
            .finish()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SecurityAuditEventType {
    NodeAuthentication,
    CapabilityDecision,
    ApprovalRequested,
    ApprovalGranted,
    ApprovalDenied,
    SecretRedacted,
}

/// Marker used by custom `Debug` impls for secret fields.
pub struct Redacted;

impl fmt::Debug for Redacted {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("[REDACTED]")
    }
}

impl fmt::Display for Redacted {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("[REDACTED]")
    }
}

/// Redact common secret-bearing keys and inline token/password assignments.
#[must_use]
pub fn redact_secrets(input: &str) -> String {
    input
        .split_whitespace()
        .map(redact_token)
        .collect::<Vec<_>>()
        .join(" ")
}

fn redact_token(token: &str) -> String {
    let trimmed = token.trim_matches(|ch: char| ch == ',' || ch == ';');
    let suffix = &token[trimmed.len()..];
    let (key, sep, _value) = if let Some((key, value)) = trimmed.split_once('=') {
        (key, "=", value)
    } else if let Some((key, value)) = trimmed.split_once(':') {
        (key, ":", value)
    } else {
        return if looks_like_bearer_token(trimmed) {
            "[REDACTED]".to_string()
        } else {
            token.to_string()
        };
    };
    if is_secret_key(key) {
        format!("{key}{sep}[REDACTED]{suffix}")
    } else {
        token.to_string()
    }
}

#[must_use]
pub fn redact_metadata(metadata: &BTreeMap<String, String>) -> BTreeMap<String, String> {
    metadata
        .iter()
        .map(|(key, value)| {
            let redacted = if is_secret_key(key) {
                "[REDACTED]".to_string()
            } else {
                redact_secrets(value)
            };
            (key.clone(), redacted)
        })
        .collect()
}

#[must_use]
pub fn is_secret_key(key: &str) -> bool {
    let normalized = key.to_ascii_lowercase();
    [
        "secret",
        "token",
        "password",
        "passwd",
        "api_key",
        "apikey",
        "authorization",
        "bearer",
        "private_key",
        "node_key",
        "node_token",
        "credential",
    ]
    .iter()
    .any(|needle| normalized.contains(needle))
}

fn looks_like_bearer_token(value: &str) -> bool {
    value.eq_ignore_ascii_case("bearer")
        || value
            .strip_prefix("Bearer")
            .is_some_and(|suffix| suffix.starts_with('.') || suffix.starts_with('_'))
}

fn validate_component(kind: &'static str, value: &str) -> Result<String, IdentityError> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Err(IdentityError::Empty { kind });
    }
    let valid = trimmed
        .bytes()
        .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.' | b':'));
    if valid {
        Ok(trimmed.to_string())
    } else {
        Err(IdentityError::InvalidCharacter {
            kind,
            value: trimmed.to_string(),
        })
    }
}

fn normalize_permission(value: &str) -> String {
    value.trim().to_ascii_lowercase().replace([' ', '.'], "_")
}

fn hex_encode(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 0x0f) as usize] as char);
    }
    out
}

fn hex_decode_exact<const N: usize>(input: &str) -> Result<[u8; N], &'static str> {
    if input.len() != N * 2 {
        return Err("invalid key length");
    }
    let mut out = [0u8; N];
    for (i, chunk) in input.as_bytes().chunks_exact(2).enumerate() {
        out[i] = (hex_value(chunk[0])? << 4) | hex_value(chunk[1])?;
    }
    Ok(out)
}

fn hex_value(byte: u8) -> Result<u8, &'static str> {
    match byte {
        b'0'..=b'9' => Ok(byte - b'0'),
        b'a'..=b'f' => Ok(byte - b'a' + 10),
        b'A'..=b'F' => Ok(byte - b'A' + 10),
        _ => Err("invalid hex in key"),
    }
}

fn constant_time_eq(left: &[u8], right: &[u8]) -> bool {
    let max_len = left.len().max(right.len());
    let mut diff = left.len() ^ right.len();
    for idx in 0..max_len {
        let a = left.get(idx).copied().unwrap_or(0);
        let b = right.get(idx).copied().unwrap_or(0);
        diff |= usize::from(a ^ b);
    }
    diff == 0
}

fn set_private_dir_permissions(path: &Path) -> std::io::Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(path, fs::Permissions::from_mode(0o700))?;
    }
    #[cfg(not(unix))]
    {
        let _ = path;
    }
    Ok(())
}

fn set_private_file_permissions(path: &Path) -> std::io::Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(path, fs::Permissions::from_mode(0o600))?;
    }
    #[cfg(not(unix))]
    {
        let _ = path;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn node_key_round_trips_and_debug_redacts_secret() {
        let key = NodeKey::generate().unwrap();
        let parsed = NodeKey::from_persisted_str(&key.to_persisted_string()).unwrap();
        assert_eq!(key, parsed);
        let debug = format!("{key:?}");
        assert!(debug.contains("[REDACTED]"));
        assert!(!debug.contains(&hex_encode(key.expose_secret())));
    }

    #[test]
    fn scoped_agent_identity_round_trips_under_node() {
        let scoped = ScopedAgentIdentity::parse("node-a/agent-b").unwrap();
        assert!(scoped.is_under_node(&NodeId::new("node-a").unwrap()));
        assert_eq!(scoped.to_string(), "node-a/agent-b");
    }

    #[test]
    fn token_authenticator_accepts_valid_and_denies_bad_tokens() {
        let node = NodeId::new("node-a").unwrap();
        let token = NodeToken::new("0123456789abcdef").unwrap();
        let mut auth = NodeTokenAuthenticator::new();
        auth.insert(node.clone(), token);
        assert_eq!(
            auth.authenticate(&node, "0123456789abcdef"),
            AuthDecision::Allowed
        );
        assert_eq!(
            auth.authenticate(&node, "wrongwrongwrongwrong"),
            AuthDecision::Denied(AuthError::InvalidToken)
        );
        auth.revoke(&node);
        assert_eq!(
            auth.authenticate(&node, "0123456789abcdef"),
            AuthDecision::Denied(AuthError::RevokedNode)
        );
    }

    #[test]
    fn capability_policy_orders_agent_node_global_default() {
        let node = NodeId::new("node-a").unwrap();
        let agent = ScopedAgentIdentity::parse("node-a/agent-a").unwrap();
        let mut policy = CapabilityPolicy::new(CapabilityPermission::Deny);
        policy.set_global("shell", CapabilityPermission::ApprovalRequired);
        policy.set_node(node.clone(), "shell", CapabilityPermission::Allow);
        policy.set_agent(agent.clone(), "shell", CapabilityPermission::Deny);
        assert!(matches!(
            policy.evaluate(&PolicySubject::agent(agent), "shell"),
            PolicyDecision::Deny { .. }
        ));
        assert!(matches!(
            policy.evaluate(&PolicySubject::node(node), "shell"),
            PolicyDecision::Allow { .. }
        ));
    }

    #[test]
    fn redaction_hides_secret_metadata_and_inline_tokens() {
        let redacted = redact_secrets("token=abc password:secret ordinary=value");
        assert_eq!(
            redacted,
            "token=[REDACTED] password:[REDACTED] ordinary=value"
        );
        let token = NodeToken::new("0123456789abcdef").unwrap();
        assert_eq!(format!("{token}"), "[REDACTED]");
    }
}
