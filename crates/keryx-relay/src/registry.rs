//! In-memory skill registry for capability-based agent discovery.

use std::collections::{HashMap, HashSet};
use std::fmt;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use keryx_core::PeerId;
use serde::{Deserialize, Serialize};
use tokio::sync::{broadcast, RwLock};
use tokio::task::JoinHandle;
use tracing::debug;

/// Default registration TTL when callers omit or pass zero.
pub const DEFAULT_REGISTRATION_TTL: Duration = Duration::from_secs(300);

/// Interval between background expiry sweeps.
pub const DEFAULT_CLEANUP_INTERVAL: Duration = Duration::from_secs(1);

/// Gossipsub topic used by relays to exchange registry updates.
pub const REGISTRY_GOSSIP_TOPIC: &str = "/hermes/keryx/registry/v1";

const DEFAULT_GOSSIP_BUFFER: usize = 128;

/// A skill entry stored in the registry.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoredSkill {
    pub skill_id: String,
    pub description: String,
    pub tags: Vec<String>,
}

/// A peer registration with expiry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Registration {
    pub peer_id: PeerId,
    pub skills: Vec<StoredSkill>,
    pub name: String,
    pub description: String,
    pub expires_at: Instant,
    pub expires_at_unix_ms: u64,
    pub updated_at_unix_ms: u64,
}

#[derive(Debug, Default)]
struct RegistryState {
    nodes: HashMap<PeerId, Registration>,
    skill_index: HashMap<String, HashSet<PeerId>>,
}

/// Thread-safe skill registry keyed by peer, with a live skill -> peer index.
#[derive(Debug)]
pub struct SkillRegistry {
    inner: RwLock<RegistryState>,
    default_ttl: Duration,
    gossip_tx: broadcast::Sender<Vec<u8>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct GossipRegistration {
    peer_id: String,
    skills: Vec<StoredSkill>,
    name: String,
    description: String,
    expires_at_unix_ms: u64,
    updated_at_unix_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum RegistryGossipMessage {
    Upsert { registration: GossipRegistration },
    Remove { peer_id: String, updated_at_unix_ms: u64 },
    Snapshot { registrations: Vec<GossipRegistration> },
}

/// Error returned when a remote registry gossip payload cannot be applied.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RegistryGossipError(String);

impl fmt::Display for RegistryGossipError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl std::error::Error for RegistryGossipError {}

impl SkillRegistry {
    #[must_use]
    pub fn new() -> Self {
        Self::with_default_ttl(DEFAULT_REGISTRATION_TTL)
    }

    #[must_use]
    pub fn with_default_ttl(default_ttl: Duration) -> Self {
        let (gossip_tx, _) = broadcast::channel(DEFAULT_GOSSIP_BUFFER);
        Self {
            inner: RwLock::new(RegistryState::default()),
            default_ttl,
            gossip_tx,
        }
    }

    /// Subscribe to local registry changes that should be published on relay gossip.
    pub fn subscribe_gossip(&self) -> broadcast::Receiver<Vec<u8>> {
        self.gossip_tx.subscribe()
    }

    /// Register skills for a peer, replacing any prior skill set for that peer.
    pub async fn register(
        &self,
        peer_id: PeerId,
        skills: Vec<StoredSkill>,
        name: String,
        description: String,
        ttl: Option<Duration>,
    ) {
        let registration = self.registration_for(peer_id, skills, name, description, ttl);
        self.upsert_registration(registration, true).await;
    }

    /// Ensure a peer appears in the registry even before it has published skills.
    pub async fn upsert_node(
        &self,
        peer_id: PeerId,
        name: String,
        description: String,
        ttl: Option<Duration>,
    ) {
        let registration = {
            let mut guard = self.inner.write().await;
            guard.purge_expired_locked();
            let mut registration = guard.nodes.get(&peer_id).cloned().unwrap_or_else(|| {
                self.registration_for(peer_id.clone(), Vec::new(), String::new(), String::new(), ttl)
            });
            let now_ms = unix_ms_now();
            let ttl = ttl.unwrap_or(self.default_ttl);
            registration.expires_at = Instant::now() + ttl;
            registration.expires_at_unix_ms = now_ms + millis_u64(ttl);
            registration.updated_at_unix_ms = now_ms;
            if !name.trim().is_empty() || registration.name.is_empty() {
                registration.name = name;
            }
            if !description.trim().is_empty() || registration.description.is_empty() {
                registration.description = description;
            }
            guard.insert_registration(registration.clone());
            registration
        };
        self.emit_gossip(RegistryGossipMessage::Upsert {
            registration: GossipRegistration::from(&registration),
        });
    }

    /// Refresh a peer expiry without changing its advertised skills.
    pub async fn touch_node(&self, peer_id: PeerId, ttl: Option<Duration>) {
        self.upsert_node(peer_id, String::new(), String::new(), ttl).await;
    }

    /// Merge additional skills into a peer registration without dropping existing skills.
    pub async fn add_skills(
        &self,
        peer_id: PeerId,
        skills: Vec<StoredSkill>,
        name: String,
        description: String,
        ttl: Option<Duration>,
    ) {
        if skills.is_empty() {
            self.upsert_node(peer_id, name, description, ttl).await;
            return;
        }

        let registration = {
            let mut guard = self.inner.write().await;
            guard.purge_expired_locked();
            let mut registration = guard.nodes.get(&peer_id).cloned().unwrap_or_else(|| {
                self.registration_for(peer_id.clone(), Vec::new(), String::new(), String::new(), ttl)
            });
            let now_ms = unix_ms_now();
            let ttl = ttl.unwrap_or(self.default_ttl);
            registration.expires_at = Instant::now() + ttl;
            registration.expires_at_unix_ms = now_ms + millis_u64(ttl);
            registration.updated_at_unix_ms = now_ms;
            if !name.trim().is_empty() || registration.name.is_empty() {
                registration.name = name;
            }
            if !description.trim().is_empty() || registration.description.is_empty() {
                registration.description = description;
            }
            merge_skills(&mut registration.skills, skills);
            guard.insert_registration(registration.clone());
            registration
        };
        self.emit_gossip(RegistryGossipMessage::Upsert {
            registration: GossipRegistration::from(&registration),
        });
    }

    /// Remove specific skills from a peer's active registration. An empty skill list removes the peer.
    pub async fn unregister(&self, peer_id: &PeerId, skill_ids: &[String]) -> bool {
        let removed = {
            let mut guard = self.inner.write().await;
            guard.purge_expired_locked();
            if skill_ids.is_empty() {
                guard.remove_peer(peer_id).is_some()
            } else {
                let Some(mut registration) = guard.nodes.get(peer_id).cloned() else {
                    return false;
                };
                registration
                    .skills
                    .retain(|skill| !skill_ids.iter().any(|id| id == &skill.skill_id));
                if registration.skills.is_empty() {
                    guard.remove_peer(peer_id).is_some()
                } else {
                    registration.updated_at_unix_ms = unix_ms_now();
                    guard.insert_registration(registration);
                    false
                }
            }
        };

        if removed {
            self.emit_gossip(RegistryGossipMessage::Remove {
                peer_id: peer_id.as_str().to_string(),
                updated_at_unix_ms: unix_ms_now(),
            });
        } else if let Some(registration) = self.get(peer_id).await {
            self.emit_gossip(RegistryGossipMessage::Upsert {
                registration: GossipRegistration::from(&registration),
            });
        }
        true
    }

    /// Discover registrations matching optional skill id and tag filters.
    pub async fn discover(
        &self,
        skill_id: Option<&str>,
        tags: &[String],
        limit: usize,
    ) -> Vec<Registration> {
        self.purge_expired().await;
        let guard = self.inner.read().await;
        let effective_limit = if limit == 0 { usize::MAX } else { limit };
        let mut out = if let Some(want) = skill_id.filter(|value| !value.is_empty()) {
            let mut peers: Vec<_> = guard
                .skill_index
                .get(want)
                .into_iter()
                .flat_map(|ids| ids.iter())
                .filter_map(|peer_id| guard.nodes.get(peer_id))
                .filter(|reg| registration_matches(reg, Some(want), tags))
                .cloned()
                .collect();
            peers.sort_by(|a, b| a.peer_id.as_str().cmp(b.peer_id.as_str()));
            peers
        } else {
            let mut registrations: Vec<_> = guard
                .nodes
                .values()
                .filter(|reg| tags.is_empty() || registration_matches(reg, None, tags))
                .cloned()
                .collect();
            registrations.sort_by(|a, b| a.peer_id.as_str().cmp(b.peer_id.as_str()));
            registrations
        };
        out.truncate(effective_limit);
        out
    }

    /// Return a single active registration by peer id.
    pub async fn get(&self, peer_id: &PeerId) -> Option<Registration> {
        self.purge_expired().await;
        self.inner.read().await.nodes.get(peer_id).cloned()
    }

    /// Count active (non-expired) peer registrations.
    pub async fn registration_count(&self) -> usize {
        self.purge_expired().await;
        let guard = self.inner.read().await;
        guard.nodes.len()
    }

    /// Serialize the current active registry as a gossip snapshot.
    pub async fn gossip_snapshot_bytes(&self) -> Vec<u8> {
        self.purge_expired().await;
        let guard = self.inner.read().await;
        let registrations = guard.nodes.values().map(GossipRegistration::from).collect();
        serde_json::to_vec(&RegistryGossipMessage::Snapshot { registrations })
            .expect("registry gossip snapshot serializes")
    }

    /// Apply a registry gossip payload received from another relay.
    pub async fn apply_gossip_bytes(&self, payload: &[u8]) -> Result<(), RegistryGossipError> {
        let message: RegistryGossipMessage = serde_json::from_slice(payload)
            .map_err(|err| RegistryGossipError(format!("decode registry gossip: {err}")))?;
        match message {
            RegistryGossipMessage::Upsert { registration } => {
                self.apply_gossip_registration(registration).await?;
            }
            RegistryGossipMessage::Remove {
                peer_id,
                updated_at_unix_ms,
            } => {
                let peer_id = parse_peer_id(&peer_id)?;
                let mut guard = self.inner.write().await;
                let should_remove = match guard.nodes.get(&peer_id) {
                    Some(existing) => updated_at_unix_ms >= existing.updated_at_unix_ms,
                    None => true,
                };
                if should_remove {
                    guard.remove_peer(&peer_id);
                }
            }
            RegistryGossipMessage::Snapshot { registrations } => {
                for registration in registrations {
                    self.apply_gossip_registration(registration).await?;
                }
            }
        }
        Ok(())
    }

    async fn apply_gossip_registration(
        &self,
        gossip: GossipRegistration,
    ) -> Result<(), RegistryGossipError> {
        let registration = gossip.try_into_registration()?;
        let mut guard = self.inner.write().await;
        if registration.expires_at <= Instant::now() {
            guard.remove_peer(&registration.peer_id);
            return Ok(());
        }
        if guard
            .nodes
            .get(&registration.peer_id)
            .is_some_and(|existing| existing.updated_at_unix_ms > registration.updated_at_unix_ms)
        {
            return Ok(());
        }
        guard.insert_registration(registration);
        Ok(())
    }

    fn registration_for(
        &self,
        peer_id: PeerId,
        skills: Vec<StoredSkill>,
        name: String,
        description: String,
        ttl: Option<Duration>,
    ) -> Registration {
        let ttl = ttl.unwrap_or(self.default_ttl);
        let now_ms = unix_ms_now();
        Registration {
            peer_id,
            skills,
            name,
            description,
            expires_at: Instant::now() + ttl,
            expires_at_unix_ms: now_ms + millis_u64(ttl),
            updated_at_unix_ms: now_ms,
        }
    }

    async fn upsert_registration(&self, registration: Registration, emit_gossip: bool) {
        {
            let mut guard = self.inner.write().await;
            guard.purge_expired_locked();
            guard.insert_registration(registration.clone());
        }
        if emit_gossip {
            self.emit_gossip(RegistryGossipMessage::Upsert {
                registration: GossipRegistration::from(&registration),
            });
        }
    }

    async fn purge_expired(&self) {
        let mut guard = self.inner.write().await;
        guard.purge_expired_locked();
    }

    /// Spawn a background task that periodically removes expired registrations.
    pub fn spawn_cleanup(self: &Arc<Self>, interval: Duration) -> JoinHandle<()> {
        let registry = Arc::clone(self);
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(interval);
            loop {
                ticker.tick().await;
                registry.purge_expired().await;
                debug!("skill registry cleanup sweep complete");
            }
        })
    }

    fn emit_gossip(&self, message: RegistryGossipMessage) {
        if let Ok(payload) = serde_json::to_vec(&message) {
            let _ = self.gossip_tx.send(payload);
        }
    }
}

impl Default for SkillRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl RegistryState {
    fn insert_registration(&mut self, registration: Registration) {
        self.remove_peer(&registration.peer_id);
        for skill in &registration.skills {
            if !skill.skill_id.trim().is_empty() {
                self.skill_index
                    .entry(skill.skill_id.clone())
                    .or_default()
                    .insert(registration.peer_id.clone());
            }
        }
        self.nodes.insert(registration.peer_id.clone(), registration);
    }

    fn remove_peer(&mut self, peer_id: &PeerId) -> Option<Registration> {
        let removed = self.nodes.remove(peer_id);
        if removed.is_some() {
            for peers in self.skill_index.values_mut() {
                peers.remove(peer_id);
            }
            self.skill_index.retain(|_, peers| !peers.is_empty());
        }
        removed
    }

    fn purge_expired_locked(&mut self) {
        let now = Instant::now();
        let expired: Vec<PeerId> = self
            .nodes
            .iter()
            .filter(|(_, reg)| reg.expires_at <= now)
            .map(|(peer_id, _)| peer_id.clone())
            .collect();
        for peer_id in expired {
            self.remove_peer(&peer_id);
        }
    }
}

impl From<&Registration> for GossipRegistration {
    fn from(reg: &Registration) -> Self {
        Self {
            peer_id: reg.peer_id.as_str().to_string(),
            skills: reg.skills.clone(),
            name: reg.name.clone(),
            description: reg.description.clone(),
            expires_at_unix_ms: reg.expires_at_unix_ms,
            updated_at_unix_ms: reg.updated_at_unix_ms,
        }
    }
}

impl GossipRegistration {
    fn try_into_registration(self) -> Result<Registration, RegistryGossipError> {
        let peer_id = parse_peer_id(&self.peer_id)?;
        Ok(Registration {
            peer_id,
            skills: self.skills,
            name: self.name,
            description: self.description,
            expires_at: instant_from_unix_ms(self.expires_at_unix_ms),
            expires_at_unix_ms: self.expires_at_unix_ms,
            updated_at_unix_ms: self.updated_at_unix_ms,
        })
    }
}

fn registration_matches(reg: &Registration, skill_id: Option<&str>, tags: &[String]) -> bool {
    reg.skills.iter().any(|skill| {
        if let Some(want) = skill_id {
            if !want.is_empty() && skill.skill_id != want {
                return false;
            }
        }
        if tags.is_empty() {
            return true;
        }
        tags.iter().all(|tag| skill.tags.iter().any(|t| t == tag))
    })
}

fn merge_skills(existing: &mut Vec<StoredSkill>, incoming: Vec<StoredSkill>) {
    for skill in incoming {
        if skill.skill_id.trim().is_empty() {
            continue;
        }
        if let Some(current) = existing
            .iter_mut()
            .find(|current| current.skill_id == skill.skill_id)
        {
            *current = skill;
        } else {
            existing.push(skill);
        }
    }
}

fn parse_peer_id(raw: &str) -> Result<PeerId, RegistryGossipError> {
    PeerId::new(raw).map_err(|err| RegistryGossipError(format!("invalid peer id {raw:?}: {err}")))
}

fn unix_ms_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

fn millis_u64(duration: Duration) -> u64 {
    duration.as_millis().min(u128::from(u64::MAX)) as u64
}

fn instant_from_unix_ms(expires_at_unix_ms: u64) -> Instant {
    let now_ms = unix_ms_now();
    if expires_at_unix_ms <= now_ms {
        Instant::now()
    } else {
        Instant::now() + Duration::from_millis(expires_at_unix_ms - now_ms)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::time::{sleep, Duration as TokioDuration};

    fn peer(id: &str) -> PeerId {
        PeerId::new(id).expect("peer id")
    }

    fn skill(id: &str, tags: &[&str]) -> StoredSkill {
        StoredSkill {
            skill_id: id.to_string(),
            description: format!("{id} desc"),
            tags: tags.iter().map(|t| (*t).to_string()).collect(),
        }
    }

    #[tokio::test]
    async fn register_then_discover_by_skill() {
        let registry = SkillRegistry::new();
        registry
            .register(
                peer("peer-a"),
                vec![skill("rust", &[])],
                "Agent A".into(),
                "does rust".into(),
                None,
            )
            .await;

        let found = registry.discover(Some("rust"), &[], 10).await;
        assert_eq!(found.len(), 1);
        assert_eq!(found[0].peer_id.as_str(), "peer-a");
        assert_eq!(found[0].skills[0].skill_id, "rust");
    }

    #[tokio::test]
    async fn node_registration_appears_without_skills() {
        let registry = SkillRegistry::new();
        registry
            .upsert_node(peer("node-a"), "Node A".into(), String::new(), None)
            .await;

        assert_eq!(registry.registration_count().await, 1);
        let all = registry.discover(None, &[], 10).await;
        assert_eq!(all.len(), 1);
        assert_eq!(all[0].peer_id.as_str(), "node-a");
        assert!(all[0].skills.is_empty());
    }

    #[tokio::test]
    async fn discover_filters_by_tags() {
        let registry = SkillRegistry::new();
        registry
            .register(
                peer("peer-tagged"),
                vec![skill("api", &["backend", "grpc"])],
                "API".into(),
                "".into(),
                None,
            )
            .await;
        registry
            .register(
                peer("peer-other"),
                vec![skill("api", &["frontend"])],
                "UI".into(),
                "".into(),
                None,
            )
            .await;

        let backend = registry
            .discover(Some("api"), &["backend".into()], 10)
            .await;
        assert_eq!(backend.len(), 1);
        assert_eq!(backend[0].peer_id.as_str(), "peer-tagged");
    }

    #[tokio::test]
    async fn ttl_expiry_removes_registration() {
        let registry = SkillRegistry::new();
        registry
            .register(
                peer("peer-ttl"),
                vec![skill("ephemeral", &[])],
                "tmp".into(),
                "".into(),
                Some(Duration::from_millis(50)),
            )
            .await;

        assert_eq!(registry.discover(Some("ephemeral"), &[], 10).await.len(), 1);
        sleep(TokioDuration::from_millis(80)).await;
        assert_eq!(registry.discover(Some("ephemeral"), &[], 10).await.len(), 0);
    }

    #[tokio::test]
    async fn unregister_removes_skills_from_discovery() {
        let registry = SkillRegistry::new();
        let id = peer("peer-unreg");
        registry
            .register(
                id.clone(),
                vec![skill("keep", &[]), skill("drop", &[])],
                "mix".into(),
                "".into(),
                None,
            )
            .await;

        registry.unregister(&id, &["drop".into()]).await;
        let found = registry.discover(None, &[], 10).await;
        assert_eq!(found.len(), 1);
        assert_eq!(found[0].skills.len(), 1);
        assert_eq!(found[0].skills[0].skill_id, "keep");

        registry.unregister(&id, &["keep".into()]).await;
        assert!(registry.discover(None, &[], 10).await.is_empty());
    }

    #[tokio::test]
    async fn discover_respects_limit() {
        let registry = SkillRegistry::new();
        for i in 0..5 {
            registry
                .register(
                    peer(&format!("peer-{i}")),
                    vec![skill("shared", &[])],
                    format!("n{i}"),
                    "".into(),
                    None,
                )
                .await;
        }
        let limited = registry.discover(Some("shared"), &[], 2).await;
        assert_eq!(limited.len(), 2);
    }

    #[tokio::test]
    async fn gossip_snapshot_syncs_registry() {
        let source = SkillRegistry::new();
        let target = SkillRegistry::new();
        source
            .register(
                peer("peer-gossip"),
                vec![skill("sync", &["relay"])],
                "gossip".into(),
                "".into(),
                None,
            )
            .await;

        let payload = source.gossip_snapshot_bytes().await;
        target.apply_gossip_bytes(&payload).await.unwrap();

        let found = target.discover(Some("sync"), &["relay".into()], 10).await;
        assert_eq!(found.len(), 1);
        assert_eq!(found[0].peer_id.as_str(), "peer-gossip");
    }
}
