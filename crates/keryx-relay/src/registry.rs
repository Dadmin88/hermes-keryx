//! In-memory skill registry for capability-based agent discovery.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use keryx_core::PeerId;
use tokio::sync::RwLock;
use tokio::task::JoinHandle;
use tracing::debug;

/// Default registration TTL when callers omit or pass zero.
pub const DEFAULT_REGISTRATION_TTL: Duration = Duration::from_secs(300);

/// Interval between background expiry sweeps.
pub const DEFAULT_CLEANUP_INTERVAL: Duration = Duration::from_secs(1);

/// A skill entry stored in the registry.
#[derive(Debug, Clone, PartialEq, Eq)]
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
}

/// Thread-safe skill registry keyed by peer.
#[derive(Debug)]
pub struct SkillRegistry {
    inner: RwLock<HashMap<PeerId, Vec<Registration>>>,
    default_ttl: Duration,
}

impl SkillRegistry {
    #[must_use]
    pub fn new() -> Self {
        Self::with_default_ttl(DEFAULT_REGISTRATION_TTL)
    }

    #[must_use]
    pub fn with_default_ttl(default_ttl: Duration) -> Self {
        Self {
            inner: RwLock::new(HashMap::new()),
            default_ttl,
        }
    }

    /// Register skills for a peer, replacing any prior registrations for that peer.
    pub async fn register(
        &self,
        peer_id: PeerId,
        skills: Vec<StoredSkill>,
        name: String,
        description: String,
        ttl: Option<Duration>,
    ) {
        let ttl = ttl.unwrap_or(self.default_ttl);
        let expires_at = Instant::now() + ttl;
        let registration = Registration {
            peer_id: peer_id.clone(),
            skills,
            name,
            description,
            expires_at,
        };
        let mut guard = self.inner.write().await;
        guard.insert(peer_id, vec![registration]);
    }

    /// Remove specific skills from a peer's active registration.
    pub async fn unregister(&self, peer_id: &PeerId, skill_ids: &[String]) -> bool {
        let mut guard = self.inner.write().await;
        let Some(entries) = guard.get_mut(peer_id) else {
            return false;
        };
        let now = Instant::now();
        entries.retain(|reg| reg.expires_at > now);
        if entries.is_empty() {
            guard.remove(peer_id);
            return true;
        }
        for reg in entries.iter_mut() {
            if !skill_ids.is_empty() {
                reg.skills
                    .retain(|skill| !skill_ids.iter().any(|id| id == &skill.skill_id));
            }
        }
        entries.retain(|reg| !reg.skills.is_empty());
        if entries.is_empty() {
            guard.remove(peer_id);
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
        let mut out = Vec::new();
        'peers: for entries in guard.values() {
            for reg in entries {
                if reg.expires_at <= Instant::now() {
                    continue;
                }
                if !registration_matches(reg, skill_id, tags) {
                    continue;
                }
                out.push(reg.clone());
                if out.len() >= effective_limit {
                    break 'peers;
                }
            }
        }
        out
    }

    /// Count active (non-expired) peer registrations.
    pub async fn registration_count(&self) -> usize {
        self.purge_expired().await;
        let guard = self.inner.read().await;
        guard.len()
    }

    async fn purge_expired(&self) {
        let mut guard = self.inner.write().await;
        let now = Instant::now();
        guard.retain(|_, entries| {
            entries.retain(|reg| reg.expires_at > now);
            !entries.is_empty()
        });
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
}

impl Default for SkillRegistry {
    fn default() -> Self {
        Self::new()
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
}
