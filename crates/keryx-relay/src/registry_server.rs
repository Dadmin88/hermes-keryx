//! gRPC surface for the relay skill registry.

use std::sync::Arc;
use std::time::Duration;

use keryx_core::PeerId;
use keryx_proto::v1::{
    registry_service_server::{RegistryService, RegistryServiceServer},
    DiscoverBySkillRequest, DiscoverBySkillResponse, RegisterSkillsRequest, RegisterSkillsResponse,
    Registration as ProtoRegistration, SkillInfo, Timestamp, UnregisterSkillsRequest,
    UnregisterSkillsResponse,
};
use tonic::{Request, Response, Status};

use keryx_observe::RelayMetrics;

use crate::registry::{SkillRegistry, StoredSkill};

/// Shared registry service state.
#[derive(Clone)]
pub struct RegistryRpcService {
    registry: Arc<SkillRegistry>,
    metrics: Option<Arc<RelayMetrics>>,
}

impl RegistryRpcService {
    #[must_use]
    pub fn new(registry: Arc<SkillRegistry>) -> Self {
        Self {
            registry,
            metrics: None,
        }
    }

    #[must_use]
    pub fn with_metrics(registry: Arc<SkillRegistry>, metrics: Arc<RelayMetrics>) -> Self {
        Self {
            registry,
            metrics: Some(metrics),
        }
    }

    async fn sync_registry_metric(&self) {
        let Some(metrics) = &self.metrics else {
            return;
        };
        let count = self.registry.registration_count().await as u64;
        metrics.set_registry_size(count);
    }
}

/// Serve the registry gRPC API on the given TCP listener stream.
pub async fn serve_registry_rpc(
    service: RegistryRpcService,
    incoming: tokio_stream::wrappers::TcpListenerStream,
) -> Result<(), tonic::transport::Error> {
    tonic::transport::Server::builder()
        .add_service(RegistryServiceServer::new(service))
        .serve_with_incoming(incoming)
        .await
}

fn parse_peer_id(raw: &str) -> Result<PeerId, Status> {
    PeerId::new(raw).map_err(|e| Status::invalid_argument(e.to_string()))
}

fn proto_timestamp_from_unix_ms(unix_ms: i64) -> Timestamp {
    Timestamp { unix_ms }
}

fn registration_to_proto(reg: &crate::registry::Registration) -> ProtoRegistration {
    let expires_ms = reg
        .expires_at
        .checked_duration_since(std::time::Instant::now())
        .map(|d| {
            let now_ms = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as i64;
            now_ms + d.as_millis() as i64
        })
        .unwrap_or(0);
    ProtoRegistration {
        peer_id: reg.peer_id.as_str().to_string(),
        skills: reg
            .skills
            .iter()
            .map(|s| SkillInfo {
                skill_id: s.skill_id.clone(),
                description: s.description.clone(),
                tags: s.tags.clone(),
            })
            .collect(),
        name: reg.name.clone(),
        description: reg.description.clone(),
        expires_at: Some(proto_timestamp_from_unix_ms(expires_ms)),
    }
}

#[tonic::async_trait]
impl RegistryService for RegistryRpcService {
    async fn register_skills(
        &self,
        request: Request<RegisterSkillsRequest>,
    ) -> Result<Response<RegisterSkillsResponse>, Status> {
        let inner = request.into_inner();
        let peer_id = parse_peer_id(&inner.peer_id)?;
        let skills: Vec<StoredSkill> = inner
            .skills
            .into_iter()
            .map(|s| StoredSkill {
                skill_id: s.skill_id,
                description: s.description,
                tags: s.tags,
            })
            .collect();
        let ttl = if inner.ttl_seconds == 0 {
            None
        } else {
            Some(Duration::from_secs(inner.ttl_seconds))
        };
        self.registry
            .register(peer_id, skills, inner.name, inner.description, ttl)
            .await;
        self.sync_registry_metric().await;
        Ok(Response::new(RegisterSkillsResponse { accepted: true }))
    }

    async fn unregister_skills(
        &self,
        request: Request<UnregisterSkillsRequest>,
    ) -> Result<Response<UnregisterSkillsResponse>, Status> {
        let inner = request.into_inner();
        let peer_id = parse_peer_id(&inner.peer_id)?;
        self.registry.unregister(&peer_id, &inner.skill_ids).await;
        self.sync_registry_metric().await;
        Ok(Response::new(UnregisterSkillsResponse { accepted: true }))
    }

    async fn discover_by_skill(
        &self,
        request: Request<DiscoverBySkillRequest>,
    ) -> Result<Response<DiscoverBySkillResponse>, Status> {
        let inner = request.into_inner();
        let skill_id = if inner.skill_id.is_empty() {
            None
        } else {
            Some(inner.skill_id.as_str())
        };
        let found = self
            .registry
            .discover(skill_id, &inner.tags, inner.limit as usize)
            .await;
        Ok(Response::new(DiscoverBySkillResponse {
            registrations: found.iter().map(registration_to_proto).collect(),
        }))
    }
}
