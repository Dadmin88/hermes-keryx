//! gRPC surface for the relay skill registry.

use std::sync::Arc;
use std::time::Duration;

use keryx_core::{NodeId, PeerId};
use keryx_proto::v1::{
    registry_service_server::{RegistryService, RegistryServiceServer},
    DiscoverBySkillRequest, DiscoverBySkillResponse, RegisterSkillsRequest, RegisterSkillsResponse,
    Registration as ProtoRegistration, SkillInfo, Timestamp, UnregisterSkillsRequest,
    UnregisterSkillsResponse,
};
use tonic::transport::{Identity, ServerTlsConfig};
use tonic::{Request, Response, Status};

use keryx_observe::RelayMetrics;

use crate::health_server::{NODE_ID_METADATA_KEY, NODE_TOKEN_METADATA_KEY};
use crate::registry::{SkillRegistry, StoredSkill};
use crate::security::NodeTokenAuth;

/// Shared registry service state.
#[derive(Clone)]
pub struct RegistryRpcService {
    registry: Arc<SkillRegistry>,
    metrics: Option<Arc<RelayMetrics>>,
    node_auth: Option<Arc<NodeTokenAuth>>,
}

impl RegistryRpcService {
    #[must_use]
    pub fn new(registry: Arc<SkillRegistry>) -> Self {
        Self {
            registry,
            metrics: None,
            node_auth: None,
        }
    }

    #[must_use]
    pub fn with_metrics(registry: Arc<SkillRegistry>, metrics: Arc<RelayMetrics>) -> Self {
        Self {
            registry,
            metrics: Some(metrics),
            node_auth: None,
        }
    }

    #[must_use]
    pub fn with_auth(registry: Arc<SkillRegistry>, node_auth: Arc<NodeTokenAuth>) -> Self {
        Self {
            registry,
            metrics: None,
            node_auth: Some(node_auth),
        }
    }

    #[must_use]
    pub fn with_metrics_and_auth(
        registry: Arc<SkillRegistry>,
        metrics: Arc<RelayMetrics>,
        node_auth: Arc<NodeTokenAuth>,
    ) -> Self {
        Self {
            registry,
            metrics: Some(metrics),
            node_auth: Some(node_auth),
        }
    }

    fn authenticate_mutation<T>(
        &self,
        request: &Request<T>,
        claimed_peer_id: &str,
    ) -> Result<PeerId, Status> {
        let Some(auth) = &self.node_auth else {
            return Err(Status::unauthenticated(
                "registry mutation requires node authentication",
            ));
        };
        let claimed_peer_id = claimed_peer_id.trim();
        let metadata_node_id = request
            .metadata()
            .get(NODE_ID_METADATA_KEY)
            .and_then(|value| value.to_str().ok())
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .ok_or_else(|| Status::unauthenticated("node id metadata is required"))?;
        let node_id = NodeId::new(metadata_node_id)
            .map_err(|_| Status::unauthenticated("invalid node identity metadata"))?;
        let token = request
            .metadata()
            .get(NODE_TOKEN_METADATA_KEY)
            .and_then(|value| value.to_str().ok());
        auth.authenticate_optional(&node_id, token)
            .map_err(|failure| {
                Status::unauthenticated(format!("node authentication failed: {}", failure.reason()))
            })?;
        if claimed_peer_id.is_empty() {
            return Err(Status::invalid_argument("peer id is required"));
        }
        if metadata_node_id != claimed_peer_id {
            return Err(Status::permission_denied(
                "claimed peer id does not match authenticated node metadata",
            ));
        }
        PeerId::new(metadata_node_id)
            .map_err(|_| Status::unauthenticated("invalid node identity metadata"))
    }

    async fn sync_registry_metric(&self) {
        let Some(metrics) = &self.metrics else {
            return;
        };
        let count = self.registry.registration_count().await as u64;
        metrics.set_registry_size(count);
    }
}

/// Serve the registry gRPC API, requiring loopback when TLS is absent.
pub async fn serve_registry_rpc(
    service: RegistryRpcService,
    listener: tokio::net::TcpListener,
) -> anyhow::Result<()> {
    serve_registry_rpc_with_tls(service, listener, None).await
}

/// Serve the registry gRPC API, requiring loopback when TLS is absent.
pub async fn serve_registry_rpc_with_tls(
    service: RegistryRpcService,
    listener: tokio::net::TcpListener,
    identity: Option<Identity>,
) -> anyhow::Result<()> {
    let addr = listener.local_addr()?;
    if identity.is_none() && !addr.ip().is_loopback() {
        anyhow::bail!("non-loopback registry listeners require TLS");
    }
    let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener);
    let mut server = tonic::transport::Server::builder();
    if let Some(identity) = identity {
        server = server.tls_config(ServerTlsConfig::new().identity(identity))?;
    }
    server
        .add_service(RegistryServiceServer::new(service))
        .serve_with_incoming(incoming)
        .await?;
    Ok(())
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
        protocol_features: reg.protocol_features.clone(),
    }
}

#[tonic::async_trait]
impl RegistryService for RegistryRpcService {
    async fn register_skills(
        &self,
        request: Request<RegisterSkillsRequest>,
    ) -> Result<Response<RegisterSkillsResponse>, Status> {
        let peer_id = self.authenticate_mutation(&request, &request.get_ref().peer_id)?;
        let inner = request.into_inner();
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
            .register_with_features(
                peer_id,
                skills,
                inner.name,
                inner.description,
                inner.protocol_features,
                ttl,
            )
            .await;
        self.sync_registry_metric().await;
        Ok(Response::new(RegisterSkillsResponse { accepted: true }))
    }

    async fn unregister_skills(
        &self,
        request: Request<UnregisterSkillsRequest>,
    ) -> Result<Response<UnregisterSkillsResponse>, Status> {
        let peer_id = self.authenticate_mutation(&request, &request.get_ref().peer_id)?;
        let inner = request.into_inner();
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
