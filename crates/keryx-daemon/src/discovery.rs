//! Relay skill registry registration and discovery proxy for the local daemon.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use keryx_core::PeerId;
use keryx_proto::v1::{
    registry_service_client::RegistryServiceClient, DiscoverBySkillRequest,
    DiscoverBySkillResponse, DiscoverSkillsRequest, DiscoverSkillsResponse, RegisterSkillsRequest,
    SkillInfo, UnregisterSkillsRequest,
};
use tokio::sync::{watch, Mutex};
use tokio::task::JoinHandle;
use tonic::transport::Channel;
use tonic::{Request, Status};
use tracing::{debug, error, info, warn};

use crate::grpc_transport::secure_grpc_endpoint;

/// Default registration TTL when callers omit an explicit value.
pub const DEFAULT_REGISTRATION_TTL_SECONDS: u64 = 60;

/// Seconds before expiry to re-register when TTL is [`DEFAULT_REGISTRATION_TTL_SECONDS`].
pub const DEFAULT_REFRESH_LEAD_SECONDS: u64 = 5;
pub const ABSOLUTE_DEADLINES_FEATURE: &str = "absolute_deadlines_v1";
pub const RESULT_ARTIFACT_BYTES_FEATURE: &str = "result_artifact_bytes_v1";
pub const SUPPORTED_PROTOCOL_FEATURES: &[&str] =
    &[ABSOLUTE_DEADLINES_FEATURE, RESULT_ARTIFACT_BYTES_FEATURE];

/// A skill advertised by this daemon in the relay registry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConfiguredSkill {
    pub skill_id: String,
    pub description: String,
    pub tags: Vec<String>,
}

/// Periodic registration settings (skills must be non-empty).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RegistrationSettings {
    pub skills: Vec<ConfiguredSkill>,
    pub name: String,
    pub description: String,
    pub ttl_seconds: u64,
    pub refresh_interval: Duration,
}

impl RegistrationSettings {
    #[must_use]
    pub fn refresh_interval_for_ttl(ttl_seconds: u64) -> Duration {
        let lead = DEFAULT_REFRESH_LEAD_SECONDS.min(ttl_seconds.saturating_sub(1));
        Duration::from_secs(ttl_seconds.saturating_sub(lead).max(1))
    }

    #[must_use]
    pub fn with_ttl_seconds(mut self, ttl_seconds: u64) -> Self {
        self.ttl_seconds = ttl_seconds;
        self.refresh_interval = Self::refresh_interval_for_ttl(ttl_seconds);
        self
    }
}

/// Discovery + optional registration against a relay registry gRPC endpoint.
#[derive(Clone, PartialEq, Eq)]
pub struct DiscoverySettings {
    pub registry_endpoint: String,
    pub registry_ca_cert_path: Option<PathBuf>,
    pub registration: Option<RegistrationSettings>,
    pub node_token: Option<String>,
}

impl std::fmt::Debug for DiscoverySettings {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DiscoverySettings")
            .field("registry_endpoint", &self.registry_endpoint)
            .field(
                "registry_ca_cert_configured",
                &self.registry_ca_cert_path.is_some(),
            )
            .field("registration", &self.registration)
            .field("node_token_configured", &self.node_token.is_some())
            .finish()
    }
}

/// Active registry client and background registration loop.
pub struct DiscoveryHandle {
    registry_endpoint: String,
    client: Arc<Mutex<RegistryServiceClient<Channel>>>,
    registration: Option<RegistrationSettings>,
    peer_id: PeerId,
    node_token: Option<String>,
    shutdown_tx: watch::Sender<bool>,
    loop_task: Mutex<Option<JoinHandle<()>>>,
}

impl std::fmt::Debug for DiscoveryHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DiscoveryHandle")
            .field("registry_endpoint", &self.registry_endpoint)
            .field("peer_id", &self.peer_id)
            .field("registration", &self.registration)
            .finish_non_exhaustive()
    }
}

impl DiscoveryHandle {
    pub async fn connect(settings: &DiscoverySettings, peer_id: PeerId) -> Result<Self, Status> {
        let client = connect_registry_client(
            &settings.registry_endpoint,
            settings.registry_ca_cert_path.as_deref(),
        )
        .await?;
        Ok(Self {
            registry_endpoint: settings.registry_endpoint.clone(),
            client: Arc::new(Mutex::new(client)),
            registration: settings.registration.clone(),
            peer_id,
            node_token: settings.node_token.clone(),
            shutdown_tx: watch::channel(false).0,
            loop_task: Mutex::new(None),
        })
    }

    /// Register once and spawn TTL refresh until [`shutdown`](Self::shutdown) is called.
    pub async fn start_registration_loop(&self) -> Result<(), Status> {
        let Some(registration) = self.registration.clone() else {
            return Ok(());
        };

        self.register_now(&registration).await?;

        let client = Arc::clone(&self.client);
        let peer_id = self.peer_id.clone();
        let node_token = self.node_token.clone();
        let mut shutdown_rx = self.shutdown_tx.subscribe();
        let refresh = registration.refresh_interval;
        let skill_count = registration.skills.len();
        let registration = registration.clone();

        let task = tokio::spawn(async move {
            let mut interval = tokio::time::interval(refresh);
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                tokio::select! {
                    changed = shutdown_rx.changed() => {
                        if changed.is_ok() && *shutdown_rx.borrow() {
                            break;
                        }
                    }
                    _ = interval.tick() => {
                        if let Err(err) = register_peer(
                            &client,
                            &peer_id,
                            &registration,
                            node_token.as_deref(),
                        ).await {
                            warn!(
                                component = "discovery",
                                error = %err,
                                "periodic skill registration failed"
                            );
                        } else {
                            debug!(
                                component = "discovery",
                                peer_id = %peer_id.as_str(),
                                "refreshed skill registration"
                            );
                        }
                    }
                }
            }
        });

        *self.loop_task.lock().await = Some(task);
        info!(
            component = "discovery",
            peer_id = %self.peer_id.as_str(),
            endpoint = %self.registry_endpoint,
            skill_count = skill_count,
            refresh_secs = refresh.as_secs(),
            "daemon skill registration loop started"
        );
        Ok(())
    }

    pub async fn discover(
        &self,
        request: DiscoverSkillsRequest,
    ) -> Result<DiscoverSkillsResponse, Status> {
        let mut client = self.client.lock().await;
        let response = client
            .discover_by_skill(DiscoverBySkillRequest {
                skill_id: request.skill_id,
                tags: request.tags,
                limit: request.limit,
            })
            .await
            .map_err(registry_rpc_error)?
            .into_inner();
        Ok(discover_response_to_daemon(response))
    }

    pub async fn shutdown(&self) {
        let _ = self.shutdown_tx.send(true);
        if let Some(task) = self.loop_task.lock().await.take() {
            task.abort();
            let _ = task.await;
        }
        if let Some(registration) = self.registration.clone() {
            if let Err(err) = self.deregister_all(&registration).await {
                error!(
                    component = "discovery",
                    peer_id = %self.peer_id.as_str(),
                    error = %err,
                    "failed to deregister skills on shutdown"
                );
            } else {
                info!(
                    component = "discovery",
                    peer_id = %self.peer_id.as_str(),
                    "deregistered skills from relay registry"
                );
            }
        }
    }

    async fn register_now(&self, registration: &RegistrationSettings) -> Result<(), Status> {
        register_peer(
            &self.client,
            &self.peer_id,
            registration,
            self.node_token.as_deref(),
        )
        .await
    }

    async fn deregister_all(&self, registration: &RegistrationSettings) -> Result<(), Status> {
        let skill_ids: Vec<String> = registration
            .skills
            .iter()
            .map(|skill| skill.skill_id.clone())
            .collect();
        let mut client = self.client.lock().await;
        let request = authenticated_registry_request(
            UnregisterSkillsRequest {
                peer_id: self.peer_id.as_str().to_string(),
                skill_ids,
            },
            &self.peer_id,
            self.node_token.as_deref(),
        )?;
        client
            .unregister_skills(request)
            .await
            .map_err(registry_rpc_error)?;
        Ok(())
    }
}

async fn connect_registry_client(
    endpoint: &str,
    ca_cert_path: Option<&std::path::Path>,
) -> Result<RegistryServiceClient<Channel>, Status> {
    let endpoint_builder = secure_grpc_endpoint(endpoint, ca_cert_path)
        .map_err(|error| Status::failed_precondition(error.to_string()))?;
    let channel = endpoint_builder.connect().await.map_err(|error| {
        Status::unavailable(format!(
            "failed to connect to relay registry at {endpoint}: {error}"
        ))
    })?;
    Ok(RegistryServiceClient::new(channel))
}

async fn register_peer(
    client: &Arc<Mutex<RegistryServiceClient<Channel>>>,
    peer_id: &PeerId,
    registration: &RegistrationSettings,
    node_token: Option<&str>,
) -> Result<(), Status> {
    let skills: Vec<SkillInfo> = registration
        .skills
        .iter()
        .map(|skill| SkillInfo {
            skill_id: skill.skill_id.clone(),
            description: skill.description.clone(),
            tags: skill.tags.clone(),
        })
        .collect();
    let mut guard = client.lock().await;
    let request = authenticated_registry_request(
        RegisterSkillsRequest {
            peer_id: peer_id.as_str().to_string(),
            skills,
            name: registration.name.clone(),
            description: registration.description.clone(),
            ttl_seconds: registration.ttl_seconds,
            protocol_features: SUPPORTED_PROTOCOL_FEATURES
                .iter()
                .map(|feature| (*feature).to_string())
                .collect(),
        },
        peer_id,
        node_token,
    )?;
    guard
        .register_skills(request)
        .await
        .map_err(registry_rpc_error)?;
    Ok(())
}

fn authenticated_registry_request<T>(
    message: T,
    peer_id: &PeerId,
    node_token: Option<&str>,
) -> Result<Request<T>, Status> {
    let mut request = Request::new(message);
    request.metadata_mut().insert(
        "x-keryx-node-id",
        peer_id.as_str().parse().map_err(|error| {
            Status::invalid_argument(format!("invalid peer id metadata: {error}"))
        })?,
    );
    if let Some(token) = node_token {
        request.metadata_mut().insert(
            "x-keryx-node-token",
            token.parse().map_err(|error| {
                Status::invalid_argument(format!("invalid node token metadata: {error}"))
            })?,
        );
    }
    Ok(request)
}

fn discover_response_to_daemon(response: DiscoverBySkillResponse) -> DiscoverSkillsResponse {
    DiscoverSkillsResponse {
        registrations: response.registrations,
    }
}

fn registry_rpc_error(error: tonic::Status) -> Status {
    Status::unavailable(format!("relay registry RPC failed: {error}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn refresh_interval_defaults_to_five_seconds_before_ttl() {
        let interval = RegistrationSettings::refresh_interval_for_ttl(60);
        assert_eq!(interval, Duration::from_secs(55));
    }

    #[test]
    fn refresh_interval_clamps_short_ttl() {
        let interval = RegistrationSettings::refresh_interval_for_ttl(3);
        assert_eq!(interval, Duration::from_secs(1));
    }

    #[tokio::test]
    async fn remote_plaintext_registry_endpoint_fails_closed() {
        let error = connect_registry_client("http://192.0.2.1:50053", None)
            .await
            .unwrap_err();
        assert_eq!(error.code(), tonic::Code::FailedPrecondition);
        assert!(error.message().contains("require TLS"));
    }

    #[test]
    fn discovery_settings_debug_redacts_node_token() {
        let settings = DiscoverySettings {
            registry_endpoint: "http://127.0.0.1:50053".into(),
            registry_ca_cert_path: None,
            registration: None,
            node_token: Some("super-secret-node-token".into()),
        };

        let debug = format!("{settings:?}");
        assert!(debug.contains("node_token_configured: true"));
        assert!(!debug.contains("super-secret-node-token"));
    }

    #[test]
    fn authenticated_registry_request_attaches_identity_and_token_metadata() {
        let peer_id = PeerId::new("peer-authenticated").unwrap();
        let request = authenticated_registry_request((), &peer_id, Some("node-secret")).unwrap();

        assert_eq!(
            request
                .metadata()
                .get("x-keryx-node-id")
                .unwrap()
                .to_str()
                .unwrap(),
            "peer-authenticated"
        );
        assert_eq!(
            request
                .metadata()
                .get("x-keryx-node-token")
                .unwrap()
                .to_str()
                .unwrap(),
            "node-secret"
        );
    }
}
