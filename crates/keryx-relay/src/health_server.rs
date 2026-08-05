//! gRPC health endpoint for the relay.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt;
use keryx_proto::v1::keryx_relay_server::{KeryxRelay, KeryxRelayServer};
use keryx_proto::v1::{
    AckFrameRequest, AckFrameResponse, AckTaskRequest, AckTaskResponse, HealthRequest,
    HealthResponse, NodeFrame, PublishResultRequest, PublishResultResponse, PublishTaskRequest,
    PublishTaskResponse, RegisterNodeRequest, RegisterNodeResponse, RelayFrame, TaskEnvelope,
};
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::{Request, Response, Status};

use crate::health::RelayHealthReport;
use crate::registry::{SkillRegistry, StoredSkill};
use crate::runtime::RelayRuntime;
use crate::security::NodeTokenAuth;
use keryx_core::{PeerId, RESULT_ARTIFACT_FRAME_MAX_BYTES};

/// gRPC metadata key used by `ConnectNode` to identify the streaming node.
pub const NODE_ID_METADATA_KEY: &str = "x-keryx-node-id";
pub const NODE_TOKEN_METADATA_KEY: &str = "x-keryx-node-token";

const RELAY_STREAM_BUFFER: usize = 128;
const TARGET_NODE_METADATA_KEYS: &[&str] = &[
    "target_node_id",
    "target_node",
    "recipient_node_id",
    "recipient_node",
    "destination_node_id",
    "destination_node",
    "node_id",
    "keryx.target_node_id",
];
const SKILL_METADATA_KEYS: &[&str] = &[
    "keryx.capability_id",
    "capability_id",
    "capability",
    "skill_id",
    "skill",
];

pub struct RelayHealthService {
    runtime: Arc<RelayRuntime>,
    registry: Option<Arc<SkillRegistry>>,
    node_auth: Option<Arc<NodeTokenAuth>>,
}

impl RelayHealthService {
    #[must_use]
    pub fn new(runtime: Arc<RelayRuntime>) -> Self {
        Self {
            runtime,
            registry: None,
            node_auth: None,
        }
    }

    #[must_use]
    pub fn with_registry(runtime: Arc<RelayRuntime>, registry: Arc<SkillRegistry>) -> Self {
        Self {
            runtime,
            registry: Some(registry),
            node_auth: None,
        }
    }

    #[must_use]
    pub fn with_registry_and_auth(
        runtime: Arc<RelayRuntime>,
        registry: Arc<SkillRegistry>,
        node_auth: Arc<NodeTokenAuth>,
    ) -> Self {
        Self {
            runtime,
            registry: Some(registry),
            node_auth: Some(node_auth),
        }
    }

    fn authenticate_request<T>(
        &self,
        request: &Request<T>,
        claimed_node_id: &str,
    ) -> Result<String, Status> {
        let claimed_node_id = claimed_node_id.trim();
        if claimed_node_id.is_empty() {
            return Err(Status::invalid_argument("source node id is required"));
        }
        let Some(auth) = &self.node_auth else {
            return Ok(claimed_node_id.to_string());
        };
        let metadata_node_id = request
            .metadata()
            .get(NODE_ID_METADATA_KEY)
            .and_then(|value| value.to_str().ok())
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .ok_or_else(|| Status::unauthenticated("node id metadata is required"))?;
        if metadata_node_id != claimed_node_id {
            return Err(Status::permission_denied(
                "claimed source node does not match authenticated node metadata",
            ));
        }
        let token = request
            .metadata()
            .get(NODE_TOKEN_METADATA_KEY)
            .and_then(|value| value.to_str().ok());
        let node_id = metadata_node_id
            .parse()
            .map_err(|error| Status::invalid_argument(format!("invalid node id: {error}")))?;
        auth.authenticate_optional(&node_id, token)
            .map_err(|failure| {
                Status::unauthenticated(format!("node authentication failed: {}", failure.reason()))
            })?;
        Ok(metadata_node_id.to_string())
    }

    async fn refresh_registry_metric(&self) {
        let Some(registry) = &self.registry else {
            return;
        };
        let count = registry.registration_count().await as u64;
        self.runtime.metrics().set_registry_size(count);
    }
}

#[tonic::async_trait]
impl KeryxRelay for RelayHealthService {
    type ConnectNodeStream = ReceiverStream<Result<keryx_proto::v1::RelayFrame, Status>>;

    async fn connect_node(
        &self,
        request: Request<tonic::Streaming<NodeFrame>>,
    ) -> Result<Response<Self::ConnectNodeStream>, Status> {
        let node_id = node_id_from_metadata(&request)?;
        let node_id = self.authenticate_request(&request, &node_id)?;
        let mut inbound = request.into_inner();
        let (tx, rx) = mpsc::channel(RELAY_STREAM_BUFFER);

        let pending = self.runtime.connect_node(node_id.clone(), tx.clone());
        for frame in pending {
            if tx.try_send(Ok(frame.clone())).is_err() {
                self.runtime.route_frame(node_id.clone(), frame);
            }
        }

        let runtime = Arc::clone(&self.runtime);
        let source_node_id = node_id.clone();
        tokio::spawn(async move {
            while let Some(next) = inbound.next().await {
                match next {
                    Ok(frame) => {
                        if let Err(err) = route_node_frame(&runtime, &source_node_id, frame) {
                            tracing::warn!(
                                source_node_id = %source_node_id,
                                error = %err,
                                "dropping malformed node relay frame"
                            );
                        }
                    }
                    Err(err) => {
                        tracing::debug!(
                            source_node_id = %source_node_id,
                            error = %err,
                            "node relay stream ended with error"
                        );
                        break;
                    }
                }
            }
            runtime.disconnect_node(&source_node_id);
        });

        tracing::debug!(%node_id, "node connected to relay stream");
        Ok(Response::new(ReceiverStream::new(rx)))
    }

    async fn register_node(
        &self,
        request: Request<RegisterNodeRequest>,
    ) -> Result<Response<RegisterNodeResponse>, Status> {
        let inner = request.into_inner();
        let node_id = inner
            .node_id
            .as_ref()
            .map(|node_id| node_id.value.trim())
            .filter(|value| !value.is_empty())
            .ok_or_else(|| Status::invalid_argument("RegisterNode requires node_id"))?;
        if let Some(auth) = &self.node_auth {
            let parsed = node_id
                .parse()
                .map_err(|error| Status::invalid_argument(format!("invalid node id: {error}")))?;
            auth.authenticate(&parsed, inner.token.trim())
                .map_err(|failure| {
                    Status::unauthenticated(format!(
                        "node authentication failed: {}",
                        failure.reason()
                    ))
                })?;
        }
        self.runtime.register_node(node_id.to_string());
        if let Some(registry) = &self.registry {
            let peer_id = parse_registry_peer_id(node_id)?;
            registry
                .upsert_node(peer_id, node_id.to_string(), String::new(), None)
                .await;
            self.refresh_registry_metric().await;
        }
        Ok(Response::new(RegisterNodeResponse { accepted: true }))
    }

    async fn publish_task(
        &self,
        request: Request<PublishTaskRequest>,
    ) -> Result<Response<PublishTaskResponse>, Status> {
        let claimed_source = request.get_ref().source_node_id.clone();
        let authenticated_source = self.authenticate_request(&request, &claimed_source)?;
        let inner = request.into_inner();
        let task = inner
            .task
            .ok_or_else(|| Status::invalid_argument("PublishTask requires task"))?;
        let target_node_id = if inner.target_node_id.trim().is_empty() {
            target_node_id_from_task(&task)?
        } else {
            inner.target_node_id.trim().to_string()
        };
        let source_node_id = authenticated_source;
        if let Some(registry) = &self.registry {
            let peer_id = parse_registry_peer_id(&target_node_id)?;
            if let Some(skill) = skill_from_task(&task) {
                registry
                    .add_skills(
                        peer_id,
                        vec![skill],
                        target_node_id.clone(),
                        String::new(),
                        Some(Duration::from_secs(300)),
                    )
                    .await;
            } else {
                registry
                    .touch_node(peer_id, Some(Duration::from_secs(300)))
                    .await;
            }
            self.refresh_registry_metric().await;
        }
        let task_id = task
            .task_id
            .clone()
            .ok_or_else(|| Status::invalid_argument("PublishTask requires task.task_id"))?;
        if task_id.value.trim().is_empty() {
            return Err(Status::invalid_argument(
                "PublishTask requires non-empty task.task_id",
            ));
        }

        let frame_id = frame_id_for_task(&task);
        let frame = RelayFrame {
            frame_id: frame_id.clone(),
            task: Some(task),
            result: None,
            authenticated_source_node_id: source_node_id,
            destination_node_id: target_node_id.clone(),
        };
        self.runtime.route_frame(target_node_id, frame);
        Ok(Response::new(PublishTaskResponse {
            task_id: Some(task_id),
            frame_id,
        }))
    }

    async fn publish_result(
        &self,
        request: Request<PublishResultRequest>,
    ) -> Result<Response<PublishResultResponse>, Status> {
        if self.node_auth.is_none() {
            return Err(Status::unauthenticated(
                "terminal results require configured node authentication",
            ));
        }
        let claimed_source = request.get_ref().source_node_id.clone();
        let authenticated_source = self.authenticate_request(&request, &claimed_source)?;
        let inner = request.into_inner();
        let result = inner
            .result
            .ok_or_else(|| Status::invalid_argument("PublishResult requires result"))?;
        let target_node_id = required_node_value(&inner.target_node_id, "target_node_id")?;
        let source_node_id = authenticated_source;
        let task_id = result
            .task_id
            .as_ref()
            .map(|value| value.value.trim())
            .filter(|value| !value.is_empty())
            .ok_or_else(|| Status::invalid_argument("PublishResult requires result.task_id"))?;
        let frame_id = if inner.frame_id.trim().is_empty() {
            format!("result-{task_id}")
        } else {
            inner.frame_id.trim().to_string()
        };
        self.runtime.route_frame(
            target_node_id.clone(),
            RelayFrame {
                frame_id: frame_id.clone(),
                task: None,
                result: Some(result),
                authenticated_source_node_id: source_node_id,
                destination_node_id: target_node_id,
            },
        );
        Ok(Response::new(PublishResultResponse {
            accepted: true,
            frame_id,
        }))
    }

    async fn ack_frame(
        &self,
        request: Request<AckFrameRequest>,
    ) -> Result<Response<AckFrameResponse>, Status> {
        let node_id = node_id_from_metadata(&request)?;
        self.authenticate_request(&request, &node_id)?;
        let accepted = self.runtime.ack_frame(&request.into_inner().frame_id);
        Ok(Response::new(AckFrameResponse { accepted }))
    }

    async fn ack_task(
        &self,
        request: Request<AckTaskRequest>,
    ) -> Result<Response<AckTaskResponse>, Status> {
        let task_id = request
            .into_inner()
            .task_id
            .ok_or_else(|| Status::invalid_argument("AckTask requires task_id"))?;
        let accepted = self.runtime.ack_task(task_id.value.trim());
        Ok(Response::new(AckTaskResponse { accepted }))
    }

    async fn health(
        &self,
        _request: Request<HealthRequest>,
    ) -> Result<Response<HealthResponse>, Status> {
        self.refresh_registry_metric().await;
        let report = RelayHealthReport::from_runtime(&self.runtime);
        Ok(Response::new(HealthResponse {
            healthy: report.healthy,
            connected_peers: report.connected_peers,
            registry_size: report.registry_size,
            uptime_seconds: report.uptime_seconds,
            transport_status: report.transport_status,
            tasks_routed: report.tasks_routed,
            local_peer_id: report.local_peer_id,
        }))
    }
}

fn node_id_from_metadata<T>(request: &Request<T>) -> Result<String, Status> {
    request
        .metadata()
        .get(NODE_ID_METADATA_KEY)
        .ok_or_else(|| {
            Status::unauthenticated(format!(
                "ConnectNode requires {NODE_ID_METADATA_KEY} metadata"
            ))
        })?
        .to_str()
        .map(str::trim)
        .map(str::to_string)
        .map_err(|_| Status::invalid_argument("ConnectNode node metadata must be ASCII"))
        .and_then(|value| {
            if value.is_empty() {
                Err(Status::invalid_argument(
                    "ConnectNode node id cannot be empty",
                ))
            } else {
                Ok(value)
            }
        })
}

fn required_node_value(value: &str, field: &str) -> Result<String, Status> {
    let value = value.trim();
    if value.is_empty() {
        Err(Status::invalid_argument(format!("{field} is required")))
    } else {
        Ok(value.to_string())
    }
}

fn parse_registry_peer_id(value: &str) -> Result<PeerId, Status> {
    PeerId::new(value.trim()).map_err(|error| {
        Status::invalid_argument(format!("node id is not a valid registry peer id: {error}"))
    })
}

fn route_node_frame(
    runtime: &RelayRuntime,
    source_node_id: &str,
    frame: NodeFrame,
) -> Result<(), Status> {
    let target_node_id = required_node_value(&frame.target_node_id, "target_node_id")?;
    let has_task = frame.task.is_some();
    let has_result = frame.result.is_some();
    if has_task == has_result {
        return Err(Status::invalid_argument(
            "NodeFrame must contain exactly one of task or result",
        ));
    }
    let frame_id = if frame.frame_id.trim().is_empty() {
        if let Some(task) = frame.task.as_ref() {
            frame_id_for_task(task)
        } else {
            let task_id = frame
                .result
                .as_ref()
                .and_then(|result| result.task_id.as_ref())
                .map(|task_id| task_id.value.trim())
                .unwrap_or("unknown");
            format!("result-{task_id}")
        }
    } else {
        frame.frame_id
    };
    runtime.route_frame(
        target_node_id.clone(),
        RelayFrame {
            frame_id,
            task: frame.task,
            result: frame.result,
            authenticated_source_node_id: source_node_id.to_string(),
            destination_node_id: target_node_id,
        },
    );
    Ok(())
}

fn target_node_id_from_task(task: &TaskEnvelope) -> Result<String, Status> {
    TARGET_NODE_METADATA_KEYS
        .iter()
        .find_map(|key| task.metadata.get(*key))
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .ok_or_else(|| {
            Status::invalid_argument(format!(
                "task metadata must include one of: {}",
                TARGET_NODE_METADATA_KEYS.join(", ")
            ))
        })
}

fn skill_from_task(task: &TaskEnvelope) -> Option<StoredSkill> {
    SKILL_METADATA_KEYS
        .iter()
        .find_map(|key| task.metadata.get(*key))
        .map(|value| value.trim())
        .filter(|value| !value.is_empty())
        .map(|skill_id| StoredSkill {
            skill_id: skill_id.to_string(),
            description: String::new(),
            tags: task
                .metadata
                .get("skill_tags")
                .or_else(|| task.metadata.get("keryx.skill_tags"))
                .map(|raw| {
                    raw.split(',')
                        .map(str::trim)
                        .filter(|tag| !tag.is_empty())
                        .map(ToOwned::to_owned)
                        .collect()
                })
                .unwrap_or_default(),
        })
}

fn frame_id_for_task(task: &TaskEnvelope) -> String {
    task.metadata
        .get("frame_id")
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .or_else(|| {
            task.task_id
                .as_ref()
                .map(|task_id| format!("relay-{}", task_id.value))
        })
        .unwrap_or_else(|| "relay-frame".to_string())
}

/// Bind and serve the relay gRPC surface (health RPC today; other RPCs stubbed).
pub async fn serve_grpc_health(
    runtime: Arc<RelayRuntime>,
    registry: Option<Arc<SkillRegistry>>,
    addr: SocketAddr,
) -> Result<(), tonic::transport::Error> {
    let listener = TcpListener::bind(addr).await.expect("bind health grpc");
    let incoming = TcpListenerStream::new(listener);
    let service = match registry {
        Some(registry) => RelayHealthService::with_registry(runtime, registry),
        None => RelayHealthService::new(runtime),
    };
    tonic::transport::Server::builder()
        .add_service(
            KeryxRelayServer::new(service)
                .max_decoding_message_size(RESULT_ARTIFACT_FRAME_MAX_BYTES)
                .max_encoding_message_size(RESULT_ARTIFACT_FRAME_MAX_BYTES),
        )
        .serve_with_incoming(incoming)
        .await
}

pub async fn serve_grpc_health_with_auth(
    runtime: Arc<RelayRuntime>,
    registry: Arc<SkillRegistry>,
    node_auth: Arc<NodeTokenAuth>,
    addr: SocketAddr,
) -> Result<(), tonic::transport::Error> {
    let listener = TcpListener::bind(addr).await.expect("bind health grpc");
    let incoming = TcpListenerStream::new(listener);
    let service = RelayHealthService::with_registry_and_auth(runtime, registry, node_auth);
    tonic::transport::Server::builder()
        .add_service(
            KeryxRelayServer::new(service)
                .max_decoding_message_size(RESULT_ARTIFACT_FRAME_MAX_BYTES)
                .max_encoding_message_size(RESULT_ARTIFACT_FRAME_MAX_BYTES),
        )
        .serve_with_incoming(incoming)
        .await
}

/// Accept HTTP `GET /health` on `addr` until `shutdown` completes.
pub async fn serve_http_health(
    runtime: Arc<RelayRuntime>,
    addr: SocketAddr,
    mut shutdown: tokio::sync::broadcast::Receiver<()>,
) {
    let listener = match TcpListener::bind(addr).await {
        Ok(listener) => listener,
        Err(err) => {
            tracing::error!(%addr, error = %err, "failed to bind HTTP health listener");
            return;
        }
    };
    loop {
        tokio::select! {
            _ = shutdown.recv() => break,
            accept = listener.accept() => {
                match accept {
                    Ok((stream, _)) => {
                        let rt = Arc::clone(&runtime);
                        tokio::spawn(async move {
                            if let Err(err) = crate::health::serve_http_health_once(rt, stream).await {
                                tracing::debug!(error = %err, "HTTP health connection ended");
                            }
                        });
                    }
                    Err(err) => tracing::warn!(error = %err, "HTTP health accept failed"),
                }
            }
        }
    }
}
