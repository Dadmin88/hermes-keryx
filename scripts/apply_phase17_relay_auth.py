#!/usr/bin/env python3
"""Bind relay source identities to configured node tokens."""
from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def read(path: str) -> str:
    return (ROOT / path).read_text()


def write(path: str, value: str) -> None:
    (ROOT / path).write_text(value)


def replace_once(path: str, old: str, new: str) -> None:
    text = read(path)
    if new in text:
        return
    if old not in text:
        raise RuntimeError(f"anchor missing in {path}: {old[:100]!r}")
    write(path, text.replace(old, new, 1))


# Relay TOML supports dynamic control-plane endpoints for authenticated tests and deployments.
security_path = "crates/keryx-relay/src/security.rs"
text = read(security_path)
replace_pairs = [
    (
        '''    #[serde(default)]
    pub use_ipv6: bool,
}''',
        '''    #[serde(default)]
    pub use_ipv6: bool,
    #[serde(default = "crate::config::default_health_grpc_bind")]
    pub health_grpc_bind: String,
    #[serde(default = "crate::config::default_health_http_bind")]
    pub health_http_bind: String,
    #[serde(default = "crate::config::default_registry_grpc_bind")]
    pub registry_grpc_bind: String,
}''',
    ),
    (
        '''            connection_timeout_ms: crate::config::default_connection_timeout_ms(),
            use_ipv6: false,
        }''',
        '''            connection_timeout_ms: crate::config::default_connection_timeout_ms(),
            use_ipv6: false,
            health_grpc_bind: crate::config::default_health_grpc_bind(),
            health_http_bind: crate::config::default_health_http_bind(),
            registry_grpc_bind: crate::config::default_registry_grpc_bind(),
        }''',
    ),
    (
        '''            health_grpc_bind: crate::config::default_health_grpc_bind(),
            health_http_bind: crate::config::default_health_http_bind(),
            registry_grpc_bind: crate::config::default_registry_grpc_bind(),''',
        '''            health_grpc_bind: self.relay.health_grpc_bind.clone(),
            health_http_bind: self.relay.health_http_bind.clone(),
            registry_grpc_bind: self.relay.registry_grpc_bind.clone(),''',
    ),
]
for old, new in replace_pairs:
    if new not in text:
        if old not in text:
            raise RuntimeError(f"security.rs anchor missing: {old[:80]!r}")
        text = text.replace(old, new, 1)
write(security_path, text)

# Authenticated relay service.
health_path = "crates/keryx-relay/src/health_server.rs"
text = read(health_path)
text = text.replace(
    "use crate::runtime::RelayRuntime;",
    "use crate::runtime::RelayRuntime;\nuse crate::security::NodeTokenAuth;",
)
text = text.replace(
    'pub const NODE_ID_METADATA_KEY: &str = "x-keryx-node-id";',
    'pub const NODE_ID_METADATA_KEY: &str = "x-keryx-node-id";\npub const NODE_TOKEN_METADATA_KEY: &str = "x-keryx-node-token";',
)
text = text.replace(
    '''pub struct RelayHealthService {
    runtime: Arc<RelayRuntime>,
    registry: Option<Arc<SkillRegistry>>,
}''',
    '''pub struct RelayHealthService {
    runtime: Arc<RelayRuntime>,
    registry: Option<Arc<SkillRegistry>>,
    node_auth: Option<Arc<NodeTokenAuth>>,
}''',
)
text = text.replace(
    '''        Self {
            runtime,
            registry: None,
        }''',
    '''        Self {
            runtime,
            registry: None,
            node_auth: None,
        }''',
    1,
)
text = text.replace(
    '''        Self {
            runtime,
            registry: Some(registry),
        }''',
    '''        Self {
            runtime,
            registry: Some(registry),
            node_auth: None,
        }''',
    1,
)
auth_constructor_anchor = '''    async fn refresh_registry_metric(&self) {
'''
auth_constructor = '''    #[must_use]
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
                Status::unauthenticated(format!(
                    "node authentication failed: {}",
                    failure.reason()
                ))
            })?;
        Ok(metadata_node_id.to_string())
    }

'''
if auth_constructor not in text:
    text = text.replace(auth_constructor_anchor, auth_constructor + auth_constructor_anchor)
# ConnectNode authentication.
text = text.replace(
    '''        let node_id = node_id_from_metadata(&request)?;
        let mut inbound = request.into_inner();''',
    '''        let node_id = node_id_from_metadata(&request)?;
        let node_id = self.authenticate_request(&request, &node_id)?;
        let mut inbound = request.into_inner();''',
)
# RegisterNode body-token authentication.
text = text.replace(
    '''        let request = request.into_inner();
        let node_id = request
            .node_id''',
    '''        let inner = request.into_inner();
        let node_id = inner
            .node_id''',
)
text = text.replace(
    '''            .ok_or_else(|| Status::invalid_argument("RegisterNode requires node_id"))?;
        self.runtime.register_node(node_id.to_string());''',
    '''            .ok_or_else(|| Status::invalid_argument("RegisterNode requires node_id"))?;
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
        self.runtime.register_node(node_id.to_string());''',
)
# PublishTask authenticates before consuming request.
text = text.replace(
    '''        let inner = request.into_inner();
        let task = inner
            .task''',
    '''        let claimed_source = request.get_ref().source_node_id.clone();
        let authenticated_source = self.authenticate_request(&request, &claimed_source)?;
        let inner = request.into_inner();
        let task = inner
            .task''',
    1,
)
text = text.replace(
    '''        let source_node_id = required_node_value(&inner.source_node_id, "source_node_id")?;''',
    '''        let source_node_id = authenticated_source;''',
    1,
)
# PublishResult authentication.
result_method_anchor = '''    async fn publish_result(
        &self,
        request: Request<PublishResultRequest>,
    ) -> Result<Response<PublishResultResponse>, Status> {
        let inner = request.into_inner();'''
result_method_new = '''    async fn publish_result(
        &self,
        request: Request<PublishResultRequest>,
    ) -> Result<Response<PublishResultResponse>, Status> {
        let claimed_source = request.get_ref().source_node_id.clone();
        let authenticated_source = self.authenticate_request(&request, &claimed_source)?;
        let inner = request.into_inner();'''
text = text.replace(result_method_anchor, result_method_new)
text = text.replace(
    '''        let source_node_id = required_node_value(&inner.source_node_id, "source_node_id")?;''',
    '''        let source_node_id = authenticated_source;''',
    1,
)
# AckFrame requires authenticated metadata node.
ack_anchor = '''    async fn ack_frame(
        &self,
        request: Request<AckFrameRequest>,
    ) -> Result<Response<AckFrameResponse>, Status> {
        let accepted = self.runtime.ack_frame(&request.into_inner().frame_id);'''
ack_new = '''    async fn ack_frame(
        &self,
        request: Request<AckFrameRequest>,
    ) -> Result<Response<AckFrameResponse>, Status> {
        let node_id = node_id_from_metadata(&request)?;
        self.authenticate_request(&request, &node_id)?;
        let accepted = self.runtime.ack_frame(&request.into_inner().frame_id);'''
text = text.replace(ack_anchor, ack_new)
# Auth-aware server entrypoint.
serve_anchor = '''/// Accept HTTP `GET /health` on `addr` until `shutdown` completes.'''
serve_auth = '''pub async fn serve_grpc_health_with_auth(
    runtime: Arc<RelayRuntime>,
    registry: Arc<SkillRegistry>,
    node_auth: Arc<NodeTokenAuth>,
    addr: SocketAddr,
) -> Result<(), tonic::transport::Error> {
    let listener = TcpListener::bind(addr).await.expect("bind health grpc");
    let incoming = TcpListenerStream::new(listener);
    let service = RelayHealthService::with_registry_and_auth(runtime, registry, node_auth);
    tonic::transport::Server::builder()
        .add_service(KeryxRelayServer::new(service))
        .serve_with_incoming(incoming)
        .await
}

'''
if serve_auth not in text:
    text = text.replace(serve_anchor, serve_auth + serve_anchor)
write(health_path, text)

# Relay process loads configured auth and uses the authenticated control-plane server.
main_path = "crates/keryx-relay/src/main.rs"
text = read(main_path)
text = text.replace(
    "health_server::{serve_grpc_health, serve_http_health},",
    "health_server::{serve_grpc_health, serve_grpc_health_with_auth, serve_http_health},",
)
node_auth_anchor = '''    let shared_allowlist: Option<SharedAllowlist> = process
        .allowlist'''
node_auth_code = '''    let node_auth = process
        .toml
        .as_ref()
        .map(|config| config.load_node_token_auth(&config_path))
        .transpose()?
        .unwrap_or_default();
    let node_auth_configured = node_auth.is_configured();
    let node_auth = Arc::new(node_auth);

'''
if node_auth_code not in text:
    text = text.replace(node_auth_anchor, node_auth_code + node_auth_anchor)
text = text.replace(
    '''        let rt = Arc::clone(&runtime);
        let reg = Arc::clone(&registry);
        tokio::spawn(async move {
            if let Err(err) = serve_grpc_health(rt, Some(reg), addr).await {''',
    '''        let rt = Arc::clone(&runtime);
        let reg = Arc::clone(&registry);
        let auth = Arc::clone(&node_auth);
        tokio::spawn(async move {
            let result = if node_auth_configured {
                serve_grpc_health_with_auth(rt, reg, auth, addr).await
            } else {
                serve_grpc_health(rt, Some(reg), addr).await
            };
            if let Err(err) = result {''',
)
write(main_path, text)

# Daemon publisher attaches its authenticated source identity and token.
routing_path = "crates/keryx-daemon/src/routing.rs"
text = read(routing_path)
text = text.replace(
    '''pub struct GrpcRelayTaskPublisher {
    endpoint: String,
    source_peer_id: PeerId,
}''',
    '''pub struct GrpcRelayTaskPublisher {
    endpoint: String,
    source_peer_id: PeerId,
    node_token: Option<String>,
}''',
)
text = text.replace(
    '''        Self {
            endpoint: endpoint.into(),
            source_peer_id,
        }''',
    '''        Self {
            endpoint: endpoint.into(),
            source_peer_id,
            node_token: std::env::var("HERMES_KERYX_NODE_TOKEN")
                .ok()
                .map(|value| value.trim().to_string())
                .filter(|value| !value.is_empty()),
        }''',
)
old_publish = '''        client
            .publish_task(Request::new(PublishTaskRequest {
                task: Some(envelope),
                target_node_id: target_peer_id.as_str().to_string(),
                source_node_id: self.source_peer_id.as_str().to_string(),
            }))'''
new_publish = '''        let mut request = Request::new(PublishTaskRequest {
            task: Some(envelope),
            target_node_id: target_peer_id.as_str().to_string(),
            source_node_id: self.source_peer_id.as_str().to_string(),
        });
        request.metadata_mut().insert(
            "x-keryx-node-id",
            self.source_peer_id.as_str().parse().map_err(|error| RoutingError::RelayFailed {
                peer_id: target_peer_id.to_string(),
                reason: format!("invalid source peer metadata: {error}"),
            })?,
        );
        if let Some(token) = &self.node_token {
            request.metadata_mut().insert(
                "x-keryx-node-token",
                token.parse().map_err(|error| RoutingError::RelayFailed {
                    peer_id: target_peer_id.to_string(),
                    reason: format!("invalid node token metadata: {error}"),
                })?,
            );
        }
        client
            .publish_task(request)'''
text = text.replace(old_publish, new_publish)
write(routing_path, text)

# Edge attaches credentials to stream, result publication, and acknowledgements.
node_path = "crates/keryx-relay/src/node.rs"
text = read(node_path)
text = text.replace(
    'const NODE_RELAY_HEALTH_ENDPOINT_ENV: &str = "HERMES_KERYX_RELAY_HEALTH_ENDPOINT";',
    'const NODE_RELAY_HEALTH_ENDPOINT_ENV: &str = "HERMES_KERYX_RELAY_HEALTH_ENDPOINT";\nconst NODE_TOKEN_ENV: &str = "HERMES_KERYX_NODE_TOKEN";',
)
text = text.replace(
    '''                    run_relay_stream(relay_endpoint, registry_peer_id, daemon_endpoint).await''',
    '''                    run_relay_stream(
                        relay_endpoint,
                        registry_peer_id,
                        node_token(),
                        daemon_endpoint,
                    )
                    .await''',
)
text = text.replace(
    '''async fn run_relay_stream(
    relay_endpoint: String,
    registry_peer_id: String,
    daemon_endpoint: String,
) -> Result<()> {''',
    '''async fn run_relay_stream(
    relay_endpoint: String,
    registry_peer_id: String,
    node_token: Option<String>,
    daemon_endpoint: String,
) -> Result<()> {''',
)
text = text.replace(
    '''    request.metadata_mut().insert(
        crate::health_server::NODE_ID_METADATA_KEY,
        registry_peer_id.parse()?,
    );''',
    '''    add_node_auth_metadata(&mut request, &registry_peer_id, node_token.as_deref())?;''',
)
text = text.replace(
    '''                let mut ack_client = KeryxRelayClient::connect(relay_endpoint.clone()).await?;
                ack_client
                    .ack_frame(AckFrameRequest { frame_id: frame.frame_id })''',
    '''                let mut ack_client = KeryxRelayClient::connect(relay_endpoint.clone()).await?;
                let mut ack_request = Request::new(AckFrameRequest { frame_id: frame.frame_id });
                add_node_auth_metadata(
                    &mut ack_request,
                    &registry_peer_id,
                    node_token.as_deref(),
                )?;
                ack_client
                    .ack_frame(ack_request)''',
)
text = text.replace(
    '''                let publish = relay
                    .publish_result(PublishResultRequest {
                        result: delivery.result,
                        target_node_id: delivery.target_peer_id,
                        source_node_id: registry_peer_id.clone(),
                        frame_id: delivery.delivery_id.clone(),
                    })
                    .await;''',
    '''                let mut publish_request = Request::new(PublishResultRequest {
                    result: delivery.result,
                    target_node_id: delivery.target_peer_id,
                    source_node_id: registry_peer_id.clone(),
                    frame_id: delivery.delivery_id.clone(),
                });
                add_node_auth_metadata(
                    &mut publish_request,
                    &registry_peer_id,
                    node_token.as_deref(),
                )?;
                let publish = relay.publish_result(publish_request).await;''',
)
helper_anchor = '''fn daemon_endpoint() -> Option<String> {'''
helper = '''fn node_token() -> Option<String> {
    std::env::var(NODE_TOKEN_ENV)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

fn add_node_auth_metadata<T>(
    request: &mut Request<T>,
    node_id: &str,
    token: Option<&str>,
) -> Result<()> {
    request.metadata_mut().insert(
        crate::health_server::NODE_ID_METADATA_KEY,
        node_id.parse()?,
    );
    if let Some(token) = token {
        request.metadata_mut().insert(
            crate::health_server::NODE_TOKEN_METADATA_KEY,
            token.parse()?,
        );
    }
    Ok(())
}

'''
if helper not in text:
    text = text.replace(helper_anchor, helper + helper_anchor)
write(node_path, text)

# E2E uses authenticated TOML relay config and shared node tokens.
e2e_path = "scripts/e2e_two_node.py"
text = read(e2e_path)
text = text.replace(
    'EXPECTED_TEXT = "remote-result:phase17-cross-node"',
    'EXPECTED_TEXT = "remote-result:phase17-cross-node"\nSENDER_TOKEN = "sender-token-phase17"\nRECEIVER_TOKEN = "receiver-token-phase17"',
)
text = text.replace(
    '''            "HERMES_KERYX_RELAY_REGISTRY_ENDPOINT": f"http://127.0.0.1:{registry_port}",
        }''',
    '''            "HERMES_KERYX_RELAY_REGISTRY_ENDPOINT": f"http://127.0.0.1:{registry_port}",
            "HERMES_KERYX_NODE_TOKEN": SENDER_TOKEN if peer_id == SENDER_PEER else RECEIVER_TOKEN,
        }''',
    1,
)
text = text.replace(
    '''            "HERMES_KERYX_NODE_SKILLS": skills,
        }''',
    '''            "HERMES_KERYX_NODE_SKILLS": skills,
            "HERMES_KERYX_NODE_TOKEN": SENDER_TOKEN if peer_id == SENDER_PEER else RECEIVER_TOKEN,
        }''',
)
json_block_start = '''    relay_config = work_dir / "relay.json"
    relay_config.write_text(
        json.dumps(
            {
                "listen_addresses": ["tcp:0"],
                "bootstrap_peers": [],
                "enable_mdns": False,
                "keypair_path": None,
                "max_circuits": 16,
                "max_reservations": 16,
                "max_reservations_per_peer": 4,
                "connection_timeout_ms": 5_000,
                "use_ipv6": False,
                "health_grpc_bind": f"127.0.0.1:{relay_port}",
                "health_http_bind": "",
                "registry_grpc_bind": f"127.0.0.1:{registry_port}",
            },
            indent=2,
        )
        + "\\n",
        encoding="utf-8",
    )'''
toml_block = '''    relay_config = work_dir / "relay.toml"
    relay_config.write_text(
        f'''[relay]
listen_addresses = ["tcp:0"]
bootstrap_peers = []
enable_mdns = false
max_circuits = 16
max_reservations = 16
max_reservations_per_peer = 4
connection_timeout_ms = 5000
use_ipv6 = false
health_grpc_bind = "127.0.0.1:{relay_port}"
health_http_bind = ""
registry_grpc_bind = "127.0.0.1:{registry_port}"

[[security.node_tokens]]
node_id = "{SENDER_PEER}"
token = "{SENDER_TOKEN}"

[[security.node_tokens]]
node_id = "{RECEIVER_PEER}"
token = "{RECEIVER_TOKEN}"
''',
        encoding="utf-8",
    )'''
if json_block_start in text:
    text = text.replace(json_block_start, toml_block)
else:
    raise RuntimeError("E2E relay JSON config anchor missing")
write(e2e_path, text)

print("Phase 17 authenticated relay source binding applied")
