#!/usr/bin/env python3
from __future__ import annotations

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def read(path: str) -> str:
    return (ROOT / path).read_text(encoding="utf-8")


def write(path: str, text: str) -> None:
    (ROOT / path).write_text(text, encoding="utf-8")


def replace_exact(path: str, old: str, new: str, expected: int = 1) -> None:
    text = read(path)
    count = text.count(old)
    if count != expected:
        raise SystemExit(f"{path}: expected {expected} occurrences, found {count}: {old[:100]!r}")
    write(path, text.replace(old, new))


def insert_rpc_auth(text: str, method: str) -> str:
    start = text.find(f"    async fn {method}(")
    if start < 0:
        raise SystemExit(f"daemon RPC method not found: {method}")
    next_method = text.find("    async fn ", start + 8)
    end = len(text) if next_method < 0 else next_method
    guard = "        let _rpc = RpcInFlightGuard::enter(&self.runtime)?;"
    guard_at = text.find(guard, start, end)
    if guard_at < 0:
        raise SystemExit(f"RPC guard not found for {method}")
    auth = "\n        authorize_daemon_request(&self.runtime, &request)?;"
    if auth.strip() in text[guard_at:end]:
        raise SystemExit(f"auth already present for {method}")
    insert_at = guard_at + len(guard)
    return text[:insert_at] + auth + text[insert_at:]


# --- Store cancellation ownership error and result-preserving cancellation fence. ---
replace_exact(
    "crates/keryx-store/src/lib.rs",
    '    #[error("lease {lease_id} does not own task {task_id}")]\n    LeaseMismatch { task_id: TaskId, lease_id: LeaseId },\n',
    '    #[error("running task cancellation requires active lease ownership proof: {0}")]\n'
    '    CancellationLeaseProofRequired(TaskId),\n'
    '    #[error("lease {lease_id} does not own task {task_id}")]\n'
    '    LeaseMismatch { task_id: TaskId, lease_id: LeaseId },\n',
)

results_path = "crates/keryx-store/src/results.rs"
results = read(results_path)
sig_old = "        task_id: &TaskId,\n        _reason: &str,\n        now_ms: i64,\n        result: TerminalResultRecord,\n"
sig_new = "        task_id: &TaskId,\n        lease_id: Option<&LeaseId>,\n        worker_id: Option<&AgentId>,\n        _reason: &str,\n        now_ms: i64,\n        result: TerminalResultRecord,\n"
if results.count(sig_old) != 2:
    raise SystemExit(f"results.rs: expected two cancel-result signatures, found {results.count(sig_old)}")
results = results.replace(sig_old, sig_new)
active_line = "            ensure_active_lease_unexpired(&active, now_ms)?;\n"
if results.count(active_line) != 2:
    raise SystemExit(
        f"results.rs: expected two cancellation active-lease checks, found {results.count(active_line)}"
    )
proof = (
    "            let lease_id = lease_id.ok_or_else(|| {\n"
    "                StoreError::CancellationLeaseProofRequired(task_id.clone())\n"
    "            })?;\n"
    "            let worker_id = worker_id.ok_or_else(|| {\n"
    "                StoreError::CancellationLeaseProofRequired(task_id.clone())\n"
    "            })?;\n"
    "            ensure_matching_lease_id(task_id, &active, lease_id)?;\n"
    "            ensure_matching_worker_id(task_id, &active, worker_id)?;\n"
)
results = results.replace(active_line, proof + active_line)
write(results_path, results)

# --- Typed cancellation proof on the daemon protocol. ---
replace_exact(
    "proto/hermes/keryx/v1/daemon.proto",
    "message CancelTaskRequest {\n"
    "  // A task targeted at another executor fails closed with FAILED_PRECONDITION;\n"
    "  // this RPC does not claim to stop remote work.\n"
    "  TaskId task_id = 1;\n"
    "  string reason = 2;\n"
    "  map<string, string> metadata = 3;\n"
    "}\n",
    "message CancelTaskRequest {\n"
    "  // A task targeted at another executor fails closed with FAILED_PRECONDITION;\n"
    "  // this RPC does not claim to stop remote work.\n"
    "  TaskId task_id = 1;\n"
    "  string reason = 2;\n"
    "  map<string, string> metadata = 3;\n"
    "  // Required when canceling a running task; proves the active lease generation.\n"
    "  LeaseId lease_id = 4;\n"
    "  // Required when canceling a running task; must match the active lease owner.\n"
    "  AgentId worker_id = 5;\n"
    "}\n",
)

# --- Current daemon authorization contract. ---
daemon_path = "crates/keryx-daemon/src/lib.rs"
daemon = read(daemon_path)
for old, new in [
    (
        "    discovery: Option<DiscoverySettings>,\n    relay_endpoint: Option<String>,\n}",
        "    discovery: Option<DiscoverySettings>,\n    relay_endpoint: Option<String>,\n    daemon_rpc_token: Option<String>,\n}",
    ),
    (
        "            discovery: None,\n            relay_endpoint: None,\n        }",
        "            discovery: None,\n            relay_endpoint: None,\n            daemon_rpc_token: None,\n        }",
    ),
]:
    if daemon.count(old) != 1:
        raise SystemExit(f"daemon config shape drifted: {old!r}")
    daemon = daemon.replace(old, new, 1)

accessor = "    #[must_use]\n    pub fn relay_endpoint(&self) -> Option<&str> {\n        self.relay_endpoint.as_deref()\n    }\n"
if daemon.count(accessor) != 1:
    raise SystemExit("daemon relay accessor shape drifted")
daemon = daemon.replace(
    accessor,
    accessor
    + "\n"
    + "    #[must_use]\n"
    + "    pub fn with_daemon_rpc_token(mut self, token: Option<String>) -> Self {\n"
    + "        self.daemon_rpc_token = token\n"
    + "            .map(|value| value.trim().to_string())\n"
    + "            .filter(|value| !value.is_empty());\n"
    + "        self\n"
    + "    }\n\n"
    + "    #[must_use]\n"
    + "    pub fn daemon_rpc_token(&self) -> Option<&str> {\n"
    + "        self.daemon_rpc_token.as_deref()\n"
    + "    }\n",
    1,
)

const_marker = 'const DAEMON_REGISTRATION_TTL_ENV: &str = "HERMES_KERYX_DAEMON_REGISTRATION_TTL_SECONDS";\n'
if daemon.count(const_marker) != 1:
    raise SystemExit("daemon env constant marker drifted")
daemon = daemon.replace(
    const_marker,
    const_marker
    + 'const DAEMON_RPC_TOKEN_ENV: &str = "HERMES_KERYX_DAEMON_TOKEN";\n'
    + 'const DAEMON_AUTHORIZATION_HEADER: &str = "authorization";\n\n'
    + "/// Build local daemon RPC bearer-token material from the environment.\n"
    + "#[must_use]\n"
    + "pub fn daemon_rpc_token_from_env() -> Option<String> {\n"
    + "    std::env::var(DAEMON_RPC_TOKEN_ENV)\n"
    + "        .ok()\n"
    + "        .map(|value| value.trim().to_string())\n"
    + "        .filter(|value| !value.is_empty())\n"
    + "}\n",
    1,
)

helper_marker = "/// Serve the minimal local daemon RPC surface used by the CLI readiness client.\n"
if daemon.count(helper_marker) != 1:
    raise SystemExit("daemon service marker drifted")
auth_helper = r'''fn constant_time_token_eq(left: &[u8], right: &[u8]) -> bool {
    let mut diff = left.len() ^ right.len();
    let width = left.len().max(right.len());
    for index in 0..width {
        let left_byte = left.get(index).copied().unwrap_or_default();
        let right_byte = right.get(index).copied().unwrap_or_default();
        diff |= usize::from(left_byte ^ right_byte);
    }
    diff == 0
}

fn authorize_daemon_request<T>(
    runtime: &KeryxDaemonRuntime,
    request: &Request<T>,
) -> Result<(), Status> {
    // Direct in-process service tests may intentionally construct a runtime without
    // a listener credential. The network server below refuses to start that way.
    let Some(expected) = runtime.config().daemon_rpc_token() else {
        return Ok(());
    };
    let raw = request
        .metadata()
        .get(DAEMON_AUTHORIZATION_HEADER)
        .and_then(|value| value.to_str().ok())
        .ok_or_else(|| Status::unauthenticated("daemon RPC bearer token is required"))?;
    let supplied = raw
        .strip_prefix("Bearer ")
        .filter(|value| !value.is_empty())
        .ok_or_else(|| Status::unauthenticated("daemon RPC bearer token is required"))?;
    if constant_time_token_eq(expected.as_bytes(), supplied.as_bytes()) {
        Ok(())
    } else {
        Err(Status::permission_denied("daemon RPC bearer token is invalid"))
    }
}

'''
daemon = daemon.replace(helper_marker, auth_helper + helper_marker, 1)

serve_guard = "pub async fn serve_daemon_rpc(\n    runtime: KeryxDaemonRuntime,\n    incoming: TcpListenerStream,\n) -> Result<(), tonic::transport::Error> {\n    let shutdown_signal = runtime.shutdown.grpc_shutdown_wait();\n"
if daemon.count(serve_guard) != 1:
    raise SystemExit("serve_daemon_rpc shape drifted")
daemon = daemon.replace(
    serve_guard,
    "pub async fn serve_daemon_rpc(\n"
    "    runtime: KeryxDaemonRuntime,\n"
    "    incoming: TcpListenerStream,\n"
    ") -> Result<(), tonic::transport::Error> {\n"
    "    assert!(\n"
    "        runtime.config().daemon_rpc_token().is_some(),\n"
    '        "daemon RPC listeners require HERMES_KERYX_DAEMON_TOKEN"\n'
    "    );\n"
    "    let shutdown_signal = runtime.shutdown.grpc_shutdown_wait();\n",
    1,
)

sensitive = [
    "submit_task",
    "submit_remote_task",
    "claim_task",
    "claim_next_task",
    "heartbeat",
    "complete_task",
    "fail_task",
    "cancel_task",
    "put_artifact",
    "get_artifact",
    "list_artifacts",
    "delete_artifact",
    "get_task_result",
    "claim_next_result_delivery",
    "ack_result_delivery",
    "fail_result_delivery",
    "ingest_remote_result",
    "send_task",
]
for method in sensitive:
    daemon = insert_rpc_auth(daemon, method)

cancel_trace = '        fields(task_id = tracing::field::Empty, reason = tracing::field::Empty)\n'
if daemon.count(cancel_trace) != 1:
    raise SystemExit("cancel trace shape drifted")
daemon = daemon.replace(
    cancel_trace,
    "        fields(\n"
    "            task_id = tracing::field::Empty,\n"
    "            lease_id = tracing::field::Empty,\n"
    "            worker_id = tracing::field::Empty,\n"
    "            reason = tracing::field::Empty\n"
    "        )\n",
    1,
)

cancel_parse = (
    "        let task_id = parse_required_task_id(inner.task_id.as_ref())?;\n"
    "        let reason = normalized_cancel_reason(&inner.reason);\n"
    "        tracing::Span::current().record(\"task_id\", tracing::field::display(task_id.as_str()));\n"
    "        tracing::Span::current().record(\"reason\", tracing::field::display(&reason));\n"
)
if daemon.count(cancel_parse) != 1:
    raise SystemExit("cancel parse shape drifted")
daemon = daemon.replace(
    cancel_parse,
    "        let task_id = parse_required_task_id(inner.task_id.as_ref())?;\n"
    "        let lease_id = parse_optional_lease_id(inner.lease_id.as_ref())?;\n"
    "        let worker_id = parse_optional_agent_id(inner.worker_id.as_ref())?;\n"
    "        let reason = normalized_cancel_reason(&inner.reason);\n"
    "        tracing::Span::current().record(\"task_id\", tracing::field::display(task_id.as_str()));\n"
    "        if let Some(lease_id) = lease_id.as_ref() {\n"
    "            tracing::Span::current().record(\"lease_id\", tracing::field::display(lease_id.as_str()));\n"
    "        }\n"
    "        if let Some(worker_id) = worker_id.as_ref() {\n"
    "            tracing::Span::current().record(\"worker_id\", tracing::field::display(worker_id.as_str()));\n"
    "        }\n"
    "        tracing::Span::current().record(\"reason\", tracing::field::display(&reason));\n",
    1,
)

cancel_call = "            .cancel_task_with_result(\n                &task_id,\n                &reason,\n"
if daemon.count(cancel_call) != 1:
    raise SystemExit("cancel result call shape drifted")
daemon = daemon.replace(
    cancel_call,
    "            .cancel_task_with_result(\n"
    "                &task_id,\n"
    "                lease_id.as_ref(),\n"
    "                worker_id.as_ref(),\n"
    "                &reason,\n",
    1,
)

lease_fn = re.search(
    r"fn parse_required_lease_id\([^\n]*\n(?:.*\n)*?\}\n",
    daemon,
)
if lease_fn is None:
    raise SystemExit("parse_required_lease_id function not found")
optional_helpers = r'''
fn parse_optional_agent_id(id: Option<&ProtoAgentId>) -> Result<Option<AgentId>, Status> {
    id.map(|id| parse_required_agent_id(Some(id))).transpose()
}

fn parse_optional_lease_id(id: Option<&ProtoLeaseId>) -> Result<Option<LeaseId>, Status> {
    id.map(|id| parse_required_lease_id(Some(id))).transpose()
}
'''
insert_at = lease_fn.end()
daemon = daemon[:insert_at] + optional_helpers + daemon[insert_at:]

mapping_marker = "        StoreError::LeaseMismatch { task_id, lease_id } => Status::permission_denied(format!(\n"
if daemon.count(mapping_marker) != 1:
    raise SystemExit("store error mapping marker drifted")
daemon = daemon.replace(
    mapping_marker,
    "        StoreError::CancellationLeaseProofRequired(task_id) => Status::permission_denied(\n"
    "            format!(\"running task cancellation requires active lease ownership proof for {}\", task_id.as_str()),\n"
    "        ),\n"
    + mapping_marker,
    1,
)
write(daemon_path, daemon)

# --- keryxd binary loads one token and refuses a listening daemon without it. ---
main_path = "crates/keryx-daemon/src/main.rs"
main = read(main_path)
main = main.replace(
    "    discovery_settings_from_env, relay_endpoint_from_env, serve_daemon_rpc, KeryxDaemonConfig,\n"
    "    KeryxDaemonRuntime,\n",
    "    daemon_rpc_token_from_env, discovery_settings_from_env, relay_endpoint_from_env,\n"
    "    serve_daemon_rpc, KeryxDaemonConfig, KeryxDaemonRuntime,\n",
    1,
)
old = (
    "    if let Some(relay_endpoint) = relay_endpoint_from_env() {\n"
    "        config = config.with_relay_endpoint(Some(relay_endpoint));\n"
    "    }\n"
    "    let runtime = Arc::new(KeryxDaemonRuntime::startup(config).await?);\n"
)
new = (
    "    if let Some(relay_endpoint) = relay_endpoint_from_env() {\n"
    "        config = config.with_relay_endpoint(Some(relay_endpoint));\n"
    "    }\n"
    "    if let Some(token) = daemon_rpc_token_from_env() {\n"
    "        config = config.with_daemon_rpc_token(Some(token));\n"
    "    }\n"
    "    let daemon_addr = daemon_addr()?;\n"
    "    if daemon_addr.is_some() {\n"
    "        anyhow::ensure!(\n"
    "            config.daemon_rpc_token().is_some(),\n"
    '            "HERMES_KERYX_DAEMON_TOKEN is required when HERMES_KERYX_DAEMON_ADDR enables the daemon RPC listener"\n'
    "        );\n"
    "    }\n"
    "    let runtime = Arc::new(KeryxDaemonRuntime::startup(config).await?);\n"
)
if main.count(old) != 1:
    raise SystemExit("keryxd startup shape drifted")
main = main.replace(old, new, 1)
if main.count("    if let Some(addr) = daemon_addr()? {\n") != 1:
    raise SystemExit("keryxd listener shape drifted")
main = main.replace("    if let Some(addr) = daemon_addr()? {\n", "    if let Some(addr) = daemon_addr {\n", 1)
write(main_path, main)

# --- Test RPC harness authenticates transparently so existing integration tests keep their intent. ---
common_path = "crates/keryx-daemon/tests/common/mod.rs"
common = read(common_path)
common = common.replace(
    "use tokio_stream::wrappers::TcpListenerStream;\n",
    "use tokio_stream::wrappers::TcpListenerStream;\n"
    "use tonic::service::interceptor::InterceptedService;\n"
    "use tonic::service::Interceptor;\n"
    "use tonic::transport::Channel;\n"
    "use tonic::Request;\n",
    1,
)
client_import = "use keryx_proto::v1::keryx_daemon_client::KeryxDaemonClient;\n"
marker = client_import + "use keryx_proto::v1::TaskEnvelope;\n"
if common.count(marker) != 1:
    raise SystemExit("RPC harness import marker drifted")
common = common.replace(
    marker,
    marker
    + "\n"
    + 'const TEST_DAEMON_TOKEN: &str = "keryx-rpc-test-daemon-token";\n\n'
    + "#[derive(Clone)]\n"
    + "struct TestDaemonTokenInterceptor;\n\n"
    + "impl Interceptor for TestDaemonTokenInterceptor {\n"
    + "    fn call(&mut self, mut request: Request<()>) -> Result<Request<()>, tonic::Status> {\n"
    + "        request.metadata_mut().insert(\n"
    + '            "authorization",\n'
    + "            format!(\"Bearer {TEST_DAEMON_TOKEN}\")\n"
    + "                .parse()\n"
    + "                .expect(\"static test daemon token is valid metadata\"),\n"
    + "        );\n"
    + "        Ok(request)\n"
    + "    }\n"
    + "}\n\n"
    + "type TestDaemonClient =\n"
    + "    KeryxDaemonClient<InterceptedService<Channel, TestDaemonTokenInterceptor>>;\n\n"
    + "async fn authenticated_client(addr: std::net::SocketAddr) -> TestDaemonClient {\n"
    + "    let endpoint = format!(\"http://{addr}\");\n"
    + "    let channel = tonic::transport::Endpoint::from_shared(endpoint)\n"
    + "        .unwrap()\n"
    + "        .connect()\n"
    + "        .await\n"
    + "        .unwrap();\n"
    + "    KeryxDaemonClient::with_interceptor(channel, TestDaemonTokenInterceptor)\n"
    + "}\n",
    1,
)
common = common.replace(
    "    pub client: KeryxDaemonClient<tonic::transport::Channel>,\n",
    "    pub client: TestDaemonClient,\n",
    1,
)
common = common.replace(
    "        let runtime = Arc::new(KeryxDaemonRuntime::startup(config).await.unwrap());\n",
    "        let config = config.with_daemon_rpc_token(Some(TEST_DAEMON_TOKEN.to_string()));\n"
    "        let runtime = Arc::new(KeryxDaemonRuntime::startup(config).await.unwrap());\n",
    1,
)
old_config = (
    "        let config = KeryxDaemonConfig::new(data_dir.clone(), 42)\n"
    "            .with_fail_retry_policy(keryx_core::RetryPolicy::no_retries());\n"
)
if common.count(old_config) != 1:
    raise SystemExit("RPC harness default config shape drifted")
common = common.replace(
    old_config,
    "        let config = KeryxDaemonConfig::new(data_dir.clone(), 42)\n"
    "            .with_fail_retry_policy(keryx_core::RetryPolicy::no_retries())\n"
    "            .with_daemon_rpc_token(Some(TEST_DAEMON_TOKEN.to_string()));\n",
    1,
)
old_client = (
    "        let client = KeryxDaemonClient::connect(format!(\"http://{addr}\"))\n"
    "            .await\n"
    "            .unwrap();\n"
)
if common.count(old_client) != 2:
    raise SystemExit(f"RPC harness expected two client constructors, found {common.count(old_client)}")
common = common.replace(old_client, "        let client = authenticated_client(addr).await;\n")
write(common_path, common)

# --- Canonical relay destination metadata without changing relay/mailbox routing policy. ---
routing_path = "crates/keryx-daemon/src/routing.rs"
routing = read(routing_path)
route_import_marker = "use tracing::{info, instrument, warn};\n"
if routing.count(route_import_marker) != 1:
    raise SystemExit("routing import marker drifted")
routing = routing.replace(
    route_import_marker,
    route_import_marker
    + "\n"
    + "const RELAY_TARGET_METADATA_KEYS: &[&str] = &[\n"
    + '    "target_node_id",\n'
    + '    "target_node",\n'
    + '    "recipient_node_id",\n'
    + '    "recipient_node",\n'
    + '    "destination_node_id",\n'
    + '    "destination_node",\n'
    + '    "node_id",\n'
    + '    "keryx.target_node_id",\n'
    + "];\n"
    + 'const CANONICAL_RELAY_TARGET_METADATA_KEY: &str = "keryx.target_node_id";\n',
    1,
)
metadata_insert = (
    "        envelope.metadata.insert(\n"
    '            "target_node_id".to_string(),\n'
    "            target_peer_id.as_str().to_string(),\n"
    "        );\n"
)
if routing.count(metadata_insert) != 1:
    raise SystemExit("relay target metadata insertion shape drifted")
routing = routing.replace(
    metadata_insert,
    "        canonicalize_relay_target_metadata(&mut envelope, target_peer_id);\n",
    1,
)
validate_marker = "fn validate_relay_receipt(\n"
if routing.count(validate_marker) != 1:
    raise SystemExit("relay receipt marker drifted")
routing = routing.replace(
    validate_marker,
    "fn canonicalize_relay_target_metadata(envelope: &mut TaskEnvelope, target_peer_id: &PeerId) {\n"
    "    for key in RELAY_TARGET_METADATA_KEYS {\n"
    "        envelope.metadata.remove(*key);\n"
    "    }\n"
    "    envelope.metadata.insert(\n"
    "        CANONICAL_RELAY_TARGET_METADATA_KEY.to_string(),\n"
    "        target_peer_id.as_str().to_string(),\n"
    "    );\n"
    "}\n\n"
    + validate_marker,
    1,
)
# Add a unit regression without changing the existing relay-routable policy.
last = routing.rfind("}\n")
if last < 0 or "relay_target_metadata_is_canonicalized" in routing:
    raise SystemExit("routing test module tail unavailable")
routing_test = r'''
    #[test]
    fn relay_target_metadata_is_canonicalized() {
        let target = PeerId::new("node-canonical-target").unwrap();
        let mut envelope = TaskEnvelope::default();
        for key in RELAY_TARGET_METADATA_KEYS {
            envelope
                .metadata
                .insert((*key).to_string(), "node-poisoned".to_string());
        }
        canonicalize_relay_target_metadata(&mut envelope, &target);
        for key in RELAY_TARGET_METADATA_KEYS {
            if *key != CANONICAL_RELAY_TARGET_METADATA_KEY {
                assert!(!envelope.metadata.contains_key(*key));
            }
        }
        assert_eq!(
            envelope
                .metadata
                .get(CANONICAL_RELAY_TARGET_METADATA_KEY)
                .map(String::as_str),
            Some(target.as_str())
        );
    }
'''
routing = routing[:last] + routing_test + routing[last:]
write(routing_path, routing)

# --- Rust CLI: a single bearer credential interceptor for daemon-bound calls. ---
cli_path = "crates/keryx-cli/src/main.rs"
cli = read(cli_path)
cli = cli.replace(
    "use relay::RelayCommand;\n",
    "use relay::RelayCommand;\n"
    "use tonic::service::interceptor::InterceptedService;\n"
    "use tonic::service::Interceptor;\n"
    "use tonic::transport::Channel;\n"
    "use tonic::Request;\n",
    1,
)
cli = cli.replace(
    'const DAEMON_ENDPOINT_ENV: &str = "HERMES_KERYX_DAEMON_ENDPOINT";\n',
    'const DAEMON_ENDPOINT_ENV: &str = "HERMES_KERYX_DAEMON_ENDPOINT";\n'
    'const DAEMON_TOKEN_ENV: &str = "HERMES_KERYX_DAEMON_TOKEN";\n',
    1,
)
connect_marker = "async fn connect_daemon(\n"
if cli.count(connect_marker) != 1:
    raise SystemExit("CLI daemon connector marker drifted")
interceptor = r'''
#[derive(Clone)]
struct DaemonTokenInterceptor {
    authorization: Option<tonic::metadata::MetadataValue<tonic::metadata::Ascii>>,
}

impl DaemonTokenInterceptor {
    fn from_env() -> Result<Self> {
        let token = std::env::var(DAEMON_TOKEN_ENV)
            .ok()
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty());
        let authorization = token
            .map(|token| format!("Bearer {token}").parse())
            .transpose()
            .context("HERMES_KERYX_DAEMON_TOKEN is not valid gRPC metadata")?;
        Ok(Self { authorization })
    }
}

impl Interceptor for DaemonTokenInterceptor {
    fn call(&mut self, mut request: Request<()>) -> std::result::Result<Request<()>, tonic::Status> {
        if let Some(authorization) = &self.authorization {
            request
                .metadata_mut()
                .insert("authorization", authorization.clone());
        }
        Ok(request)
    }
}

type AuthorizedDaemonClient =
    KeryxDaemonClient<InterceptedService<Channel, DaemonTokenInterceptor>>;

'''
cli = cli.replace(connect_marker, interceptor + connect_marker, 1)
old_signature = (
    "async fn connect_daemon(\n"
    "    endpoint: &str,\n"
    "    operation: &str,\n"
    ") -> Result<KeryxDaemonClient<tonic::transport::Channel>> {\n"
)
if cli.count(old_signature) != 1:
    raise SystemExit("CLI connect return type drifted")
cli = cli.replace(
    old_signature,
    "async fn connect_daemon(\n"
    "    endpoint: &str,\n"
    "    operation: &str,\n"
    ") -> Result<AuthorizedDaemonClient> {\n",
    1,
)
old_return = (
    "    Ok(KeryxDaemonClient::new(channel)\n"
    "        .max_decoding_message_size(ARTIFACT_RPC_MAX_BYTES)\n"
    "        .max_encoding_message_size(ARTIFACT_RPC_MAX_BYTES))\n"
)
if cli.count(old_return) != 1:
    raise SystemExit("CLI daemon client construction drifted")
cli = cli.replace(
    old_return,
    "    Ok(KeryxDaemonClient::with_interceptor(\n"
    "        channel,\n"
    "        DaemonTokenInterceptor::from_env()?,\n"
    "    )\n"
    "    .max_decoding_message_size(ARTIFACT_RPC_MAX_BYTES)\n"
    "    .max_encoding_message_size(ARTIFACT_RPC_MAX_BYTES))\n",
    1,
)
write(cli_path, cli)

# --- Edge runtime: the same daemon token on internal task/result mutations. ---
node_rust_path = "crates/keryx-relay/src/node.rs"
node_rust = read(node_rust_path)
node_rust = node_rust.replace(
    "use tonic::transport::{Certificate, ClientTlsConfig, Endpoint};\n"
    "use tonic::{Code, Request};\n",
    "use tonic::service::interceptor::InterceptedService;\n"
    "use tonic::service::Interceptor;\n"
    "use tonic::transport::{Certificate, Channel, ClientTlsConfig, Endpoint};\n"
    "use tonic::{Code, Request};\n",
    1,
)
node_rust = node_rust.replace(
    'const DAEMON_ENDPOINT_ENV: &str = "HERMES_KERYX_DAEMON_ENDPOINT";\n',
    'const DAEMON_ENDPOINT_ENV: &str = "HERMES_KERYX_DAEMON_ENDPOINT";\n'
    'const DAEMON_TOKEN_ENV: &str = "HERMES_KERYX_DAEMON_TOKEN";\n',
    1,
)
insert_marker = "#[derive(Debug, Clone, Copy)]\nstruct RelayReconnectPolicy {\n"
if node_rust.count(insert_marker) != 1:
    raise SystemExit("edge node interceptor marker drifted")
edge_interceptor = r'''#[derive(Clone)]
struct DaemonTokenInterceptor {
    authorization: tonic::metadata::MetadataValue<tonic::metadata::Ascii>,
}

impl DaemonTokenInterceptor {
    fn from_env() -> Result<Self> {
        let token = std::env::var(DAEMON_TOKEN_ENV)
            .ok()
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty())
            .context("HERMES_KERYX_DAEMON_TOKEN is required for edge-to-daemon mutations")?;
        let authorization = format!("Bearer {token}")
            .parse()
            .context("HERMES_KERYX_DAEMON_TOKEN is not valid gRPC metadata")?;
        Ok(Self { authorization })
    }
}

impl Interceptor for DaemonTokenInterceptor {
    fn call(&mut self, mut request: Request<()>) -> std::result::Result<Request<()>, tonic::Status> {
        request
            .metadata_mut()
            .insert("authorization", self.authorization.clone());
        Ok(request)
    }
}

type AuthorizedDaemonClient =
    KeryxDaemonClient<InterceptedService<Channel, DaemonTokenInterceptor>>;

async fn connect_authenticated_daemon(endpoint: String) -> Result<AuthorizedDaemonClient> {
    let channel = Endpoint::from_shared(endpoint.clone())
        .with_context(|| format!("invalid daemon endpoint {endpoint}"))?
        .connect()
        .await
        .with_context(|| format!("daemon unavailable at {endpoint}"))?;
    Ok(KeryxDaemonClient::with_interceptor(
        channel,
        DaemonTokenInterceptor::from_env()?,
    )
    .max_encoding_message_size(RESULT_ARTIFACT_FRAME_MAX_BYTES)
    .max_decoding_message_size(RESULT_ARTIFACT_FRAME_MAX_BYTES))
}

'''
node_rust = node_rust.replace(insert_marker, edge_interceptor + insert_marker, 1)
old_connect_1 = (
    "    let mut daemon = KeryxDaemonClient::connect(daemon_endpoint)\n"
    "        .await?\n"
    "        .max_encoding_message_size(RESULT_ARTIFACT_FRAME_MAX_BYTES)\n"
    "        .max_decoding_message_size(RESULT_ARTIFACT_FRAME_MAX_BYTES);\n"
)
if node_rust.count(old_connect_1) != 1:
    raise SystemExit("edge result-delivery daemon connector drifted")
node_rust = node_rust.replace(
    old_connect_1,
    "    let mut daemon = connect_authenticated_daemon(daemon_endpoint).await?;\n",
    1,
)
old_connect_2 = (
    "        let mut daemon = KeryxDaemonClient::connect(daemon_endpoint.clone())\n"
    "            .await\n"
    "            .with_context(|| format!(\"keryx node stream: daemon unavailable at {daemon_endpoint}\"))?\n"
    "            .max_encoding_message_size(RESULT_ARTIFACT_FRAME_MAX_BYTES)\n"
    "            .max_decoding_message_size(RESULT_ARTIFACT_FRAME_MAX_BYTES);\n"
)
if node_rust.count(old_connect_2) != 1:
    raise SystemExit("edge incoming daemon connector drifted")
node_rust = node_rust.replace(
    old_connect_2,
    "        let mut daemon = connect_authenticated_daemon(daemon_endpoint.clone())\n"
    "            .await\n"
    "            .with_context(|| format!(\"keryx node stream: daemon unavailable at {daemon_endpoint}\"))?;\n",
    1,
)
write(node_rust_path, node_rust)

# --- Python SDK configuration and channel-wide unary auth interceptor. ---
config_path = "sdk/python/keryx/config.py"
config = read(config_path)
config = config.replace(
    "    daemon_endpoint: str = field(default_factory=default_daemon_endpoint)\n",
    "    daemon_endpoint: str = field(default_factory=default_daemon_endpoint)\n"
    "    daemon_token: str | None = None\n",
    1,
)
config = config.replace(
    "            registry_endpoint=_first_env(\n",
    "            daemon_token=_first_env(\n"
    "                source,\n"
    '                "HERMES_KERYX_DAEMON_TOKEN",\n'
    '                "KERYX_DAEMON_TOKEN",\n'
    "            ),\n"
    "            registry_endpoint=_first_env(\n",
    1,
)
config = config.replace(
    "            registry_endpoint=_optional_str(\n"
    "                data.get(\"registry_endpoint\") or registry.get(\"endpoint\")\n"
    "            ),\n",
    "            daemon_token=_optional_str(\n"
    "                data.get(\"daemon_token\") or daemon.get(\"token\")\n"
    "            ),\n"
    "            registry_endpoint=_optional_str(\n"
    "                data.get(\"registry_endpoint\") or registry.get(\"endpoint\")\n"
    "            ),\n",
    1,
)
override_marker = (
    "        if registry := _first_env(source, \"HERMES_KERYX_REGISTRY_ENDPOINT\", \"KERYX_REGISTRY_ENDPOINT\"):\n"
)
if config.count(override_marker) != 1:
    raise SystemExit("Python config override marker drifted")
config = config.replace(
    override_marker,
    "        if daemon_token := _first_env(\n"
    "            source, \"HERMES_KERYX_DAEMON_TOKEN\", \"KERYX_DAEMON_TOKEN\"\n"
    "        ):\n"
    "            changes[\"daemon_token\"] = daemon_token\n"
    + override_marker,
    1,
)
write(config_path, config)

client_path = "sdk/python/keryx/client.py"
client = read(client_path)
channel_marker = "def _registry_endpoint_target(endpoint: str) -> tuple[str, bool]:\n"
if client.count(channel_marker) != 1:
    raise SystemExit("Python client channel marker drifted")
client_auth = r'''class _DaemonAuthInterceptor(grpc.aio.UnaryUnaryClientInterceptor):
    def __init__(self, token: str) -> None:
        self._authorization = f"Bearer {token}"

    async def intercept_unary_unary(
        self,
        continuation: Any,
        client_call_details: grpc.aio.ClientCallDetails,
        request: Any,
    ) -> Any:
        metadata = list(client_call_details.metadata or ())
        metadata.append(("authorization", self._authorization))
        details = grpc.aio.ClientCallDetails(
            client_call_details.method,
            client_call_details.timeout,
            metadata,
            client_call_details.credentials,
            client_call_details.wait_for_ready,
        )
        return await continuation(details, request)


def _daemon_channel(
    endpoint: str,
    daemon_token: str | None,
    *,
    options: tuple[tuple[str, int], ...] = RESULT_ARTIFACT_GRPC_OPTIONS,
) -> grpc.aio.Channel:
    interceptors = (
        [_DaemonAuthInterceptor(daemon_token)] if daemon_token is not None else None
    )
    return grpc.aio.insecure_channel(
        _grpc_target(endpoint),
        options=options,
        interceptors=interceptors,
    )


'''
client = client.replace(channel_marker, client_auth + channel_marker, 1)
client = client.replace(
    "        daemon_endpoint: str,\n        registry_endpoint: str | None = None,\n",
    "        daemon_endpoint: str,\n        daemon_token: str | None = None,\n        registry_endpoint: str | None = None,\n",
    1,
)
client = client.replace(
    "        self._daemon_endpoint = daemon_endpoint\n",
    "        self._daemon_endpoint = daemon_endpoint\n"
    "        configured_daemon_token = daemon_token or os.environ.get(\n"
    '            "HERMES_KERYX_DAEMON_TOKEN"\n'
    "        ) or os.environ.get(\"KERYX_DAEMON_TOKEN\")\n"
    "        self._daemon_token = (\n"
    "            configured_daemon_token.strip()\n"
    "            if configured_daemon_token and configured_daemon_token.strip()\n"
    "            else None\n"
    "        )\n",
    1,
)
client = client.replace(
    "            self._channel = grpc.aio.insecure_channel(\n"
    "                _grpc_target(self._daemon_endpoint),\n"
    "                options=RESULT_ARTIFACT_GRPC_OPTIONS,\n"
    "            )\n",
    "            self._channel = _daemon_channel(\n"
    "                self._daemon_endpoint,\n"
    "                self._daemon_token,\n"
    "            )\n",
    1,
)
write(client_path, client)

node_py_path = "sdk/python/keryx/node.py"
node_py = read(node_py_path)
node_py = node_py.replace(
    "    RESULT_ARTIFACT_GRPC_OPTIONS,\n    DaemonClient,\n",
    "    DaemonClient,\n    _daemon_channel,\n",
    1,
)
node_py = node_py.replace(
    "from keryx.config import KeryxConfig, grpc_target, load_config\n",
    "from keryx.config import KeryxConfig, load_config\n",
    1,
)
node_py = node_py.replace(
    "        daemon_endpoint: str | None = None,\n        daemon_addr: str | None = None,\n",
    "        daemon_endpoint: str | None = None,\n        daemon_addr: str | None = None,\n        daemon_token: str | None = None,\n",
    1,
)
node_py = node_py.replace(
    "        if daemon_endpoint or daemon_addr or registry_endpoint or relay_endpoint or relay or worker_id:\n",
    "        if (\n"
    "            daemon_endpoint\n"
    "            or daemon_addr\n"
    "            or daemon_token\n"
    "            or registry_endpoint\n"
    "            or relay_endpoint\n"
    "            or relay\n"
    "            or worker_id\n"
    "        ):\n",
    1,
)
node_py = node_py.replace(
    "                daemon_endpoint=daemon_endpoint or daemon_addr or loaded_config.daemon_endpoint,\n",
    "                daemon_endpoint=daemon_endpoint or daemon_addr or loaded_config.daemon_endpoint,\n"
    "                daemon_token=daemon_token or loaded_config.daemon_token,\n",
    1,
)
node_py = node_py.replace(
    "        self._daemon_endpoint = loaded_config.daemon_endpoint\n",
    "        self._daemon_endpoint = loaded_config.daemon_endpoint\n"
    "        self._daemon_token = loaded_config.daemon_token\n",
    1,
)
node_py = node_py.replace(
    "            self._channel = grpc.aio.insecure_channel(\n"
    "                grpc_target(self._daemon_endpoint), options=RESULT_ARTIFACT_GRPC_OPTIONS\n"
    "            )\n",
    "            self._channel = _daemon_channel(\n"
    "                self._daemon_endpoint,\n"
    "                self._daemon_token,\n"
    "            )\n",
    1,
)
node_py = node_py.replace(
    "        client_kwargs: dict[str, Any] = dict(\n"
    "            daemon_endpoint=self._daemon_endpoint,\n"
    "            registry_endpoint=self._registry_endpoint,\n"
    "        )\n",
    "        client_kwargs: dict[str, Any] = dict(\n"
    "            daemon_endpoint=self._daemon_endpoint,\n"
    "            daemon_token=self._daemon_token,\n"
    "            registry_endpoint=self._registry_endpoint,\n"
    "        )\n",
    1,
)
old_cancel = r'''    async def cancel(
        self,
        task_id: str,
        *,
        reason: str = "",
        metadata: Mapping[str, str] | None = None,
    ) -> TaskResult:
        daemon = await self._daemon()
        response = await daemon.CancelTask(
            daemon_pb2.CancelTaskRequest(
                task_id=common_pb2.TaskId(value=task_id),
                reason=reason,
                metadata=dict(metadata or {}),
            )
        )
        return TaskResult.from_cancel(response)
'''
new_cancel = r'''    async def cancel(
        self,
        task_id: str,
        *,
        reason: str = "",
        metadata: Mapping[str, str] | None = None,
        lease_id: str | None = None,
        worker_id: str | None = None,
    ) -> TaskResult:
        daemon = await self._daemon()
        request = daemon_pb2.CancelTaskRequest(
            task_id=common_pb2.TaskId(value=task_id),
            reason=reason,
            metadata=dict(metadata or {}),
        )
        if lease_id:
            request.lease_id.value = lease_id
        if worker_id:
            request.worker_id.value = worker_id
        response = await daemon.CancelTask(request)
        return TaskResult.from_cancel(response)
'''
if node_py.count(old_cancel) != 1:
    raise SystemExit("Python cancel helper shape drifted")
node_py = node_py.replace(old_cancel, new_cancel, 1)
write(node_py_path, node_py)

# --- Existing two-node proof now exercises authenticated daemon clients end to end. ---
e2e_path = "scripts/e2e_two_node.py"
e2e = read(e2e_path)
e2e = e2e.replace(
    'RECEIVER_TOKEN = "receiver-token-phase17"\n',
    'RECEIVER_TOKEN = "receiver-token-phase17"\n'
    'DAEMON_TOKEN = "daemon-token-cross-node-e2e"\n',
    1,
)
e2e = e2e.replace(
    '            "RUST_LOG": env.get("RUST_LOG", "info"),\n',
    '            "RUST_LOG": env.get("RUST_LOG", "info"),\n'
    '            "HERMES_KERYX_DAEMON_TOKEN": DAEMON_TOKEN,\n',
    1,
)
write(e2e_path, e2e)

# --- Operator dual-run refuses to start an unauthenticated daemon. ---
dual_path = "scripts/keryx-dual-run.sh"
dual = read(dual_path)
dual = dual.replace(
    'DAEMON_ENDPOINT=${HERMES_KERYX_DAEMON_ENDPOINT:-"http://${DAEMON_ADDR}"}\n',
    'DAEMON_ENDPOINT=${HERMES_KERYX_DAEMON_ENDPOINT:-"http://${DAEMON_ADDR}"}\n'
    'DAEMON_TOKEN=${HERMES_KERYX_DAEMON_TOKEN:-}\n',
    1,
)
dual = dual.replace(
    "  HERMES_KERYX_DAEMON_ENDPOINT         default: ${DAEMON_ENDPOINT}\n",
    "  HERMES_KERYX_DAEMON_ENDPOINT         default: ${DAEMON_ENDPOINT}\n"
    "  HERMES_KERYX_DAEMON_TOKEN            required for --start\n",
    1,
)
start_marker = "start_all() {\n  ensure_dirs\n"
if dual.count(start_marker) != 1:
    raise SystemExit("dual-run start marker drifted")
dual = dual.replace(
    start_marker,
    "start_all() {\n"
    "  if [[ -z \"$DAEMON_TOKEN\" ]]; then\n"
    "    log \"HERMES_KERYX_DAEMON_TOKEN is required to start the daemon RPC listener\"\n"
    "    return 2\n"
    "  fi\n"
    "  export HERMES_KERYX_DAEMON_TOKEN=\"$DAEMON_TOKEN\"\n"
    "  ensure_dirs\n",
    1,
)
write(dual_path, dual)

# --- Regression tests for one auth gate and canonical token config. ---
(ROOT / "crates/keryx-daemon/tests/daemon_auth.rs").write_text(
r'''mod common;

use keryx_daemon::{serve_daemon_rpc, KeryxDaemonConfig, KeryxDaemonRuntime};
use keryx_proto::v1::keryx_daemon_client::KeryxDaemonClient;
use keryx_proto::v1::{
    GetTaskResultRequest, StatusRequest, SubmitTaskRequest, TaskEnvelope, TaskId,
};
use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::{Code, Request};

const TOKEN: &str = "unified-daemon-auth-test-token";

fn envelope(task_id: &str) -> TaskEnvelope {
    TaskEnvelope {
        task_id: Some(TaskId {
            value: task_id.to_string(),
        }),
        ..TaskEnvelope::default()
    }
}

#[tokio::test]
async fn network_daemon_allows_public_reads_but_denies_sensitive_calls_without_bearer() {
    let dir = tempfile::tempdir().unwrap();
    let config = KeryxDaemonConfig::new(dir.path(), 0)
        .with_daemon_rpc_token(Some(TOKEN.to_string()));
    let runtime = KeryxDaemonRuntime::startup(config).await.unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(serve_daemon_rpc(runtime, TcpListenerStream::new(listener)));

    let mut raw = KeryxDaemonClient::connect(format!("http://{addr}"))
        .await
        .unwrap();
    raw.status(StatusRequest {}).await.unwrap();

    let missing = raw
        .submit_task(SubmitTaskRequest {
            envelope: Some(envelope("auth-missing")),
        })
        .await
        .unwrap_err();
    assert_eq!(missing.code(), Code::Unauthenticated);

    let result_read = raw
        .get_task_result(GetTaskResultRequest {
            task_id: Some(TaskId {
                value: "auth-missing".to_string(),
            }),
        })
        .await
        .unwrap_err();
    assert_eq!(result_read.code(), Code::Unauthenticated);

    let mut wrong = Request::new(SubmitTaskRequest {
        envelope: Some(envelope("auth-wrong")),
    });
    wrong
        .metadata_mut()
        .insert("authorization", "Bearer wrong-token".parse().unwrap());
    let wrong = raw.submit_task(wrong).await.unwrap_err();
    assert_eq!(wrong.code(), Code::PermissionDenied);

    let mut authorized = Request::new(SubmitTaskRequest {
        envelope: Some(envelope("auth-ok")),
    });
    authorized.metadata_mut().insert(
        "authorization",
        format!("Bearer {TOKEN}").parse().unwrap(),
    );
    raw.submit_task(authorized).await.unwrap();

    server.abort();
}
''',
    encoding="utf-8",
)

(ROOT / "sdk/python/tests/test_daemon_auth_config.py").write_text(
r'''from keryx.config import KeryxConfig


def test_daemon_token_loads_from_prefixed_environment() -> None:
    config = KeryxConfig.from_env(
        {
            "HERMES_KERYX_DAEMON_ENDPOINT": "127.0.0.1:50051",
            "HERMES_KERYX_DAEMON_TOKEN": "  unified-token  ",
        }
    )
    assert config.daemon_token == "unified-token"


def test_daemon_token_alias_is_supported() -> None:
    config = KeryxConfig.from_env({"KERYX_DAEMON_TOKEN": "alias-token"})
    assert config.daemon_token == "alias-token"
''',
    encoding="utf-8",
)

# Existing running-cancel regression must supply the active lease proof after the protocol grows it.
task_cancel_path = "crates/keryx-daemon/tests/task_cancel.rs"
task_cancel = read(task_cancel_path)
running_claim = r'''    harness
        .client
        .claim_task(ClaimTaskRequest {
            task_id: Some(TaskId {
                value: "cancel-running".to_string(),
            }),
            worker_id: Some(AgentId {
                value: "cancel-worker".to_string(),
            }),
            lease_duration_ms: 60_000,
        })
        .await
        .unwrap();
'''
if task_cancel.count(running_claim) != 1:
    raise SystemExit("running cancel claim fixture drifted")
task_cancel = task_cancel.replace(
    running_claim,
    running_claim.replace("    harness\n", "    let running_claim = harness\n").replace(
        "        .unwrap();\n", "        .unwrap()\n        .into_inner();\n"
    ),
    1,
)
running_cancel = r'''        .cancel_task(CancelTaskRequest {
            task_id: Some(TaskId {
                value: "cancel-running".to_string(),
            }),
            reason: "operator request".to_string(),
            metadata: Default::default(),
        })
'''
if task_cancel.count(running_cancel) != 1:
    raise SystemExit("running cancel request fixture drifted")
task_cancel = task_cancel.replace(
    running_cancel,
    r'''        .cancel_task(CancelTaskRequest {
            task_id: Some(TaskId {
                value: "cancel-running".to_string(),
            }),
            reason: "operator request".to_string(),
            metadata: Default::default(),
            lease_id: running_claim.lease_id.clone(),
            worker_id: Some(AgentId {
                value: "cancel-worker".to_string(),
            }),
        })
''',
    1,
)
write(task_cancel_path, task_cancel)

# Add default None fields to all remaining Rust CancelTaskRequest literals so the new protocol
# stays source-compatible; the focused running test above deliberately supplies real proof.
def fill_cancel_defaults(path: Path) -> None:
    text = path.read_text(encoding="utf-8")
    cursor = 0
    changed = False
    while True:
        start = text.find("CancelTaskRequest {", cursor)
        if start < 0:
            break
        brace = text.find("{", start)
        depth = 0
        end = None
        for index in range(brace, len(text)):
            if text[index] == "{":
                depth += 1
            elif text[index] == "}":
                depth -= 1
                if depth == 0:
                    end = index
                    break
        if end is None:
            raise SystemExit(f"unbalanced CancelTaskRequest in {path}")
        block = text[start : end + 1]
        if "lease_id:" not in block:
            line_start = text.rfind("\n", 0, end) + 1
            closing_indent = text[line_start:end]
            field_indent = closing_indent + "    "
            insertion = f"{field_indent}lease_id: None,\n{field_indent}worker_id: None,\n"
            text = text[:line_start] + insertion + text[line_start:]
            end += len(insertion)
            changed = True
        cursor = end + 1
    if changed:
        path.write_text(text, encoding="utf-8")

for path in (ROOT / "crates").rglob("*.rs"):
    fill_cancel_defaults(path)

# Preserve the real-proof fields in the running cancellation test if the generic filler touched it.
task_cancel = read(task_cancel_path)
task_cancel = task_cancel.replace(
    "            lease_id: running_claim.lease_id.clone(),\n"
    "            worker_id: Some(AgentId {\n"
    "                value: \"cancel-worker\".to_string(),\n"
    "            }),\n"
    "            lease_id: None,\n"
    "            worker_id: None,\n",
    "            lease_id: running_claim.lease_id.clone(),\n"
    "            worker_id: Some(AgentId {\n"
    "                value: \"cancel-worker\".to_string(),\n"
    "            }),\n",
)
write(task_cancel_path, task_cancel)

# Durable operator docs. Keep current product truth and add only the new cross-cutting contract.
for path in ["README.md", "docs/operations.md", "docs/current-product.md", "sdk/python/README.md"]:
    text = read(path)
    if "HERMES_KERYX_DAEMON_TOKEN" in text:
        continue
    appendix = (
        "\n\n### Local daemon RPC authorization\n\n"
        "When `keryxd` listens on `HERMES_KERYX_DAEMON_ADDR`, "
        "`HERMES_KERYX_DAEMON_TOKEN` is required. Sensitive reads, task/result dequeue, "
        "artifact access, task lifecycle mutation, remote ingress, and `SendTask` use the same "
        "`Authorization: Bearer ...` credential. Status, doctor, liveness, readiness, peer listing, "
        "and skill discovery remain read-only public-local diagnostics. Running-task cancellation "
        "also requires the exact active lease id and worker id; the daemon token alone does not "
        "grant lease ownership.\n"
    )
    write(path, text.rstrip() + appendix + "\n")

print("unified daemon authorization reconciliation staged")
