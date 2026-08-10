#!/usr/bin/env python3
from pathlib import Path

# Anchor the generated routing regression inside the existing unit-test module.
path = Path("crates/keryx-daemon/src/routing.rs")
text = path.read_text(encoding="utf-8")

test = r'''
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

if text.count(test) != 1:
    raise SystemExit(f"expected exactly one generated routing regression, found {text.count(test)}")
text = text.replace(test, "", 1)
marker = "mod tests {\n    use super::*;\n    use keryx_proto::v1::TaskId as ProtoTaskId;\n"
if text.count(marker) != 1:
    raise SystemExit("routing test-module anchor drifted")
text = text.replace(marker, marker + test, 1)
path.write_text(text, encoding="utf-8")

# The public test harness exposes its client at pub(crate), so its interceptor
# must have matching visibility under -D warnings/private_interfaces.
common_path = Path("crates/keryx-daemon/tests/common/mod.rs")
common = common_path.read_text(encoding="utf-8")
old = "struct TestDaemonTokenInterceptor;"
new = "pub(crate) struct TestDaemonTokenInterceptor;"
if common.count(old) != 1:
    raise SystemExit(f"expected one test interceptor declaration, found {common.count(old)}")
common_path.write_text(common.replace(old, new, 1), encoding="utf-8")

# These cancellation fixtures represent PENDING remote-origin tasks, not
# RUNNING leased tasks, so no lease ownership proof is required. Keep that
# explicit while adapting to the six-argument result-preserving cancel API.
store_test_path = Path("crates/keryx-store/tests/result_artifact_ingest.rs")
store_test = store_test_path.read_text(encoding="utf-8")
old_call = '.cancel_task_with_result(&task_id, "owner canceled", 15, canceled.clone())'
new_call = '.cancel_task_with_result(&task_id, None, None, "owner canceled", 15, canceled.clone())'
if store_test.count(old_call) != 1:
    raise SystemExit(f"expected one pending store cancellation fixture, found {store_test.count(old_call)}")
store_test_path.write_text(store_test.replace(old_call, new_call, 1), encoding="utf-8")

transport_path = Path("crates/keryx-daemon/tests/result_artifact_transport.rs")
transport = transport_path.read_text(encoding="utf-8")
old_transport = "        .cancel_task_with_result(\n            &core_id,\n            \"owner canceled\",\n"
new_transport = "        .cancel_task_with_result(\n            &core_id,\n            None,\n            None,\n            \"owner canceled\",\n"
if transport.count(old_transport) != 2:
    raise SystemExit(
        f"expected two pending daemon transport cancellation fixtures, found {transport.count(old_transport)}"
    )
transport_path.write_text(transport.replace(old_transport, new_transport), encoding="utf-8")

# Direct network-daemon CLI fixtures must configure the listener credential.
# Status/doctor/node-status remain deliberately public RPCs, so those CLI calls
# do not need the bearer token. Mutation/artifact CLI fixtures do.
TEST_TOKEN = "keryx-cli-test-daemon-token"

relay_cli_path = Path("crates/keryx-cli/tests/cli_relay_node.rs")
relay_cli = relay_cli_path.read_text(encoding="utf-8")
old_node_runtime = '''    let runtime = KeryxDaemonRuntime::startup(KeryxDaemonConfig::new(
        dir.path().join("node-status-home"),
        123,
    ))
    .await
    .unwrap();
'''
new_node_runtime = f'''    let runtime = KeryxDaemonRuntime::startup(
        KeryxDaemonConfig::new(dir.path().join("node-status-home"), 123)
            .with_daemon_rpc_token(Some("{TEST_TOKEN}".to_string())),
    )
    .await
    .unwrap();
'''
if relay_cli.count(old_node_runtime) != 1:
    raise SystemExit(f"expected one node-status direct daemon fixture, found {relay_cli.count(old_node_runtime)}")
relay_cli_path.write_text(relay_cli.replace(old_node_runtime, new_node_runtime, 1), encoding="utf-8")

client_path = Path("crates/keryx-cli/tests/daemon_client.rs")
client = client_path.read_text(encoding="utf-8")

# All three direct listeners in this file need an actual configured credential.
runtime_patterns = [
    ("cli-rpc-keryx-home", "123"),
    ("cli-doctor-rpc-keryx-home", "123"),
    ("cli-artifact-rpc-keryx-home", "123"),
]
for data_dir, now in runtime_patterns:
    old_runtime = f'''    let runtime = KeryxDaemonRuntime::startup(KeryxDaemonConfig::new(
        dir.path().join("{data_dir}"),
        {now},
    ))
    .await
    .unwrap();
'''
    new_runtime = f'''    let runtime = KeryxDaemonRuntime::startup(
        KeryxDaemonConfig::new(dir.path().join("{data_dir}"), {now})
            .with_daemon_rpc_token(Some("{TEST_TOKEN}".to_string())),
    )
    .await
    .unwrap();
'''
    if client.count(old_runtime) != 1:
        raise SystemExit(f"expected one direct daemon fixture for {data_dir}, found {client.count(old_runtime)}")
    client = client.replace(old_runtime, new_runtime, 1)

# The generic argument helper is used only by sensitive task/artifact commands
# in this file, so attach the same daemon token there. The simple status/doctor
# helper intentionally remains token-free and proves those RPCs stay public.
old_args_helper = '''fn run_keryx_args(args: &[&str], endpoint: String) -> std::process::Output {
    std::process::Command::new(env!("CARGO_BIN_EXE_keryx"))
        .args(args)
        .env("HERMES_KERYX_DAEMON_ENDPOINT", endpoint)
        .output()
        .unwrap()
}
'''
new_args_helper = f'''fn run_keryx_args(args: &[&str], endpoint: String) -> std::process::Output {{
    std::process::Command::new(env!("CARGO_BIN_EXE_keryx"))
        .args(args)
        .env("HERMES_KERYX_DAEMON_ENDPOINT", endpoint)
        .env("HERMES_KERYX_DAEMON_TOKEN", "{TEST_TOKEN}")
        .output()
        .unwrap()
}}
'''
if client.count(old_args_helper) != 1:
    raise SystemExit(f"expected one daemon-client args helper, found {client.count(old_args_helper)}")
client = client.replace(old_args_helper, new_args_helper, 1)
client_path.write_text(client, encoding="utf-8")

# Discovery integration starts a real daemon listener but calls only the
# deliberately public DiscoverSkills RPC. Configure the server credential while
# keeping the raw client unauthenticated to preserve that contract in the test.
discovery_path = Path("crates/keryx-daemon/tests/discovery_integration.rs")
discovery = discovery_path.read_text(encoding="utf-8")
old_discovery_config = '''    KeryxDaemonConfig::new(data_dir, 1)
        .with_local_peer_id(PeerId::new(peer_id).unwrap())
        .with_discovery(Some(settings))
'''
new_discovery_config = '''    KeryxDaemonConfig::new(data_dir, 1)
        .with_local_peer_id(PeerId::new(peer_id).unwrap())
        .with_discovery(Some(settings))
        .with_daemon_rpc_token(Some("keryx-discovery-test-daemon-token".to_string()))
'''
if discovery.count(old_discovery_config) != 1:
    raise SystemExit(
        f"expected one discovery daemon config builder, found {discovery.count(old_discovery_config)}"
    )
discovery_path.write_text(
    discovery.replace(old_discovery_config, new_discovery_config, 1),
    encoding="utf-8",
)

print("generated unified-auth integration fixtures repaired")
