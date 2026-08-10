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

print("generated unified-auth integration fixtures repaired")
