#!/usr/bin/env python3
from pathlib import Path

path = Path("crates/keryx-daemon/tests/health_probes.rs")
text = path.read_text(encoding="utf-8")

# These tests deliberately prove Liveness and Readiness remain public. Only the
# network listener needs a configured credential; the raw clients remain
# unauthenticated.
old_liveness = '''    let runtime = KeryxDaemonRuntime::startup(KeryxDaemonConfig::new(data_dir, 42))
        .await
        .unwrap();
'''
new_liveness = '''    let runtime = KeryxDaemonRuntime::startup(
        KeryxDaemonConfig::new(data_dir, 42)
            .with_daemon_rpc_token(Some("keryx-health-test-daemon-token".to_string())),
    )
    .await
    .unwrap();
'''
if text.count(old_liveness) != 1:
    raise SystemExit(f"expected one liveness runtime fixture, found {text.count(old_liveness)}")
text = text.replace(old_liveness, new_liveness, 1)

old_readiness = '''        KeryxDaemonRuntime::startup(KeryxDaemonConfig::new(data_dir.clone(), 1))
            .await
            .unwrap(),
'''
new_readiness = '''        KeryxDaemonRuntime::startup(
            KeryxDaemonConfig::new(data_dir.clone(), 1)
                .with_daemon_rpc_token(Some("keryx-health-test-daemon-token".to_string())),
        )
        .await
        .unwrap(),
'''
if text.count(old_readiness) != 1:
    raise SystemExit(f"expected one readiness runtime fixture, found {text.count(old_readiness)}")
text = text.replace(old_readiness, new_readiness, 1)

path.write_text(text, encoding="utf-8")

# Status and Doctor are also deliberately public-local diagnostics. Configure
# the server credential while preserving the unauthenticated raw client.
status_path = Path("crates/keryx-daemon/tests/rpc_status.rs")
status = status_path.read_text(encoding="utf-8")
old_status_runtime = '''    let runtime = KeryxDaemonRuntime::startup(KeryxDaemonConfig::new(data_dir.clone(), 42))
        .await
        .unwrap();
'''
new_status_runtime = '''    let runtime = KeryxDaemonRuntime::startup(
        KeryxDaemonConfig::new(data_dir.clone(), 42)
            .with_daemon_rpc_token(Some("keryx-status-test-daemon-token".to_string())),
    )
    .await
    .unwrap();
'''
if status.count(old_status_runtime) != 1:
    raise SystemExit(
        f"expected one status/doctor runtime fixture, found {status.count(old_status_runtime)}"
    )
status_path.write_text(status.replace(old_status_runtime, new_status_runtime, 1), encoding="utf-8")

print("public health/status daemon listeners configured; raw clients remain unauthenticated")
