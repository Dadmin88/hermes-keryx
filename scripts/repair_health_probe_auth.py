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
print("health-probe daemon listeners configured; public clients remain unauthenticated")
