#!/usr/bin/env python3
"""Update relay publisher tests with deterministic authenticated source peers."""
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
path = ROOT / "crates/keryx-daemon/tests/task_routing.rs"
text = path.read_text()
old = 'GrpcRelayTaskPublisher::new(format!("http://{addr}"))'
new = '''GrpcRelayTaskPublisher::new(
        format!("http://{addr}"),
        PeerId::new("node-grpc-source").unwrap(),
    )'''
count = text.count(old)
if count not in {0, 2}:
    raise RuntimeError(f"expected zero or two relay publisher test anchors, found {count}")
if count:
    text = text.replace(old, new)
path.write_text(text)
