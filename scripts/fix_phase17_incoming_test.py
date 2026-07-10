#!/usr/bin/env python3
"""Update the incoming relay-frame test for the Phase 17 frame schema."""
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
path = ROOT / "crates/keryx-daemon/src/incoming.rs"
text = path.read_text()
old = '''            RelayFrame {
                frame_id: "frame-1".to_string(),
                task: Some(envelope("task-1")),
            },'''
new = '''            RelayFrame {
                frame_id: "frame-1".to_string(),
                task: Some(envelope("task-1")),
                result: None,
                authenticated_source_node_id: "node-remote".to_string(),
                destination_node_id: "node-local".to_string(),
            },'''
if new not in text:
    if old not in text:
        raise RuntimeError("incoming relay-frame test anchor not found")
    path.write_text(text.replace(old, new, 1))
