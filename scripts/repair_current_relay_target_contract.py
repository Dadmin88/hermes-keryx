#!/usr/bin/env python3
from pathlib import Path

path = Path("crates/keryx-daemon/src/routing.rs")
text = path.read_text(encoding="utf-8")
old = 'const CANONICAL_RELAY_TARGET_METADATA_KEY: &str = "keryx.target_node_id";'
new = 'const CANONICAL_RELAY_TARGET_METADATA_KEY: &str = "target_node_id";'
if text.count(old) != 1:
    raise SystemExit(f"expected one generated canonical relay target constant, found {text.count(old)}")
path.write_text(text.replace(old, new, 1), encoding="utf-8")
print("preserved current-main target_node_id wire key while stripping all aliases")
