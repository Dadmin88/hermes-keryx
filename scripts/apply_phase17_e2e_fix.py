#!/usr/bin/env python3
"""Make the two-node harness create deterministic test keypair seeds."""
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
path = ROOT / "scripts/e2e_two_node.py"
text = path.read_text()
text = text.replace(
    '    env = base_env()\n    env.update(\n        {\n            "HERMES_KERYX_NODE_PEER_ID": peer_id,',
    '    key_path.parent.mkdir(parents=True, exist_ok=True)\n'
    '    seed = 1 if peer_id == SENDER_PEER else 2\n'
    '    key_path.write_bytes(bytes([seed]) + bytes(31))\n'
    '    env = base_env()\n'
    '    env.update(\n'
    '        {\n'
    '            "HERMES_KERYX_NODE_PEER_ID": peer_id,',
)
text = text.replace(
    '                "keypair_path": str(work_dir / "relay.key"),',
    '                "keypair_path": None,',
)
path.write_text(text)
