#!/usr/bin/env python3
"""Harden deterministic identities and Python endpoints in the two-node harness."""
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
text = text.replace(
    '        daemon_endpoint=f"http://127.0.0.1:{sender_port}",',
    '        daemon_endpoint=f"127.0.0.1:{sender_port}",',
)
text = text.replace(
    '        registry_endpoint=f"http://127.0.0.1:{registry_port}",',
    '        registry_endpoint=f"127.0.0.1:{registry_port}",',
)
text = text.replace(
    '                f"http://127.0.0.1:{receiver_port}",',
    '                f"127.0.0.1:{receiver_port}",',
)
path.write_text(text)
