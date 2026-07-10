#!/usr/bin/env python3
"""Repair the relay-auth builder's embedded TOML source block."""
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
path = ROOT / "scripts/apply_phase17_relay_auth.py"
text = path.read_text()
start = text.index("toml_block = '''")
end = text.index("if json_block_start in text:", start)
replacement = """toml_block = \"\"\"    relay_config = work_dir / \"relay.toml\"
    relay_config.write_text(
        f'''[relay]
listen_addresses = [\"tcp:0\"]
bootstrap_peers = []
enable_mdns = false
max_circuits = 16
max_reservations = 16
max_reservations_per_peer = 4
connection_timeout_ms = 5000
use_ipv6 = false
health_grpc_bind = \"127.0.0.1:{relay_port}\"
health_http_bind = \"\"
registry_grpc_bind = \"127.0.0.1:{registry_port}\"

[[security.node_tokens]]
node_id = \"{SENDER_PEER}\"
token = \"{SENDER_TOKEN}\"

[[security.node_tokens]]
node_id = \"{RECEIVER_PEER}\"
token = \"{RECEIVER_TOKEN}\"
''',
        encoding=\"utf-8\",
    )\"\"\"
"""
path.write_text(text[:start] + replacement + text[end:])
