#!/usr/bin/env python3
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PATH = ROOT / "scripts/e2e_two_node.py"
text = PATH.read_text(encoding="utf-8")

if 'DAEMON_TOKEN = "daemon-token-cross-node-e2e"' not in text:
    raise SystemExit("post-transform E2E daemon token constant is missing")
if '"HERMES_KERYX_DAEMON_TOKEN": DAEMON_TOKEN,' not in text:
    raise SystemExit("post-transform subprocess daemon token wiring is missing")

worker_old = '''    node = KeryxNode(
        card,
        daemon_endpoint=daemon_endpoint,
        worker_id="phase17-worker",
'''
worker_new = '''    node = KeryxNode(
        card,
        daemon_endpoint=daemon_endpoint,
        daemon_token=DAEMON_TOKEN,
        worker_id="phase17-worker",
'''
if text.count(worker_old) != 1:
    raise SystemExit(f"expected one worker KeryxNode constructor, found {text.count(worker_old)}")
text = text.replace(worker_old, worker_new, 1)

sender_old = '''    node = KeryxNode(
        daemon_endpoint=f"127.0.0.1:{sender_port}",
        registry_endpoint=f"127.0.0.1:{registry_port}",
        worker_id="phase17-sender",
'''
sender_new = '''    node = KeryxNode(
        daemon_endpoint=f"127.0.0.1:{sender_port}",
        daemon_token=DAEMON_TOKEN,
        registry_endpoint=f"127.0.0.1:{registry_port}",
        worker_id="phase17-sender",
'''
if text.count(sender_old) != 1:
    raise SystemExit(f"expected one sender KeryxNode constructor, found {text.count(sender_old)}")
text = text.replace(sender_old, sender_new, 1)

PATH.write_text(text, encoding="utf-8")
print("real two-node Python clients explicitly authenticate to daemon RPC")
