#!/usr/bin/env python3
from __future__ import annotations

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
NODE = ROOT / "crates/keryx-relay/src/node.rs"
TESTS = ROOT / "crates/keryx-relay/src/node/tests.rs"

TEST_TOKEN_CONST = 'const TEST_DAEMON_RPC_TOKEN: &str = "keryx-relay-node-test-daemon-token";'


def function_block(text: str, name: str) -> tuple[int, int, str]:
    marker = f"async fn {name}()"
    start = text.find(marker)
    if start < 0:
        raise SystemExit(f"missing test function {name}")
    brace = text.find("{", start)
    if brace < 0:
        raise SystemExit(f"missing opening brace for {name}")
    depth = 0
    for index in range(brace, len(text)):
        ch = text[index]
        if ch == "{":
            depth += 1
        elif ch == "}":
            depth -= 1
            if depth == 0:
                return start, index + 1, text[start : index + 1]
    raise SystemExit(f"unterminated function {name}")


node = NODE.read_text(encoding="utf-8")
env_const = 'const DAEMON_TOKEN_ENV: &str = "HERMES_KERYX_DAEMON_TOKEN";\n'
if node.count(env_const) != 1:
    raise SystemExit(f"expected one transformed daemon token env constant, found {node.count(env_const)}")
node = node.replace(
    env_const,
    env_const + f"#[cfg(test)]\n{TEST_TOKEN_CONST}\n",
    1,
)

old_token = '''        let token = std::env::var(DAEMON_TOKEN_ENV)
            .ok()
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty())
            .context("HERMES_KERYX_DAEMON_TOKEN is required for edge-to-daemon mutations")?;
'''
new_token = '''        let token = std::env::var(DAEMON_TOKEN_ENV)
            .ok()
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty());
        #[cfg(test)]
        let token = token.or_else(|| Some(TEST_DAEMON_RPC_TOKEN.to_string()));
        let token = token
            .context("HERMES_KERYX_DAEMON_TOKEN is required for edge-to-daemon mutations")?;
'''
if node.count(old_token) != 1:
    raise SystemExit(f"expected transformed daemon token loader once, found {node.count(old_token)}")
node = node.replace(old_token, new_token, 1)
NODE.write_text(node, encoding="utf-8")


tests = TESTS.read_text(encoding="utf-8")
function_names = [
    "exhausted_transient_result_delivery_retries_dead_letter_without_losing_artifacts",
    "relay_restart_reconnects_reapplies_auth_and_processes_later_task",
    "late_result_is_settled_and_next_result_continues_on_same_stream",
    "result_outbox_survives_relay_drop_then_reconnects_delivers_and_processes_next_task",
    "temporary_daemon_failure_does_not_ack_and_retries_after_recovery",
]

configured = [0]
raw_clients = 0
for name in function_names:
    start, end, block = function_block(tests, name)

    def add_token(match: re.Match[str]) -> str:
        configured[0] += 1
        indent = match.group("indent")
        expr = match.group("expr")
        return (
            f"{indent}{expr}\n"
            f"{indent}.with_daemon_rpc_token(Some(TEST_DAEMON_RPC_TOKEN.to_string())),"
        )

    block = re.sub(
        r'(?m)^(?P<indent>\s*)(?P<expr>\.with_local_peer_id\(PeerId::new\([A-Z_]+\)\.unwrap\(\)\)),$',
        add_token,
        block,
    )
    replacements = block.count("KeryxDaemonClient::connect(")
    raw_clients += replacements
    block = block.replace("KeryxDaemonClient::connect(", "connect_authenticated_daemon(")
    tests = tests[:start] + block + tests[end:]

if configured[0] != 6:
    raise SystemExit(f"expected six daemon runtime token insertions, found {configured[0]}")
if raw_clients != 2:
    raise SystemExit(f"expected two raw daemon client replacements, found {raw_clients}")
if "KeryxDaemonClient::connect(" in "\n".join(function_block(tests, name)[2] for name in function_names):
    raise SystemExit("raw daemon mutation client remains in repaired relay-node fixtures")

TESTS.write_text(tests, encoding="utf-8")
print("relay-node daemon listeners and mutation clients authenticated with cfg(test) credential")
