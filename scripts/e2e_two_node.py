#!/usr/bin/env python3
"""Prove the durable Keryx task round trip across real processes.

Topology:
    relay + registry
      ├── sender daemon + sender edge
      └── receiver daemon + receiver edge + Python worker

The script intentionally uses isolated SQLite stores, dynamic loopback ports,
and no maintainer-local configuration. On failure it preserves the work dir and
prints process log tails. On success it removes the work dir unless --keep is set.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import time
from dataclasses import dataclass
from pathlib import Path
from typing import IO

ROOT = Path(__file__).resolve().parents[1]
SDK_ROOT = ROOT / "sdk" / "python"
if str(SDK_ROOT) not in sys.path:
    sys.path.insert(0, str(SDK_ROOT))

from keryx.card import AgentCard, Skill  # noqa: E402
from keryx.node import KeryxNode  # noqa: E402
from keryx.task import Artifact, Message, Part, TaskStatus  # noqa: E402

SKILL_ID = "e2e.echo"
SENDER_PEER = "sender-peer"
RECEIVER_PEER = "receiver-peer"
EXPECTED_ARTIFACT_BYTES = b"\x00\xffkeryx-cross-node-artifact\n" + bytes(range(256))
EXPECTED_ARTIFACT_NAME = "../../phase17-result.bin"
SENDER_TOKEN = "sender-token-phase17"
RECEIVER_TOKEN = "receiver-token-phase17"


@dataclass
class ManagedProcess:
    name: str
    process: subprocess.Popen[str]
    log_path: Path
    log_handle: IO[str]


class ProcessGroup:
    def __init__(self, work_dir: Path) -> None:
        self.work_dir = work_dir
        self.processes: list[ManagedProcess] = []

    def start(self, name: str, command: list[str], env: dict[str, str]) -> ManagedProcess:
        log_path = self.work_dir / "logs" / f"{name}.log"
        log_path.parent.mkdir(parents=True, exist_ok=True)
        handle = log_path.open("w", encoding="utf-8")
        process = subprocess.Popen(
            command,
            cwd=ROOT,
            env=env,
            stdout=handle,
            stderr=subprocess.STDOUT,
            text=True,
            start_new_session=True,
        )
        managed = ManagedProcess(name, process, log_path, handle)
        self.processes.append(managed)
        return managed

    def assert_alive(self) -> None:
        failed = [item for item in self.processes if item.process.poll() is not None]
        if failed:
            details = ", ".join(
                f"{item.name}=exit:{item.process.returncode}" for item in failed
            )
            raise RuntimeError(f"child process exited unexpectedly: {details}")

    def stop_all(self) -> None:
        for item in reversed(self.processes):
            if item.process.poll() is None:
                try:
                    os.killpg(item.process.pid, signal.SIGINT)
                except ProcessLookupError:
                    pass
        deadline = time.monotonic() + 5
        for item in reversed(self.processes):
            if item.process.poll() is None:
                timeout = max(0.1, deadline - time.monotonic())
                try:
                    item.process.wait(timeout=timeout)
                except subprocess.TimeoutExpired:
                    try:
                        os.killpg(item.process.pid, signal.SIGKILL)
                    except ProcessLookupError:
                        pass
                    item.process.wait(timeout=2)
            item.log_handle.close()

    def print_tails(self, lines: int = 80) -> None:
        for item in self.processes:
            item.log_handle.flush()
            print(f"\n===== {item.name}: {item.log_path} =====", file=sys.stderr)
            try:
                content = item.log_path.read_text(encoding="utf-8", errors="replace")
            except OSError as error:
                print(f"could not read log: {error}", file=sys.stderr)
                continue
            print("\n".join(content.splitlines()[-lines:]), file=sys.stderr)


def free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
        probe.bind(("127.0.0.1", 0))
        return int(probe.getsockname()[1])


def wait_tcp(port: int, group: ProcessGroup, timeout: float = 20.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        group.assert_alive()
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.25):
                return
        except OSError:
            time.sleep(0.05)
    raise TimeoutError(f"TCP endpoint 127.0.0.1:{port} did not become ready")


def base_env() -> dict[str, str]:
    env = dict(os.environ)
    python_path = str(SDK_ROOT)
    if env.get("PYTHONPATH"):
        python_path = f"{python_path}{os.pathsep}{env['PYTHONPATH']}"
    env.update(
        {
            "PYTHONPATH": python_path,
            "PYTHONUNBUFFERED": "1",
            "RUST_LOG": env.get("RUST_LOG", "info"),
        }
    )
    return env


def daemon_env(
    *,
    peer_id: str,
    data_dir: Path,
    daemon_port: int,
    relay_port: int,
    registry_port: int,
) -> dict[str, str]:
    env = base_env()
    env.update(
        {
            "HERMES_KERYX_DATA_DIR": str(data_dir),
            "HERMES_KERYX_DAEMON_ADDR": f"127.0.0.1:{daemon_port}",
            "HERMES_KERYX_DAEMON_PEER_ID": peer_id,
            "HERMES_KERYX_RELAY_ENDPOINT": f"http://127.0.0.1:{relay_port}",
            "HERMES_KERYX_RELAY_HEALTH_ENDPOINT": f"http://127.0.0.1:{relay_port}",
            "HERMES_KERYX_RELAY_REGISTRY_ENDPOINT": f"http://127.0.0.1:{registry_port}",
            "HERMES_KERYX_NODE_TOKEN": SENDER_TOKEN if peer_id == SENDER_PEER else RECEIVER_TOKEN,
        }
    )
    return env


def edge_env(
    *,
    peer_id: str,
    daemon_port: int,
    relay_port: int,
    registry_port: int,
    key_path: Path,
    skills: str = "",
) -> dict[str, str]:
    key_path.parent.mkdir(parents=True, exist_ok=True)
    seed = 1 if peer_id == SENDER_PEER else 2
    key_path.write_bytes(bytes([seed]) + bytes(31))
    env = base_env()
    env.update(
        {
            "HERMES_KERYX_NODE_PEER_ID": peer_id,
            "HERMES_KERYX_NODE_KEYPAIR_PATH": str(key_path),
            "HERMES_KERYX_DAEMON_ENDPOINT": f"http://127.0.0.1:{daemon_port}",
            "HERMES_KERYX_RELAY_ENDPOINT": f"http://127.0.0.1:{relay_port}",
            "HERMES_KERYX_RELAY_HEALTH_ENDPOINT": f"http://127.0.0.1:{relay_port}",
            "HERMES_KERYX_RELAY_REGISTRY_ENDPOINT": f"http://127.0.0.1:{registry_port}",
            "HERMES_KERYX_NODE_NAME": peer_id,
            "HERMES_KERYX_NODE_DESCRIPTION": f"Phase 17 test node {peer_id}",
            "HERMES_KERYX_NODE_SKILLS": skills,
            "HERMES_KERYX_NODE_TOKEN": SENDER_TOKEN if peer_id == SENDER_PEER else RECEIVER_TOKEN,
        }
    )
    return env


async def run_worker(daemon_endpoint: str, signal_path: Path) -> None:
    card = AgentCard(
        name="Phase 17 Receiver",
        description="Real cross-process Keryx integration worker",
        skills=[Skill(id=SKILL_ID, description="Returns a deterministic artifact")],
    )
    node = KeryxNode(
        card,
        daemon_endpoint=daemon_endpoint,
        worker_id="phase17-worker",
        claim_wait_timeout_ms=250,
        heartbeat_interval_ms=500,
    )

    @node.on_task
    async def handle(task) -> None:  # type: ignore[no-untyped-def]
        if task.peer_id != SENDER_PEER:
            await task.fail(f"unexpected authenticated sender: {task.peer_id}")
            return
        text = "\n".join(
            part.text or ""
            for message in task.messages
            for part in message.parts
            if part.text
        )
        if text != "phase17-cross-node":
            await task.fail(f"unexpected payload: {text!r}")
            return
        await task.complete(
            [
                Artifact(
                    artifact_id="phase17-artifact",
                    name=EXPECTED_ARTIFACT_NAME,
                    parts=[
                        Part(
                            raw=EXPECTED_ARTIFACT_BYTES,
                            media_type="application/octet-stream",
                        )
                    ],
                )
            ]
        )
        signal_path.write_text("completed\n", encoding="utf-8")

    await node.start()
    try:
        await node.serve_forever()
    finally:
        await node.stop()


async def wait_for_skill(node: KeryxNode, timeout: float = 20.0) -> None:
    deadline = asyncio.get_running_loop().time() + timeout
    while asyncio.get_running_loop().time() < deadline:
        discovered = await node.discover(SKILL_ID, limit=10)
        if any(item.get("peer_id") == RECEIVER_PEER for item in discovered):
            return
        await asyncio.sleep(0.1)
    raise TimeoutError(f"receiver skill {SKILL_ID!r} was not discoverable")


async def send_and_assert(sender_port: int, registry_port: int, work_dir: Path) -> None:
    node = KeryxNode(
        daemon_endpoint=f"127.0.0.1:{sender_port}",
        registry_endpoint=f"127.0.0.1:{registry_port}",
        worker_id="phase17-sender",
    )
    await node.start()
    try:
        await wait_for_skill(node)
        print("PASS receiver skill discovered")
        handle = await node.send_task(
            Message(parts=[Part(text="phase17-cross-node")]),
            skill=SKILL_ID,
            metadata={"skill": SKILL_ID},
        )
        print(f"PASS task dispatched id={handle.task_id}")
        result = await handle.wait(timeout=30)
        if result.status is not TaskStatus.COMPLETED:
            raise AssertionError(
                f"remote task did not complete: status={result.status.value} metadata={result.metadata}"
            )
        if result.originator_peer_id:
            raise AssertionError("sender-side result must not overwrite task originator")
        if not result.metadata or result.metadata.get("executor_peer_id") != RECEIVER_PEER:
            raise AssertionError(f"unexpected executor metadata: {result.metadata}")
        if len(result.artifacts) != 1:
            raise AssertionError(f"unexpected artifact descriptors: {result.artifacts}")
        descriptor = result.artifacts[0]
        if not descriptor.artifact_id:
            raise AssertionError("origin-assigned artifact id missing")
        if descriptor.name != EXPECTED_ARTIFACT_NAME:
            raise AssertionError(f"logical artifact name changed: {descriptor.name!r}")
        artifact = await node.get_artifact(descriptor.artifact_id)
        if artifact.content != EXPECTED_ARTIFACT_BYTES:
            raise AssertionError("retrieved artifact bytes differ")
        download_dir = work_dir / "sender-download"
        download_dir.mkdir(mode=0o700)
        download_path = download_dir / "chosen-output.bin"
        await node.download_artifact(descriptor.artifact_id, download_path)
        if download_path.read_bytes() != EXPECTED_ARTIFACT_BYTES:
            raise AssertionError("downloaded artifact bytes differ")
        if (work_dir / "phase17-result.bin").exists():
            raise AssertionError("remote logical name influenced the local download path")
        print("PASS terminal result returned through relay")
        print("PASS authenticated executor verified")
        print("PASS artifact descriptor canonicalized")
        print("PASS exact artifact bytes retrieved")
        print("PASS explicit-path artifact download verified")
    finally:
        await node.stop()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--bin-dir", type=Path, default=ROOT / "target" / "debug")
    parser.add_argument("--work-dir", type=Path)
    parser.add_argument("--keep", action="store_true")
    parser.add_argument("--worker", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument("--daemon-endpoint", help=argparse.SUPPRESS)
    parser.add_argument("--signal-path", type=Path, help=argparse.SUPPRESS)
    return parser.parse_args()


def require_binary(path: Path) -> Path:
    path = path.resolve()
    if not path.is_file():
        raise FileNotFoundError(f"required binary does not exist: {path}")
    return path


def supervisor(args: argparse.Namespace) -> int:
    generated_work_dir = args.work_dir is None
    work_dir = (
        Path(tempfile.mkdtemp(prefix="keryx-phase17-e2e-"))
        if args.work_dir is None
        else args.work_dir.resolve()
    )
    if work_dir.exists() and not generated_work_dir:
        shutil.rmtree(work_dir)
    work_dir.mkdir(parents=True, exist_ok=True)

    relay_bin = require_binary(args.bin_dir / "keryx-relay")
    daemon_bin = require_binary(args.bin_dir / "keryxd")
    edge_bin = require_binary(args.bin_dir / "keryx-node")

    relay_port = free_port()
    registry_port = free_port()
    sender_port = free_port()
    receiver_port = free_port()
    relay_config = work_dir / "relay.toml"
    relay_config.write_text(
        f'''[relay]
listen_addresses = ["tcp:0"]
bootstrap_peers = []
enable_mdns = false
max_circuits = 16
max_reservations = 16
max_reservations_per_peer = 4
connection_timeout_ms = 5000
use_ipv6 = false
health_grpc_bind = "127.0.0.1:{relay_port}"
health_http_bind = ""
registry_grpc_bind = "127.0.0.1:{registry_port}"

[[security.node_tokens]]
node_id = "{SENDER_PEER}"
token = "{SENDER_TOKEN}"

[[security.node_tokens]]
node_id = "{RECEIVER_PEER}"
token = "{RECEIVER_TOKEN}"
''',
        encoding="utf-8",
    )

    group = ProcessGroup(work_dir)
    success = False
    try:
        relay_env = base_env()
        relay_env["HERMES_KERYX_RELAY_CONFIG"] = str(relay_config)
        group.start("relay", [str(relay_bin)], relay_env)
        wait_tcp(relay_port, group)
        wait_tcp(registry_port, group)
        print("PASS relay and registry ready")

        group.start(
            "sender-daemon",
            [str(daemon_bin)],
            daemon_env(
                peer_id=SENDER_PEER,
                data_dir=work_dir / "sender-data",
                daemon_port=sender_port,
                relay_port=relay_port,
                registry_port=registry_port,
            ),
        )
        group.start(
            "receiver-daemon",
            [str(daemon_bin)],
            daemon_env(
                peer_id=RECEIVER_PEER,
                data_dir=work_dir / "receiver-data",
                daemon_port=receiver_port,
                relay_port=relay_port,
                registry_port=registry_port,
            ),
        )
        wait_tcp(sender_port, group)
        wait_tcp(receiver_port, group)
        print("PASS two daemons ready")

        group.start(
            "sender-edge",
            [str(edge_bin)],
            edge_env(
                peer_id=SENDER_PEER,
                daemon_port=sender_port,
                relay_port=relay_port,
                registry_port=registry_port,
                key_path=work_dir / "sender-edge.key",
            ),
        )
        group.start(
            "receiver-edge",
            [str(edge_bin)],
            edge_env(
                peer_id=RECEIVER_PEER,
                daemon_port=receiver_port,
                relay_port=relay_port,
                registry_port=registry_port,
                key_path=work_dir / "receiver-edge.key",
                skills=SKILL_ID,
            ),
        )
        worker_signal = work_dir / "worker-completed"
        group.start(
            "receiver-worker",
            [
                sys.executable,
                str(Path(__file__).resolve()),
                "--worker",
                "--daemon-endpoint",
                f"127.0.0.1:{receiver_port}",
                "--signal-path",
                str(worker_signal),
            ],
            base_env(),
        )
        time.sleep(0.5)
        group.assert_alive()
        print("PASS edges and receiver worker started")

        asyncio.run(send_and_assert(sender_port, registry_port, work_dir))
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline and not worker_signal.exists():
            group.assert_alive()
            time.sleep(0.05)
        if not worker_signal.exists():
            raise AssertionError("receiver worker did not record terminal completion")
        print("PASS receiver handler completed")
        success = True
        return 0
    except Exception as error:
        print(f"FAIL {type(error).__name__}: {error}", file=sys.stderr)
        group.print_tails()
        print(f"Preserved failure state: {work_dir}", file=sys.stderr)
        return 1
    finally:
        group.stop_all()
        if success and not args.keep:
            shutil.rmtree(work_dir, ignore_errors=True)
        elif success:
            print(f"Preserved successful state: {work_dir}")


def main() -> int:
    args = parse_args()
    if args.worker:
        if not args.daemon_endpoint or args.signal_path is None:
            raise SystemExit("worker mode requires --daemon-endpoint and --signal-path")
        asyncio.run(run_worker(args.daemon_endpoint, args.signal_path))
        return 0
    return supervisor(args)


if __name__ == "__main__":
    raise SystemExit(main())
