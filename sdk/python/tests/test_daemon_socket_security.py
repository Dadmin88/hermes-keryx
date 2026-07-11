from __future__ import annotations

import socket
import tempfile
from collections.abc import Iterator
from pathlib import Path

import pytest

from keryx.client import _validate_unix_socket_endpoint, default_daemon_endpoint
from keryx.card import AgentCard
from keryx.node import KeryxNode


def _bind_socket(path: Path) -> socket.socket:
    sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    sock.bind(str(path))
    return sock


@pytest.fixture
def short_socket_dir() -> Iterator[Path]:
    # Linux limits AF_UNIX path names to roughly 108 bytes. Keep the bind path
    # short even when pytest itself runs below a deeply nested temporary root.
    temp_root = Path("/tmp") if Path("/tmp").is_dir() else Path(tempfile.gettempdir())
    with tempfile.TemporaryDirectory(prefix="kx-sock-", dir=temp_root) as directory:
        yield Path(directory)


def test_default_daemon_endpoint_uses_owner_run_directory(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setenv("HOME", str(tmp_path))
    monkeypatch.delenv("HERMES_KERYX_DAEMON_ENDPOINT", raising=False)

    endpoint = default_daemon_endpoint()
    node = KeryxNode(AgentCard(name="test", description="test", skills=[]))

    assert endpoint == f"unix://{tmp_path}/.hermes/keryx/run/keryx-daemon.sock"
    assert node._daemon_endpoint == endpoint


def test_validate_unix_socket_rejects_world_writable_parent(short_socket_dir: Path) -> None:
    socket_path = short_socket_dir / "keryx-daemon.sock"
    sock = _bind_socket(socket_path)
    try:
        short_socket_dir.chmod(0o777)
        with pytest.raises(RuntimeError, match="directory must not be accessible"):
            _validate_unix_socket_endpoint(f"unix://{socket_path}")
    finally:
        short_socket_dir.chmod(0o700)
        sock.close()


def test_validate_unix_socket_rejects_world_writable_socket(short_socket_dir: Path) -> None:
    short_socket_dir.chmod(0o700)
    socket_path = short_socket_dir / "keryx-daemon.sock"
    sock = _bind_socket(socket_path)
    try:
        socket_path.chmod(0o777)
        with pytest.raises(RuntimeError, match="must not be writable"):
            _validate_unix_socket_endpoint(f"unix://{socket_path}")
    finally:
        sock.close()


def test_validate_unix_socket_accepts_owner_only_socket(short_socket_dir: Path) -> None:
    short_socket_dir.chmod(0o700)
    socket_path = short_socket_dir / "keryx-daemon.sock"
    sock = _bind_socket(socket_path)
    try:
        socket_path.chmod(0o600)
        _validate_unix_socket_endpoint(f"unix://{socket_path}")
    finally:
        sock.close()
