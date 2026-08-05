"""Async gRPC client for Keryx daemon and relay registry."""

from __future__ import annotations

import hashlib
import os
import re
import socket
import stat
import struct
import sys
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any

import grpc

_PROTO_ROOT = Path(__file__).resolve().parent / "proto"
if str(_PROTO_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROTO_ROOT))

from hermes.keryx.v1 import (  # noqa: E402
    common_pb2,
    daemon_pb2,
    daemon_pb2_grpc,
    registry_pb2,
    registry_pb2_grpc,
    task_pb2,
)

from keryx.models import ArtifactContent  # noqa: E402

if TYPE_CHECKING:
    from keryx.card import AgentCard


RESULT_ARTIFACT_FRAME_MAX_BYTES = 5 * 1024 * 1024
RESULT_ARTIFACT_GRPC_OPTIONS = (
    ("grpc.max_send_message_length", RESULT_ARTIFACT_FRAME_MAX_BYTES),
    ("grpc.max_receive_message_length", RESULT_ARTIFACT_FRAME_MAX_BYTES),
)
_SHA256_RE = re.compile(r"[0-9a-f]{64}\Z")
REGISTRY_RPC_TIMEOUT_SECONDS = 10.0


def _validate_registration_ttl(ttl_seconds: object) -> int:
    if (
        isinstance(ttl_seconds, bool)
        or not isinstance(ttl_seconds, int)
        or not 0 < ttl_seconds <= 2**64 - 1
    ):
        raise ValueError("ttl_seconds must be a positive unsigned 64-bit integer")
    return ttl_seconds


def default_daemon_endpoint() -> str:
    socket_path = (
        Path.home().expanduser() / ".hermes" / "keryx" / "run" / "keryx-daemon.sock"
    )
    return f"unix://{socket_path}"


def _unix_socket_path(endpoint: str) -> Path | None:
    if not endpoint.startswith("unix://"):
        return None
    return Path(endpoint.removeprefix("unix://")).expanduser()


def _validate_unix_socket_endpoint(endpoint: str) -> None:
    path = _unix_socket_path(endpoint)
    if path is None:
        return

    try:
        parent_stat = path.parent.stat()
        socket_stat = path.stat()
    except FileNotFoundError as exc:
        raise RuntimeError(f"daemon socket does not exist: {path}") from exc

    current_uid = os.getuid()
    if parent_stat.st_uid != current_uid:
        raise RuntimeError(
            f"daemon socket directory is not owned by the current user: {path.parent}"
        )
    if parent_stat.st_mode & (stat.S_IRWXG | stat.S_IRWXO):
        raise RuntimeError(
            "daemon socket directory must not be accessible by group or other users: "
            f"{path.parent}"
        )
    if socket_stat.st_uid != current_uid:
        raise RuntimeError(f"daemon socket is not owned by the current user: {path}")
    if not stat.S_ISSOCK(socket_stat.st_mode):
        raise RuntimeError(f"daemon endpoint is not a Unix socket: {path}")
    if socket_stat.st_mode & (stat.S_IWGRP | stat.S_IWOTH):
        raise RuntimeError(
            f"daemon socket must not be writable by group or other users: {path}"
        )


def _assert_unix_peer_owned_by_current_user(endpoint: str) -> None:
    path = _unix_socket_path(endpoint)
    if path is None or not hasattr(socket, "SO_PEERCRED"):
        return

    try:
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as probe:
            probe.connect(str(path))
            credentials = probe.getsockopt(socket.SOL_SOCKET, socket.SO_PEERCRED, 12)
    except OSError as exc:
        raise RuntimeError(f"daemon socket peer could not be verified: {path}") from exc
    _, peer_uid, _ = struct.unpack("3i", credentials)
    if peer_uid != os.getuid():
        raise RuntimeError(
            f"daemon socket peer is not owned by the current user: {path}"
        )


def _grpc_target(endpoint: str) -> str:
    if endpoint.startswith("unix://"):
        return endpoint
    if endpoint.startswith("tcp://"):
        return endpoint.removeprefix("tcp://")
    return endpoint


@dataclass
class PeerInfo:
    peer_id: str
    connected: bool
    local: bool


class DaemonClient:
    """Thin wrapper over ``KeryxDaemon`` and ``RegistryService`` stubs."""

    def __init__(
        self,
        *,
        daemon_endpoint: str,
        registry_endpoint: str | None = None,
        channel: grpc.aio.Channel | None = None,
        registry_channel: grpc.aio.Channel | None = None,
    ) -> None:
        self._daemon_endpoint = daemon_endpoint
        self._registry_endpoint = registry_endpoint or os.environ.get(
            "HERMES_KERYX_REGISTRY_ENDPOINT"
        )
        self._channel = channel
        self._registry_channel = registry_channel
        self._daemon: daemon_pb2_grpc.KeryxDaemonStub | None = None
        self._registry: registry_pb2_grpc.RegistryServiceStub | None = None

    async def connect(self) -> None:
        if self._channel is None:
            _validate_unix_socket_endpoint(self._daemon_endpoint)
            _assert_unix_peer_owned_by_current_user(self._daemon_endpoint)
            self._channel = grpc.aio.insecure_channel(
                _grpc_target(self._daemon_endpoint),
                options=RESULT_ARTIFACT_GRPC_OPTIONS,
            )
        self._daemon = daemon_pb2_grpc.KeryxDaemonStub(self._channel)
        if self._registry_endpoint:
            if self._registry_channel is None:
                self._registry_channel = grpc.aio.insecure_channel(
                    _grpc_target(self._registry_endpoint)
                )
            self._registry = registry_pb2_grpc.RegistryServiceStub(
                self._registry_channel
            )

    async def close(self) -> None:
        if self._channel is not None:
            await self._channel.close()
            self._channel = None
        if self._registry_channel is not None:
            await self._registry_channel.close()
            self._registry_channel = None
        self._daemon = None
        self._registry = None

    async def list_peers(self) -> list[PeerInfo]:
        assert self._daemon is not None
        response = await self._daemon.ListPeers(daemon_pb2.ListPeersRequest())
        return [
            PeerInfo(peer_id=item.peer_id, connected=item.connected, local=item.local)
            for item in response.peers
        ]

    async def local_peer_id(self) -> str:
        for peer in await self.list_peers():
            if peer.local:
                return peer.peer_id
        peers = await self.list_peers()
        if peers:
            return peers[0].peer_id
        raise RuntimeError("daemon did not report a local peer id")

    async def send_task(
        self,
        *,
        target_peer_id: str,
        task_id: str,
        message_text: str,
        metadata: dict[str, str] | None = None,
        deadline_ms: int = 0,
        timeout_ms: int = 0,
    ) -> daemon_pb2.SendTaskResponse:
        assert self._daemon is not None
        if (
            isinstance(deadline_ms, bool)
            or not isinstance(deadline_ms, int)
            or not 0 <= deadline_ms <= 2**63 - 1
        ):
            raise ValueError("deadline_ms must be zero or a positive signed 64-bit integer")
        envelope = task_pb2.TaskEnvelope(
            task_id=common_pb2.TaskId(value=task_id),
            status=task_pb2.TASK_STATUS_CREATED,
            messages=[
                task_pb2.TaskMessage(
                    parts=[
                        task_pb2.TaskMessagePart(
                            text=message_text,
                            media_type="text/plain",
                        )
                    ]
                )
            ],
            metadata=metadata or {},
            deadline_ms=deadline_ms,
        )
        request = daemon_pb2.SendTaskRequest(
            target_peer_id=target_peer_id,
            envelope=envelope,
            timeout_ms=timeout_ms,
        )
        return await self._daemon.SendTask(request)

    async def get_task_result(
        self, task_id: str
    ) -> daemon_pb2.GetTaskResultResponse:
        assert self._daemon is not None
        return await self._daemon.GetTaskResult(
            daemon_pb2.GetTaskResultRequest(
                task_id=common_pb2.TaskId(value=task_id)
            )
        )

    async def get_artifact(
        self, artifact_id: str, *, metadata_only: bool = False
    ) -> ArtifactContent:
        assert self._daemon is not None
        response = await self._daemon.GetArtifact(
            daemon_pb2.GetArtifactRequest(
                artifact_id=common_pb2.ArtifactId(value=artifact_id),
                metadata_only=metadata_only,
            )
        )
        return _verified_artifact_content(
            response,
            requested_artifact_id=artifact_id,
            metadata_only=metadata_only,
        )

    async def download_artifact(
        self,
        artifact_id: str,
        destination: str | Path,
        *,
        overwrite: bool = False,
    ) -> ArtifactContent:
        artifact = await self.get_artifact(artifact_id)
        _write_artifact_download(artifact, destination, overwrite=overwrite)
        return artifact

    async def cancel_task(
        self, task_id: str, *, reason: str = ""
    ) -> daemon_pb2.CancelTaskResponse:
        assert self._daemon is not None
        return await self._daemon.CancelTask(
            daemon_pb2.CancelTaskRequest(
                task_id=common_pb2.TaskId(value=task_id),
                reason=reason,
            )
        )

    async def discover(
        self,
        skill_id: str,
        *,
        tags: list[str] | None = None,
        limit: int = 10,
    ) -> list[dict[str, Any]]:
        if self._registry is None:
            return []
        assert self._registry is not None
        requested_tags = {tag.strip() for tag in tags or [] if tag.strip()}
        response = await self._registry.DiscoverBySkill(
            registry_pb2.DiscoverBySkillRequest(
                skill_id=skill_id,
                tags=tags or [],
                limit=limit,
            )
        )
        registrations = list(response.registrations)

        # Registry gossip can briefly expose a node before its secondary skill
        # index catches up. Fall back to one bounded full-registry read and
        # filter locally so discovery remains correct without an unbounded scan.
        if skill_id and not registrations:
            fallback_limit = limit if limit > 0 else 100
            fallback = await self._registry.DiscoverBySkill(
                registry_pb2.DiscoverBySkillRequest(
                    skill_id="",
                    limit=fallback_limit,
                )
            )
            registrations = []
            for registration in fallback.registrations:
                matching_skill = next(
                    (
                        skill
                        for skill in registration.skills
                        if skill.skill_id == skill_id
                    ),
                    None,
                )
                if matching_skill is None:
                    continue
                skill_tags = {tag.strip() for tag in matching_skill.tags if tag.strip()}
                if requested_tags and not requested_tags.issubset(skill_tags):
                    continue
                registrations.append(registration)
                if limit > 0 and len(registrations) >= limit:
                    break

        return [
            {
                "peer_id": registration.peer_id,
                "agent_name": registration.name,
                "agent_description": registration.description,
                "skills": [skill.skill_id for skill in registration.skills],
            }
            for registration in registrations
        ]

    async def register_skills(
        self,
        *,
        peer_id: str,
        name: str,
        description: str,
        skills: list[tuple[str, str, list[str]]],
        ttl_seconds: int = 300,
    ) -> bool:
        ttl_seconds = _validate_registration_ttl(ttl_seconds)
        if self._registry is None:
            return False
        assert self._registry is not None
        request = registry_pb2.RegisterSkillsRequest(
            peer_id=peer_id,
            name=name,
            description=description,
            ttl_seconds=ttl_seconds,
            skills=[
                registry_pb2.SkillInfo(
                    skill_id=skill_id,
                    description=skill_description,
                    tags=tags,
                )
                for skill_id, skill_description, tags in skills
            ],
        )
        response = await self._registry.RegisterSkills(
            request, timeout=REGISTRY_RPC_TIMEOUT_SECONDS
        )
        return response.accepted

    async def unregister_skills(
        self, *, peer_id: str, skill_ids: list[str]
    ) -> bool:
        if self._registry is None:
            return False
        assert self._registry is not None
        response = await self._registry.UnregisterSkills(
            registry_pb2.UnregisterSkillsRequest(
                peer_id=peer_id,
                skill_ids=skill_ids,
            ),
            timeout=REGISTRY_RPC_TIMEOUT_SECONDS,
        )
        return response.accepted

    async def get_card(self, peer_id: str) -> "AgentCard":
        from keryx.card import AgentCard, Skill

        if self._registry is None:
            raise RuntimeError("registry client is not configured")
        assert self._registry is not None
        response = await self._registry.DiscoverBySkill(
            registry_pb2.DiscoverBySkillRequest(skill_id="", limit=100)
        )
        for registration in response.registrations:
            if registration.peer_id == peer_id:
                return AgentCard(
                    name=registration.name or registration.peer_id,
                    description=registration.description,
                    skills=[
                        Skill(
                            id=skill.skill_id,
                            description=skill.description,
                            tags=list(skill.tags),
                        )
                        for skill in registration.skills
                    ],
                    peer_id=registration.peer_id,
                )
        raise RuntimeError(f"No agent card for peer {peer_id}")


def _verified_artifact_content(
    response: daemon_pb2.GetArtifactResponse,
    *,
    requested_artifact_id: str,
    metadata_only: bool,
) -> ArtifactContent:
    returned_artifact_id = response.artifact_id.value
    if returned_artifact_id != requested_artifact_id:
        raise ValueError("returned artifact id does not match the request")
    if not _SHA256_RE.fullmatch(response.digest):
        raise ValueError("artifact digest must be lowercase SHA-256")

    content = bytes(response.content)
    if not metadata_only or content:
        if response.byte_len != len(content):
            raise ValueError("artifact byte_len does not match content")
        if hashlib.sha256(content).hexdigest() != response.digest:
            raise ValueError("artifact digest does not match content")

    return ArtifactContent(
        artifact_id=returned_artifact_id,
        task_id=response.task_id.value,
        digest=response.digest,
        media_type=response.media_type,
        byte_len=response.byte_len,
        inline=response.inline,
        created_at=response.created_at,
        content=None if metadata_only else content,
    )


def _write_artifact_download(
    artifact: ArtifactContent,
    destination: str | Path,
    *,
    overwrite: bool,
) -> Path:
    if artifact.content is None:
        raise ValueError("artifact content is required for download")
    content = artifact.content
    if len(content) != artifact.byte_len:
        raise ValueError("artifact byte_len does not match content")
    if not _SHA256_RE.fullmatch(artifact.digest):
        raise ValueError("artifact digest must be lowercase SHA-256")
    if hashlib.sha256(content).hexdigest() != artifact.digest:
        raise ValueError("artifact digest does not match content")

    target = Path(destination).expanduser()
    parent = target.parent
    if not parent.exists() or not parent.is_dir():
        raise FileNotFoundError(
            f"artifact destination parent is not a directory: {parent}"
        )
    if target.is_symlink():
        raise ValueError("artifact destination must not be a symlink")
    if target.exists() and not overwrite:
        raise FileExistsError(target)

    fd, temporary_name = tempfile.mkstemp(
        prefix=f".{target.name}.", suffix=".tmp", dir=parent
    )
    temporary = Path(temporary_name)
    published = False
    try:
        os.fchmod(fd, 0o600)
        with os.fdopen(fd, "wb", closefd=True) as handle:
            fd = -1
            handle.write(content)
            handle.flush()
            os.fsync(handle.fileno())
        if overwrite:
            if target.is_symlink():
                raise ValueError("artifact destination must not be a symlink")
            os.replace(temporary, target)
        else:
            os.link(temporary, target, follow_symlinks=False)
            temporary.unlink()
        published = True
        directory_fd = os.open(
            parent, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
        )
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
    finally:
        if fd >= 0:
            os.close(fd)
        if not published:
            temporary.unlink(missing_ok=True)
    return target
