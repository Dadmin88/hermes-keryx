"""Dataclasses mirroring Keryx daemon task proto responses."""

from __future__ import annotations

import hashlib
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

_PROTO_ROOT = Path(__file__).resolve().parent / "proto"
if str(_PROTO_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROTO_ROOT))

from hermes.keryx.v1 import common_pb2, daemon_pb2  # noqa: E402


@dataclass(slots=True)
class TaskArtifact:
    """An output-artifact descriptor with optional in-memory binary content.

    ``path`` is display metadata only. The SDK never resolves it as a local path
    or reads it during task completion.
    """

    path: str
    media_type: str = "application/octet-stream"
    metadata: dict[str, str] = field(default_factory=dict)
    content: bytes | None = None
    sha256: str | None = None
    byte_len: int | None = None

    @classmethod
    def from_proto(cls, artifact: daemon_pb2.TaskArtifact) -> "TaskArtifact":
        content = bytes(artifact.content) if artifact.content_present else None
        return cls(
            path=artifact.path,
            media_type=artifact.media_type,
            metadata=dict(artifact.metadata),
            content=content,
            sha256=artifact.sha256 or None,
            byte_len=(
                artifact.byte_len
                if artifact.content_present or artifact.byte_len
                else None
            ),
        )

    def to_proto(self) -> daemon_pb2.TaskArtifact:
        if self.content is None:
            if self.sha256 is not None or self.byte_len is not None:
                raise ValueError("descriptor-only artifacts cannot declare digest or byte_len")
            return daemon_pb2.TaskArtifact(
                path=self.path,
                media_type=self.media_type,
                metadata=self.metadata,
                content_present=False,
            )
        if not isinstance(self.content, bytes):
            raise TypeError("TaskArtifact.content must be bytes or None")

        computed_sha256 = hashlib.sha256(self.content).hexdigest()
        computed_byte_len = len(self.content)
        if self.sha256 is not None:
            if not isinstance(self.sha256, str):
                raise TypeError("TaskArtifact.sha256 must be a string or None")
            if self.sha256 != computed_sha256:
                raise ValueError("TaskArtifact.sha256 does not match content")
        if self.byte_len is not None:
            if isinstance(self.byte_len, bool) or not isinstance(self.byte_len, int):
                raise TypeError("TaskArtifact.byte_len must be an integer or None")
            if self.byte_len != computed_byte_len:
                raise ValueError("TaskArtifact.byte_len does not match content")
        return daemon_pb2.TaskArtifact(
            path=self.path,
            media_type=self.media_type,
            metadata=self.metadata,
            content=self.content,
            sha256=computed_sha256,
            byte_len=computed_byte_len,
            content_present=True,
        )


@dataclass(slots=True)
class ArtifactContent:
    """Verified artifact metadata and, unless metadata-only, its content bytes."""

    artifact_id: str
    task_id: str
    digest: str
    media_type: str
    byte_len: int
    inline: bool
    created_at: str
    content: bytes | None = None


@dataclass(slots=True)
class TaskState:
    """State returned by SubmitTask, ClaimTask, and Heartbeat RPCs."""

    task_id: str
    status: str = ""
    lease_id: str = ""
    worker_id: str = ""
    leased_at_ms: int = 0
    expires_at_ms: int = 0
    retry_count: int = 0
    dead_lettered: bool = False
    metadata: dict[str, str] = field(default_factory=dict)

    @classmethod
    def from_submit(cls, response: daemon_pb2.SubmitTaskResponse) -> "TaskState":
        return cls(
            task_id=_id_value(response.task_id),
            status=response.status,
        )

    @classmethod
    def from_claim(cls, response: daemon_pb2.ClaimTaskResponse) -> "TaskState":
        return cls(
            task_id=_id_value(response.task_id),
            status=response.status,
            lease_id=_id_value(response.lease_id),
            worker_id=_id_value(response.worker_id),
            leased_at_ms=response.leased_at_ms,
            expires_at_ms=response.expires_at_ms,
            retry_count=response.retry_count,
            dead_lettered=response.dead_lettered,
        )

    @classmethod
    def from_heartbeat(
        cls,
        response: daemon_pb2.HeartbeatResponse,
        *,
        task_id: str,
        worker_id: str,
    ) -> "TaskState":
        return cls(
            task_id=task_id,
            lease_id=_id_value(response.lease_id),
            worker_id=worker_id,
            expires_at_ms=response.expires_at_ms,
        )


@dataclass(slots=True)

class ClaimedTask:
    # A task atomically dequeued from the daemon for worker execution.

    has_task: bool
    task_id: str = ""
    lease_id: str = ""
    worker_id: str = ""
    leased_at_ms: int = 0
    expires_at_ms: int = 0
    status: str = ""
    retry_count: int = 0
    dead_lettered: bool = False
    sender_peer_id: str = ""
    envelope: Any | None = None

    @classmethod
    def from_proto(cls, response: daemon_pb2.ClaimNextTaskResponse) -> "ClaimedTask":
        envelope = response.envelope if response.has_task and response.HasField("envelope") else None
        return cls(
            has_task=response.has_task,
            task_id=_id_value(response.task_id),
            lease_id=_id_value(response.lease_id),
            worker_id=_id_value(response.worker_id),
            leased_at_ms=response.leased_at_ms,
            expires_at_ms=response.expires_at_ms,
            status=response.status,
            retry_count=response.retry_count,
            dead_lettered=response.dead_lettered,
            sender_peer_id=response.sender_peer_id,
            envelope=envelope,
        )


@dataclass(slots=True)
class TaskResult:
    """Terminal or post-action result returned by lifecycle RPCs."""

    task_id: str
    status: str = ""
    duration_ms: int = 0
    result_metadata: dict[str, str] = field(default_factory=dict)
    output_artifacts: list[TaskArtifact] = field(default_factory=list)
    error_reason: str = ""
    failure_metadata: dict[str, str] = field(default_factory=dict)
    retry_count: int = 0
    dead_lettered: bool = False
    canceled: bool = False
    reason: str = ""

    @classmethod
    def from_complete(cls, response: daemon_pb2.CompleteTaskResponse) -> "TaskResult":
        return cls(
            task_id=_id_value(response.task_id),
            status=response.status,
            duration_ms=response.duration_ms,
            result_metadata=dict(response.result_metadata),
            output_artifacts=[TaskArtifact.from_proto(item) for item in response.output_artifacts],
        )

    @classmethod
    def from_fail(cls, response: daemon_pb2.FailTaskResponse) -> "TaskResult":
        return cls(
            task_id=_id_value(response.task_id),
            status=response.status,
            duration_ms=response.duration_ms,
            error_reason=response.error_reason,
            failure_metadata=dict(response.failure_metadata),
            retry_count=response.retry_count,
            dead_lettered=response.dead_lettered,
        )

    @classmethod
    def from_cancel(cls, response: daemon_pb2.CancelTaskResponse) -> "TaskResult":
        return cls(
            task_id=_id_value(response.task_id),
            status=response.status,
            canceled=response.canceled,
            reason=response.reason,
        )


def _id_value(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value
    if isinstance(
        value,
        (
            common_pb2.TaskId,
            common_pb2.LeaseId,
            common_pb2.AgentId,
            common_pb2.NodeId,
            common_pb2.CorrelationId,
            common_pb2.IdempotencyKey,
        ),
    ):
        return value.value
    return getattr(value, "value", "")
