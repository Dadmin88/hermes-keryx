"""KeryxNode — Python SDK entry point for Hermes Agency profiles."""

from __future__ import annotations

import asyncio
import inspect
import logging
import math
import sys
import time
import uuid
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import asdict, is_dataclass
from pathlib import Path
from typing import Any

import grpc

from keryx.card import AgentCard
from keryx.client import (
    RESULT_ARTIFACT_GRPC_OPTIONS,
    DaemonClient,
    _validate_registration_ttl,
    _verified_artifact_content,
    _write_artifact_download,
)
from keryx.config import KeryxConfig, grpc_target, load_config
from keryx.models import (
    ArtifactContent,
    ClaimedTask,
    TaskArtifact,
    TaskResult,
    TaskState,
)
from keryx.task import (
    Artifact,
    IncomingTask,
    Message,
    Part,
    SubmissionReceipt,
    Task,
    TaskHandle,
    TaskStatus as LegacyTaskStatus,
)

_PROTO_ROOT = Path(__file__).resolve().parent / "proto"
if str(_PROTO_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROTO_ROOT))

from hermes.keryx.v1 import common_pb2, daemon_pb2, daemon_pb2_grpc, task_pb2  # noqa: E402

logger = logging.getLogger(__name__)

TaskHandler = Callable[[IncomingTask], Awaitable[None]]


class KeryxNode:
    """Async gRPC wrapper around the Keryx daemon.

    The Phase 15A API is the direct daemon lifecycle surface used by Hermes
    Agency profiles: ``connect()``, ``submit()/claim()/heartbeat()/complete()/
    fail()/cancel()``, and query helpers for status/doctor/peers/skills.

    The older AgentAnycast-compatible helpers (``start()``, ``send_task()``,
    ``register_skills()``) are retained so existing SDK callers keep working
    while Agency migrates to the native Keryx lifecycle methods.
    """

    def __init__(
        self,
        card: AgentCard | None = None,
        *,
        config: KeryxConfig | None = None,
        config_path: str | Path | None = None,
        relay_endpoint: str | None = None,
        relay: str | None = None,
        daemon_endpoint: str | None = None,
        daemon_addr: str | None = None,
        registry_endpoint: str | None = None,
        worker_id: str | None = None,
        home: str | Path | None = None,
        daemon_bin: str | Path | None = None,
        status_callback: Callable[[str], None] | None = None,
        channel: grpc.aio.Channel | None = None,
        daemon_stub: Any | None = None,
        worker_concurrency: int = 1,
        claim_wait_timeout_ms: int = 1_000,
        heartbeat_interval_ms: int | None = None,
        shutdown_grace_seconds: float = 5.0,
        registration_stop_timeout_seconds: float = 1.0,
        client_factory: Callable[..., DaemonClient] | type[DaemonClient] | None = None,
        **_ignored: Any,
    ) -> None:
        loaded_config = config or load_config(config_path)
        if daemon_endpoint or daemon_addr or registry_endpoint or relay_endpoint or relay or worker_id:
            loaded_config = KeryxConfig(
                daemon_endpoint=daemon_endpoint or daemon_addr or loaded_config.daemon_endpoint,
                registry_endpoint=registry_endpoint or loaded_config.registry_endpoint,
                relay_endpoint=relay_endpoint or relay or loaded_config.relay_endpoint,
                worker_id=worker_id or loaded_config.worker_id,
                default_lease_duration_ms=loaded_config.default_lease_duration_ms,
                request_timeout_ms=loaded_config.request_timeout_ms,
            )

        self._config = loaded_config
        self._card = card
        self._relay = loaded_config.relay_endpoint
        self._home = Path(home).expanduser() if home is not None else None
        self._daemon_bin = daemon_bin
        self._status_callback = status_callback
        self._daemon_endpoint = loaded_config.daemon_endpoint
        self._registry_endpoint = loaded_config.registry_endpoint
        self._worker_id = loaded_config.worker_id

        self._channel = channel
        self._owns_channel = channel is None
        self._daemon_stub = daemon_stub
        self._connected = daemon_stub is not None

        self._client_factory = client_factory or DaemonClient
        self._client: DaemonClient | None = None
        self._peer_id: str | None = None
        self._running = False
        if worker_concurrency < 1:
            raise ValueError("worker_concurrency must be at least 1")
        if claim_wait_timeout_ms < 0:
            raise ValueError("claim_wait_timeout_ms cannot be negative")
        if heartbeat_interval_ms is not None and heartbeat_interval_ms < 1:
            raise ValueError("heartbeat_interval_ms must be positive")
        if shutdown_grace_seconds < 0:
            raise ValueError("shutdown_grace_seconds cannot be negative")
        if (
            isinstance(registration_stop_timeout_seconds, bool)
            or not isinstance(registration_stop_timeout_seconds, (int, float))
            or not math.isfinite(registration_stop_timeout_seconds)
            or registration_stop_timeout_seconds <= 0
        ):
            raise ValueError("registration_stop_timeout_seconds must be positive and finite")
        self._serve_stop = asyncio.Event()
        self._serve_done = asyncio.Event()
        self._serve_done.set()
        self._worker_concurrency = worker_concurrency
        self._claim_wait_timeout_ms = claim_wait_timeout_ms
        self._heartbeat_interval_ms = heartbeat_interval_ms
        self._shutdown_grace_seconds = shutdown_grace_seconds
        self._registration_stop_timeout_seconds = float(
            registration_stop_timeout_seconds
        )
        self._task_handlers: list[TaskHandler] = []
        self._registration_stop = asyncio.Event()
        self._registration_lock = asyncio.Lock()
        self._registration_task: asyncio.Task[None] | None = None
        self._registration_cleanup_task: asyncio.Task[None] | None = None
        self._registration_close_client_after_cleanup = False
        self._registration_card: AgentCard | None = None
        self._registration_last_error: str | None = None
        self._registration_consecutive_failures = 0
        self._registration_last_success_ms = 0

    @property
    def config(self) -> KeryxConfig:
        return self._config

    @property
    def peer_id(self) -> str:
        if not self._peer_id:
            raise RuntimeError("Node not started. Call await node.start() or await node.connect() first.")
        return self._peer_id

    @property
    def card(self) -> AgentCard | None:
        return self._card

    async def connect(self, *, wait_ready: bool = False) -> "KeryxNode":
        """Create the daemon gRPC stub and optionally wait for channel readiness."""

        if self._connected:
            return self
        if self._status_callback:
            self._status_callback("Connecting to Keryx daemon")
        if self._channel is None:
            self._channel = grpc.aio.insecure_channel(
                grpc_target(self._daemon_endpoint), options=RESULT_ARTIFACT_GRPC_OPTIONS
            )
            self._owns_channel = True
        if wait_ready and hasattr(self._channel, "channel_ready"):
            await self._channel.channel_ready()
        self._daemon_stub = daemon_pb2_grpc.KeryxDaemonStub(self._channel)
        self._connected = True
        try:
            self._peer_id = await self.local_peer_id()
            if self._card is not None:
                self._card.peer_id = self._peer_id
        except Exception:  # pragma: no cover - daemon may not expose peers during startup
            logger.debug("Unable to discover local peer id during connect", exc_info=True)
        return self

    async def close(self) -> None:
        """Close the SDK-owned gRPC channel."""

        stop_result: dict[str, Any] | None = None
        if self._registration_task is not None:
            try:
                stop_result = await self.stop_registration()
            except Exception:
                logger.warning("Unable to deregister skills during shutdown", exc_info=True)
        client = self._client
        client_transferred = False
        if stop_result and stop_result.get("cleanup_pending") and client is not None:
            async with self._registration_lock:
                if self._registration_cleanup_task is not None and self._client is client:
                    self._registration_close_client_after_cleanup = True
                    self._client = None
                    client_transferred = True
        if client is not None and not client_transferred:
            await client.close()
            if self._client is client:
                self._client = None
            async with self._registration_lock:
                if self._registration_cleanup_task is None:
                    self._registration_task = None
                    self._registration_card = None
        if self._channel is not None and self._owns_channel:
            result = self._channel.close()
            if inspect.isawaitable(result):
                await result
        self._channel = None
        self._daemon_stub = None
        self._connected = False
        self._running = False

    async def __aenter__(self) -> "KeryxNode":
        return await self.connect()

    async def __aexit__(self, *_exc: object) -> None:
        await self.close()

    async def status(self) -> dict[str, Any]:
        daemon = await self._daemon()
        return _proto_to_dict(await daemon.Status(daemon_pb2.StatusRequest()))

    async def doctor(self) -> dict[str, Any]:
        daemon = await self._daemon()
        return _proto_to_dict(await daemon.Doctor(daemon_pb2.DoctorRequest()))

    async def peers(self) -> list[dict[str, Any]]:
        daemon = await self._daemon()
        response = await daemon.ListPeers(daemon_pb2.ListPeersRequest())
        return [_proto_to_dict(peer) for peer in response.peers]

    async def skills(
        self,
        skill_id: str = "",
        *,
        tags: Sequence[str] | Mapping[str, str] | None = None,
        limit: int = 10,
    ) -> list[dict[str, Any]]:
        daemon = await self._daemon()
        response = await daemon.DiscoverSkills(
            daemon_pb2.DiscoverSkillsRequest(
                skill_id=skill_id,
                tags=_tag_list(tags),
                limit=limit,
            )
        )
        return [_proto_to_dict(registration) for registration in response.registrations]

    async def submit(
        self,
        task_id: str | None = None,
        *,
        message: str | Message | Mapping[str, Any] | None = None,
        messages: Sequence[str | Message | Mapping[str, Any]] | None = None,
        metadata: Mapping[str, str] | None = None,
        correlation_id: str | None = None,
        idempotency_key: str | None = None,
    ) -> TaskState:
        daemon = await self._daemon()
        resolved_task_id = task_id or str(uuid.uuid4())
        envelope = _task_envelope(
            task_id=resolved_task_id,
            message=message,
            messages=messages,
            metadata=metadata,
            correlation_id=correlation_id,
            idempotency_key=idempotency_key,
        )
        response = await daemon.SubmitTask(daemon_pb2.SubmitTaskRequest(envelope=envelope))
        state = TaskState.from_submit(response)
        if not state.task_id:
            state.task_id = resolved_task_id
        return state

    async def submit_task(self, *args: Any, **kwargs: Any) -> TaskState:
        return await self.submit(*args, **kwargs)

    async def claim(
        self,
        task_id: str,
        *,
        worker_id: str | None = None,
        lease_duration_ms: int | None = None,
    ) -> TaskState:
        daemon = await self._daemon()
        worker = self._resolve_worker_id(worker_id)
        response = await daemon.ClaimTask(
            daemon_pb2.ClaimTaskRequest(
                task_id=common_pb2.TaskId(value=task_id),
                worker_id=common_pb2.AgentId(value=worker),
                lease_duration_ms=(
                    self._config.default_lease_duration_ms
                    if lease_duration_ms is None
                    else lease_duration_ms
                ),
            )
        )
        return TaskState.from_claim(response)

    async def claim_task(self, *args: Any, **kwargs: Any) -> TaskState:
        return await self.claim(*args, **kwargs)


    async def claim_next(
        self,
        *,
        worker_id: str | None = None,
        accepted_skill_ids: Sequence[str] | None = None,
        accepted_capability_ids: Sequence[str] | None = None,
        lease_duration_ms: int | None = None,
        wait_timeout_ms: int = 0,
    ) -> ClaimedTask:
        daemon = await self._daemon()
        worker = self._resolve_worker_id(worker_id)
        response = await daemon.ClaimNextTask(
            daemon_pb2.ClaimNextTaskRequest(
                worker_id=common_pb2.AgentId(value=worker),
                accepted_skill_ids=list(accepted_skill_ids or []),
                accepted_capability_ids=list(accepted_capability_ids or []),
                lease_duration_ms=(
                    self._config.default_lease_duration_ms
                    if lease_duration_ms is None
                    else lease_duration_ms
                ),
                wait_timeout_ms=wait_timeout_ms,
            )
        )
        return ClaimedTask.from_proto(response)

    async def claim_next_task(self, **kwargs: Any) -> ClaimedTask:
        return await self.claim_next(**kwargs)

    async def heartbeat(
        self,
        task_id: str,
        lease_id: str,
        *,
        worker_id: str | None = None,
        lease_duration_ms: int | None = None,
    ) -> TaskState:
        daemon = await self._daemon()
        worker = self._resolve_worker_id(worker_id)
        response = await daemon.Heartbeat(
            daemon_pb2.HeartbeatRequest(
                task_id=common_pb2.TaskId(value=task_id),
                lease_id=common_pb2.LeaseId(value=lease_id),
                worker_id=common_pb2.AgentId(value=worker),
                lease_duration_ms=(
                    self._config.default_lease_duration_ms
                    if lease_duration_ms is None
                    else lease_duration_ms
                ),
            )
        )
        return TaskState.from_heartbeat(response, task_id=task_id, worker_id=worker)

    async def heartbeat_task(self, *args: Any, **kwargs: Any) -> TaskState:
        return await self.heartbeat(*args, **kwargs)

    async def complete(
        self,
        task_id: str,
        lease_id: str,
        *,
        worker_id: str | None = None,
        duration_ms: int = 0,
        result_metadata: Mapping[str, str] | None = None,
        output_artifacts: Sequence[TaskArtifact | Mapping[str, Any]] | None = None,
    ) -> TaskResult:
        daemon = await self._daemon()
        response = await daemon.CompleteTask(
            daemon_pb2.CompleteTaskRequest(
                task_id=common_pb2.TaskId(value=task_id),
                lease_id=common_pb2.LeaseId(value=lease_id),
                worker_id=common_pb2.AgentId(value=self._resolve_worker_id(worker_id)),
                duration_ms=duration_ms,
                result_metadata=dict(result_metadata or {}),
                output_artifacts=[_artifact_proto(item) for item in (output_artifacts or [])],
            )
        )
        return TaskResult.from_complete(response)

    async def complete_task(self, *args: Any, **kwargs: Any) -> TaskResult:
        return await self.complete(*args, **kwargs)

    async def get_artifact(
        self, artifact_id: str, *, metadata_only: bool = False
    ) -> ArtifactContent:
        daemon = await self._daemon()
        response = await daemon.GetArtifact(
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

    async def fail(
        self,
        task_id: str,
        lease_id: str,
        error_reason: str,
        *,
        worker_id: str | None = None,
        duration_ms: int = 0,
        failure_metadata: Mapping[str, str] | None = None,
    ) -> TaskResult:
        daemon = await self._daemon()
        response = await daemon.FailTask(
            daemon_pb2.FailTaskRequest(
                task_id=common_pb2.TaskId(value=task_id),
                lease_id=common_pb2.LeaseId(value=lease_id),
                worker_id=common_pb2.AgentId(value=self._resolve_worker_id(worker_id)),
                duration_ms=duration_ms,
                error_reason=error_reason,
                failure_metadata=dict(failure_metadata or {}),
            )
        )
        return TaskResult.from_fail(response)

    async def fail_task(self, *args: Any, **kwargs: Any) -> TaskResult:
        return await self.fail(*args, **kwargs)

    async def cancel(
        self,
        task_id: str,
        *,
        reason: str = "",
        metadata: Mapping[str, str] | None = None,
    ) -> TaskResult:
        daemon = await self._daemon()
        response = await daemon.CancelTask(
            daemon_pb2.CancelTaskRequest(
                task_id=common_pb2.TaskId(value=task_id),
                reason=reason,
                metadata=dict(metadata or {}),
            )
        )
        return TaskResult.from_cancel(response)

    async def cancel_task(self, *args: Any, **kwargs: Any) -> TaskResult:
        return await self.cancel(*args, **kwargs)

    # --- AgentAnycast-compatible transition helpers retained from earlier SDK phases. ---

    def on_task(self, handler: TaskHandler | None = None, *, timeout: float | None = None) -> Any:
        def _wrap(fn: TaskHandler) -> TaskHandler:
            if timeout is not None:

                async def _guarded(task: IncomingTask) -> None:
                    await asyncio.wait_for(fn(task), timeout=timeout)

                self._task_handlers.append(_guarded)
                return _guarded

            self._task_handlers.append(fn)
            return fn

        if handler is not None:
            return _wrap(handler)
        return _wrap

    async def start(self) -> None:
        if self._running:
            return
        if self._registration_task is not None:
            raise RuntimeError("node restart is blocked while registration cleanup is pending")
        factory = self._client_factory or DaemonClient
        self._client = factory(
            daemon_endpoint=self._daemon_endpoint,
            registry_endpoint=self._registry_endpoint,
        )
        await self._client.connect()
        self._peer_id = await self._client.local_peer_id()
        if self._card is not None:
            self._card.peer_id = self._peer_id
        self._running = True
        logger.info("KeryxNode started with peer_id=%s", self._peer_id)

    async def stop(self) -> None:
        if not self._running and not self._connected:
            return
        self._serve_stop.set()
        if not self._serve_done.is_set():
            try:
                await asyncio.wait_for(
                    self._serve_done.wait(), timeout=self._shutdown_grace_seconds
                )
            except TimeoutError:
                logger.warning("Keryx worker shutdown exceeded grace period")
        await self.close()
        logger.info("KeryxNode stopped")

    async def serve_forever(self) -> None:
        self._ensure_running()
        if not self._task_handlers:
            raise RuntimeError("serve_forever requires at least one on_task handler")
        if not self._serve_done.is_set():
            raise RuntimeError("serve_forever is already running")
        self._serve_stop.clear()
        self._serve_done.clear()
        workers = [
            asyncio.create_task(
                self._worker_loop(index), name=f"keryx-worker-{index}"
            )
            for index in range(self._worker_concurrency)
        ]
        stop_wait = asyncio.create_task(self._serve_stop.wait())
        try:
            done, _ = await asyncio.wait(
                [stop_wait, *workers], return_when=asyncio.FIRST_COMPLETED
            )
            for task in done:
                if task is stop_wait or task.cancelled():
                    continue
                error = task.exception()
                if error is not None:
                    raise error
        finally:
            self._serve_stop.set()
            stop_wait.cancel()
            for worker in workers:
                worker.cancel()
            await asyncio.gather(stop_wait, *workers, return_exceptions=True)
            self._serve_done.set()


    async def _worker_loop(self, worker_index: int) -> None:
        accepted_skills = [skill.id for skill in (self._card.skills if self._card else [])]
        while not self._serve_stop.is_set():
            try:
                claimed = await self.claim_next(
                    accepted_skill_ids=accepted_skills,
                    wait_timeout_ms=self._claim_wait_timeout_ms,
                )
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                if self._serve_stop.is_set():
                    return
                logger.warning(
                    "Keryx worker %s could not claim work: %s: %s",
                    worker_index,
                    type(exc).__name__,
                    exc,
                )
                await self._wait_or_stop(0.5)
                continue
            if not claimed.has_task:
                continue
            await self._process_claimed_task(claimed)

    async def _process_claimed_task(self, claimed: ClaimedTask) -> None:
        if (
            claimed.envelope is None
            or not claimed.task_id
            or not claimed.lease_id
            or not claimed.worker_id
        ):
            logger.error("Keryx daemon returned an incomplete claimed task")
            return
        started = time.monotonic()

        async def update_remote(
            task_id: str,
            status: LegacyTaskStatus,
            artifacts: list[Artifact] | None,
            error: str | None,
        ) -> None:
            duration_ms = int((time.monotonic() - started) * 1_000)
            if status == LegacyTaskStatus.COMPLETED:
                result_metadata, descriptors = _completion_payload(artifacts)
                await self.complete(
                    task_id,
                    claimed.lease_id,
                    worker_id=claimed.worker_id,
                    duration_ms=duration_ms,
                    result_metadata=result_metadata,
                    output_artifacts=descriptors,
                )
            elif status == LegacyTaskStatus.FAILED:
                await self.fail(
                    task_id,
                    claimed.lease_id,
                    error or "task failed",
                    worker_id=claimed.worker_id,
                    duration_ms=duration_ms,
                )

        incoming = IncomingTask(
            _task_from_claim(claimed),
            None,
            update_remote,
            lease_id=claimed.lease_id,
            worker_id=claimed.worker_id,
            sender_peer_id=claimed.sender_peer_id,
        )
        heartbeat = asyncio.create_task(self._heartbeat_claim(claimed, incoming))
        terminal_wait: asyncio.Task[Any] | None = None
        stop_wait: asyncio.Task[Any] | None = None
        try:
            try:
                for handler in tuple(self._task_handlers):
                    await handler(incoming)
                    if incoming.is_terminal:
                        break
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                if not incoming.is_terminal:
                    try:
                        await incoming.fail(f"{type(exc).__name__}: {exc}")
                    except Exception:
                        logger.exception(
                            "Keryx could not persist handler failure for task %s",
                            claimed.task_id,
                        )
                return

            if incoming.is_terminal:
                return
            terminal_wait = asyncio.create_task(incoming.wait_terminal())
            stop_wait = asyncio.create_task(self._serve_stop.wait())
            done, _ = await asyncio.wait(
                [terminal_wait, stop_wait, heartbeat],
                return_when=asyncio.FIRST_COMPLETED,
            )
            if heartbeat in done and not incoming.is_terminal:
                error = heartbeat.exception()
                if error is not None:
                    logger.error(
                        "Keryx lease heartbeat failed for task %s: %s: %s",
                        claimed.task_id,
                        type(error).__name__,
                        error,
                    )
        finally:
            for task in (terminal_wait, stop_wait, heartbeat):
                if task is not None and not task.done():
                    task.cancel()
            await asyncio.gather(
                *(task for task in (terminal_wait, stop_wait, heartbeat) if task is not None),
                return_exceptions=True,
            )

    async def _heartbeat_claim(
        self, claimed: ClaimedTask, incoming: IncomingTask
    ) -> None:
        lease_ttl_ms = max(
            1_000,
            claimed.expires_at_ms - claimed.leased_at_ms,
            self._config.default_lease_duration_ms,
        )
        interval_ms = self._heartbeat_interval_ms or max(250, lease_ttl_ms // 3)
        while not incoming.is_terminal and not self._serve_stop.is_set():
            if await self._wait_or_stop(interval_ms / 1_000):
                return
            if incoming.is_terminal:
                return
            await self.heartbeat(
                claimed.task_id,
                claimed.lease_id,
                worker_id=claimed.worker_id,
                lease_duration_ms=lease_ttl_ms,
            )

    async def _wait_or_stop(self, timeout_seconds: float) -> bool:
        try:
            await asyncio.wait_for(
                self._serve_stop.wait(), timeout=max(0.0, timeout_seconds)
            )
            return True
        except TimeoutError:
            return False

    async def list_peers(self) -> list[dict[str, Any]]:
        if self._client is not None and self._running:
            return [
                {"peer_id": peer.peer_id, "connected": peer.connected, "local": peer.local}
                for peer in await self._client.list_peers()
            ]
        return await self.peers()

    async def local_peer_id(self) -> str:
        peers = await self.peers()
        for peer in peers:
            if peer.get("local"):
                return str(peer.get("peer_id") or "")
        if peers:
            return str(peers[0].get("peer_id") or "")
        raise RuntimeError("daemon did not report a local peer id")

    async def discover(
        self,
        skill: str,
        *,
        tags: Mapping[str, str] | None = None,
        limit: int = 10,
    ) -> list[dict[str, Any]]:
        if self._client is not None and self._running:
            tag_list = list(tags.values()) if tags else []
            return await self._client.discover(skill, tags=tag_list, limit=limit)
        return await self.skills(skill, tags=tags, limit=limit)

    async def get_card(self, peer_id: str) -> AgentCard:
        self._ensure_running()
        assert self._client is not None
        return await self._client.get_card(peer_id)

    async def send_task(
        self,
        message: dict[str, Any] | Message,
        *,
        peer_id: str | None = None,
        skill: str | None = None,
        url: str | None = None,
        metadata: dict[str, str] | None = None,
        deadline_ms: int = 0,
    ) -> TaskHandle:
        self._ensure_running()
        assert self._client is not None
        targets = sum(item is not None for item in (peer_id, skill, url))
        if targets != 1:
            raise ValueError("Exactly one of peer_id, skill, or url must be provided")
        resolved_skill = skill
        if skill is not None:
            discovered = await self.discover(skill, limit=1)
            if not discovered:
                raise RuntimeError(f"no agents found for skill {skill}")
            peer_id = discovered[0]["peer_id"]
        if url is not None:
            raise NotImplementedError("HTTP bridge outbound is not implemented in keryx-py yet")
        assert peer_id is not None
        text = _message_text(message)
        task_id = str(uuid.uuid4())
        try:
            response = await self._client.send_task(
                target_peer_id=peer_id,
                task_id=task_id,
                message_text=text,
                metadata=metadata,
                deadline_ms=deadline_ms,
            )
        except Exception as exc:
            if resolved_skill is not None and _is_unknown_peer_error(exc):
                raise NotImplementedError(
                    "Keryx discovered a peer for skill "
                    f"{resolved_skill!r} ({peer_id}), but the local daemon cannot route "
                    "tasks to registry-discovered peers yet. Cross-node Keryx task "
                    "delivery requires a relay-backed daemon route / task publisher."
                ) from exc
            raise
        task = Task(task_id=response.task_id.value or task_id, status=LegacyTaskStatus.SUBMITTED)

        async def refresh_remote() -> Task:
            assert self._client is not None
            result_response = await self._client.get_task_result(task.task_id)
            task.status = _legacy_status(result_response.status)
            if result_response.found and result_response.HasField("result"):
                result = result_response.result
                task.status = _legacy_status(result_response.status, outcome=result.outcome)
                artifacts: list[Artifact] = []
                result_text = result.result_metadata.get("result_text", "")
                for item in result.output_artifacts:
                    preview = item.metadata.get("text_preview", "")
                    parts = [Part(text=preview, media_type=item.media_type or "text/plain")] if preview else []
                    artifacts.append(
                        Artifact(
                            artifact_id=(
                                item.artifact_id.value
                                if item.HasField("artifact_id")
                                else ""
                            ),
                            name=item.path,
                            parts=parts,
                        )
                    )
                if result_text and not artifacts:
                    artifacts.append(
                        Artifact(
                            name="result",
                            parts=[Part(text=result_text, media_type="text/plain")],
                        )
                    )
                task.artifacts = artifacts
                task.metadata = {
                    **dict(task.metadata or {}),
                    **{str(key): str(value) for key, value in result.result_metadata.items()},
                    "executor_peer_id": result.executor_peer_id,
                    "duration_ms": str(result.duration_ms),
                    "error_reason": result.error_reason,
                }
            return task

        async def cancel_remote() -> None:
            assert self._client is not None
            await self._client.cancel_task(task.task_id, reason="canceled by TaskHandle")

        return TaskHandle(
            task=task,
            receipt=SubmissionReceipt(
                task_id=task.task_id,
                status=response.status,
                routed_to=response.routed_to,
                delivery_route=response.delivery_route,
            ),
            refresh_fn=refresh_remote,
            cancel_fn=cancel_remote,
        )

    async def register_skills(
        self,
        card: AgentCard | None = None,
        *,
        capacity: int | None = None,
        current_load: int = 0,
        ttl_seconds: int = 300,
    ) -> dict[str, Any]:
        self._ensure_running()
        ttl_seconds = _validate_registration_ttl(ttl_seconds)
        assert self._client is not None
        active_card = card or self._card
        if active_card is None:
            raise RuntimeError("register_skills requires an AgentCard")
        accepted = await self._client.register_skills(
            peer_id=self.peer_id,
            name=active_card.name,
            description=active_card.description,
            skills=[
                (skill.id, skill.description, list(skill.tags))
                for skill in active_card.skills
            ],
            ttl_seconds=ttl_seconds,
        )
        return {
            "accepted": accepted,
            "peer_id": self.peer_id,
            "capacity": capacity,
            "current_load": current_load,
        }

    async def deregister_skills(self, card: AgentCard | None = None) -> dict[str, Any]:
        self._ensure_running()
        assert self._client is not None
        active_card = card or self._card
        if active_card is None:
            raise RuntimeError("deregister_skills requires an AgentCard")
        skill_ids = [skill.id for skill in active_card.skills]
        accepted = await self._client.unregister_skills(peer_id=self.peer_id, skill_ids=skill_ids)
        return {"accepted": accepted, "peer_id": self.peer_id}

    async def start_registration(
        self,
        card: AgentCard | None = None,
        *,
        capacity: int | None = None,
        current_load: int = 0,
        ttl_seconds: int = 300,
        refresh_interval_seconds: float | None = None,
    ) -> dict[str, Any]:
        """Register skills now and keep their TTL alive until stopped."""

        async with self._registration_lock:
            return await self._start_registration_unlocked(
                card,
                capacity=capacity,
                current_load=current_load,
                ttl_seconds=ttl_seconds,
                refresh_interval_seconds=refresh_interval_seconds,
            )

    async def _start_registration_unlocked(
        self,
        card: AgentCard | None = None,
        *,
        capacity: int | None = None,
        current_load: int = 0,
        ttl_seconds: int = 300,
        refresh_interval_seconds: float | None = None,
    ) -> dict[str, Any]:
        self._ensure_running()
        source_card = card or self._card
        if source_card is None:
            raise RuntimeError("start_registration requires an AgentCard")
        active_card = AgentCard.from_dict(source_card.to_dict())
        ttl_seconds = _validate_registration_ttl(ttl_seconds)
        refresh_interval = (
            ttl_seconds / 2 if refresh_interval_seconds is None else refresh_interval_seconds
        )
        if (
            isinstance(refresh_interval, bool)
            or not isinstance(refresh_interval, (int, float))
            or not math.isfinite(refresh_interval)
            or refresh_interval <= 0
            or refresh_interval >= ttl_seconds
        ):
            raise ValueError("refresh_interval_seconds must be positive and less than ttl_seconds")
        if self._registration_task is not None:
            raise RuntimeError("skill registration lifecycle is already running")

        result = await self.register_skills(
            active_card,
            capacity=capacity,
            current_load=current_load,
            ttl_seconds=ttl_seconds,
        )
        if not result["accepted"]:
            raise RuntimeError("skill registration was rejected")

        self._registration_last_error = None
        self._registration_consecutive_failures = 0
        self._registration_last_success_ms = int(time.time() * 1_000)
        self._registration_stop.clear()
        self._registration_card = active_card
        self._registration_task = asyncio.create_task(
            self._registration_refresh_loop(
                active_card,
                capacity=capacity,
                current_load=current_load,
                ttl_seconds=ttl_seconds,
                refresh_interval_seconds=refresh_interval,
            ),
            name="keryx-skill-registration-refresh",
        )
        return result

    def registration_status(self) -> dict[str, Any]:
        """Return a bounded snapshot of TTL registration lifecycle health."""

        active = self._registration_task is not None
        if self._registration_last_error:
            state = "degraded"
        elif not active:
            state = "inactive"
        else:
            state = "healthy"
        return {
            "active": active,
            "state": state,
            "cleanup_pending": self._registration_cleanup_task is not None,
            "last_error": self._registration_last_error,
            "consecutive_failures": self._registration_consecutive_failures,
            "last_success_ms": self._registration_last_success_ms,
        }

    async def stop_registration(self) -> dict[str, Any] | None:
        """Stop TTL refresh and deregister the exact active skill set."""

        async with self._registration_lock:
            return await self._stop_registration_unlocked()

    async def _stop_registration_unlocked(self) -> dict[str, Any] | None:
        refresh_task = self._registration_task
        active_card = self._registration_card
        if refresh_task is None or active_card is None:
            return None
        if self._registration_cleanup_task is not None:
            return self._registration_pending_result()
        client = self._client
        if client is None:
            raise RuntimeError("registration lifecycle client is unavailable")
        peer_id = self.peer_id
        deadline = (
            asyncio.get_running_loop().time()
            + self._registration_stop_timeout_seconds
        )

        self._registration_stop.set()
        refresh_task.cancel()
        if not await self._wait_for_registration_task(refresh_task, deadline):
            self._mark_registration_cleanup_pending(
                "registration refresh cancellation acknowledgement timed out; "
                "deregistration is pending"
            )
            self._registration_cleanup_task = asyncio.create_task(
                self._finish_registration_stop(
                    refresh_task,
                    None,
                    active_card,
                    client,
                    peer_id,
                ),
                name="keryx-skill-registration-cleanup",
            )
            return self._registration_pending_result()

        await asyncio.gather(refresh_task, return_exceptions=True)
        deregistration_task = self._start_registration_deregistration(
            client, active_card, peer_id
        )
        if not await self._wait_for_registration_task(deregistration_task, deadline):
            self._mark_registration_cleanup_pending(
                "skill deregistration exceeded the registration stop bound; "
                "cleanup is pending"
            )
            self._registration_cleanup_task = asyncio.create_task(
                self._finish_registration_stop(
                    refresh_task,
                    deregistration_task,
                    active_card,
                    client,
                    peer_id,
                ),
                name="keryx-skill-registration-cleanup",
            )
            return self._registration_pending_result()
        return await self._complete_registration_stop_unlocked(
            deregistration_task, peer_id
        )

    async def _finish_registration_stop(
        self,
        refresh_task: asyncio.Task[None],
        deregistration_task: asyncio.Task[bool] | None,
        active_card: AgentCard,
        client: Any,
        peer_id: str,
    ) -> None:
        await asyncio.gather(refresh_task, return_exceptions=True)
        if deregistration_task is None:
            deregistration_task = self._start_registration_deregistration(
                client, active_card, peer_id
            )
        await asyncio.gather(deregistration_task, return_exceptions=True)

        close_client = False
        async with self._registration_lock:
            close_client = self._registration_close_client_after_cleanup
            try:
                if (
                    self._registration_task is refresh_task
                    and self._registration_card is active_card
                ):
                    try:
                        await self._complete_registration_stop_unlocked(
                            deregistration_task, peer_id
                        )
                    except Exception:
                        logger.warning(
                            "Unable to deregister skills after delayed refresh shutdown",
                            exc_info=True,
                        )
                        if close_client:
                            self._registration_task = None
                            self._registration_card = None
            finally:
                self._registration_cleanup_task = None
                self._registration_close_client_after_cleanup = False

        if close_client:
            try:
                await client.close()
            except Exception:
                logger.warning(
                    "Unable to close registration client after delayed cleanup",
                    exc_info=True,
                )

    def _start_registration_deregistration(
        self, client: Any, active_card: AgentCard, peer_id: str
    ) -> asyncio.Task[bool]:
        return asyncio.create_task(
            client.unregister_skills(
                peer_id=peer_id,
                skill_ids=[skill.id for skill in active_card.skills],
            ),
            name="keryx-skill-deregistration",
        )

    async def _wait_for_registration_task(
        self, task: asyncio.Task[Any], deadline: float
    ) -> bool:
        if task.done():
            return True
        remaining = max(0.0, deadline - asyncio.get_running_loop().time())
        done, _ = await asyncio.wait({task}, timeout=remaining)
        return task in done

    def _mark_registration_cleanup_pending(self, error: str) -> None:
        self._registration_last_error = error[:512]
        self._registration_consecutive_failures += 1

    def _registration_pending_result(self) -> dict[str, Any]:
        return {
            "accepted": False,
            "peer_id": self.peer_id,
            "cleanup_pending": True,
        }

    async def _complete_registration_stop_unlocked(
        self, deregistration_task: asyncio.Task[bool], peer_id: str
    ) -> dict[str, Any]:
        try:
            accepted = await deregistration_task
            if not accepted:
                raise RuntimeError("skill deregistration was rejected")
        except Exception as exc:
            self._registration_last_error = f"{type(exc).__name__}: {exc}"[:512]
            self._registration_consecutive_failures += 1
            raise
        self._registration_task = None
        self._registration_card = None
        self._registration_last_error = None
        self._registration_consecutive_failures = 0
        return {"accepted": True, "peer_id": peer_id}

    async def _registration_refresh_loop(
        self,
        card: AgentCard,
        *,
        capacity: int | None,
        current_load: int,
        ttl_seconds: int,
        refresh_interval_seconds: float,
    ) -> None:
        while not self._registration_stop.is_set():
            try:
                await asyncio.wait_for(
                    self._registration_stop.wait(), timeout=refresh_interval_seconds
                )
                return
            except TimeoutError:
                pass
            try:
                result = await self.register_skills(
                    card,
                    capacity=capacity,
                    current_load=current_load,
                    ttl_seconds=ttl_seconds,
                )
                if not result["accepted"]:
                    self._registration_last_error = "registration refresh was rejected"
                    self._registration_consecutive_failures += 1
                    logger.warning("Keryx skill registration refresh was rejected")
                else:
                    self._registration_last_error = None
                    self._registration_consecutive_failures = 0
                    self._registration_last_success_ms = int(time.time() * 1_000)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self._registration_last_error = f"{type(exc).__name__}: {exc}"[:512]
                self._registration_consecutive_failures += 1
                logger.warning("Keryx skill registration refresh failed", exc_info=True)

    async def _daemon(self) -> Any:
        if not self._connected or self._daemon_stub is None:
            await self.connect()
        assert self._daemon_stub is not None
        return self._daemon_stub

    def _resolve_worker_id(self, worker_id: str | None) -> str:
        resolved = worker_id or self._worker_id or self._peer_id
        if not resolved:
            raise ValueError("worker_id is required (or set HERMES_KERYX_WORKER_ID)")
        return resolved

    def _ensure_running(self) -> None:
        if not self._running or self._client is None:
            raise RuntimeError("Node not started. Call await node.start() first.")



def _legacy_status(status: str, *, outcome: int = 0) -> LegacyTaskStatus:
    normalized = status.strip().lower()
    if normalized == "completed":
        return LegacyTaskStatus.COMPLETED
    if normalized in {"failed", "dead_lettered", "timed_out"}:
        return LegacyTaskStatus.FAILED
    if normalized == "canceled":
        return LegacyTaskStatus.CANCELED
    if normalized == "rejected":
        return LegacyTaskStatus.REJECTED
    if normalized in {"running", "working", "leased"}:
        return LegacyTaskStatus.WORKING
    if outcome == 1:
        return LegacyTaskStatus.COMPLETED
    if outcome in {2, 4}:
        return LegacyTaskStatus.FAILED
    if outcome == 3:
        return LegacyTaskStatus.CANCELED
    if outcome == 5:
        return LegacyTaskStatus.REJECTED
    return LegacyTaskStatus.SUBMITTED


def _task_from_claim(claimed: ClaimedTask) -> Task:
    envelope = claimed.envelope
    if envelope is None:
        raise ValueError("claimed task is missing its envelope")
    metadata = {str(key): str(value) for key, value in envelope.metadata.items()}
    context_id = (
        envelope.correlation_id.value
        if envelope.HasField("correlation_id")
        else ""
    )
    messages = []
    for proto_message in envelope.messages:
        message_metadata = {
            str(key): str(value) for key, value in proto_message.metadata.items()
        }
        messages.append(
            Message(
                role=message_metadata.get("role", "user"),
                parts=[
                    Part(
                        text=part.text or None,
                        raw=bytes(part.raw),
                        media_type=part.media_type or "text/plain",
                        metadata={
                            str(key): str(value)
                            for key, value in part.metadata.items()
                        },
                    )
                    for part in proto_message.parts
                ],
                metadata=message_metadata,
            )
        )
    target_skill_id = next(
        (
            metadata[key]
            for key in ("skill", "skill_id", "target_skill_id")
            if metadata.get(key)
        ),
        "",
    )
    return Task(
        task_id=claimed.task_id,
        context_id=context_id,
        status=LegacyTaskStatus.WORKING,
        messages=messages,
        target_skill_id=target_skill_id,
        originator_peer_id=claimed.sender_peer_id,
        metadata=metadata,
    )


def _completion_payload(
    artifacts: list[Artifact] | None,
) -> tuple[dict[str, str], list[TaskArtifact]]:
    descriptors: list[TaskArtifact] = []
    result_texts: list[str] = []
    for index, artifact in enumerate(artifacts or [], start=1):
        text = "\n".join(part.text for part in artifact.parts if part.text)
        raw_parts = [part for part in artifact.parts if part.raw]
        if len(raw_parts) > 1:
            raise ValueError("artifact contains multiple raw parts")
        content = raw_parts[0].raw if raw_parts else None
        metadata: dict[str, str] = {}
        if artifact.artifact_id:
            metadata["artifact_id"] = artifact.artifact_id
        if text:
            metadata["text_preview"] = text[:4_096]
            result_texts.append(text)
        descriptors.append(
            TaskArtifact(
                path=artifact.name or artifact.artifact_id or f"artifact-{index}",
                media_type=(
                    raw_parts[0].media_type
                    if raw_parts
                    else "text/plain" if text else "application/octet-stream"
                ),
                metadata=metadata,
                content=content,
            )
        )
    result_metadata: dict[str, str] = {}
    if result_texts:
        result_metadata["result_text"] = "\n\n".join(result_texts)[:65_536]
    return result_metadata, descriptors

def _task_envelope(
    *,
    task_id: str,
    message: str | Message | Mapping[str, Any] | None,
    messages: Sequence[str | Message | Mapping[str, Any]] | None,
    metadata: Mapping[str, str] | None,
    correlation_id: str | None,
    idempotency_key: str | None,
) -> task_pb2.TaskEnvelope:
    envelope = task_pb2.TaskEnvelope(
        task_id=common_pb2.TaskId(value=task_id),
        status=task_pb2.TASK_STATUS_CREATED,
        metadata=dict(metadata or {}),
    )
    if correlation_id:
        envelope.correlation_id.CopyFrom(common_pb2.CorrelationId(value=correlation_id))
    if idempotency_key:
        envelope.idempotency_key.CopyFrom(common_pb2.IdempotencyKey(value=idempotency_key))
    if messages is not None:
        envelope.messages.extend(_task_message(item) for item in messages)
    elif message is not None:
        envelope.messages.append(_task_message(message))
    return envelope


def _task_message(message: str | Message | Mapping[str, Any]) -> task_pb2.TaskMessage:
    if isinstance(message, str):
        return task_pb2.TaskMessage(
            parts=[task_pb2.TaskMessagePart(media_type="text/plain", text=message)]
        )
    if isinstance(message, Message):
        return task_pb2.TaskMessage(
            parts=[
                task_pb2.TaskMessagePart(media_type="text/plain", text=part.text or "")
                for part in message.parts
            ]
        )
    parts = message.get("parts", [])
    metadata = dict(message.get("metadata", {}) or {})
    return task_pb2.TaskMessage(
        parts=[_message_part(part) for part in parts],
        metadata=metadata,
    )


def _message_part(part: Any) -> task_pb2.TaskMessagePart:
    if isinstance(part, Part):
        return task_pb2.TaskMessagePart(media_type="text/plain", text=part.text or "")
    if isinstance(part, str):
        return task_pb2.TaskMessagePart(media_type="text/plain", text=part)
    return task_pb2.TaskMessagePart(
        media_type=str(part.get("media_type") or "text/plain"),
        text=str(part.get("text") or ""),
        raw=part.get("raw") or b"",
        metadata=dict(part.get("metadata", {}) or {}),
    )


def _artifact_proto(item: TaskArtifact | Mapping[str, Any]) -> daemon_pb2.TaskArtifact:
    if isinstance(item, TaskArtifact):
        return item.to_proto()
    return TaskArtifact(
        path=str(item.get("path") or ""),
        media_type=str(item.get("media_type") or "application/octet-stream"),
        metadata=dict(item.get("metadata", {}) or {}),
    ).to_proto()


def _tag_list(tags: Sequence[str] | Mapping[str, str] | None) -> list[str]:
    if tags is None:
        return []
    if isinstance(tags, Mapping):
        return [str(value) for value in tags.values()]
    return [str(value) for value in tags]



def _is_unknown_peer_error(exc: BaseException) -> bool:
    """Return True for Keryx daemon NOT_FOUND unknown-peer routing errors."""

    code = getattr(exc, "code", None)
    details = getattr(exc, "details", None)
    try:
        status_code = code() if callable(code) else code
    except Exception:
        status_code = None
    try:
        detail_text = details() if callable(details) else details
    except Exception:
        detail_text = None
    text = " ".join(str(part) for part in (detail_text, exc) if part)
    return status_code == grpc.StatusCode.NOT_FOUND and "unknown peer" in text.lower()

def _message_text(message: dict[str, Any] | Message) -> str:
    if isinstance(message, dict):
        parts = message.get("parts") or []
        if parts and isinstance(parts[0], dict):
            return str(parts[0].get("text") or "")
        if parts:
            return str(parts[0])
        return ""
    return (message.parts[0].text if message.parts else "") or ""


def _proto_to_dict(message: Any) -> dict[str, Any]:
    if is_dataclass(message) and not isinstance(message, type):
        return asdict(message)
    result: dict[str, Any] = {}
    for field in message.DESCRIPTOR.fields:
        value = getattr(message, field.name)
        if field.is_repeated:
            if field.message_type is not None and field.message_type.GetOptions().map_entry:
                result[field.name] = dict(value)
            else:
                result[field.name] = [_proto_scalar(item) for item in value]
        elif field.message_type is not None:
            if field.has_presence and not message.HasField(field.name):
                result[field.name] = None
            else:
                result[field.name] = _proto_to_dict(value)
        elif field.has_presence and not message.HasField(field.name):
            result[field.name] = None
        else:
            result[field.name] = value
    return result


def _proto_scalar(value: Any) -> Any:
    if hasattr(value, "DESCRIPTOR"):
        return _proto_to_dict(value)
    return value
