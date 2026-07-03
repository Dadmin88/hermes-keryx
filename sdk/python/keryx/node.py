"""KeryxNode — Python entry point matching the AgentAnycast Node contract."""

from __future__ import annotations

import asyncio
import logging
import os
import uuid
from collections.abc import Awaitable, Callable, Coroutine
from pathlib import Path
from typing import Any

from keryx.card import AgentCard
from keryx.client import DaemonClient
from keryx.task import IncomingTask, Message, Part, Task, TaskHandle, TaskStatus

logger = logging.getLogger(__name__)

TaskHandler = Callable[[IncomingTask], Awaitable[None]]


class KeryxNode:
    def __init__(
        self,
        card: AgentCard,
        *,
        relay_endpoint: str | None = None,
        relay: str | None = None,
        daemon_endpoint: str | None = None,
        daemon_addr: str | None = None,
        registry_endpoint: str | None = None,
        home: str | Path | None = None,
        daemon_bin: str | Path | None = None,
        status_callback: Callable[[str], None] | None = None,
        client_factory: Callable[..., DaemonClient] | type[DaemonClient] | None = None,
        **_ignored: Any,
    ) -> None:
        self._card = card
        self._relay = relay_endpoint or relay
        self._home = Path(home).expanduser() if home is not None else None
        self._daemon_bin = daemon_bin
        self._status_callback = status_callback
        default_daemon = os.environ.get(
            "HERMES_KERYX_DAEMON_ENDPOINT",
            "unix:///tmp/keryx-daemon.sock",
        )
        self._daemon_endpoint = daemon_endpoint or daemon_addr or default_daemon
        self._registry_endpoint = registry_endpoint or os.environ.get("HERMES_KERYX_REGISTRY_ENDPOINT")
        self._client_factory = client_factory or DaemonClient
        self._client: DaemonClient | None = None
        self._peer_id: str | None = None
        self._running = False
        self._serve_stop = asyncio.Event()
        self._task_handlers: list[TaskHandler] = []

    @property
    def peer_id(self) -> str:
        if not self._peer_id:
            raise RuntimeError("Node not started. Call await node.start() first.")
        return self._peer_id

    @property
    def card(self) -> AgentCard:
        return self._card

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
        if self._status_callback:
            self._status_callback("Connecting to Keryx daemon")
        factory = self._client_factory or DaemonClient
        self._client = factory(
            daemon_endpoint=self._daemon_endpoint,
            registry_endpoint=self._registry_endpoint,
        )
        await self._client.connect()
        self._peer_id = await self._client.local_peer_id()
        self._card.peer_id = self._peer_id
        self._running = True
        logger.info("KeryxNode started with peer_id=%s", self._peer_id)

    async def stop(self) -> None:
        if not self._running:
            return
        self._serve_stop.set()
        if self._client is not None:
            await self._client.close()
            self._client = None
        self._running = False
        logger.info("KeryxNode stopped")

    async def serve_forever(self) -> None:
        self._ensure_running()
        self._serve_stop.clear()
        while not self._serve_stop.is_set():
            await asyncio.sleep(0.25)

    async def list_peers(self) -> list[dict[str, Any]]:
        self._ensure_running()
        assert self._client is not None
        return [
            {"peer_id": peer.peer_id, "connected": peer.connected, "local": peer.local}
            for peer in await self._client.list_peers()
        ]

    async def discover(
        self,
        skill: str,
        *,
        tags: dict[str, str] | None = None,
        limit: int = 10,
    ) -> list[dict[str, Any]]:
        self._ensure_running()
        assert self._client is not None
        tag_list = list(tags.values()) if tags else []
        return await self._client.discover(skill, tags=tag_list, limit=limit)

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
    ) -> TaskHandle:
        self._ensure_running()
        assert self._client is not None
        targets = sum(item is not None for item in (peer_id, skill, url))
        if targets != 1:
            raise ValueError("Exactly one of peer_id, skill, or url must be provided")
        if skill is not None:
            discovered = await self.discover(skill, limit=1)
            if not discovered:
                raise RuntimeError(f"no agents found for skill {skill}")
            peer_id = discovered[0]["peer_id"]
        if url is not None:
            raise NotImplementedError("HTTP bridge outbound is not implemented in keryx-py yet")
        assert peer_id is not None
        if isinstance(message, dict):
            text = ""
            parts = message.get("parts") or []
            if parts and isinstance(parts[0], dict):
                text = str(parts[0].get("text") or "")
        else:
            text = (message.parts[0].text if message.parts else "") or ""
        task_id = str(uuid.uuid4())
        response = await self._client.send_task(
            target_peer_id=peer_id,
            task_id=task_id,
            message_text=text,
            metadata=metadata,
        )
        task = Task(task_id=response.task_id.value or task_id, status=TaskStatus.SUBMITTED)
        return TaskHandle(task=task)

    async def register_skills(
        self,
        card: AgentCard | None = None,
        *,
        capacity: int | None = None,
        current_load: int = 0,
        ttl_seconds: int = 300,
    ) -> dict[str, Any]:
        self._ensure_running()
        assert self._client is not None
        active_card = card or self._card
        accepted = await self._client.register_skills(
            peer_id=self.peer_id,
            name=active_card.name,
            description=active_card.description,
            skills=[(skill.id, skill.description) for skill in active_card.skills],
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
        skill_ids = [skill.id for skill in active_card.skills]
        accepted = await self._client.unregister_skills(peer_id=self.peer_id, skill_ids=skill_ids)
        return {"accepted": accepted, "peer_id": self.peer_id}

    def _ensure_running(self) -> None:
        if not self._running or self._client is None:
            raise RuntimeError("Node not started. Call await node.start() first.")