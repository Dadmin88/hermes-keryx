from __future__ import annotations

import asyncio
import math
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from hermes.keryx.v1 import registry_pb2

from keryx import AgentCard, KeryxNode, Skill
from keryx.client import REGISTRY_RPC_TIMEOUT_SECONDS, DaemonClient


async def wait_until(predicate, *, timeout: float = 1.0) -> None:
    deadline = asyncio.get_running_loop().time() + timeout
    while not predicate():
        if asyncio.get_running_loop().time() >= deadline:
            raise TimeoutError("condition was not met")
        await asyncio.sleep(0.002)


def test_skill_tags_round_trip() -> None:
    skill = Skill(id="backend", description="Backend work", tags=["python", "linux"])

    assert skill.to_dict()["tags"] == ["python", "linux"]
    assert Skill.from_dict(skill.to_dict()) == skill


@pytest.mark.parametrize("tags", ["python", ["python", 1]])
def test_skill_rejects_malformed_tags(tags: object) -> None:
    with pytest.raises(ValueError, match="tags"):
        Skill.from_dict({"id": "backend", "tags": tags})


@pytest.mark.parametrize("tags", ["python", ("python",), ["python", 1], [""]])
def test_skill_constructor_rejects_malformed_tags(tags: object) -> None:
    with pytest.raises(ValueError, match="tags"):
        Skill(id="backend", tags=tags)  # type: ignore[arg-type]


class RegistryStub:
    def __init__(self) -> None:
        self.requests: list[registry_pb2.RegisterSkillsRequest] = []
        self.timeouts: list[float | None] = []
        self.unregister_requests: list[registry_pb2.UnregisterSkillsRequest] = []
        self.unregister_timeouts: list[float | None] = []

    async def RegisterSkills(
        self,
        request: registry_pb2.RegisterSkillsRequest,
        *,
        timeout: float | None = None,
    ) -> registry_pb2.RegisterSkillsResponse:
        self.requests.append(request)
        self.timeouts.append(timeout)
        return registry_pb2.RegisterSkillsResponse(accepted=True)

    async def UnregisterSkills(
        self,
        request: registry_pb2.UnregisterSkillsRequest,
        *,
        timeout: float | None = None,
    ) -> registry_pb2.UnregisterSkillsResponse:
        self.unregister_requests.append(request)
        self.unregister_timeouts.append(timeout)
        return registry_pb2.UnregisterSkillsResponse(accepted=True)

    async def DiscoverBySkill(
        self, _request: registry_pb2.DiscoverBySkillRequest
    ) -> registry_pb2.DiscoverBySkillResponse:
        return registry_pb2.DiscoverBySkillResponse(
            registrations=[
                registry_pb2.Registration(
                    peer_id="peer-worker",
                    name="worker",
                    skills=[
                        registry_pb2.SkillInfo(
                            skill_id="backend",
                            description="Backend work",
                            tags=["python", "linux"],
                        )
                    ],
                )
            ]
        )


@pytest.mark.asyncio
async def test_client_registration_serializes_skill_tags() -> None:
    registry = RegistryStub()
    client = DaemonClient(daemon_endpoint="unix:///tmp/keryx-unused.sock")
    client._registry = registry

    accepted = await client.register_skills(
        peer_id="peer-worker",
        name="worker",
        description="Worker",
        skills=[("backend", "Backend work", ["python", "linux"])],
        ttl_seconds=120,
    )

    assert accepted is True
    assert registry.timeouts == [REGISTRY_RPC_TIMEOUT_SECONDS]
    assert list(registry.requests[0].skills[0].tags) == ["python", "linux"]

    unregistered = await client.unregister_skills(
        peer_id="peer-worker", skill_ids=["backend"]
    )
    assert unregistered is True
    assert registry.unregister_timeouts == [REGISTRY_RPC_TIMEOUT_SECONDS]


@pytest.mark.asyncio
@pytest.mark.parametrize("ttl_seconds", [True, 1.5, 0, 2**64])
async def test_one_shot_registration_rejects_invalid_ttl(ttl_seconds: object) -> None:
    registry = RegistryStub()
    client = DaemonClient(daemon_endpoint="unix:///tmp/keryx-unused.sock")
    client._registry = registry

    with pytest.raises(ValueError, match="ttl_seconds"):
        await client.register_skills(
            peer_id="peer-worker",
            name="worker",
            description="Worker",
            skills=[("backend", "Backend work", [])],
            ttl_seconds=ttl_seconds,  # type: ignore[arg-type]
        )
    assert registry.requests == []

    fake_client = SimpleNamespace(register_skills=AsyncMock(return_value=True))
    node = KeryxNode(AgentCard(name="worker", skills=[Skill(id="backend")]))
    node._client = fake_client
    node._peer_id = "peer-worker"
    node._running = True
    with pytest.raises(ValueError, match="ttl_seconds"):
        await node.register_skills(ttl_seconds=ttl_seconds)  # type: ignore[arg-type]
    fake_client.register_skills.assert_not_awaited()


@pytest.mark.parametrize(
    "timeout_seconds", [True, "1", 0, -1, float("nan"), float("inf")]
)
def test_registration_stop_timeout_requires_positive_finite_number(
    timeout_seconds: object,
) -> None:
    with pytest.raises(ValueError, match="registration_stop_timeout_seconds"):
        KeryxNode(
            registration_stop_timeout_seconds=timeout_seconds,  # type: ignore[arg-type]
        )


@pytest.mark.asyncio
async def test_get_card_restores_skill_tags() -> None:
    client = DaemonClient(daemon_endpoint="unix:///tmp/keryx-unused.sock")
    client._registry = RegistryStub()

    card = await client.get_card("peer-worker")

    assert card.skills[0].tags == ["python", "linux"]


@pytest.mark.asyncio
async def test_registration_lifecycle_refreshes_then_deregisters() -> None:
    client = SimpleNamespace(
        register_skills=AsyncMock(return_value=True),
        unregister_skills=AsyncMock(return_value=True),
        close=AsyncMock(),
    )
    card = AgentCard(
        name="worker",
        description="Worker",
        skills=[Skill(id="backend", tags=["python", "linux"])],
    )
    node = KeryxNode(card)
    node._client = client
    node._peer_id = "peer-worker"
    node._running = True

    await node.start_registration(ttl_seconds=2, refresh_interval_seconds=0.01)
    for _ in range(50):
        if client.register_skills.await_count >= 2:
            break
        await asyncio.sleep(0.005)
    assert client.register_skills.await_count >= 2

    await node.stop_registration()
    refresh_count = client.register_skills.await_count
    await asyncio.sleep(0.02)

    assert client.register_skills.await_count == refresh_count
    assert client.register_skills.await_args.kwargs["skills"] == [
        ("backend", "", ["python", "linux"])
    ]
    client.unregister_skills.assert_awaited_once_with(
        peer_id="peer-worker", skill_ids=["backend"]
    )


@pytest.mark.asyncio
async def test_close_stops_registration_before_closing_client() -> None:
    events: list[str] = []

    async def unregister_skills(**_kwargs: object) -> bool:
        events.append("deregister")
        return True

    async def close_client() -> None:
        events.append("close")

    client = SimpleNamespace(
        register_skills=AsyncMock(return_value=True),
        unregister_skills=AsyncMock(side_effect=unregister_skills),
        close=AsyncMock(side_effect=close_client),
    )
    node = KeryxNode(AgentCard(name="worker", skills=[Skill(id="backend")]))
    node._client = client
    node._peer_id = "peer-worker"
    node._running = True
    await node.start_registration(ttl_seconds=2, refresh_interval_seconds=1)

    await node.close()

    assert events == ["deregister", "close"]


@pytest.mark.asyncio
async def test_close_continues_when_deregistration_fails() -> None:
    client = SimpleNamespace(
        register_skills=AsyncMock(return_value=True),
        unregister_skills=AsyncMock(side_effect=OSError("registry unavailable")),
        close=AsyncMock(),
    )
    node = KeryxNode(AgentCard(name="worker", skills=[Skill(id="backend")]))
    node._client = client
    node._peer_id = "peer-worker"
    node._running = True
    await node.start_registration(ttl_seconds=2, refresh_interval_seconds=1)

    await node.close()

    client.close.assert_awaited_once()
    status = node.registration_status()
    assert status["active"] is False
    assert status["state"] == "degraded"
    assert status["cleanup_pending"] is False
    assert "registry unavailable" in status["last_error"]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("ttl_seconds", "refresh_interval_seconds", "message"),
    [
        (0, 0.1, "ttl_seconds"),
        (2, 0, "refresh_interval_seconds"),
        (2, 2, "refresh_interval_seconds"),
    ],
)
async def test_registration_lifecycle_rejects_invalid_intervals(
    ttl_seconds: int, refresh_interval_seconds: float, message: str
) -> None:
    client = SimpleNamespace(
        register_skills=AsyncMock(return_value=True),
        unregister_skills=AsyncMock(return_value=True),
        close=AsyncMock(),
    )
    node = KeryxNode(AgentCard(name="worker", skills=[Skill(id="backend")]))
    node._client = client
    node._peer_id = "peer-worker"
    node._running = True

    try:
        with pytest.raises(ValueError, match=message):
            await node.start_registration(
                ttl_seconds=ttl_seconds,
                refresh_interval_seconds=refresh_interval_seconds,
            )
    finally:
        await node.stop_registration()


@pytest.mark.asyncio
async def test_registration_lifecycle_rejects_failed_initial_registration() -> None:
    client = SimpleNamespace(
        register_skills=AsyncMock(return_value=False),
        unregister_skills=AsyncMock(return_value=True),
        close=AsyncMock(),
    )
    node = KeryxNode(AgentCard(name="worker", skills=[Skill(id="backend")]))
    node._client = client
    node._peer_id = "peer-worker"
    node._running = True

    try:
        with pytest.raises(RuntimeError, match="registration was rejected"):
            await node.start_registration(ttl_seconds=2, refresh_interval_seconds=1)
    finally:
        await node.stop_registration()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("ttl_seconds", "refresh_interval_seconds", "message"),
    [
        (1.5, 0.5, "ttl_seconds"),
        (2**64, 1, "ttl_seconds"),
        (2, True, "refresh_interval_seconds"),
        (2, math.nan, "refresh_interval_seconds"),
    ],
)
async def test_registration_lifecycle_rejects_invalid_numeric_types(
    ttl_seconds: object, refresh_interval_seconds: object, message: str
) -> None:
    client = SimpleNamespace(
        register_skills=AsyncMock(return_value=True),
        unregister_skills=AsyncMock(return_value=True),
        close=AsyncMock(),
    )
    node = KeryxNode(AgentCard(name="worker", skills=[Skill(id="backend")]))
    node._client = client
    node._peer_id = "peer-worker"
    node._running = True

    try:
        with pytest.raises(ValueError, match=message):
            await node.start_registration(
                ttl_seconds=ttl_seconds,  # type: ignore[arg-type]
                refresh_interval_seconds=refresh_interval_seconds,  # type: ignore[arg-type]
            )
    finally:
        await node.stop_registration()


@pytest.mark.asyncio
@pytest.mark.parametrize("refresh_failure", [False, OSError("registry unavailable")])
async def test_registration_status_reports_refresh_failure_and_recovery(
    refresh_failure: object,
) -> None:
    calls = 0
    allow_recovery = asyncio.Event()

    async def register_skills(**_kwargs: object) -> bool:
        nonlocal calls
        calls += 1
        if calls == 1:
            return True
        if calls == 2:
            if isinstance(refresh_failure, Exception):
                raise refresh_failure
            return bool(refresh_failure)
        await allow_recovery.wait()
        return True

    client = SimpleNamespace(
        register_skills=AsyncMock(side_effect=register_skills),
        unregister_skills=AsyncMock(return_value=True),
        close=AsyncMock(),
    )
    node = KeryxNode(AgentCard(name="worker", skills=[Skill(id="backend")]))
    node._client = client
    node._peer_id = "peer-worker"
    node._running = True
    await node.start_registration(ttl_seconds=2, refresh_interval_seconds=0.01)

    await wait_until(lambda: node.registration_status()["state"] == "degraded")
    degraded = node.registration_status()
    assert degraded["active"] is True
    assert degraded["consecutive_failures"] == 1
    assert degraded["last_error"]

    allow_recovery.set()
    await wait_until(lambda: node.registration_status()["state"] == "healthy")
    healthy = node.registration_status()
    assert healthy["consecutive_failures"] == 0
    assert healthy["last_error"] is None
    assert healthy["last_success_ms"] > 0

    await node.stop_registration()


@pytest.mark.asyncio
async def test_registration_serializes_stop_before_new_start() -> None:
    unregister_started = asyncio.Event()
    release_unregister = asyncio.Event()

    async def unregister_skills(**_kwargs: object) -> bool:
        unregister_started.set()
        await release_unregister.wait()
        return True

    client = SimpleNamespace(
        register_skills=AsyncMock(return_value=True),
        unregister_skills=AsyncMock(side_effect=unregister_skills),
        close=AsyncMock(),
    )
    node = KeryxNode(AgentCard(name="worker", skills=[Skill(id="backend")]))
    node._client = client
    node._peer_id = "peer-worker"
    node._running = True
    await node.start_registration(ttl_seconds=2, refresh_interval_seconds=1)

    stopping = asyncio.create_task(node.stop_registration())
    await asyncio.wait_for(unregister_started.wait(), timeout=1)
    starting = asyncio.create_task(
        node.start_registration(ttl_seconds=2, refresh_interval_seconds=1)
    )
    try:
        await asyncio.sleep(0.01)
        assert starting.done() is False
        assert client.register_skills.await_count == 1
    finally:
        release_unregister.set()
        await stopping
        await starting
        await node.stop_registration()


@pytest.mark.asyncio
async def test_concurrent_registration_stops_deregister_once() -> None:
    release_unregister = asyncio.Event()

    async def unregister_skills(**_kwargs: object) -> bool:
        await release_unregister.wait()
        return True

    client = SimpleNamespace(
        register_skills=AsyncMock(return_value=True),
        unregister_skills=AsyncMock(side_effect=unregister_skills),
        close=AsyncMock(),
    )
    node = KeryxNode(AgentCard(name="worker", skills=[Skill(id="backend")]))
    node._client = client
    node._peer_id = "peer-worker"
    node._running = True
    await node.start_registration(ttl_seconds=2, refresh_interval_seconds=1)

    stops = [
        asyncio.create_task(node.stop_registration()),
        asyncio.create_task(node.stop_registration()),
    ]
    try:
        await wait_until(lambda: client.unregister_skills.await_count >= 1)
        await asyncio.sleep(0.01)
        assert client.unregister_skills.await_count == 1
    finally:
        release_unregister.set()
        await asyncio.gather(*stops)


@pytest.mark.asyncio
async def test_registration_lifecycle_snapshots_mutable_card() -> None:
    client = SimpleNamespace(
        register_skills=AsyncMock(return_value=True),
        unregister_skills=AsyncMock(return_value=True),
        close=AsyncMock(),
    )
    card = AgentCard(
        name="worker",
        skills=[Skill(id="backend", tags=["python"])],
    )
    node = KeryxNode(card)
    node._client = client
    node._peer_id = "peer-worker"
    node._running = True
    await node.start_registration(ttl_seconds=2, refresh_interval_seconds=0.01)

    card.skills[0].tags.append("mutated")
    card.skills.append(Skill(id="new-skill"))
    await wait_until(lambda: client.register_skills.await_count >= 2)
    await node.stop_registration()

    assert client.register_skills.await_args.kwargs["skills"] == [
        ("backend", "", ["python"])
    ]
    client.unregister_skills.assert_awaited_once_with(
        peer_id="peer-worker", skill_ids=["backend"]
    )


@pytest.mark.asyncio
async def test_registration_stop_defers_cleanup_for_cancellation_resistant_refresh() -> (
    None
):
    refresh_started = asyncio.Event()
    release_refresh = asyncio.Event()
    events: list[str] = []
    calls = 0

    async def register_skills(**_kwargs: object) -> bool:
        nonlocal calls
        calls += 1
        if calls == 1:
            return True
        refresh_started.set()
        try:
            await release_refresh.wait()
        except asyncio.CancelledError:
            await release_refresh.wait()
        events.append("refresh-finished")
        return True

    async def unregister_skills(**_kwargs: object) -> bool:
        events.append("deregistered")
        return True

    client = SimpleNamespace(
        register_skills=AsyncMock(side_effect=register_skills),
        unregister_skills=AsyncMock(side_effect=unregister_skills),
        close=AsyncMock(),
    )
    node = KeryxNode(
        AgentCard(name="worker", skills=[Skill(id="backend")]),
        registration_stop_timeout_seconds=0.01,
    )
    node._client = client
    node._peer_id = "peer-worker"
    node._running = True
    await node.start_registration(ttl_seconds=2, refresh_interval_seconds=0.01)
    await asyncio.wait_for(refresh_started.wait(), timeout=1)

    stopping = asyncio.create_task(node.stop_registration())
    try:
        done, _ = await asyncio.wait({stopping}, timeout=0.1)
        assert stopping in done
        result = stopping.result()
        assert result == {
            "accepted": False,
            "peer_id": "peer-worker",
            "cleanup_pending": True,
        }
        status = node.registration_status()
        assert status["state"] == "degraded"
        assert status["cleanup_pending"] is True
        assert events == []
        with pytest.raises(RuntimeError, match="already running"):
            await node.start_registration(ttl_seconds=2, refresh_interval_seconds=0.01)
        assert calls == 2
    finally:
        release_refresh.set()
        await stopping

    await wait_until(lambda: node.registration_status()["state"] == "inactive")
    assert events == ["refresh-finished", "deregistered"]


@pytest.mark.asyncio
async def test_node_stop_transfers_client_to_delayed_registration_cleanup() -> None:
    refresh_started = asyncio.Event()
    release_refresh = asyncio.Event()
    events: list[str] = []
    calls = 0

    async def register_skills(**_kwargs: object) -> bool:
        nonlocal calls
        calls += 1
        if calls == 1:
            return True
        refresh_started.set()
        try:
            await release_refresh.wait()
        except asyncio.CancelledError:
            await release_refresh.wait()
        events.append("refresh-finished")
        return True

    async def unregister_skills(**_kwargs: object) -> bool:
        events.append("deregistered")
        return True

    async def close_client() -> None:
        events.append("client-closed")

    client = SimpleNamespace(
        register_skills=AsyncMock(side_effect=register_skills),
        unregister_skills=AsyncMock(side_effect=unregister_skills),
        close=AsyncMock(side_effect=close_client),
    )
    node = KeryxNode(
        AgentCard(name="worker", skills=[Skill(id="backend")]),
        registration_stop_timeout_seconds=0.01,
    )
    node._client = client
    node._peer_id = "peer-worker"
    node._running = True
    await node.start_registration(ttl_seconds=2, refresh_interval_seconds=0.01)
    await asyncio.wait_for(refresh_started.wait(), timeout=1)

    try:
        await node.stop()
        status = node.registration_status()
        assert status["cleanup_pending"] is True
        assert client.close.await_count == 0
        assert node._client is None
        with pytest.raises(RuntimeError, match="cleanup is pending"):
            await node.start()
    finally:
        release_refresh.set()

    await wait_until(lambda: node.registration_status()["state"] == "inactive")
    assert events == ["refresh-finished", "deregistered", "client-closed"]


@pytest.mark.asyncio
async def test_registration_stop_bound_includes_stalled_deregistration() -> None:
    deregistration_started = asyncio.Event()
    release_deregistration = asyncio.Event()
    events: list[str] = []

    async def unregister_skills(**_kwargs: object) -> bool:
        deregistration_started.set()
        await release_deregistration.wait()
        events.append("deregistered")
        return True

    client = SimpleNamespace(
        register_skills=AsyncMock(return_value=True),
        unregister_skills=AsyncMock(side_effect=unregister_skills),
        close=AsyncMock(),
    )
    node = KeryxNode(
        AgentCard(name="worker", skills=[Skill(id="backend")]),
        registration_stop_timeout_seconds=0.01,
    )
    node._client = client
    node._peer_id = "peer-worker"
    node._running = True
    await node.start_registration(ttl_seconds=2, refresh_interval_seconds=1)

    stopping = asyncio.create_task(node.stop_registration())
    await asyncio.wait_for(deregistration_started.wait(), timeout=1)
    try:
        done, _ = await asyncio.wait({stopping}, timeout=0.1)
        assert stopping in done
        result = stopping.result()
        assert result == {
            "accepted": False,
            "peer_id": "peer-worker",
            "cleanup_pending": True,
        }
        status = node.registration_status()
        assert status["state"] == "degraded"
        assert status["cleanup_pending"] is True
        assert events == []
    finally:
        release_deregistration.set()
        await stopping

    await wait_until(lambda: node.registration_status()["state"] == "inactive")
    assert events == ["deregistered"]
