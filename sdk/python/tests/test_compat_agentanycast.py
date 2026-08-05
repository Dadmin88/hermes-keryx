"""AgentAnycast compatibility shim tests."""

from __future__ import annotations

import base58
import warnings
from typing import Any

import pytest

from keryx.card import AgentCard, Skill
from keryx.client import DaemonClient, PeerInfo
from keryx.node import KeryxNode
from hermes.keryx.v1 import common_pb2, daemon_pb2, registry_pb2, result_pb2


def _make_peer_id(pubkey_bytes: bytes) -> str:
    proto = bytes([0x08, 0x01, 0x12, len(pubkey_bytes)]) + pubkey_bytes
    mh = bytes([0x00, len(proto)]) + proto
    return base58.b58encode(mh).decode("ascii")


class _FakeDaemonStub:
    def __init__(self, local_peer_id: str) -> None:
        self._local_peer_id = local_peer_id
        self.sent: list[daemon_pb2.SendTaskRequest] = []
        self.result_response = daemon_pb2.GetTaskResultResponse(status="submitted")

    async def ListPeers(self, _request: Any) -> daemon_pb2.ListPeersResponse:
        return daemon_pb2.ListPeersResponse(
            peers=[
                daemon_pb2.PeerDescriptor(peer_id=self._local_peer_id, connected=True, local=True),
                daemon_pb2.PeerDescriptor(peer_id="12D3KooWRemote", connected=True, local=False),
            ]
        )

    async def SendTask(self, request: daemon_pb2.SendTaskRequest) -> daemon_pb2.SendTaskResponse:
        self.sent.append(request)
        return daemon_pb2.SendTaskResponse(
            task_id=request.envelope.task_id,
            status="submitted",
            routed_to=request.target_peer_id,
            delivery_route="local",
        )

    async def GetTaskResult(
        self, _request: daemon_pb2.GetTaskResultRequest
    ) -> daemon_pb2.GetTaskResultResponse:
        return self.result_response


class _FakeRegistryStub:
    def __init__(self) -> None:
        self.register_calls: list[registry_pb2.RegisterSkillsRequest] = []
        self.unregister_calls: list[registry_pb2.UnregisterSkillsRequest] = []

    async def RegisterSkills(
        self,
        request: registry_pb2.RegisterSkillsRequest,
        *,
        timeout: float | None = None,
        metadata: object = None,
    ) -> registry_pb2.RegisterSkillsResponse:
        self.register_calls.append(request)
        return registry_pb2.RegisterSkillsResponse(accepted=True)

    async def UnregisterSkills(
        self,
        request: registry_pb2.UnregisterSkillsRequest,
        *,
        timeout: float | None = None,
        metadata: object = None,
    ) -> registry_pb2.UnregisterSkillsResponse:
        self.unregister_calls.append(request)
        return registry_pb2.UnregisterSkillsResponse(accepted=True)

    async def DiscoverBySkill(self, request: registry_pb2.DiscoverBySkillRequest) -> registry_pb2.DiscoverBySkillResponse:
        if request.skill_id != "demo-skill":
            return registry_pb2.DiscoverBySkillResponse()
        return registry_pb2.DiscoverBySkillResponse(
            registrations=[
                registry_pb2.Registration(
                    peer_id="12D3KooWRemote",
                    name="remote-agent",
                    description="test agent",
                    skills=[registry_pb2.SkillInfo(skill_id="demo-skill", description="demo")],
                )
            ]
        )


class FakeDaemonClient(DaemonClient):
    def __init__(self, *, local_peer_id: str, **kwargs: Any) -> None:
        super().__init__(daemon_endpoint="unix:///tmp/fake", registry_endpoint="tcp://127.0.0.1:50051")
        self._local_peer_id = local_peer_id
        self._fake_daemon = _FakeDaemonStub(local_peer_id)
        self._fake_registry = _FakeRegistryStub()

    async def connect(self) -> None:
        self._daemon = self._fake_daemon  # type: ignore[assignment]
        self._registry = self._fake_registry  # type: ignore[assignment]

    async def close(self) -> None:
        return None

    async def list_peers(self) -> list[PeerInfo]:
        return [PeerInfo(peer_id=self._local_peer_id, connected=True, local=True)]


@pytest.fixture
def sample_card() -> AgentCard:
    return AgentCard(name="demo", description="demo agent", skills=[Skill(id="demo-skill", description="demo")])


@pytest.mark.asyncio
async def test_agentanycast_import_emits_deprecation_warning(sample_card: AgentCard) -> None:
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        import importlib

        import agentanycast

        importlib.reload(agentanycast)
        from agentanycast import Node  # noqa: F401

    messages = [str(item.message) for item in caught if issubclass(item.category, DeprecationWarning)]
    assert any("agentanycast is deprecated" in message for message in messages)


@pytest.mark.asyncio
async def test_agentanycast_node_is_keryx_node(sample_card: AgentCard) -> None:
    peer_id = _make_peer_id(bytes(range(32)))
    node = KeryxNode(sample_card, client_factory=lambda **_: FakeDaemonClient(local_peer_id=peer_id))
    await node.start()
    assert node.peer_id == peer_id
    await node.stop()


@pytest.mark.asyncio
async def test_peer_id_to_did_key_compat(sample_card: AgentCard) -> None:
    from agentanycast import peer_id_to_did_key

    peer_id = _make_peer_id(bytes(range(32)))
    did = peer_id_to_did_key(peer_id)
    assert did.startswith("did:key:z")


@pytest.mark.asyncio
async def test_register_and_deregister_wrappers(sample_card: AgentCard) -> None:
    from agentanycast import deregister_agent, register_agent

    peer_id = _make_peer_id(bytes(range(32)))
    fake_client = FakeDaemonClient(local_peer_id=peer_id)
    node = KeryxNode(sample_card, client_factory=lambda **_: fake_client)
    await node.start()

    reg = await register_agent(node, sample_card, current_load=1)
    assert reg["accepted"] is True
    assert reg["peer_id"] == peer_id
    assert fake_client._fake_registry.register_calls[0].peer_id == peer_id

    dereg = await deregister_agent(node, card=sample_card)
    assert dereg["accepted"] is True
    assert fake_client._fake_registry.unregister_calls[0].skill_ids == ["demo-skill"]


@pytest.mark.asyncio
async def test_send_task_maps_to_daemon_rpc(sample_card: AgentCard) -> None:
    peer_id = _make_peer_id(bytes(range(32)))
    fake_client = FakeDaemonClient(local_peer_id=peer_id)
    node = KeryxNode(sample_card, client_factory=lambda **_: fake_client)
    await node.start()

    deadline_ms = 1_800_000_000_000
    handle = await node.send_task(
        {"role": "user", "parts": [{"text": "hello"}]},
        peer_id="12D3KooWRemote",
        deadline_ms=deadline_ms,
    )
    assert handle.task_id
    assert fake_client._fake_daemon.sent[0].target_peer_id == "12D3KooWRemote"
    assert fake_client._fake_daemon.sent[0].envelope.messages[0].parts[0].text == "hello"
    assert fake_client._fake_daemon.sent[0].envelope.deadline_ms == deadline_ms


@pytest.mark.asyncio
async def test_task_handle_preserves_origin_artifact_id(sample_card: AgentCard) -> None:
    peer_id = _make_peer_id(bytes(range(32)))
    fake_client = FakeDaemonClient(local_peer_id=peer_id)
    node = KeryxNode(sample_card, client_factory=lambda **_: fake_client)
    await node.start()
    handle = await node.send_task(
        {"role": "user", "parts": [{"text": "hello"}]},
        peer_id="12D3KooWRemote",
    )
    fake_client._fake_daemon.result_response = daemon_pb2.GetTaskResultResponse(
        found=True,
        status="completed",
        result=result_pb2.TaskResultEnvelope(
            protocol_version=2,
            task_id=common_pb2.TaskId(value=handle.task_id),
            outcome=result_pb2.TERMINAL_OUTCOME_COMPLETED,
            output_artifacts=[
                result_pb2.ResultArtifact(
                    path="../../display-only.bin",
                    artifact_id=common_pb2.ArtifactId(value="origin-artifact-1"),
                    sha256="0" * 64,
                    byte_len=4,
                )
            ],
        ),
    )

    result = await handle.wait(timeout=1)

    assert result.artifacts[0].artifact_id == "origin-artifact-1"
    assert result.artifacts[0].name == "../../display-only.bin"


@pytest.mark.asyncio
async def test_daemon_client_keeps_execution_deadline_distinct_from_delivery_timeout() -> None:
    client = FakeDaemonClient(local_peer_id="peer-local")
    await client.connect()

    await client.send_task(
        target_peer_id="peer-remote",
        task_id="task-deadline",
        message_text="hello",
        deadline_ms=1_800_000_000_000,
        timeout_ms=4_321,
    )

    request = client._fake_daemon.sent[0]
    assert request.envelope.deadline_ms == 1_800_000_000_000
    assert request.timeout_ms == 4_321


@pytest.mark.asyncio
@pytest.mark.parametrize("deadline_ms", [True, -1, 2**63])
async def test_daemon_client_rejects_invalid_execution_deadline(deadline_ms: int) -> None:
    client = FakeDaemonClient(local_peer_id="peer-local")
    await client.connect()

    with pytest.raises(ValueError, match="deadline_ms"):
        await client.send_task(
            target_peer_id="peer-remote",
            task_id="task-deadline",
            message_text="hello",
            deadline_ms=deadline_ms,
        )


@pytest.mark.asyncio
async def test_discover_maps_registry_results(sample_card: AgentCard) -> None:
    peer_id = _make_peer_id(bytes(range(32)))
    fake_client = FakeDaemonClient(local_peer_id=peer_id)
    node = KeryxNode(sample_card, client_factory=lambda **_: fake_client)
    await node.start()

    results = await node.discover("demo-skill")
    assert results[0]["peer_id"] == "12D3KooWRemote"
    assert results[0]["agent_name"] == "remote-agent"