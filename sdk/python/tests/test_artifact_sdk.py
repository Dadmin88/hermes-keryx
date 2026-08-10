from __future__ import annotations

import hashlib
import os
from pathlib import Path
from typing import Any

import pytest
from hermes.keryx.v1 import common_pb2, daemon_pb2

from keryx import TaskArtifact
from keryx.client import RESULT_ARTIFACT_FRAME_MAX_BYTES, DaemonClient
from keryx.node import KeryxNode


def _response(
    content: bytes = b"\x00artifact\xff", *, artifact_id: str = "artifact-1", **kwargs: Any
) -> daemon_pb2.GetArtifactResponse:
    fields: dict[str, Any] = {
        "artifact_id": common_pb2.ArtifactId(value=artifact_id),
        "task_id": common_pb2.TaskId(value="task-1"),
        "digest": hashlib.sha256(content).hexdigest(),
        "media_type": "application/x-test",
        "byte_len": len(content),
        "content": content,
    }
    fields.update(kwargs)
    return daemon_pb2.GetArtifactResponse(**fields)


def test_task_artifact_distinguishes_descriptor_from_zero_byte_content() -> None:
    descriptor = TaskArtifact(path="remote/display-name.bin")
    zero_byte = TaskArtifact(path="remote/empty.bin", content=b"")

    descriptor_proto = descriptor.to_proto()
    zero_byte_proto = zero_byte.to_proto()

    assert descriptor_proto.content_present is False
    assert descriptor_proto.content == b""
    assert descriptor_proto.byte_len == 0
    assert descriptor_proto.sha256 == ""
    assert zero_byte_proto.content_present is True
    assert zero_byte_proto.content == b""
    assert zero_byte_proto.byte_len == 0
    assert zero_byte_proto.sha256 == hashlib.sha256(b"").hexdigest()


def test_task_artifact_round_trips_binary_payloads_and_declared_metadata() -> None:
    first = TaskArtifact(path="display/one", content=b"\x00\xff\nbytes", metadata={"kind": "first"})
    second = TaskArtifact(path="display/two", content=b"second\x00payload")

    round_tripped = [TaskArtifact.from_proto(item.to_proto()) for item in (first, second)]

    assert [item.content for item in round_tripped] == [b"\x00\xff\nbytes", b"second\x00payload"]
    assert [item.byte_len for item in round_tripped] == [8, 14]
    assert [item.sha256 for item in round_tripped] == [
        hashlib.sha256(b"\x00\xff\nbytes").hexdigest(),
        hashlib.sha256(b"second\x00payload").hexdigest(),
    ]
    assert round_tripped[0].metadata == {"kind": "first"}


def test_task_artifact_rejects_digest_length_and_content_type_mismatches() -> None:
    with pytest.raises(ValueError, match="sha256"):
        TaskArtifact(path="display", content=b"payload", sha256="bad").to_proto()
    with pytest.raises(ValueError, match="byte_len"):
        TaskArtifact(path="display", content=b"payload", byte_len=999).to_proto()
    with pytest.raises(TypeError, match="content"):
        TaskArtifact(path="display", content="payload").to_proto()  # type: ignore[arg-type]
    with pytest.raises(TypeError, match="sha256"):
        TaskArtifact(path="display", content=b"payload", sha256=123).to_proto()  # type: ignore[arg-type]
    with pytest.raises(TypeError, match="byte_len"):
        TaskArtifact(path="display", content=b"payload", byte_len=True).to_proto()


def test_legacy_completion_preserves_one_raw_part_as_binary_artifact() -> None:
    from keryx.node import _completion_payload
    from keryx.task import Artifact, Part

    _, artifacts = _completion_payload(
        [
            Artifact(name="display.bin", parts=[Part(raw=b"\x00\xffbinary", media_type="application/x-test")]),
            Artifact(name="descriptor", parts=[]),
        ]
    )

    binary, descriptor = [item.to_proto() for item in artifacts]
    assert binary.content_present is True
    assert binary.content == b"\x00\xffbinary"
    assert binary.media_type == "application/x-test"
    assert descriptor.content_present is False


def test_legacy_completion_rejects_ambiguous_multiple_raw_parts() -> None:
    from keryx.node import _completion_payload
    from keryx.task import Artifact, Part

    with pytest.raises(ValueError, match="multiple raw"):
        _completion_payload([Artifact(name="ambiguous", parts=[Part(raw=b"one"), Part(raw=b"two")])])


class _ArtifactDaemon:
    def __init__(self, response: daemon_pb2.GetArtifactResponse) -> None:
        self.response = response
        self.requests: list[daemon_pb2.GetArtifactRequest] = []

    async def GetArtifact(self, request: daemon_pb2.GetArtifactRequest) -> daemon_pb2.GetArtifactResponse:
        self.requests.append(request)
        return self.response


def _client(response: daemon_pb2.GetArtifactResponse) -> DaemonClient:
    client = DaemonClient(daemon_endpoint="127.0.0.1:50051", channel=object())
    client._daemon = _ArtifactDaemon(response)  # type: ignore[assignment]
    return client


@pytest.mark.asyncio
async def test_get_artifact_returns_verified_content() -> None:
    content = b"\x00artifact\xff"
    client = _client(_response(content))

    artifact = await client.get_artifact("artifact-1")

    assert artifact.artifact_id == "artifact-1"
    assert artifact.task_id == "task-1"
    assert artifact.content == content
    assert client._daemon.requests[0].artifact_id.value == "artifact-1"  # type: ignore[union-attr]
    assert client._daemon.requests[0].metadata_only is False  # type: ignore[union-attr]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("response", "match"),
    [
        (_response(artifact_id="other"), "artifact id"),
        (_response(byte_len=99), "byte_len"),
        (_response(digest="a" * 63), "digest"),
        (_response(digest="A" * 64), "digest"),
        (_response(digest="0" * 64), "digest"),
    ],
)
async def test_get_artifact_rejects_invalid_response(response: daemon_pb2.GetArtifactResponse, match: str) -> None:
    with pytest.raises(ValueError, match=match):
        await _client(response).get_artifact("artifact-1")


@pytest.mark.asyncio
async def test_get_artifact_metadata_only_accepts_omitted_content_and_validates_metadata() -> None:
    content = b"metadata content"
    response = _response(content)
    response.content = b""
    client = _client(response)

    artifact = await client.get_artifact("artifact-1", metadata_only=True)

    assert artifact.content is None
    assert client._daemon.requests[0].metadata_only is True  # type: ignore[union-attr]
    response.digest = "bad"
    with pytest.raises(ValueError, match="digest"):
        await _client(response).get_artifact("artifact-1", metadata_only=True)


@pytest.mark.asyncio
async def test_download_artifact_uses_explicit_destination_not_remote_display_name(tmp_path: Path) -> None:
    destination = tmp_path / "chosen.bin"
    artifact = await _client(_response(b"verified")).download_artifact("artifact-1", destination)

    assert artifact.content == b"verified"
    assert destination.read_bytes() == b"verified"
    assert not (tmp_path / "../../remote-display-name").exists()


@pytest.mark.asyncio
async def test_download_artifact_refuses_existing_destination_without_overwrite(tmp_path: Path) -> None:
    destination = tmp_path / "existing.bin"
    destination.write_bytes(b"old")

    with pytest.raises(FileExistsError):
        await _client(_response(b"new")).download_artifact("artifact-1", destination)

    assert destination.read_bytes() == b"old"


@pytest.mark.asyncio
async def test_download_artifact_replaces_existing_destination_only_with_overwrite(tmp_path: Path) -> None:
    destination = tmp_path / "existing.bin"
    destination.write_bytes(b"old")

    await _client(_response(b"new")).download_artifact("artifact-1", destination, overwrite=True)

    assert destination.read_bytes() == b"new"


@pytest.mark.asyncio
async def test_download_artifact_rejects_symlink_destination(tmp_path: Path) -> None:
    target = tmp_path / "target.bin"
    target.write_bytes(b"target")
    destination = tmp_path / "link.bin"
    destination.symlink_to(target)

    with pytest.raises(ValueError, match="symlink"):
        await _client(_response(b"new")).download_artifact("artifact-1", destination, overwrite=True)

    assert target.read_bytes() == b"target"


@pytest.mark.asyncio
async def test_download_artifact_cleans_temp_after_publish_failure(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    destination = tmp_path / "publish.bin"

    def fail_link(*_args: object, **_kwargs: object) -> None:
        raise OSError("simulated publish failure")

    monkeypatch.setattr(os, "link", fail_link)
    with pytest.raises(OSError, match="simulated publish failure"):
        await _client(_response(b"new")).download_artifact("artifact-1", destination)

    assert not list(tmp_path.glob(".publish.bin.*.tmp"))


@pytest.mark.asyncio
async def test_node_get_and_download_artifact_use_direct_daemon_stub(tmp_path: Path) -> None:
    daemon = _ArtifactDaemon(_response(b"node bytes"))
    node = KeryxNode(daemon_stub=daemon)

    artifact = await node.get_artifact("artifact-1")
    await node.download_artifact("artifact-1", tmp_path / "node.bin")

    assert artifact.content == b"node bytes"
    assert (tmp_path / "node.bin").read_bytes() == b"node bytes"
    assert len(daemon.requests) == 2


@pytest.mark.asyncio
async def test_sdk_created_daemon_channels_have_artifact_frame_options(monkeypatch: pytest.MonkeyPatch) -> None:
    import keryx.client as client_module
    import keryx.node as node_module

    calls: list[tuple[str, tuple[tuple[str, int], ...], object | None]] = []

    class _Channel:
        async def close(self) -> None:
            return None

    def insecure_channel(
        target: str,
        options: tuple[tuple[str, int], ...],
        interceptors: object | None = None,
    ) -> _Channel:
        calls.append((target, options, interceptors))
        return _Channel()

    monkeypatch.setattr(client_module.grpc.aio, "insecure_channel", insecure_channel)
    monkeypatch.setattr(node_module.grpc.aio, "insecure_channel", insecure_channel)
    monkeypatch.setattr(client_module.daemon_pb2_grpc, "KeryxDaemonStub", lambda channel: object())
    monkeypatch.setattr(node_module.daemon_pb2_grpc, "KeryxDaemonStub", lambda channel: object())

    client = DaemonClient(
        daemon_endpoint="127.0.0.1:50051",
        daemon_token="sdk-channel-test-token",
    )
    await client.connect()
    node = KeryxNode(
        daemon_endpoint="127.0.0.1:50051",
        daemon_token="sdk-channel-test-token",
    )
    await node.connect()

    expected = (
        ("grpc.max_send_message_length", RESULT_ARTIFACT_FRAME_MAX_BYTES),
        ("grpc.max_receive_message_length", RESULT_ARTIFACT_FRAME_MAX_BYTES),
    )
    assert len(calls) == 2
    for target, options, interceptors in calls:
        assert target == "127.0.0.1:50051"
        assert options == expected
        assert isinstance(interceptors, list)
        assert len(interceptors) == 1
        assert isinstance(interceptors[0], client_module._DaemonAuthInterceptor)


@pytest.mark.asyncio
async def test_injected_daemon_channels_are_untouched(monkeypatch: pytest.MonkeyPatch) -> None:
    class _Channel:
        async def close(self) -> None:
            return None

    def fail_channel(*_args: object, **_kwargs: object) -> None:
        raise AssertionError("insecure_channel should not be called")

    monkeypatch.setattr("keryx.client.grpc.aio.insecure_channel", fail_channel)
    monkeypatch.setattr("keryx.node.grpc.aio.insecure_channel", fail_channel)
    monkeypatch.setattr("keryx.client.daemon_pb2_grpc.KeryxDaemonStub", lambda channel: object())
    monkeypatch.setattr("keryx.node.daemon_pb2_grpc.KeryxDaemonStub", lambda channel: object())

    await DaemonClient(daemon_endpoint="127.0.0.1:50051", channel=_Channel()).connect()
    await KeryxNode(daemon_endpoint="127.0.0.1:50051", channel=_Channel()).connect()
