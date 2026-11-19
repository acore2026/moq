"""Real QUIC checks of ACN binding, reliable fanout and revocation."""

import asyncio
import socket

import pytest

from moq.acn.client import AcnClient, RequestFailed
from moq.acn.contracts import full_track
from moq.acn.relay import AcnRelay
from moq.acn.wire import ObjectReader, json_object
from moq.encoding import VarInt
from moq.messages import RequestErrorMessage
from moq.messages.data import SubgroupHeader, SubgroupObject


def port():
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


@pytest.mark.asyncio
async def test_binding_catalog_replay_fanout_and_revocation():
    relay = AcnRelay("127.0.0.1", port())
    await relay.start()
    owner = object()
    relay.open_management(owner)
    received = asyncio.Queue()
    pub = AcnClient("127.0.0.1", relay.server.port, "A", "publisher")
    sub = AcnClient("127.0.0.1", relay.server.port, "B", "subscriber", on_object=lambda req, value: received.put_nowait((req.track.track_name, value)))
    try:
        relay.apply(owner, "g", 1, "PREPARED", ["A", "B"])
        await pub.connect()
        await sub.connect()
        track = full_track("g", 1, "A", "status")
        with pytest.raises(RequestFailed) as error:
            await pub.request(track)
        assert error.value.retry_interval == 501
        relay.apply(owner, "g", 1, "ACTIVE", ["A", "B"])
        published = await pub.request(track)
        await sub.request(track)
        await pub.send(published, {})
        assert await asyncio.wait_for(received.get(), 2) == (b"status", {})
        directory_track = full_track("g", 1, "A", "group_catalog")
        directory = await pub.request(directory_track)
        snapshot = {"group_id": "g", "group_version": 1, "publisher_agent_id": "A", "directory_revision": 1,
                    "tracks": [{"name": "status", "content_type": "application/json"}]}
        await pub.send(directory, snapshot)
        await sub.request(directory_track)
        assert await asyncio.wait_for(received.get(), 2) == (b"group_catalog", snapshot)
        relay.delete(owner, "g", 1)
        await asyncio.sleep(.05)
        assert not relay.publications and not relay.subscriptions and not relay.bindings
    finally:
        await pub.close()
        await sub.close()
        await relay.stop()


@pytest.mark.asyncio
async def test_same_role_replacement_preserves_other_role_and_open_clears_all():
    relay = AcnRelay("127.0.0.1", port())
    await relay.start()
    owner = object()
    relay.open_management(owner)
    relay.apply(owner, "g", 1, "PREPARED", ["A", "B"])
    clients = [AcnClient("127.0.0.1", relay.server.port, "A", role) for role in ("publisher", "subscriber", "publisher")]
    try:
        for client in clients:
            await client.connect()
        for _ in range(200):
            if not clients[0].bound:
                break
            await asyncio.sleep(.01)
        assert clients[1].bound and clients[2].bound and not clients[0].bound
        assert len(relay.bindings) == 2
        relay.open_management(object())
        await asyncio.sleep(.05)
        assert not relay.bindings and not relay.policy.groups
    finally:
        for client in clients:
            await client.close()
        await relay.stop()


def test_fragmented_objects_and_ordinary_error_bytes():
    original = RequestErrorMessage(1, 0, "later")
    extended = RequestErrorMessage(1, 0, "later", 501)
    assert original.encode() != extended.encode()
    reader = ObjectReader()
    data = VarInt.encode(1) + SubgroupHeader(7, 0, 0, 128).encode() + SubgroupObject(0, json_object({}, 65536)).encode()
    objects = []
    for byte in data:
        objects.extend(reader.feed(bytes([byte])))
    assert len(objects) == 1 and objects[0][1].strip() == b"{}"
