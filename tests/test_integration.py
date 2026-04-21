#!/usr/bin/env python3
"""
Integration test for MOQ Transport.
Tests basic functionality of Publisher, Subscriber, and Relay.
"""

import asyncio
import json
import tempfile
import sys
import logging
from typing import List
from pathlib import Path

import pytest

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

from _path_helper import ensure_repo_root

ensure_repo_root()

from moq.encoding import FullTrackName, VarInt
from moq.messages import (
    SetupMessage, SubscribeMessage, SubscribeOkMessage,
    PublishMessage, PublishOkMessage,
    ObjectHeader, ObjectDatagram, SubgroupHeader, SubgroupObject,
    StreamType,
    GroupOrder, SubscribeFilter
)
from moq.encoding import Parameters
from moq.transport import StreamData


def test_varint_encoding():
    """Test VarInt encoding/decoding."""
    logger.info("Testing VarInt encoding...")
    
    test_values = [
        0, 1, 127, 128, 16383, 16384, 2097151,
        2097152, 268435455, 34359738367, 4398046511103
    ]
    
    for value in test_values:
        encoded = VarInt.encode(value)
        decoded, consumed = VarInt.decode(encoded)
        assert decoded == value, f"VarInt failed for {value}: got {decoded}"
        logger.info(f"  {value} -> {len(encoded)} bytes -> {decoded} ✓")
    
    logger.info("VarInt encoding test passed!\n")


def test_full_track_name():
    """Test FullTrackName encoding/decoding."""
    logger.info("Testing FullTrackName encoding...")
    
    test_names = [
        FullTrackName([], b"simple"),
        FullTrackName([b"ns"], b"track"),
        FullTrackName([b"example", b"live"], b"stream1"),
    ]
    
    for name in test_names:
        encoded = name.encode()
        decoded, consumed = FullTrackName.decode(encoded)
        assert decoded == name, f"FullTrackName failed"
        logger.info(f"  {name} -> {len(encoded)} bytes -> {decoded} ✓")
    
    logger.info("FullTrackName encoding test passed!\n")


def test_setup_message():
    """Test SETUP message."""
    logger.info("Testing SETUP message...")
    
    params = Parameters()
    msg = SetupMessage(version=0xFF000011, role=0x03, parameters=params)
    
    encoded = msg.encode()
    logger.info(f"  Encoded SETUP: {len(encoded)} bytes")
    logger.info("SETUP message test passed!\n")


def test_subscribe_message():
    """Test SUBSCRIBE message."""
    logger.info("Testing SUBSCRIBE message...")
    
    track_name = FullTrackName([b"test"], b"stream")
    
    msg = SubscribeMessage(
        request_id=1,
        track_alias=10,
        full_track_name=track_name,
        subscriber_priority=128,
        group_order=GroupOrder.ASCENDING,
        filter_type=SubscribeFilter.LATEST_OBJECT
    )
    
    encoded = msg.encode()
    
    # Skip message type prefix for decoding test
    decoded, consumed = SubscribeMessage.decode(encoded, offset=2)
    
    assert decoded.request_id == 1
    assert decoded.track_alias == 10
    assert decoded.subscriber_priority == 128
    assert decoded.filter_type == SubscribeFilter.LATEST_OBJECT
    
    logger.info(f"  Encoded SUBSCRIBE: {len(encoded)} bytes")
    logger.info("SUBSCRIBE message test passed!\n")


def test_publish_message():
    """Test PUBLISH message."""
    logger.info("Testing PUBLISH message...")
    
    track_name = FullTrackName([b"live"], b"video")
    
    msg = PublishMessage(
        request_id=1,
        track_alias=5,
        full_track_name=track_name
    )
    
    encoded = msg.encode()
    decoded, _ = PublishMessage.decode(encoded, offset=2)
    
    assert decoded.request_id == 1
    assert decoded.track_alias == 5
    
    logger.info(f"  Encoded PUBLISH: {len(encoded)} bytes")
    logger.info("PUBLISH message test passed!\n")


def test_object_datagram():
    """Test Object Datagram."""
    logger.info("Testing Object Datagram...")
    
    header = ObjectHeader(
        track_alias=1,
        group_id=1,
        object_id=1,
        publisher_priority=128
    )
    
    datagram = ObjectDatagram(
        header=header,
        payload=b"Hello, MOQ!"
    )
    
    encoded = datagram.encode()
    decoded, _ = ObjectDatagram.decode(encoded)
    
    assert decoded.header.track_alias == 1
    assert decoded.header.group_id == 1
    assert decoded.header.object_id == 1
    assert decoded.payload == b"Hello, MOQ!"
    
    logger.info(f"  Encoded ObjectDatagram: {len(encoded)} bytes")
    logger.info("Object Datagram test passed!\n")


@pytest.mark.asyncio
async def test_session_management():
    """Test session management."""
    logger.info("Testing Session Management...")
    
    from moq.session import MOQSession, Role
    
    session = MOQSession(session_id="test-session", role=Role.PUBSUB)
    
    # Test request ID generation
    id1 = session._get_next_request_id()
    id2 = session._get_next_request_id()
    assert id2 == id1 + 1
    
    # Test track alias generation
    track_name = FullTrackName([b"test"], b"track")
    alias1 = session._get_or_create_track_alias(track_name)
    alias2 = session._get_or_create_track_alias(track_name)
    assert alias1 == alias2  # Same track, same alias
    
    logger.info("Session Management test passed!\n")


@pytest.mark.asyncio
async def test_session_waits_for_async_control_send():
    """Test that control messages await async send callbacks."""
    logger.info("Testing async control send...")

    from moq.session import MOQSession, Role

    session = MOQSession(session_id="async-send-test", role=Role.SUBSCRIBER)
    send_events = []
    send_done = asyncio.Event()

    async def send_callback(data: bytes):
        await asyncio.sleep(0.01)
        send_events.append(data)
        send_done.set()

    session.set_send_callback(send_callback)

    track_name = FullTrackName([b"test"], b"stream")
    request_id = await session.subscribe(track_name)

    assert request_id == 0
    assert send_done.is_set()
    assert len(send_events) == 1
    assert request_id in session.subscriptions

    logger.info("Async control send test passed!\n")


@pytest.mark.asyncio
async def test_publisher_reassembles_fragmented_publish_ok():
    """Test publisher handles fragmented PUBLISH_OK on the control stream."""
    logger.info("Testing fragmented PUBLISH_OK handling...")

    from moq.pub.publisher import MOQPublisher
    from moq.session import MOQSession, Role

    publisher = MOQPublisher("127.0.0.1", 4443)
    publisher._session = MOQSession(session_id="pub-test", role=Role.PUBLISHER)

    request_id = 7
    publisher._publish_waiters[request_id] = asyncio.Event()
    publisher._active_tracks[request_id] = FullTrackName([b"time"], b"updates")

    encoded = PublishOkMessage(request_id=request_id).encode()
    split_at = len(encoded) // 2

    await publisher._handle_stream_data(None, StreamData(stream_id=0, data=encoded[:split_at]))
    assert not publisher._publish_waiters[request_id].is_set()

    await publisher._handle_stream_data(None, StreamData(stream_id=0, data=encoded[split_at:]))
    assert publisher._publish_waiters[request_id].is_set()

    logger.info("Fragmented PUBLISH_OK handling test passed!\n")


@pytest.mark.asyncio
async def test_relay_reassembles_fragmented_publish():
    """Test relay handles fragmented PUBLISH on the control stream."""
    logger.info("Testing fragmented PUBLISH handling at relay...")

    from moq.relay.relay import MOQRelay

    relay = MOQRelay(host="127.0.0.1", port=4443, cache_dir="/tmp/moq_test_cache_frag")

    class FakeQuic:
        host_cid = b"relay-test-client"

    class FakeProtocol:
        _quic = FakeQuic()

    protocol = FakeProtocol()
    track_name = FullTrackName([b"live"], b"video")
    encoded = PublishMessage(request_id=3, track_alias=11, full_track_name=track_name).encode()
    split_at = len(encoded) // 2

    seen = []

    async def capture_publish(client, msg):
        seen.append((client.session_id, msg.request_id, msg.track_alias, msg.full_track_name))

    relay._handle_publish = capture_publish

    await relay._on_quic_stream_data(protocol, StreamData(stream_id=0, data=encoded[:split_at]))
    assert seen == []

    await relay._on_quic_stream_data(protocol, StreamData(stream_id=0, data=encoded[split_at:]))
    assert seen == [("b'relay-test-client'", 3, 11, track_name)]

    logger.info("Fragmented PUBLISH handling test passed!\n")


@pytest.mark.asyncio
async def test_subscriber_reassembles_fragmented_subgroup_object():
    """Test subscriber handles a subgroup object split across QUIC events."""
    logger.info("Testing fragmented subgroup object handling at subscriber...")

    from moq.sub.subscriber import MOQSubscriber

    subscriber = MOQSubscriber("127.0.0.1", 4443)
    track_name = FullTrackName([b"live"], b"video")
    track_alias = 9
    subscriber._track_aliases[track_alias] = track_name

    payload = b"fragmented-video-payload" * 64
    encoded = (
        VarInt.encode(StreamType.SUBGROUP_HEADER)
        + SubgroupHeader(
            track_alias=track_alias,
            group_id=1,
            subgroup_id=0,
            publisher_priority=128,
        ).encode()
        + SubgroupObject(object_id=1, payload=payload).encode()
    )

    first_split = 5
    second_split = len(encoded) - 7

    await subscriber._handle_stream_data(
        None, StreamData(stream_id=4, data=encoded[:first_split], end_stream=False)
    )
    assert subscriber._object_queue.empty()

    await subscriber._handle_stream_data(
        None,
        StreamData(
            stream_id=4,
            data=encoded[first_split:second_split],
            end_stream=False,
        ),
    )
    assert subscriber._object_queue.empty()

    await subscriber._handle_stream_data(
        None, StreamData(stream_id=4, data=encoded[second_split:], end_stream=True)
    )

    obj = await subscriber.get_next_object(timeout=0.1)
    assert obj is not None
    assert obj.group_id == 1
    assert obj.object_id == 1
    assert obj.payload == payload
    assert 4 not in subscriber._data_stream_buffers

    logger.info("Fragmented subgroup object handling test passed!\n")


@pytest.mark.asyncio
async def test_relay_forwards_fragmented_subgroup_stream_on_one_downstream_stream():
    """Test relay preserves stream continuity when forwarding fragmented subgroup data."""
    logger.info("Testing fragmented subgroup stream forwarding at relay...")

    from moq.relay.relay import MOQRelay, ClientSession

    class DummyQuicConnection:
        def __init__(self):
            self.sent_streams = []
            self.next_stream_id = 2

        def get_next_available_stream_id(self, is_unidirectional=False):
            stream_id = self.next_stream_id
            self.next_stream_id += 4
            return stream_id

        def send_stream_data(self, stream_id, data, end_stream=False):
            self.sent_streams.append((stream_id, data, end_stream))

    class DummyProtocol:
        def __init__(self):
            self.transmit_calls = 0

        def transmit(self):
            self.transmit_calls += 1

    relay = MOQRelay(host="127.0.0.1", port=4443, cache_dir="/tmp/moq_test_cache_stream_frag")

    track_name = FullTrackName([b"live"], b"video")
    track_alias = 11
    payload = b"relay-forwarded-video" * 128
    encoded = (
        VarInt.encode(StreamType.SUBGROUP_HEADER)
        + SubgroupHeader(
            track_alias=track_alias,
            group_id=7,
            subgroup_id=0,
            publisher_priority=128,
        ).encode()
        + SubgroupObject(object_id=3, payload=payload).encode()
    )

    publisher = ClientSession(
        session_id="publisher",
        protocol=DummyProtocol(),
        quic_connection=DummyQuicConnection(),
    )
    publisher.publications[track_name] = {"track_alias": track_alias, "request_id": 1}

    subscriber = ClientSession(
        session_id="subscriber",
        protocol=DummyProtocol(),
        quic_connection=DummyQuicConnection(),
    )
    relay._subscriptions[track_name] = [subscriber]

    first_split = 4
    second_split = len(encoded) - 9

    await relay._handle_data_stream(
        publisher,
        StreamData(stream_id=6, data=encoded[:first_split], end_stream=False),
    )
    await relay._handle_data_stream(
        publisher,
        StreamData(stream_id=6, data=encoded[first_split:second_split], end_stream=False),
    )
    await relay._handle_data_stream(
        publisher,
        StreamData(stream_id=6, data=encoded[second_split:], end_stream=True),
    )

    sent = subscriber.quic_connection.sent_streams
    assert len(sent) == 1
    assert len({stream_id for stream_id, _, _ in sent}) == 1
    assert b"".join(chunk for _, chunk, _ in sent) == encoded
    assert sent[-1][2] is True
    assert relay._object_cache[track_name][0]["payload"] == payload

    logger.info("Fragmented subgroup stream forwarding test passed!\n")


@pytest.mark.asyncio
async def test_relay_prepends_subgroup_header_for_late_subscriber():
    """Test late subscribers receive a fresh subgroup header on their downstream stream."""
    logger.info("Testing late subscriber subgroup header forwarding at relay...")

    from moq.relay.relay import MOQRelay, ClientSession, InboundDataStream

    class DummyQuicConnection:
        def __init__(self):
            self.sent_streams = []
            self.next_stream_id = 2

        def get_next_available_stream_id(self, is_unidirectional=False):
            stream_id = self.next_stream_id
            self.next_stream_id += 4
            return stream_id

        def send_stream_data(self, stream_id, data, end_stream=False):
            self.sent_streams.append((stream_id, data, end_stream))

    class DummyProtocol:
        def __init__(self):
            self.transmit_calls = 0

        def transmit(self):
            self.transmit_calls += 1

    relay = MOQRelay(host="127.0.0.1", port=4443, cache_dir="/tmp/moq_test_cache_late_subscriber")

    track_name = FullTrackName([b"live"], b"video")
    subgroup_header = SubgroupHeader(
        track_alias=11,
        group_id=7,
        subgroup_id=0,
        publisher_priority=128,
    )
    first_payload = b"first-fragment" * 64
    second_payload = b"second-fragment" * 64

    state = InboundDataStream(
        subgroup_header=subgroup_header,
        track_name=track_name,
    )
    relay._append_forward_subgroup_object(state, SubgroupObject(object_id=1, payload=first_payload))

    # No subscribers yet: the relay should drop the buffered bytes without mutating future header behavior.
    await relay._flush_forward_buffer(track_name, state, end_stream=False)
    assert state.forward_buffer == bytearray()

    late_subscriber = ClientSession(
        session_id="late-subscriber",
        protocol=DummyProtocol(),
        quic_connection=DummyQuicConnection(),
    )
    relay._subscriptions[track_name] = [late_subscriber]

    relay._append_forward_subgroup_object(state, SubgroupObject(object_id=2, payload=second_payload))
    await relay._flush_forward_buffer(track_name, state, end_stream=False)

    sent = late_subscriber.quic_connection.sent_streams
    assert len(sent) == 1
    assert sent[0][1] == (
        VarInt.encode(StreamType.SUBGROUP_HEADER)
        + subgroup_header.encode()
        + SubgroupObject(object_id=2, payload=second_payload).encode()
    )
    assert sent[0][2] is False

    logger.info("Late subscriber subgroup header forwarding test passed!\n")


@pytest.mark.asyncio
async def test_cache():
    """Test caching functionality."""
    logger.info("Testing Cache...")
    
    from moq.relay import ObjectCache, CachedObject
    
    cache = ObjectCache(
        max_memory_size=10 * 1024 * 1024,  # 10MB
        disk_cache_dir="/tmp/moq_test_cache",
        max_disk_size=50 * 1024 * 1024     # 50MB
    )
    
    track_name = FullTrackName([b"test"], b"track")
    
    # Add objects to cache
    for i in range(5):
        obj = CachedObject(
            track_alias=1,
            group_id=1,
            object_id=i + 1,
            publisher_priority=128,
            payload=f"Object {i+1}".encode()
        )
        cache.put(track_name, obj)
    
    # Retrieve objects
    from moq.encoding import Location
    
    for i in range(1, 6):
        location = Location(group=1, object_id=i)
        cached = cache.get(track_name, location)
        assert cached is not None, f"Object {i} not found in cache"
        assert cached.object_id == i
        logger.info(f"  Retrieved object {i} from cache ✓")
    
    # Check statistics
    stats = cache.get_cache_stats()
    logger.info(f"  Cache stats: hits={stats['hits']}, misses={stats['misses']}")
    
    logger.info("Cache test passed!\n")


@pytest.mark.asyncio
async def test_relay_start_clears_disk_cache():
    """Test relay startup clears any existing disk cache."""
    logger.info("Testing Relay startup cache reset...")

    from moq.relay import MOQRelay

    class DummyQuicServer:
        def set_handlers(self, **kwargs):
            self.handlers = kwargs

        async def start(self):
            return None

        async def stop(self):
            return None

    with tempfile.TemporaryDirectory() as tmpdir:
        cache_dir = Path(tmpdir)
        stale_dir = cache_dir / "stale-track" / "1"
        stale_dir.mkdir(parents=True)
        (stale_dir / "1.obj").write_bytes(b"stale-object")
        (cache_dir / "cache_index.json").write_text(json.dumps({"stale-track": {"1": str(stale_dir / "1.obj")}}))

        relay = MOQRelay(
            host="127.0.0.1",
            port=0,
            cache_dir=str(cache_dir),
            max_memory_cache=1024,
            max_disk_cache=1024
        )
        relay._quic_server = DummyQuicServer()

        await relay.start()

        assert (cache_dir / "cache_index.json").exists()
        assert json.loads((cache_dir / "cache_index.json").read_text()) == {}
        assert not any(path.name == "stale-track" for path in cache_dir.iterdir())

    logger.info("Relay startup cache reset test passed!\n")


@pytest.mark.asyncio
async def test_relay_can_start_quic_and_webtransport_together():
    """Test relay can start both listeners in one instance."""
    logger.info("Testing dual-transport relay startup...")

    from moq.relay import MOQRelay

    class DummyServer:
        def __init__(self, name):
            self.name = name
            self.handlers = None
            self.started = 0
            self.stopped = 0

        def set_handlers(self, **kwargs):
            self.handlers = kwargs

        async def start(self):
            self.started += 1

        async def stop(self):
            self.stopped += 1

    relay = MOQRelay(
        host="127.0.0.1",
        port=4443,
        webtransport_port=4433,
        cache_dir=None,
        max_memory_cache=1024,
        max_disk_cache=1024,
        transport="both",
    )
    relay._quic_server = DummyServer("quic")
    relay._webtransport_server = DummyServer("webtransport")

    await relay.start()

    assert relay._quic_server.started == 1
    assert relay._webtransport_server.started == 1
    assert relay._quic_server.handlers is not None
    assert relay._webtransport_server.handlers is not None

    await relay.stop()

    assert relay._quic_server.stopped == 1
    assert relay._webtransport_server.stopped == 1

    logger.info("Dual-transport relay startup test passed!\n")


def test_relay_uses_combined_server_for_single_port_dual_transport():
    """Test relay chooses the combined server when both transports share one port."""
    logger.info("Testing single-port dual-transport relay selection...")

    from moq.relay import MOQRelay
    from moq.transport import CombinedTransportServer

    relay = MOQRelay(
        host="127.0.0.1",
        port=4443,
        cache_dir=None,
        max_memory_cache=1024,
        max_disk_cache=1024,
        transport="both",
    )

    assert isinstance(relay._combined_server, CombinedTransportServer)
    assert relay._quic_server is relay._combined_server
    assert relay._webtransport_server is relay._combined_server
    assert relay.webtransport_port == 4443

    logger.info("Single-port dual-transport relay selection test passed!\n")


@pytest.mark.asyncio
async def test_relay_registers_client_lazily_for_first_publish():
    """Test relay accepts the first publish even if connect callback is delayed."""
    logger.info("Testing lazy relay client registration...")

    from moq.relay import MOQRelay
    from moq.transport import StreamData
    from moq.messages import PublishMessage

    class DummyQuicConnection:
        def __init__(self):
            self.sent_streams = []

        def send_stream_data(self, stream_id, data, end_stream=False):
            self.sent_streams.append((stream_id, data, end_stream))

    class DummyProtocol:
        def __init__(self):
            self._quic = type("DummyQuic", (), {"host_cid": "lazy-client"})()
            self.transmit_calls = 0

        def transmit(self):
            self.transmit_calls += 1

    relay = MOQRelay(
        host="127.0.0.1",
        port=0,
        cache_dir=None,
        max_memory_cache=1024,
        max_disk_cache=1024
    )

    protocol = DummyProtocol()
    protocol._quic = DummyQuicConnection()
    protocol._quic.host_cid = "lazy-client"

    track_name = FullTrackName([b"time"], b"updates")
    publish_msg = PublishMessage(
        request_id=7,
        track_alias=3,
        full_track_name=track_name
    )

    await relay._on_quic_stream_data(
        protocol,
        StreamData(stream_id=0, data=publish_msg.encode(), end_stream=False)
    )

    assert "lazy-client" in relay._clients
    client = relay._clients["lazy-client"]
    assert client.control_stream_id == 0
    assert track_name in relay._publications
    assert client.publications[track_name]["request_id"] == 7
    assert protocol._quic.sent_streams, "relay did not send PUBLISH_OK"

    logger.info("Lazy relay client registration test passed!\n")


async def run_all_tests():
    """Run all integration tests."""
    logger.info("=" * 60)
    logger.info("MOQ Transport Integration Tests")
    logger.info("=" * 60 + "\n")
    
    # Run synchronous tests
    test_varint_encoding()
    test_full_track_name()
    test_setup_message()
    test_subscribe_message()
    test_publish_message()
    test_object_datagram()
    
    # Run async tests
    await test_session_management()
    await test_cache()
    await test_relay_start_clears_disk_cache()
    await test_relay_can_start_quic_and_webtransport_together()
    test_relay_uses_combined_server_for_single_port_dual_transport()
    await test_relay_registers_client_lazily_for_first_publish()
    
    logger.info("=" * 60)
    logger.info("All tests passed! ✓")
    logger.info("=" * 60)


def main():
    """Main entry point."""
    try:
        asyncio.run(run_all_tests())
        return 0
    except AssertionError as e:
        logger.error(f"Test failed: {e}")
        return 1
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())
