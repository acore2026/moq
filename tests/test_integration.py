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
    ObjectHeader, ObjectDatagram,
    GroupOrder, SubscribeFilter
)
from moq.encoding import Parameters


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
