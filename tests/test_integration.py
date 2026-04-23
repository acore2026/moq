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
from unittest.mock import AsyncMock, call

import pytest

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

from _path_helper import ensure_repo_root

ensure_repo_root()

from moq.encoding import FullTrackName, Location, VarInt
from moq.messages import (
    SetupMessage, SubscribeMessage, SubscribeOkMessage,
    PublishMessage, PublishOkMessage,
    ObjectHeader, ObjectDatagram, SubgroupHeader, SubgroupObject,
    StreamType,
    GroupOrder, SubscribeFilter, FetchMessage, FetchOkMessage,
    RequestOkMessage, RequestUpdateMessage,
    ParameterType, SubscriptionFilterValue,
    ObjectStatus, FetchHeader, FetchObject,
    PublishDoneMessage, PublishDoneStatus, StreamResetCode, decode_control_message,
)
from moq.encoding import Parameters
from moq.transport import StreamData, StreamResetData


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
        full_track_name=track_name,
        subscriber_priority=128,
        group_order=GroupOrder.ASCENDING,
        filter_type=SubscribeFilter.LATEST_OBJECT
    )
    
    encoded = msg.encode()
    
    # Skip message type prefix for decoding test
    decoded, consumed = SubscribeMessage.decode(encoded, offset=2)
    
    assert decoded.request_id == 1
    assert decoded.required_request_id_delta == 0
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
        required_request_id_delta=0,
        track_alias=5,
        full_track_name=track_name
    )
    
    encoded = msg.encode()
    decoded, _ = PublishMessage.decode(encoded, offset=2)
    
    assert decoded.request_id == 1
    assert decoded.required_request_id_delta == 0
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
    assert id2 == id1 + 2
    
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
    publisher._request_stream_ids[request_id] = 0
    publisher._active_tracks[request_id] = FullTrackName([b"time"], b"updates")

    encoded = PublishOkMessage(request_id=request_id).encode(include_request_id=False)
    split_at = len(encoded) // 2

    await publisher._handle_stream_data(None, StreamData(stream_id=0, data=encoded[:split_at]))
    assert not publisher._publish_waiters[request_id].is_set()

    await publisher._handle_stream_data(None, StreamData(stream_id=0, data=encoded[split_at:]))
    assert publisher._publish_waiters[request_id].is_set()

    logger.info("Fragmented PUBLISH_OK handling test passed!\n")


@pytest.mark.asyncio
async def test_publisher_request_stream_reset_rejects_pending_publish():
    """Test publisher unblocks and rejects a pending publish when its request stream resets."""
    from moq.pub.publisher import MOQPublisher
    from moq.session import MOQSession, Role

    publisher = MOQPublisher("127.0.0.1", 4443)
    publisher._session = MOQSession(session_id="pub-test", role=Role.PUBLISHER)

    track_name = FullTrackName([b"time"], b"updates")
    request_id = 9
    stream_id = 4
    observed = []
    publisher.set_handlers(
        on_publication_rejected=lambda tn, reason: observed.append((tn, reason))
    )

    await publisher._session.publish(track_name, request_id=request_id, stream_id=None)
    publisher._publications[track_name] = request_id
    publisher._active_tracks[request_id] = track_name
    publisher._publish_waiters[request_id] = asyncio.Event()
    publisher._request_stream_ids[request_id] = stream_id

    await publisher._handle_stream_reset(
        None,
        StreamResetData(stream_id=stream_id, error_code=0, event_type="reset"),
    )

    assert publisher._publish_waiters[request_id].is_set()
    assert track_name not in publisher._publications
    assert request_id not in publisher._active_tracks
    assert request_id not in publisher._request_stream_ids
    assert observed == [(track_name, "request stream reset: INTERNAL_ERROR")]


@pytest.mark.asyncio
async def test_publisher_request_stream_stop_sending_uses_protocol_reason():
    """Test publisher reports a protocol-aware reason for request stream STOP_SENDING."""
    from moq.pub.publisher import MOQPublisher
    from moq.session import MOQSession, Role

    publisher = MOQPublisher("127.0.0.1", 4443)
    publisher._session = MOQSession(session_id="pub-test", role=Role.PUBLISHER)

    track_name = FullTrackName([b"time"], b"updates")
    request_id = 9
    stream_id = 4
    observed = []
    publisher.set_handlers(
        on_publication_rejected=lambda tn, reason: observed.append((tn, reason))
    )

    await publisher._session.publish(track_name, request_id=request_id, stream_id=None)
    publisher._publications[track_name] = request_id
    publisher._active_tracks[request_id] = track_name
    publisher._publish_waiters[request_id] = asyncio.Event()
    publisher._request_stream_ids[request_id] = stream_id

    await publisher._handle_stream_reset(
        None,
        StreamResetData(
            stream_id=stream_id,
            error_code=int(StreamResetCode.SESSION_CLOSED),
            event_type="stop_sending",
        ),
    )

    assert publisher._publish_waiters[request_id].is_set()
    assert track_name not in publisher._publications
    assert request_id not in publisher._active_tracks
    assert request_id not in publisher._request_stream_ids
    assert observed == [(track_name, "request stream stop_sending: SESSION_CLOSED")]


@pytest.mark.asyncio
async def test_publisher_data_stream_reset_reopens_subgroup_state():
    """Test publisher forgets a reset subgroup stream so future sends can reopen it."""
    from moq.pub.publisher import MOQPublisher
    from moq.session import MOQSession, Role

    publisher = MOQPublisher("127.0.0.1", 4443)
    publisher._session = MOQSession(session_id="pub-test", role=Role.PUBLISHER)

    publisher._streams[(1, 2, 0)] = {"stream_id": 12, "last_object_id": 7}

    await publisher._handle_stream_reset(
        None,
        StreamResetData(stream_id=12, error_code=0, event_type="stop_sending"),
    )

    assert publisher._streams == {}


@pytest.mark.asyncio
async def test_publisher_unpublish_closes_track_streams_before_publish_done():
    """Test unpublish closes subgroup streams before sending TRACK_ENDED."""
    from moq.pub.publisher import MOQPublisher
    from moq.session import MOQSession, Publication, Role

    publisher = MOQPublisher("127.0.0.1", 4443)
    publisher._client = type("DummyClient", (), {"send_stream_data": AsyncMock()})()
    publisher._session = MOQSession(session_id="pub-test", role=Role.PUBLISHER)
    publisher._session.send_publish_done = AsyncMock()

    track_name = FullTrackName([b"time"], b"updates")
    other_track = FullTrackName([b"time"], b"other")
    request_id = 9
    other_request_id = 11

    publisher._publications[track_name] = request_id
    publisher._active_tracks[request_id] = track_name
    publisher._request_stream_ids[request_id] = 4

    publisher._session.publications[request_id] = Publication(
        request_id=request_id,
        track_alias=7,
        full_track_name=track_name,
        active=True,
    )
    publisher._session.publications[other_request_id] = Publication(
        request_id=other_request_id,
        track_alias=8,
        full_track_name=other_track,
        active=True,
    )

    publisher._streams[(7, 1, 0)] = {"stream_id": 12, "last_object_id": 3}
    publisher._streams[(7, 2, 0)] = {"stream_id": 16, "last_object_id": 5}
    publisher._streams[(8, 1, 0)] = {"stream_id": 20, "last_object_id": 1}

    await publisher.unpublish(track_name, reason="done")

    assert publisher._client.send_stream_data.await_args_list == [
        call(12, b"", end_stream=True),
        call(16, b"", end_stream=True),
    ]
    publisher._session.send_publish_done.assert_awaited_once_with(
        request_id,
        int(PublishDoneStatus.TRACK_ENDED),
        "done",
        stream_count=2,
    )
    assert (7, 1, 0) not in publisher._streams
    assert (7, 2, 0) not in publisher._streams
    assert (8, 1, 0) in publisher._streams
    assert track_name not in publisher._publications
    assert request_id not in publisher._active_tracks
    assert request_id not in publisher._request_stream_ids
    assert request_id not in publisher._session.publications
    assert other_request_id in publisher._session.publications


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
    encoded = PublishMessage(
        request_id=3,
        required_request_id_delta=0,
        track_alias=11,
        full_track_name=track_name,
    ).encode()
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
async def test_relay_new_publication_clears_stale_track_cache(tmp_path):
    """Test a fresh publication resets stale cache for the same track."""
    from moq.relay.relay import MOQRelay, ClientSession, CachedObject

    class DummyQuicConnection:
        def __init__(self):
            self.sent_streams = []

        def get_next_available_stream_id(self, is_unidirectional=False):
            return 2

        def send_stream_data(self, stream_id, data, end_stream=False):
            self.sent_streams.append((stream_id, data, end_stream))

    class DummyProtocol:
        def transmit(self):
            return None

    relay = MOQRelay(host="127.0.0.1", port=4443, cache_dir=str(tmp_path / "relay-cache"))
    track_name = FullTrackName([b"video"], b"h264-live")
    protocol = DummyProtocol()
    client = ClientSession(
        session_id="publisher",
        protocol=protocol,
        quic_connection=DummyQuicConnection(),
    )

    relay._object_cache[track_name] = [{
        "track_alias": 1,
        "group_id": 1,
        "object_id": 1,
        "publisher_priority": 128,
        "object_status": ObjectStatus.NORMAL,
        "payload": b"stale",
    }]
    relay.cache.put(
        track_name,
        CachedObject(
            track_alias=1,
            group_id=1,
            object_id=1,
            publisher_priority=128,
            payload=b"stale",
        ),
    )

    await relay._handle_publish(
        client,
        PublishMessage(
            request_id=7,
            required_request_id_delta=0,
            full_track_name=track_name,
            track_alias=3,
        ),
    )

    assert relay._object_cache.get(track_name) is None
    assert relay.cache.get(track_name, Location(1, 1)) is None


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


@pytest.mark.asyncio
async def test_subscriber_decodes_sequential_subgroup_object_deltas():
    """Test subscriber decodes draft-17 subgroup object deltas on one stream."""
    from moq.sub.subscriber import MOQSubscriber

    subscriber = MOQSubscriber("127.0.0.1", 4443)
    track_name = FullTrackName([b"live"], b"video")
    track_alias = 10
    subscriber._track_aliases[track_alias] = track_name

    first = SubgroupObject(object_id=7, payload=b"first")
    second = SubgroupObject(object_id=8, payload=b"second")

    encoded = (
        VarInt.encode(StreamType.SUBGROUP_HEADER)
        + SubgroupHeader(
            track_alias=track_alias,
            group_id=2,
            subgroup_id=0,
            publisher_priority=128,
        ).encode()
        + first.encode()
        + second.encode(previous_object_id=first.object_id)
    )

    await subscriber._handle_stream_data(
        None, StreamData(stream_id=8, data=encoded, end_stream=True)
    )

    first_obj = await subscriber.get_next_object(timeout=0.1)
    second_obj = await subscriber.get_next_object(timeout=0.1)
    assert first_obj is not None
    assert second_obj is not None
    assert first_obj.object_id == 7
    assert first_obj.payload == b"first"
    assert second_obj.object_id == 8
    assert second_obj.payload == b"second"

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
    subscriber.subscriptions[track_name] = {
        "track_alias": 17,
        "request_id": 5,
        "filter_type": SubscribeFilter.LATEST_OBJECT,
        "start_group": None,
        "start_object": None,
        "end_group": None,
        "end_object": None,
    }
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
    await asyncio.sleep(0)
    await asyncio.sleep(0)

    sent = subscriber.quic_connection.sent_streams
    assert len(sent) == 1
    assert len({stream_id for stream_id, _, _ in sent}) == 1
    assert b"".join(chunk for _, chunk, _ in sent) == (
        VarInt.encode(StreamType.SUBGROUP_HEADER)
        + SubgroupHeader(
            track_alias=17,
            group_id=7,
            subgroup_id=0,
            publisher_priority=128,
        ).encode()
        + SubgroupObject(object_id=3, payload=payload).encode()
    )
    assert sent[-1][2] is True
    assert relay._object_cache[track_name][0]["payload"] == payload

    logger.info("Fragmented subgroup stream forwarding test passed!\n")


@pytest.mark.asyncio
async def test_relay_flushes_complete_subgroup_object_without_waiting_for_fin():
    """Test relay forwards a parsed subgroup object immediately on live streams."""
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
        def transmit(self):
            return None

    relay = MOQRelay(host="127.0.0.1", port=4443, cache_dir="/tmp/moq_test_cache_live_flush")

    track_name = FullTrackName([b"live"], b"video")
    track_alias = 11
    payload = b"live-fragment" * 32
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
    subscriber.subscriptions[track_name] = {
        "track_alias": 17,
        "request_id": 5,
        "filter_type": SubscribeFilter.LATEST_OBJECT,
        "start_group": None,
        "start_object": None,
        "end_group": None,
        "end_object": None,
    }
    relay._subscriptions[track_name] = [subscriber]

    await relay._handle_data_stream(
        publisher,
        StreamData(stream_id=6, data=encoded, end_stream=False),
    )
    await asyncio.sleep(0)
    await asyncio.sleep(0)

    sent = subscriber.quic_connection.sent_streams
    assert len(sent) == 1
    assert sent[0][1] == (
        VarInt.encode(StreamType.SUBGROUP_HEADER)
        + SubgroupHeader(
            track_alias=17,
            group_id=7,
            subgroup_id=0,
            publisher_priority=128,
        ).encode()
        + SubgroupObject(object_id=3, payload=payload).encode()
    )
    assert sent[0][2] is False


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
    assert state.forward_objects == []
    assert state.buffered_forward_bytes == 0

    late_subscriber = ClientSession(
        session_id="late-subscriber",
        protocol=DummyProtocol(),
        quic_connection=DummyQuicConnection(),
    )
    late_subscriber.subscriptions[track_name] = {
        "track_alias": 23,
        "request_id": 6,
        "filter_type": SubscribeFilter.LATEST_OBJECT,
        "start_group": None,
        "start_object": None,
        "end_group": None,
        "end_object": None,
    }
    relay._subscriptions[track_name] = [late_subscriber]

    relay._append_forward_subgroup_object(state, SubgroupObject(object_id=2, payload=second_payload))
    await relay._flush_forward_buffer(track_name, state, end_stream=False)
    await asyncio.sleep(0)
    await asyncio.sleep(0)

    sent = late_subscriber.quic_connection.sent_streams
    assert len(sent) == 1
    assert sent[0][1] == (
        VarInt.encode(StreamType.SUBGROUP_HEADER)
        + SubgroupHeader(
            track_alias=23,
            group_id=7,
            subgroup_id=0,
            publisher_priority=128,
        ).encode()
        + SubgroupObject(object_id=2, payload=second_payload).encode()
    )
    assert sent[0][2] is False

    logger.info("Late subscriber subgroup header forwarding test passed!\n")


@pytest.mark.asyncio
async def test_relay_rewrites_track_alias_per_subscriber_for_datagrams():
    """Test relay rewrites publisher aliases to each subscriber alias."""
    logger.info("Testing datagram alias rewrite at relay...")

    from moq.relay.relay import MOQRelay, ClientSession

    class DummyQuicConnection:
        def __init__(self):
            self.sent_datagrams = []

        def send_datagram_frame(self, data):
            self.sent_datagrams.append(data)

    class DummyProtocol:
        def __init__(self):
            self.transmit_calls = 0

        def transmit(self):
            self.transmit_calls += 1

    relay = MOQRelay(host="127.0.0.1", port=4443, cache_dir="/tmp/moq_test_cache_alias_rewrite")
    track_name = FullTrackName([b"live"], b"video")

    subscriber_a = ClientSession(
        session_id="subscriber-a",
        protocol=DummyProtocol(),
        quic_connection=DummyQuicConnection(),
    )
    subscriber_a.subscriptions[track_name] = {
        "track_alias": 31,
        "request_id": 1,
        "filter_type": SubscribeFilter.LATEST_OBJECT,
        "start_group": None,
        "start_object": None,
        "end_group": None,
        "end_object": None,
    }

    subscriber_b = ClientSession(
        session_id="subscriber-b",
        protocol=DummyProtocol(),
        quic_connection=DummyQuicConnection(),
    )
    subscriber_b.subscriptions[track_name] = {
        "track_alias": 32,
        "request_id": 2,
        "filter_type": SubscribeFilter.LATEST_OBJECT,
        "start_group": None,
        "start_object": None,
        "end_group": None,
        "end_object": None,
    }

    relay._subscriptions[track_name] = [subscriber_a, subscriber_b]

    await relay._forward_object(
        track_name,
        ObjectDatagram(
            header=ObjectHeader(
                track_alias=7,
                group_id=5,
                object_id=9,
                publisher_priority=128,
            ),
            payload=b"fanout",
        ),
    )
    await asyncio.sleep(0)
    await asyncio.sleep(0)

    datagram_a, _ = ObjectDatagram.decode(subscriber_a.quic_connection.sent_datagrams[0])
    datagram_b, _ = ObjectDatagram.decode(subscriber_b.quic_connection.sent_datagrams[0])

    assert datagram_a.header.track_alias == 31
    assert datagram_b.header.track_alias == 32
    assert datagram_a.payload == b"fanout"
    assert datagram_b.payload == b"fanout"

    logger.info("Datagram alias rewrite test passed!\n")


@pytest.mark.asyncio
async def test_relay_filters_objects_per_subscriber_range():
    """Test relay applies subscriber ranges independently during fanout."""
    logger.info("Testing subscriber-specific filtering at relay...")

    from moq.relay.relay import MOQRelay, ClientSession

    class DummyQuicConnection:
        def __init__(self):
            self.sent_datagrams = []

        def send_datagram_frame(self, data):
            self.sent_datagrams.append(data)

    class DummyProtocol:
        def __init__(self):
            self.transmit_calls = 0

        def transmit(self):
            self.transmit_calls += 1

    relay = MOQRelay(host="127.0.0.1", port=4443, cache_dir="/tmp/moq_test_cache_filtering")
    track_name = FullTrackName([b"live"], b"video")

    ranged_subscriber = ClientSession(
        session_id="ranged",
        protocol=DummyProtocol(),
        quic_connection=DummyQuicConnection(),
    )
    ranged_subscriber.subscriptions[track_name] = {
        "track_alias": 41,
        "request_id": 1,
        "filter_type": SubscribeFilter.ABSOLUTE_RANGE,
        "start_group": 2,
        "start_object": 2,
        "end_group": 2,
        "end_object": 3,
    }

    live_subscriber = ClientSession(
        session_id="live",
        protocol=DummyProtocol(),
        quic_connection=DummyQuicConnection(),
    )
    live_subscriber.subscriptions[track_name] = {
        "track_alias": 42,
        "request_id": 2,
        "filter_type": SubscribeFilter.LATEST_OBJECT,
        "start_group": None,
        "start_object": None,
        "end_group": None,
        "end_object": None,
    }

    relay._subscriptions[track_name] = [ranged_subscriber, live_subscriber]

    for object_id in (1, 2, 4):
        await relay._forward_object(
            track_name,
            ObjectDatagram(
                header=ObjectHeader(
                    track_alias=7,
                    group_id=2,
                    object_id=object_id,
                    publisher_priority=128,
                ),
                payload=f"obj-{object_id}".encode(),
            ),
        )
    await asyncio.sleep(0)
    await asyncio.sleep(0)

    assert len(ranged_subscriber.quic_connection.sent_datagrams) == 1
    filtered_obj, _ = ObjectDatagram.decode(ranged_subscriber.quic_connection.sent_datagrams[0])
    assert filtered_obj.header.object_id == 2

    assert len(live_subscriber.quic_connection.sent_datagrams) == 3

    logger.info("Subscriber-specific filtering test passed!\n")


@pytest.mark.asyncio
async def test_relay_fetch_requests_do_not_join_live_broadcast_list():
    """Test fetch requests stay separate from live fanout subscribers."""
    logger.info("Testing fetch isolation from live subscriptions at relay...")

    from moq.relay.relay import MOQRelay, ClientSession

    class DummyQuicConnection:
        def __init__(self):
            self.sent_datagrams = []
            self.sent_streams = []
            self.next_stream_id = 2

        def get_next_available_stream_id(self, is_unidirectional=False):
            stream_id = self.next_stream_id
            self.next_stream_id += 4
            return stream_id

        def send_stream_data(self, stream_id, data, end_stream=False):
            self.sent_streams.append((stream_id, data, end_stream))

        def send_datagram_frame(self, data):
            self.sent_datagrams.append(data)

    class DummyProtocol:
        def __init__(self):
            self.transmit_calls = 0

        def transmit(self):
            self.transmit_calls += 1

    relay = MOQRelay(host="127.0.0.1", port=4443, cache_dir="/tmp/moq_test_cache_fetch_isolation")
    track_name = FullTrackName([b"archive"], b"video")

    fetch_client = ClientSession(
        session_id="fetch-client",
        protocol=DummyProtocol(),
        quic_connection=DummyQuicConnection(),
    )

    relay._object_cache[track_name] = [{
        "track_alias": 7,
        "group_id": 1,
        "object_id": 1,
        "publisher_priority": 128,
        "object_status": ObjectStatus.NORMAL,
        "payload": b"cached",
    }]

    await relay._handle_fetch(
        fetch_client,
        FetchMessage(
            request_id=9,
            full_track_name=track_name,
            subscriber_priority=128,
            group_order=GroupOrder.ASCENDING,
            start_group=1,
            start_object=1,
            end_group=1,
            end_object=1,
        ),
    )

    assert track_name not in relay._subscriptions
    assert fetch_client.quic_connection.sent_streams, "relay did not send stream responses"

    by_stream_id = {}
    for stream_id, chunk, end_stream in fetch_client.quic_connection.sent_streams:
        entry = by_stream_id.setdefault(stream_id, {"chunks": [], "end": False})
        entry["chunks"].append(chunk)
        entry["end"] = entry["end"] or end_stream

    assert len(by_stream_id) == 2

    control_stream_id = min(by_stream_id)
    fetch_stream_id = max(by_stream_id)

    response_bytes = b"".join(by_stream_id[control_stream_id]["chunks"])
    response, _ = decode_control_message(response_bytes)
    assert response.request_id == 9
    assert isinstance(response, FetchOkMessage)
    assert response.end_of_track is False
    assert response.end_location.group == 1
    assert response.end_location.object_id == 1

    fetch_bytes = b"".join(by_stream_id[fetch_stream_id]["chunks"])
    stream_type, consumed = VarInt.decode(fetch_bytes, 0)
    assert stream_type == StreamType.FETCH_HEADER
    fetch_header, header_consumed = FetchHeader.decode(fetch_bytes, consumed)
    assert fetch_header.subscribe_id == 9

    fetched_obj, _ = FetchObject.decode(fetch_bytes[consumed + header_consumed:])
    assert fetched_obj.payload == b"cached"

    logger.info("Fetch isolation test passed!\n")


@pytest.mark.asyncio
async def test_relay_request_update_merges_subscription_parameters():
    """Test relay handles REQUEST_UPDATE on an existing subscription."""
    from moq.relay.relay import MOQRelay, ClientSession

    class DummyQuicConnection:
        def __init__(self):
            self.sent_streams = []
            self.sent_datagrams = []
            self.next_stream_id = 0

        def get_next_available_stream_id(self, is_unidirectional=False):
            stream_id = self.next_stream_id
            self.next_stream_id += 4
            return stream_id

        def send_stream_data(self, stream_id, data, end_stream=False):
            self.sent_streams.append((stream_id, data, end_stream))

        def send_datagram_frame(self, data):
            self.sent_datagrams.append(data)

    class DummyProtocol:
        def transmit(self):
            pass

    relay = MOQRelay(host="127.0.0.1", port=4443, cache_dir="/tmp/moq_test_cache_request_update")
    track_name = FullTrackName([b"live"], b"video")
    client = ClientSession(
        session_id="update-client",
        protocol=DummyProtocol(),
        quic_connection=DummyQuicConnection(),
    )

    relay._object_cache[track_name] = [
        {
            "track_alias": 1,
            "group_id": 1,
            "object_id": 1,
            "publisher_priority": 128,
            "object_status": ObjectStatus.NORMAL,
            "payload": b"before",
        },
        {
            "track_alias": 1,
            "group_id": 1,
            "object_id": 2,
            "publisher_priority": 128,
            "object_status": ObjectStatus.NORMAL,
            "payload": b"after",
        },
    ]

    initial = Parameters()
    initial.set(ParameterType.SUBSCRIBER_PRIORITY, 1)
    update = Parameters()
    update.set(ParameterType.GROUP_ORDER, 0x2)
    update.set(
        ParameterType.SUBSCRIPTION_FILTER,
        SubscriptionFilterValue(
            filter_type=SubscribeFilter.ABSOLUTE_RANGE,
            start_group=1,
            start_object=2,
            end_group_delta=0,
        ).encode(),
    )

    await relay._handle_subscribe(
        client,
        SubscribeMessage(
            request_id=8,
            full_track_name=track_name,
            subscriber_priority=128,
            group_order=GroupOrder.ASCENDING,
            filter_type=SubscribeFilter.LATEST_OBJECT,
            parameters=initial,
        ),
    )

    await relay._handle_request_update(
        client,
        RequestUpdateMessage(
            request_id=8,
            parameters=update,
        ),
    )

    subscription = client.subscriptions[track_name]
    assert subscription["parameters"].get(ParameterType.SUBSCRIBER_PRIORITY) == 1
    assert subscription["parameters"].get(ParameterType.GROUP_ORDER) == 0x2
    assert subscription["filter_type"] == SubscribeFilter.ABSOLUTE_RANGE
    assert subscription["start_group"] == 1
    assert subscription["start_object"] == 2
    assert subscription["end_group"] == 1
    assert subscription["end_object"] is None

    response_bytes = client.quic_connection.sent_streams[-1][1]
    response, _ = decode_control_message(response_bytes)
    assert isinstance(response, RequestOkMessage)
    assert response.request_id == 8
    assert response.parameters.get(ParameterType.SUBSCRIBER_PRIORITY) == 1
    assert response.parameters.get(ParameterType.GROUP_ORDER) == 0x2
    assert len(client.quic_connection.sent_datagrams) == 1
    backfilled, _ = ObjectDatagram.decode(client.quic_connection.sent_datagrams[0])
    assert backfilled.header.object_id == 2
    assert backfilled.payload == b"after"


@pytest.mark.asyncio
async def test_relay_slow_subscriber_does_not_block_other_subscribers():
    """Test one stalled subscriber no longer blocks fanout to others."""
    logger.info("Testing slow subscriber isolation at relay...")

    from moq.relay.relay import MOQRelay, ClientSession

    gate = asyncio.Event()

    class SlowQuicConnection:
        def __init__(self):
            self.sent_datagrams = []

        async def send_datagram_frame(self, data):
            await gate.wait()
            self.sent_datagrams.append(data)

    class FastQuicConnection:
        def __init__(self):
            self.sent_datagrams = []

        def send_datagram_frame(self, data):
            self.sent_datagrams.append(data)

    class DummyProtocol:
        def __init__(self):
            self.transmit_calls = 0

        def transmit(self):
            self.transmit_calls += 1

    relay = MOQRelay(host="127.0.0.1", port=4443, cache_dir="/tmp/moq_test_cache_slow_subscriber")
    track_name = FullTrackName([b"live"], b"video")

    slow_subscriber = ClientSession(
        session_id="slow",
        protocol=DummyProtocol(),
        quic_connection=SlowQuicConnection(),
    )
    slow_subscriber.subscriptions[track_name] = {
        "track_alias": 51,
        "request_id": 1,
        "filter_type": SubscribeFilter.LATEST_OBJECT,
        "start_group": None,
        "start_object": None,
        "end_group": None,
        "end_object": None,
    }

    fast_subscriber = ClientSession(
        session_id="fast",
        protocol=DummyProtocol(),
        quic_connection=FastQuicConnection(),
    )
    fast_subscriber.subscriptions[track_name] = {
        "track_alias": 52,
        "request_id": 2,
        "filter_type": SubscribeFilter.LATEST_OBJECT,
        "start_group": None,
        "start_object": None,
        "end_group": None,
        "end_object": None,
    }

    relay._subscriptions[track_name] = [slow_subscriber, fast_subscriber]

    await asyncio.wait_for(
        relay._forward_object(
            track_name,
            ObjectDatagram(
                header=ObjectHeader(
                    track_alias=7,
                    group_id=9,
                    object_id=1,
                    publisher_priority=128,
                ),
                payload=b"isolation-check",
            ),
        ),
        timeout=0.05,
    )

    await asyncio.sleep(0)
    await asyncio.sleep(0)

    assert fast_subscriber.quic_connection.sent_datagrams, "fast subscriber did not receive queued object"
    assert slow_subscriber.quic_connection.sent_datagrams == []

    gate.set()
    await asyncio.sleep(0)
    await asyncio.sleep(0)

    assert slow_subscriber.quic_connection.sent_datagrams, "slow subscriber never drained after gate release"

    await relay._cleanup_client(slow_subscriber)
    await relay._cleanup_client(fast_subscriber)

    logger.info("Slow subscriber isolation test passed!\n")


@pytest.mark.asyncio
async def test_relay_disconnect_notifies_subscription_end_with_publish_done():
    """Test relay sends PUBLISH_DONE before disconnecting a slow subscriber."""
    logger.info("Testing relay publish_done notification on disconnect...")

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
            self.closed = False

        def close(self):
            self.closed = True

    relay = MOQRelay(host="127.0.0.1", port=4443, cache_dir="/tmp/moq_test_cache_publish_done_disconnect")
    track_name = FullTrackName([b"live"], b"video")

    subscriber = ClientSession(
        session_id="lagging-subscriber",
        protocol=DummyProtocol(),
        quic_connection=DummyQuicConnection(),
    )
    subscriber.subscriptions[track_name] = {
        "track_alias": 61,
        "request_id": 12,
        "filter_type": SubscribeFilter.LATEST_OBJECT,
        "start_group": None,
        "start_object": None,
        "end_group": None,
        "end_object": None,
        "opened_data_stream_ids": set(),
    }
    relay._clients[subscriber.session_id] = subscriber
    relay._subscriptions[track_name] = [subscriber]

    await relay._disconnect_client(subscriber, "outbound send queue full")

    assert subscriber.protocol.closed is True
    assert track_name not in relay._subscriptions

    sent_bytes = b"".join(chunk for _, chunk, _ in subscriber.quic_connection.sent_streams)
    msg, _ = decode_control_message(sent_bytes)
    assert isinstance(msg, PublishDoneMessage)
    assert msg.status_code == int(PublishDoneStatus.TOO_FAR_BEHIND)
    assert msg.stream_count == 0
    assert msg.reason == "outbound send queue full"

    logger.info("Relay publish_done notification test passed!\n")


@pytest.mark.asyncio
async def test_relay_prioritizes_publish_done_over_queued_data():
    """Test disconnect notifications are sent ahead of already queued data sends."""
    logger.info("Testing control priority over queued data at relay...")

    from moq.relay.relay import MOQRelay, ClientSession, SEND_PRIORITY_DATA

    class DummyQuicConnection:
        def __init__(self):
            self.sent_streams = []
            self.next_stream_id = 2
            self.first_data_send = True

        def get_next_available_stream_id(self, is_unidirectional=False):
            stream_id = self.next_stream_id
            self.next_stream_id += 4
            return stream_id

        async def send_stream_data(self, stream_id, data, end_stream=False):
            if self.first_data_send and data == b"queued-data-1":
                self.first_data_send = False
                await asyncio.sleep(0.02)
            self.sent_streams.append((stream_id, data, end_stream))

    class DummyProtocol:
        def __init__(self):
            self.closed = False

        def close(self):
            self.closed = True

    relay = MOQRelay(host="127.0.0.1", port=4443, cache_dir="/tmp/moq_test_cache_control_priority")
    track_name = FullTrackName([b"live"], b"video")

    subscriber = ClientSession(
        session_id="priority-subscriber",
        protocol=DummyProtocol(),
        quic_connection=DummyQuicConnection(),
    )
    subscriber.subscriptions[track_name] = {
        "track_alias": 71,
        "request_id": 14,
        "filter_type": SubscribeFilter.LATEST_OBJECT,
        "start_group": None,
        "start_object": None,
        "end_group": None,
        "end_object": None,
        "opened_data_stream_ids": set(),
    }
    relay._clients[subscriber.session_id] = subscriber
    relay._subscriptions[track_name] = [subscriber]

    stream_id = await relay._open_stream(subscriber, unidirectional=True)
    await relay._enqueue_client_send(
        subscriber,
        lambda: relay._send_stream_bytes(subscriber, stream_id, b"queued-data-1", end_stream=False),
        description="queued data one",
        priority=SEND_PRIORITY_DATA,
        wait=False,
    )
    await relay._enqueue_client_send(
        subscriber,
        lambda: relay._send_stream_bytes(subscriber, stream_id, b"queued-data-2", end_stream=False),
        description="queued data two",
        priority=SEND_PRIORITY_DATA,
        wait=False,
    )
    await asyncio.sleep(0)

    await relay._disconnect_client(subscriber, "outbound send queue full")

    payloads = [data for _, data, _ in subscriber.quic_connection.sent_streams]
    assert b"queued-data-1" in payloads
    assert b"queued-data-2" not in payloads

    publish_done_index = next(
        i for i, payload in enumerate(payloads)
        if payload != b"queued-data-1"
    )
    msg, _ = decode_control_message(payloads[publish_done_index])
    assert isinstance(msg, PublishDoneMessage)
    assert msg.status_code == int(PublishDoneStatus.TOO_FAR_BEHIND)
    assert msg.stream_count == 0

    logger.info("Control priority test passed!\n")


@pytest.mark.asyncio
async def test_relay_publish_done_reports_exact_opened_stream_count():
    """Test relay reports the exact number of subscription data streams it opened."""
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
            self.closed = False

        def close(self):
            self.closed = True

    relay = MOQRelay(host="127.0.0.1", port=4443, cache_dir="/tmp/moq_test_cache_publish_done_exact_count")
    track_name = FullTrackName([b"live"], b"video")

    subscriber = ClientSession(
        session_id="counted-subscriber",
        protocol=DummyProtocol(),
        quic_connection=DummyQuicConnection(),
    )
    subscriber.subscriptions[track_name] = {
        "track_alias": 81,
        "request_id": 16,
        "subscriber_priority": 128,
        "group_order": GroupOrder.ASCENDING,
        "filter_type": SubscribeFilter.LATEST_OBJECT,
        "start_group": None,
        "start_object": None,
        "end_group": None,
        "end_object": None,
        "opened_data_stream_ids": set(),
    }
    relay._clients[subscriber.session_id] = subscriber
    relay._subscriptions[track_name] = [subscriber]

    state = InboundDataStream(
        subgroup_header=SubgroupHeader(
            track_alias=3,
            group_id=1,
            subgroup_id=0,
            publisher_priority=128,
        ),
        track_name=track_name,
    )
    state.forward_objects.append(SubgroupObject(object_id=1, payload=b"hello"))
    state.buffered_forward_bytes = len(state.forward_objects[0].encode())

    await relay._flush_forward_buffer(track_name, state, end_stream=True)
    await relay._disconnect_client(subscriber, "outbound send queue full")

    control_payload = subscriber.quic_connection.sent_streams[-1][1]
    msg, _ = decode_control_message(control_payload)
    assert isinstance(msg, PublishDoneMessage)
    assert msg.stream_count == 1


@pytest.mark.asyncio
async def test_relay_stream_reset_cleans_request_and_data_state():
    from moq.relay.relay import MOQRelay, ClientSession, InboundDataStream
    from moq.transport import StreamResetData

    class DummyProtocol:
        pass

    protocol = DummyProtocol()
    relay = MOQRelay(host="127.0.0.1", port=4443, cache_dir=None)
    client = ClientSession(
        session_id="reset-client",
        protocol=protocol,
        quic_connection=object(),
    )
    client.control_stream_id = 2
    client.control_buffer = b"control"
    client.request_stream_buffers[4] = bytearray(b"request")
    client.data_streams[6] = InboundDataStream()
    relay._clients[client.session_id] = client

    await relay._on_quic_stream_reset(
        protocol,
        StreamResetData(stream_id=4, error_code=0, event_type="reset"),
    )
    await relay._on_quic_stream_reset(
        protocol,
        StreamResetData(stream_id=6, error_code=0, event_type="reset"),
    )
    await relay._on_quic_stream_reset(
        protocol,
        StreamResetData(stream_id=2, error_code=0, event_type="reset"),
    )

    assert 4 not in client.request_stream_buffers
    assert 6 not in client.data_streams
    assert client.control_stream_id is None
    assert client.control_buffer == b""


@pytest.mark.asyncio
async def test_relay_request_stream_reset_cancels_subscription_lifecycle():
    from moq.relay.relay import MOQRelay, ClientSession
    from moq.transport import StreamResetData

    class DummyProtocol:
        pass

    relay = MOQRelay(host="127.0.0.1", port=4443, cache_dir=None)
    track_name = FullTrackName([b"live"], b"video")
    protocol = DummyProtocol()
    client = ClientSession(
        session_id="sub-client",
        protocol=protocol,
        quic_connection=object(),
    )
    client.subscriptions[track_name] = {
        "track_alias": 21,
        "request_id": 31,
        "request_stream_id": 4,
        "subscriber_priority": 128,
        "group_order": GroupOrder.ASCENDING,
        "filter_type": SubscribeFilter.LATEST_OBJECT,
        "parameters": None,
        "start_group": None,
        "start_object": None,
        "end_group": None,
        "end_object": None,
        "opened_data_stream_ids": set(),
    }
    relay._clients[client.session_id] = client
    relay._subscriptions[track_name] = [client]
    relay.subscriber_sessions[track_name] = [client]

    await relay._on_quic_stream_reset(
        protocol,
        StreamResetData(stream_id=4, error_code=0, event_type="reset"),
    )

    assert track_name not in client.subscriptions
    assert track_name not in relay._subscriptions
    assert track_name not in relay.subscriber_sessions


@pytest.mark.asyncio
async def test_relay_request_stream_reset_cancels_publication_lifecycle():
    from moq.relay.relay import MOQRelay, ClientSession
    from moq.transport import StreamResetData

    class DummyProtocol:
        pass

    relay = MOQRelay(host="127.0.0.1", port=4443, cache_dir=None)
    track_name = FullTrackName([b"live"], b"video")
    protocol = DummyProtocol()
    client = ClientSession(
        session_id="pub-client",
        protocol=protocol,
        quic_connection=object(),
    )
    client.publications[track_name] = {
        "track_alias": 11,
        "request_id": 41,
        "request_stream_id": 8,
        "parameters": None,
        "track_properties": b"",
    }
    relay._clients[client.session_id] = client
    relay._publications[track_name] = client
    relay.publisher_sessions[track_name] = [client]

    await relay._on_quic_stream_reset(
        protocol,
        StreamResetData(stream_id=8, error_code=0, event_type="stop_sending"),
    )

    assert track_name not in client.publications
    assert track_name not in relay._publications
    assert track_name not in relay.publisher_sessions


@pytest.mark.asyncio
async def test_relay_publication_reset_notifies_and_removes_downstream_subscriptions():
    from moq.relay.relay import MOQRelay, ClientSession
    from moq.transport import StreamResetData

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
        pass

    relay = MOQRelay(host="127.0.0.1", port=4443, cache_dir=None)
    track_name = FullTrackName([b"live"], b"video")

    publisher = ClientSession(
        session_id="pub-client",
        protocol=DummyProtocol(),
        quic_connection=object(),
    )
    publisher.publications[track_name] = {
        "track_alias": 11,
        "request_id": 41,
        "request_stream_id": 8,
        "parameters": None,
        "track_properties": b"",
    }

    subscriber = ClientSession(
        session_id="sub-client",
        protocol=DummyProtocol(),
        quic_connection=DummyQuicConnection(),
    )
    subscriber.subscriptions[track_name] = {
        "track_alias": 21,
        "request_id": 51,
        "request_stream_id": 4,
        "subscriber_priority": 128,
        "group_order": GroupOrder.ASCENDING,
        "filter_type": SubscribeFilter.LATEST_OBJECT,
        "parameters": None,
        "start_group": None,
        "start_object": None,
        "end_group": None,
        "end_object": None,
        "opened_data_stream_ids": set(),
    }

    relay._clients[publisher.session_id] = publisher
    relay._clients[subscriber.session_id] = subscriber
    relay._publications[track_name] = publisher
    relay.publisher_sessions[track_name] = [publisher]
    relay._subscriptions[track_name] = [subscriber]
    relay.subscriber_sessions[track_name] = [subscriber]

    await relay._on_quic_stream_reset(
        publisher.protocol,
        StreamResetData(stream_id=8, error_code=0, event_type="reset"),
    )

    assert track_name not in publisher.publications
    assert track_name not in relay._publications
    assert track_name not in subscriber.subscriptions
    assert track_name not in relay._subscriptions

    sent_payload = b"".join(chunk for _, chunk, _ in subscriber.quic_connection.sent_streams)
    msg, _ = decode_control_message(sent_payload)
    assert isinstance(msg, PublishDoneMessage)
    assert msg.request_id == 51
    assert msg.status_code == int(PublishDoneStatus.INTERNAL_ERROR)
    assert msg.reason == "publication request stream reset: INTERNAL_ERROR"


@pytest.mark.asyncio
async def test_relay_publication_stop_sending_maps_session_closed_to_going_away():
    from moq.relay.relay import MOQRelay, ClientSession
    from moq.transport import StreamResetData

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
        pass

    relay = MOQRelay(host="127.0.0.1", port=4443, cache_dir=None)
    track_name = FullTrackName([b"live"], b"video")

    publisher = ClientSession(
        session_id="pub-client",
        protocol=DummyProtocol(),
        quic_connection=object(),
    )
    publisher.publications[track_name] = {
        "track_alias": 11,
        "request_id": 41,
        "request_stream_id": 8,
        "parameters": None,
        "track_properties": b"",
    }

    subscriber = ClientSession(
        session_id="sub-client",
        protocol=DummyProtocol(),
        quic_connection=DummyQuicConnection(),
    )
    subscriber.subscriptions[track_name] = {
        "track_alias": 21,
        "request_id": 51,
        "request_stream_id": 4,
        "subscriber_priority": 128,
        "group_order": GroupOrder.ASCENDING,
        "filter_type": SubscribeFilter.LATEST_OBJECT,
        "parameters": None,
        "start_group": None,
        "start_object": None,
        "end_group": None,
        "end_object": None,
        "opened_data_stream_ids": set(),
    }

    relay._clients[publisher.session_id] = publisher
    relay._clients[subscriber.session_id] = subscriber
    relay._publications[track_name] = publisher
    relay.publisher_sessions[track_name] = [publisher]
    relay._subscriptions[track_name] = [subscriber]
    relay.subscriber_sessions[track_name] = [subscriber]

    await relay._on_quic_stream_reset(
        publisher.protocol,
        StreamResetData(
            stream_id=8,
            error_code=int(StreamResetCode.SESSION_CLOSED),
            event_type="stop_sending",
        ),
    )

    assert track_name not in publisher.publications
    assert track_name not in relay._publications
    assert track_name not in subscriber.subscriptions
    assert track_name not in relay._subscriptions

    sent_payload = b"".join(chunk for _, chunk, _ in subscriber.quic_connection.sent_streams)
    msg, _ = decode_control_message(sent_payload)
    assert isinstance(msg, PublishDoneMessage)
    assert msg.request_id == 51
    assert msg.status_code == int(PublishDoneStatus.GOING_AWAY)
    assert msg.reason == "publication request stream stop_sending: SESSION_CLOSED"


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
        required_request_id_delta=0,
        track_alias=3,
        full_track_name=track_name
    )

    await relay._on_quic_stream_data(
        protocol,
        StreamData(stream_id=0, data=publish_msg.encode(), end_stream=False)
    )

    assert "lazy-client" in relay._clients
    client = relay._clients["lazy-client"]
    assert client.control_stream_id is None
    assert track_name in relay._publications
    assert client.publications[track_name]["request_id"] == 7
    assert protocol._quic.sent_streams, "relay did not send PUBLISH_OK"
    assert protocol._quic.sent_streams[0][0] == 0
    assert protocol._quic.sent_streams[0][1] == PublishOkMessage(
        request_id=7
    ).encode(include_request_id=False)

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
