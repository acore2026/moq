import asyncio
from unittest.mock import AsyncMock

import pytest

from _path_helper import ensure_repo_root

ensure_repo_root()

from moq.sub.subscriber import MOQSubscriber
from moq.session import MOQSession, Role, Subscription, FetchRequest
from moq.encoding import FullTrackName, Parameters
from moq.messages import (
    PublishDoneMessage,
    RequestOkMessage,
    SubscribeOkMessage,
    FetchOkMessage,
    GroupOrder,
    SubscribeFilter,
    StreamType,
    StreamResetCode,
    UNKNOWN_PUBLISH_DONE_STREAM_COUNT,
)
from moq.messages.data import SubgroupHeader
from moq.encoding import VarInt
from moq.encoding import Location
from moq.transport.quic_transport import StreamData
from moq.transport import StreamResetData


@pytest.mark.asyncio
async def test_first_peer_stream_is_treated_as_control_stream():
    subscriber = MOQSubscriber("127.0.0.1", 4443)
    subscriber._handle_control_data = AsyncMock()
    subscriber._handle_data_stream = AsyncMock()

    stream_data = StreamData(stream_id=3, data=b"\x0b\x00", end_stream=False)
    await subscriber._handle_stream_data(None, stream_data)

    assert subscriber._peer_control_stream_id == 3
    subscriber._handle_control_data.assert_awaited_once_with(stream_data.data, end_stream=False)
    subscriber._handle_data_stream.assert_not_called()


@pytest.mark.asyncio
async def test_first_peer_unidirectional_data_stream_is_not_misclassified_as_control():
    subscriber = MOQSubscriber("127.0.0.1", 4443)
    subscriber._handle_control_data = AsyncMock()
    subscriber._handle_data_stream = AsyncMock(return_value=True)

    payload = (
        VarInt.encode(StreamType.SUBGROUP_HEADER)
        + SubgroupHeader(track_alias=0, group_id=1, subgroup_id=0, publisher_priority=128).encode()
    )
    stream_data = StreamData(stream_id=3, data=payload, end_stream=False)
    await subscriber._handle_stream_data(None, stream_data)

    assert subscriber._peer_control_stream_id is None
    subscriber._handle_data_stream.assert_awaited_once_with(3, payload, end_stream=False)
    subscriber._handle_control_data.assert_not_called()


def test_publish_done_removes_subscription_and_emits_callback():
    subscriber = MOQSubscriber("127.0.0.1", 4443)
    subscriber._session = MOQSession(session_id="sub-test", role=Role.SUBSCRIBER)

    track_name = FullTrackName([b"live"], b"video")
    request_id = 7
    track_alias = 11

    subscriber._session.subscriptions[request_id] = Subscription(
        request_id=request_id,
        track_alias=track_alias,
        full_track_name=track_name,
        subscriber_priority=128,
        group_order=GroupOrder.ASCENDING,
        filter_type=SubscribeFilter.LATEST_OBJECT,
        subscriber=subscriber._session,
    )
    subscriber._subscriptions[track_name] = request_id
    subscriber._active_subscriptions[request_id] = track_name
    subscriber._track_aliases[track_alias] = track_name

    observed = []
    subscriber.set_handlers(
        on_subscription_ended=lambda tn, status, reason: observed.append((tn, status, reason))
    )

    subscriber._handle_publish_done(
        PublishDoneMessage(
            request_id=request_id,
            status_code=6,
            stream_count=UNKNOWN_PUBLISH_DONE_STREAM_COUNT,
            reason="outbound send queue full",
        )
    )

    assert track_name not in subscriber._subscriptions
    assert request_id not in subscriber._active_subscriptions
    assert track_alias not in subscriber._track_aliases
    assert request_id not in subscriber._session.subscriptions
    assert observed == [(track_name, 6, "outbound send queue full")]


def test_request_ok_updates_subscription_parameters():
    subscriber = MOQSubscriber("127.0.0.1", 4443)
    subscriber._session = MOQSession(session_id="sub-test", role=Role.SUBSCRIBER)

    track_name = FullTrackName([b"live"], b"video")
    request_id = 8

    subscriber._session.subscriptions[request_id] = Subscription(
        request_id=request_id,
        track_alias=12,
        full_track_name=track_name,
        subscriber_priority=128,
        group_order=GroupOrder.ASCENDING,
        filter_type=SubscribeFilter.LATEST_OBJECT,
        parameters=None,
        subscriber=subscriber._session,
    )

    params = Parameters()
    params.set(0x20, 5)

    subscriber._handle_request_ok(
        RequestOkMessage(
            request_id=request_id,
            parameters=params,
        )
    )

    assert subscriber._session.subscriptions[request_id].parameters.get(0x20) == 5


@pytest.mark.asyncio
async def test_request_stream_subscribe_ok_uses_stream_context_request_id():
    subscriber = MOQSubscriber("127.0.0.1", 4443)
    subscriber._session = MOQSession(session_id="sub-test", role=Role.SUBSCRIBER)

    track_name = FullTrackName([b"live"], b"video")
    request_id = 10
    stream_id = 4

    subscriber._session.subscriptions[request_id] = Subscription(
        request_id=request_id,
        track_alias=None,
        full_track_name=track_name,
        subscriber_priority=128,
        group_order=GroupOrder.ASCENDING,
        filter_type=SubscribeFilter.LATEST_OBJECT,
        subscriber=subscriber._session,
    )
    subscriber._subscriptions[track_name] = request_id
    subscriber._request_stream_ids[request_id] = stream_id

    encoded = SubscribeOkMessage(
        request_id=request_id,
        track_alias=33,
    ).encode(include_request_id=False)

    await subscriber._handle_request_control_data(stream_id, encoded)

    subscription = subscriber._session.subscriptions[request_id]
    assert subscription.track_alias == 33
    assert subscriber._active_subscriptions[request_id] == track_name
    assert subscriber._track_aliases[33] == track_name


@pytest.mark.asyncio
async def test_request_stream_fetch_ok_uses_stream_context_request_id():
    subscriber = MOQSubscriber("127.0.0.1", 4443)
    subscriber._session = MOQSession(session_id="sub-test", role=Role.SUBSCRIBER)

    track_name = FullTrackName([b"live"], b"video")
    request_id = 12
    stream_id = 8

    subscriber._session.fetches[request_id] = FetchRequest(
        request_id=request_id,
        full_track_name=track_name,
        subscriber_priority=128,
        group_order=GroupOrder.ASCENDING,
        start_group=0,
        start_object=0,
        end_group=10,
        end_object=10,
    )
    subscriber._request_stream_ids[request_id] = stream_id

    encoded = FetchOkMessage(
        request_id=request_id,
        end_of_track=True,
        end_location=Location(4, 7),
    ).encode(include_request_id=False)

    await subscriber._handle_request_control_data(stream_id, encoded)

    fetch_request = subscriber._session.fetches[request_id]
    assert fetch_request.end_of_track is True
    assert fetch_request.resolved_end_group == 4
    assert fetch_request.resolved_end_object == 7


@pytest.mark.asyncio
async def test_request_stream_publish_done_uses_stream_context_request_id():
    subscriber = MOQSubscriber("127.0.0.1", 4443)
    subscriber._session = MOQSession(session_id="sub-test", role=Role.SUBSCRIBER)

    track_name = FullTrackName([b"live"], b"video")
    request_id = 14
    stream_id = 12

    subscriber._session.subscriptions[request_id] = Subscription(
        request_id=request_id,
        track_alias=44,
        full_track_name=track_name,
        subscriber_priority=128,
        group_order=GroupOrder.ASCENDING,
        filter_type=SubscribeFilter.LATEST_OBJECT,
        subscriber=subscriber._session,
    )
    subscriber._subscriptions[track_name] = request_id
    subscriber._active_subscriptions[request_id] = track_name
    subscriber._track_aliases[44] = track_name
    subscriber._request_stream_ids[request_id] = stream_id

    encoded = PublishDoneMessage(
        request_id=request_id,
        status_code=6,
        stream_count=UNKNOWN_PUBLISH_DONE_STREAM_COUNT,
        reason="queue full",
    ).encode(include_request_id=False)

    await subscriber._handle_request_control_data(stream_id, encoded)

    assert track_name not in subscriber._subscriptions
    assert request_id not in subscriber._active_subscriptions
    assert 44 not in subscriber._track_aliases
    assert request_id not in subscriber._session.subscriptions


def test_publish_done_waits_for_active_data_streams_before_cleanup():
    subscriber = MOQSubscriber("127.0.0.1", 4443)
    subscriber._session = MOQSession(session_id="sub-test", role=Role.SUBSCRIBER)

    track_name = FullTrackName([b"live"], b"video")
    request_id = 18
    stream_id = 22
    track_alias = 55

    subscription = Subscription(
        request_id=request_id,
        track_alias=track_alias,
        full_track_name=track_name,
        subscriber_priority=128,
        group_order=GroupOrder.ASCENDING,
        filter_type=SubscribeFilter.LATEST_OBJECT,
        subscriber=subscriber._session,
    )
    subscriber._session.subscriptions[request_id] = subscription
    subscriber._subscriptions[track_name] = request_id
    subscriber._active_subscriptions[request_id] = track_name
    subscriber._track_aliases[track_alias] = track_name
    subscriber._subscription_seen_streams[request_id] = {stream_id}
    subscriber._subscription_active_streams[request_id] = {stream_id}
    subscriber._stream_subscription_requests[stream_id] = request_id

    subscriber._handle_publish_done(
        PublishDoneMessage(
            request_id=request_id,
            status_code=6,
            stream_count=1,
            reason="queue full",
        )
    )

    assert request_id in subscriber._session.subscriptions
    assert request_id in subscriber._pending_subscription_ends

    subscriber._cleanup_data_stream(stream_id)

    assert request_id not in subscriber._session.subscriptions
    assert request_id not in subscriber._pending_subscription_ends


@pytest.mark.asyncio
async def test_publish_done_timeout_forces_cleanup_when_stream_stays_open():
    subscriber = MOQSubscriber("127.0.0.1", 4443, delivery_timeout=0.01)
    subscriber._session = MOQSession(session_id="sub-test", role=Role.SUBSCRIBER)
    subscriber._client = AsyncMock()

    track_name = FullTrackName([b"live"], b"video")
    request_id = 20
    stream_id = 24
    request_stream_id = 30
    track_alias = 57

    subscriber._session.subscriptions[request_id] = Subscription(
        request_id=request_id,
        track_alias=track_alias,
        full_track_name=track_name,
        subscriber_priority=128,
        group_order=GroupOrder.ASCENDING,
        filter_type=SubscribeFilter.LATEST_OBJECT,
        subscriber=subscriber._session,
    )
    subscriber._subscriptions[track_name] = request_id
    subscriber._active_subscriptions[request_id] = track_name
    subscriber._track_aliases[track_alias] = track_name
    subscriber._subscription_seen_streams[request_id] = {stream_id}
    subscriber._subscription_active_streams[request_id] = {stream_id}
    subscriber._stream_subscription_requests[stream_id] = request_id
    subscriber._request_stream_ids[request_id] = request_stream_id

    subscriber._handle_publish_done(
        PublishDoneMessage(
            request_id=request_id,
            status_code=6,
            stream_count=1,
            reason="queue full",
        )
    )

    await asyncio.sleep(0.03)

    assert request_id not in subscriber._session.subscriptions
    assert request_id not in subscriber._pending_subscription_ends
    assert stream_id not in subscriber._stream_subscription_requests
    subscriber._client.stop_stream.assert_awaited_once_with(
        stream_id,
        error_code=int(StreamResetCode.CANCELLED),
    )
    subscriber._client.reset_stream.assert_awaited_once_with(
        request_stream_id,
        error_code=int(StreamResetCode.CANCELLED),
    )


@pytest.mark.asyncio
async def test_unsubscribe_cancels_request_stream_and_cleans_local_state():
    subscriber = MOQSubscriber("127.0.0.1", 4443)
    subscriber._client = type(
        "DummyClient",
        (),
        {
            "stop_stream": AsyncMock(),
            "reset_stream": AsyncMock(),
        },
    )()
    subscriber._session = MOQSession(session_id="sub-test", role=Role.SUBSCRIBER)

    track_name = FullTrackName([b"live"], b"video")
    request_id = 22
    request_stream_id = 18
    data_stream_id = 26
    track_alias = 59

    subscriber._subscriptions[track_name] = request_id
    subscriber._active_subscriptions[request_id] = track_name
    subscriber._request_stream_ids[request_id] = request_stream_id
    subscriber._track_aliases[track_alias] = track_name
    subscriber._stream_subscription_requests[data_stream_id] = request_id
    subscriber._subscription_seen_streams[request_id] = {data_stream_id}
    subscriber._subscription_active_streams[request_id] = {data_stream_id}
    subscriber._data_stream_buffers[data_stream_id] = bytearray(b"pending")
    subscriber._data_stream_types[data_stream_id] = 0x10
    subscriber._subgroup_previous_object_ids[data_stream_id] = 7

    subscriber._session.subscriptions[request_id] = Subscription(
        request_id=request_id,
        track_alias=track_alias,
        full_track_name=track_name,
        subscriber_priority=128,
        group_order=GroupOrder.ASCENDING,
        filter_type=SubscribeFilter.LATEST_OBJECT,
        active=True,
    )

    await subscriber.unsubscribe(track_name)

    subscriber._client.stop_stream.assert_awaited_once_with(
        data_stream_id,
        error_code=int(StreamResetCode.CANCELLED),
    )
    subscriber._client.reset_stream.assert_awaited_once_with(
        request_stream_id,
        error_code=int(StreamResetCode.CANCELLED),
    )
    assert track_name not in subscriber._subscriptions
    assert request_id not in subscriber._active_subscriptions
    assert request_id not in subscriber._request_stream_ids
    assert request_id not in subscriber._session.subscriptions
    assert track_alias not in subscriber._track_aliases
    assert data_stream_id not in subscriber._stream_subscription_requests
    assert data_stream_id not in subscriber._data_stream_buffers


@pytest.mark.asyncio
async def test_stream_reset_cleans_up_active_subscription_stream():
    subscriber = MOQSubscriber("127.0.0.1", 4443)
    subscriber._session = MOQSession(session_id="sub-test", role=Role.SUBSCRIBER)

    track_name = FullTrackName([b"live"], b"video")
    request_id = 22
    stream_id = 26
    track_alias = 59

    subscriber._session.subscriptions[request_id] = Subscription(
        request_id=request_id,
        track_alias=track_alias,
        full_track_name=track_name,
        subscriber_priority=128,
        group_order=GroupOrder.ASCENDING,
        filter_type=SubscribeFilter.LATEST_OBJECT,
        subscriber=subscriber._session,
    )
    subscriber._subscriptions[track_name] = request_id
    subscriber._active_subscriptions[request_id] = track_name
    subscriber._track_aliases[track_alias] = track_name
    subscriber._stream_subscription_requests[stream_id] = request_id
    subscriber._subscription_seen_streams[request_id] = {stream_id}
    subscriber._subscription_active_streams[request_id] = {stream_id}
    subscriber._data_stream_buffers[stream_id] = bytearray(b"partial")
    subscriber._pending_subscription_ends[request_id] = PublishDoneMessage(
        request_id=request_id,
        status_code=6,
        stream_count=1,
        reason="queue full",
    )

    await subscriber._handle_stream_reset(
        None,
        StreamResetData(stream_id=stream_id, error_code=0, event_type="reset"),
    )

    assert stream_id not in subscriber._data_stream_buffers
    assert request_id not in subscriber._session.subscriptions
