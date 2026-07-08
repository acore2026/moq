#!/usr/bin/env python3
"""
Compatibility smoke tests for the official Rust relay wrapper.
"""

import asyncio
import contextlib
import json
import os
import sys

import pytest

MOQ_RUST_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
WORKSPACE_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(MOQ_RUST_ROOT)))
MOQ_PYTHON_ROOT = os.path.join(WORKSPACE_ROOT, 'moq-python')
REPO_ROOT = os.path.dirname(MOQ_RUST_ROOT)
sys.path.insert(0, REPO_ROOT)
sys.path.insert(0, MOQ_RUST_ROOT)
sys.path.insert(0, MOQ_PYTHON_ROOT)

from moq import FullTrackName
from moq.pub import MOQPublisher, PublishedObject
from moq.sub import MOQSubscriber
from moq_official_relay import RustMOQRelay

TEST_RELAY_PORT = 9004
VIDEO_TRACK = FullTrackName([b'agent', b'video', b'camera-001'], b'h264')
VIDEO_FRAME_COUNT = 6


def _official_relay_available() -> bool:
    return bool(
        os.environ.get('MOQ_OFFICIAL_RELAY_BIN')
        or os.environ.get('MOQ_OFFICIAL_RELAY_SOURCE')
    )


def _make_h264_video_payload(frame_id: int, *, keyframe: bool = False) -> bytes:
    """
    Build a deterministic video-frame payload carried by the Python MOQ pub/sub API.

    The payload is bytes because the Python MOQ implementation treats object data as
    opaque bytes. The envelope gives tests enough metadata to assert video-frame
    ordering while keeping the media bytes in Annex-B-like H.264 form.
    """
    nal_type = b'\x65' if keyframe else b'\x41'
    nal_payload = bytes([
        frame_id & 0xFF,
        (frame_id * 3) & 0xFF,
        (frame_id * 7) & 0xFF,
        (frame_id * 11) & 0xFF,
    ])
    frame = b'\x00\x00\x00\x01' + nal_type + nal_payload
    header = {
        'codec': 'h264',
        'frame_id': frame_id,
        'keyframe': keyframe,
        'timestamp_ms': frame_id * 33,
        'width': 320,
        'height': 180,
    }
    return json.dumps(header, sort_keys=True).encode('utf-8') + b'\n\n' + frame


def _decode_h264_video_payload(payload: bytes) -> tuple[dict, bytes]:
    header_bytes, frame = payload.split(b'\n\n', 1)
    return json.loads(header_bytes.decode('utf-8')), frame


@pytest.mark.asyncio
@pytest.mark.skipif(
    not _official_relay_available(),
    reason='Set MOQ_OFFICIAL_RELAY_BIN or MOQ_OFFICIAL_RELAY_SOURCE to run official relay compatibility',
)
async def test_python_pub_sub_directly_against_official_rust_relay(tmp_path):
    """
    Verify whether the legacy Python pub/sub protocol can talk directly to official moq-relay.

    This is intentionally strict when enabled: a failure means bridge mode is required.
    """
    port = int(os.environ.get('MOQ_OFFICIAL_RELAY_TEST_PORT', str(TEST_RELAY_PORT)))
    relay = RustMOQRelay(
        host='127.0.0.1',
        port=port,
        cache_dir=str(tmp_path / 'rust-relay'),
        startup_timeout=60.0,
    )
    received = []
    sub_accepted = asyncio.Event()
    pub_accepted = asyncio.Event()
    obj_received = asyncio.Event()
    track = FullTrackName([b'agent', b'sensors'], b'official-compat')
    pub = None
    sub = None

    await relay.start()
    try:
        pub = MOQPublisher('127.0.0.1', port)
        pub.set_handlers(on_publication_accepted=lambda _: pub_accepted.set())
        assert await asyncio.wait_for(pub.connect(), timeout=8)
        assert await asyncio.wait_for(pub.publish(track), timeout=8)
        await asyncio.wait_for(pub_accepted.wait(), timeout=5)

        sub = MOQSubscriber('127.0.0.1', port, delivery_timeout=1.0)
        sub.set_handlers(
            on_subscription_accepted=lambda _: sub_accepted.set(),
            on_object_received=lambda obj: (received.append(obj), obj_received.set()),
        )
        assert await asyncio.wait_for(sub.connect(), timeout=8)
        assert await asyncio.wait_for(
            sub.subscribe(track, start_group=0, start_object=0),
            timeout=8,
        )
        await asyncio.wait_for(sub_accepted.wait(), timeout=5)
        await pub.send_object(
            track,
            PublishedObject(group_id=0, object_id=0, payload=b'official-compat-ok'),
        )
        await asyncio.wait_for(obj_received.wait(), timeout=5)

        assert received[0].payload == b'official-compat-ok'
    finally:
        if pub is not None:
            with contextlib.suppress(Exception):
                pub.disconnect()
        if sub is not None:
            with contextlib.suppress(Exception):
                sub.disconnect()
        await relay.stop()


@pytest.mark.asyncio
@pytest.mark.skipif(
    not _official_relay_available(),
    reason='Set MOQ_OFFICIAL_RELAY_BIN or MOQ_OFFICIAL_RELAY_SOURCE to run official relay video compatibility',
)
async def test_python_video_pub_sub_against_official_rust_relay(tmp_path):
    """
    Send video frames with Python MOQPublisher/MOQSubscriber through official Rust moq-relay.

    This keeps the client-side format identical to the Python implementation:
    FullTrackName + PublishedObject payload bytes. A failure here means the old
    Python pub/sub wire protocol is not directly compatible with the official relay.
    """
    port = int(os.environ.get('MOQ_OFFICIAL_RELAY_TEST_PORT', str(TEST_RELAY_PORT)))
    relay = RustMOQRelay(
        host='127.0.0.1',
        port=port,
        cache_dir=str(tmp_path / 'rust-video-relay'),
        startup_timeout=60.0,
    )
    received = []
    sub_accepted = asyncio.Event()
    pub_accepted = asyncio.Event()
    all_frames_received = asyncio.Event()
    pub = None
    sub = None

    def on_object_received(obj):
        received.append(obj)
        if len(received) >= VIDEO_FRAME_COUNT:
            all_frames_received.set()

    await relay.start()
    try:
        pub = MOQPublisher('127.0.0.1', port)
        pub.set_handlers(on_publication_accepted=lambda _: pub_accepted.set())
        assert await asyncio.wait_for(pub.connect(), timeout=8)
        assert await asyncio.wait_for(pub.publish(VIDEO_TRACK), timeout=8)
        await asyncio.wait_for(pub_accepted.wait(), timeout=5)

        sub = MOQSubscriber('127.0.0.1', port, delivery_timeout=1.0)
        sub.set_handlers(
            on_subscription_accepted=lambda _: sub_accepted.set(),
            on_object_received=on_object_received,
        )
        assert await asyncio.wait_for(sub.connect(), timeout=8)
        assert await asyncio.wait_for(
            sub.subscribe(VIDEO_TRACK, start_group=0, start_object=0),
            timeout=8,
        )
        await asyncio.wait_for(sub_accepted.wait(), timeout=5)

        expected_payloads = [
            _make_h264_video_payload(frame_id, keyframe=(frame_id == 0))
            for frame_id in range(VIDEO_FRAME_COUNT)
        ]
        for frame_id, payload in enumerate(expected_payloads):
            await pub.send_object(
                VIDEO_TRACK,
                PublishedObject(
                    group_id=0,
                    object_id=frame_id,
                    publisher_priority=128,
                    subgroup_id=0,
                    payload=payload,
                    use_datagram=False,
                ),
            )

        await asyncio.wait_for(all_frames_received.wait(), timeout=10)

        received = sorted(received, key=lambda obj: obj.object_id)
        assert [obj.group_id for obj in received] == [0] * VIDEO_FRAME_COUNT
        assert [obj.object_id for obj in received] == list(range(VIDEO_FRAME_COUNT))
        assert [obj.payload for obj in received] == expected_payloads

        first_header, first_frame = _decode_h264_video_payload(received[0].payload)
        assert first_header['codec'] == 'h264'
        assert first_header['keyframe'] is True
        assert first_frame.startswith(b'\x00\x00\x00\x01\x65')

        last_header, last_frame = _decode_h264_video_payload(received[-1].payload)
        assert last_header['frame_id'] == VIDEO_FRAME_COUNT - 1
        assert last_header['timestamp_ms'] == (VIDEO_FRAME_COUNT - 1) * 33
        assert last_frame.startswith(b'\x00\x00\x00\x01\x41')
    finally:
        if pub is not None:
            with contextlib.suppress(Exception):
                pub.disconnect()
        if sub is not None:
            with contextlib.suppress(Exception):
                sub.disconnect()
        await relay.stop()
