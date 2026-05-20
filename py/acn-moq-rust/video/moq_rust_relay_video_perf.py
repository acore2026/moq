#!/usr/bin/env python3
"""
Measure Python MOQ pub/sub video delivery through the official Rust moq-relay.
"""

import argparse
import asyncio
import contextlib
import json
import os
import statistics
import sys
import tempfile
import time
from pathlib import Path

MOQ_RUST_ROOT = Path(__file__).resolve().parents[1]
REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT))
sys.path.insert(0, str(MOQ_RUST_ROOT))

from moq import FullTrackName
from moq.pub import MOQPublisher, PublishedObject
from moq.sub import MOQSubscriber
from moq_official_relay import RustMOQRelay


VIDEO_TRACK = FullTrackName([b'agent', b'video', b'perf-camera'], b'h264')


def build_payload(frame_id: int, sent_ns: int, payload_size: int) -> bytes:
    header = {
        'codec': 'h264',
        'frame_id': frame_id,
        'keyframe': frame_id % 30 == 0,
        'sent_ns': sent_ns,
        'timestamp_ms': int(frame_id * 1000 / 30),
        'width': 1280,
        'height': 720,
    }
    prefix = json.dumps(header, sort_keys=True).encode('utf-8') + b'\n\n'
    nal = b'\x00\x00\x00\x01' + (b'\x65' if header['keyframe'] else b'\x41')
    body_len = max(0, payload_size - len(prefix) - len(nal))
    body = bytes([(frame_id * 17) & 0xFF]) * body_len
    return prefix + nal + body


def decode_payload(payload: bytes) -> dict:
    header, _frame = payload.split(b'\n\n', 1)
    return json.loads(header.decode('utf-8'))


def rss_bytes(pid: int | None) -> int:
    if not pid:
        return 0
    try:
        with open(f'/proc/{pid}/status', 'r', encoding='utf-8') as status:
            for line in status:
                if line.startswith('VmRSS:'):
                    return int(line.split()[1]) * 1024
    except OSError:
        return 0
    return 0


async def run(args):
    relay = RustMOQRelay(
        host='127.0.0.1',
        port=args.port,
        cache_dir=tempfile.mkdtemp(prefix='moq-rust-relay-perf-'),
        startup_timeout=60.0,
    )
    pub = None
    sub = None
    received = []
    rss_samples = []
    sub_accepted = asyncio.Event()
    pub_accepted = asyncio.Event()
    all_received = asyncio.Event()
    expected_frames = int(args.duration * args.fps)

    def on_object_received(obj):
        received_ns = time.monotonic_ns()
        try:
            header = decode_payload(obj.payload)
        except Exception:
            return
        received.append({
            'frame_id': header['frame_id'],
            'object_id': obj.object_id,
            'sent_ns': header['sent_ns'],
            'received_ns': received_ns,
            'payload_size': len(obj.payload),
        })
        if len(received) >= expected_frames:
            all_received.set()

    async def monitor_rss():
        while True:
            process = relay._process
            rss_samples.append(rss_bytes(process.pid if process else None))
            await asyncio.sleep(0.1)

    await relay.start()
    monitor_task = asyncio.create_task(monitor_rss())
    try:
        pub = MOQPublisher('127.0.0.1', args.port)
        pub.set_handlers(on_publication_accepted=lambda _: pub_accepted.set())
        if not await asyncio.wait_for(pub.connect(), timeout=8):
            raise RuntimeError('publisher failed to connect')
        if not await asyncio.wait_for(pub.publish(VIDEO_TRACK), timeout=8):
            raise RuntimeError('publisher failed to publish')
        await asyncio.wait_for(pub_accepted.wait(), timeout=5)

        sub = MOQSubscriber('127.0.0.1', args.port, delivery_timeout=1.0)
        sub.set_handlers(
            on_subscription_accepted=lambda _: sub_accepted.set(),
            on_object_received=on_object_received,
        )
        if not await asyncio.wait_for(sub.connect(), timeout=8):
            raise RuntimeError('subscriber failed to connect')
        if not await asyncio.wait_for(
            sub.subscribe(VIDEO_TRACK, start_group=0, start_object=0),
            timeout=8,
        ):
            raise RuntimeError('subscriber failed to subscribe')
        await asyncio.wait_for(sub_accepted.wait(), timeout=5)

        interval = 1.0 / args.fps
        start = time.monotonic()
        for frame_id in range(expected_frames):
            target = start + frame_id * interval
            delay = target - time.monotonic()
            if delay > 0:
                await asyncio.sleep(delay)
            sent_ns = time.monotonic_ns()
            payload = build_payload(frame_id, sent_ns, args.payload_size)
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

        with contextlib.suppress(asyncio.TimeoutError):
            await asyncio.wait_for(all_received.wait(), timeout=args.drain_timeout)
    finally:
        monitor_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await monitor_task
        if pub is not None:
            with contextlib.suppress(Exception):
                pub.disconnect()
        if sub is not None:
            with contextlib.suppress(Exception):
                sub.disconnect()
        await relay.stop()

    by_frame = {item['frame_id']: item for item in received}
    received_ids = sorted(by_frame)
    missing = [frame_id for frame_id in range(expected_frames) if frame_id not in by_frame]
    latency_ms = [
        (item['received_ns'] - item['sent_ns']) / 1_000_000
        for item in by_frame.values()
    ]
    interarrival_ms = [
        (by_frame[right]['received_ns'] - by_frame[left]['received_ns']) / 1_000_000
        for left, right in zip(received_ids, received_ids[1:])
    ]
    expected_interval_ms = 1000 / args.fps
    jitter_ms = [abs(value - expected_interval_ms) for value in interarrival_ms]
    duration_s = max(args.duration, 0.001)
    bytes_received = sum(item['payload_size'] for item in by_frame.values())

    def pct(values, percentile):
        if not values:
            return None
        values = sorted(values)
        index = min(len(values) - 1, round((percentile / 100) * (len(values) - 1)))
        return values[index]

    result = {
        'port': args.port,
        'fps': args.fps,
        'duration_s': args.duration,
        'payload_size_bytes': args.payload_size,
        'expected_frames': expected_frames,
        'received_frames': len(by_frame),
        'lost_frames': len(missing),
        'loss_rate_percent': (len(missing) / expected_frames * 100) if expected_frames else 0,
        'received_payload_bytes': bytes_received,
        'received_payload_mbps': (bytes_received * 8 / duration_s / 1_000_000),
        'latency_ms': {
            'min': min(latency_ms) if latency_ms else None,
            'avg': statistics.fmean(latency_ms) if latency_ms else None,
            'p50': pct(latency_ms, 50),
            'p95': pct(latency_ms, 95),
            'max': max(latency_ms) if latency_ms else None,
            'stddev': statistics.pstdev(latency_ms) if len(latency_ms) > 1 else 0,
        },
        'interarrival_jitter_ms': {
            'avg_abs_from_frame_interval': statistics.fmean(jitter_ms) if jitter_ms else None,
            'p95_abs_from_frame_interval': pct(jitter_ms, 95),
            'max_abs_from_frame_interval': max(jitter_ms) if jitter_ms else None,
        },
        'relay_rss_bytes': {
            'min': min(rss_samples) if rss_samples else 0,
            'avg': int(statistics.fmean(rss_samples)) if rss_samples else 0,
            'max': max(rss_samples) if rss_samples else 0,
        },
        'missing_frame_ids': missing[:20],
    }
    print(json.dumps(result, indent=2, sort_keys=True))


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--port', type=int, default=9004)
    parser.add_argument('--fps', type=float, default=30.0)
    parser.add_argument('--duration', type=float, default=10.0)
    parser.add_argument('--payload-size', type=int, default=50000)
    parser.add_argument('--drain-timeout', type=float, default=5.0)
    return parser.parse_args()


if __name__ == '__main__':
    asyncio.run(run(parse_args()))
