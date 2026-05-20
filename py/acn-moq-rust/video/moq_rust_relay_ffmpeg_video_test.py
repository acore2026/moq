#!/usr/bin/env python3
"""
Send an ffmpeg-generated H.264 stream through Python MOQ pub/sub and Rust moq-relay.
"""

import argparse
import asyncio
import contextlib
import json
import os
import statistics
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from typing import Iterable, Optional

MOQ_RUST_ROOT = Path(__file__).resolve().parents[1]
REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT))
sys.path.insert(0, str(MOQ_RUST_ROOT))

from moq import FullTrackName
from moq.pub import MOQPublisher, PublishedObject
from moq.sub import MOQSubscriber
from moq_official_relay import RustMOQRelay


VIDEO_TRACK = FullTrackName([b'agent', b'video', b'ffmpeg-camera'], b'h264')
VCL_NAL_TYPES = {1, 5}


def run_checked(command: list[str], description: str) -> subprocess.CompletedProcess:
    result = subprocess.run(
        command,
        check=False,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    if result.returncode != 0:
        detail = result.stderr.strip() or result.stdout.strip()
        raise RuntimeError(f'{description} failed with rc={result.returncode}: {detail}')
    return result


def generate_h264(args, output_path: Path) -> None:
    size = f'{args.width}x{args.height}'
    command = [
        'ffmpeg',
        '-y',
        '-hide_banner',
        '-loglevel',
        'error',
        '-f',
        'lavfi',
        '-i',
        f'testsrc2=size={size}:rate={args.fps}',
        '-t',
        str(args.duration),
        '-an',
        '-threads',
        '1',
        '-c:v',
        'libx264',
        '-preset',
        args.preset,
        '-tune',
        'zerolatency',
        '-x264-params',
        f'keyint={args.gop}:min-keyint={args.gop}:scenecut=0',
        '-b:v',
        args.bitrate,
        '-maxrate',
        args.bitrate,
        '-bufsize',
        args.bufsize,
        '-pix_fmt',
        'yuv420p',
        '-f',
        'h264',
        str(output_path),
    ]
    run_checked(command, 'ffmpeg encode')


def find_start_codes(data: bytes) -> list[tuple[int, int]]:
    starts = []
    index = 0
    length = len(data)
    while index < length - 3:
        if data[index:index + 3] == b'\x00\x00\x01':
            starts.append((index, 3))
            index += 3
            continue
        if index < length - 4 and data[index:index + 4] == b'\x00\x00\x00\x01':
            starts.append((index, 4))
            index += 4
            continue
        index += 1
    return starts


def split_h264_frames(data: bytes) -> list[bytes]:
    starts = find_start_codes(data)
    if not starts:
        raise RuntimeError('generated H.264 stream has no Annex-B start codes')

    ranges = []
    for item_index, (start, prefix_len) in enumerate(starts):
        end = starts[item_index + 1][0] if item_index + 1 < len(starts) else len(data)
        if end > start + prefix_len:
            ranges.append((start, prefix_len, end))

    frames = []
    pending_prefix = bytearray()
    for start, prefix_len, end in ranges:
        nal = data[start:end]
        nal_type = data[start + prefix_len] & 0x1F
        if nal_type in VCL_NAL_TYPES:
            frame = bytes(pending_prefix) + nal
            pending_prefix.clear()
            frames.append(frame)
        else:
            pending_prefix.extend(nal)

    if not frames:
        raise RuntimeError('generated H.264 stream has no VCL NAL units')
    if pending_prefix:
        frames[-1] += bytes(pending_prefix)
    return frames


def rss_bytes(pid: Optional[int]) -> int:
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


def percentile(values: Iterable[float], percentile_value: float) -> Optional[float]:
    values = sorted(values)
    if not values:
        return None
    index = min(
        len(values) - 1,
        round((percentile_value / 100) * (len(values) - 1)),
    )
    return values[index]


def stats(values: list[float]) -> dict[str, Optional[float]]:
    if not values:
        return {
            'min': None,
            'avg': None,
            'p50': None,
            'p95': None,
            'max': None,
            'stddev': None,
        }
    return {
        'min': min(values),
        'avg': statistics.fmean(values),
        'p50': percentile(values, 50),
        'p95': percentile(values, 95),
        'max': max(values),
        'stddev': statistics.pstdev(values) if len(values) > 1 else 0,
    }


def ffprobe_frame_count(path: Path) -> Optional[int]:
    result = run_checked(
        [
            'ffprobe',
            '-v',
            'error',
            '-count_frames',
            '-select_streams',
            'v:0',
            '-show_entries',
            'stream=nb_read_frames',
            '-of',
            'json',
            str(path),
        ],
        'ffprobe frame count',
    )
    payload = json.loads(result.stdout or '{}')
    streams = payload.get('streams') or []
    if not streams:
        return None
    count = streams[0].get('nb_read_frames')
    return int(count) if count is not None else None


def validate_decode(path: Path) -> None:
    run_checked(
        [
            'ffmpeg',
            '-hide_banner',
            '-loglevel',
            'error',
            '-i',
            str(path),
            '-f',
            'null',
            '-',
        ],
        'ffmpeg decode',
    )


async def run_video_transfer(args, frames: list[bytes], received_path: Path) -> dict:
    relay = RustMOQRelay(
        host='127.0.0.1',
        port=args.port,
        cache_dir=tempfile.mkdtemp(prefix='moq-rust-relay-ffmpeg-'),
        startup_timeout=60.0,
    )
    publisher = None
    subscriber = None
    publish_accepted = asyncio.Event()
    subscribe_accepted = asyncio.Event()
    all_received = asyncio.Event()
    received_frames: dict[int, bytes] = {}
    send_times_ns: dict[int, int] = {}
    receive_times_ns: dict[int, int] = {}
    rss_samples = []

    def on_object_received(obj):
        receive_times_ns[obj.object_id] = time.monotonic_ns()
        received_frames[obj.object_id] = bytes(obj.payload)
        if len(received_frames) >= len(frames):
            all_received.set()

    async def monitor_rss():
        while True:
            process = relay._process
            rss_samples.append(rss_bytes(process.pid if process else None))
            await asyncio.sleep(0.1)

    await relay.start()
    monitor_task = asyncio.create_task(monitor_rss())
    try:
        publisher = MOQPublisher('127.0.0.1', args.port)
        publisher.set_handlers(on_publication_accepted=lambda _: publish_accepted.set())
        if not await asyncio.wait_for(publisher.connect(), timeout=8):
            raise RuntimeError('publisher failed to connect')
        if not await asyncio.wait_for(publisher.publish(VIDEO_TRACK), timeout=8):
            raise RuntimeError('publisher failed to publish')
        await asyncio.wait_for(publish_accepted.wait(), timeout=5)

        subscriber = MOQSubscriber('127.0.0.1', args.port, delivery_timeout=1.0)
        subscriber.set_handlers(
            on_subscription_accepted=lambda _: subscribe_accepted.set(),
            on_object_received=on_object_received,
        )
        if not await asyncio.wait_for(subscriber.connect(), timeout=8):
            raise RuntimeError('subscriber failed to connect')
        if not await asyncio.wait_for(
            subscriber.subscribe(VIDEO_TRACK, start_group=0, start_object=0),
            timeout=8,
        ):
            raise RuntimeError('subscriber failed to subscribe')
        await asyncio.wait_for(subscribe_accepted.wait(), timeout=5)

        interval = 1.0 / args.fps
        start_time = time.monotonic()
        for frame_id, payload in enumerate(frames):
            target_time = start_time + frame_id * interval
            delay = target_time - time.monotonic()
            if delay > 0:
                await asyncio.sleep(delay)
            send_times_ns[frame_id] = time.monotonic_ns()
            await publisher.send_object(
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
        if publisher is not None:
            with contextlib.suppress(Exception):
                publisher.disconnect()
        if subscriber is not None:
            with contextlib.suppress(Exception):
                subscriber.disconnect()
        await relay.stop()

    received_ids = sorted(received_frames)
    with received_path.open('wb') as output:
        for frame_id in received_ids:
            output.write(received_frames[frame_id])

    missing = [frame_id for frame_id in range(len(frames)) if frame_id not in received_frames]
    latency_ms = [
        (receive_times_ns[frame_id] - send_times_ns[frame_id]) / 1_000_000
        for frame_id in received_ids
        if frame_id in send_times_ns and frame_id in receive_times_ns
    ]
    interarrival_ms = [
        (receive_times_ns[right] - receive_times_ns[left]) / 1_000_000
        for left, right in zip(received_ids, received_ids[1:])
        if left in receive_times_ns and right in receive_times_ns
    ]
    expected_interval_ms = 1000 / args.fps
    jitter_ms = [abs(value - expected_interval_ms) for value in interarrival_ms]

    return {
        'sent_frames': len(frames),
        'received_frames': len(received_frames),
        'lost_frames': len(missing),
        'loss_rate_percent': (len(missing) / len(frames) * 100) if frames else 0,
        'missing_frame_ids': missing[:20],
        'latency_ms': stats(latency_ms),
        'interarrival_jitter_ms': {
            'avg_abs_from_frame_interval': (
                statistics.fmean(jitter_ms) if jitter_ms else None
            ),
            'p95_abs_from_frame_interval': percentile(jitter_ms, 95),
            'max_abs_from_frame_interval': max(jitter_ms) if jitter_ms else None,
        },
        'relay_rss_bytes': {
            'min': min(rss_samples) if rss_samples else 0,
            'avg': int(statistics.fmean(rss_samples)) if rss_samples else 0,
            'max': max(rss_samples) if rss_samples else 0,
        },
    }


async def run(args) -> None:
    output_dir = Path(args.output_dir).resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    source_path = output_dir / 'ffmpeg-source.h264'
    received_path = output_dir / 'moq-received.h264'

    generate_h264(args, source_path)
    source_bytes = source_path.read_bytes()
    frames = split_h264_frames(source_bytes)

    transfer = await run_video_transfer(args, frames, received_path)
    decode_ok = True
    decode_error = None
    decoded_frames = None
    try:
        validate_decode(received_path)
        decoded_frames = ffprobe_frame_count(received_path)
    except Exception as exc:
        decode_ok = False
        decode_error = str(exc)

    source_frames = None
    with contextlib.suppress(Exception):
        source_frames = ffprobe_frame_count(source_path)

    duration_s = max(args.duration, 0.001)
    result = {
        'port': args.port,
        'video': {
            'width': args.width,
            'height': args.height,
            'fps': args.fps,
            'duration_s': args.duration,
            'target_bitrate': args.bitrate,
            'source_bytes': source_path.stat().st_size,
            'source_payload_mbps': source_path.stat().st_size * 8 / duration_s / 1_000_000,
            'source_ffprobe_frames': source_frames,
            'split_payload_frames': len(frames),
            'received_bytes': received_path.stat().st_size,
            'received_payload_mbps': (
                received_path.stat().st_size * 8 / duration_s / 1_000_000
            ),
            'decoded_frames': decoded_frames,
            'ffmpeg_decode_ok': decode_ok,
            'ffmpeg_decode_error': decode_error,
        },
        'transfer': transfer,
        'files': {
            'source_h264': str(source_path),
            'received_h264': str(received_path),
        },
    }
    print(json.dumps(result, indent=2, sort_keys=True))


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--port', type=int, default=9004)
    parser.add_argument('--width', type=int, default=1920)
    parser.add_argument('--height', type=int, default=1080)
    parser.add_argument('--fps', type=float, default=30.0)
    parser.add_argument('--duration', type=float, default=5.0)
    parser.add_argument('--bitrate', default='2500k')
    parser.add_argument('--bufsize', default='5000k')
    parser.add_argument('--preset', default='ultrafast')
    parser.add_argument('--gop', type=int, default=30)
    parser.add_argument('--drain-timeout', type=float, default=5.0)
    parser.add_argument(
        '--output-dir',
        default=os.path.join(tempfile.gettempdir(), 'moq-rust-relay-ffmpeg-video-test'),
    )
    return parser.parse_args()


if __name__ == '__main__':
    asyncio.run(run(parse_args()))
