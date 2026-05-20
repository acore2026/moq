#!/usr/bin/env python3
"""
Publish a Windows DirectShow camera to a MOQ relay with Python MOQPublisher.
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import json
import logging
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from moq.pub import MOQPublisher, PublishedObject
from moq_live_video_common import (
    AnnexBFrameSplitter,
    DEFAULT_META_NAME,
    DEFAULT_NAMESPACE,
    DEFAULT_VIDEO_NAME,
    build_track,
    contains_idr,
)


logger = logging.getLogger('moq-windows-camera-publisher')


def list_windows_cameras(ffmpeg_bin: str) -> int:
    command = [
        ffmpeg_bin,
        '-hide_banner',
        '-list_devices',
        'true',
        '-f',
        'dshow',
        '-i',
        'dummy',
    ]
    subprocess.run(command, check=False)
    return 0


def build_ffmpeg_command(args) -> list[str]:
    size = f'{args.width}x{args.height}'
    return [
        args.ffmpeg_bin,
        '-hide_banner',
        '-loglevel',
        args.ffmpeg_log_level,
        '-fflags',
        'nobuffer',
        '-flags',
        'low_delay',
        '-rtbufsize',
        args.rtbufsize,
        '-f',
        'dshow',
        '-video_size',
        size,
        '-framerate',
        str(args.fps),
        '-i',
        f'video={args.camera_name}',
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
        f'keyint={args.gop}:min-keyint={args.gop}:scenecut=0:sliced-threads=0',
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
        '-',
    ]


async def drain_stderr(process: asyncio.subprocess.Process) -> None:
    if process.stderr is None:
        return
    while True:
        line = await process.stderr.readline()
        if not line:
            return
        logger.info('ffmpeg: %s', line.decode('utf-8', errors='replace').rstrip())


async def publish_camera(args) -> None:
    video_track = build_track(args.namespace, args.video_name)
    meta_track = build_track(args.namespace, args.meta_name)
    publisher = MOQPublisher(args.relay_host, args.relay_port)
    accepted_tracks = set()
    accepted = asyncio.Event()

    def on_publication_accepted(track):
        accepted_tracks.add(track)
        if video_track in accepted_tracks and meta_track in accepted_tracks:
            accepted.set()

    publisher.set_handlers(on_publication_accepted=on_publication_accepted)
    if not await asyncio.wait_for(publisher.connect(), timeout=args.connect_timeout):
        raise RuntimeError('publisher failed to connect to relay')
    if not await asyncio.wait_for(publisher.publish(video_track), timeout=args.connect_timeout):
        raise RuntimeError('publisher failed to publish video track')
    if not await asyncio.wait_for(publisher.publish(meta_track), timeout=args.connect_timeout):
        raise RuntimeError('publisher failed to publish metadata track')
    await asyncio.wait_for(accepted.wait(), timeout=args.connect_timeout)

    command = build_ffmpeg_command(args)
    logger.info('starting ffmpeg camera capture: %s', ' '.join(command))
    process = await asyncio.create_subprocess_exec(
        *command,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    stderr_task = asyncio.create_task(drain_stderr(process))
    splitter = AnnexBFrameSplitter()
    frame_id = 0
    started_at = time.monotonic()

    try:
        if process.stdout is None:
            raise RuntimeError('ffmpeg stdout pipe was not created')
        while True:
            chunk = await process.stdout.read(args.read_size)
            if not chunk:
                break
            for frame in splitter.feed(chunk):
                keyframe = contains_idr(frame)
                sent_epoch_ms = time.time() * 1000
                sent_monotonic_ns = time.monotonic_ns()
                timestamp_us = int(frame_id * 1_000_000 / args.fps)
                metadata = {
                    'type': 'frame_meta',
                    'frame_id': frame_id,
                    'sent_epoch_ms': sent_epoch_ms,
                    'sent_monotonic_ns': sent_monotonic_ns,
                    'timestamp_us': timestamp_us,
                    'keyframe': keyframe,
                    'width': args.width,
                    'height': args.height,
                    'fps': args.fps,
                    'codec': args.codec,
                    'payload_bytes': len(frame),
                }
                await publisher.send_object(
                    meta_track,
                    PublishedObject(
                        group_id=0,
                        object_id=frame_id,
                        payload=json.dumps(metadata, separators=(',', ':')).encode('utf-8'),
                    ),
                )
                await publisher.send_object(
                    video_track,
                    PublishedObject(group_id=0, object_id=frame_id, payload=frame),
                )
                frame_id += 1
                if frame_id % args.log_every == 0:
                    elapsed = max(time.monotonic() - started_at, 0.001)
                    logger.info(
                        'published frames=%s fps=%.2f last_keyframe=%s',
                        frame_id,
                        frame_id / elapsed,
                        keyframe,
                    )

        for frame in splitter.flush():
            await publisher.send_object(
                video_track,
                PublishedObject(group_id=0, object_id=frame_id, payload=frame),
            )
            frame_id += 1

        return_code = await process.wait()
        if return_code != 0:
            raise RuntimeError(f'ffmpeg exited with rc={return_code}')
    finally:
        stderr_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await stderr_task
        if process.returncode is None:
            process.terminate()
            with contextlib.suppress(asyncio.TimeoutError):
                await asyncio.wait_for(process.wait(), timeout=3)
            if process.returncode is None:
                process.kill()
                await process.wait()
        publisher.disconnect()


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--relay-host', required=False, default='127.0.0.1')
    parser.add_argument('--relay-port', type=int, default=9007)
    parser.add_argument('--camera-name')
    parser.add_argument('--list-cameras', action='store_true')
    parser.add_argument('--ffmpeg-bin', default='ffmpeg')
    parser.add_argument('--ffmpeg-log-level', default='warning')
    parser.add_argument('--width', type=int, default=1280)
    parser.add_argument('--height', type=int, default=720)
    parser.add_argument('--fps', type=float, default=30.0)
    parser.add_argument('--bitrate', default='2500k')
    parser.add_argument('--bufsize', default='5000k')
    parser.add_argument('--rtbufsize', default='100M')
    parser.add_argument('--preset', default='ultrafast')
    parser.add_argument('--gop', type=int, default=30)
    parser.add_argument('--codec', default='avc1.42E01F')
    parser.add_argument('--namespace', default=DEFAULT_NAMESPACE)
    parser.add_argument('--video-name', default=DEFAULT_VIDEO_NAME)
    parser.add_argument('--meta-name', default=DEFAULT_META_NAME)
    parser.add_argument('--connect-timeout', type=float, default=10.0)
    parser.add_argument('--read-size', type=int, default=65536)
    parser.add_argument('--log-every', type=int, default=30)
    parser.add_argument('--log-level', default='INFO')
    return parser.parse_args()


async def main() -> None:
    args = parse_args()
    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format='%(asctime)s %(levelname)s %(message)s',
    )
    if args.list_cameras:
        raise SystemExit(list_windows_cameras(args.ffmpeg_bin))
    if not args.camera_name:
        raise SystemExit('--camera-name is required unless --list-cameras is used')
    await publish_camera(args)


if __name__ == '__main__':
    asyncio.run(main())
