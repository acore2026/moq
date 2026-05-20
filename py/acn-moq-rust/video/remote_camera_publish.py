#!/usr/bin/env python3
"""
Publish a real camera to a Rust MoQ relay through moq_rust_video.

Run this on the remote endpoint after installing the moq_rust_video wheel.
"""

from __future__ import annotations

import argparse
import time

from moq_rust_video import CameraPublisher


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--relay-url',
        default='http://101.245.78.174:9007/',
        help='Rust relay URL.',
    )
    parser.add_argument('--name', default='camera', help='MoQ broadcast name.')
    parser.add_argument(
        '--backend',
        default='auto',
        choices=['auto', 'dshow', 'v4l2', 'avfoundation'],
        help='Camera backend. Windows uses dshow, Linux uses v4l2, macOS uses avfoundation.',
    )
    parser.add_argument(
        '--camera-name',
        default='HD Camera',
        help='Camera name for Windows dshow or macOS avfoundation.',
    )
    parser.add_argument(
        '--device',
        default=None,
        help='Camera device, for example /dev/video0 on Linux.',
    )
    parser.add_argument('--width', type=int, default=1280)
    parser.add_argument('--height', type=int, default=720)
    parser.add_argument('--fps', type=float, default=30.0)
    parser.add_argument('--bitrate', default='2500k')
    parser.add_argument('--ffmpeg-path', default=None)
    parser.add_argument('--moq-cli-path', default=None)
    parser.add_argument('--client-bind', default='0.0.0.0:0')
    parser.add_argument(
        '--print-logs',
        action='store_true',
        help='Print recent ffmpeg/moq-cli logs while publishing.',
    )
    parser.add_argument('--log-interval', type=float, default=2.0)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    publisher = CameraPublisher(
        relay_url=args.relay_url,
        name=args.name,
        backend=args.backend,
        camera_name=args.camera_name,
        device=args.device,
        width=args.width,
        height=args.height,
        fps=args.fps,
        bitrate=args.bitrate,
        ffmpeg_path=args.ffmpeg_path,
        moq_cli_path=args.moq_cli_path,
        client_bind=args.client_bind,
        log_level='info',
        ffmpeg_log_level='warning',
        verify_moq_cli=True,
    )

    print('starting camera publisher')
    print('relay_url =', args.relay_url)
    print('name =', args.name)
    print('backend =', args.backend)
    print('camera_name =', args.camera_name)
    if args.device:
        print('device =', args.device)
    print('resolution =', f'{args.width}x{args.height}@{args.fps}')
    print('bitrate =', args.bitrate)

    publisher.start()

    try:
        if not args.print_logs:
            return publisher.wait()

        printed_count = 0
        while True:
            status = publisher.status()
            print('status =', status)
            logs = publisher.logs()
            for line in logs[printed_count:]:
                print(line)
            printed_count = len(logs)
            if not status.running:
                return status.moq_returncode or status.ffmpeg_returncode or 1
            time.sleep(args.log_interval)
    except KeyboardInterrupt:
        print('stopping camera publisher')
        return 130
    finally:
        publisher.stop()


if __name__ == '__main__':
    raise SystemExit(main())
