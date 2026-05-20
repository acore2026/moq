#!/usr/bin/env python3
"""
Debug remote video publishing through moq_rust_video.

This script is intended to run on the remote endpoint after installing the
moq_rust_video wheel. It prints the resolved binaries, child process commands,
process status, and recent ffmpeg/moq-cli logs.
"""

from __future__ import annotations

import argparse
import time

from moq_rust_video import CameraPublisher
from moq_rust_video.binaries import resolve_ffmpeg, resolve_moq_cli


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--relay-url',
        default='http://101.245.78.174:9007/',
        help='Relay URL, for example http://101.245.78.174:9007/.',
    )
    parser.add_argument('--name', default='camera', help='MoQ broadcast name.')
    parser.add_argument(
        '--mode',
        choices=['camera', 'testsrc'],
        default='camera',
        help='Use a real camera or ffmpeg test source.',
    )
    parser.add_argument(
        '--backend',
        default='dshow',
        choices=['auto', 'dshow', 'v4l2', 'avfoundation', 'lavfi'],
        help='ffmpeg capture backend.',
    )
    parser.add_argument('--camera-name', default='HD Camera')
    parser.add_argument('--device', default=None)
    parser.add_argument('--width', type=int, default=1280)
    parser.add_argument('--height', type=int, default=720)
    parser.add_argument('--fps', type=float, default=30.0)
    parser.add_argument('--bitrate', default='2500k')
    parser.add_argument('--ffmpeg-path', default=None)
    parser.add_argument('--moq-cli-path', default=None)
    parser.add_argument('--client-bind', default='0.0.0.0:0')
    parser.add_argument('--interval', type=float, default=2.0)
    parser.add_argument('--tail', type=int, default=40)
    parser.add_argument('--duration', type=float, default=0.0)
    return parser.parse_args()


def build_publisher(args: argparse.Namespace) -> CameraPublisher:
    common = {
        'relay_url': args.relay_url,
        'name': args.name,
        'width': args.width,
        'height': args.height,
        'fps': args.fps,
        'bitrate': args.bitrate,
        'ffmpeg_path': args.ffmpeg_path,
        'moq_cli_path': args.moq_cli_path,
        'client_bind': args.client_bind,
        'log_level': 'debug',
        'ffmpeg_log_level': 'info',
        'verify_moq_cli': True,
    }

    if args.mode == 'testsrc':
        return CameraPublisher.test_source(**common)

    return CameraPublisher(
        backend=args.backend,
        camera_name=args.camera_name,
        device=args.device,
        **common,
    )


def print_lines(title: str, lines: list[str]) -> None:
    print(f'\n[{title}]')
    if not lines:
        print('(no logs yet)')
        return
    for line in lines:
        print(line)


def main() -> int:
    args = parse_args()

    moq_cli = resolve_moq_cli(args.moq_cli_path)
    ffmpeg = resolve_ffmpeg(args.ffmpeg_path)

    print('relay_url =', args.relay_url)
    print('name =', args.name)
    print('mode =', args.mode)
    print('moq-cli =', moq_cli)
    print('ffmpeg =', ffmpeg)

    publisher = build_publisher(args)
    print('\n[ffmpeg command]')
    print(' '.join(publisher.build_ffmpeg_command(ffmpeg)))
    print('\n[moq-cli command]')
    print(' '.join(publisher.build_moq_command(moq_cli)))

    publisher.start()
    started = time.monotonic()
    printed_count = 0

    try:
        while True:
            status = publisher.status()
            print('\n[status]')
            print(status)

            logs = publisher.logs()
            new_logs = logs[printed_count:]
            if new_logs:
                print_lines('new logs', new_logs[-args.tail:])
                printed_count = len(logs)
            else:
                print_lines('new logs', [])

            if not status.running:
                print_lines('final log tail', publisher.logs()[-args.tail:])
                return status.moq_returncode or status.ffmpeg_returncode or 1

            if args.duration > 0 and time.monotonic() - started >= args.duration:
                print('\n[duration reached, stopping]')
                return 0

            time.sleep(args.interval)
    except KeyboardInterrupt:
        print('\n[interrupted, stopping]')
        return 130
    finally:
        publisher.stop()


if __name__ == '__main__':
    raise SystemExit(main())
