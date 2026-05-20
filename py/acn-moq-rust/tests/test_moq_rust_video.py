#!/usr/bin/env python3
"""
Tests for the moq_rust_video Python wheel API.
"""

import io
import stat
import struct
import subprocess
import sys
from pathlib import Path

import pytest

MOQ_RUST_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(MOQ_RUST_ROOT))

from moq_rust_video.binaries import (
    platform_key,
    resolve_moq_cli,
    verify_moq_cli_supports_avc3,
)
from moq_rust_video.errors import FrameDecodeError, ProcessStartError
from moq_rust_video.publisher import CameraPublisher
from moq_rust_video.subscriber import read_avc3_frame


def test_platform_key_maps_supported_targets():
    assert platform_key('Windows', 'AMD64') == 'win_amd64'
    assert platform_key('Linux', 'x86_64') == 'manylinux_x86_64'
    assert platform_key('Darwin', 'arm64') == 'macosx_arm64'
    assert platform_key('Darwin', 'x86_64') == 'macosx_x86_64'


def test_resolve_moq_cli_prefers_explicit_path(tmp_path):
    binary = tmp_path / 'moq-cli'
    binary.write_text('#!/bin/sh\nexit 0\n', encoding='utf-8')
    binary.chmod(binary.stat().st_mode | stat.S_IXUSR)

    assert resolve_moq_cli(binary) == str(binary)


def test_verify_moq_cli_supports_avc3_accepts_help_output(monkeypatch):
    def fake_run(*args, **kwargs):
        return subprocess.CompletedProcess(
            args=args[0],
            returncode=0,
            stdout='[possible values: avc3, fmp4]',
        )

    monkeypatch.setattr(subprocess, 'run', fake_run)

    verify_moq_cli_supports_avc3('/fake/moq-cli')


def test_verify_moq_cli_supports_avc3_rejects_old_cli(monkeypatch):
    def fake_run(*args, **kwargs):
        return subprocess.CompletedProcess(
            args=args[0],
            returncode=0,
            stdout='[possible values: fmp4]',
        )

    monkeypatch.setattr(subprocess, 'run', fake_run)

    with pytest.raises(ProcessStartError, match='does not support'):
        verify_moq_cli_supports_avc3('/fake/moq-cli')


def test_camera_publisher_builds_windows_dshow_avc3_pipeline():
    publisher = CameraPublisher(
        relay_url='http://relay.example:9007/',
        name='camera',
        camera_name='HD Camera',
        backend='dshow',
        width=1280,
        height=720,
        fps=30,
        bitrate='2500k',
    )

    ffmpeg_cmd = publisher.build_ffmpeg_command('ffmpeg')
    moq_cmd = publisher.build_moq_command('moq-cli.exe')

    assert ffmpeg_cmd[:4] == ['ffmpeg', '-hide_banner', '-loglevel', 'warning']
    assert ['-f', 'dshow'] == ffmpeg_cmd[4:6]
    assert 'video=HD Camera' in ffmpeg_cmd
    assert '-f' in ffmpeg_cmd
    assert ffmpeg_cmd[-2:] == ['h264', '-']
    assert any('repeat-headers=1' in part for part in ffmpeg_cmd)
    assert moq_cmd == [
        'moq-cli.exe',
        '--log-level',
        'warn',
        '--iroh-enabled=false',
        'publish',
        '--client-bind',
        '0.0.0.0:0',
        '--url',
        'http://relay.example:9007/',
        '--name',
        'camera',
        'avc3',
    ]


def test_camera_publisher_builds_test_source():
    publisher = CameraPublisher.test_source(
        relay_url='http://relay.example:9007/',
        width=640,
        height=360,
        fps=30,
        bitrate='800k',
    )

    command = publisher.build_ffmpeg_command('ffmpeg')

    assert '-re' in command
    assert 'testsrc2=size=640x360:rate=30' in command
    assert '-bufsize' in command
    assert '1600k' in command


def test_read_avc3_frame_decodes_mavc_stream():
    payload = b'\x00\x00\x00\x01\x65abc'
    stream = io.BytesIO(
        struct.pack('!4sQBI', b'MAVC', 12345, 1, len(payload)) + payload
    )

    frame = read_avc3_frame(stream)

    assert frame.timestamp_us == 12345
    assert frame.keyframe is True
    assert frame.payload == payload
    assert frame.received_epoch_ms > 0


def test_read_avc3_frame_rejects_bad_magic():
    stream = io.BytesIO(struct.pack('!4sQBI', b'BAD!', 0, 0, 1) + b'x')

    with pytest.raises(FrameDecodeError, match='magic'):
        read_avc3_frame(stream)


def test_read_avc3_frame_rejects_oversized_payload():
    stream = io.BytesIO(struct.pack('!4sQBI', b'MAVC', 0, 0, 10) + b'1234567890')

    with pytest.raises(FrameDecodeError, match='payload length'):
        read_avc3_frame(stream, max_frame_bytes=4)
