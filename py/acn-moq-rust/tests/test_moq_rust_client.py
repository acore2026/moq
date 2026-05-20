#!/usr/bin/env python3
"""
Tests for the ACN SDK compatible Rust moq-cli client wrapper.
"""

import io
import stat
import sys
from pathlib import Path

MOQ_RUST_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(MOQ_RUST_ROOT))

from moq_rust_client import RustCliMoQClient
from moq_rust_client.acn_client import OBJECT_OP_OBJECT, read_object_frame, write_object_frame


def test_object_frame_roundtrip():
    stream = io.BytesIO()

    write_object_frame(stream, OBJECT_OP_OBJECT, 'Location', 2, 7, b'payload')
    stream.seek(0)
    event = read_object_frame(stream)

    assert event == {
        'op': OBJECT_OP_OBJECT,
        'track': 'Location',
        'group_id': 2,
        'object_id': 7,
        'payload': b'payload',
    }


def _fake_moq_cli(tmp_path: Path) -> Path:
    binary = tmp_path / 'moq-cli'
    binary.write_text('#!/bin/sh\nexit 0\n', encoding='utf-8')
    binary.chmod(binary.stat().st_mode | stat.S_IXUSR)
    return binary


def test_acn_client_builds_publish_command(tmp_path):
    binary = _fake_moq_cli(tmp_path)
    client = RustCliMoQClient(
        '127.0.0.1',
        9007,
        'publisher',
        moq_cli_path=binary,
        verify_cli=False,
    )
    client.connect()

    assert client.build_publish_command('/task-123/agent-1') == [
        str(binary),
        '--log-level',
        'warn',
        '--iroh-enabled=false',
        'publish',
        '--client-bind',
        '0.0.0.0:0',
        '--url',
        'http://127.0.0.1:9007/',
        '--name',
        'task-123/agent-1',
        'object',
    ]


def test_acn_client_builds_subscribe_command(tmp_path):
    binary = _fake_moq_cli(tmp_path)
    client = RustCliMoQClient(
        'http://relay.example:9007/',
        9007,
        'subscriber',
        moq_cli_path=binary,
        verify_cli=False,
        max_latency_ms=50,
    )
    client.connect()

    assert client.build_subscribe_command('/task-123/agent-1', 'Location') == [
        str(binary),
        '--log-level',
        'warn',
        '--iroh-enabled=false',
        'subscribe',
        '--client-bind',
        '0.0.0.0:0',
        '--url',
        'http://relay.example:9007/',
        '--name',
        'task-123/agent-1',
        '--output',
        'object',
        '--track',
        'Location',
        '--max-latency',
        '50',
    ]


def test_acn_client_builds_fetch_range_subscribe_command(tmp_path):
    binary = _fake_moq_cli(tmp_path)
    client = RustCliMoQClient(
        'http://relay.example:9007/',
        9007,
        'subscriber',
        moq_cli_path=binary,
        verify_cli=False,
    )
    client.connect()

    assert client.build_subscribe_command(
        '/task-123/agent-1',
        'Location',
        start_group=2,
        start_object=2,
        end_group=8,
        end_object=8,
    )[-8:] == [
        '--start-group',
        '2',
        '--start-object',
        '2',
        '--end-group',
        '8',
        '--end-object',
        '8',
    ]


def test_acn_client_builds_fetch_command(tmp_path):
    binary = _fake_moq_cli(tmp_path)
    client = RustCliMoQClient(
        'http://relay.example:9007/',
        9007,
        'subscriber',
        moq_cli_path=binary,
        verify_cli=False,
        fetch_idle_timeout_ms=750,
    )
    client.connect()

    assert client.build_fetch_command(
        '/task-123/agent-1',
        'Location',
        start_group=2,
        start_object=4,
        end_group=8,
        end_object=16,
    ) == [
        str(binary),
        '--log-level',
        'warn',
        '--iroh-enabled=false',
        'fetch',
        '--client-bind',
        '0.0.0.0:0',
        '--url',
        'http://relay.example:9007/',
        '--name',
        'task-123/agent-1',
        '--output',
        'object',
        '--track',
        'Location',
        '--start-group',
        '2',
        '--start-object',
        '4',
        '--idle-timeout-ms',
        '750',
        '--end-group',
        '8',
        '--end-object',
        '16',
    ]


def test_acn_client_filters_events_by_fetch_range(tmp_path):
    binary = _fake_moq_cli(tmp_path)
    client = RustCliMoQClient(
        '127.0.0.1',
        9007,
        'subscriber',
        moq_cli_path=binary,
        verify_cli=False,
    )
    client.connect()
    key = client._track_key('/task-123/agent-1', 'Location')
    client._subscription_ranges[key] = (2, 2, 8, 8)

    assert not client._event_in_range(key, {'group_id': 1, 'object_id': 1})
    assert client._event_in_range(key, {'group_id': 5, 'object_id': 5})
    assert not client._event_in_range(key, {'group_id': 9, 'object_id': 9})


class _FakeProcess:
    def __init__(self, returncode=None):
        self.returncode = returncode

    def poll(self):
        return self.returncode


def test_fetch_starts_independent_fetch_process(tmp_path):
    binary = _fake_moq_cli(tmp_path)
    client = RustCliMoQClient(
        '127.0.0.1',
        9007,
        'subscriber',
        moq_cli_path=binary,
        verify_cli=False,
    )
    client.connect()
    calls = []

    def fake_start(namespace, track, start_group, start_object, end_group, end_object, request_id):
        calls.append((namespace, track, start_group, start_object, end_group, end_object, request_id))

    client._start_fetch = fake_start

    request_id = client.fetch('/task-123/agent-1', 'Location', start_object=3, end_object=3)

    assert request_id == 1
    assert calls == [('/task-123/agent-1', 'Location', 0, 3, None, 3, 1)]


def test_fetch_does_not_depend_on_existing_subscriber_process(tmp_path):
    binary = _fake_moq_cli(tmp_path)
    client = RustCliMoQClient(
        '127.0.0.1',
        9007,
        'subscriber',
        moq_cli_path=binary,
        verify_cli=False,
    )
    client.connect()
    key = client._track_key('/task-123/agent-1', 'Location')
    client._subscription_processes[key] = _FakeProcess(returncode=42)
    calls = []

    def fake_start(namespace, track, start_group, start_object, end_group, end_object, request_id):
        calls.append((namespace, track, start_group, start_object, end_group, end_object, request_id))

    client._start_fetch = fake_start

    client.fetch('/task-123/agent-1', 'Location', start_group=2, start_object=4)

    assert calls == [('/task-123/agent-1', 'Location', 2, 4, None, None, 1)]
