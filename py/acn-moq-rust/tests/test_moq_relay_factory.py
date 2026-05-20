#!/usr/bin/env python3
"""
Tests for selectable MOQ relay implementations.
"""

import os
import sys
from pathlib import Path

import pytest

MOQ_RUST_ROOT = Path(__file__).resolve().parents[1]
REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT))
sys.path.insert(0, str(MOQ_RUST_ROOT))

from moq_rust.relay_factory import (
    BRIDGE_RELAY_IMPL,
    PYTHON_RELAY_IMPL,
    RUST_RELAY_IMPL,
    get_bridge_rust_port,
    get_relay_port_checks,
    get_relay_impl,
)
from moq_official_relay import BridgeMOQRelay, RustMOQRelay, RustRelayStartupError

TEST_RELAY_PORT = 9004
TEST_BRIDGE_RUST_PORT = 19004


def test_get_relay_impl_defaults_to_python(monkeypatch):
    monkeypatch.delenv('MOQ_RELAY_IMPL', raising=False)

    assert get_relay_impl() == PYTHON_RELAY_IMPL


def test_get_relay_impl_validates_supported_values(monkeypatch):
    monkeypatch.setenv('MOQ_RELAY_IMPL', 'unknown')

    with pytest.raises(ValueError, match='Unsupported MOQ_RELAY_IMPL'):
        get_relay_impl()


def test_get_bridge_rust_port_defaults_to_public_port_plus_offset(monkeypatch):
    monkeypatch.delenv('MOQ_RUST_RELAY_PORT', raising=False)

    assert get_bridge_rust_port(TEST_RELAY_PORT) == TEST_BRIDGE_RUST_PORT


@pytest.mark.parametrize(
    'relay_impl, expected',
    [
        (PYTHON_RELAY_IMPL, [('MOQT Relay server', '127.0.0.1', TEST_RELAY_PORT, 'udp')]),
        (
            RUST_RELAY_IMPL,
            [
                ('Rust MOQT Relay QUIC server', '127.0.0.1', TEST_RELAY_PORT, 'udp'),
                ('Rust MOQT Relay HTTP server', '127.0.0.1', TEST_RELAY_PORT, 'tcp'),
            ],
        ),
        (
            BRIDGE_RELAY_IMPL,
            [
                ('Bridge Python MOQT Relay server', '127.0.0.1', TEST_RELAY_PORT, 'udp'),
                ('Bridge Rust MOQT Relay QUIC server', '127.0.0.1', TEST_BRIDGE_RUST_PORT, 'udp'),
                ('Bridge Rust MOQT Relay HTTP server', '127.0.0.1', TEST_BRIDGE_RUST_PORT, 'tcp'),
            ],
        ),
    ],
)
def test_get_relay_port_checks(monkeypatch, relay_impl, expected):
    monkeypatch.delenv('MOQ_RUST_RELAY_PORT', raising=False)

    assert get_relay_port_checks('127.0.0.1', TEST_RELAY_PORT, relay_impl) == expected


def test_rust_relay_writes_official_config(tmp_path):
    relay = RustMOQRelay(host='127.0.0.1', port=9443, cache_dir=str(tmp_path))

    config_path = relay._write_config()
    config = config_path.read_text(encoding='utf-8')

    assert '[server]' in config
    assert '[cache]' not in config
    assert 'group_max_age_secs' not in config
    assert 'listen = "127.0.0.1:9443"' in config
    assert '[web.http]' in config
    assert 'public = ""' in config
    assert 'tls.generate = ["localhost", "127.0.0.1"]' in config


def test_rust_relay_sets_group_cache_age_env(tmp_path):
    relay = RustMOQRelay(
        host='127.0.0.1',
        port=9443,
        cache_dir=str(tmp_path),
        group_cache_age_secs=600,
    )

    env = relay._build_env()

    assert env['MOQ_LITE_MAX_GROUP_AGE_SECS'] == '600'


def test_rust_relay_ignores_invalid_group_cache_age_env(tmp_path, monkeypatch):
    monkeypatch.setenv('MOQ_LITE_MAX_GROUP_AGE_SECS', 'invalid')
    relay = RustMOQRelay(host='127.0.0.1', port=9443, cache_dir=str(tmp_path))

    assert relay._build_env()['MOQ_LITE_MAX_GROUP_AGE_SECS'] == '300'


def test_rust_relay_requires_binary_or_source(tmp_path, monkeypatch):
    monkeypatch.delenv('MOQ_OFFICIAL_RELAY_BIN', raising=False)
    monkeypatch.delenv('MOQ_OFFICIAL_RELAY_SOURCE', raising=False)
    monkeypatch.setattr('moq_official_relay.relay.shutil.which', lambda _: None)
    relay = RustMOQRelay(host='127.0.0.1', port=9443, cache_dir=str(tmp_path))

    with pytest.raises(RustRelayStartupError, match='Official Rust relay is not available'):
        relay._build_command(tmp_path / 'relay.toml')


@pytest.mark.asyncio
async def test_bridge_mode_defaults_to_python_relay_only(tmp_path, monkeypatch):
    monkeypatch.setenv('MOQ_BRIDGE_START_RUST', 'false')
    relay = BridgeMOQRelay(
        host='localhost',
        port=TEST_RELAY_PORT,
        rust_port=TEST_BRIDGE_RUST_PORT,
        cache_dir=str(tmp_path),
    )
    relay.python_relay.cache.clear_disk_cache = lambda: None

    async def fake_python_start():
        relay.python_started = True

    async def fake_python_stop():
        relay.python_stopped = True

    async def fail_rust_start():
        raise AssertionError('rust sidecar should not start by default')

    relay.python_relay.start = fake_python_start
    relay.python_relay.stop = fake_python_stop
    relay.rust_relay.start = fail_rust_start

    await relay.start()
    await relay.stop()

    assert relay.python_started is True
    assert relay.python_stopped is True
