import errno
from datetime import timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest
from cryptography import x509
from cryptography.hazmat.primitives.asymmetric import ec

from _path_helper import ensure_repo_root

ensure_repo_root()

from examples.video_webtransport_subscriber_example import BrowserPageServer
from examples.video_webtransport_subscriber_example import (
    BrowserBridgeProtocol,
    BrowserH3Connection,
    SETTINGS_WT_MAX_SESSIONS,
    WT_MAX_SESSIONS,
    build_browser_metadata,
    build_player_page,
    find_listener_pids,
    generate_webtransport_certificate,
    infer_avc1_codec_from_init_segment,
    release_listener_port,
    address_in_use_error,
)


@pytest.mark.asyncio
async def test_browser_page_server_falls_back_when_port_is_in_use(monkeypatch):
    preferred_port = 9004
    fallback_port = 18080
    fake_server = AsyncMock()
    fake_server.close = Mock()
    fake_server.sockets = [SimpleNamespace(getsockname=lambda: ("127.0.0.1", fallback_port))]

    start_server = AsyncMock(
        side_effect=[
            OSError(errno.EADDRINUSE, "address already in use"),
            fake_server,
        ]
    )
    monkeypatch.setattr("examples.video_webtransport_subscriber_example.asyncio.start_server", start_server)
    stop_listener_processes = Mock(return_value=[])
    monkeypatch.setattr(
        "examples.video_webtransport_subscriber_example.stop_listener_processes",
        stop_listener_processes,
    )

    page_server = BrowserPageServer(b"<html></html>", port=preferred_port)

    await page_server.start()
    assert page_server.port == fallback_port
    assert start_server.await_args_list[0].args[1:] == ("127.0.0.1", preferred_port)
    assert start_server.await_args_list[1].args[1:] == ("127.0.0.1", 0)

    await page_server.stop()
    fake_server.close.assert_called_once()
    fake_server.wait_closed.assert_awaited_once()


@pytest.mark.asyncio
async def test_browser_page_server_retries_preferred_port_after_stopping_existing_listener(monkeypatch):
    preferred_port = 9004
    fake_server = AsyncMock()
    fake_server.close = Mock()
    fake_server.sockets = [SimpleNamespace(getsockname=lambda: ("127.0.0.1", preferred_port))]

    start_server = AsyncMock(
        side_effect=[
            OSError(errno.EADDRINUSE, "address already in use"),
            fake_server,
        ]
    )
    monkeypatch.setattr("examples.video_webtransport_subscriber_example.asyncio.start_server", start_server)
    stop_listener_processes = Mock(side_effect=[[], [43210]])
    monkeypatch.setattr(
        "examples.video_webtransport_subscriber_example.stop_listener_processes",
        stop_listener_processes,
    )
    monkeypatch.setattr("examples.video_webtransport_subscriber_example.asyncio.sleep", AsyncMock())

    page_server = BrowserPageServer(b"<html></html>", port=preferred_port, fallback_to_ephemeral=False)

    await page_server.start()
    assert page_server.port == preferred_port
    assert start_server.await_args_list[0].args[1:] == ("127.0.0.1", preferred_port)
    assert start_server.await_args_list[1].args[1:] == ("127.0.0.1", preferred_port)

    await page_server.stop()
    fake_server.close.assert_called_once()
    fake_server.wait_closed.assert_awaited_once()


def test_find_listener_pids_parses_ss_output(monkeypatch):
    tcp_completed = SimpleNamespace(
        returncode=0,
        stdout='LISTEN 0 100 127.0.0.1:9004 0.0.0.0:* users:(("python3",pid=1234,fd=5))\n',
    )
    udp_completed = SimpleNamespace(
        returncode=0,
        stdout='UNCONN 0 0 127.0.0.1:9004 0.0.0.0:* users:(("python3",pid=5678,fd=7))\n',
    )
    run = Mock(side_effect=[tcp_completed, udp_completed])
    monkeypatch.setattr("examples.video_webtransport_subscriber_example.platform.system", lambda: "Linux")
    monkeypatch.setattr("examples.video_webtransport_subscriber_example.shutil.which", lambda name: "/usr/bin/ss")
    monkeypatch.setattr(
        "examples.video_webtransport_subscriber_example.subprocess.run",
        run,
    )

    assert find_listener_pids(9004) == {1234, 5678}
    assert run.call_args_list[0].args[0] == ["ss", "-ltnp", "sport = :9004"]
    assert run.call_args_list[1].args[0] == ["ss", "-lunp", "sport = :9004"]


def test_address_in_use_error_includes_exact_listener_address():
    error = address_in_use_error(
        "WebTransport bridge",
        "127.0.0.1",
        4433,
        OSError(errno.EADDRINUSE, "address already in use"),
    )

    assert error.errno == errno.EADDRINUSE
    assert "WebTransport bridge address already in use: 127.0.0.1:4433" in str(error)


@pytest.mark.asyncio
async def test_release_listener_port_waits_after_stopping_existing_listener(monkeypatch):
    stop_listener_processes = Mock(return_value=[43210])
    find_pids = Mock(return_value=[])
    sleep = AsyncMock()
    monkeypatch.setattr(
        "examples.video_webtransport_subscriber_example.stop_listener_processes",
        stop_listener_processes,
    )
    monkeypatch.setattr("examples.video_webtransport_subscriber_example.find_listener_pids", find_pids)
    monkeypatch.setattr("examples.video_webtransport_subscriber_example.asyncio.sleep", sleep)

    assert await release_listener_port(4433, "WebTransport bridge port") is True
    stop_listener_processes.assert_called_once_with(4433)
    find_pids.assert_called_once_with(4433)
    sleep.assert_awaited_once_with(0.2)


@pytest.mark.asyncio
async def test_release_listener_port_returns_false_when_no_listener_was_stopped(monkeypatch):
    stop_listener_processes = Mock(return_value=[])
    sleep = AsyncMock()
    monkeypatch.setattr(
        "examples.video_webtransport_subscriber_example.stop_listener_processes",
        stop_listener_processes,
    )
    monkeypatch.setattr("examples.video_webtransport_subscriber_example.asyncio.sleep", sleep)

    assert await release_listener_port(4433, "WebTransport bridge port") is False
    stop_listener_processes.assert_called_once_with(4433)
    sleep.assert_not_called()


@pytest.mark.asyncio
async def test_release_listener_port_force_stops_stubborn_listener(monkeypatch):
    stop_listener_processes = Mock(return_value=[43210])
    find_pids = Mock(return_value={43210})
    sleep = AsyncMock()
    kill = Mock()
    monkeypatch.setattr(
        "examples.video_webtransport_subscriber_example.stop_listener_processes",
        stop_listener_processes,
    )
    monkeypatch.setattr("examples.video_webtransport_subscriber_example.find_listener_pids", find_pids)
    monkeypatch.setattr("examples.video_webtransport_subscriber_example.asyncio.sleep", sleep)
    monkeypatch.setattr("examples.video_webtransport_subscriber_example.os.kill", kill)

    assert await release_listener_port(9004, "Browser page port") is True
    kill.assert_called_once()
    assert kill.call_args.args[0] == 43210


def test_browser_h3_connection_advertises_current_webtransport_setting():
    connection = object.__new__(BrowserH3Connection)
    connection._max_table_capacity = 4096
    connection._blocked_streams = 16
    connection._enable_webtransport = True

    settings = BrowserH3Connection._get_local_settings(connection)

    assert WT_MAX_SESSIONS > 1
    assert settings[SETTINGS_WT_MAX_SESSIONS] == WT_MAX_SESSIONS


def test_drop_session_removes_browser_session_from_bridge():
    protocol = object.__new__(BrowserBridgeProtocol)
    protocol._bridge = SimpleNamespace(remove_session=Mock(), sessions=set())
    session = SimpleNamespace(close=Mock(), session_id=7, origin=None)
    protocol._sessions = {7: session}

    BrowserBridgeProtocol.drop_session(protocol, 7)

    assert protocol._sessions == {}
    protocol._bridge.remove_session.assert_called_once_with(session)
    session.close.assert_called_once()


def test_build_player_page_uses_explicit_webtransport_host():
    page = build_player_page(
        "0123456789abcdef",
        webtransport_host="127.0.0.1",
        webtransport_port=4433,
    ).decode("utf-8")

    assert 'const WT_HOST = "127.0.0.1";' in page
    assert 'const WT_PORT = 4433;' in page
    assert "candidateWebTransportHosts" in page
    assert "window.location.hostname" in page
    assert "All WebTransport connection attempts failed." in page
    assert "if (state.mediaSource) {" in page
    assert 'logLine("segment received before metadata; queued")' in page
    assert "flushPendingSegments();" in page
    assert "function syncPlaybackPosition(reason)" in page
    assert "rangeEnd - 2.0" in page
    assert "bufferAhead >= 0.75" in page
    assert 'logLine(`seeked to buffered range (${reason}): ${liveEdge.toFixed(3)}s`)' in page
    assert 'logLine(`appendBuffer failed: ${error.message}`)' in page
    assert 'logLine(`addSourceBuffer failed: ${error.message}`)' in page
    assert 'logLine("video playing")' in page
    assert 'logLine(`video error (${detail})`)' in page
    assert 'document.addEventListener("visibilitychange"' in page
    assert 'requestPlayback("visibilitychange")' in page
    assert 'elements.video.addEventListener("click"' in page
    assert 'window.addEventListener("pagehide"' in page
    assert 'window.addEventListener("beforeunload"' in page
    assert 'closeActiveTransport("pagehide")' in page


def test_infer_avc1_codec_from_init_segment_reads_avcc_profile_triplet():
    init_segment = (
        b"\x00\x00\x00\x33avcC"
        b"\x01"
        b"\x64"
        b"\x00"
        b"\x1f"
        b"\xff\xe1\x00\x18"
    )

    assert infer_avc1_codec_from_init_segment(init_segment) == "avc1.64001F"


def test_build_browser_metadata_prefers_codec_inferred_from_init_segment():
    metadata = {"codec": "H.264"}
    init_segment = (
        b"\x00\x00\x00\x33avcC"
        b"\x01"
        b"\x64"
        b"\x00"
        b"\x1f"
        b"\xff\xe1\x00\x18"
    )

    browser_metadata = build_browser_metadata(metadata, init_segment=init_segment)

    assert browser_metadata["mse_codec"] == "avc1.64001F"
    assert browser_metadata["mime_type"] == 'video/mp4; codecs="avc1.64001F"'


def test_generate_webtransport_certificate_uses_browser_compatible_ecdsa_cert():
    cert_path, _, cert_hash_hex, temp_dir = generate_webtransport_certificate("127.0.0.1")
    try:
        with open(cert_path, "rb") as cert_file:
            certificate = x509.load_pem_x509_certificate(cert_file.read())

        assert isinstance(certificate.public_key(), ec.EllipticCurvePublicKey)
        assert certificate.public_key().curve.name == "secp256r1"
        assert certificate.not_valid_after_utc - certificate.not_valid_before_utc <= timedelta(days=14)
        assert len(cert_hash_hex) == 64
    finally:
        temp_dir.cleanup()


def test_generate_webtransport_certificate_reuses_cached_certificate(monkeypatch, tmp_path):
    monkeypatch.setattr(
        "examples.video_webtransport_subscriber_example.WEBTRANSPORT_CERT_CACHE_ROOT",
        str(tmp_path),
    )

    first_cert_path, first_key_path, first_hash, first_dir = generate_webtransport_certificate("127.0.0.1")
    second_cert_path, second_key_path, second_hash, second_dir = generate_webtransport_certificate("127.0.0.1")
    try:
        assert second_cert_path == first_cert_path
        assert second_key_path == first_key_path
        assert second_hash == first_hash
        assert first_dir.name == second_dir.name
    finally:
        first_dir.cleanup()
        second_dir.cleanup()
