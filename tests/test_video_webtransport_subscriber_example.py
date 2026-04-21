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
    generate_webtransport_certificate,
    infer_avc1_codec_from_init_segment,
)


@pytest.mark.asyncio
async def test_browser_page_server_falls_back_when_port_is_in_use(monkeypatch):
    preferred_port = 8080
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

    page_server = BrowserPageServer(b"<html></html>", port=preferred_port)

    await page_server.start()
    assert page_server.port == fallback_port
    assert start_server.await_args_list[0].args[1:] == ("127.0.0.1", preferred_port)
    assert start_server.await_args_list[1].args[1:] == ("127.0.0.1", 0)

    await page_server.stop()
    fake_server.close.assert_called_once()
    fake_server.wait_closed.assert_awaited_once()


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
