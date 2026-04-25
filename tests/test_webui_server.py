import asyncio
import json
from collections import deque
from unittest.mock import AsyncMock, Mock

import pytest
from fastapi import HTTPException
from starlette.responses import StreamingResponse

from _path_helper import ensure_repo_root

ensure_repo_root()

from moq import FullTrackName
from examples.webui import server as webui_server
from examples.webui.server import (
    HTTP_PORT,
    PUBLIC_WEB_HOST,
    STATIC_DIR,
    WEBUI_REPLAY_FRAGMENTS,
    WEBUI_ROOT,
    SubscriberBridgeService,
    build_allowed_origins,
    build_control_page,
    create_app,
    format_track_name,
    parse_subscription_track,
)


class DummySubscriber:
    def __init__(self, running: bool = False):
        self.running = running
        self.cert_hash_hex = "abc123"
        self.started = 0
        self.stopped = 0
        self.removed = []
        self.track = "video/h264-live"
        self.tracks = [self.track] if running else []

    async def start(self, track_name=None):
        next_track = format_track_name(track_name) if track_name is not None else self.track
        self.running = True
        self.started += 1
        self.track = next_track
        if next_track not in self.tracks:
            self.tracks.append(next_track)

    async def stop(self):
        self.running = False
        self.stopped += 1
        self.tracks = []

    async def remove_subscription(self, track_name=None):
        next_track = format_track_name(track_name) if track_name is not None else self.track
        self.removed.append(next_track)
        self.tracks = [track for track in self.tracks if track != next_track]
        if self.track == next_track:
            self.track = self.tracks[-1] if self.tracks else "video/h264-live"

    @property
    def active_tracks(self):
        return list(self.tracks)

    def status(self):
        return {
            "running": self.running,
            "cert_hash_hex": self.cert_hash_hex if self.running else None,
            "track": self.track,
            "tracks": list(self.tracks),
            "viewers": 0,
            "metadata_ready": False,
            "init_ready": False,
            "fragments": 0,
            "bytes": 0,
        }


class DummyLogs:
    def __init__(self):
        self.lines = []

    def add(self, message):
        self.lines.append(message)

    def snapshot(self):
        return self.lines or ["hello", "world"]


class DummyController:
    def __init__(self, subscriber_running: bool = False):
        self.subscriber = DummySubscriber(subscriber_running)
        self.logs = DummyLogs()

    async def shutdown(self):
        return None

    def _notify_status_changed(self):
        return None

    async def status_event_stream(self):
        yield f"event: status\ndata: {json.dumps(self.status())}\n\n"
        yield f"event: logs\ndata: {json.dumps(self.logs.snapshot())}\n\n"

    def status(self):
        return {
            "relay_host": "127.0.0.1",
            "relay_port": 28446,
            "http_port": HTTP_PORT,
            "webtransport_port": HTTP_PORT,
            "webtransport_public_host": PUBLIC_WEB_HOST,
            "relay": {"running": None, "host": "127.0.0.1", "port": 28446, "managed_by_webui": False},
            "subscriber": self.subscriber.status(),
            "logs": self.logs.snapshot(),
        }


def route_endpoint(app, path: str, method: str):
    for route in app.routes:
        if getattr(route, "path", None) == path and method in getattr(route, "methods", set()):
            return route.endpoint
    raise AssertionError(f"Route not found: {method} {path}")


def test_build_allowed_origins_includes_public_and_local_hosts():
    origins = build_allowed_origins("101.245.78.174", HTTP_PORT)

    assert f"http://101.245.78.174:{HTTP_PORT}" in origins
    assert f"https://101.245.78.174:{HTTP_PORT}" in origins
    assert f"http://localhost:{HTTP_PORT}" in origins
    assert f"https://localhost:{HTTP_PORT}" in origins
    assert f"http://127.0.0.1:{HTTP_PORT}" in origins
    assert f"https://127.0.0.1:{HTTP_PORT}" in origins


def test_webui_local_publisher_wrappers_default_to_embedded_relay_port():
    video_script = (WEBUI_ROOT / "video_publisher.py").read_text(encoding="utf-8")

    assert 'os.environ.get("MOQ_RELAY_PORT", "28446")' in video_script
    assert 'os.environ.get("MOQ_TEST_SOURCE", "testsrc2")' in video_script
    assert '"smptebars"' in video_script
    assert '"mandelbrot"' in video_script


def test_webui_manual_relay_and_publisher_scripts_are_split():
    relay_script = (WEBUI_ROOT / "start_relay.py").read_text(encoding="utf-8")
    publishers_script = (WEBUI_ROOT / "start_publishers.py").read_text(encoding="utf-8")

    assert "MOQRelay" in relay_script
    assert "video_publisher.py" not in relay_script
    assert "MOQRelay" not in publishers_script
    assert "video_publisher.py" in publishers_script
    assert 'DEFAULT_SOURCES = ("testsrc2", "testsrc", "smptebars")' in publishers_script
    assert "--sources" in publishers_script


def test_webui_single_publisher_start_scripts_select_expected_sources():
    h264_script = (WEBUI_ROOT / "start_publisher_h264.py").read_text(encoding="utf-8")
    testsrc_script = (WEBUI_ROOT / "start_publisher_testsrc.py").read_text(encoding="utf-8")
    smptebars_script = (WEBUI_ROOT / "start_publisher_smptebars.py").read_text(encoding="utf-8")

    assert 'main("testsrc2")' in h264_script
    assert 'main("testsrc")' in testsrc_script
    assert 'main("smptebars")' in smptebars_script


def test_build_control_page_contains_control_buttons():
    page = build_control_page("101.245.78.174")

    assert "MANAGE TRACK LIST" in page
    assert "Add Track" in page
    assert "Start Subscriber" not in page
    assert "Start Publisher" not in page
    assert 'id="testSourceSelect"' not in page
    assert 'id="subscriberButton"' in page
    assert 'id="subscriberStopButton"' not in page
    assert 'id="subscriptionListWrap"' in page
    assert 'id="subscriptionChips"' in page
    assert 'id="publisherButton"' not in page
    assert 'id="namespaceInput"' in page
    assert 'id="trackNameInput"' in page
    assert 'id="previewTrackLabel"' in page
    assert 'id="logPanel"' in page
    assert 'id="logs"' in page
    assert "Operator Log" not in page
    assert "Preview Track: video/h264-live" in page
    assert 'wtHost: "101.245.78.174"' in page
    assert "<video id=\"video\"" in page
    assert "video-overlay" not in page
    assert "video-notice" not in page
    assert 'id="byteCount"' in page
    assert 'id="fragmentCount"' in page


def test_webui_page_certificate_uses_public_host_certificate(monkeypatch):
    cert_factory = Mock(return_value=("/tmp/cert.pem", "/tmp/key.pem", "abc123", Mock()))
    monkeypatch.setattr(webui_server, "generate_webtransport_certificate", cert_factory)

    assert webui_server.webui_page_certificate() == ("/tmp/cert.pem", "/tmp/key.pem", "abc123")
    cert_factory.assert_called_once_with(PUBLIC_WEB_HOST)


def test_webui_main_runs_https_server(monkeypatch):
    run = Mock()
    monkeypatch.setattr(webui_server, "uvicorn", Mock(run=run))
    monkeypatch.setattr(webui_server, "webui_page_certificate", Mock(return_value=("/tmp/cert.pem", "/tmp/key.pem", "abc123")))

    webui_server.main()

    run.assert_called_once()
    assert run.call_args.kwargs["ssl_certfile"] == "/tmp/cert.pem"
    assert run.call_args.kwargs["ssl_keyfile"] == "/tmp/key.pem"
    assert run.call_args.kwargs["port"] == HTTP_PORT


def test_webui_script_prefills_subscription_track_for_publishers():
    script = (STATIC_DIR / "app.js").read_text(encoding="utf-8")

    assert "function selectedTestSource()" not in script
    assert "function candidateWebTransportHosts()" in script
    assert '["localhost", "127.0.0.1", "::1"].includes(window.location.hostname)' in script
    assert "All WebTransport connection attempts failed" in script
    assert "state.transport = null;" in script
    assert "state.transportError = true;" in script
    assert "WebTransport requires a secure context" in script
    assert "WebTransport API is unavailable in this browser" in script
    assert "Waiting for browser WebTransport connection." in script
    assert 'line.classList.add("error")' in script
    assert "elements.testSourceSelect" not in script
    assert "Stop selected real-time test source" not in script
    assert "function selectedPublisherStatus()" not in script
    assert "Switching preview to selected track. Waiting for live stream." in script
    assert "Switching preview to ${track}. Waiting for live stream." in script
    assert "state.playbackGeneration += 1;" in script
    assert "this.playbackGeneration !== state.playbackGeneration" in script
    assert "URL.revokeObjectURL(state.videoUrl)" in script
    assert "state.metadata = null;" in script
    assert "closeActiveTransport();" in script
    assert "await reconnectPreviewTransport(payload.status.subscriber.cert_hash_hex)" in script
    assert "function shouldDisplayServerLog(message)" in script
    assert "/^(camera publisher|video publisher|publisher(?:\\s+\\S+)?):/i" in script
    assert "Subscriber: forwarded live fragment" in script
    assert 'requestPlayback("fragment")' not in script
    assert "if (!state.videoUrl && !elements.video.currentSrc)" in script
    assert "const MAX_APPEND_QUEUE_SEGMENTS = 8;" in script
    assert "function recoverMediaPipeline(reason)" in script
    assert "function setPreviewPlaceholder(title, detail)" in script
    assert '".preview-placeholder-title, .empty-text"' in script
    assert "sourceBuffer.addEventListener(\"error\"" in script
    assert "elements.video.addEventListener(\"error\"" in script
    assert "dropped ${state.droppedFragments} stale fragment(s) to keep live latency low" in script
    assert "state.fragments === 1 || state.fragments % 30 === 0" in script
    assert "Track name is required" in script


def test_webui_subscriber_bridge_uses_larger_fragment_replay_window():
    bridge = SubscriberBridgeService(DummyLogs())._bridge

    assert bridge.recent_fragments.maxlen == WEBUI_REPLAY_FRAGMENTS


def test_webui_restore_preview_track_replays_cached_state():
    service = SubscriberBridgeService(DummyLogs())
    first_track = FullTrackName([b"video"], b"h264-live")
    second_track = FullTrackName([b"video"], b"testsrc-live")

    first_state = service._get_track_state(first_track)  # noqa: SLF001
    first_state.metadata = {"mime_type": 'video/mp4; codecs="avc1.64001F"'}
    first_state.init_segment = b"first-init"
    first_state.recent_fragments = deque([b"first-fragment"], maxlen=WEBUI_REPLAY_FRAGMENTS)

    second_state = service._get_track_state(second_track)  # noqa: SLF001
    second_state.metadata = {"mime_type": 'video/mp4; codecs="avc1.64001F"'}
    second_state.init_segment = b"second-init"
    second_state.recent_fragments = deque([b"second-fragment-1", b"second-fragment-2"], maxlen=WEBUI_REPLAY_FRAGMENTS)

    service._preview_track = second_track  # noqa: SLF001
    service._restore_preview_track()  # noqa: SLF001

    assert service._bridge.metadata == second_state.metadata  # noqa: SLF001
    assert service._bridge.init_segment == b"second-init"  # noqa: SLF001
    assert list(service._bridge.recent_fragments) == [b"second-fragment-1", b"second-fragment-2"]  # noqa: SLF001


def test_webui_subscriber_status_reports_preview_delivery_state():
    service = SubscriberBridgeService(DummyLogs())
    video_track = FullTrackName([b"video"], b"h264-live")
    video_state = service._get_track_state(video_track)  # noqa: SLF001
    video_state.metadata = {"mime_type": 'video/mp4; codecs="avc1.64001F"'}
    video_state.init_segment = b"video-init"
    video_state.total_bytes = 1234
    video_state.total_fragments = 5
    service._preview_track = video_track  # noqa: SLF001
    service._active_tracks = [video_track]  # noqa: SLF001
    service._running = True  # noqa: SLF001

    status = service.status()

    assert status["track"] == "video/h264-live"
    assert status["metadata_ready"] is True
    assert status["init_ready"] is True
    assert status["fragments"] == 5
    assert status["bytes"] == 1234
    assert status["viewers"] == 0


def test_webui_track_ready_event_requires_metadata_and_init_segment():
    service = SubscriberBridgeService(DummyLogs())
    video_track = FullTrackName([b"video"], b"h264-live")
    video_state = service._get_track_state(video_track)  # noqa: SLF001
    ready_event = service._get_track_ready_event(video_track)  # noqa: SLF001

    video_state.metadata = {"mime_type": 'video/mp4; codecs="avc1.64001F"'}
    service._mark_track_ready_if_possible(video_track)  # noqa: SLF001

    assert not ready_event.is_set()

    video_state.init_segment = b"video-init"
    service._mark_track_ready_if_possible(video_track)  # noqa: SLF001

    assert ready_event.is_set()


@pytest.mark.asyncio
async def test_webui_subscriber_releases_webtransport_port_before_binding(monkeypatch):
    release_port = AsyncMock()
    fake_server = Mock()
    fake_server.close = Mock()
    serve = AsyncMock(return_value=fake_server)
    subscriber = AsyncMock()
    subscriber.connect = AsyncMock(return_value=False)
    subscriber.set_handlers = Mock()
    monkeypatch.setattr("examples.webui.server.release_listener_port", release_port)
    monkeypatch.setattr("examples.webui.server.serve", serve)
    monkeypatch.setattr("examples.webui.server.MOQSubscriber", Mock(return_value=subscriber))
    monkeypatch.setattr(
        "examples.webui.server.generate_webtransport_certificate",
        Mock(return_value=("/tmp/cert.pem", "/tmp/key.pem", "abc123", Mock())),
    )
    monkeypatch.setattr("examples.webui.server.QuicConfiguration.load_cert_chain", Mock())

    service = SubscriberBridgeService(DummyLogs())

    with pytest.raises(RuntimeError, match="Failed to connect subscriber to relay"):
        await service.start()

    release_port.assert_awaited_once_with(HTTP_PORT, "WebTransport bridge port")
    serve.assert_awaited_once()


def test_parse_subscription_track_uses_default_when_empty():
    track = parse_subscription_track("", "")

    assert format_track_name(track) == "video/h264-live"


def test_parse_subscription_track_accepts_custom_namespace_and_track():
    track = parse_subscription_track("demo/live", "cam-1")

    assert format_track_name(track) == "demo/live/cam-1"


def test_webui_status_endpoint_returns_controller_state():
    app = create_app(DummyController())
    endpoint = route_endpoint(app, "/api/status", "GET")

    payload = asyncio.run(endpoint())

    assert payload["ok"] is True
    assert payload["status"]["relay"]["managed_by_webui"] is False
    assert payload["status"]["subscriber"]["running"] is False
    assert payload["status"]["logs"] == ["hello", "world"]


def test_webui_events_endpoint_streams_initial_status():
    app = create_app(DummyController())
    endpoint = route_endpoint(app, "/api/events", "GET")

    response = asyncio.run(endpoint())

    assert isinstance(response, StreamingResponse)

    async def read_first_chunk():
        async for chunk in response.body_iterator:
            return chunk
        raise AssertionError("Expected at least one SSE chunk")

    first_chunk = asyncio.run(read_first_chunk())
    if isinstance(first_chunk, bytes):
        first_chunk = first_chunk.decode("utf-8")

    assert "event: status" in first_chunk
    payload = first_chunk.split("data: ", 1)[1].strip()
    parsed = json.loads(payload)
    assert parsed["subscriber"]["running"] is False


def test_webui_events_endpoint_streams_initial_logs_snapshot():
    app = create_app(DummyController())
    endpoint = route_endpoint(app, "/api/events", "GET")

    response = asyncio.run(endpoint())

    async def read_two_chunks():
        chunks = []
        async for chunk in response.body_iterator:
            chunks.append(chunk.decode("utf-8") if isinstance(chunk, bytes) else chunk)
            if len(chunks) == 2:
                return chunks
        raise AssertionError("Expected two SSE chunks")

    first_chunk, second_chunk = asyncio.run(read_two_chunks())

    assert "event: status" in first_chunk
    assert "event: logs" in second_chunk
    payload = second_chunk.split("data: ", 1)[1].strip()
    parsed = json.loads(payload)
    assert parsed == ["hello", "world"]


@pytest.mark.asyncio
async def test_webui_lifespan_starts_controller_before_serving():
    controller = DummyController()
    controller.startup = AsyncMock()
    controller.shutdown = AsyncMock()
    app = create_app(controller)

    async with app.router.lifespan_context(app):
        pass

    controller.startup.assert_awaited_once()
    controller.shutdown.assert_awaited_once()


def test_webui_can_start_subscriber_with_custom_track():
    controller = DummyController(subscriber_running=False)
    app = create_app(controller)
    subscriber_start = route_endpoint(app, "/api/subscriber/start", "POST")

    payload = asyncio.run(subscriber_start({"namespace": "demo/live", "trackName": "cam-1"}))

    assert payload["ok"] is True
    assert controller.subscriber.started == 1
    assert controller.subscriber.track == "demo/live/cam-1"


def test_webui_can_add_subscription_while_running():
    controller = DummyController(subscriber_running=True)
    app = create_app(controller)
    subscriber_start = route_endpoint(app, "/api/subscriber/start", "POST")

    payload = asyncio.run(subscriber_start({"namespace": "demo/live", "trackName": "cam-2"}))

    assert payload["ok"] is True
    assert controller.subscriber.started == 1
    assert controller.subscriber.track == "demo/live/cam-2"
    assert controller.subscriber.tracks == ["video/h264-live", "demo/live/cam-2"]


def test_webui_can_remove_subscription_while_running():
    controller = DummyController(subscriber_running=True)
    controller.subscriber.tracks = ["video/h264-live", "demo/live/cam-2"]
    controller.subscriber.track = "demo/live/cam-2"
    app = create_app(controller)
    subscriber_remove = route_endpoint(app, "/api/subscriber/remove", "POST")

    payload = asyncio.run(subscriber_remove({"namespace": "demo/live", "trackName": "cam-2"}))

    assert payload["ok"] is True
    assert controller.subscriber.removed == ["demo/live/cam-2"]
    assert controller.subscriber.track == "video/h264-live"
    assert controller.subscriber.tracks == ["video/h264-live"]
