#!/usr/bin/env python3
"""
MOQ Camera Web UI.

Starts a browser-accessible control page on port 9004 for subscribing to
externally published MOQ video tracks and displaying the selected track.

Typical usage:
    1. python examples/webui/start_relay.py
    2. python examples/webui/start_publishers.py
    3. python examples/webui/server.py
    4. Open https://101.245.78.174:9004/ in an external browser
    5. Add or remove tracks from the Manage Track List panel
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import sys
from collections import deque
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, AsyncIterator

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

try:
    from examples._bootstrap import ensure_repo_root, setup_logging
except ImportError:  # pragma: no cover - direct script execution fallback
    from _bootstrap import ensure_repo_root, setup_logging

ensure_repo_root()
setup_logging()

from aioquic.asyncio import serve
from aioquic.h3.connection import H3_ALPN
from aioquic.quic.configuration import QuicConfiguration
from fastapi import Body, FastAPI, HTTPException
from fastapi.responses import HTMLResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
import uvicorn

from moq import FullTrackName, MOQSubscriber, ObjectStatus, ReceivedObject
from examples.video_webtransport_subscriber_example import (
    BrowserBridgeProtocol,
    BrowserBroadcastState,
    WEBTRANSPORT_PATH,
    address_in_use_error,
    build_browser_metadata,
    generate_webtransport_certificate,
    release_listener_port,
)

logger = logging.getLogger(__name__)

WEBUI_ROOT = Path(__file__).resolve().parent
STATIC_DIR = WEBUI_ROOT / "static"

HTTP_BIND_HOST = "0.0.0.0"
HTTP_PORT = 9004
WEBTRANSPORT_BIND_HOST = "0.0.0.0"
WEBTRANSPORT_PORT = int(os.environ.get("MOQ_WEBTRANSPORT_PORT", str(HTTP_PORT)))
PUBLIC_WEB_HOST = os.environ.get("MOQ_WEBUI_PUBLIC_HOST", "101.245.78.174")
RELAY_HOST = os.environ.get("MOQ_RELAY_HOST", "127.0.0.1")
RELAY_PORT = int(os.environ.get("MOQ_RELAY_PORT", "28446"))
MAX_LOG_LINES = 120
WEBUI_REPLAY_FRAGMENTS = 12
WEBUI_TRACK_READY_TIMEOUT = 5.0
DEFAULT_WEBUI_TRACK = FullTrackName([b"video"], b"h264-live")


def build_allowed_origins(public_host: str, http_port: int) -> set[str]:
    hosts = {public_host, "localhost", "127.0.0.1"}
    return {f"{scheme}://{host}:{http_port}" for scheme in ("http", "https") for host in hosts}


def parse_subscription_track(namespace: str | None, track_name: str | None) -> FullTrackName:
    raw_namespace = (namespace or "").strip()
    raw_track_name = (track_name or "").strip()

    if not raw_namespace and not raw_track_name:
        return DEFAULT_WEBUI_TRACK

    namespace_fields = [field.strip() for field in raw_namespace.split("/") if field.strip()] if raw_namespace else []
    if not namespace_fields:
        namespace_fields = [field.decode("utf-8") for field in DEFAULT_WEBUI_TRACK.namespace]

    resolved_track_name = raw_track_name or DEFAULT_WEBUI_TRACK.track_name.decode("utf-8")
    return FullTrackName([field.encode("utf-8") for field in namespace_fields], resolved_track_name.encode("utf-8"))


def format_track_name(track_name: FullTrackName) -> str:
    namespace = "/".join(field.decode("utf-8", errors="replace") for field in track_name.namespace)
    name = track_name.track_name.decode("utf-8", errors="replace")
    return f"{namespace}/{name}" if namespace else name


def build_control_page(public_host: str) -> str:
    template = (STATIC_DIR / "index.html").read_text(encoding="utf-8")
    return (
        template.replace("__PUBLIC_HOST__", public_host)
        .replace("__HTTP_PORT__", str(HTTP_PORT))
        .replace("__WEBTRANSPORT_PORT__", str(WEBTRANSPORT_PORT))
        .replace("__WEBTRANSPORT_PATH__", WEBTRANSPORT_PATH)
    )


def webui_page_certificate() -> tuple[str, str, str]:
    cert_path, key_path, cert_hash_hex, _ = generate_webtransport_certificate(PUBLIC_WEB_HOST)
    return cert_path, key_path, cert_hash_hex


class UILogBuffer:
    def __init__(self, limit: int = MAX_LOG_LINES, on_change=None):
        self._lines: deque[str] = deque(maxlen=limit)
        self._on_change = on_change

    def add(self, message: str):
        logger.info(message)
        self._lines.append(message)
        if self._on_change is not None:
            self._on_change(message)

    def snapshot(self) -> list[str]:
        return list(self._lines)


@dataclass
class TrackPreviewState:
    metadata: dict | None = None
    init_segment: bytes | None = None
    recent_fragments: deque[bytes] = field(default_factory=lambda: deque(maxlen=WEBUI_REPLAY_FRAGMENTS))
    total_bytes: int = 0
    total_fragments: int = 0


class SubscriberBridgeService:
    def __init__(self, log_buffer: UILogBuffer):
        self._logs = log_buffer
        self._subscriber: MOQSubscriber | None = None
        self._bridge = BrowserBroadcastState()
        self._bridge.recent_fragments = deque(maxlen=WEBUI_REPLAY_FRAGMENTS)
        self._processor_task: asyncio.Task | None = None
        self._object_queue: asyncio.Queue[ReceivedObject] = asyncio.Queue()
        self._webtransport_server = None
        self._cert_dir = None
        self._cert_hash_hex: str | None = None
        self._active_tracks: list[FullTrackName] = []
        self._preview_track: FullTrackName = DEFAULT_WEBUI_TRACK
        self._track_states: dict[str, TrackPreviewState] = {}
        self._track_ready_events: dict[str, asyncio.Event] = {}
        self._running = False

    @property
    def running(self) -> bool:
        return self._running

    @property
    def cert_hash_hex(self) -> str | None:
        return self._cert_hash_hex

    @property
    def track_name(self) -> FullTrackName:
        return self._preview_track

    @property
    def active_tracks(self) -> list[FullTrackName]:
        return list(self._active_tracks)

    def _resolve_track_for_alias(self, track_alias: int) -> FullTrackName | None:
        if not self._subscriber:
            return None
        return self._subscriber._track_aliases.get(track_alias)  # noqa: SLF001

    def _track_key(self, track_name: FullTrackName) -> str:
        return format_track_name(track_name)

    def _get_track_state(self, track_name: FullTrackName) -> TrackPreviewState:
        key = self._track_key(track_name)
        if key not in self._track_states:
            self._track_states[key] = TrackPreviewState()
        return self._track_states[key]

    def _get_track_ready_event(self, track_name: FullTrackName) -> asyncio.Event:
        key = self._track_key(track_name)
        if key not in self._track_ready_events:
            self._track_ready_events[key] = asyncio.Event()
        return self._track_ready_events[key]

    def _mark_track_ready_if_possible(self, track_name: FullTrackName) -> None:
        state = self._get_track_state(track_name)
        if state.metadata is not None and state.init_segment is not None:
            self._get_track_ready_event(track_name).set()

    async def _wait_for_track_ready(self, track_name: FullTrackName) -> bool:
        state = self._get_track_state(track_name)
        if state.metadata is not None and state.init_segment is not None:
            return True
        try:
            await asyncio.wait_for(
                self._get_track_ready_event(track_name).wait(),
                timeout=WEBUI_TRACK_READY_TIMEOUT,
            )
            return True
        except asyncio.TimeoutError:
            self._logs.add(
                "Subscriber: timed out waiting for browser init data on "
                f"{format_track_name(track_name)}"
            )
            return False

    def _reset_bridge(self) -> None:
        self._bridge.end_stream()
        self._bridge.metadata = None
        self._bridge.init_segment = None
        self._bridge.recent_fragments.clear()
        self._bridge.total_bytes = 0
        self._bridge.total_fragments = 0

    def _restore_preview_track(self) -> None:
        self._reset_bridge()
        state = self._get_track_state(self._preview_track)
        if state.metadata is not None:
            self._bridge.set_metadata(state.metadata)
        if state.init_segment is not None:
            self._bridge.set_init_segment(state.init_segment)
        for fragment in state.recent_fragments:
            self._bridge.push_fragment(fragment)

    async def _process_received_objects(self):
        while True:
            obj = await self._object_queue.get()
            resolved_track = self._resolve_track_for_alias(obj.track_alias)
            if resolved_track is None:
                continue
            if obj.group_id != 1:
                continue
            track_state = self._get_track_state(resolved_track)
            is_preview_track = resolved_track == self._preview_track
            if obj.object_status == ObjectStatus.END_OF_SUBGROUP:
                if is_preview_track:
                    self._bridge.end_stream()
                self._logs.add(f"Subscriber: received end of live subgroup for {format_track_name(resolved_track)}")
                continue
            if obj.object_id == 1:
                try:
                    metadata = build_browser_metadata(json.loads(obj.payload.decode("utf-8")))
                except ValueError as exc:
                    if is_preview_track:
                        self._bridge.end_stream()
                    self._logs.add(
                        f"Subscriber: rejected metadata for browser preview on {format_track_name(resolved_track)} ({exc})"
                    )
                    continue
                track_state.metadata = metadata
                track_state.init_segment = None
                track_state.recent_fragments.clear()
                track_state.total_bytes = 0
                track_state.total_fragments = 0
                self._get_track_ready_event(resolved_track).clear()
                if is_preview_track:
                    self._bridge.set_metadata(metadata)
                self._logs.add(f"Subscriber: received stream metadata for {format_track_name(resolved_track)}")
                continue
            if track_state.init_segment is None:
                if track_state.metadata is not None:
                    try:
                        metadata = build_browser_metadata(track_state.metadata, init_segment=obj.payload)
                    except ValueError as exc:
                        if is_preview_track:
                            self._bridge.end_stream()
                        self._logs.add(
                            f"Subscriber: rejected init segment for browser preview on {format_track_name(resolved_track)} ({exc})"
                        )
                        continue
                    if metadata != track_state.metadata:
                        track_state.metadata = metadata
                        if is_preview_track:
                            self._bridge.set_metadata(metadata)
                track_state.init_segment = obj.payload
                track_state.total_bytes += len(obj.payload)
                self._mark_track_ready_if_possible(resolved_track)
                if is_preview_track:
                    self._bridge.set_init_segment(obj.payload)
                self._logs.add(
                    f"Subscriber: stored init segment for {format_track_name(resolved_track)} ({len(obj.payload)} bytes)"
                )
                continue
            track_state.recent_fragments.append(obj.payload)
            track_state.total_bytes += len(obj.payload)
            track_state.total_fragments += 1
            if is_preview_track:
                self._bridge.push_fragment(obj.payload)
                self._logs.add(
                    "Subscriber: forwarded live fragment for "
                    f"{format_track_name(resolved_track)} "
                    f"({len(obj.payload)} bytes, total_fragments={track_state.total_fragments}, "
                    f"viewers={len(self._bridge.sessions)})"
                )

    async def start(self, track_name: FullTrackName | None = None):
        requested_track = track_name or DEFAULT_WEBUI_TRACK

        if self._running:
            if requested_track not in self._active_tracks:
                assert self._subscriber is not None
                await self._subscriber.subscribe(requested_track, start_group=1, start_object=1)
                self._active_tracks.append(requested_track)
                self._logs.add(f"Subscriber: added subscription {format_track_name(requested_track)}")
            self._preview_track = requested_track
            await self._wait_for_track_ready(requested_track)
            self._restore_preview_track()
            self._logs.add(f"Subscriber: switched preview to {format_track_name(requested_track)}")
            return

        cert_path, key_path, cert_hash_hex, cert_dir = generate_webtransport_certificate(PUBLIC_WEB_HOST)
        quic_config = QuicConfiguration(
            alpn_protocols=H3_ALPN,
            is_client=False,
            max_datagram_frame_size=65536,
        )
        quic_config.load_cert_chain(cert_path, key_path)

        allowed_origins = build_allowed_origins(PUBLIC_WEB_HOST, HTTP_PORT)
        await release_listener_port(WEBTRANSPORT_PORT, "WebTransport bridge port")
        try:
            self._webtransport_server = await serve(
                WEBTRANSPORT_BIND_HOST,
                WEBTRANSPORT_PORT,
                configuration=quic_config,
                create_protocol=lambda *args, **kwargs: BrowserBridgeProtocol(
                    *args,
                    bridge=self._bridge,
                    allowed_origins=allowed_origins,
                    **kwargs,
                ),
            )
        except OSError as exc:
            raise address_in_use_error("WebTransport bridge", WEBTRANSPORT_BIND_HOST, WEBTRANSPORT_PORT, exc) from exc

        subscriber = MOQSubscriber(relay_host=RELAY_HOST, relay_port=RELAY_PORT)
        subscriber.set_handlers(
            on_connected=lambda: self._logs.add("Subscriber: connected to relay"),
            on_disconnected=lambda: self._logs.add("Subscriber: disconnected from relay"),
            on_subscription_accepted=lambda track_name: self._logs.add(f"Subscriber: accepted {track_name}"),
            on_subscription_rejected=lambda track_name, reason: self._logs.add(
                f"Subscriber: rejected {track_name} ({reason})"
            ),
            on_object_received=lambda obj: self._object_queue.put_nowait(obj),
        )

        if not await subscriber.connect():
            if self._webtransport_server is not None:
                self._webtransport_server.close()
                await asyncio.sleep(0)
                self._webtransport_server = None
            raise RuntimeError("Failed to connect subscriber to relay")

        self._subscriber = subscriber
        self._track_states = {}
        self._track_ready_events = {}
        self._processor_task = asyncio.create_task(self._process_received_objects())
        await subscriber.subscribe(requested_track, start_group=1, start_object=1)
        self._cert_dir = cert_dir
        self._cert_hash_hex = cert_hash_hex
        self._active_tracks = [requested_track]
        self._preview_track = requested_track
        self._running = True
        await self._wait_for_track_ready(requested_track)
        self._restore_preview_track()
        self._logs.add(
            f"Subscriber: bridge live at https://{PUBLIC_WEB_HOST}:{WEBTRANSPORT_PORT}{WEBTRANSPORT_PATH} "
            f"for {format_track_name(self._preview_track)} (cert sha256={cert_hash_hex})"
        )

    async def remove_subscription(self, track_name: FullTrackName | None = None):
        requested_track = track_name or DEFAULT_WEBUI_TRACK
        if not self._running or self._subscriber is None:
            raise RuntimeError("Subscriber is not running")
        if requested_track not in self._active_tracks:
            raise RuntimeError(f"Track is not subscribed: {format_track_name(requested_track)}")

        await self._subscriber.unsubscribe(requested_track)
        self._active_tracks = [track for track in self._active_tracks if track != requested_track]
        self._track_states.pop(self._track_key(requested_track), None)
        self._track_ready_events.pop(self._track_key(requested_track), None)
        self._logs.add(f"Subscriber: removed subscription {format_track_name(requested_track)}")

        if requested_track == self._preview_track:
            if self._active_tracks:
                self._preview_track = self._active_tracks[-1]
                self._restore_preview_track()
                self._logs.add(f"Subscriber: switched preview to {format_track_name(self._preview_track)}")
            else:
                self._preview_track = DEFAULT_WEBUI_TRACK
                self._reset_bridge()
                self._logs.add("Subscriber: no active subscriptions remain")

    async def stop(self):
        if not self._running:
            return

        if self._processor_task is not None:
            self._processor_task.cancel()
            try:
                await self._processor_task
            except asyncio.CancelledError:
                pass
            self._processor_task = None

        if self._webtransport_server is not None:
            self._webtransport_server.close()
            await asyncio.sleep(0)
            self._webtransport_server = None

        if self._subscriber is not None:
            for track_name in list(self._active_tracks):
                try:
                    await self._subscriber.unsubscribe(track_name)
                except Exception:
                    pass
            self._subscriber.disconnect()
            self._subscriber = None

        if self._cert_dir is not None:
            self._cert_dir.cleanup()
            self._cert_dir = None

        self._reset_bridge()
        self._cert_hash_hex = None
        self._active_tracks = []
        self._preview_track = DEFAULT_WEBUI_TRACK
        self._track_states = {}
        self._track_ready_events = {}
        self._running = False
        self._logs.add("Subscriber: stopped")

    def status(self) -> dict[str, Any]:
        preview_state = self._track_states.get(self._track_key(self._preview_track))
        return {
            "running": self._running,
            "cert_hash_hex": self._cert_hash_hex,
            "track": format_track_name(self._preview_track),
            "tracks": [format_track_name(track_name) for track_name in self._active_tracks],
            "viewers": len(self._bridge.sessions),
            "metadata_ready": bool(preview_state and preview_state.metadata is not None),
            "init_ready": bool(preview_state and preview_state.init_segment is not None),
            "fragments": preview_state.total_fragments if preview_state else 0,
            "bytes": preview_state.total_bytes if preview_state else 0,
        }


class WebUIController:
    def __init__(self):
        self._event_watchers: set[asyncio.Queue[dict[str, Any]]] = set()
        self.logs = UILogBuffer(on_change=self._notify_log_appended)
        self.subscriber = SubscriberBridgeService(self.logs)

    async def startup(self):
        return None

    async def shutdown(self):
        await self.subscriber.stop()

    def _publish_event(self, event_name: str, payload: Any):
        event = {"event": event_name, "data": payload}
        for watcher in list(self._event_watchers):
            try:
                watcher.put_nowait(event)
            except asyncio.QueueFull:
                try:
                    watcher.get_nowait()
                except asyncio.QueueEmpty:
                    pass
                try:
                    watcher.put_nowait(event)
                except asyncio.QueueFull:
                    pass

    def _notify_status_changed(self):
        self._publish_event("status", self.status())

    def _notify_log_appended(self, message: str):
        self._publish_event("log", message)

    def register_status_watcher(self) -> asyncio.Queue[dict[str, Any]]:
        queue: asyncio.Queue[dict[str, Any]] = asyncio.Queue(maxsize=8)
        queue.put_nowait({"event": "status", "data": self.status()})
        queue.put_nowait({"event": "logs", "data": self.logs.snapshot()})
        self._event_watchers.add(queue)
        return queue

    def unregister_status_watcher(self, queue: asyncio.Queue[dict[str, Any]]):
        self._event_watchers.discard(queue)

    async def status_event_stream(self) -> AsyncIterator[str]:
        queue = self.register_status_watcher()
        try:
            while True:
                event = await queue.get()
                yield f"event: {event['event']}\ndata: {json.dumps(event['data'])}\n\n"
        finally:
            self.unregister_status_watcher(queue)

    def status(self) -> dict[str, Any]:
        return {
            "relay_host": RELAY_HOST,
            "relay_port": RELAY_PORT,
            "http_port": HTTP_PORT,
            "webtransport_port": WEBTRANSPORT_PORT,
            "webtransport_public_host": PUBLIC_WEB_HOST,
            "relay": {
                "running": None,
                "host": RELAY_HOST,
                "port": RELAY_PORT,
                "managed_by_webui": False,
            },
            "subscriber": self.subscriber.status(),
            "logs": self.logs.snapshot(),
        }


def create_app(controller: WebUIController | None = None) -> FastAPI:
    controller = controller or WebUIController()

    @asynccontextmanager
    async def lifespan(_: FastAPI):
        startup = getattr(controller, "startup", None)
        if startup is not None:
            await startup()
        try:
            yield
        finally:
            await controller.shutdown()

    app = FastAPI(title="MOQ Camera Web UI", lifespan=lifespan)
    page_html = build_control_page(PUBLIC_WEB_HOST)
    app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")

    @app.get("/", response_class=HTMLResponse)
    async def index():
        return HTMLResponse(page_html)

    @app.get("/api/status")
    async def api_status():
        return {"ok": True, "status": controller.status()}

    @app.get("/api/events")
    async def api_events():
        return StreamingResponse(controller.status_event_stream(), media_type="text/event-stream")

    @app.post("/api/subscriber/start")
    async def api_subscriber_start(payload: dict[str, str] | None = Body(default=None)):
        try:
            payload_dict = payload if isinstance(payload, dict) else None
            requested_track = parse_subscription_track(
                payload_dict.get("namespace") if payload_dict else None,
                payload_dict.get("trackName") if payload_dict else None,
            )
            await controller.subscriber.start(requested_track)
        except Exception as exc:
            logger.exception("Failed to start subscriber bridge")
            raise HTTPException(status_code=500, detail=str(exc)) from exc
        controller._notify_status_changed()
        return {"ok": True, "status": controller.status()}

    @app.post("/api/subscriber/stop")
    async def api_subscriber_stop():
        await controller.subscriber.stop()
        controller._notify_status_changed()
        return {"ok": True, "status": controller.status()}

    @app.post("/api/subscriber/remove")
    async def api_subscriber_remove(payload: dict[str, str] | None = Body(default=None)):
        try:
            payload_dict = payload if isinstance(payload, dict) else None
            requested_track = parse_subscription_track(
                payload_dict.get("namespace") if payload_dict else None,
                payload_dict.get("trackName") if payload_dict else None,
            )
            await controller.subscriber.remove_subscription(requested_track)
            if not controller.subscriber.active_tracks:
                await controller.subscriber.stop()
        except Exception as exc:
            logger.exception("Failed to remove subscriber track")
            raise HTTPException(status_code=500, detail=str(exc)) from exc
        controller._notify_status_changed()
        return {"ok": True, "status": controller.status()}

    return app


app = create_app()


def main():
    logger.info("Starting MOQ camera web UI")
    cert_path, key_path, cert_hash_hex = webui_page_certificate()
    logger.info("Page: https://0.0.0.0:%d/", HTTP_PORT)
    logger.info("Expected external URL: https://%s:%d/", PUBLIC_WEB_HOST, HTTP_PORT)
    logger.info("HTTPS/WebTransport certificate sha256=%s", cert_hash_hex)
    uvicorn.run(
        app,
        host=HTTP_BIND_HOST,
        port=HTTP_PORT,
        log_level="info",
        ssl_certfile=cert_path,
        ssl_keyfile=key_path,
    )


if __name__ == "__main__":
    main()
