#!/usr/bin/env python3
"""
MOQ Live Video Subscriber with WebTransport browser preview.

This example subscribes to the live `video/h264-live` track from the relay,
keeps the initialization segment plus a short fragment replay window in memory,
and bridges the received fMP4 fragments to a browser over WebTransport.

Usage:
    1. python examples/relay_example.py
    2. python examples/video_publisher_example.py
    3. python examples/video_webtransport_subscriber_example.py
    4. Open the browser page URL printed by the subscriber
"""

import asyncio
import errno
import hashlib
import ipaddress
import json
import logging
import tempfile
from collections import deque
from datetime import datetime, timedelta
from email.utils import formatdate

try:
    from examples._bootstrap import ensure_repo_root, setup_logging
except ImportError:  # pragma: no cover - direct script execution fallback
    from _bootstrap import ensure_repo_root, setup_logging

ensure_repo_root()
setup_logging()

from aioquic.asyncio import QuicConnectionProtocol, serve
from aioquic.h3.connection import H3_ALPN, H3Connection
from aioquic.h3.events import DatagramReceived, H3Event, HeadersReceived, WebTransportStreamDataReceived
from aioquic.quic.configuration import QuicConfiguration
from aioquic.quic.events import ConnectionTerminated, ProtocolNegotiated, QuicEvent
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import NameOID

from moq import MOQSubscriber, FullTrackName, ObjectStatus, ReceivedObject

logger = logging.getLogger(__name__)

RELAY_HOST = "127.0.0.1"
RELAY_PORT = 4443
TRACK_NAME = FullTrackName([b"video"], b"h264-live")

PAGE_HOST = "127.0.0.1"
PAGE_PORT = 8080
WEBTRANSPORT_HOST = "127.0.0.1"
WEBTRANSPORT_PORT = 4433
WEBTRANSPORT_PATH = "/wt"
MAX_REPLAY_FRAGMENTS = 8
DEFAULT_MIME_TYPE = 'video/mp4; codecs="avc1.42E01F"'

FRAME_TYPE_JSON = 0x01
FRAME_TYPE_INIT = 0x02
FRAME_TYPE_FRAGMENT = 0x03
FRAME_TYPE_END = 0x04

HTML_TEMPLATE = """<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <title>MOQ Browser Subscriber</title>
    <style>
      :root {
        --paper: #f3ecdf;
        --ink: #171412;
        --accent: #b34622;
        --accent-soft: rgba(179, 70, 34, 0.16);
        --panel: rgba(255, 249, 240, 0.92);
        --edge: rgba(23, 20, 18, 0.14);
        --shadow: 0 24px 80px rgba(40, 18, 7, 0.18);
      }

      * {
        box-sizing: border-box;
      }

      body {
        margin: 0;
        min-height: 100vh;
        color: var(--ink);
        background:
          radial-gradient(circle at top left, rgba(179, 70, 34, 0.20), transparent 34%),
          radial-gradient(circle at bottom right, rgba(33, 92, 124, 0.18), transparent 32%),
          linear-gradient(135deg, #efe3d3 0%, #f8f1e8 46%, #ebdfd0 100%);
        font-family: "IBM Plex Mono", "SFMono-Regular", "Consolas", monospace;
      }

      body::before {
        content: "";
        position: fixed;
        inset: 0;
        pointer-events: none;
        opacity: 0.07;
        background-image:
          linear-gradient(rgba(0, 0, 0, 0.8) 1px, transparent 1px),
          linear-gradient(90deg, rgba(0, 0, 0, 0.8) 1px, transparent 1px);
        background-size: 28px 28px;
      }

      main {
        width: min(1180px, calc(100vw - 32px));
        margin: 0 auto;
        padding: 32px 0 40px;
      }

      .masthead {
        display: grid;
        gap: 14px;
        margin-bottom: 28px;
      }

      .eyebrow {
        display: inline-flex;
        align-items: center;
        gap: 10px;
        width: fit-content;
        padding: 8px 14px;
        border: 1px solid rgba(23, 20, 18, 0.1);
        border-radius: 999px;
        background: rgba(255, 255, 255, 0.54);
        letter-spacing: 0.08em;
        font-size: 12px;
        text-transform: uppercase;
      }

      h1 {
        margin: 0;
        max-width: 11ch;
        font-family: "Iowan Old Style", "Palatino Linotype", "Book Antiqua", serif;
        font-size: clamp(46px, 8vw, 92px);
        line-height: 0.94;
        letter-spacing: -0.04em;
      }

      .subcopy {
        max-width: 62ch;
        font-size: 14px;
        line-height: 1.8;
      }

      .grid {
        display: grid;
        grid-template-columns: minmax(0, 1.35fr) minmax(300px, 0.85fr);
        gap: 24px;
      }

      .panel {
        position: relative;
        overflow: hidden;
        border: 1px solid var(--edge);
        border-radius: 28px;
        background: var(--panel);
        box-shadow: var(--shadow);
        backdrop-filter: blur(14px);
      }

      .panel::after {
        content: "";
        position: absolute;
        inset: 0;
        border-radius: inherit;
        pointer-events: none;
        background: linear-gradient(180deg, rgba(255, 255, 255, 0.32), transparent 24%);
      }

      .stage {
        padding: 20px;
      }

      .stage-top {
        display: flex;
        justify-content: space-between;
        align-items: center;
        gap: 12px;
        margin-bottom: 16px;
      }

      .stage-title {
        font-size: 12px;
        letter-spacing: 0.12em;
        text-transform: uppercase;
      }

      .badge {
        display: inline-flex;
        align-items: center;
        gap: 8px;
        padding: 8px 12px;
        border-radius: 999px;
        background: var(--accent-soft);
        color: var(--accent);
        font-size: 12px;
      }

      .dot {
        width: 9px;
        height: 9px;
        border-radius: 999px;
        background: currentColor;
        box-shadow: 0 0 18px currentColor;
      }

      .video-wrap {
        position: relative;
        border-radius: 20px;
        overflow: hidden;
        background:
          linear-gradient(145deg, rgba(31, 22, 18, 0.95), rgba(54, 32, 24, 0.88)),
          repeating-linear-gradient(0deg, rgba(255, 255, 255, 0.04), rgba(255, 255, 255, 0.04) 2px, transparent 2px, transparent 6px);
        aspect-ratio: 16 / 9;
      }

      video {
        width: 100%;
        height: 100%;
        display: block;
        background: transparent;
      }

      .overlay {
        position: absolute;
        inset: auto 16px 16px 16px;
        display: grid;
        gap: 10px;
        padding: 14px 16px;
        border-radius: 18px;
        background: rgba(16, 12, 10, 0.64);
        color: #f9f3ea;
        font-size: 12px;
        backdrop-filter: blur(10px);
      }

      .stats {
        display: grid;
        grid-template-columns: repeat(4, minmax(0, 1fr));
        gap: 12px;
        padding: 20px;
      }

      .stat {
        padding: 14px 16px;
        border-radius: 18px;
        background: rgba(255, 255, 255, 0.6);
        border: 1px solid rgba(23, 20, 18, 0.08);
      }

      .stat-label {
        font-size: 11px;
        text-transform: uppercase;
        letter-spacing: 0.1em;
        opacity: 0.6;
      }

      .stat-value {
        margin-top: 10px;
        font-size: 24px;
        line-height: 1;
      }

      .sidebar {
        padding: 22px;
        display: grid;
        gap: 16px;
      }

      .card {
        padding: 18px;
        border-radius: 22px;
        background: rgba(255, 255, 255, 0.68);
        border: 1px solid rgba(23, 20, 18, 0.08);
      }

      .card h2 {
        margin: 0 0 10px;
        font-size: 13px;
        text-transform: uppercase;
        letter-spacing: 0.14em;
      }

      .meta-list {
        display: grid;
        gap: 9px;
        font-size: 13px;
      }

      .meta-row {
        display: flex;
        justify-content: space-between;
        gap: 16px;
      }

      .meta-key {
        opacity: 0.55;
      }

      .logs {
        min-height: 220px;
        max-height: 360px;
        overflow: auto;
        padding-right: 8px;
        font-size: 12px;
        line-height: 1.7;
      }

      .log-entry {
        padding: 10px 0;
        border-bottom: 1px dashed rgba(23, 20, 18, 0.12);
      }

      .hint {
        font-size: 12px;
        line-height: 1.7;
        opacity: 0.72;
      }

      @media (max-width: 980px) {
        .grid {
          grid-template-columns: 1fr;
        }

        .stats {
          grid-template-columns: repeat(2, minmax(0, 1fr));
        }
      }

      @media (max-width: 640px) {
        main {
          width: min(100vw - 20px, 1180px);
          padding-top: 18px;
        }

        .stats {
          grid-template-columns: 1fr;
        }

        .stage,
        .sidebar {
          padding: 16px;
        }
      }
    </style>
  </head>
  <body>
    <main>
      <section class="masthead">
        <span class="eyebrow">MOQ relay -> subscriber -> WebTransport -> browser</span>
        <h1>Live video in the browser.</h1>
        <div class="subcopy">
          This page is fed by a Python subscriber that receives fMP4 objects over MOQ,
          then rebroadcasts the initialization segment and live fragments to the browser
          through a dedicated WebTransport session.
        </div>
      </section>

      <section class="grid">
        <div class="panel">
          <div class="stage">
            <div class="stage-top">
              <div class="stage-title">Playback Deck</div>
              <div class="badge" id="connectionBadge"><span class="dot"></span><span id="connectionText">connecting</span></div>
            </div>

            <div class="video-wrap">
              <video id="video" autoplay muted playsinline controls></video>
              <div class="overlay">
                <div id="statusLine">Preparing WebTransport session…</div>
                <div id="detailLine">Waiting for metadata from the subscriber bridge.</div>
              </div>
            </div>
          </div>

          <div class="stats">
            <div class="stat">
              <div class="stat-label">Fragments</div>
              <div class="stat-value" id="fragmentCount">0</div>
            </div>
            <div class="stat">
              <div class="stat-label">Bytes</div>
              <div class="stat-value" id="byteCount">0</div>
            </div>
            <div class="stat">
              <div class="stat-label">Codec</div>
              <div class="stat-value" id="codecValue">-</div>
            </div>
            <div class="stat">
              <div class="stat-label">Resolution</div>
              <div class="stat-value" id="resolutionValue">-</div>
            </div>
          </div>
        </div>

        <aside class="panel">
          <div class="sidebar">
            <section class="card">
              <h2>Stream Metadata</h2>
              <div class="meta-list" id="metadataList">
                <div class="meta-row"><span class="meta-key">track</span><span>video/h264-live</span></div>
                <div class="meta-row"><span class="meta-key">transport</span><span>WebTransport uni stream</span></div>
                <div class="meta-row"><span class="meta-key">mime</span><span id="mimeValue">-</span></div>
                <div class="meta-row"><span class="meta-key">generated</span><span id="generatedAt">-</span></div>
              </div>
            </section>

            <section class="card">
              <h2>Operator Log</h2>
              <div class="logs" id="logs"></div>
            </section>

            <section class="card hint">
              The page pins the WebTransport server certificate hash generated by the
              subscriber example, so you do not need to trust the self-signed
              certificate manually. Use a Chromium-based browser with WebTransport and
              MediaSource support.
            </section>
          </div>
        </aside>
      </section>
    </main>

    <script>
      const FRAME_JSON = 1;
      const FRAME_INIT = 2;
      const FRAME_FRAGMENT = 3;
      const FRAME_END = 4;
      const WT_PORT = __WT_PORT__;
      const WT_PATH = "__WT_PATH__";
      const CERT_HASH_HEX = "__CERT_HASH__";

      const elements = {
        byteCount: document.getElementById("byteCount"),
        codecValue: document.getElementById("codecValue"),
        connectionBadge: document.getElementById("connectionBadge"),
        connectionText: document.getElementById("connectionText"),
        detailLine: document.getElementById("detailLine"),
        fragmentCount: document.getElementById("fragmentCount"),
        generatedAt: document.getElementById("generatedAt"),
        logs: document.getElementById("logs"),
        mimeValue: document.getElementById("mimeValue"),
        resolutionValue: document.getElementById("resolutionValue"),
        statusLine: document.getElementById("statusLine"),
        video: document.getElementById("video"),
      };

      const state = {
        appendQueue: [],
        bytes: 0,
        codec: null,
        fragments: 0,
        mediaSource: null,
        metadata: null,
        sourceBuffer: null,
        transport: null,
      };

      function hexToUint8Array(hex) {
        const pairs = hex.match(/.{1,2}/g) || [];
        return Uint8Array.from(pairs.map((pair) => parseInt(pair, 16)));
      }

      function setConnectionState(label, color) {
        elements.connectionText.textContent = label;
        elements.connectionBadge.style.color = color;
      }

      function logLine(message) {
        const line = document.createElement("div");
        line.className = "log-entry";
        line.textContent = `[${new Date().toLocaleTimeString()}] ${message}`;
        elements.logs.prepend(line);
      }

      function updateStats() {
        elements.byteCount.textContent = new Intl.NumberFormat().format(state.bytes);
        elements.fragmentCount.textContent = new Intl.NumberFormat().format(state.fragments);
        elements.codecValue.textContent = state.metadata?.mse_codec || state.metadata?.codec || "-";
        elements.resolutionValue.textContent = state.metadata ? `${state.metadata.width}x${state.metadata.height}` : "-";
        elements.mimeValue.textContent = state.metadata?.mime_type || "-";
        elements.generatedAt.textContent = state.metadata?.generated_at || "-";
      }

      function resetPlayer(reason) {
        if (state.mediaSource && state.mediaSource.readyState === "open") {
          try {
            state.mediaSource.endOfStream();
          } catch (error) {
            console.debug("endOfStream skipped", error);
          }
        }
        state.appendQueue = [];
        state.sourceBuffer = null;
        state.mediaSource = null;
        elements.video.removeAttribute("src");
        elements.video.load();
        elements.detailLine.textContent = reason;
      }

      function flushSourceBuffer() {
        if (!state.sourceBuffer || state.sourceBuffer.updating || state.appendQueue.length === 0) {
          return;
        }
        const next = state.appendQueue.shift();
        state.sourceBuffer.appendBuffer(next);
      }

      function ensurePlayer(metadata) {
        if (state.mediaSource && state.sourceBuffer) {
          return;
        }

        const mimeType = metadata.mime_type || `video/mp4; codecs="${metadata.mse_codec || "avc1.42E01F"}"`;
        if (!("MediaSource" in window) || !MediaSource.isTypeSupported(mimeType)) {
          throw new Error(`MediaSource does not support ${mimeType}`);
        }

        const mediaSource = new MediaSource();
        mediaSource.addEventListener("sourceopen", () => {
          const sourceBuffer = mediaSource.addSourceBuffer(mimeType);
          sourceBuffer.mode = "segments";
          sourceBuffer.addEventListener("updateend", flushSourceBuffer);
          state.sourceBuffer = sourceBuffer;
          flushSourceBuffer();
        }, { once: true });

        state.mediaSource = mediaSource;
        elements.video.src = URL.createObjectURL(mediaSource);
        elements.video.play().catch(() => {});
      }

      function enqueueSegment(bytes) {
        if (!state.metadata) {
          logLine("segment received before metadata; ignored");
          return;
        }
        ensurePlayer(state.metadata);
        state.appendQueue.push(bytes);
        flushSourceBuffer();
      }

      function handleControlMessage(message) {
        if (message.type === "metadata") {
          state.metadata = message.metadata;
          state.codec = state.metadata.mse_codec || state.metadata.codec || null;
          resetPlayer("Metadata updated. Waiting for init segment.");
          updateStats();
          elements.statusLine.textContent = "Metadata received from MOQ subscriber bridge.";
          logLine(`metadata received (${state.metadata.mime_type || "unknown mime"})`);
          return;
        }

        if (message.type === "end") {
          elements.statusLine.textContent = "Publisher closed the live subgroup.";
          elements.detailLine.textContent = "No new fragments are expected.";
          logLine("received end-of-stream marker");
        }
      }

      class FrameReader {
        constructor() {
          this.buffer = new Uint8Array(0);
        }

        push(chunk) {
          const merged = new Uint8Array(this.buffer.length + chunk.length);
          merged.set(this.buffer, 0);
          merged.set(chunk, this.buffer.length);
          this.buffer = merged;

          while (this.buffer.length >= 5) {
            const view = new DataView(this.buffer.buffer, this.buffer.byteOffset, this.buffer.byteLength);
            const frameType = view.getUint8(0);
            const frameLength = view.getUint32(1);
            if (this.buffer.length < frameLength + 5) {
              return;
            }

            const payload = this.buffer.slice(5, 5 + frameLength);
            this.buffer = this.buffer.slice(5 + frameLength);
            this.handleFrame(frameType, payload);
          }
        }

        handleFrame(frameType, payload) {
          if (frameType === FRAME_JSON) {
            handleControlMessage(JSON.parse(new TextDecoder().decode(payload)));
            return;
          }

          if (frameType === FRAME_INIT) {
            state.bytes += payload.byteLength;
            updateStats();
            elements.statusLine.textContent = "Initialization segment appended.";
            elements.detailLine.textContent = "Live fragments will continue on the same WebTransport session.";
            enqueueSegment(payload);
            logLine(`init segment (${payload.byteLength} bytes)`);
            return;
          }

          if (frameType === FRAME_FRAGMENT) {
            state.bytes += payload.byteLength;
            state.fragments += 1;
            updateStats();
            elements.statusLine.textContent = "Playing live fragments from WebTransport.";
            elements.detailLine.textContent = `Appended ${state.fragments} fragment(s) from the MOQ bridge.`;
            enqueueSegment(payload);
            return;
          }

          if (frameType === FRAME_END) {
            handleControlMessage({ type: "end" });
          }
        }
      }

      async function consumeReadableStream(readableStream) {
        const frameReader = new FrameReader();
        const reader = readableStream.getReader();
        try {
          while (true) {
            const { value, done } = await reader.read();
            if (done) {
              return;
            }
            frameReader.push(value);
          }
        } finally {
          reader.releaseLock();
        }
      }

      async function consumeIncomingStreams(transport) {
        const reader = transport.incomingUnidirectionalStreams.getReader();
        try {
          while (true) {
            const { value, done } = await reader.read();
            if (done) {
              return;
            }
            consumeReadableStream(value).catch((error) => {
              logLine(`incoming stream failed: ${error.message}`);
            });
          }
        } finally {
          reader.releaseLock();
        }
      }

      async function main() {
        if (!("WebTransport" in window)) {
          setConnectionState("unsupported", "#8e1f0d");
          elements.statusLine.textContent = "This browser does not expose WebTransport.";
          elements.detailLine.textContent = "Use a recent Chromium-based browser.";
          return;
        }

        const hostname = window.location.hostname || "127.0.0.1";
        const url = `https://${hostname}:${WT_PORT}${WT_PATH}`;
        const transport = new WebTransport(url, {
          serverCertificateHashes: [
            {
              algorithm: "sha-256",
              value: hexToUint8Array(CERT_HASH_HEX),
            },
          ],
        });

        state.transport = transport;
        setConnectionState("negotiating", "#b34622");
        logLine(`connecting to ${url}`);

        transport.closed
          .then(() => {
            setConnectionState("closed", "#5d524c");
            logLine("transport closed");
          })
          .catch((error) => {
            setConnectionState("error", "#8e1f0d");
            elements.statusLine.textContent = "WebTransport closed with an error.";
            elements.detailLine.textContent = error.message;
            logLine(`transport closed with error: ${error.message}`);
          });

        await transport.ready;
        setConnectionState("live", "#1f7a4b");
        elements.statusLine.textContent = "WebTransport session is ready.";
        elements.detailLine.textContent = "Waiting for subscriber metadata and init segment.";
        logLine("transport ready");
        await consumeIncomingStreams(transport);
      }

      main().catch((error) => {
        setConnectionState("failed", "#8e1f0d");
        elements.statusLine.textContent = "Failed to initialize the browser player.";
        elements.detailLine.textContent = error.message;
        logLine(`bootstrap failed: ${error.message}`);
      });
    </script>
  </body>
</html>
"""


def pack_frame(frame_type: int, payload: bytes = b"") -> bytes:
    """Serialize a binary frame for the browser WebTransport stream."""
    return bytes([frame_type]) + len(payload).to_bytes(4, "big") + payload


def build_player_page(cert_hash_hex: str, webtransport_port: int = WEBTRANSPORT_PORT) -> bytes:
    """Render the browser player HTML with runtime transport settings."""
    html = (
        HTML_TEMPLATE
        .replace("__WT_PORT__", str(webtransport_port))
        .replace("__WT_PATH__", WEBTRANSPORT_PATH)
        .replace("__CERT_HASH__", cert_hash_hex)
    )
    return html.encode("utf-8")


def generate_webtransport_certificate(host: str) -> tuple[str, str, str, tempfile.TemporaryDirectory]:
    """Generate a temporary self-signed certificate and return its SHA-256 digest."""
    temp_dir = tempfile.TemporaryDirectory(prefix="moq-webtransport-")
    cert_path = f"{temp_dir.name}/cert.pem"
    key_path = f"{temp_dir.name}/key.pem"

    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    subject = issuer = x509.Name([
        x509.NameAttribute(NameOID.COMMON_NAME, host),
    ])

    san_values = [x509.DNSName("localhost")]
    try:
        san_values.append(x509.IPAddress(ipaddress.ip_address(host)))
    except ValueError:
        san_values.append(x509.DNSName(host))

    cert = (
        x509.CertificateBuilder()
        .subject_name(subject)
        .issuer_name(issuer)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(datetime.utcnow() - timedelta(minutes=1))
        .not_valid_after(datetime.utcnow() + timedelta(days=7))
        .add_extension(x509.SubjectAlternativeName(san_values), critical=False)
        .sign(key, hashes.SHA256())
    )

    with open(cert_path, "wb") as cert_file:
        cert_file.write(cert.public_bytes(serialization.Encoding.PEM))

    with open(key_path, "wb") as key_file:
        key_file.write(
            key.private_bytes(
                encoding=serialization.Encoding.PEM,
                format=serialization.PrivateFormat.TraditionalOpenSSL,
                encryption_algorithm=serialization.NoEncryption(),
            )
        )

    cert_hash = hashlib.sha256(cert.public_bytes(serialization.Encoding.DER)).hexdigest()
    return cert_path, key_path, cert_hash, temp_dir


class BrowserBroadcastState:
    """Track metadata and replayable fragments for browser sessions."""

    def __init__(self):
        self.metadata: dict | None = None
        self.init_segment: bytes | None = None
        self.recent_fragments: deque[bytes] = deque(maxlen=MAX_REPLAY_FRAGMENTS)
        self.sessions: set["BrowserWebTransportSession"] = set()
        self.total_bytes = 0
        self.total_fragments = 0

    def add_session(self, session: "BrowserWebTransportSession"):
        self.sessions.add(session)
        if self.metadata is not None:
            session.send_json({"type": "metadata", "metadata": self.metadata})
        if self.init_segment is not None:
            session.send_binary(FRAME_TYPE_INIT, self.init_segment)
        for fragment in self.recent_fragments:
            session.send_binary(FRAME_TYPE_FRAGMENT, fragment)

    def remove_session(self, session: "BrowserWebTransportSession"):
        self.sessions.discard(session)

    def set_metadata(self, metadata: dict):
        self.metadata = metadata
        self.init_segment = None
        self.recent_fragments.clear()
        self.total_bytes = 0
        self.total_fragments = 0
        for session in tuple(self.sessions):
            session.send_json({"type": "metadata", "metadata": metadata})

    def set_init_segment(self, payload: bytes):
        self.init_segment = payload
        self.total_bytes += len(payload)
        for session in tuple(self.sessions):
            session.send_binary(FRAME_TYPE_INIT, payload)

    def push_fragment(self, payload: bytes):
        self.recent_fragments.append(payload)
        self.total_bytes += len(payload)
        self.total_fragments += 1
        for session in tuple(self.sessions):
            session.send_binary(FRAME_TYPE_FRAGMENT, payload)

    def end_stream(self):
        for session in tuple(self.sessions):
            session.send_json({"type": "end"})
            session.send_binary(FRAME_TYPE_END, b"")


class BrowserWebTransportSession:
    """One browser-facing WebTransport session."""

    def __init__(self, protocol: "BrowserBridgeProtocol", session_id: int):
        self.protocol = protocol
        self.session_id = session_id
        self.closed = False
        self.output_stream_id = protocol.http.create_webtransport_stream(
            session_id=session_id,
            is_unidirectional=True,
        )
        self.protocol.transmit()

    def send_binary(self, frame_type: int, payload: bytes):
        if self.closed:
            return
        self.protocol.http._quic.send_stream_data(
            self.output_stream_id,
            pack_frame(frame_type, payload),
            end_stream=False,
        )
        self.protocol.transmit()

    def send_json(self, payload: dict):
        self.send_binary(FRAME_TYPE_JSON, json.dumps(payload).encode("utf-8"))

    def close(self):
        if self.closed:
            return
        self.closed = True
        try:
            self.protocol.http._quic.send_stream_data(self.output_stream_id, b"", end_stream=True)
            self.protocol.transmit()
        except Exception:
            logger.debug("Failed to close browser output stream cleanly", exc_info=True)


class BrowserBridgeProtocol(QuicConnectionProtocol):
    """Accept browser WebTransport sessions and stream fMP4 fragments to them."""

    def __init__(self, *args, bridge: BrowserBroadcastState, allowed_origins: set[str], **kwargs):
        super().__init__(*args, **kwargs)
        self._bridge = bridge
        self._allowed_origins = allowed_origins
        self._http: H3Connection | None = None
        self._sessions: dict[int, BrowserWebTransportSession] = {}

    @property
    def http(self) -> H3Connection:
        assert self._http is not None
        return self._http

    def quic_event_received(self, event: QuicEvent) -> None:
        if isinstance(event, ProtocolNegotiated) and event.alpn_protocol in H3_ALPN:
            self._http = H3Connection(self._quic, enable_webtransport=True)
        elif isinstance(event, ConnectionTerminated):
            self._close_all_sessions()

        if self._http is None:
            return

        for http_event in self._http.handle_event(event):
            self._handle_http_event(http_event)

    def _handle_http_event(self, event: H3Event):
        if isinstance(event, HeadersReceived):
            self._handle_headers(event)
        elif isinstance(event, DatagramReceived):
            logger.debug("Ignoring WebTransport datagram on session %d", event.stream_id)
        elif isinstance(event, WebTransportStreamDataReceived):
            logger.debug(
                "Ignoring browser stream data: session=%d stream=%d bytes=%d",
                event.session_id,
                event.stream_id,
                len(event.data),
            )

    def _handle_headers(self, event: HeadersReceived):
        headers = {name: value for name, value in event.headers}
        method = headers.get(b":method", b"").decode("utf-8", errors="ignore")
        path = headers.get(b":path", b"").decode("utf-8", errors="ignore")
        protocol = headers.get(b":protocol", b"").decode("utf-8", errors="ignore")
        origin = headers.get(b"origin", b"").decode("utf-8", errors="ignore")

        if method != "CONNECT" or path != WEBTRANSPORT_PATH:
            self._reject_session(event.stream_id, 404)
            return

        if protocol not in {"webtransport", "webtransport-h3"}:
            logger.warning("Rejecting CONNECT with unsupported protocol: %s", protocol)
            self._reject_session(event.stream_id, 400)
            return

        if origin and origin not in self._allowed_origins:
            logger.warning("Rejecting browser origin %s", origin)
            self._reject_session(event.stream_id, 403)
            return

        self.http.send_headers(
            stream_id=event.stream_id,
            headers=[
                (b":status", b"200"),
                (b"server", b"moq-webtransport-bridge"),
                (b"date", formatdate(timeval=None, localtime=False, usegmt=True).encode("ascii")),
                (b"sec-webtransport-http3-draft", b"draft02"),
            ],
        )
        session = BrowserWebTransportSession(protocol=self, session_id=event.stream_id)
        self._sessions[event.stream_id] = session
        self._bridge.add_session(session)
        logger.info("Accepted browser WebTransport session %d from %s", event.stream_id, origin or "unknown-origin")

    def _reject_session(self, stream_id: int, status: int):
        self.http.send_headers(
            stream_id=stream_id,
            headers=[(b":status", str(status).encode("ascii"))],
            end_stream=True,
        )
        self.transmit()

    def _close_all_sessions(self):
        for session in tuple(self._sessions.values()):
            self._bridge.remove_session(session)
            session.close()
        self._sessions.clear()


class BrowserPageServer:
    """Serve the local browser page over plain HTTP on localhost."""

    def __init__(
        self,
        html: bytes,
        host: str = PAGE_HOST,
        port: int = PAGE_PORT,
        fallback_to_ephemeral: bool = True,
    ):
        self._html = html
        self._host = host
        self._preferred_port = port
        self._fallback_to_ephemeral = fallback_to_ephemeral
        self._server: asyncio.AbstractServer | None = None
        self._bound_port: int | None = None

    @property
    def port(self) -> int:
        if self._bound_port is None:
            raise RuntimeError("Browser page server has not started yet")
        return self._bound_port

    async def start(self):
        try:
            self._server = await asyncio.start_server(self._handle_client, self._host, self._preferred_port)
        except OSError as exc:
            if (
                exc.errno != errno.EADDRINUSE
                or not self._fallback_to_ephemeral
                or self._preferred_port == 0
            ):
                raise
            logger.warning(
                "Browser page port %d is already in use; falling back to an ephemeral localhost port",
                self._preferred_port,
            )
            self._server = await asyncio.start_server(self._handle_client, self._host, 0)

        sockets = self._server.sockets or []
        if not sockets:
            raise RuntimeError("Browser page server did not expose a listening socket")

        self._bound_port = int(sockets[0].getsockname()[1])
        logger.info("Browser page available at http://localhost:%d", self.port)

    async def stop(self):
        if self._server is None:
            return
        self._server.close()
        await self._server.wait_closed()
        self._server = None
        self._bound_port = None

    async def _handle_client(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        try:
            request = await reader.readuntil(b"\r\n\r\n")
        except (asyncio.IncompleteReadError, asyncio.LimitOverrunError):
            writer.close()
            await writer.wait_closed()
            return

        request_line = request.split(b"\r\n", 1)[0].decode("ascii", errors="ignore")
        parts = request_line.split(" ")
        method = parts[0] if len(parts) > 0 else ""
        path = parts[1] if len(parts) > 1 else "/"

        if method == "GET" and path in {"/", "/index.html"}:
            body = self._html
            writer.write(
                b"HTTP/1.1 200 OK\r\n"
                b"Content-Type: text/html; charset=utf-8\r\n"
                b"Cache-Control: no-store\r\n"
                b"Content-Length: " + str(len(body)).encode("ascii") + b"\r\n\r\n" + body
            )
        else:
            body = b"Not Found"
            writer.write(
                b"HTTP/1.1 404 Not Found\r\n"
                b"Content-Type: text/plain; charset=utf-8\r\n"
                b"Content-Length: " + str(len(body)).encode("ascii") + b"\r\n\r\n" + body
            )

        await writer.drain()
        writer.close()
        await writer.wait_closed()


async def main():
    """Subscribe to MOQ video and bridge it to a browser over WebTransport."""
    logger.info("Starting MOQ browser video subscriber")

    cert_path, key_path, cert_hash_hex, cert_dir = generate_webtransport_certificate(WEBTRANSPORT_HOST)
    bridge = BrowserBroadcastState()
    page_server = BrowserPageServer(build_player_page(cert_hash_hex, webtransport_port=WEBTRANSPORT_PORT))
    subscriber = MOQSubscriber(relay_host=RELAY_HOST, relay_port=RELAY_PORT)
    object_queue: asyncio.Queue[ReceivedObject] = asyncio.Queue()
    running = asyncio.Event()

    quic_config = QuicConfiguration(
        alpn_protocols=H3_ALPN,
        is_client=False,
        max_datagram_frame_size=65536,
    )
    quic_config.load_cert_chain(cert_path, key_path)

    async def process_received_objects():
        while True:
            obj = await object_queue.get()

            if obj.group_id != 1:
                logger.warning("Ignoring unexpected group: %d", obj.group_id)
                continue

            if obj.object_status == ObjectStatus.END_OF_SUBGROUP:
                logger.info("Received end of live subgroup")
                bridge.end_stream()
                continue

            if obj.object_id == 1:
                metadata = json.loads(obj.payload.decode("utf-8"))
                metadata.setdefault("mime_type", DEFAULT_MIME_TYPE)
                metadata.setdefault("mse_codec", "avc1.42E01F")
                bridge.set_metadata(metadata)
                logger.info(
                    "Received metadata: codec=%s resolution=%sx%s fps=%s mime=%s",
                    metadata.get("codec"),
                    metadata.get("width"),
                    metadata.get("height"),
                    metadata.get("fps"),
                    metadata.get("mime_type"),
                )
                continue

            if bridge.init_segment is None:
                bridge.set_init_segment(obj.payload)
                logger.info("Stored initialization segment as object=%d (%d bytes)", obj.object_id, len(obj.payload))
                continue

            bridge.push_fragment(obj.payload)
            logger.info(
                "Forwarded live fragment as object=%d (%d bytes, total_fragments=%d, viewers=%d)",
                obj.object_id,
                len(obj.payload),
                bridge.total_fragments,
                len(bridge.sessions),
            )

    def on_object_received(obj: ReceivedObject):
        object_queue.put_nowait(obj)

    subscriber.set_handlers(
        on_connected=lambda: logger.info("Subscriber connected to relay"),
        on_disconnected=lambda: logger.info("Subscriber disconnected from relay"),
        on_object_received=on_object_received,
        on_subscription_accepted=lambda track_name: logger.info("Subscription accepted: %s", track_name),
        on_subscription_rejected=lambda track_name, reason: logger.warning(
            "Subscription rejected: %s - %s", track_name, reason
        ),
    )

    if not await subscriber.connect():
        cert_dir.cleanup()
        logger.error("Failed to connect to relay")
        return

    processor_task = asyncio.create_task(process_received_objects())
    webtransport_server = None

    try:
        await page_server.start()
        allowed_origins = {
            f"http://localhost:{page_server.port}",
            f"http://127.0.0.1:{page_server.port}",
        }
        webtransport_server = await serve(
            WEBTRANSPORT_HOST,
            WEBTRANSPORT_PORT,
            configuration=quic_config,
            create_protocol=lambda *args, **kwargs: BrowserBridgeProtocol(
                *args,
                bridge=bridge,
                allowed_origins=allowed_origins,
                **kwargs,
            ),
        )
        logger.info(
            "WebTransport bridge listening at https://%s:%d%s (cert sha256=%s)",
            WEBTRANSPORT_HOST,
            WEBTRANSPORT_PORT,
            WEBTRANSPORT_PATH,
            cert_hash_hex,
        )

        await subscriber.subscribe(TRACK_NAME, start_group=1, start_object=1)
        logger.info("Subscribed to track: %s", TRACK_NAME)
        logger.info("Open http://localhost:%d to view the live stream", page_server.port)
        await running.wait()
    except KeyboardInterrupt:
        logger.info("Browser subscriber stopped by user")
    except Exception as exc:
        logger.error("Subscriber bridge error: %s", exc)
        raise
    finally:
        processor_task.cancel()
        try:
            await processor_task
        except asyncio.CancelledError:
            pass

        if webtransport_server is not None:
            webtransport_server.close()
            await asyncio.sleep(0)

        await page_server.stop()

        try:
            await subscriber.unsubscribe(TRACK_NAME)
        except Exception as exc:
            logger.error("Error during unsubscribe: %s", exc)
        subscriber.disconnect()
        cert_dir.cleanup()


if __name__ == "__main__":
    asyncio.run(main())
