#!/usr/bin/env python3
"""
Run a local Rust MOQ relay and browser viewer.

Supported playback paths:
- Python MOQSubscriber -> raw H.264 frames -> WebCodecs.
- Rust moq-cli subscribe -> fMP4 chunks -> Media Source Extensions.
- Rust moq-cli subscribe -> AVC3/H.264 frames -> WebCodecs.
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import json
import logging
import os
import signal
import socket
import statistics
import struct
import sys
import tempfile
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

import uvicorn
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import NameOID
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse, JSONResponse

MOQ_RUST_ROOT = Path(__file__).resolve().parents[1]
REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT))
sys.path.insert(0, str(MOQ_RUST_ROOT))
sys.path.insert(0, str(Path(__file__).resolve().parent))

try:
    from moq.sub import MOQSubscriber
except ImportError:
    MOQSubscriber = None
from moq_live_video_common import (
    DEFAULT_META_NAME,
    DEFAULT_NAMESPACE,
    DEFAULT_VIDEO_NAME,
    build_track,
)
from moq_official_relay import RustMOQRelay


logger = logging.getLogger('moq-live-video-viewer')

AVC3_FRAME_MAGIC = b'MAVC'
AVC3_TIMED_FRAME_MAGIC = b'MAVT'
AVC3_LEGACY_FRAME_HEADER = struct.Struct('!QBI')
AVC3_TIMED_FRAME_HEADER = struct.Struct('!QBQI')


VIEWER_HTML = r"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>MOQ Live Video</title>
  <style>
    :root {
      color-scheme: dark;
      font-family: Inter, ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
      background: #101113;
      color: #f2f4f8;
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      min-height: 100vh;
      background: #101113;
    }
    .shell {
      display: grid;
      grid-template-columns: minmax(0, 1fr) 320px;
      gap: 16px;
      min-height: 100vh;
      padding: 16px;
    }
    .stage {
      min-width: 0;
      display: grid;
      grid-template-rows: minmax(0, 1fr) auto;
      gap: 12px;
    }
    .video-wrap {
      position: relative;
      min-height: 360px;
      background: #050607;
      border: 1px solid #2a2e35;
      overflow: hidden;
    }
    canvas, video {
      width: 100%;
      height: 100%;
      display: block;
      object-fit: contain;
    }
    video {
      background: #050607;
    }
    [hidden] {
      display: none !important;
    }
    .overlay {
      position: absolute;
      left: 14px;
      top: 12px;
      display: flex;
      gap: 8px;
      align-items: center;
      color: #fff;
      text-shadow: 0 1px 2px #000;
      font-size: 13px;
    }
    .dot {
      width: 9px;
      height: 9px;
      border-radius: 999px;
      background: #e05555;
    }
    .dot.live { background: #45c26b; }
    .clock {
      font-variant-numeric: tabular-nums;
      font-size: 48px;
      line-height: 1;
      letter-spacing: 0;
    }
    .panel {
      border-left: 1px solid #2a2e35;
      padding-left: 16px;
      display: grid;
      align-content: start;
      gap: 14px;
    }
    h1 {
      margin: 0;
      font-size: 18px;
      font-weight: 650;
    }
    .grid {
      display: grid;
      grid-template-columns: 1fr 1fr;
      gap: 8px;
    }
    .metric {
      border: 1px solid #2a2e35;
      background: #17191d;
      border-radius: 8px;
      padding: 10px;
      min-width: 0;
    }
    .label {
      color: #9da7b6;
      font-size: 11px;
      line-height: 1.2;
      margin-bottom: 5px;
    }
    .value {
      font-variant-numeric: tabular-nums;
      font-size: 20px;
      line-height: 1.1;
      white-space: nowrap;
    }
    .wide { grid-column: 1 / -1; }
    .status {
      color: #c5cedb;
      font-size: 13px;
      line-height: 1.4;
      min-height: 18px;
    }
    @media (max-width: 860px) {
      .shell {
        grid-template-columns: 1fr;
      }
      .panel {
        border-left: 0;
        padding-left: 0;
      }
      .clock { font-size: 34px; }
    }
  </style>
</head>
<body>
  <main class="shell">
    <section class="stage">
      <div class="video-wrap">
        <canvas id="canvas"></canvas>
        <video id="mseVideo" autoplay muted playsinline controls hidden></video>
        <div class="overlay"><span id="dot" class="dot"></span><span id="state">connecting</span></div>
      </div>
      <div class="clock" id="clock">--:--:--.---</div>
    </section>
    <aside class="panel">
      <h1>MOQ Live Video</h1>
      <div class="status" id="status">Waiting for stream...</div>
      <div class="grid">
        <div class="metric"><div class="label">FPS 60s</div><div class="value" id="fps">0.0</div></div>
        <div class="metric"><div class="label">FPS 5s</div><div class="value" id="fps5s">0.0</div></div>
        <div class="metric"><div class="label">Bitrate 60s</div><div class="value" id="bitrate">0.00 Mbps</div></div>
        <div class="metric"><div class="label">Frames</div><div class="value" id="frames">0</div></div>
        <div class="metric"><div class="label">Dropped</div><div class="value" id="dropped">0</div></div>
        <div class="metric"><div class="label">Latency P50</div><div class="value" id="latP50">n/a</div></div>
        <div class="metric"><div class="label">Latency P95</div><div class="value" id="latP95">n/a</div></div>
        <div class="metric"><div class="label">Latest Latency</div><div class="value" id="latNow">n/a</div></div>
        <div class="metric"><div class="label">Jitter P95</div><div class="value" id="jitP95">n/a</div></div>
        <div class="metric wide"><div class="label">Decoder</div><div class="value" id="decoder">idle</div></div>
      </div>
    </aside>
  </main>
  <script>
    const canvas = document.getElementById('canvas');
    const ctx = canvas.getContext('2d');
    const mseVideo = document.getElementById('mseVideo');
    const dot = document.getElementById('dot');
    const state = document.getElementById('state');
    const statusEl = document.getElementById('status');
    const decoderEl = document.getElementById('decoder');
    const metas = new Map();
    const latencies = [];
    const jitters = [];
    const frameArrival = [];
    const recentSamples = [];
    let decoder = null;
    let configured = false;
    let frames = 0;
    let bytes = 0;
    let dropped = 0;
    let lastFrameId = null;
    let firstSampleAt = null;
    let mediaMode = null;
    let mediaSource = null;
    let sourceBuffer = null;
    let appendQueue = [];
    let fmp4Chunks = 0;
    let fmp4Mime = 'video/mp4; codecs="avc1.42E01F"';

    function setText(id, value) {
      document.getElementById(id).textContent = value;
    }

    function percentile(values, p) {
      if (!values.length) return null;
      const sorted = [...values].sort((a, b) => a - b);
      const index = Math.min(sorted.length - 1, Math.round((p / 100) * (sorted.length - 1)));
      return sorted[index];
    }

    function fmtMs(value) {
      return value == null ? 'n/a' : `${value.toFixed(1)} ms`;
    }

    function recordRecentSample(byteLength) {
      const now = performance.now();
      if (firstSampleAt === null) firstSampleAt = now;
      recentSamples.push({ at: now, bytes: byteLength });
      const cutoff = now - 60000;
      while (recentSamples.length && recentSamples[0].at < cutoff) {
        recentSamples.shift();
      }
    }

    function windowRates(windowMs) {
      const now = performance.now();
      const cutoff = now - windowMs;
      const samples = recentSamples.filter((sample) => sample.at >= cutoff);
      if (!samples.length) {
        return { fps: 0, mbps: 0, seconds: 0 };
      }
      const windowSeconds = Math.max((now - samples[0].at) / 1000, 0.001);
      const windowBytes = samples.reduce((total, sample) => total + sample.bytes, 0);
      return {
        fps: samples.length / windowSeconds,
        mbps: windowBytes * 8 / windowSeconds / 1000000,
        seconds: windowSeconds,
      };
    }

    function recentRates() {
      const now = performance.now();
      const cutoff = now - 60000;
      while (recentSamples.length && recentSamples[0].at < cutoff) {
        recentSamples.shift();
      }
      const rates60s = windowRates(60000);
      const rates5s = windowRates(5000);
      return {
        fps: rates60s.fps,
        fps5s: rates5s.fps,
        mbps: rates60s.mbps,
      };
    }

    function updateClock() {
      const now = new Date();
      const h = String(now.getHours()).padStart(2, '0');
      const m = String(now.getMinutes()).padStart(2, '0');
      const s = String(now.getSeconds()).padStart(2, '0');
      const ms = String(now.getMilliseconds()).padStart(3, '0');
      document.getElementById('clock').textContent = `${h}:${m}:${s}.${ms}`;
      requestAnimationFrame(updateClock);
    }

    function containsIdr(data) {
      for (let i = 0; i < data.length - 4; i++) {
        let prefix = 0;
        if (data[i] === 0 && data[i + 1] === 0 && data[i + 2] === 1) prefix = 3;
        if (data[i] === 0 && data[i + 1] === 0 && data[i + 2] === 0 && data[i + 3] === 1) prefix = 4;
        if (prefix) {
          const nal = data[i + prefix] & 31;
          if (nal === 5) return true;
          i += prefix;
        }
      }
      return false;
    }

    async function configure(meta) {
      if (configured) return;
      if (!('VideoDecoder' in window)) {
        throw new Error('WebCodecs VideoDecoder is not available');
      }
      decoder = new VideoDecoder({
        output(frame) {
          if (canvas.width !== frame.displayWidth || canvas.height !== frame.displayHeight) {
            canvas.width = frame.displayWidth;
            canvas.height = frame.displayHeight;
          }
          ctx.drawImage(frame, 0, 0, canvas.width, canvas.height);
          frame.close();
        },
        error(error) {
          decoderEl.textContent = error.message || String(error);
        },
      });
      const config = {
        codec: meta.codec || 'avc1.42E01F',
        codedWidth: meta.width || 1280,
        codedHeight: meta.height || 720,
        optimizeForLatency: true,
        hardwareAcceleration: 'prefer-hardware',
        avc: { format: 'annexb' },
      };
      try {
        decoder.configure(config);
      } catch (_error) {
        delete config.avc;
        decoder.configure(config);
      }
      configured = true;
      decoderEl.textContent = `${config.codec} ${config.codedWidth}x${config.codedHeight}`;
    }

    function updateMetrics() {
      const rates = recentRates();
      setText('fps', rates.fps.toFixed(1));
      setText('fps5s', rates.fps5s.toFixed(1));
      setText('bitrate', `${rates.mbps.toFixed(2)} Mbps`);
      setText('frames', mediaMode === 'fmp4' ? String(fmp4Chunks) : String(frames));
      setText('dropped', String(dropped));
      setText('latNow', fmtMs(latencies.at(-1)));
      setText('latP50', fmtMs(percentile(latencies, 50)));
      setText('latP95', fmtMs(percentile(latencies, 95)));
      setText('jitP95', fmtMs(percentile(jitters, 95)));
      chaseLiveEdge();
    }

    function setMediaMode(mode, options = {}) {
      if (mediaMode === mode) return;
      if (mediaMode && mediaMode !== mode) {
        throw new Error(`media mode changed from ${mediaMode} to ${mode}`);
      }
      mediaMode = mode;
      if (mode === 'fmp4') {
        canvas.hidden = true;
        mseVideo.hidden = false;
        setupMse(options.mime || fmp4Mime);
        return;
      }
      canvas.hidden = false;
      mseVideo.hidden = true;
      decoderEl.textContent = 'WebCodecs raw H.264';
    }

    function setupMse(mime) {
      fmp4Mime = mime;
      if (!('MediaSource' in window)) {
        throw new Error('MediaSource is not available');
      }
      if (typeof MediaSource.isTypeSupported === 'function' && !MediaSource.isTypeSupported(mime)) {
        statusEl.textContent = `MSE MIME may be unsupported: ${mime}`;
      }
      mediaSource = new MediaSource();
      mseVideo.src = URL.createObjectURL(mediaSource);
      mseVideo.muted = true;
      mseVideo.play().catch(() => {});
      mediaSource.addEventListener('sourceopen', () => {
        if (sourceBuffer) return;
        sourceBuffer = mediaSource.addSourceBuffer(mime);
        sourceBuffer.mode = 'segments';
        sourceBuffer.addEventListener('updateend', appendNextFmp4);
        decoderEl.textContent = `MSE ${mime}`;
        appendNextFmp4();
      });
    }

    function appendNextFmp4() {
      if (!sourceBuffer || sourceBuffer.updating || appendQueue.length === 0) return;
      try {
        sourceBuffer.appendBuffer(appendQueue.shift());
      } catch (error) {
        statusEl.textContent = error.message || String(error);
        appendQueue = [];
      }
    }

    function chaseLiveEdge() {
      if (mediaMode !== 'fmp4' || mseVideo.buffered.length === 0) return;
      const end = mseVideo.buffered.end(mseVideo.buffered.length - 1);
      const lag = end - mseVideo.currentTime;
      if (lag > 0.8) {
        mseVideo.currentTime = Math.max(0, end - 0.2);
      }
      mseVideo.playbackRate = lag > 0.35 ? 1.05 : 1.0;
    }

    function handleMeta(meta) {
      metas.set(meta.frame_id, meta);
      if (metas.size > 240) {
        const oldest = Math.min(...metas.keys());
        metas.delete(oldest);
      }
    }

    async function handleVideo(buffer) {
      setMediaMode('h264');
      const view = new DataView(buffer);
      const frameId = view.getUint32(0, false);
      const data = new Uint8Array(buffer, 4);
      const meta = metas.get(frameId) || {};
      await configure(meta);
      const now = Date.now();
      if (meta.sent_epoch_ms) {
        latencies.push(now - meta.sent_epoch_ms);
        if (latencies.length > 300) latencies.shift();
      }
      if (lastFrameId !== null && frameId > lastFrameId + 1) {
        dropped += frameId - lastFrameId - 1;
      }
      lastFrameId = frameId;
      if (frameArrival.length) {
        const interval = now - frameArrival.at(-1);
        const expected = 1000 / (meta.fps || 30);
        jitters.push(Math.abs(interval - expected));
        if (jitters.length > 300) jitters.shift();
      }
      frameArrival.push(now);
      if (frameArrival.length > 300) frameArrival.shift();
      frames += 1;
      bytes += data.byteLength;
      recordRecentSample(data.byteLength);
      const chunk = new EncodedVideoChunk({
        type: meta.keyframe || containsIdr(data) ? 'key' : 'delta',
        timestamp: meta.timestamp_us || Math.round(frameId * 1000000 / (meta.fps || 30)),
        data,
      });
      decoder.decode(chunk);
      updateMetrics();
    }

    function handleFmp4(buffer) {
      setMediaMode('fmp4');
      const chunk = new Uint8Array(buffer);
      bytes += chunk.byteLength;
      recordRecentSample(chunk.byteLength);
      fmp4Chunks += 1;
      frames = fmp4Chunks;
      appendQueue.push(chunk);
      if (appendQueue.length > 120) {
        appendQueue = appendQueue.slice(-30);
      }
      appendNextFmp4();
      updateMetrics();
    }

    function connect() {
      const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
      const ws = new WebSocket(`${protocol}//${window.location.host}/ws`);
      ws.binaryType = 'arraybuffer';
      ws.onopen = () => {
        dot.classList.add('live');
        state.textContent = 'live';
        statusEl.textContent = 'Connected';
      };
      ws.onclose = () => {
        dot.classList.remove('live');
        state.textContent = 'reconnecting';
        statusEl.textContent = 'Reconnecting...';
        setTimeout(connect, 1000);
      };
      ws.onerror = () => {
        statusEl.textContent = 'WebSocket error';
      };
      ws.onmessage = async (event) => {
        try {
          if (typeof event.data === 'string') {
            const message = JSON.parse(event.data);
            if (message.media_mode) setMediaMode(message.media_mode, message);
            if (message.type === 'frame_meta') handleMeta(message);
            if (message.type === 'status') statusEl.textContent = message.message;
            return;
          }
          if (mediaMode === 'fmp4') {
            handleFmp4(event.data);
          } else {
            await handleVideo(event.data);
          }
        } catch (error) {
          statusEl.textContent = error.message || String(error);
        }
      };
    }

    updateClock();
    connect();
    setInterval(updateMetrics, 1000);
  </script>
</body>
</html>
"""


@dataclass
class RuntimeStats:
    media_mode: str = 'h264'
    video_frames: int = 0
    meta_frames: int = 0
    video_bytes: int = 0
    fmp4_chunks: int = 0
    dropped_frames: int = 0
    last_frame_id: Optional[int] = None
    started_at: float = field(default_factory=time.monotonic)
    relay_rss_bytes: int = 0
    subscriber_state: str = 'starting'
    latencies_ms: list[float] = field(default_factory=list)
    recent_samples: list[tuple[float, int]] = field(default_factory=list)

    def on_meta(self, meta: dict) -> None:
        self.meta_frames += 1
        sent = meta.get('sent_epoch_ms')
        if sent is not None:
            self.latencies_ms.append(time.time() * 1000 - float(sent))
            if len(self.latencies_ms) > 300:
                self.latencies_ms.pop(0)

    def on_video(self, frame_id: int, payload_bytes: int) -> None:
        self.video_frames += 1
        self.video_bytes += payload_bytes
        self._record_recent_sample(payload_bytes)
        if self.last_frame_id is not None and frame_id > self.last_frame_id + 1:
            self.dropped_frames += frame_id - self.last_frame_id - 1
        self.last_frame_id = frame_id

    def on_fmp4_chunk(self, payload_bytes: int) -> None:
        self.fmp4_chunks += 1
        self.video_frames = self.fmp4_chunks
        self.video_bytes += payload_bytes
        self._record_recent_sample(payload_bytes)

    def _record_recent_sample(self, payload_bytes: int) -> None:
        now = time.monotonic()
        self.recent_samples.append((now, payload_bytes))
        self._prune_recent_samples(now)

    def _prune_recent_samples(self, now: Optional[float] = None) -> None:
        now = time.monotonic() if now is None else now
        cutoff = now - 60
        while self.recent_samples and self.recent_samples[0][0] < cutoff:
            self.recent_samples.pop(0)

    def _recent_rates(self, window_seconds: float) -> tuple[float, float, float]:
        now = time.monotonic()
        self._prune_recent_samples(now)
        cutoff = now - window_seconds
        samples = [
            (sample_at, payload_bytes)
            for sample_at, payload_bytes in self.recent_samples
            if sample_at >= cutoff
        ]
        if not samples:
            return 0.0, 0.0, 0.0
        observed_seconds = max(now - samples[0][0], 0.001)
        window_bytes = sum(payload_bytes for _, payload_bytes in samples)
        return (
            len(samples) / observed_seconds,
            window_bytes * 8 / observed_seconds / 1_000_000,
            observed_seconds,
        )

    def snapshot(self) -> dict:
        elapsed = max(time.monotonic() - self.started_at, 0.001)
        fps_60s, mbps_60s, window_60s = self._recent_rates(60)
        fps_5s, mbps_5s, window_5s = self._recent_rates(5)
        return {
            'media_mode': self.media_mode,
            'subscriber_state': self.subscriber_state,
            'video_frames': self.video_frames,
            'meta_frames': self.meta_frames,
            'fmp4_chunks': self.fmp4_chunks,
            'dropped_frames': self.dropped_frames,
            'fps': fps_60s,
            'mbps': mbps_60s,
            'fps_5s': fps_5s,
            'mbps_5s': mbps_5s,
            'fps_total': self.video_frames / elapsed,
            'mbps_total': self.video_bytes * 8 / elapsed / 1_000_000,
            'stats_window_seconds': window_60s,
            'stats_window_5s_seconds': window_5s,
            'relay_rss_bytes': self.relay_rss_bytes,
            'latency_ms': {
                'avg': (
                    statistics.fmean(self.latencies_ms)
                    if self.latencies_ms else None
                ),
                'p50': percentile(self.latencies_ms, 50),
                'p95': percentile(self.latencies_ms, 95),
            },
        }


class ClientManager:
    def __init__(self) -> None:
        self.clients: set[WebSocket] = set()

    async def add(self, websocket: WebSocket) -> None:
        await websocket.accept()
        self.clients.add(websocket)

    def remove(self, websocket: WebSocket) -> None:
        self.clients.discard(websocket)

    async def broadcast_text(self, payload: str) -> None:
        await self._broadcast(payload, binary=False)

    async def broadcast_bytes(self, payload: bytes) -> None:
        await self._broadcast(payload, binary=True)

    async def _broadcast(self, payload, *, binary: bool) -> None:
        stale = []
        for client in list(self.clients):
            try:
                if binary:
                    await client.send_bytes(payload)
                else:
                    await client.send_text(payload)
            except Exception:
                stale.append(client)
        for client in stale:
            self.remove(client)


def percentile(values: list[float], percentile_value: float) -> Optional[float]:
    if not values:
        return None
    ordered = sorted(values)
    index = min(
        len(ordered) - 1,
        round((percentile_value / 100) * (len(ordered) - 1)),
    )
    return ordered[index]


def rss_bytes(pid: Optional[int]) -> int:
    if not pid:
        return 0
    try:
        with open(f'/proc/{pid}/status', 'r', encoding='utf-8') as status:
            for line in status:
                if line.startswith('VmRSS:'):
                    return int(line.split()[1]) * 1024
    except OSError:
        return 0
    return 0


def assert_port_free(host: str, port: int, socket_type: int, label: str) -> None:
    family = socket.AF_INET6 if ':' in host and host != '0.0.0.0' else socket.AF_INET
    bind_host = host if host not in ('', '0.0.0.0') else '0.0.0.0'
    with socket.socket(family, socket_type) as sock:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            sock.bind((bind_host, port))
        except OSError as exc:
            raise RuntimeError(f'{label} is not free on {host}:{port}: {exc}') from exc


def check_required_ports(args) -> None:
    assert_port_free(args.relay_host, args.relay_port, socket.SOCK_DGRAM, 'relay UDP port')
    assert_port_free(args.relay_host, args.relay_port, socket.SOCK_STREAM, 'relay TCP port')
    assert_port_free(args.web_host, args.web_port, socket.SOCK_STREAM, 'viewer TCP port')


def prepare_tls(args, runtime_cache: Path) -> tuple[Optional[str], Optional[str]]:
    if args.no_https:
        return None, None
    if args.tls_certfile and args.tls_keyfile:
        return args.tls_certfile, args.tls_keyfile
    cert_dir = runtime_cache / 'tls'
    cert_dir.mkdir(parents=True, exist_ok=True)
    cert_path = cert_dir / 'viewer-cert.pem'
    key_path = cert_dir / 'viewer-key.pem'
    if cert_path.exists() and key_path.exists():
        return str(cert_path), str(key_path)

    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    subject = issuer = x509.Name([
        x509.NameAttribute(NameOID.COMMON_NAME, 'MOQ Live Video Viewer'),
    ])
    names = [
        x509.DNSName('localhost'),
        x509.IPAddress(__import__('ipaddress').ip_address('127.0.0.1')),
    ]
    if args.cert_host:
        with contextlib.suppress(ValueError):
            names.append(x509.IPAddress(__import__('ipaddress').ip_address(args.cert_host)))
        if args.cert_host != '127.0.0.1':
            names.append(x509.DNSName(args.cert_host))

    cert = (
        x509.CertificateBuilder()
        .subject_name(subject)
        .issuer_name(issuer)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(datetime.now(timezone.utc) - timedelta(minutes=1))
        .not_valid_after(datetime.now(timezone.utc) + timedelta(days=30))
        .add_extension(x509.SubjectAlternativeName(names), critical=False)
        .sign(key, hashes.SHA256())
    )
    key_path.write_bytes(
        key.private_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PrivateFormat.TraditionalOpenSSL,
            encryption_algorithm=serialization.NoEncryption(),
        )
    )
    cert_path.write_bytes(cert.public_bytes(serialization.Encoding.PEM))
    return str(cert_path), str(key_path)


def make_app(manager: ClientManager, stats: RuntimeStats, args) -> FastAPI:
    app = FastAPI()

    @app.get('/')
    async def index():
        return HTMLResponse(VIEWER_HTML)

    @app.get('/api/status')
    async def status():
        return JSONResponse(stats.snapshot())

    @app.websocket('/ws')
    async def websocket_endpoint(websocket: WebSocket):
        await manager.add(websocket)
        try:
            await websocket.send_text(json.dumps({
                'type': 'status',
                'message': stats.subscriber_state,
                'media_mode': stats.media_mode,
                'mime': args.fmp4_mime,
            }))
            while True:
                await websocket.receive_text()
        except WebSocketDisconnect:
            pass
        finally:
            manager.remove(websocket)

    return app


def put_drop_oldest(queue: asyncio.Queue, item) -> None:
    if queue.full():
        with contextlib.suppress(asyncio.QueueEmpty):
            queue.get_nowait()
    queue.put_nowait(item)


async def subscriber_loop(args, output_queue: asyncio.Queue, stats: RuntimeStats) -> None:
    if MOQSubscriber is None:
        raise RuntimeError(
            'Python MOQSubscriber is unavailable. Use --subscriber rust-avc3 '
            'or run from an Agent GW checkout that includes the Python moq package.'
        )
    video_track = build_track(args.namespace, args.video_name)
    meta_track = build_track(args.namespace, args.meta_name)

    while True:
        subscriber = MOQSubscriber('127.0.0.1', args.relay_port, delivery_timeout=1.0)
        accepted = set()
        rejected = asyncio.Event()
        ready = asyncio.Event()

        def on_subscription_accepted(track):
            accepted.add(track)
            if video_track in accepted and meta_track in accepted:
                ready.set()

        def on_subscription_rejected(_track, reason):
            logger.info('subscription rejected: %s', reason)
            rejected.set()

        def on_object_received(obj):
            track = subscriber._track_aliases.get(obj.track_alias)
            if track == video_track:
                put_drop_oldest(output_queue, ('video', obj.object_id, bytes(obj.payload)))
            elif track == meta_track:
                put_drop_oldest(output_queue, ('meta', obj.object_id, bytes(obj.payload)))

        subscriber.set_handlers(
            on_object_received=on_object_received,
            on_subscription_accepted=on_subscription_accepted,
            on_subscription_rejected=on_subscription_rejected,
        )

        try:
            stats.subscriber_state = 'connecting to relay'
            if not await asyncio.wait_for(subscriber.connect(), timeout=8):
                raise RuntimeError('subscriber failed to connect')

            stats.subscriber_state = 'waiting for publisher'
            await subscriber.subscribe(video_track, start_group=0, start_object=0)
            await subscriber.subscribe(meta_track, start_group=0, start_object=0)

            wait_tasks = [
                asyncio.create_task(ready.wait()),
                asyncio.create_task(rejected.wait()),
            ]
            done, pending = await asyncio.wait(
                wait_tasks,
                timeout=args.subscribe_timeout,
                return_when=asyncio.FIRST_COMPLETED,
            )
            for task in pending:
                task.cancel()
            for task in pending:
                with contextlib.suppress(asyncio.CancelledError):
                    await task
            if not done or rejected.is_set():
                raise RuntimeError('tracks are not published yet')

            stats.subscriber_state = 'subscribed'
            await asyncio.Future()
        except asyncio.CancelledError:
            subscriber.disconnect()
            raise
        except Exception as exc:
            stats.subscriber_state = f'retrying subscription: {exc}'
            logger.info('subscriber retry: %s', exc)
            subscriber.disconnect()
            await asyncio.sleep(args.retry_interval)


async def broadcast_loop(
    output_queue: asyncio.Queue,
    manager: ClientManager,
    stats: RuntimeStats,
    args,
) -> None:
    while True:
        kind, frame_id, payload = await output_queue.get()
        if kind == 'fmp4':
            stats.on_fmp4_chunk(len(payload))
            await manager.broadcast_bytes(payload)
            continue

        if kind == 'meta':
            try:
                meta = json.loads(payload.decode('utf-8'))
            except Exception:
                continue
            meta['subscriber_received_epoch_ms'] = time.time() * 1000
            meta['subscriber_received_monotonic_ns'] = time.monotonic_ns()
            stats.on_meta(meta)
            await manager.broadcast_text(json.dumps(meta, separators=(',', ':')))
            continue

        stats.on_video(frame_id, len(payload))
        await manager.broadcast_bytes(struct.pack('!I', frame_id) + payload)


async def drain_process_stderr(process: asyncio.subprocess.Process, label: str) -> None:
    if process.stderr is None:
        return
    while True:
        line = await process.stderr.readline()
        if not line:
            return
        logger.info('%s: %s', label, line.decode('utf-8', errors='replace').rstrip())


async def rust_fmp4_subscriber_loop(
    args,
    output_queue: asyncio.Queue,
    stats: RuntimeStats,
) -> None:
    url = args.moq_url or f'http://127.0.0.1:{args.relay_port}/'
    command = [
        args.moq_cli_bin,
        '--log-level',
        args.moq_cli_log_level,
        '--iroh-enabled=false',
        'subscribe',
        '--url',
        url,
        '--name',
        args.broadcast_name,
        '--output',
        'fmp4',
        '--max-latency',
        str(args.max_latency),
    ]

    while True:
        stats.subscriber_state = 'starting moq-cli subscribe'
        logger.info('starting rust fMP4 subscriber: %s', ' '.join(command))
        process = await asyncio.create_subprocess_exec(
            *command,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stderr_task = asyncio.create_task(drain_process_stderr(process, 'moq-cli'))
        try:
            if process.stdout is None:
                raise RuntimeError('moq-cli stdout pipe was not created')
            stats.subscriber_state = 'rust fMP4 subscriber running'
            while True:
                chunk = await process.stdout.read(args.fmp4_read_size)
                if not chunk:
                    break
                put_drop_oldest(output_queue, ('fmp4', None, bytes(chunk)))

            return_code = await process.wait()
            raise RuntimeError(f'moq-cli subscribe exited rc={return_code}')
        except asyncio.CancelledError:
            if process.returncode is None:
                process.terminate()
                with contextlib.suppress(asyncio.TimeoutError):
                    await asyncio.wait_for(process.wait(), timeout=3)
                if process.returncode is None:
                    process.kill()
                    await process.wait()
            raise
        except Exception as exc:
            stats.subscriber_state = f'restarting rust subscriber: {exc}'
            logger.info('rust fMP4 subscriber retry: %s', exc)
            if process.returncode is None:
                process.terminate()
                with contextlib.suppress(asyncio.TimeoutError):
                    await asyncio.wait_for(process.wait(), timeout=3)
                if process.returncode is None:
                    process.kill()
                    await process.wait()
            await asyncio.sleep(args.retry_interval)
        finally:
            stderr_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await stderr_task


async def rust_avc3_subscriber_loop(
    args,
    output_queue: asyncio.Queue,
    stats: RuntimeStats,
) -> None:
    url = args.moq_url or f'http://127.0.0.1:{args.relay_port}/'
    command = [
        args.moq_cli_bin,
        '--log-level',
        args.moq_cli_log_level,
        '--iroh-enabled=false',
        'subscribe',
        '--url',
        url,
        '--name',
        args.broadcast_name,
        '--output',
        'avc3',
        '--max-latency',
        str(args.max_latency),
    ]

    frame_id = 0
    while True:
        stats.subscriber_state = 'starting moq-cli AVC3 subscribe'
        logger.info('starting rust AVC3 subscriber: %s', ' '.join(command))
        process = await asyncio.create_subprocess_exec(
            *command,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stderr_task = asyncio.create_task(drain_process_stderr(process, 'moq-cli'))
        try:
            if process.stdout is None:
                raise RuntimeError('moq-cli stdout pipe was not created')
            stats.subscriber_state = 'rust AVC3 subscriber running'
            while True:
                magic = await process.stdout.readexactly(4)
                if magic == AVC3_TIMED_FRAME_MAGIC:
                    header = await process.stdout.readexactly(AVC3_TIMED_FRAME_HEADER.size)
                    timestamp_us, keyframe, sent_epoch_ms, payload_len = (
                        AVC3_TIMED_FRAME_HEADER.unpack(header)
                    )
                elif magic == AVC3_FRAME_MAGIC:
                    header = await process.stdout.readexactly(AVC3_LEGACY_FRAME_HEADER.size)
                    timestamp_us, keyframe, payload_len = (
                        AVC3_LEGACY_FRAME_HEADER.unpack(header)
                    )
                    sent_epoch_ms = 0
                else:
                    raise RuntimeError(f'invalid AVC3 frame magic: {magic!r}')
                if payload_len <= 0 or payload_len > args.avc3_max_frame_bytes:
                    raise RuntimeError(f'invalid AVC3 frame length: {payload_len}')
                payload = await process.stdout.readexactly(payload_len)
                meta = {
                    'type': 'frame_meta',
                    'frame_id': frame_id,
                    'timestamp_us': timestamp_us,
                    'keyframe': bool(keyframe),
                    'width': args.h264_width,
                    'height': args.h264_height,
                    'fps': args.h264_fps,
                    'codec': args.h264_codec,
                    'subscriber_received_epoch_ms': time.time() * 1000,
                    'subscriber_received_monotonic_ns': time.monotonic_ns(),
                }
                if sent_epoch_ms > 0:
                    meta['sent_epoch_ms'] = sent_epoch_ms
                put_drop_oldest(
                    output_queue,
                    ('meta', frame_id, json.dumps(meta, separators=(',', ':')).encode('utf-8')),
                )
                put_drop_oldest(output_queue, ('video', frame_id, payload))
                frame_id += 1

            return_code = await process.wait()
            raise RuntimeError(f'moq-cli subscribe exited rc={return_code}')
        except asyncio.IncompleteReadError as exc:
            return_code = await process.wait()
            stats.subscriber_state = (
                f'restarting rust AVC3 subscriber: stream ended rc={return_code}'
            )
            logger.info('rust AVC3 subscriber stream ended rc=%s: %s', return_code, exc)
            await asyncio.sleep(args.retry_interval)
        except asyncio.CancelledError:
            if process.returncode is None:
                process.terminate()
                with contextlib.suppress(asyncio.TimeoutError):
                    await asyncio.wait_for(process.wait(), timeout=3)
                if process.returncode is None:
                    process.kill()
                    await process.wait()
            raise
        except Exception as exc:
            stats.subscriber_state = f'restarting rust AVC3 subscriber: {exc}'
            logger.info('rust AVC3 subscriber retry: %s', exc)
            if process.returncode is None:
                process.terminate()
                with contextlib.suppress(asyncio.TimeoutError):
                    await asyncio.wait_for(process.wait(), timeout=3)
                if process.returncode is None:
                    process.kill()
                    await process.wait()
            await asyncio.sleep(args.retry_interval)
        finally:
            stderr_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await stderr_task


async def relay_rss_loop(relay: RustMOQRelay, stats: RuntimeStats) -> None:
    while True:
        process = relay._process
        stats.relay_rss_bytes = rss_bytes(process.pid if process else None)
        await asyncio.sleep(1)


async def run(args) -> None:
    check_required_ports(args)
    runtime_cache = Path(
        args.cache_dir or tempfile.mkdtemp(prefix='moq-live-video-viewer-')
    )
    certfile, keyfile = prepare_tls(args, runtime_cache)
    relay = RustMOQRelay(
        host=args.relay_host,
        port=args.relay_port,
        cache_dir=str(runtime_cache / 'relay'),
        startup_timeout=60.0,
    )
    manager = ClientManager()
    media_mode = 'fmp4' if args.subscriber == 'rust-fmp4' else 'h264'
    stats = RuntimeStats(media_mode=media_mode)
    output_queue: asyncio.Queue = asyncio.Queue(maxsize=args.queue_size)
    app = make_app(manager, stats, args)
    server = uvicorn.Server(
        uvicorn.Config(
            app,
            host=args.web_host,
            port=args.web_port,
            log_level=args.uvicorn_log_level,
            ssl_certfile=certfile,
            ssl_keyfile=keyfile,
        )
    )
    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()
    for signum in (signal.SIGINT, signal.SIGTERM):
        with contextlib.suppress(NotImplementedError):
            loop.add_signal_handler(signum, stop_event.set)

    await relay.start()
    logger.info('Rust MOQ relay listening on %s:%s', args.relay_host, args.relay_port)
    if args.subscriber == 'rust-fmp4':
        subscriber_task = rust_fmp4_subscriber_loop(args, output_queue, stats)
    elif args.subscriber == 'rust-avc3':
        subscriber_task = rust_avc3_subscriber_loop(args, output_queue, stats)
    else:
        subscriber_task = subscriber_loop(args, output_queue, stats)
    tasks = [
        asyncio.create_task(server.serve()),
        asyncio.create_task(subscriber_task),
        asyncio.create_task(broadcast_loop(output_queue, manager, stats, args)),
        asyncio.create_task(relay_rss_loop(relay, stats)),
    ]
    scheme = 'http' if args.no_https else 'https'
    logger.info('viewer listening on %s://%s:%s', scheme, args.web_host, args.web_port)
    try:
        await stop_event.wait()
    finally:
        server.should_exit = True
        for task in tasks:
            task.cancel()
        for task in tasks:
            with contextlib.suppress(asyncio.CancelledError):
                await task
        await relay.stop()


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--relay-host', default='0.0.0.0')
    parser.add_argument('--relay-port', type=int, default=9007)
    parser.add_argument('--web-host', default='0.0.0.0')
    parser.add_argument('--web-port', type=int, default=9008)
    parser.add_argument('--namespace', default=DEFAULT_NAMESPACE)
    parser.add_argument('--video-name', default=DEFAULT_VIDEO_NAME)
    parser.add_argument('--meta-name', default=DEFAULT_META_NAME)
    parser.add_argument(
        '--subscriber',
        choices=['python', 'rust-fmp4', 'rust-avc3'],
        default='python',
        help='Playback data path: Python H.264, Rust fMP4/MSE, or Rust AVC3/WebCodecs.',
    )
    parser.add_argument(
        '--moq-cli-bin',
        default='/home/acn/zqm/test/moq-rust/moq/target/release/moq-cli',
    )
    parser.add_argument('--moq-url')
    parser.add_argument('--broadcast-name', default='camera')
    parser.add_argument('--max-latency', type=int, default=100)
    parser.add_argument('--fmp4-read-size', type=int, default=65536)
    parser.add_argument(
        '--fmp4-mime',
        default='video/mp4; codecs="avc1.42E01F"',
    )
    parser.add_argument('--moq-cli-log-level', default='warn')
    parser.add_argument('--avc3-max-frame-bytes', type=int, default=2_000_000)
    parser.add_argument('--h264-width', type=int, default=1280)
    parser.add_argument('--h264-height', type=int, default=720)
    parser.add_argument('--h264-fps', type=float, default=30.0)
    parser.add_argument('--h264-codec', default='avc1.42E01F')
    parser.add_argument('--subscribe-timeout', type=float, default=5.0)
    parser.add_argument('--retry-interval', type=float, default=2.0)
    parser.add_argument('--queue-size', type=int, default=240)
    parser.add_argument('--cache-dir')
    parser.add_argument('--no-https', action='store_true')
    parser.add_argument('--tls-certfile')
    parser.add_argument('--tls-keyfile')
    parser.add_argument('--cert-host')
    parser.add_argument('--uvicorn-log-level', default='warning')
    parser.add_argument('--log-level', default='INFO')
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format='%(asctime)s %(levelname)s %(message)s',
    )
    asyncio.run(run(args))


if __name__ == '__main__':
    main()
