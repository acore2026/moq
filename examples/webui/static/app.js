const config = window.MOQ_WEBUI_CONFIG || {};
const API_EVENTS = "/api/events";
const API_STATUS = "/api/status";
const API_SUBSCRIBER_START = "/api/subscriber/start";
const API_SUBSCRIBER_REMOVE = "/api/subscriber/remove";
const FRAME_JSON = 1;
const FRAME_INIT = 2;
const FRAME_FRAGMENT = 3;
const FRAME_END = 4;
const LIVE_EDGE_DELAY_SECONDS = 1.2;
const MIN_BUFFER_AHEAD_SECONDS = 0.5;
const MAX_APPEND_QUEUE_SEGMENTS = 8;
const DROP_LOG_INTERVAL_MS = 2500;
const MEDIA_RECOVERY_INTERVAL_MS = 1500;

const elements = {
  bridgeValue: document.getElementById("bridgeValue"),
  byteCount: document.getElementById("byteCount"),
  codecValue: document.getElementById("codecValue"),
  connectionBadge: document.getElementById("connectionBadge"),
  connectionText: document.getElementById("connectionText"),
  detailLine: document.getElementById("detailLine"),
  fragmentCount: document.getElementById("fragmentCount"),
  logs: document.getElementById("logs"),
  mimeValue: document.getElementById("mimeValue"),
  namespaceInput: document.getElementById("namespaceInput"),
  previewPlaceholder: document.getElementById("previewPlaceholder"),
  previewPanel: document.getElementById("previewPanel"),
  previewStats: document.getElementById("previewStats"),
  previewTrackLabel: document.getElementById("previewTrackLabel"),
  relayValue: document.getElementById("relayValue"),
  resolutionValue: document.getElementById("resolutionValue"),
  statusLine: document.getElementById("statusLine"),
  subscriberButton: document.getElementById("subscriberButton"),
  subscriptionChips: document.getElementById("subscriptionChips"),
  subscriptionListWrap: document.getElementById("subscriptionListWrap"),
  trackNameInput: document.getElementById("trackNameInput"),
  trackValue: document.getElementById("trackValue"),
  video: document.getElementById("video"),
  videoWrap: document.getElementById("videoWrap"),
};

const state = {
  appendQueue: [],
  bytes: 0,
  certHashHex: null,
  droppedFragments: 0,
  fragments: 0,
  initSegment: null,
  mediaSource: null,
  metadata: null,
  pendingSegments: [],
  playbackGeneration: 0,
  renderedLogCount: 0,
  shuttingDown: false,
  sourceBuffer: null,
  statusStream: null,
  status: null,
  transport: null,
  transportCertHashHex: null,
  transportConnectPromise: null,
  transportError: false,
  lastDropLogAt: 0,
  lastMediaRecoveryAt: 0,
  videoUrl: null,
};

function logLine(message) {
  const line = document.createElement("div");
  line.className = "log-entry";
  if (/fail|error|reject|unsupported|blocked|closed with an error/i.test(message)) {
    line.classList.add("error");
  }
  line.textContent = `[${new Date().toLocaleTimeString()}] ${message}`;
  elements.logs.prepend(line);
}

function shouldDisplayServerLog(message) {
  const text = String(message || "");
  return !/^(camera publisher|video publisher|publisher(?:\s+\S+)?):/i.test(text) &&
    !/^Subscriber: forwarded live fragment\b/i.test(text);
}

function appendServerLogLine(message, appendAtEnd = false) {
  if (!shouldDisplayServerLog(message)) {
    return;
  }
  const line = document.createElement("div");
  line.className = "log-entry server";
  line.textContent = message;
  if (appendAtEnd) {
    elements.logs.appendChild(line);
    return;
  }
  elements.logs.prepend(line);
}

function setConnectionState(label, color) {
  elements.connectionText.textContent = label;
  elements.connectionBadge.style.color = color;
}

function setTransportFailure(message, detail) {
  setConnectionState("error", "#b42318");
  elements.statusLine.textContent = message;
  elements.detailLine.textContent = detail;
}

function setPlayerStatus(message) {
  elements.statusLine.textContent = message;
}

function setPreviewPlaceholder(title, detail) {
  const titleElement = elements.previewPlaceholder.querySelector(".preview-placeholder-title, .empty-text");
  const detailElement = elements.previewPlaceholder.querySelector(".preview-placeholder-text, .empty-subtext");
  if (titleElement) {
    titleElement.textContent = title;
  }
  if (detailElement) {
    detailElement.textContent = detail;
  }
}

function requestPlayback(reason) {
  if (!state.videoUrl && !elements.video.currentSrc) {
    return;
  }
  if (elements.video.error) {
    const detail = elements.video.error.message || elements.video.error.code;
    setPlayerStatus(`Video error: ${detail}`);
    logLine(`video element is in error state (${reason}): ${detail}`);
    recoverMediaPipeline(`video element error during ${reason}`);
    return;
  }
  const playPromise = elements.video.play();
  if (playPromise && typeof playPromise.catch === "function") {
    playPromise.catch((error) => {
      setPlayerStatus(`Playback rejected: ${error.message}`);
      logLine(`video.play() rejected (${reason}): ${error.message}`);
    });
  }
}

function releaseVideoSource() {
  if (state.videoUrl) {
    URL.revokeObjectURL(state.videoUrl);
    state.videoUrl = null;
  }
  elements.video.removeAttribute("src");
  elements.video.load();
}

function recoverMediaPipeline(reason) {
  const now = Date.now();
  if (!state.metadata || !state.initSegment || now - state.lastMediaRecoveryAt < MEDIA_RECOVERY_INTERVAL_MS) {
    return;
  }
  state.lastMediaRecoveryAt = now;
  state.appendQueue = [];
  state.pendingSegments = [];
  state.sourceBuffer = null;
  state.mediaSource = null;
  releaseVideoSource();
  elements.detailLine.textContent = `Recovered media pipeline after ${reason}. Waiting for live fragments.`;
  setPlayerStatus(`Recovered media pipeline: ${reason}`);
  logLine(`media pipeline reset (${reason})`);
  enqueueSegment(state.initSegment);
}

function inferAvc1CodecFromInitSegment(bytes) {
  for (let index = 0; index <= bytes.length - 8; index += 1) {
    if (
      bytes[index] === 0x61 &&
      bytes[index + 1] === 0x76 &&
      bytes[index + 2] === 0x63 &&
      bytes[index + 3] === 0x43
    ) {
      const profile = bytes[index + 5];
      const compatibility = bytes[index + 6];
      const level = bytes[index + 7];
      return `avc1.${profile.toString(16).padStart(2, "0").toUpperCase()}${compatibility.toString(16).padStart(2, "0").toUpperCase()}${level.toString(16).padStart(2, "0").toUpperCase()}`;
    }
  }
  return "avc1.64001F";
}

function hexToUint8Array(hex) {
  const pairs = hex.match(/.{1,2}/g) || [];
  return Uint8Array.from(pairs.map((pair) => parseInt(pair, 16)));
}

function candidateWebTransportHosts() {
  const candidates = [window.location.hostname, config.wtHost];
  if (["localhost", "127.0.0.1", "::1"].includes(window.location.hostname)) {
    candidates.push("127.0.0.1", "localhost");
  }
  return [...new Set(candidates.filter(Boolean))];
}

function tunePlaybackRate(bufferAhead) {
  if (!Number.isFinite(bufferAhead)) {
    elements.video.playbackRate = 1.0;
    return;
  }
  if (bufferAhead > LIVE_EDGE_DELAY_SECONDS + 0.6) {
    elements.video.playbackRate = 1.04;
  } else if (bufferAhead > LIVE_EDGE_DELAY_SECONDS + 0.3) {
    elements.video.playbackRate = 1.02;
  } else {
    elements.video.playbackRate = 1.0;
  }
}

function syncPlaybackPosition(reason) {
  const buffered = elements.video.buffered;
  if (!buffered || buffered.length === 0) {
    return;
  }
  const lastRange = buffered.length - 1;
  const rangeStart = buffered.start(lastRange);
  const rangeEnd = buffered.end(lastRange);
  const currentTime = elements.video.currentTime || 0;
  const bufferAhead = rangeEnd - currentTime;
  tunePlaybackRate(bufferAhead);
  if (bufferAhead >= MIN_BUFFER_AHEAD_SECONDS && currentTime >= rangeStart && currentTime <= rangeEnd) {
    return;
  }
  const liveEdge = Math.max(rangeStart, rangeEnd - LIVE_EDGE_DELAY_SECONDS);
  if (liveEdge > currentTime + 0.15) {
    elements.video.currentTime = liveEdge;
    logLine(`seeked to buffered range (${reason}): ${liveEdge.toFixed(3)}s`);
  }
}

function updateStats() {
  elements.byteCount.textContent = new Intl.NumberFormat().format(state.bytes);
  elements.fragmentCount.textContent = new Intl.NumberFormat().format(state.fragments);
  elements.codecValue.textContent = state.metadata?.mse_codec || state.metadata?.codec || "-";
  elements.resolutionValue.textContent = state.metadata ? `${state.metadata.width}x${state.metadata.height}` : "-";
  elements.mimeValue.textContent = state.metadata?.mime_type || "-";
  elements.previewStats.textContent = state.metadata?.fps ? `${state.metadata.fps}` : "-";
}

function resetPlayer(reason) {
  state.playbackGeneration += 1;
  state.appendQueue = [];
  state.bytes = 0;
  state.droppedFragments = 0;
  state.fragments = 0;
  state.pendingSegments = [];
  state.sourceBuffer = null;
  state.mediaSource = null;
  state.metadata = null;
  state.initSegment = null;
  state.lastDropLogAt = 0;
  state.lastMediaRecoveryAt = 0;
  releaseVideoSource();
  elements.detailLine.textContent = reason;
  updateStats();
}

function ensurePlayer(metadata) {
  if (state.mediaSource) {
    return;
  }
  const mimeType = metadata.mime_type || `video/mp4; codecs="${metadata.mse_codec || "avc1.42E01F"}"`;
  if (!("MediaSource" in window) || !MediaSource.isTypeSupported(mimeType)) {
    throw new Error(`MediaSource does not support ${mimeType}`);
  }

  const mediaSource = new MediaSource();
  mediaSource.addEventListener("sourceopen", () => {
    try {
      const sourceBuffer = mediaSource.addSourceBuffer(mimeType);
      sourceBuffer.mode = "segments";
      sourceBuffer.addEventListener("error", () => {
        setPlayerStatus("SourceBuffer error");
        logLine("sourceBuffer error");
        recoverMediaPipeline("sourceBuffer error");
      });
      sourceBuffer.addEventListener("abort", () => {
        setPlayerStatus("SourceBuffer aborted");
        logLine("sourceBuffer aborted");
        recoverMediaPipeline("sourceBuffer abort");
      });
      sourceBuffer.addEventListener("updateend", () => {
        if (state.fragments === 0 || state.fragments % 30 === 0) {
          setPlayerStatus(`Buffered live media (${state.fragments} fragments)`);
        }
        flushSourceBuffer();
        syncPlaybackPosition("updateend");
        requestPlayback("updateend");
      });
      state.sourceBuffer = sourceBuffer;
      setPlayerStatus("MediaSource open");
      logLine(`media source open (${mimeType})`);
      flushSourceBuffer();
    } catch (error) {
      setPlayerStatus(`SourceBuffer setup failed: ${error.message}`);
      logLine(`addSourceBuffer failed: ${error.message}`);
    }
  }, { once: true });

  state.mediaSource = mediaSource;
  state.videoUrl = URL.createObjectURL(mediaSource);
  elements.video.src = state.videoUrl;
}

function recordDroppedFragments(count) {
  state.droppedFragments += count;
  const now = Date.now();
  if (now - state.lastDropLogAt < DROP_LOG_INTERVAL_MS) {
    return;
  }
  state.lastDropLogAt = now;
  logLine(`dropped ${state.droppedFragments} stale fragment(s) to keep live latency low`);
  state.droppedFragments = 0;
}

function enqueueSegment(bytes) {
  if (!state.metadata) {
    state.pendingSegments.push(bytes);
    return;
  }
  ensurePlayer(state.metadata);
  if (state.initSegment && bytes !== state.initSegment && state.appendQueue.length >= MAX_APPEND_QUEUE_SEGMENTS) {
    const dropped = Math.max(0, state.appendQueue.length - (MAX_APPEND_QUEUE_SEGMENTS - 1));
    state.appendQueue.splice(0, dropped);
    if (dropped > 0) {
      recordDroppedFragments(dropped);
    }
  }
  state.appendQueue.push(bytes);
  flushSourceBuffer();
}

function flushSourceBuffer() {
  if (!state.sourceBuffer || state.sourceBuffer.updating || state.appendQueue.length === 0) {
    return;
  }
  if (!state.mediaSource || state.mediaSource.readyState !== "open") {
    return;
  }
  const next = state.appendQueue.shift();
  try {
    state.sourceBuffer.appendBuffer(next);
  } catch (error) {
    setPlayerStatus(`appendBuffer failed: ${error.message}`);
    logLine(`appendBuffer failed: ${error.message}`);
    recoverMediaPipeline("appendBuffer failure");
  }
}

function applyMetadata(metadata) {
  state.metadata = state.metadata ? { ...state.metadata, ...metadata } : metadata;
  updateStats();
  for (const pending of state.pendingSegments.splice(0)) {
    enqueueSegment(pending);
  }
}

function handleControlMessage(message) {
  if (message.type === "metadata") {
    applyMetadata(message.metadata);
    elements.statusLine.textContent = "Metadata received from subscriber bridge.";
    elements.detailLine.textContent = "Waiting for init segment and live fragments.";
    logLine(`metadata received (${state.metadata.mime_type || "unknown"})`);
    return;
  }
  if (message.type === "end") {
    logLine("publisher ended stream");
  }
}

class FrameReader {
  constructor(playbackGeneration) {
    this.buffer = new Uint8Array(0);
    this.playbackGeneration = playbackGeneration;
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
    if (this.playbackGeneration !== state.playbackGeneration) {
      return;
    }
    if (frameType === FRAME_JSON) {
      handleControlMessage(JSON.parse(new TextDecoder().decode(payload)));
      return;
    }
    if (frameType === FRAME_INIT) {
      state.initSegment = payload;
      if (!state.metadata) {
        const mseCodec = inferAvc1CodecFromInitSegment(payload);
        applyMetadata({
          codec: "H.264",
          mse_codec: mseCodec,
          mime_type: `video/mp4; codecs="${mseCodec}"`,
        });
      }
      state.bytes += payload.byteLength;
      updateStats();
      enqueueSegment(payload);
      setPlayerStatus("Initialization segment received");
      logLine(`init segment (${payload.byteLength} bytes)`);
      return;
    }
    if (frameType === FRAME_FRAGMENT) {
      state.bytes += payload.byteLength;
      state.fragments += 1;
      updateStats();
      enqueueSegment(payload);
      if (state.fragments === 1 || state.fragments % 30 === 0) {
        setPlayerStatus(`Receiving live fragments (${state.fragments})`);
        logLine(`fragment received (${payload.byteLength} bytes, count=${state.fragments})`);
      }
      return;
    }
    if (frameType === FRAME_END) {
      handleControlMessage({ type: "end" });
    }
  }
}

async function consumeReadableStream(readableStream, playbackGeneration) {
  const frameReader = new FrameReader(playbackGeneration);
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
  const playbackGeneration = state.playbackGeneration;
  const reader = transport.incomingUnidirectionalStreams.getReader();
  try {
    while (true) {
      const { value, done } = await reader.read();
      if (done) {
        return;
      }
      consumeReadableStream(value, playbackGeneration).catch((error) => {
        logLine(`incoming stream failed: ${error.message}`);
      });
    }
  } finally {
    reader.releaseLock();
  }
}

function closeActiveTransport() {
  state.transportConnectPromise = null;
  state.transportCertHashHex = null;
  state.transportError = false;
  if (!state.transport) {
    return;
  }
  try {
    state.transport.close();
  } catch (error) {
    console.debug("transport close skipped", error);
  }
  state.transport = null;
}

function attachTransportClosedHandlers(transport, url) {
  transport.closed
    .then(() => {
      const isCurrent = state.transport === transport;
      if (isCurrent) {
        state.transport = null;
      }
      if (!state.shuttingDown && isCurrent) {
        setConnectionState("closed", "#6b7280");
        elements.statusLine.textContent = "WebTransport session closed.";
        elements.detailLine.textContent = url;
        logLine(`transport closed (${url})`);
      }
    })
    .catch((error) => {
      const isCurrent = state.transport === transport;
      if (isCurrent) {
        state.transport = null;
      }
      if (!state.shuttingDown && isCurrent) {
        state.transportError = true;
        setConnectionState("error", "#b42318");
        elements.statusLine.textContent = "WebTransport closed with an error.";
        elements.detailLine.textContent = error.message;
        logLine(`transport closed with error (${url}): ${error.message}`);
      }
    });
}

async function connectTransport(certHashHex) {
  if (!certHashHex) {
    return;
  }
  if (!window.isSecureContext) {
    state.transportError = true;
    setTransportFailure(
      "WebTransport requires a secure page.",
      "Open this WebUI over HTTPS, or use localhost. Public http:// origins cannot start WebTransport in current browsers."
    );
    throw new Error("WebTransport requires a secure context; use HTTPS or localhost");
  }
  if (!("WebTransport" in window)) {
    state.transportError = true;
    setTransportFailure(
      "This browser does not expose WebTransport.",
      "Use a browser/version with WebTransport over HTTP/3 support."
    );
    throw new Error("WebTransport API is unavailable in this browser");
  }
  if (state.transport && state.transportCertHashHex === certHashHex) {
    return;
  }
  if (state.transportConnectPromise && state.transportCertHashHex === certHashHex) {
    return state.transportConnectPromise;
  }

  closeActiveTransport();
  state.transportCertHashHex = certHashHex;
  state.transportError = false;

  const connectPromise = (async () => {
    const certificateHashes = [{
      algorithm: "sha-256",
      value: hexToUint8Array(certHashHex),
    }];
    const failures = [];

    for (const host of candidateWebTransportHosts()) {
      const url = `https://${host}:${config.wtPort}${config.wtPath}`;
      const transport = new WebTransport(url, { serverCertificateHashes: certificateHashes });
      state.transport = transport;
      setConnectionState("negotiating", "#7aa6ff");
      elements.statusLine.textContent = "Connecting to WebTransport.";
      elements.detailLine.textContent = url;
      logLine(`connecting to ${url}`);

      try {
        await transport.ready;
      } catch (error) {
        failures.push(`${url}: ${error.message}`);
        logLine(`connect failed for ${url}: ${error.message}`);
        if (state.transport === transport) {
          state.transport = null;
        }
        try {
          transport.close();
        } catch (closeError) {
          console.debug("transport close skipped", closeError);
        }
        continue;
      }

      if (state.transport !== transport) {
        return;
      }
      attachTransportClosedHandlers(transport, url);
      state.transportError = false;
      setConnectionState("live", "#56f0c9");
      elements.statusLine.textContent = "WebTransport session is ready.";
      elements.detailLine.textContent = "Waiting for metadata from the MOQ subscriber bridge.";
      logLine(`transport ready via ${url}`);
      consumeIncomingStreams(transport).catch((error) => {
        setConnectionState("error", "#b42318");
        elements.statusLine.textContent = "Incoming WebTransport stream failed.";
        elements.detailLine.textContent = error.message;
        logLine(`incoming stream failed: ${error.message}`);
      });
      return;
    }

    state.transport = null;
    state.transportError = true;
    setTransportFailure("WebTransport connection failed.", failures.join(" | "));
    throw new Error(`All WebTransport connection attempts failed. ${failures.join(" | ")}`);
  })();

  state.transportConnectPromise = connectPromise;
  try {
    await connectPromise;
  } finally {
    if (state.transportConnectPromise === connectPromise) {
      state.transportConnectPromise = null;
    }
  }
}

async function fetchJson(url, options = undefined) {
  const response = await fetch(url, options);
  const payload = await response.json();
  if (!response.ok || !payload.ok) {
    throw new Error(payload.error || payload.detail || `request failed: ${response.status}`);
  }
  return payload;
}

function renderLogs(lines) {
  elements.logs.innerHTML = "";
  for (const line of [...lines].filter(shouldDisplayServerLog).reverse()) {
    appendServerLogLine(line, true);
  }
}

function splitTrackPath(track) {
  const normalized = String(track || "").trim();
  if (!normalized) {
    return { namespace: "", trackName: "" };
  }
  const parts = normalized.split("/").filter(Boolean);
  if (parts.length === 0) {
    return { namespace: "", trackName: "" };
  }
  return {
    namespace: parts.slice(0, -1).join("/"),
    trackName: parts[parts.length - 1],
  };
}

async function startOrSwitchSubscription(namespace, trackName) {
  const payload = await fetchJson(API_SUBSCRIBER_START, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      namespace,
      trackName,
    }),
  });
  applyStatus(payload.status);
  return payload;
}

async function reconnectPreviewTransport(certHashHex) {
  if (!certHashHex) {
    return;
  }
  await connectTransport(certHashHex);
}

async function removeSubscription(namespace, trackName) {
  const payload = await fetchJson(API_SUBSCRIBER_REMOVE, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      namespace,
      trackName,
    }),
  });
  applyStatus(payload.status);
  return payload;
}

function renderSubscriptions(tracks, activeTrack) {
  elements.subscriptionChips.innerHTML = "";
  const safeTracks = Array.isArray(tracks) ? tracks : [];
  elements.subscriptionListWrap.classList.toggle("hidden", safeTracks.length === 0);
  for (const track of safeTracks) {
    const wrapper = document.createElement("div");
    wrapper.className = "subscription-chip";
    if (track === activeTrack) {
      wrapper.classList.add("active");
    }
    const chip = document.createElement("button");
    chip.type = "button";
    chip.className = "subscription-chip-main";
    chip.textContent = track;
    chip.addEventListener("click", async () => {
      try {
        const next = splitTrackPath(track);
        elements.namespaceInput.value = next.namespace;
        elements.trackNameInput.value = next.trackName;
        closeActiveTransport();
        resetPlayer(`Switching preview to ${track}. Waiting for live stream.`);
        const payload = await startOrSwitchSubscription(next.namespace, next.trackName);
        await reconnectPreviewTransport(payload.status.subscriber.cert_hash_hex);
      } catch (error) {
        logLine(`subscription switch failed: ${error.message}`);
      }
    });

    const removeButton = document.createElement("button");
    removeButton.type = "button";
    removeButton.className = "subscription-chip-remove";
    removeButton.setAttribute("aria-label", `Remove ${track}`);
    removeButton.textContent = "×";
    removeButton.addEventListener("click", async (event) => {
      event.stopPropagation();
      try {
        const next = splitTrackPath(track);
        await removeSubscription(next.namespace, next.trackName);
        if (track === activeTrack) {
          resetPlayer("Subscription removed. Waiting for the current preview track.");
        }
      } catch (error) {
        logLine(`subscription removal failed: ${error.message}`);
      }
    });

    wrapper.appendChild(chip);
    wrapper.appendChild(removeButton);
    elements.subscriptionChips.appendChild(wrapper);
  }
}

function updateSubscriberDiagnostics(subscriber) {
  if (!subscriber?.running || state.transport || state.transportError) {
    return;
  }
  const fragmentCount = Number(subscriber.fragments || 0);
  const viewerCount = Number(subscriber.viewers || 0);
  if (fragmentCount > 0 && viewerCount === 0) {
    setConnectionState("no viewer", "#ffaf45");
    elements.statusLine.textContent = "Waiting for browser WebTransport connection.";
    elements.detailLine.textContent =
      `Subscriber has ${fragmentCount} fragment(s) for ${subscriber.track}, but no browser viewer is connected to port ${config.wtPort}.`;
    return;
  }
  if (subscriber.metadata_ready && subscriber.init_ready) {
    setConnectionState("waiting", "#ffaf45");
    elements.statusLine.textContent = "Waiting for live fragments.";
    elements.detailLine.textContent = `Subscriber is ready for ${subscriber.track}, but no media fragment has reached the browser yet.`;
  }
}

function applyStatus(status) {
  state.status = status;
  state.certHashHex = status.subscriber.cert_hash_hex || null;
  elements.relayValue.textContent = `${status.relay_host}:${status.relay_port}`;
  elements.bridgeValue.textContent = status.subscriber.running
    ? `${status.webtransport_public_host}:${status.webtransport_port}`
    : "stopped";
  if (status.subscriber.track) {
    elements.trackValue.textContent = status.subscriber.track;
    elements.previewTrackLabel.textContent = `Preview Track: ${status.subscriber.track}`;
  }
  renderSubscriptions(status.subscriber.tracks, status.subscriber.track);

  const subscriberRunning = Boolean(status.subscriber.running);
  elements.previewPanel.classList.toggle("hidden", !subscriberRunning);
  elements.previewPlaceholder.classList.toggle("hidden", subscriberRunning);
  elements.videoWrap.classList.toggle("hidden", !subscriberRunning);
  elements.previewStats.classList.toggle("hidden", !subscriberRunning);

  elements.subscriberButton.classList.toggle("active", subscriberRunning);
  elements.subscriberButton.innerHTML = subscriberRunning
    ? '<div><strong>Add Track</strong><br><span>Subscribe and switch the live preview</span></div><span>+</span>'
    : '<div><strong>Add Track</strong><br><span>Subscribe and preview this MOQ track</span></div><span>+</span>';

  if (subscriberRunning && state.certHashHex && !state.transport) {
    connectTransport(state.certHashHex).catch((error) => {
      logLine(`transport auto-connect failed: ${error.message}`);
    });
  }
  updateSubscriberDiagnostics(status.subscriber);

  if (!subscriberRunning && state.transport) {
    closeActiveTransport();
  }

  if (!subscriberRunning && !state.transport) {
    elements.statusLine.textContent = "Subscriber bridge is idle.";
    elements.detailLine.textContent = "Add a track to subscribe. Relay and publishers must be started manually.";
    setPreviewPlaceholder(
      "Preview unavailable",
      "Add a track from the Manage Track List panel. Relay and publishers must be running before subscription."
    );
    setConnectionState("idle", "#a7b0c3");
  }
}

async function refreshStatus() {
  try {
    const payload = await fetchJson(API_STATUS);
    applyStatus(payload.status);
  } catch (error) {
    logLine(`status refresh failed: ${error.message}`);
  }
}

function connectStatusStream() {
  if (state.statusStream) {
    state.statusStream.close();
  }

  const stream = new EventSource(API_EVENTS);
  state.statusStream = stream;

  stream.addEventListener("status", (event) => {
    try {
      applyStatus(JSON.parse(event.data));
    } catch (error) {
      logLine(`status stream parse failed: ${error.message}`);
    }
  });

  stream.addEventListener("logs", (event) => {
    try {
      const logs = JSON.parse(event.data);
      renderLogs(Array.isArray(logs) ? logs : []);
      state.renderedLogCount = Array.isArray(logs) ? logs.length : 0;
    } catch (error) {
      logLine(`logs stream parse failed: ${error.message}`);
    }
  });

  stream.addEventListener("log", (event) => {
    try {
      const message = JSON.parse(event.data);
      appendServerLogLine(message);
      state.renderedLogCount += 1;
    } catch (error) {
      logLine(`log stream parse failed: ${error.message}`);
    }
  });

  stream.onerror = () => {
    if (!state.shuttingDown) {
      logLine("status stream reconnecting");
    }
  };
}

async function handleSubscriberClick() {
  try {
    const wasRunning = Boolean(state.status?.subscriber?.running);
    const namespace = elements.namespaceInput.value.trim();
    const trackName = elements.trackNameInput.value.trim();
    if (!trackName) {
      throw new Error("Track name is required");
    }
    if (wasRunning) {
      closeActiveTransport();
    }
    resetPlayer(
      wasRunning
        ? "Switching preview to selected track. Waiting for live stream."
        : "Subscriber started. Waiting for live stream."
    );
    const payload = await startOrSwitchSubscription(namespace, trackName);
    applyStatus(payload.status);
    await reconnectPreviewTransport(payload.status.subscriber.cert_hash_hex);
  } catch (error) {
    setTransportFailure("Subscriber action failed.", error.message);
    logLine(`subscriber action failed: ${error.message}`);
  }
}

elements.video.addEventListener("waiting", () => {
  setPlayerStatus("Video waiting for buffer");
  logLine("video waiting");
  syncPlaybackPosition("waiting");
});
elements.video.addEventListener("playing", () => {
  setPlayerStatus("Video playing");
  logLine("video playing");
});
elements.video.addEventListener("error", () => {
  const error = elements.video.error;
  const detail = error ? (error.message || `code ${error.code}`) : "unknown media error";
  setPlayerStatus(`Video element error: ${detail}`);
  logLine(`video element error: ${detail}`);
  recoverMediaPipeline("video element error");
});
elements.subscriberButton.addEventListener("click", handleSubscriberClick);
window.addEventListener("beforeunload", () => {
  state.shuttingDown = true;
  if (state.statusStream) {
    state.statusStream.close();
    state.statusStream = null;
  }
  closeActiveTransport();
});

connectStatusStream();
refreshStatus();
window.setInterval(() => {
  if (!state.shuttingDown) {
    refreshStatus();
  }
}, 2000);
