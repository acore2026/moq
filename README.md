<p align="center">
	<img height="128px" src="https://github.com/moq-dev/moq/blob/main/.github/logo.svg" alt="Media over QUIC">
</p>

![License](https://img.shields.io/badge/license-MIT%2FApache--2.0-blue)
[![Discord](https://img.shields.io/discord/1124083992740761730)](https://discord.gg/FCYF3p99mr)
[![Crates.io](https://img.shields.io/crates/v/moq-lite)](https://crates.io/crates/moq-lite)
[![npm](https://img.shields.io/npm/v/@moq/lite)](https://www.npmjs.com/package/@moq/lite)

# Media over QUIC

[Media over QUIC](https://moq.dev) (MoQ) is a next-generation live media protocol that provides **real-time latency** at **massive scale**.
Built using modern web technologies, MoQ delivers WebRTC-like latency without the constraints of WebRTC.
The core networking is delegated to a QUIC library but the rest is in application-space, giving you full control over your media pipeline.

**Key Features:**

- 🚀 **Real-time latency** using QUIC for prioritization and partial reliability.
- 📈 **Massive scale** designed for fan-out and supports cross-region clustering.
- 🌐 **Modern Web** using [WebTransport](https://developer.mozilla.org/en-US/docs/Web/API/WebTransport_API), [WebCodecs](https://developer.mozilla.org/en-US/docs/Web/API/WebCodecs_API), and [WebAudio](https://developer.mozilla.org/en-US/docs/Web/API/Web_Audio_API).
- 🎯 **Multi-language** with both Rust (native) and TypeScript (web) libraries.
- 🔧 **Generic** for any live data, not just media. Includes text chat as both an example and a core feature.

> **Note:** This project implements [moq-lite](https://doc.moq.dev/concept/layer/moq-lite), a forwards-compatible subset of the IETF [moq-transport](https://datatracker.ietf.org/doc/draft-ietf-moq-transport/) draft. moq-lite works with any moq-transport CDN (ex. [Cloudflare](https://moq.dev/blog/first-cdn/)). The focus is narrower, prioritizing simplicity and deployability.

## Fork Status

This repository is based on the upstream [`moq-dev/moq`](https://github.com/moq-dev/moq) project.
The local Rust branch keeps the upstream layered design, with additional work focused on interoperability, object streams, and backfill workflows:

- **IETF draft-17 interop**: updated message sizing, subscribe parameter decoding, and publish response handling for draft-17 peers.
- **Range-aware subscriptions**: tracks can carry optional start and end group bounds for replay, fetch, and backfill use cases.
- **Fetch support**: the IETF publisher path can answer standalone fetch requests from cached groups; joining fetch remains unsupported.
- **Object CLI pipeline**: `moq-cli` can publish, subscribe, and fetch arbitrary object frames through stdin/stdout, in addition to media formats.
- **Configurable cache retention**: `MOQ_LITE_MAX_GROUP_AGE_SECS` can extend the in-memory group cache beyond the default 30 seconds.
- **Python bindings path**: Python applications can use `py/moq-lite`, which wraps the Rust `moq-ffi` bindings.

## Demo

This repository is split into multiple binaries and libraries across different languages.
It can get overwhelming, so there's an included [demo](demo/web) with some examples.

**Note:** this demo uses an insecure HTTP fetch intended for *local development only*.
In production, you'll need a proper domain and a matching TLS certificate via [LetsEncrypt](https://letsencrypt.org/docs/) or similar.

### Quick Setup

**Requirements:**

- [Nix](https://nixos.org/download.html)
- [Nix Flakes enabled](https://nixos.wiki/wiki/Flakes)

```sh
# Runs a relay, demo media, and the web server
nix develop -c just
```

Then visit <https://localhost:8080> to see the demo.
Note that this uses an insecure HTTP fetch for local development only; in production you'll need a proper domain + TLS certificate.

*TIP:* If you've installed [nix-direnv](https://github.com/nix-community/nix-direnv), then only `just` is required.

### Object and Fetch CLI

This branch extends `moq-cli` with an object-oriented stdin/stdout pipeline for non-media data and replay tests.
The object mode uses the `MOBJ` frame format implemented in [`rs/moq-cli/src/object.rs`](rs/moq-cli/src/object.rs).

```sh
# Publish object frames from stdin.
moq publish --url http://localhost:4443 --name demo object < objects.mobj

# Subscribe to a live object track.
moq subscribe --url http://localhost:4443 --name demo --output object --track events > live.mobj

# Request a bounded object range and exit after the idle timeout.
moq fetch --url http://localhost:4443 --name demo --output object --track events --start-group 0 --end-group 10 > replay.mobj
```

For longer backfill windows, increase the in-memory group retention before starting the publisher or relay:

```sh
MOQ_LITE_MAX_GROUP_AGE_SECS=300 just relay
```

### ACN / Agent GW Integration

This branch is also used by the ACN Agent GW MoQ Rust integration. In that setup,
this repository provides the patched Rust binaries:

```text
target/release/moq-cli
target/release/moq-relay
```

The Python integration layer lives in this repository:

```text
py/acn-moq-rust/
```

That Python layer packages `moq-cli` into a wheel and exposes Python-friendly
APIs for:

- ACN SDK object/bytes publish, subscribe, and fetch workflows.
- Real-time camera publishing through `ffmpeg -> moq-cli publish avc3`.
- Local relay and browser viewer tests using `moq-relay` and
  `moq-cli subscribe --output avc3`.

The recommended real-time video path is:

```text
Remote camera
  -> ffmpeg H.264 Annex-B
  -> Python wheel starts moq-cli publish avc3
  -> moq-relay on :9007
  -> moq-cli subscribe --output avc3
  -> Agent GW test viewer on :9008
  -> Browser WebCodecs
```

Python manages process lifecycle only; video bytes stay on the native
`ffmpeg -> moq-cli` hot path.

### Build Patched Binaries for ACN

Build the patched CLI and relay from this repository:

```sh
cd /path/to/moq
cargo build --release --package moq-cli
cargo build --release --package moq-relay
```

The Linux outputs are:

```text
target/release/moq-cli
target/release/moq-relay
```

On Windows, build at least `moq-cli.exe` for remote camera publishers:

```powershell
cd D:\path\to\moq
cargo build --release --package moq-cli
```

The Windows output is:

```text
target\release\moq-cli.exe
```

Verify that the build contains the ACN video path:

```sh
target/release/moq-cli subscribe --help
```

The output must include:

```text
avc3
```

### Package moq-cli into the Python Wheel

In the ACN Python integration directory, copy the built binary into the Python
package data directory before building the wheel.

Linux example:

```sh
cd /path/to/moq/py/acn-moq-rust

python3 tools/package_moq_rust_video_binary.py \
  /path/to/moq/target/release/moq-cli \
  --system Linux \
  --machine x86_64

python3 -m pip wheel . -w dist --no-deps --no-build-isolation
```

Windows example:

```powershell
cd D:\path\to\moq\py\acn-moq-rust

python tools\package_moq_rust_video_binary.py `
  D:\path\to\moq\target\release\moq-cli.exe `
  --system Windows `
  --machine AMD64

python -m pip wheel . -w dist --no-deps --no-build-isolation
```

The resulting wheel contains the platform-specific `moq-cli` binary. `ffmpeg`
is not bundled and must be installed separately or passed to the Python API by
absolute path.

### Real-Time Video Test with Agent GW Viewer

The Agent GW test viewer uses the following ports by convention:

```text
9007: Rust moq-relay
9008: HTTPS browser viewer
```

Do not use the Agent GW production ports for this test unless explicitly
intended:

```text
9001: ARF
9002: ACF
9003: Python MOQT relay
```

Start the relay and HTTPS viewer from the ACN Python integration directory:

```sh
cd /path/to/moq/py/acn-moq-rust

MOQ_OFFICIAL_RELAY_BIN=/path/to/moq/target/release/moq-relay \
python3 video/moq_live_video_viewer.py \
  --subscriber rust-avc3 \
  --moq-cli-bin /path/to/moq/target/release/moq-cli \
  --relay-host 0.0.0.0 \
  --relay-port 9007 \
  --web-host 0.0.0.0 \
  --web-port 9008 \
  --cert-host <server-ip> \
  --log-level INFO
```

Open:

```text
https://<server-ip>:9008/
```

The viewer uses browser WebCodecs, so remote browser access should use HTTPS.
When using the generated self-signed certificate, the browser will require a
manual trust/continue action.

Remote Windows camera publishing uses the Python wheel API:

```powershell
python .\remote_camera_publish.py `
  --relay-url http://<server-ip>:9007/ `
  --name camera `
  --backend dshow `
  --camera-name "HD Camera" `
  --width 1280 `
  --height 720 `
  --fps 30 `
  --bitrate 2500k `
  --print-logs
```

The relay URL remains HTTP because it is used by `moq-cli` for the relay
certificate fetch and WebTransport connection setup:

```text
http://<server-ip>:9007/
```

### ACN Compatibility Notes

The current integration targets the ACN SDK's common `moq-python` usage:

- live object publish/subscribe
- arbitrary bytes payloads
- bounded fetch/backfill workflows
- Rust relay fan-out
- low-latency real-time video through `avc3`

It is not a complete IETF draft-17 implementation of every protocol surface.
Known boundaries:

- joining fetch is not supported;
- advanced priority and all fine-grained draft-17 controls are not fully
  exposed through the Python wrapper;
- precise end-to-end video latency metrics require an application timestamp
  side channel or an extended AVC3 frame header.

For production ACN usage, prefer:

```text
Object/bytes data: moq_rust_client.RustCliMoQClient + moq-relay
Real-time video:   moq_rust_video + ffmpeg + moq-cli avc3 + moq-relay
```

### Full Setup

If you don't like Nix, then you can install dependencies manually:

**Requirements:**

- [Just](https://github.com/casey/just)
- [Rust](https://www.rust-lang.org/tools/install)
- [Bun](https://bun.sh/)
- [FFmpeg](https://ffmpeg.org/download.html)
- ...probably some other stuff

**Run it:**

```sh
# Install some more dependencies
just install

# Runs a relay, demo media, and the web server
just
```

Then visit <http://localhost:5173> to see the demo.

## Architecture

MoQ is designed as a layered protocol stack.

**Rule 1**: The CDN MUST NOT know anything about your application, media codecs, or even the available tracks.
Everything could be fully E2EE and the CDN wouldn't care. **No business logic allowed**.

Instead, [`moq-relay`](rs/moq-relay) operates on rules encoded in the [`moq-lite`](https://docs.rs/moq-lite) header.
These rules are based on video encoding but are generic enough to be used for any live data.
The goal is to keep the server as dumb as possible while supporting a wide range of use-cases.

The media logic is split into another protocol called [`hang`](https://docs.rs/hang).
It's pretty simple and only intended to be used by clients or media servers.
If you want to do something more custom, then you can always extend it or replace it entirely.

Think of `hang` as like HLS/DASH, while `moq-lite` is like HTTP.

```
┌─────────────────┐
│   Application   │   🏢 Your business logic
│                 │    - authentication, non-media tracks, etc.
├─────────────────┤
│      hang       │   🎬 Media-specific encoding/streaming
│                 │     - codecs, containers, catalog
├─────────────────├
│    moq-lite     │  🚌 Generic pub/sub transport
│                 │     - broadcasts, tracks, groups, frames
├─────────────────┤
│  WebTransport   │  🌐 Browser-compatible QUIC
│      QUIC       │     - HTTP/3 handshake, multiplexing, etc.
└─────────────────┘
```

## Libraries

This repository provides both [Rust](rs) and [TypeScript](js) libraries with similar APIs but language-specific optimizations.

### Rust

| Crate                       | Description                                                                                                                           | Docs                                                                           |
|-----------------------------|---------------------------------------------------------------------------------------------------------------------------------------|--------------------------------------------------------------------------------|
| [moq-lite](rs/moq-lite)          | The core pub/sub transport protocol. Has built-in concurrency, deduplication, range-aware subscriptions, and IETF fetch handling.     | [![docs.rs](https://docs.rs/moq-lite/badge.svg)](https://docs.rs/moq-lite)     |
| [moq-relay](rs/moq-relay)   | A clusterable relay server. This relay performs fan-out connecting multiple clients and servers together.                             |                                                                                |
| [moq-token](rs/moq-token)   | An authentication scheme supported by `moq-relay`. Can be used as a library or as [a CLI](rs/moq-token-cli) to authenticate sessions. |                                                                                |
| [moq-native](rs/moq-native) | Opinionated helpers to configure a Quinn QUIC endpoint. It's harder than it should be.                                                | [![docs.rs](https://docs.rs/moq-native/badge.svg)](https://docs.rs/moq-native) |
| [libmoq](rs/libmoq)         | C bindings for `moq-lite`.                                                                                                            | [![docs.rs](https://docs.rs/libmoq/badge.svg)](https://docs.rs/libmoq)         |
| [moq-ffi](rs/moq-ffi)       | UniFFI bindings used by Python and other native language wrappers.                                                                     |                                                                                |
| [hang](rs/hang)             | Media-specific encoding/streaming layered on top of `moq-lite`. Can be used as a library.                     | [![docs.rs](https://docs.rs/hang/badge.svg)](https://docs.rs/hang)             |
| [moq-cli](rs/moq-cli)       | A CLI for publishing media, subscribing to fMP4/AVC3 output, and moving arbitrary object frames through MoQ relays.                  |                                                                                |
| [moq-mux](rs/moq-mux)       | Media muxers and demuxers (fMP4/CMAF, HLS) for importing content into MoQ broadcasts.                                                 | [![docs.rs](https://docs.rs/moq-mux/badge.svg)](https://docs.rs/moq-mux)       |
| [moq-gst](rs/moq-gst)       | A GStreamer plugin for publishing or consuming MoQ broadcasts. Not built by default; requires GStreamer dev libraries.                         |                                                                                |

### Python

| Package | Description |
|---------|-------------|
| [moq-lite](py/moq-lite) | Pythonic wrapper around the Rust `moq-ffi` bindings with async iterators, context managers, and simplified connection setup. |
| [moq-ffi](rs/moq-ffi) | Lower-level UniFFI surface generated from the Rust protocol stack. |

### TypeScript

| Package                                  | Description                                                                                                        | NPM                                                                                                   |
|------------------------------------------|--------------------------------------------------------------------------------------------------------------------|-------------------------------------------------------------------------------------------------------|
| **[@moq/lite](js/lite)**             | The core pub/sub transport protocol. Intended for browsers, but can be run server-side with a WebTransport polyfill.                                   | [![npm](https://img.shields.io/npm/v/@moq/lite)](https://www.npmjs.com/package/@moq/lite)   |
| **[@moq/token](js/token)**             |  Authentication library & CLI for JS/TS environments (see [Authentication](doc/app/relay/auth.md))                               | [![npm](https://img.shields.io/npm/v/@moq/token)](https://www.npmjs.com/package/@moq/token)   |
| **[@moq/hang](js/hang)**           | Core media library: catalog, container, and support. Shared by `@moq/watch` and `@moq/publish`. | [![npm](https://img.shields.io/npm/v/@moq/hang)](https://www.npmjs.com/package/@moq/hang) |
| **[@moq/demo](demo/web)** | Examples using `@moq/hang`.                                                                                  |                                                                                                       |
| **[@moq/watch](js/watch)**         | Subscribe to and render MoQ broadcasts (Web Component + JS API).                                                        | [![npm](https://img.shields.io/npm/v/@moq/watch)](https://www.npmjs.com/package/@moq/watch)     |
| **[@moq/publish](js/publish)**     | Publish media to MoQ broadcasts (Web Component + JS API).                                                               | [![npm](https://img.shields.io/npm/v/@moq/publish)](https://www.npmjs.com/package/@moq/publish) |
| **[@moq/ui-core](js/ui-core)**     | Shared UI components (Button, Icon, Stats, CSS theme) used by `@moq/watch/ui` and `@moq/publish/ui`.                    | [![npm](https://img.shields.io/npm/v/@moq/ui-core)](https://www.npmjs.com/package/@moq/ui-core) |

## Documentation

Additional documentation and implementation details:

- **[Authentication](doc/app/relay/auth.md)** - JWT tokens, authorization, and security
- **[Architecture](doc/concept/architecture.md)** - High-level map of the Rust crates, bindings, media layer, and transport runtime

## Protocol

Read the specifications:

- [moq-lite](https://moq-dev.github.io/drafts/draft-lcurley-moq-lite.html)
- [hang](https://moq-dev.github.io/drafts/draft-lcurley-moq-hang.html)
- [use-cases](https://moq-dev.github.io/drafts/draft-lcurley-moq-use-cases.html)

## Development

```sh
# See all available commands
just

# Build everything
just build

# Run tests and linting
just check

# Automatically fix some linting errors
just fix

# Run the demo manually
just relay    # Terminal 1: Start relay server
just pub tos  # Terminal 2: Publish a demo video using ffmpeg
just web      # Terminal 3: Start web server
```

There are more commands: check out the [justfile](justfile).

## Iroh support

The `moq-native` and `moq-relay` crates optionally support connecting via [iroh](https://github.com/n0-computer/iroh). The iroh integration is disabled by default, to use it enable the `iroh` feature.

When the iroh feature is enabled, you can connect to iroh endpoints with these URLs:

- `iroh://<ENDPOINT_ID>`: Connect via moq-lite over raw QUIC.
- `moql+iroh://<ENDPOINT_ID>`: Connect via moq-lite over raw QUIC (same as above)
- `moqt+iroh://<ENDPOINT_ID>`: Connect via IETF MoQ over raw QUIC
- `h3+iroh://<ENDPOINT_ID>/optional/path?with=query`: Connect via WebTransport over HTTP/3.

`ENDPOINT_ID` must be the hex-encoded iroh endpoint id. It is currently not possible to set direct addresses or iroh relay URLs. The iroh integration in moq-native uses iroh's default discovery mechanisms to discover other endpoints by their endpoint id.

You can run a demo like this:

```sh
# Terminal 1: Start a relay server
just relay --iroh-enabled
# Copy the endpoint id printed at "iroh listening"

# Terminal 2: Publish via moq-lite over raw iroh QUIC
#
# Replace ENDPOINT_ID with the relay's endpoint id.
#
# We set an `anon/` prefix to match the broadcast name the web ui expects
# Because moq-lite does not have headers if using raw QUIC, only the hostname
# in the URL can be used.
just pub-iroh bbb iroh://ENDPOINT_ID  anon/
# Alternatively you can use WebTransport over HTTP/3 over iroh,
# which allows to set a path prefix in the URL:
just pub-iroh bbb h3+iroh://ENDPOINT_ID/anon

# Terminal 3: Start web server
just web
```

Then open [localhost:5173](http://localhost:5173) and watch BBB, pushed from terminal 1 via iroh to the relay running in terminal 2, from where the browser fetches it over regular WebTransport.

`just serve` serves a video via iroh alongside regular QUIC (it enables the `iroh` feature). This repo currently does not provide a native viewer, so you can't subscribe to it directly. However, you can use the [watch example from iroh-live](https://github.com/n0-computer/iroh-live/blob/main/iroh-live/examples/watch.rs) to view a video published via `moq-native`.

## License

Licensed under either:

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE) or https://www.apache.org/licenses/LICENSE-2.0)
- MIT license ([LICENSE-MIT](LICENSE-MIT) or https://opensource.org/licenses/MIT)
