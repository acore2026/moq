---
title: Architecture
description: High-level architecture of the MoQ Rust implementation
---

# Architecture

MoQ is split into small protocol, media, runtime, and application layers. The Rust implementation provides the relay, command-line tools, media adapters, native transport integration, and FFI bindings used by Python and other languages.

```mermaid
flowchart TB
    subgraph Apps["Applications"]
        Relay["moq-relay<br/>fan-out, cache, clustering"]
        Cli["moq-cli<br/>publish and subscribe media"]
        Clock["moq-clock<br/>example live broadcast"]
        Boy["moq-boy<br/>interactive demo"]
        Gst["moq-gst<br/>GStreamer plugin"]
    end

    subgraph Bindings["Language Bindings"]
        Libmoq["libmoq<br/>C ABI"]
        Ffi["moq-ffi<br/>UniFFI bindings"]
        Py["moq-lite Python<br/>Pythonic wrapper"]
    end

    subgraph Media["Media Layer"]
        Hang["hang<br/>catalog and media tracks"]
        Mux["moq-mux<br/>fMP4, CMAF, HLS import/export"]
        Codec["moq-codec<br/>codec helpers"]
        Msf["moq-msf<br/>media stream format"]
    end

    subgraph Core["Core MoQ Layer"]
        Lite["moq-lite<br/>broadcasts, tracks, groups, frames"]
        Token["moq-token<br/>JWT auth claims"]
        Native["moq-native<br/>native client/server helpers"]
        WebTransport["web-transport<br/>HTTP/3 WebTransport plumbing"]
    end

    subgraph Runtime["Transport Runtime"]
        Quinn["quinn / quiche<br/>QUIC"]
        Iroh["iroh optional<br/>P2P discovery and QUIC"]
        Tokio["tokio<br/>async runtime"]
        TLS["rustls<br/>TLS certificates"]
    end

    Apps --> Media
    Apps --> Core
    Bindings --> Core
    Py --> Ffi
    Ffi --> Lite
    Libmoq --> Lite
    Media --> Lite
    Relay --> Token
    Relay --> Native
    Cli --> Mux
    Native --> WebTransport
    Native --> Quinn
    Native --> Iroh
    WebTransport --> Quinn
    Quinn --> TLS
    Quinn --> Tokio
    Iroh --> Tokio
```

## Data Flow

```mermaid
sequenceDiagram
    participant Publisher as Publisher
    participant Relay as moq-relay
    participant Subscriber as Subscriber

    Publisher->>Relay: Connect over WebTransport or QUIC
    Subscriber->>Relay: Connect over WebTransport or QUIC
    Publisher->>Relay: Publish broadcast path
    Relay->>Subscriber: Announce broadcast
    Subscriber->>Relay: Subscribe to track
    Publisher->>Relay: Send groups and frames
    Relay->>Subscriber: Fan out live frames
    Relay->>Relay: Cache latest groups for late subscribers
```

## Package Map

| Area | Rust crates and packages | Role |
| --- | --- | --- |
| Core protocol | `rs/moq-lite` | Broadcast, track, group, frame, subscribe, publish, fetch, and announce primitives. |
| Native transport | `rs/moq-native`, `rs/web-transport` | Native QUIC/WebTransport client and server setup. |
| Relay | `rs/moq-relay` | Server that routes publications to subscribers, caches groups, and supports clustering. |
| Media | `rs/hang`, `rs/moq-mux`, `rs/moq-msf`, `rs/moq-codec` | Catalogs, encoded media tracks, muxing, demuxing, and codec-specific helpers. |
| Auth | `rs/moq-token`, `rs/moq-token-cli` | JWT signing, verification, and path-based publish/subscribe authorization. |
| Tooling | `rs/moq-cli`, `rs/moq-clock`, `rs/moq-boy`, `rs/moq-gst` | Command-line tools, demos, and GStreamer integration. |
| Bindings | `rs/libmoq`, `rs/moq-ffi`, `py/moq-lite` | C ABI, UniFFI bindings, and Python wrapper around the Rust implementation. |

## Python Integration

Python applications should normally import `moq_lite`. That package wraps `moq-ffi`, which is generated from the Rust `rs/moq-ffi` crate and uses the same Rust protocol stack as the relay.

```mermaid
flowchart LR
    PythonApp["Python app"] --> PyLite["moq_lite"]
    PyLite --> MoqFfi["moq-ffi native wheel"]
    MoqFfi --> RustCore["Rust moq-lite"]
    RustCore --> Relay["moq-relay"]
```
