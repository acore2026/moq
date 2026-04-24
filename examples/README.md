# Examples

This directory contains the maintained MOQ example flows. The examples focus on
relay startup, publishing, subscribing, fetching cached history, reconnect
behavior, and a single-file quick start.

## Example Overview

| File | Goal | Typical usage |
| --- | --- | --- |
| `relay_example.py` | Start a relay and initialize disk-backed cache | Local development and end-to-end testing |
| `dual_transport_relay_example.py` | Start one relay instance with native QUIC and WebTransport on the same port | Mixed-client transport validation |
| `webtransport_relay_example.py` | Start a relay over HTTPS + WebTransport | Browser-friendly MOQ transport validation |
| `publisher_example.py` | Publish objects over stream and datagram transport | Publisher reference flow |
| `webtransport_publisher_example.py` | Publish over a WebTransport session | MOQ over WebTransport reference flow |
| `subscriber_example.py` | Subscribe to a track and receive live objects | Subscriber reference flow |
| `webtransport_subscriber_example.py` | Subscribe over a WebTransport session | End-to-end WebTransport receive validation |
| `video_publisher_example.py` | Generate a live timestamped H.264 test stream and publish it as stream objects | Real-time video stream validation |
| `camera_publisher_example.py` | Capture a Windows laptop webcam with ffmpeg DirectShow and publish it as stream objects | Browser live camera validation on Windows |
| `video_subscriber_example.py` | Reassemble the streamed fragmented MP4 into `receive.mp4` and preview it with `ffplay` | End-to-end live video receive validation |
| `fetch_example.py` | Fetch historical objects by range | Cache and history validation |
| `reconnection_example.py` | Reconnect and continue with relay-assisted recovery | Recovery and continuity testing |
| `basic_example.py` | Run relay, publisher, and subscriber in one file | Fast environment smoke test |

## Recommended Validation Order

1. Start `relay_example.py`.
2. Run `publisher_example.py` and confirm objects are published.
3. Run `subscriber_example.py` and confirm live delivery.
4. Run `fetch_example.py` after some objects exist in relay cache.
5. Run `reconnection_example.py` to validate cache-backed recovery.
6. Run `video_subscriber_example.py` and `video_publisher_example.py` to validate large-object stream delivery.
7. Run `dual_transport_relay_example.py` when you want one relay instance to serve QUIC and WebTransport clients at the same time.
8. Run the `webtransport_*_example.py` trio to validate MOQ over WebTransport.
9. Use `basic_example.py` when you want a fast one-process demo.

## Example Details

### `relay_example.py`

- Purpose: start the MOQ relay and reset the cache directory on startup.
- Best for: local development, integration testing, and cache behavior checks.
- Checkpoints: the relay should listen successfully and begin from a clean cache state.

### `publisher_example.py`

- Purpose: publish a sample track and send a sequence of objects.
- Best for: validating `PUBLISH`, object encoding, and stream/datagram delivery paths.
- Checkpoints: publish acknowledgement, incrementing object identifiers, and clean unpublish.

### `webtransport_relay_example.py`

- Purpose: start the relay on an HTTPS endpoint and accept WebTransport sessions on `/moq`.
- Best for: validating MOQT Section 3.1.1 style session establishment in this repo.
- Checkpoints: relay starts with TLS, accepts extended CONNECT, and forwards MOQ streams/datagrams through the session.

### `dual_transport_relay_example.py`

- Purpose: start one relay instance with native QUIC and WebTransport multiplexed on the same UDP port.
- Best for: validating mixed deployments where native clients and browser-facing clients share one relay state and cache.
- Checkpoints: one port accepts both ALPNs, both transports can attach to the same relay, and published objects are visible across transports.

### `webtransport_publisher_example.py`

- Purpose: publish a sample track over a WebTransport session.
- Best for: validating publisher-side control stream setup plus stream/datagram delivery over WebTransport.
- Checkpoints: WebTransport session establishment, publish acknowledgement, and live object delivery.

### `webtransport_subscriber_example.py`

- Purpose: subscribe to a sample track over a WebTransport session and print incoming objects.
- Best for: validating subscriber-side control stream/data stream separation over WebTransport.
- Checkpoints: subscription accepted and objects continue arriving over the same session.

### `subscriber_example.py`

- Purpose: subscribe to a sample track and print incoming objects.
- Best for: validating `SUBSCRIBE`, callback wiring, and live object reception.
- Checkpoints: successful subscribe callback and object receive callback activity.

### `video_publisher_example.py`

- Purpose: use `ffmpeg` to generate a live timestamped test pattern and publish it as H.264 fragmented MP4 over MOQ.
- Best for: validating real-time stream delivery, QUIC stream fragmentation handling, and live publisher behavior without relying on a source file.
- Checkpoints: metadata object sent first, live media chunks follow in order, and the relay forwards the subgroup stream correctly.

### `camera_publisher_example.py`

- Purpose: use `ffmpeg` DirectShow to capture a Windows laptop webcam and publish it as H.264 fragmented MP4 over MOQ.
- Best for: validating browser live camera playback with `video_webtransport_subscriber_example.py`.
- Checkpoints: confirm the default `Integrated Camera` DirectShow device exists, metadata is sent first, and live camera fragments follow continuously.
- Camera discovery: run `ffmpeg -list_devices true -f dshow -i dummy` on Windows if you need to verify the camera device name.

### `video_subscriber_example.py`

- Purpose: subscribe to the live video track, reconstruct `receive.mp4`, and try to preview the live stream with `ffplay`.
- Best for: validating incremental object parsing on the subscriber side and end-to-end live media reconstruction.
- Checkpoints: metadata arrives first, live chunks are written in order, `ffplay` can preview the stream when available, and `ffprobe` can inspect the reconstructed `receive.mp4`.

### `fetch_example.py`

- Purpose: fetch historical objects from the relay cache.
- Best for: validating `FETCH`, replay behavior, and cached object reads.
- Checkpoints: run it after publication and confirm historical objects are returned.

### `reconnection_example.py`

- Purpose: simulate disconnect, reconnect, and cache-assisted continuity.
- Best for: validating relay cache behavior after transient network loss.
- Checkpoints: objects published before a disconnect remain available after reconnect.

### `basic_example.py`

- Purpose: demonstrate relay, publisher, and subscriber roles in one file.
- Best for: first-run validation and quick local smoke tests.
- Checkpoints: useful as a demo script, but not as a project integration template.

## External Project Usage

Use the package directly from your own application rather than relying on a
dedicated integration example script.

### Installation

```bash
pip install /path/to/moq-py
```

For development:

```bash
pip install -e /path/to/moq-py
```

### Recommended Imports

```python
from moq import (
    MOQPublisher,
    MOQSubscriber,
    FullTrackName,
    PublishedObject,
    ReceivedObject,
)
```

Advanced imports are also available from `moq.pub`, `moq.sub`,
`moq.encoding`, `moq.messages`, `moq.transport`, `moq.session`, and
`moq.relay`.

### Minimal Publisher

```python
import asyncio
from moq import MOQPublisher, FullTrackName, PublishedObject


async def main():
    publisher = MOQPublisher("127.0.0.1", 4443)
    await publisher.connect()

    track_name = FullTrackName(namespace=[b"my-namespace"], name=b"my-track")
    await publisher.publish(track_name)

    for i in range(10):
        obj = PublishedObject(
            group_id=i,
            object_id=0,
            payload=b"Hello, World!",
        )
        await publisher.send_object(track_name, obj)

    await publisher.unpublish(track_name)
    publisher.disconnect()


asyncio.run(main())
```

### Minimal Subscriber

```python
import asyncio
from moq import MOQSubscriber, FullTrackName, ReceivedObject


async def main():
    subscriber = MOQSubscriber("127.0.0.1", 4443)

    def on_object(obj: ReceivedObject):
        print(obj.payload.decode("utf-8"))

    subscriber.set_handlers(on_object_received=on_object)
    await subscriber.connect()

    track_name = FullTrackName(namespace=[b"my-namespace"], name=b"my-track")
    await subscriber.subscribe(track_name)

    await asyncio.sleep(60)
    await subscriber.unsubscribe(track_name)
    subscriber.disconnect()


asyncio.run(main())
```

### Minimal Fetch Client

```python
import asyncio
from moq import MOQSubscriber, FullTrackName


async def main():
    subscriber = MOQSubscriber("127.0.0.1", 4443)

    def on_object(obj):
        print(f"Fetched group={obj.group_id}: {obj.payload!r}")

    subscriber.set_handlers(on_object_received=on_object)
    await subscriber.connect()

    track_name = FullTrackName(namespace=[b"my-namespace"], name=b"my-track")
    await subscriber.fetch(track_name=track_name)

    await asyncio.sleep(10)
    subscriber.disconnect()


asyncio.run(main())
```

## Running the Examples

Run examples from the repository root:

```bash
python examples/relay_example.py
python examples/dual_transport_relay_example.py
python examples/publisher_example.py
python examples/subscriber_example.py
python examples/webtransport_relay_example.py
python examples/webtransport_publisher_example.py
python examples/webtransport_subscriber_example.py
python examples/video_publisher_example.py
python examples/video_subscriber_example.py
python examples/fetch_example.py
```

## Notes

- Example scripts resolve the repository root automatically, so they can be run
  from the repo root without manual `sys.path` edits.
- When adding new examples, update this file instead of creating separate
  example-index Markdown files.
