# Using MOQ Transport in External Projects

This guide shows how to use the MOQ Transport library in your own Python projects.

## Installation

### From Local Directory

```bash
# Install from local path
pip install /path/to/moq-py

# Or for development (editable install)
pip install -e /path/to/moq-py
```

### From Git Repository

```bash
pip install git+https://github.com/your-org/moq-py.git
```

## Quick Start

### 1. Publisher Example

```python
import asyncio
from moq import MOQPublisher, FullTrackName, PublishedObject

async def main():
    # Create and connect publisher
    publisher = MOQPublisher(host="127.0.0.1", port=4443)
    await publisher.connect()
    
    # Define track
    track_name = FullTrackName(
        namespace=[b"my-namespace"],
        name=b"my-track"
    )
    
    # Publish track
    await publisher.publish(track_name)
    
    # Send objects
    for i in range(10):
        obj = PublishedObject(
            group_id=i,
            object_id=0,
            payload=b"Hello, World!"
        )
        await publisher.send_object(track_name, obj)
    
    # Cleanup
    await publisher.unpublish(track_name)
    publisher.disconnect()

asyncio.run(main())
```

### 2. Subscriber Example

```python
import asyncio
from moq import MOQSubscriber, FullTrackName, ReceivedObject

async def main():
    # Create subscriber
    subscriber = MOQSubscriber(host="127.0.0.1", port=4443)
    
    # Set up message handler
    def on_object(obj: ReceivedObject):
        message = obj.payload.decode('utf-8')
        print(f"Received: {message}")
    
    subscriber.set_handlers(on_object_received=on_object)
    
    # Connect and subscribe
    await subscriber.connect()
    
    track_name = FullTrackName(
        namespace=[b"my-namespace"],
        name=b"my-track"
    )
    
    await subscriber.subscribe(track_name)
    
    # Keep running
    await asyncio.sleep(60)
    
    # Cleanup
    await subscriber.unsubscribe(track_name)
    subscriber.disconnect()

asyncio.run(main())
```

### 3. Fetch Mode Example

```python
import asyncio
from moq import MOQSubscriber, FullTrackName

async def main():
    subscriber = MOQSubscriber(host="127.0.0.1", port=4443)
    
    def on_object(obj):
        print(f"Fetched: group={obj.group_id}, payload={obj.payload}")
    
    subscriber.set_handlers(on_object_received=on_object)
    await subscriber.connect()
    
    track_name = FullTrackName(
        namespace=[b"my-namespace"],
        name=b"my-track"
    )
    
    # Fetch from beginning (0,0) until latest
    request_id = await subscriber.fetch(track_name=track_name)
    
    # Or fetch from specific start
    # request_id = await subscriber.fetch(
    #     track_name=track_name,
    #     start_group=5
    # )
    
    # Or fetch specific range
    # request_id = await subscriber.fetch(
    #     track_name=track_name,
    #     start_group=0,
    #     start_object=0,
    #     end_group=10,
    #     end_object=100
    # )
    
    await asyncio.sleep(10)
    subscriber.disconnect()

asyncio.run(main())
```

## Import Options

### Option 1: Import from main package (Recommended)

```python
from moq import (
    MOQPublisher,
    MOQSubscriber,
    FullTrackName,
    ReceivedObject,
    PublishedObject,
)
```

### Option 2: Import from specific modules

```python
from moq.pub import MOQPublisher, PublishedObject
from moq.sub import MOQSubscriber, ReceivedObject
from moq.encoding import FullTrackName
from moq.messages import ObjectHeader, ObjectDatagram
```

### Option 3: Import for advanced usage

```python
# Transport layer
from moq.transport import QUICClient, QUICServer

# Session management
from moq.session import MOQSession, Role, SessionState

# Relay server
from moq.relay import MOQRelay

# Low-level encoding
from moq.encoding import VarInt, Parameters

# Low-level messages
from moq.messages import (
    SubscribeMessage,
    PublishMessage,
    FetchMessage,
    ObjectDatagram,
)
```

## Available Classes and Interfaces

### Publisher (`moq.pub`)

- `MOQPublisher` - Main publisher class
  - `connect()` - Connect to relay
  - `disconnect()` - Disconnect from relay
  - `publish(track_name)` - Publish a track
  - `unpublish(track_name)` - Unpublish a track
  - `send_object(track_name, obj)` - Send an object
  - `set_handlers()` - Set event handlers

- `PublishedObject` - Object to publish
  - `group_id` - Group identifier
  - `object_id` - Object identifier
  - `payload` - Object payload (bytes)
  - `publisher_priority` - Priority (0-255)

### Subscriber (`moq.sub`)

- `MOQSubscriber` - Main subscriber class
  - `connect()` - Connect to relay
  - `disconnect()` - Disconnect from relay
  - `subscribe(track_name)` - Subscribe to a track
  - `unsubscribe(track_name)` - Unsubscribe from a track
  - `fetch(track_name, ...)` - Fetch historical objects
  - `set_handlers()` - Set event handlers

- `ReceivedObject` - Received object
  - `group_id` - Group identifier
  - `object_id` - Object identifier
  - `payload` - Object payload (bytes)
  - `track_alias` - Track alias

### Encoding (`moq.encoding`)

- `FullTrackName` - Track name with namespace
  - `namespace` - List of namespace components (list of bytes)
  - `name` - Track name (bytes)

- `VarInt` - Variable-length integer encoding
- `Parameters` - Key-value parameters

### Messages (`moq.messages`)

- `MessageType` - Message type constants
- `ObjectDatagram` - Datagram object message
- `ObjectHeader` - Object header
- `SubscribeMessage`, `PublishMessage`, `FetchMessage` - Control messages

## Running the Example

```bash
# Terminal 1: Start relay
cd /path/to/moq-py
python -m cases.relay

# Terminal 2: Run publisher
python examples/external_usage_example.py publisher

# Terminal 3: Run subscriber
python examples/external_usage_example.py subscriber

# Terminal 4: Run fetch
python examples/external_usage_example.py fetch
```

## Dependencies

The package requires:
- Python >= 3.8
- aioquic (for QUIC transport)
- Other dependencies listed in requirements.txt

## Notes

- All operations are async/await based
- Handlers are set via `set_handlers()` method
- Remember to call `disconnect()` when done
- The package uses draft-ietf-moq-transport-17
