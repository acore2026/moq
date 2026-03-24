"""
MOQ Transport - Python implementation of draft-ietf-moq-transport-17

This package provides a Python implementation of the Media over QUIC Transport (MOQT) protocol.

Key Components:
- encoding: Variable-length integer and data structure encoding
- messages: Control and data message types
- transport: QUIC transport layer
- session: Session management
- relay: Caching relay implementation
- pub: Publisher functionality
- sub: Subscriber functionality

Example Usage:
    # Publisher
    from moq.pub import MOQPublisher
    from moq.encoding import FullTrackName
    
    publisher = MOQPublisher("relay.example.com", 4433)
    await publisher.connect()
    
    track_name = FullTrackName([b"example", b"namespace"], b"track1")
    await publisher.publish(track_name)
    
    # Send objects
    from moq.pub import PublishedObject
    obj = PublishedObject(group_id=1, object_id=1, payload=b"Hello, World!")
    await publisher.send_object(track_name, obj)
    
    # Subscriber
    from moq.sub import MOQSubscriber
    
    subscriber = MOQSubscriber("relay.example.com", 4433)
    await subscriber.connect()
    
    def on_object(obj):
        print(f"Received: group={obj.group_id}, object={obj.object_id}")
    
    subscriber.set_handlers(on_object_received=on_object)
    await subscriber.subscribe(track_name)
"""

__version__ = "0.1.0"
__author__ = "MOQ Implementation"

# Import main components for convenience
from moq.encoding import (
    VarInt,
    encode_bytes,
    decode_bytes,
    KeyValuePair,
    Parameters,
    Location,
    FullTrackName,
    TrackAlias,
)

from moq.messages import (
    MessageType,
    ErrorCode,
    SubscribeFilter,
    GroupOrder,
    SetupMessage,
    SubscribeMessage,
    SubscribeOkMessage,
    PublishMessage,
    PublishOkMessage,
    ObjectStatus,
    ObjectHeader,
    ObjectDatagram,
    FetchMessage,
    FetchOkMessage,
)

from moq.session import (
    MOQSession,
    Role,
    SessionState,
)

from moq.pub import (
    MOQPublisher,
    PublishedObject,
)

from moq.sub import (
    MOQSubscriber,
    ReceivedObject,
)

from moq.relay import (
    MOQRelay,
    ClientSession,
)

__all__ = [
    # Version
    '__version__',

    # Encoding
    'VarInt',
    'encode_bytes',
    'decode_bytes',
    'KeyValuePair',
    'Parameters',
    'Location',
    'FullTrackName',
    'TrackAlias',

    # Messages
    'MessageType',
    'ErrorCode',
    'SubscribeFilter',
    'GroupOrder',
    'SetupMessage',
    'SubscribeMessage',
    'SubscribeOkMessage',
    'PublishMessage',
    'PublishOkMessage',
    'ObjectStatus',
    'ObjectHeader',
    'ObjectDatagram',
    'FetchMessage',
    'FetchOkMessage',

    # Session
    'MOQSession',
    'Role',
    'SessionState',

    # Publisher
    'MOQPublisher',
    'PublishedObject',

    # Subscriber
    'MOQSubscriber',
    'ReceivedObject',

    # Relay
    'MOQRelay',
    'ClientSession',
]
