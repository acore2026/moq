"""
MOQ Object and Data Structures
Implements MOQ object model for media transmission
"""

from dataclasses import dataclass, field
from typing import Optional, Any
import time


@dataclass
class MOQObject:
    """
    MOQ Object representing a media object.
    
    Attributes:
        track_alias: Unique track identifier (varint)
        group_id: Group identifier for grouping objects
        object_id: Object identifier within group
        send_order: Priority for sending (lower = higher priority)
        payload: Binary payload data
        extensions: Optional extension data
        timestamp: Creation timestamp
    """
    
    track_alias: int
    group_id: int
    object_id: int
    send_order: int = 0
    payload: bytes = field(default_factory=bytes)
    extensions: dict = field(default_factory=dict)
    timestamp: float = field(default_factory=time.time)
    
    # Internal tracking
    subgroups: Optional[list] = None
    
    def __post_init__(self):
        """Validate object fields."""
        if self.track_alias < 0:
            raise ValueError("track_alias must be non-negative")
        if self.group_id < 0:
            raise ValueError("group_id must be non-negative")
        if self.object_id < 0:
            raise ValueError("object_id must be non-negative")
    
    @property
    def object_size(self) -> int:
        """Return the total size of the object in bytes."""
        return len(self.payload)
    
    def to_dict(self) -> dict:
        """Convert object to dictionary."""
        return {
            'track_alias': self.track_alias,
            'group_id': self.group_id,
            'object_id': self.object_id,
            'send_order': self.send_order,
            'payload_length': len(self.payload),
            'extensions': self.extensions,
            'timestamp': self.timestamp,
        }
    
    def __repr__(self) -> str:
        return (f"MOQObject(track_alias={self.track_alias}, "
                f"group_id={self.group_id}, object_id={self.object_id}, "
                f"payload_size={len(self.payload)})")


@dataclass
class MOQObjectHeader:
    """
    MOQ Object Header for stream delivery.
    Used when sending objects over QUIC streams.
    """
    
    track_alias: int
    group_id: int
    object_id: int
    publisher_priority: int = 0
    subgroups: Optional[list] = None
    
    def encode(self) -> bytes:
        """Encode header to bytes."""
        from moq.protocol.varint import encode_varint, encode_bytes
        
        result = encode_varint(self.track_alias)
        result += encode_varint(self.group_id)
        result += encode_varint(self.object_id)
        result += encode_varint(self.publisher_priority)
        
        if self.subgroups:
            result += encode_varint(len(self.subgroups))
            for sg in self.subgroups:
                result += encode_bytes(sg)
        
        return result


@dataclass
class MOQTrack:
    """
    MOQ Track representing a media track.
    
    Attributes:
        namespace: Track namespace tuple
        name: Track name
        alias: Track alias for efficient reference
        publisher_priority: Default priority for objects
    """
    
    namespace: tuple[str, ...]
    name: str
    alias: Optional[int] = None
    publisher_priority: int = 128
    
    def __post_init__(self):
        """Validate track fields."""
        if not self.name:
            raise ValueError("Track name cannot be empty")
    
    def full_name(self) -> str:
        """Return full track name with namespace."""
        return "/".join(self.namespace) + "/" + self.name
    
    def __hash__(self) -> int:
        return hash(self.full_name())
    
    def __eq__(self, other) -> bool:
        if not isinstance(other, MOQTrack):
            return False
        return self.full_name() == other.full_name()


@dataclass
class MOQObjectStatus:
    """
    Object status codes for MOQ objects.
    """
    
    NORMAL: int = 0x00
    GROUP_NOT_EXIST: int = 0x01
    END_OF_GROUP: int = 0x02
    END_OF_TRACK: int = 0x03


class MOQObjectBuilder:
    """
    Builder for constructing MOQ Objects with proper validation.
    """
    
    def __init__(self):
        self._track_alias: Optional[int] = None
        self._group_id: Optional[int] = None
        self._object_id: Optional[int] = None
        self._send_order: int = 0
        self._payload: bytes = b''
        self._extensions: dict = {}
    
    def track_alias(self, alias: int) -> 'MOQObjectBuilder':
        """Set track alias."""
        self._track_alias = alias
        return self
    
    def group_id(self, group_id: int) -> 'MOQObjectBuilder':
        """Set group ID."""
        self._group_id = group_id
        return self
    
    def object_id(self, object_id: int) -> 'MOQObjectBuilder':
        """Set object ID."""
        self._object_id = object_id
        return self
    
    def send_order(self, order: int) -> 'MOQObjectBuilder':
        """Set send order (priority)."""
        self._send_order = order
        return self
    
    def payload(self, data: bytes) -> 'MOQObjectBuilder':
        """Set payload."""
        self._payload = data
        return self
    
    def extension(self, key: int, value: bytes) -> 'MOQObjectBuilder':
        """Add extension."""
        self._extensions[key] = value
        return self
    
    def build(self) -> MOQObject:
        """Build and return the MOQObject."""
        if self._track_alias is None:
            raise ValueError("track_alias is required")
        if self._group_id is None:
            raise ValueError("group_id is required")
        if self._object_id is None:
            raise ValueError("object_id is required")
        
        return MOQObject(
            track_alias=self._track_alias,
            group_id=self._group_id,
            object_id=self._object_id,
            send_order=self._send_order,
            payload=self._payload,
            extensions=self._extensions,
        )
