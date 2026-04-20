"""
MOQ Transport - Location and track naming structures.
"""

from typing import Tuple, List
from .varint import VarInt, encode_bytes, decode_bytes


class Location:
    """
    Location identifies a particular Object in a Group within a Track.
    Format: {Group, Object}
    """
    
    def __init__(self, group: int, object_id: int):
        self.group = group
        self.object_id = object_id
    
    def encode(self) -> bytes:
        """Encode the location structure."""
        return VarInt.encode(self.group) + VarInt.encode(self.object_id)
    
    @staticmethod
    def decode(data: bytes, offset: int = 0) -> Tuple['Location', int]:
        """Decode a location from bytes."""
        group, consumed1 = VarInt.decode(data, offset)
        obj_id, consumed2 = VarInt.decode(data, offset + consumed1)
        return Location(group, obj_id), consumed1 + consumed2
    
    def __lt__(self, other: 'Location') -> bool:
        """Compare locations: A < B if A.Group < B.Group or (A.Group == B.Group and A.Object < B.Object)"""
        return self.group < other.group or (self.group == other.group and self.object_id < other.object_id)
    
    def __eq__(self, other) -> bool:
        if not isinstance(other, Location):
            return False
        return self.group == other.group and self.object_id == other.object_id
    
    def __hash__(self):
        return hash((self.group, self.object_id))
    
    def __repr__(self):
        return f"Location({{Group={self.group}, Object={self.object_id}}})"


class FullTrackName:
    """
    Full Track Name consisting of Track Namespace and Track Name.
    """
    
    MAX_FULL_TRACK_NAME_LENGTH = 4096
    MAX_NAMESPACE_FIELDS = 32
    
    def __init__(self, namespace: List[bytes], track_name: bytes):
        """
        Initialize a full track name.
        
        Args:
            namespace: List of namespace field bytes (0-32 fields)
            track_name: Track name bytes
        """
        if len(namespace) > self.MAX_NAMESPACE_FIELDS:
            raise ValueError(f"Too many namespace fields: {len(namespace)} > {self.MAX_NAMESPACE_FIELDS}")
        
        for field in namespace:
            if len(field) == 0:
                raise ValueError("Namespace field cannot be empty")
        
        self.namespace = namespace
        self.track_name = track_name
        
        # Validate total length
        total_length = sum(len(f) for f in namespace) + len(track_name)
        if total_length > self.MAX_FULL_TRACK_NAME_LENGTH:
            raise ValueError(f"Full track name too long: {total_length} > {self.MAX_FULL_TRACK_NAME_LENGTH}")
    
    def encode(self) -> bytes:
        """Encode the full track name."""
        encoded = VarInt.encode(len(self.namespace))
        for field in self.namespace:
            encoded += encode_bytes(field)
        encoded += encode_bytes(self.track_name)
        return encoded
    
    @staticmethod
    def decode(data: bytes, offset: int = 0) -> Tuple['FullTrackName', int]:
        """Decode a full track name from bytes."""
        field_count, consumed = VarInt.decode(data, offset)
        
        if field_count > FullTrackName.MAX_NAMESPACE_FIELDS:
            raise ValueError(f"Too many namespace fields: {field_count}")
        
        namespace = []
        for _ in range(field_count):
            field, field_consumed = decode_bytes(data, offset + consumed)
            if len(field) == 0:
                raise ValueError("Namespace field cannot be empty")
            namespace.append(field)
            consumed += field_consumed
        
        track_name, name_consumed = decode_bytes(data, offset + consumed)
        consumed += name_consumed
        
        return FullTrackName(namespace, track_name), consumed
    
    def to_string(self) -> str:
        """
        Convert to a safe string representation.
        Non-safe bytes are encoded as .xx hex.
        """
        def encode_bytes_safe(b: bytes) -> str:
            result = []
            for byte in b:
                if 0x61 <= byte <= 0x7A or 0x41 <= byte <= 0x5A or 0x30 <= byte <= 0x39 or byte == 0x5F:
                    result.append(chr(byte))
                else:
                    result.append(f".{byte:02x}")
            return ''.join(result)
        
        parts = [encode_bytes_safe(field) for field in self.namespace]
        parts.append(encode_bytes_safe(self.track_name))
        return '--'.join(parts)
    
    def __eq__(self, other) -> bool:
        if not isinstance(other, FullTrackName):
            return False
        return self.namespace == other.namespace and self.track_name == other.track_name
    
    def __hash__(self):
        return hash((tuple(self.namespace), self.track_name))
    
    def normalize(self) -> 'FullTrackName':
        """
        Normalize the full track name by removing empty namespace fields.
        This ensures consistent comparison between track names that may have
        been constructed differently (e.g., with/without leading/trailing slashes).
        """
        # Filter out empty namespace fields
        normalized_namespace = [field for field in self.namespace if len(field) > 0]
        return FullTrackName(normalized_namespace, self.track_name)
    
    def __repr__(self):
        ns_str = '/'.join(f.decode('utf-8', errors='replace') for f in self.namespace)
        name_str = self.track_name.decode('utf-8', errors='replace')
        return f"FullTrackName(namespace=[{ns_str}], track_name={name_str})"


class TrackAlias:
    """Track alias for identifying tracks within a session."""
    
    def __init__(self, alias: int):
        self.alias = alias
    
    def encode(self) -> bytes:
        return VarInt.encode(self.alias)
    
    @staticmethod
    def decode(data: bytes, offset: int = 0) -> Tuple['TrackAlias', int]:
        alias, consumed = VarInt.decode(data, offset)
        return TrackAlias(alias), consumed
    
    def __eq__(self, other):
        if not isinstance(other, TrackAlias):
            return False
        return self.alias == other.alias
    
    def __hash__(self):
        return hash(self.alias)
    
    def __repr__(self):
        return f"TrackAlias({self.alias})"
