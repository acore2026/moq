"""
MOQ Transport - Data Messages (Objects, Streams, Datagrams)
Implements data streaming as per draft-ietf-moq-transport-17 Section 10.
"""

from enum import IntEnum
from typing import Optional, Tuple
from dataclasses import dataclass
from moq.encoding import VarInt, encode_bytes, decode_bytes, KeyValuePair


class ObjectStatus(IntEnum):
    """Object status codes."""
    NORMAL = 0x00
    NON_EXISTENT = 0x01
    END_OF_GROUP = 0x02
    END_OF_TRACK = 0x03
    END_OF_SUBGROUP = 0x04


class ForwardingPreference(IntEnum):
    """Forwarding preference for objects."""
    SUBGROUP = 0x00
    DATAGRAM = 0x01
    TRACK = 0x02


@dataclass
class ObjectHeader:
    """
    Object Header for data transmission.
    Track Alias, Group ID, Object ID, Publisher Priority, Object Status
    """
    track_alias: int
    group_id: int
    object_id: int
    publisher_priority: int
    object_status: ObjectStatus = ObjectStatus.NORMAL
    
    def encode(self) -> bytes:
        """Encode object header."""
        header = VarInt.encode(self.track_alias)
        header += VarInt.encode(self.group_id)
        header += VarInt.encode(self.object_id)
        header += bytes([self.publisher_priority])
        if self.object_status != ObjectStatus.NORMAL:
            header += VarInt.encode(self.object_status)
        return header
    
    @staticmethod
    def decode(data: bytes, offset: int = 0) -> Tuple['ObjectHeader', int]:
        """Decode object header from bytes."""
        start_offset = offset
        track_alias, consumed = VarInt.decode(data, offset)
        offset += consumed
        
        group_id, consumed = VarInt.decode(data, offset)
        offset += consumed
        
        object_id, consumed = VarInt.decode(data, offset)
        offset += consumed
        
        publisher_priority = data[offset]
        offset += 1
        
        object_status = ObjectStatus.NORMAL
        if offset < len(data):
            try:
                status_val, consumed = VarInt.decode(data, offset)
                object_status = ObjectStatus(status_val)
                offset += consumed
            except:
                pass
        
        return ObjectHeader(
            track_alias=track_alias,
            group_id=group_id,
            object_id=object_id,
            publisher_priority=publisher_priority,
            object_status=object_status
        ), offset - start_offset


@dataclass
class ObjectDatagram:
    """
    Object sent as a datagram.
    Format: Object Header + Payload Length + Payload + Extensions
    """
    header: ObjectHeader
    payload: bytes
    extensions: Optional[bytes] = None
    
    def encode(self) -> bytes:
        """Encode object datagram."""
        data = self.header.encode()
        data += VarInt.encode(len(self.payload))
        data += self.payload
        if self.extensions:
            data += self.extensions
        return data
    
    @staticmethod
    def decode(data: bytes, offset: int = 0) -> Tuple['ObjectDatagram', int]:
        """Decode object datagram from bytes."""
        start_offset = offset
        
        header, consumed = ObjectHeader.decode(data, offset)
        offset += consumed
        
        payload_len, consumed = VarInt.decode(data, offset)
        offset += consumed
        
        payload = data[offset:offset + payload_len]
        offset += payload_len
        
        extensions = None
        if offset < len(data):
            extensions = data[offset:]
            offset = len(data)
        
        return ObjectDatagram(
            header=header,
            payload=payload,
            extensions=extensions
        ), offset - start_offset


@dataclass
class SubgroupHeader:
    """
    Subgroup Header for stream-based object delivery.
    Format: Track Alias, Group ID, Subgroup ID, Publisher Priority
    """
    track_alias: int
    group_id: int
    subgroup_id: int
    publisher_priority: int
    
    def encode(self) -> bytes:
        """Encode subgroup header."""
        header = VarInt.encode(self.track_alias)
        header += VarInt.encode(self.group_id)
        header += VarInt.encode(self.subgroup_id)
        header += bytes([self.publisher_priority])
        return header
    
    @staticmethod
    def decode(data: bytes, offset: int = 0) -> Tuple['SubgroupHeader', int]:
        """Decode subgroup header from bytes."""
        start_offset = offset
        track_alias, consumed = VarInt.decode(data, offset)
        offset += consumed
        
        group_id, consumed = VarInt.decode(data, offset)
        offset += consumed
        
        subgroup_id, consumed = VarInt.decode(data, offset)
        offset += consumed
        
        publisher_priority = data[offset]
        offset += 1
        
        return SubgroupHeader(
            track_alias=track_alias,
            group_id=group_id,
            subgroup_id=subgroup_id,
            publisher_priority=publisher_priority
        ), offset - start_offset


@dataclass
class SubgroupObject:
    """
    Object within a subgroup stream.
    Format: Object ID, [Object Status], [Payload Length], Payload
    """
    object_id: int
    payload: bytes
    object_status: ObjectStatus = ObjectStatus.NORMAL
    
    def encode(self) -> bytes:
        """Encode subgroup object."""
        data = VarInt.encode(self.object_id)
        if self.object_status != ObjectStatus.NORMAL:
            data += VarInt.encode(self.object_status)
        else:
            data += VarInt.encode(len(self.payload))
            data += self.payload
        return data
    
    @staticmethod
    def decode(data: bytes, offset: int = 0) -> Tuple['SubgroupObject', int]:
        """Decode subgroup object from bytes."""
        start_offset = offset
        object_id, consumed = VarInt.decode(data, offset)
        offset += consumed
        
        # Check for status or length
        next_val, consumed = VarInt.decode(data, offset)
        offset += consumed
        
        payload = b''
        object_status = ObjectStatus.NORMAL
        
        if next_val in (ObjectStatus.NON_EXISTENT, ObjectStatus.END_OF_GROUP, 
                        ObjectStatus.END_OF_TRACK, ObjectStatus.END_OF_SUBGROUP):
            object_status = ObjectStatus(next_val)
        else:
            # It's a length
            payload_len = next_val
            payload = data[offset:offset + payload_len]
            offset += payload_len
        
        return SubgroupObject(
            object_id=object_id,
            payload=payload,
            object_status=object_status
        ), offset - start_offset


@dataclass
class FetchHeader:
    """
    Header for fetch stream.
    Format: Subscribe ID
    """
    subscribe_id: int
    
    def encode(self) -> bytes:
        """Encode fetch header."""
        return VarInt.encode(self.subscribe_id)
    
    @staticmethod
    def decode(data: bytes, offset: int = 0) -> Tuple['FetchHeader', int]:
        """Decode fetch header from bytes."""
        subscribe_id, consumed = VarInt.decode(data, offset)
        return FetchHeader(subscribe_id), consumed


# Stream type identifiers
class StreamType(IntEnum):
    """Unidirectional stream types."""
    OBJECT_DATAGRAM = 0x00
    SUBGROUP_HEADER = 0x01
    FETCH_HEADER = 0x02
