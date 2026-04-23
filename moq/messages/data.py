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
    NON_EXISTENT = 0x01  # Legacy/non-standard.
    END_OF_GROUP = 0x03
    END_OF_TRACK = 0x04
    END_OF_SUBGROUP = 0x05  # Legacy/non-standard.


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
        
        if offset >= len(data):
            raise ValueError("Insufficient data for object publisher priority")
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

    This implements the draft-17 outer OBJECT_DATAGRAM wire shape with a
    leading Type field. The current implementation only emits the simple
    variant we actually use today: Track Alias + Group ID + Object ID +
    Publisher Priority + payload, or Track Alias + Group ID + Object ID +
    Publisher Priority + Object Status for status-only objects.
    """
    header: ObjectHeader
    payload: bytes
    extensions: Optional[bytes] = None

    TYPE_PAYLOAD = 0x00
    TYPE_STATUS = 0x20
    TYPE_END_OF_GROUP = 0x02
    
    def encode(self) -> bytes:
        """Encode object datagram."""
        data = bytearray()
        if self.header.object_status == ObjectStatus.NORMAL and self.payload:
            datagram_type = self.TYPE_PAYLOAD
        elif self.header.object_status == ObjectStatus.END_OF_GROUP and not self.payload:
            datagram_type = self.TYPE_END_OF_GROUP
        else:
            datagram_type = self.TYPE_STATUS

        data.extend(VarInt.encode(datagram_type))
        data.extend(VarInt.encode(self.header.track_alias))
        data.extend(VarInt.encode(self.header.group_id))
        data.extend(VarInt.encode(self.header.object_id))
        data.append(self.header.publisher_priority)

        if datagram_type == self.TYPE_STATUS:
            data.extend(VarInt.encode(self.header.object_status))
        else:
            data.extend(self.payload)
        if self.extensions:
            data.extend(self.extensions)
        return bytes(data)
    
    @staticmethod
    def decode(data: bytes, offset: int = 0) -> Tuple['ObjectDatagram', int]:
        """Decode object datagram from bytes."""
        start_offset = offset

        datagram_type, consumed = VarInt.decode(data, offset)
        offset += consumed

        if datagram_type & 0x01:
            raise ValueError("OBJECT_DATAGRAM properties are not supported")
        if datagram_type & 0x10:
            raise ValueError(f"Invalid OBJECT_DATAGRAM type: {datagram_type:#x}")
        if datagram_type & 0x04:
            object_id = 0
        else:
            track_alias, consumed = VarInt.decode(data, offset)
            offset += consumed
            group_id, consumed = VarInt.decode(data, offset)
            offset += consumed
            object_id, consumed = VarInt.decode(data, offset)
            offset += consumed
            if datagram_type & 0x08:
                raise ValueError("OBJECT_DATAGRAM default priority is not supported")
            if offset >= len(data):
                raise ValueError("Insufficient data for datagram publisher priority")
            publisher_priority = data[offset]
            offset += 1
        if datagram_type & 0x04:
            track_alias, consumed = VarInt.decode(data, offset)
            offset += consumed
            group_id, consumed = VarInt.decode(data, offset)
            offset += consumed
            if datagram_type & 0x08:
                raise ValueError("OBJECT_DATAGRAM default priority is not supported")
            if offset >= len(data):
                raise ValueError("Insufficient data for datagram publisher priority")
            publisher_priority = data[offset]
            offset += 1

        payload = b""
        object_status = ObjectStatus.NORMAL
        if datagram_type & 0x20:
            status_value, consumed = VarInt.decode(data, offset)
            offset += consumed
            object_status = ObjectStatus(status_value)
        else:
            payload = data[offset:]
            offset = len(data)
            if datagram_type & 0x02:
                object_status = ObjectStatus.END_OF_GROUP

        extensions = None
        if datagram_type & 0x20 and offset < len(data):
            extensions = data[offset:]
            offset = len(data)
        
        return ObjectDatagram(
            header=ObjectHeader(
                track_alias=track_alias,
                group_id=group_id,
                object_id=object_id,
                publisher_priority=publisher_priority,
                object_status=object_status,
            ),
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
        
        if offset >= len(data):
            raise ValueError("Insufficient data for subgroup publisher priority")
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
    Format: Object ID Delta, [Properties], Payload Length, [Status], [Payload]
    """
    object_id: int
    payload: bytes
    object_status: ObjectStatus = ObjectStatus.NORMAL
    
    def encode(self, previous_object_id: Optional[int] = None) -> bytes:
        """Encode subgroup object."""
        if previous_object_id is None:
            object_id_delta = self.object_id
        else:
            object_id_delta = self.object_id - previous_object_id - 1
            if object_id_delta < 0:
                raise ValueError("Subgroup object_id must be strictly increasing")

        data = bytearray()
        data.extend(VarInt.encode(object_id_delta))

        payload_len = len(self.payload)
        if self.object_status != ObjectStatus.NORMAL:
            payload_len = 0

        data.extend(VarInt.encode(payload_len))
        if payload_len == 0:
            data.extend(VarInt.encode(self.object_status))
        else:
            data.extend(self.payload)
        return bytes(data)
    
    @staticmethod
    def decode(
        data: bytes,
        offset: int = 0,
        previous_object_id: Optional[int] = None,
    ) -> Tuple['SubgroupObject', int]:
        """Decode subgroup object from bytes."""
        start_offset = offset
        object_id_delta, consumed = VarInt.decode(data, offset)
        offset += consumed

        if previous_object_id is None:
            object_id = object_id_delta
        else:
            object_id = previous_object_id + object_id_delta + 1

        payload_len, consumed = VarInt.decode(data, offset)
        offset += consumed

        payload = b""
        object_status = ObjectStatus.NORMAL

        if payload_len == 0:
            status_value, consumed = VarInt.decode(data, offset)
            offset += consumed
            object_status = ObjectStatus(status_value)
        else:
            if offset + payload_len > len(data):
                raise ValueError("Insufficient data for subgroup payload")
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


@dataclass
class FetchObject:
    """
    Object serialization used after FETCH_HEADER on a fetch stream.

    This implements the simple standalone form we currently emit:
    explicit Group ID, Object ID, Publisher Priority and payload, with
    the datagram/subgroup-specific fields omitted.
    """

    group_id: int
    object_id: int
    publisher_priority: int
    payload: bytes
    subgroup_id: Optional[int] = None

    FLAGS_DATAGRAM_LIKE = 0x40
    FLAGS_GROUP_PRESENT = 0x08
    FLAGS_OBJECT_PRESENT = 0x04
    FLAGS_PRIORITY_PRESENT = 0x10

    def encode(self) -> bytes:
        flags = self.FLAGS_DATAGRAM_LIKE | self.FLAGS_GROUP_PRESENT | self.FLAGS_OBJECT_PRESENT
        flags |= self.FLAGS_PRIORITY_PRESENT

        data = bytearray()
        data.extend(VarInt.encode(flags))
        data.extend(VarInt.encode(self.group_id))
        data.extend(VarInt.encode(self.object_id))
        data.append(self.publisher_priority)
        data.extend(VarInt.encode(len(self.payload)))
        data.extend(self.payload)
        return bytes(data)

    @staticmethod
    def decode(data: bytes, offset: int = 0) -> Tuple['FetchObject', int]:
        start_offset = offset
        flags, consumed = VarInt.decode(data, offset)
        offset += consumed

        if flags in (0x8C, 0x10C):
            raise ValueError("Fetch end-of-range markers are not supported")
        if flags & 0x20:
            raise ValueError("Fetch object properties are not supported")
        if not (flags & FetchObject.FLAGS_GROUP_PRESENT):
            raise ValueError("Fetch object without explicit group_id is not supported")
        if not (flags & FetchObject.FLAGS_OBJECT_PRESENT):
            raise ValueError("Fetch object without explicit object_id is not supported")
        if not (flags & FetchObject.FLAGS_PRIORITY_PRESENT):
            raise ValueError("Fetch object without explicit priority is not supported")

        subgroup_encoding = flags & 0x03
        subgroup_id = None

        group_id, consumed = VarInt.decode(data, offset)
        offset += consumed

        if not (flags & 0x40):
            if subgroup_encoding != 0x03:
                raise ValueError("Fetch object subgroup delta encoding is not supported")
            subgroup_id, consumed = VarInt.decode(data, offset)
            offset += consumed

        object_id, consumed = VarInt.decode(data, offset)
        offset += consumed

        if offset >= len(data):
            raise ValueError("Insufficient data for fetch object priority")
        publisher_priority = data[offset]
        offset += 1

        payload_len, consumed = VarInt.decode(data, offset)
        offset += consumed
        if offset + payload_len > len(data):
            raise ValueError("Insufficient data for fetch object payload")
        payload = data[offset:offset + payload_len]
        offset += payload_len

        return FetchObject(
            group_id=group_id,
            subgroup_id=subgroup_id,
            object_id=object_id,
            publisher_priority=publisher_priority,
            payload=payload,
        ), offset - start_offset


# Stream type identifiers
class StreamType(IntEnum):
    """Unidirectional stream types."""
    OBJECT_DATAGRAM = 0x00
    SUBGROUP_HEADER = 0x14
    FETCH_HEADER = 0x05


def is_subgroup_stream_type(stream_type: int) -> bool:
    """Return True when the stream type is a valid draft-17 subgroup header type."""
    if not (0x10 <= stream_type <= 0x1F or 0x30 <= stream_type <= 0x3F):
        return False
    subgroup_id_mode = (stream_type & 0x06) >> 1
    return subgroup_id_mode != 0x03
