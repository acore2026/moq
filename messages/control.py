"""
MOQ Transport - Control Message Types
Implements control messages as per draft-ietf-moq-transport-17 Section 9.
"""

from enum import IntEnum
from typing import Optional, Tuple, List
from dataclasses import dataclass
from moq.encoding import VarInt, encode_bytes, decode_bytes, Parameters, Location, FullTrackName


# Message type constants
class MessageType(IntEnum):
    """MOQT Control Message Types."""
    # Session messages
    SETUP = 0x01
    GOAWAY = 0x10
    
    # Request messages
    REQUEST_OK = 0x02
    REQUEST_ERROR = 0x03
    
    # Subscribe messages
    SUBSCRIBE = 0x04
    SUBSCRIBE_OK = 0x05
    REQUEST_UPDATE = 0x06
    
    # Publish messages
    PUBLISH = 0x07
    PUBLISH_OK = 0x08
    PUBLISH_DONE = 0x09
    
    # Fetch messages
    FETCH = 0x0A
    FETCH_OK = 0x0B
    
    # Status messages
    TRACK_STATUS = 0x0C
    
    # Namespace messages
    PUBLISH_NAMESPACE = 0x0D
    NAMESPACE = 0x0E
    NAMESPACE_DONE = 0x0F
    SUBSCRIBE_NAMESPACE = 0x11
    PUBLISH_BLOCKED = 0x12


# Error codes
class ErrorCode(IntEnum):
    """MOQT Error Codes."""
    INTERNAL_ERROR = 0x00
    UNAUTHORIZED = 0x01
    PROTOCOL_VIOLATION = 0x02
    DUPLICATE_TRACK_ALIAS = 0x03
    PARAMETER_LENGTH_MISMATCH = 0x04
    GOAWAY_TIMEOUT = 0x10
    KEY_VALUE_FORMATTING_ERROR = 0xF0


class SubscribeFilter(IntEnum):
    """Subscription filter types."""
    NONE = 0x00
    LATEST_GROUP = 0x01
    LATEST_OBJECT = 0x02
    ABSOLUTE_START = 0x03
    ABSOLUTE_RANGE = 0x04


class GroupOrder(IntEnum):
    """Group ordering for delivery."""
    ASCENDING = 0x00
    DESCENDING = 0x01


@dataclass
class ReasonPhrase:
    """Reason phrase for errors."""
    MAX_LENGTH = 1024
    
    text: str
    
    def encode(self) -> bytes:
        text_bytes = self.text.encode('utf-8')
        if len(text_bytes) > self.MAX_LENGTH:
            raise ValueError(f"Reason phrase too long: {len(text_bytes)} > {self.MAX_LENGTH}")
        return encode_bytes(text_bytes)
    
    @staticmethod
    def decode(data: bytes, offset: int = 0) -> Tuple['ReasonPhrase', int]:
        text_bytes, consumed = decode_bytes(data, offset)
        return ReasonPhrase(text_bytes.decode('utf-8')), consumed


# Setup messages
@dataclass
class SetupMessage:
    """SETUP message for session initialization."""
    version: int
    role: int  # 0x01 = publisher, 0x02 = subscriber, 0x03 = pubsub
    parameters: Parameters
    
    def encode(self) -> bytes:
        payload = VarInt.encode(self.version)
        payload += VarInt.encode(self.role)
        payload += self.parameters.encode()
        return VarInt.encode(MessageType.SETUP) + encode_bytes(payload)
    
    @staticmethod
    def decode(data: bytes, offset: int = 0) -> Tuple['SetupMessage', int]:
        version, consumed1 = VarInt.decode(data, offset)
        role, consumed2 = VarInt.decode(data, offset + consumed1)
        params, consumed3 = Parameters.decode(data, offset + consumed1 + consumed2)
        return SetupMessage(version, role, params), consumed1 + consumed2 + consumed3


@dataclass
class GoAwayMessage:
    """GOAWAY message for session termination."""
    new_session_uri: str
    
    def encode(self) -> bytes:
        payload = encode_bytes(self.new_session_uri.encode('utf-8'))
        return VarInt.encode(MessageType.GOAWAY) + encode_bytes(payload)
    
    @staticmethod
    def decode(data: bytes, offset: int = 0) -> Tuple['GoAwayMessage', int]:
        uri_bytes, consumed = decode_bytes(data, offset)
        return GoAwayMessage(uri_bytes.decode('utf-8')), consumed


# Request response messages
@dataclass
class RequestOkMessage:
    """REQUEST_OK message."""
    request_id: int
    expires: Optional[int] = None  # milliseconds
    
    def encode(self) -> bytes:
        payload = VarInt.encode(self.request_id)
        if self.expires is not None:
            payload += VarInt.encode(self.expires)
        return VarInt.encode(MessageType.REQUEST_OK) + encode_bytes(payload)
    
    @staticmethod
    def decode(data: bytes, offset: int = 0) -> Tuple['RequestOkMessage', int]:
        request_id, consumed = VarInt.decode(data, offset)
        # Check if expires field exists
        if offset + consumed < len(data):
            expires, consumed2 = VarInt.decode(data, offset + consumed)
            return RequestOkMessage(request_id, expires), consumed + consumed2
        return RequestOkMessage(request_id), consumed


@dataclass
class RequestErrorMessage:
    """REQUEST_ERROR message."""
    request_id: int
    error_code: int
    reason: str
    
    def encode(self) -> bytes:
        payload = VarInt.encode(self.request_id)
        payload += VarInt.encode(self.error_code)
        payload += ReasonPhrase(self.reason).encode()
        return VarInt.encode(MessageType.REQUEST_ERROR) + encode_bytes(payload)
    
    @staticmethod
    def decode(data: bytes, offset: int = 0) -> Tuple['RequestErrorMessage', int]:
        request_id, consumed1 = VarInt.decode(data, offset)
        error_code, consumed2 = VarInt.decode(data, offset + consumed1)
        reason, consumed3 = ReasonPhrase.decode(data, offset + consumed1 + consumed2)
        return RequestErrorMessage(request_id, error_code, reason.text), consumed1 + consumed2 + consumed3


# Subscribe messages
@dataclass
class SubscribeMessage:
    """SUBSCRIBE message."""
    request_id: int
    track_alias: int
    full_track_name: FullTrackName
    subscriber_priority: int
    group_order: GroupOrder
    filter_type: SubscribeFilter
    start_group: Optional[int] = None
    start_object: Optional[int] = None
    end_group: Optional[int] = None
    end_object: Optional[int] = None
    parameters: Optional[Parameters] = None
    
    def encode(self) -> bytes:
        payload = VarInt.encode(self.request_id)
        payload += VarInt.encode(self.track_alias)
        payload += self.full_track_name.encode()
        payload += bytes([self.subscriber_priority])
        payload += bytes([self.group_order])
        payload += VarInt.encode(self.filter_type)
        
        if self.filter_type in (SubscribeFilter.ABSOLUTE_START, SubscribeFilter.ABSOLUTE_RANGE):
            if self.start_group is None or self.start_object is None:
                raise ValueError("ABSOLUTE_START/ABSOLUTE_RANGE requires start_group and start_object")
            payload += VarInt.encode(self.start_group)
            payload += VarInt.encode(self.start_object)
        
        if self.filter_type == SubscribeFilter.ABSOLUTE_RANGE:
            if self.end_group is None or self.end_object is None:
                raise ValueError("ABSOLUTE_RANGE requires end_group and end_object")
            payload += VarInt.encode(self.end_group)
            payload += VarInt.encode(self.end_object)
        
        if self.parameters:
            payload += self.parameters.encode()
        
        return VarInt.encode(MessageType.SUBSCRIBE) + encode_bytes(payload)
    
    @staticmethod
    def decode(data: bytes, offset: int = 0) -> Tuple['SubscribeMessage', int]:
        start_offset = offset
        request_id, consumed = VarInt.decode(data, offset)
        offset += consumed
        
        track_alias, consumed = VarInt.decode(data, offset)
        offset += consumed
        
        full_track_name, consumed = FullTrackName.decode(data, offset)
        offset += consumed
        
        subscriber_priority = data[offset]
        offset += 1
        
        group_order = GroupOrder(data[offset])
        offset += 1
        
        filter_type_val, consumed = VarInt.decode(data, offset)
        filter_type = SubscribeFilter(filter_type_val)
        offset += consumed
        
        start_group = None
        start_object = None
        end_group = None
        end_object = None
        
        if filter_type in (SubscribeFilter.ABSOLUTE_START, SubscribeFilter.ABSOLUTE_RANGE):
            start_group, consumed = VarInt.decode(data, offset)
            offset += consumed
            start_object, consumed = VarInt.decode(data, offset)
            offset += consumed
        
        if filter_type == SubscribeFilter.ABSOLUTE_RANGE:
            end_group, consumed = VarInt.decode(data, offset)
            offset += consumed
            end_object, consumed = VarInt.decode(data, offset)
            offset += consumed
        
        parameters = None
        if offset < len(data):
            parameters, consumed = Parameters.decode(data, offset)
            offset += consumed
        
        msg = SubscribeMessage(
            request_id=request_id,
            track_alias=track_alias,
            full_track_name=full_track_name,
            subscriber_priority=subscriber_priority,
            group_order=group_order,
            filter_type=filter_type,
            start_group=start_group,
            start_object=start_object,
            end_group=end_group,
            end_object=end_object,
            parameters=parameters
        )
        return msg, offset - start_offset


@dataclass
class SubscribeOkMessage:
    """SUBSCRIBE_OK message."""
    request_id: int
    expires: int  # milliseconds, 0 = does not expire
    group_order: GroupOrder
    largest_group: Optional[int] = None
    largest_object: Optional[int] = None
    parameters: Optional[Parameters] = None
    
    def encode(self) -> bytes:
        payload = VarInt.encode(self.request_id)
        payload += VarInt.encode(self.expires)
        payload += bytes([self.group_order])
        
        if self.largest_group is not None:
            payload += VarInt.encode(self.largest_group)
            if self.largest_object is not None:
                payload += VarInt.encode(self.largest_object)
        
        if self.parameters:
            payload += self.parameters.encode()
        
        return VarInt.encode(MessageType.SUBSCRIBE_OK) + encode_bytes(payload)
    
    @staticmethod
    def decode(data: bytes, offset: int = 0) -> Tuple['SubscribeOkMessage', int]:
        start_offset = offset
        request_id, consumed = VarInt.decode(data, offset)
        offset += consumed
        
        expires, consumed = VarInt.decode(data, offset)
        offset += consumed
        
        group_order = GroupOrder(data[offset])
        offset += 1
        
        largest_group = None
        largest_object = None
        
        if offset < len(data):
            try:
                largest_group, consumed = VarInt.decode(data, offset)
                offset += consumed
                largest_object, consumed = VarInt.decode(data, offset)
                offset += consumed
            except:
                pass
        
        parameters = None
        if offset < len(data):
            try:
                parameters, consumed = Parameters.decode(data, offset)
                offset += consumed
            except:
                pass
        
        return SubscribeOkMessage(
            request_id=request_id,
            expires=expires,
            group_order=group_order,
            largest_group=largest_group,
            largest_object=largest_object,
            parameters=parameters
        ), offset - start_offset


# Publish messages
@dataclass
class PublishMessage:
    """PUBLISH message."""
    request_id: int
    track_alias: int
    full_track_name: FullTrackName
    parameters: Optional[Parameters] = None
    
    def encode(self) -> bytes:
        payload = VarInt.encode(self.request_id)
        payload += VarInt.encode(self.track_alias)
        payload += self.full_track_name.encode()
        if self.parameters:
            payload += self.parameters.encode()
        return VarInt.encode(MessageType.PUBLISH) + encode_bytes(payload)
    
    @staticmethod
    def decode(data: bytes, offset: int = 0) -> Tuple['PublishMessage', int]:
        start_offset = offset
        request_id, consumed = VarInt.decode(data, offset)
        offset += consumed
        
        track_alias, consumed = VarInt.decode(data, offset)
        offset += consumed
        
        full_track_name, consumed = FullTrackName.decode(data, offset)
        offset += consumed
        
        parameters = None
        if offset < len(data):
            parameters, consumed = Parameters.decode(data, offset)
            offset += consumed
        
        return PublishMessage(request_id, track_alias, full_track_name, parameters), offset - start_offset


@dataclass
class PublishOkMessage:
    """PUBLISH_OK message."""
    request_id: int
    
    def encode(self) -> bytes:
        payload = VarInt.encode(self.request_id)
        return VarInt.encode(MessageType.PUBLISH_OK) + encode_bytes(payload)
    
    @staticmethod
    def decode(data: bytes, offset: int = 0) -> Tuple['PublishOkMessage', int]:
        request_id, consumed = VarInt.decode(data, offset)
        return PublishOkMessage(request_id), consumed


@dataclass
class PublishDoneMessage:
    """PUBLISH_DONE message."""
    request_id: int
    status_code: int
    reason: str
    
    def encode(self) -> bytes:
        payload = VarInt.encode(self.request_id)
        payload += VarInt.encode(self.status_code)
        payload += ReasonPhrase(self.reason).encode()
        return VarInt.encode(MessageType.PUBLISH_DONE) + encode_bytes(payload)
    
    @staticmethod
    def decode(data: bytes, offset: int = 0) -> Tuple['PublishDoneMessage', int]:
        request_id, consumed1 = VarInt.decode(data, offset)
        status_code, consumed2 = VarInt.decode(data, offset + consumed1)
        reason, consumed3 = ReasonPhrase.decode(data, offset + consumed1 + consumed2)
        return PublishDoneMessage(request_id, status_code, reason.text), consumed1 + consumed2 + consumed3


# Fetch messages
@dataclass
class FetchMessage:
    """FETCH message for requesting specific objects.
    
    If start_group and start_object are not specified, defaults to 0.
    If end_group and end_object are not specified (None), fetches until the latest message.
    """
    request_id: int
    full_track_name: FullTrackName
    subscriber_priority: int = 128
    group_order: GroupOrder = GroupOrder.ASCENDING
    start_group: int = 0
    start_object: int = 0
    end_group: Optional[int] = None
    end_object: Optional[int] = None
    parameters: Optional[Parameters] = None
    
    def encode(self) -> bytes:
        payload = VarInt.encode(self.request_id)
        payload += self.full_track_name.encode()
        payload += bytes([self.subscriber_priority])
        payload += bytes([self.group_order])
        payload += VarInt.encode(self.start_group if self.start_group is not None else 0)
        payload += VarInt.encode(self.start_object if self.start_object is not None else 0)
        payload += VarInt.encode(self.end_group if self.end_group is not None else 0xFFFFFFFFFFFFFFFF)
        payload += VarInt.encode(self.end_object if self.end_object is not None else 0xFFFFFFFFFFFFFFFF)
        if self.parameters:
            payload += self.parameters.encode()
        return VarInt.encode(MessageType.FETCH) + encode_bytes(payload)
    
    @staticmethod
    def decode(data: bytes, offset: int = 0) -> Tuple['FetchMessage', int]:
        start_offset = offset
        request_id, consumed = VarInt.decode(data, offset)
        offset += consumed
        
        full_track_name, consumed = FullTrackName.decode(data, offset)
        offset += consumed
        
        subscriber_priority = data[offset]
        offset += 1
        
        group_order = GroupOrder(data[offset])
        offset += 1
        
        start_group, consumed = VarInt.decode(data, offset)
        offset += consumed
        
        start_object, consumed = VarInt.decode(data, offset)
        offset += consumed
        
        end_group, consumed = VarInt.decode(data, offset)
        offset += consumed
        # Special value indicates "until latest"
        if end_group == 0xFFFFFFFFFFFFFFFF:
            end_group = None
        
        end_object, consumed = VarInt.decode(data, offset)
        offset += consumed
        # Special value indicates "until latest"
        if end_object == 0xFFFFFFFFFFFFFFFF:
            end_object = None
        
        parameters = None
        if offset < len(data):
            parameters, consumed = Parameters.decode(data, offset)
            offset += consumed
        
        return FetchMessage(
            request_id=request_id,
            full_track_name=full_track_name,
            subscriber_priority=subscriber_priority,
            group_order=group_order,
            start_group=start_group,
            start_object=start_object,
            end_group=end_group,
            end_object=end_object,
            parameters=parameters
        ), offset - start_offset


@dataclass
class FetchOkMessage:
    """FETCH_OK message."""
    request_id: int
    group_order: GroupOrder
    end_of_track: bool
    largest_group: Optional[int] = None
    largest_object: Optional[int] = None
    
    def encode(self) -> bytes:
        payload = VarInt.encode(self.request_id)
        payload += bytes([self.group_order])
        payload += bytes([1 if self.end_of_track else 0])
        if self.largest_group is not None and self.largest_object is not None:
            payload += VarInt.encode(self.largest_group)
            payload += VarInt.encode(self.largest_object)
        return VarInt.encode(MessageType.FETCH_OK) + encode_bytes(payload)
    
    @staticmethod
    def decode(data: bytes, offset: int = 0) -> Tuple['FetchOkMessage', int]:
        start_offset = offset
        request_id, consumed = VarInt.decode(data, offset)
        offset += consumed
        
        group_order = GroupOrder(data[offset])
        offset += 1
        
        end_of_track = data[offset] != 0
        offset += 1
        
        largest_group = None
        largest_object = None
        
        if offset < len(data):
            largest_group, consumed = VarInt.decode(data, offset)
            offset += consumed
            largest_object, consumed = VarInt.decode(data, offset)
            offset += consumed
        
        return FetchOkMessage(
            request_id=request_id,
            group_order=group_order,
            end_of_track=end_of_track,
            largest_group=largest_group,
            largest_object=largest_object
        ), offset - start_offset


def decode_control_message(data: bytes, offset: int = 0) -> Tuple[object, int]:
    """
    Decode any control message from bytes.
    
    Returns:
        Tuple of (decoded_message, bytes_consumed)
    """
    msg_type, consumed1 = VarInt.decode(data, offset)
    msg_data, consumed2 = decode_bytes(data, offset + consumed1)
    
    if msg_type == MessageType.SETUP:
        msg, consumed = SetupMessage.decode(msg_data)
    elif msg_type == MessageType.GOAWAY:
        msg, consumed = GoAwayMessage.decode(msg_data)
    elif msg_type == MessageType.REQUEST_OK:
        msg, consumed = RequestOkMessage.decode(msg_data)
    elif msg_type == MessageType.REQUEST_ERROR:
        msg, consumed = RequestErrorMessage.decode(msg_data)
    elif msg_type == MessageType.SUBSCRIBE:
        msg, consumed = SubscribeMessage.decode(msg_data)
    elif msg_type == MessageType.SUBSCRIBE_OK:
        msg, consumed = SubscribeOkMessage.decode(msg_data)
    elif msg_type == MessageType.PUBLISH:
        msg, consumed = PublishMessage.decode(msg_data)
    elif msg_type == MessageType.PUBLISH_OK:
        msg, consumed = PublishOkMessage.decode(msg_data)
    elif msg_type == MessageType.PUBLISH_DONE:
        msg, consumed = PublishDoneMessage.decode(msg_data)
    elif msg_type == MessageType.FETCH:
        msg, consumed = FetchMessage.decode(msg_data)
    elif msg_type == MessageType.FETCH_OK:
        msg, consumed = FetchOkMessage.decode(msg_data)
    else:
        raise ValueError(f"Unknown message type: {msg_type}")
    
    return msg, consumed1 + consumed2
