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
    SETUP = 0x2F00
    GOAWAY = 0x10
    
    # Request messages
    REQUEST_OK = 0x07
    REQUEST_ERROR = 0x05
    
    # Subscribe messages
    SUBSCRIBE = 0x03
    SUBSCRIBE_OK = 0x04
    REQUEST_UPDATE = 0x02
    
    # Publish messages
    PUBLISH = 0x1D
    PUBLISH_OK = 0x1E
    PUBLISH_DONE = 0x0B
    
    # Fetch messages
    FETCH = 0x16
    FETCH_OK = 0x18
    
    # Status messages
    TRACK_STATUS = 0x0D
    
    # Namespace messages
    PUBLISH_NAMESPACE = 0x06
    NAMESPACE = 0x08
    NAMESPACE_DONE = 0x0E
    SUBSCRIBE_NAMESPACE = 0x11
    PUBLISH_BLOCKED = 0x0F


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


class StreamResetCode(IntEnum):
    """MOQT stream reset / STOP_SENDING codes."""
    INTERNAL_ERROR = 0x00
    CANCELLED = 0x01
    DELIVERY_TIMEOUT = 0x02
    SESSION_CLOSED = 0x03
    UNKNOWN_OBJECT_STATUS = 0x04
    TOO_FAR_BEHIND = 0x05
    EXCESSIVE_LOAD = 0x09
    MALFORMED_TRACK = 0x12


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


class ParameterType(IntEnum):
    """MOQT message parameter types used by the current implementation."""
    EXPIRES = 0x08
    LARGEST_OBJECT = 0x09
    FORWARD = 0x10
    SUBSCRIBER_PRIORITY = 0x20
    SUBSCRIPTION_FILTER = 0x21
    GROUP_ORDER = 0x22
    NEW_GROUP_REQUEST = 0x32


class PublishDoneStatus(IntEnum):
    """PUBLISH_DONE status codes for subscriptions/publications."""
    INTERNAL_ERROR = 0x00
    UNAUTHORIZED = 0x01
    TRACK_ENDED = 0x02
    SUBSCRIPTION_ENDED = 0x03
    GOING_AWAY = 0x04
    EXPIRED = 0x05
    TOO_FAR_BEHIND = 0x06
    UPDATE_FAILED = 0x08
    EXCESSIVE_LOAD = 0x09
    MALFORMED_TRACK = 0x12


UNKNOWN_PUBLISH_DONE_STREAM_COUNT = (1 << 62) - 1


@dataclass
class SubscriptionFilterValue:
    """Draft-17 SUBSCRIPTION_FILTER parameter value."""
    filter_type: SubscribeFilter
    start_group: Optional[int] = None
    start_object: Optional[int] = None
    end_group_delta: Optional[int] = None

    def encode(self) -> bytes:
        payload = bytearray()
        payload.extend(VarInt.encode(self.filter_type))
        if self.filter_type in (SubscribeFilter.ABSOLUTE_START, SubscribeFilter.ABSOLUTE_RANGE):
            if self.start_group is None or self.start_object is None:
                raise ValueError("absolute subscription filters require a start location")
            payload.extend(Location(self.start_group, self.start_object).encode())
        if self.filter_type == SubscribeFilter.ABSOLUTE_RANGE:
            if self.end_group_delta is None:
                raise ValueError("absolute range requires end_group_delta")
            payload.extend(VarInt.encode(self.end_group_delta))
        return bytes(payload)

    @staticmethod
    def decode(data: bytes, offset: int = 0) -> Tuple['SubscriptionFilterValue', int]:
        start_offset = offset
        filter_type_value, consumed = VarInt.decode(data, offset)
        offset += consumed
        filter_type = SubscribeFilter(filter_type_value)

        start_group = None
        start_object = None
        end_group_delta = None
        if filter_type in (SubscribeFilter.ABSOLUTE_START, SubscribeFilter.ABSOLUTE_RANGE):
            location, consumed = Location.decode(data, offset)
            offset += consumed
            start_group = location.group
            start_object = location.object_id
        if filter_type == SubscribeFilter.ABSOLUTE_RANGE:
            end_group_delta, consumed = VarInt.decode(data, offset)
            offset += consumed

        return SubscriptionFilterValue(
            filter_type=filter_type,
            start_group=start_group,
            start_object=start_object,
            end_group_delta=end_group_delta,
        ), offset - start_offset


def group_order_to_parameter_value(group_order: GroupOrder) -> int:
    """Map internal GroupOrder to the draft-17 parameter value."""
    if group_order == GroupOrder.ASCENDING:
        return 0x1
    if group_order == GroupOrder.DESCENDING:
        return 0x2
    raise ValueError(f"Unsupported group order: {group_order}")


def group_order_from_parameter_value(value: int) -> GroupOrder:
    """Map the draft-17 parameter value to internal GroupOrder."""
    if value == 0x1:
        return GroupOrder.ASCENDING
    if value == 0x2:
        return GroupOrder.DESCENDING
    raise ValueError(f"Invalid GROUP_ORDER parameter value: {value}")


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
    parameters: Optional[Parameters] = None
    
    def encode(self, include_request_id: bool = True) -> bytes:
        payload = b""
        if include_request_id:
            payload += VarInt.encode(self.request_id)
        payload += (self.parameters or Parameters()).encode()
        return VarInt.encode(MessageType.REQUEST_OK) + encode_bytes(payload)
    
    @staticmethod
    def decode(
        data: bytes,
        offset: int = 0,
        request_id: Optional[int] = None,
    ) -> Tuple['RequestOkMessage', int]:
        start_offset = offset
        if request_id is None:
            request_id, consumed = VarInt.decode(data, offset)
            offset += consumed

        parameters = Parameters()
        if offset < len(data):
            parameters, consumed = Parameters.decode(data, offset)
            offset += consumed
        return RequestOkMessage(request_id, parameters), offset - start_offset


@dataclass
class RequestErrorMessage:
    """REQUEST_ERROR message."""
    request_id: int
    error_code: int
    reason: str
    retry_interval: int = 0
    
    def encode(self, include_request_id: bool = True) -> bytes:
        payload = b""
        if include_request_id:
            payload += VarInt.encode(self.request_id)
        payload += VarInt.encode(self.error_code)
        payload += VarInt.encode(self.retry_interval)
        payload += ReasonPhrase(self.reason).encode()
        return VarInt.encode(MessageType.REQUEST_ERROR) + encode_bytes(payload)
    
    @staticmethod
    def decode(
        data: bytes,
        offset: int = 0,
        request_id: Optional[int] = None,
    ) -> Tuple['RequestErrorMessage', int]:
        start_offset = offset
        if request_id is None:
            request_id, consumed = VarInt.decode(data, offset)
            offset += consumed
        error_code, consumed = VarInt.decode(data, offset)
        offset += consumed
        retry_interval, consumed = VarInt.decode(data, offset)
        offset += consumed
        reason, consumed = ReasonPhrase.decode(data, offset)
        offset += consumed
        return RequestErrorMessage(
            request_id,
            error_code,
            reason.text,
            retry_interval,
        ), offset - start_offset


# Subscribe messages
@dataclass
class SubscribeMessage:
    """SUBSCRIBE message."""
    request_id: int
    full_track_name: FullTrackName
    subscriber_priority: int
    group_order: GroupOrder
    filter_type: SubscribeFilter
    required_request_id_delta: int = 0
    start_group: Optional[int] = None
    start_object: Optional[int] = None
    end_group: Optional[int] = None
    end_object: Optional[int] = None
    parameters: Optional[Parameters] = None
    
    def encode(self) -> bytes:
        payload = VarInt.encode(self.request_id)
        payload += VarInt.encode(self.required_request_id_delta)
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
        
        payload += (self.parameters or Parameters()).encode()
        
        return VarInt.encode(MessageType.SUBSCRIBE) + encode_bytes(payload)
    
    @staticmethod
    def decode(data: bytes, offset: int = 0) -> Tuple['SubscribeMessage', int]:
        start_offset = offset
        request_id, consumed = VarInt.decode(data, offset)
        offset += consumed
        required_request_id_delta, consumed = VarInt.decode(data, offset)
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
        
        parameters = Parameters()
        if offset < len(data):
            parameters, consumed = Parameters.decode(data, offset)
            offset += consumed

        msg = SubscribeMessage(
            request_id=request_id,
            required_request_id_delta=required_request_id_delta,
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
    track_alias: int
    parameters: Optional[Parameters] = None
    track_properties: bytes = b""
    
    def encode(self, include_request_id: bool = True) -> bytes:
        payload = b""
        if include_request_id:
            payload += VarInt.encode(self.request_id)
        payload += VarInt.encode(self.track_alias)
        if self.parameters:
            payload += self.parameters.encode()
        else:
            payload += Parameters().encode()
        payload += self.track_properties
        
        return VarInt.encode(MessageType.SUBSCRIBE_OK) + encode_bytes(payload)
    
    @staticmethod
    def decode(
        data: bytes,
        offset: int = 0,
        request_id: Optional[int] = None,
    ) -> Tuple['SubscribeOkMessage', int]:
        start_offset = offset
        if request_id is None:
            request_id, consumed = VarInt.decode(data, offset)
            offset += consumed

        track_alias, consumed = VarInt.decode(data, offset)
        offset += consumed

        parameters = Parameters()
        if offset < len(data):
            parameters, consumed = Parameters.decode(data, offset)
            offset += consumed
        track_properties = data[offset:]
        offset = len(data)
        
        return SubscribeOkMessage(
            request_id=request_id,
            track_alias=track_alias,
            parameters=parameters,
            track_properties=track_properties,
        ), offset - start_offset


# Publish messages
@dataclass
class PublishMessage:
    """PUBLISH message."""
    request_id: int
    required_request_id_delta: int
    full_track_name: FullTrackName
    track_alias: int
    parameters: Optional[Parameters] = None
    track_properties: bytes = b""
    
    def encode(self) -> bytes:
        payload = VarInt.encode(self.request_id)
        payload += VarInt.encode(self.required_request_id_delta)
        payload += self.full_track_name.encode()
        payload += VarInt.encode(self.track_alias)
        payload += (self.parameters or Parameters()).encode()
        payload += self.track_properties
        return VarInt.encode(MessageType.PUBLISH) + encode_bytes(payload)
    
    @staticmethod
    def decode(data: bytes, offset: int = 0) -> Tuple['PublishMessage', int]:
        start_offset = offset
        request_id, consumed = VarInt.decode(data, offset)
        offset += consumed
        required_request_id_delta, consumed = VarInt.decode(data, offset)
        offset += consumed
        
        full_track_name, consumed = FullTrackName.decode(data, offset)
        offset += consumed

        track_alias, consumed = VarInt.decode(data, offset)
        offset += consumed
        
        parameters = Parameters()
        if offset < len(data):
            parameters, consumed = Parameters.decode(data, offset)
            offset += consumed
        track_properties = data[offset:]
        offset = len(data)
        
        return PublishMessage(
            request_id=request_id,
            required_request_id_delta=required_request_id_delta,
            full_track_name=full_track_name,
            track_alias=track_alias,
            parameters=parameters,
            track_properties=track_properties,
        ), offset - start_offset


@dataclass
class RequestUpdateMessage:
    """REQUEST_UPDATE message."""
    request_id: int
    required_request_id_delta: int = 0
    parameters: Optional[Parameters] = None

    def encode(self) -> bytes:
        payload = VarInt.encode(self.request_id)
        payload += VarInt.encode(self.required_request_id_delta)
        payload += (self.parameters or Parameters()).encode()
        return VarInt.encode(MessageType.REQUEST_UPDATE) + encode_bytes(payload)

    @staticmethod
    def decode(data: bytes, offset: int = 0) -> Tuple['RequestUpdateMessage', int]:
        start_offset = offset
        request_id, consumed = VarInt.decode(data, offset)
        offset += consumed
        required_request_id_delta, consumed = VarInt.decode(data, offset)
        offset += consumed
        parameters = Parameters()
        if offset < len(data):
            parameters, consumed = Parameters.decode(data, offset)
            offset += consumed
        return RequestUpdateMessage(
            request_id=request_id,
            required_request_id_delta=required_request_id_delta,
            parameters=parameters,
        ), offset - start_offset


@dataclass
class PublishOkMessage:
    """PUBLISH_OK message."""
    request_id: int
    parameters: Optional[Parameters] = None
    
    def encode(self, include_request_id: bool = True) -> bytes:
        payload = b""
        if include_request_id:
            payload += VarInt.encode(self.request_id)
        payload += (self.parameters or Parameters()).encode()
        return VarInt.encode(MessageType.PUBLISH_OK) + encode_bytes(payload)
    
    @staticmethod
    def decode(
        data: bytes,
        offset: int = 0,
        request_id: Optional[int] = None,
    ) -> Tuple['PublishOkMessage', int]:
        start_offset = offset
        if request_id is None:
            request_id, consumed = VarInt.decode(data, offset)
            offset += consumed
        parameters = Parameters()
        if offset < len(data):
            parameters, consumed = Parameters.decode(data, offset)
            offset += consumed
        return PublishOkMessage(request_id, parameters), offset - start_offset


@dataclass
class PublishDoneMessage:
    """PUBLISH_DONE message."""
    request_id: int
    status_code: int
    stream_count: int = UNKNOWN_PUBLISH_DONE_STREAM_COUNT
    reason: str = ""
    
    def encode(self, include_request_id: bool = True) -> bytes:
        payload = b""
        if include_request_id:
            payload += VarInt.encode(self.request_id)
        payload += VarInt.encode(self.status_code)
        payload += VarInt.encode(self.stream_count)
        payload += ReasonPhrase(self.reason).encode()
        return VarInt.encode(MessageType.PUBLISH_DONE) + encode_bytes(payload)
    
    @staticmethod
    def decode(
        data: bytes,
        offset: int = 0,
        request_id: Optional[int] = None,
    ) -> Tuple['PublishDoneMessage', int]:
        start_offset = offset
        if request_id is None:
            request_id, consumed = VarInt.decode(data, offset)
            offset += consumed
        status_code, consumed = VarInt.decode(data, offset)
        offset += consumed
        stream_count, consumed = VarInt.decode(data, offset)
        offset += consumed
        reason, consumed = ReasonPhrase.decode(data, offset)
        offset += consumed
        return PublishDoneMessage(
            request_id=request_id,
            status_code=status_code,
            stream_count=stream_count,
            reason=reason.text,
        ), offset - start_offset


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
    required_request_id_delta: int = 0
    start_group: int = 0
    start_object: int = 0
    end_group: Optional[int] = None
    end_object: Optional[int] = None
    parameters: Optional[Parameters] = None
    
    def encode(self) -> bytes:
        payload = VarInt.encode(self.request_id)
        payload += VarInt.encode(self.required_request_id_delta)
        payload += self.full_track_name.encode()
        payload += bytes([self.subscriber_priority])
        payload += bytes([self.group_order])
        payload += VarInt.encode(self.start_group if self.start_group is not None else 0)
        payload += VarInt.encode(self.start_object if self.start_object is not None else 0)
        payload += VarInt.encode(self.end_group if self.end_group is not None else 0xFFFFFFFFFFFFFFFF)
        payload += VarInt.encode(self.end_object if self.end_object is not None else 0xFFFFFFFFFFFFFFFF)
        payload += (self.parameters or Parameters()).encode()
        return VarInt.encode(MessageType.FETCH) + encode_bytes(payload)
    
    @staticmethod
    def decode(data: bytes, offset: int = 0) -> Tuple['FetchMessage', int]:
        start_offset = offset
        request_id, consumed = VarInt.decode(data, offset)
        offset += consumed
        required_request_id_delta, consumed = VarInt.decode(data, offset)
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
        
        parameters = Parameters()
        if offset < len(data):
            parameters, consumed = Parameters.decode(data, offset)
            offset += consumed
        
        return FetchMessage(
            request_id=request_id,
            required_request_id_delta=required_request_id_delta,
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
    end_of_track: bool
    end_location: Location
    parameters: Optional[Parameters] = None
    track_properties: bytes = b""
    
    def encode(self, include_request_id: bool = True) -> bytes:
        payload = b""
        if include_request_id:
            payload += VarInt.encode(self.request_id)
        payload += bytes([1 if self.end_of_track else 0])
        payload += self.end_location.encode()
        payload += (self.parameters or Parameters()).encode()
        payload += self.track_properties
        return VarInt.encode(MessageType.FETCH_OK) + encode_bytes(payload)
    
    @staticmethod
    def decode(
        data: bytes,
        offset: int = 0,
        request_id: Optional[int] = None,
    ) -> Tuple['FetchOkMessage', int]:
        start_offset = offset
        if request_id is None:
            request_id, consumed = VarInt.decode(data, offset)
            offset += consumed

        end_of_track = data[offset] != 0
        offset += 1

        end_location, consumed = Location.decode(data, offset)
        offset += consumed

        parameters = Parameters()
        if offset < len(data):
            parameters, consumed = Parameters.decode(data, offset)
            offset += consumed
        track_properties = data[offset:]
        offset = len(data)

        return FetchOkMessage(
            request_id=request_id,
            end_of_track=end_of_track,
            end_location=end_location,
            parameters=parameters,
            track_properties=track_properties,
        ), offset - start_offset


def decode_control_message(
    data: bytes,
    offset: int = 0,
    response_request_id: Optional[int] = None,
) -> Tuple[object, int]:
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
        msg, consumed = RequestOkMessage.decode(msg_data, request_id=response_request_id)
    elif msg_type == MessageType.REQUEST_ERROR:
        msg, consumed = RequestErrorMessage.decode(msg_data, request_id=response_request_id)
    elif msg_type == MessageType.SUBSCRIBE:
        msg, consumed = SubscribeMessage.decode(msg_data)
    elif msg_type == MessageType.SUBSCRIBE_OK:
        msg, consumed = SubscribeOkMessage.decode(msg_data, request_id=response_request_id)
    elif msg_type == MessageType.REQUEST_UPDATE:
        msg, consumed = RequestUpdateMessage.decode(msg_data)
    elif msg_type == MessageType.PUBLISH:
        msg, consumed = PublishMessage.decode(msg_data)
    elif msg_type == MessageType.PUBLISH_OK:
        msg, consumed = PublishOkMessage.decode(msg_data, request_id=response_request_id)
    elif msg_type == MessageType.PUBLISH_DONE:
        msg, consumed = PublishDoneMessage.decode(msg_data, request_id=response_request_id)
    elif msg_type == MessageType.FETCH:
        msg, consumed = FetchMessage.decode(msg_data)
    elif msg_type == MessageType.FETCH_OK:
        msg, consumed = FetchOkMessage.decode(msg_data, request_id=response_request_id)
    else:
        raise ValueError(f"Unknown message type: {msg_type}")
    
    return msg, consumed1 + consumed2
