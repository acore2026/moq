"""
MOQ Protocol Message Serialization/Deserialization
Handles encoding and decoding of MOQ control messages
"""

import logging
from typing import Optional, Any, Tuple
from dataclasses import dataclass

from moq.protocol.constants import MOQMessageType, MOQErrorCode
from moq.protocol.varint import encode_varint, decode_varint, encode_string, decode_string
from moq.protocol.varint import encode_bytes, decode_bytes, encode_tuple, decode_tuple

logger = logging.getLogger(__name__)


@dataclass
class MOQMessage:
    """Base class for MOQ messages"""
    msg_type: int
    
    def encode(self) -> bytes:
        """Encode message to bytes"""
        raise NotImplementedError
    
    @classmethod
    def decode(cls, data: bytes, offset: int = 0) -> Tuple['MOQMessage', int]:
        """Decode message from bytes"""
        raise NotImplementedError


class ClientSetupMessage(MOQMessage):
    """CLIENT_SETUP message (draft-ietf-moq-transport-17 Section 6.1.1)"""
    
    def __init__(
        self,
        versions: list[int],
        role: int,
        parameters: Optional[dict] = None
    ):
        super().__init__(MOQMessageType.CLIENT_SETUP)
        self.versions = versions
        self.role = role
        self.parameters = parameters or {}
    
    def encode(self) -> bytes:
        result = encode_varint(self.msg_type)
        result += encode_varint(len(self.versions))
        for version in self.versions:
            result += encode_varint(version)
        
        # Encode role parameter
        result += encode_varint(1)  # Number of parameters
        result += encode_varint(0x00)  # Role parameter ID
        result += encode_varint(1)  # Length
        result += encode_varint(self.role)
        
        return result
    
    @classmethod
    def decode(cls, data: bytes, offset: int = 0) -> Tuple['ClientSetupMessage', int]:
        versions_count, consumed = decode_varint(data, offset)
        versions = []
        pos = offset + consumed
        
        for _ in range(versions_count):
            version, v_consumed = decode_varint(data, pos)
            versions.append(version)
            pos += v_consumed
        
        # Decode parameters
        param_count, p_consumed = decode_varint(data, pos)
        pos += p_consumed
        parameters = {}
        role = 0
        
        for _ in range(param_count):
            param_id, _ = decode_varint(data, pos)
            pos += 1
            param_len, _ = decode_varint(data, pos)
            pos += 1
            
            if param_id == 0x00:  # Role
                role, _ = decode_varint(data, pos)
            
            pos += param_len
        
        return cls(versions, role, parameters), pos - offset


class ServerSetupMessage(MOQMessage):
    """SERVER_SETUP message (draft-ietf-moq-transport-17 Section 6.1.2)"""
    
    def __init__(self, selected_version: int, role: int, parameters: Optional[dict] = None):
        super().__init__(MOQMessageType.SERVER_SETUP)
        self.selected_version = selected_version
        self.role = role
        self.parameters = parameters or {}
    
    def encode(self) -> bytes:
        result = encode_varint(self.msg_type)
        result += encode_varint(self.selected_version)
        result += encode_varint(1)  # Number of parameters
        result += encode_varint(0x00)  # Role parameter ID
        result += encode_varint(1)  # Length
        result += encode_varint(self.role)
        return result
    
    @classmethod
    def decode(cls, data: bytes, offset: int = 0) -> Tuple['ServerSetupMessage', int]:
        version, consumed = decode_varint(data, offset)
        pos = offset + consumed
        
        param_count, p_consumed = decode_varint(data, pos)
        pos += p_consumed
        
        role = 0
        for _ in range(param_count):
            param_id, _ = decode_varint(data, pos)
            pos += 1
            param_len, _ = decode_varint(data, pos)
            pos += 1
            
            if param_id == 0x00:  # Role
                role, _ = decode_varint(data, pos)
            
            pos += param_len
        
        return cls(version, role), pos - offset


class SubscribeMessage(MOQMessage):
    """SUBSCRIBE message (draft-ietf-moq-transport-17 Section 6.2)"""
    
    def __init__(
        self,
        subscribe_id: int,
        track_alias: int,
        track_namespace: tuple[str, ...],
        track_name: str,
        filter_type: int,
        start_group: Optional[int] = None,
        start_object: Optional[int] = None,
        end_group: Optional[int] = None,
        end_object: Optional[int] = None,
        subscriber_priority: int = 128,
        group_order: int = 0
    ):
        super().__init__(MOQMessageType.SUBSCRIBE)
        self.subscribe_id = subscribe_id
        self.track_alias = track_alias
        self.track_namespace = track_namespace
        self.track_name = track_name
        self.filter_type = filter_type
        self.start_group = start_group
        self.start_object = start_object
        self.end_group = end_group
        self.end_object = end_object
        self.subscriber_priority = subscriber_priority
        self.group_order = group_order
    
    def encode(self) -> bytes:
        result = encode_varint(self.msg_type)
        result += encode_varint(self.subscribe_id)
        result += encode_varint(self.track_alias)
        result += encode_tuple(self.track_namespace)
        result += encode_string(self.track_name)
        result += encode_varint(self.subscriber_priority)
        result += encode_varint(self.group_order)
        result += encode_varint(self.filter_type)
        
        if self.filter_type == 0x03:  # ABSOLUTE_START
            result += encode_varint(self.start_group or 0)
            result += encode_varint(self.start_object or 0)
        elif self.filter_type == 0x04:  # ABSOLUTE_RANGE
            result += encode_varint(self.start_group or 0)
        result += encode_varint(self.subscribe_id)
        result += encode_varint(self.track_alias)
        result += encode_tuple(self.track_namespace)
        result += encode_string(self.track_name)
        result += encode_varint(self.subscriber_priority)
        result += encode_varint(self.group_order)
        result += encode_varint(self.filter_type)
        
        if self.filter_type == 0x03:  # ABSOLUTE_START
            result += encode_varint(self.start_group or 0)
            result += encode_varint(self.start_object or 0)
        elif self.filter_type == 0x04:  # ABSOLUTE_RANGE
            result += encode_varint(self.start_group or 0)
            result += encode_varint(self.start_object or 0)
            result += encode_varint(self.end_group or 0)
            result += encode_varint(self.end_object or 0)
        
        result += encode_varint(0)  # Number of subscribe parameters
        
        return result
    
    @classmethod
    def decode(cls, data: bytes, offset: int = 0) -> Tuple['SubscribeMessage', int]:
        pos = offset
        
        subscribe_id, consumed = decode_varint(data, pos)
        pos += consumed
        
        track_alias, consumed = decode_varint(data, pos)
        pos += consumed
        
        track_namespace, consumed = decode_tuple(data, pos)
        pos += consumed
        
        track_name, consumed = decode_string(data, pos)
        pos += consumed
        
        subscriber_priority, consumed = decode_varint(data, pos)
        pos += consumed
        
        group_order, consumed = decode_varint(data, pos)
        pos += consumed
        
        filter_type, consumed = decode_varint(data, pos)
        pos += consumed
        
        start_group = None
        start_object = None
        end_group = None
        end_object = None
        
        if filter_type == 0x03:  # ABSOLUTE_START
            start_group, consumed = decode_varint(data, pos)
            pos += consumed
            start_object, consumed = decode_varint(data, pos)
            pos += consumed
        elif filter_type == 0x04:  # ABSOLUTE_RANGE
            start_group, consumed = decode_varint(data, pos)
            pos += consumed
            start_object, consumed = decode_varint(data, pos)
            pos += consumed
            end_group, consumed = decode_varint(data, pos)
            pos += consumed
            end_object, consumed = decode_varint(data, pos)
            pos += consumed
        
        # Skip parameters
        param_count, consumed = decode_varint(data, pos)
        pos += consumed
        
        for _ in range(param_count):
            _, _ = decode_varint(data, pos)
            pos += 1
            param_len, _ = decode_varint(data, pos)
            pos += 1
            pos += param_len
        
        return cls(
            subscribe_id=subscribe_id,
            track_alias=track_alias,
            track_namespace=track_namespace,
            track_name=track_name,
            filter_type=filter_type,
            start_group=start_group,
            start_object=start_object,
            end_group=end_group,
            end_object=end_object,
            subscriber_priority=subscriber_priority,
            group_order=group_order
        ), pos - offset


class SubscribeOkMessage(MOQMessage):
    """SUBSCRIBE_OK message"""
    
    def __init__(
        self,
        subscribe_id: int,
        expires: int = 0,
        group_order: int = 0,
        content_exists: bool = False,
        largest_group_id: Optional[int] = None,
        largest_object_id: Optional[int] = None
    ):
        super().__init__(MOQMessageType.SUBSCRIBE_OK)
        self.subscribe_id = subscribe_id
        self.expires = expires
        self.group_order = group_order
        self.content_exists = content_exists
        self.largest_group_id = largest_group_id
        self.largest_object_id = largest_object_id
    
    def encode(self) -> bytes:
        result = encode_varint(self.msg_type)
        result += encode_varint(self.subscribe_id)
        result += encode_varint(self.expires)
        result += encode_varint(self.group_order)
        result += encode_varint(1 if self.content_exists else 0)
        
        if self.content_exists:
            result += encode_varint(self.largest_group_id or 0)
            result += encode_varint(self.largest_object_id or 0)
        
        result += encode_varint(0)  # Number of parameters
        
        return result
    
    @classmethod
    def decode(cls, data: bytes, offset: int = 0) -> Tuple['SubscribeOkMessage', int]:
        pos = offset
        
        subscribe_id, consumed = decode_varint(data, pos)
        pos += consumed
        
        expires, consumed = decode_varint(data, pos)
        pos += consumed
        
        group_order, consumed = decode_varint(data, pos)
        pos += consumed
        
        content_exists_val, consumed = decode_varint(data, pos)
        pos += consumed
        content_exists = content_exists_val == 1
        
        largest_group_id = None
        largest_object_id = None
        
        if content_exists:
            largest_group_id, consumed = decode_varint(data, pos)
            pos += consumed
            largest_object_id, consumed = decode_varint(data, pos)
            pos += consumed
        
        # Skip parameters
        param_count, consumed = decode_varint(data, pos)
        pos += consumed
        
        for _ in range(param_count):
            _, _ = decode_varint(data, pos)
            pos += 1
            param_len, _ = decode_varint(data, pos)
            pos += 1
            pos += param_len
        
        return cls(
            subscribe_id=subscribe_id,
            expires=expires,
            group_order=group_order,
            content_exists=content_exists,
            largest_group_id=largest_group_id,
            largest_object_id=largest_object_id
        ), pos - offset


class SubscribeErrorMessage(MOQMessage):
    """SUBSCRIBE_ERROR message"""
    
    def __init__(
        self,
        subscribe_id: int,
        error_code: int,
        reason: str,
        track_alias: int
    ):
        super().__init__(MOQMessageType.SUBSCRIBE_ERROR)
        self.subscribe_id = subscribe_id
        self.error_code = error_code
        self.reason = reason
        self.track_alias = track_alias
    
    def encode(self) -> bytes:
        result = encode_varint(self.msg_type)
        result += encode_varint(self.subscribe_id)
        result += encode_varint(self.error_code)
        result += encode_string(self.reason)
        result += encode_varint(self.track_alias)
        return result
    
    @classmethod
    def decode(cls, data: bytes, offset: int = 0) -> Tuple['SubscribeErrorMessage', int]:
        pos = offset
        
        subscribe_id, consumed = decode_varint(data, pos)
        pos += consumed
        
        error_code, consumed = decode_varint(data, pos)
        pos += consumed
        
        reason, consumed = decode_string(data, pos)
        pos += consumed
        
        track_alias, consumed = decode_varint(data, pos)
        pos += consumed
        
        return cls(subscribe_id, error_code, reason, track_alias), pos - offset


class UnsubscribeMessage(MOQMessage):
    """UNSUBSCRIBE message"""
    
    def __init__(self, subscribe_id: int):
        super().__init__(MOQMessageType.UNSUBSCRIBE)
        self.subscribe_id = subscribe_id
    
    def encode(self) -> bytes:
        result = encode_varint(self.msg_type)
        result += encode_varint(self.subscribe_id)
        return result
    
    @classmethod
    def decode(cls, data: bytes, offset: int = 0) -> Tuple['UnsubscribeMessage', int]:
        subscribe_id, consumed = decode_varint(data, offset)
        return cls(subscribe_id), consumed


class AnnounceMessage(MOQMessage):
    """ANNOUNCE message"""
    
    def __init__(
        self,
        track_namespace: tuple[str, ...],
        parameters: Optional[dict] = None
    ):
        super().__init__(MOQMessageType.ANNOUNCE)
        self.track_namespace = track_namespace
        self.parameters = parameters or {}
    
    def encode(self) -> bytes:
        result = encode_varint(self.msg_type)
        result += encode_tuple(self.track_namespace)
        result += encode_varint(len(self.parameters))
        # TODO: Encode parameters
        return result
    
    @classmethod
    def decode(cls, data: bytes, offset: int = 0) -> Tuple['AnnounceMessage', int]:
        pos = offset
        
        track_namespace, consumed = decode_tuple(data, pos)
        pos += consumed
        
        # Skip parameters for now
        param_count, consumed = decode_varint(data, pos)
        pos += consumed
        
        for _ in range(param_count):
            _, _ = decode_varint(data, pos)
            pos += 1
            param_len, _ = decode_varint(data, pos)
            pos += 1
            pos += param_len
        
        return cls(track_namespace), pos - offset


class AnnounceOkMessage(MOQMessage):
    """ANNOUNCE_OK message"""
    
    def __init__(self, track_namespace: tuple[str, ...]):
        super().__init__(MOQMessageType.ANNOUNCE_OK)
        self.track_namespace = track_namespace
    
    def encode(self) -> bytes:
        result = encode_varint(self.msg_type)
        result += encode_tuple(self.track_namespace)
        return result
    
    @classmethod
    def decode(cls, data: bytes, offset: int = 0) -> Tuple['AnnounceOkMessage', int]:
        track_namespace, consumed = decode_tuple(data, offset)
        return cls(track_namespace), consumed


class AnnounceErrorMessage(MOQMessage):
    """ANNOUNCE_ERROR message"""
    
    def __init__(
        self,
        track_namespace: tuple[str, ...],
        error_code: int,
        reason: str
    ):
        super().__init__(MOQMessageType.ANNOUNCE_ERROR)
        self.track_namespace = track_namespace
        self.error_code = error_code
        self.reason = reason
    
    def encode(self) -> bytes:
        result = encode_varint(self.msg_type)
        result += encode_tuple(self.track_namespace)
        result += encode_varint(self.error_code)
        result += encode_string(self.reason)
        return result
    
    @classmethod
    def decode(cls, data: bytes, offset: int = 0) -> Tuple['AnnounceErrorMessage', int]:
        pos = offset
        
        track_namespace, consumed = decode_tuple(data, pos)
        pos += consumed
        
        error_code, consumed = decode_varint(data, pos)
        pos += consumed
        
        reason, consumed = decode_string(data, pos)
        pos += consumed
        
        return cls(track_namespace, error_code, reason), pos - offset


class UnannounceMessage(MOQMessage):
    """UNANNOUNCE message"""
    
    def __init__(self, track_namespace: tuple[str, ...]):
        super().__init__(MOQMessageType.UNANNOUNCE)
        self.track_namespace = track_namespace
    
    def encode(self) -> bytes:
        result = encode_varint(self.msg_type)
        result += encode_tuple(self.track_namespace)
        return result
    
    @classmethod
    def decode(cls, data: bytes, offset: int = 0) -> Tuple['UnannounceMessage', int]:
        track_namespace, consumed = decode_tuple(data, offset)
        return cls(track_namespace), consumed


class GoawayMessage(MOQMessage):
    """GOAWAY message"""
    
    def __init__(self, new_session_uri: str):
        super().__init__(MOQMessageType.GOAWAY)
        self.new_session_uri = new_session_uri
    
    def encode(self) -> bytes:
        result = encode_varint(self.msg_type)
        result += encode_string(self.new_session_uri)
        return result
    
    @classmethod
    def decode(cls, data: bytes, offset: int = 0) -> Tuple['GoawayMessage', int]:
        new_session_uri, consumed = decode_string(data, offset)
        return cls(new_session_uri), consumed


# Message decoder registry
MESSAGE_DECODERS = {
    MOQMessageType.CLIENT_SETUP: ClientSetupMessage,
    MOQMessageType.SERVER_SETUP: ServerSetupMessage,
    MOQMessageType.SUBSCRIBE: SubscribeMessage,
    MOQMessageType.SUBSCRIBE_OK: SubscribeOkMessage,
    MOQMessageType.SUBSCRIBE_ERROR: SubscribeErrorMessage,
    MOQMessageType.UNSUBSCRIBE: UnsubscribeMessage,
    MOQMessageType.ANNOUNCE: AnnounceMessage,
    MOQMessageType.ANNOUNCE_OK: AnnounceOkMessage,
    MOQMessageType.ANNOUNCE_ERROR: AnnounceErrorMessage,
    MOQMessageType.UNANNOUNCE: UnannounceMessage,
    MOQMessageType.GOAWAY: GoawayMessage,
}


def decode_message(data: bytes, offset: int = 0) -> Tuple[Optional[MOQMessage], int]:
    """
    Decode a MOQ message from bytes.
    
    Returns:
        Tuple of (message, bytes_consumed) or (None, 0) if insufficient data
    """
    if offset >= len(data):
        return None, 0
    
    try:
        msg_type, consumed = decode_varint(data, offset)
    except ValueError:
        return None, 0
    
    if msg_type not in MESSAGE_DECODERS:
        logger.warning(f"Unknown message type: {hex(msg_type)}")
        return None, 0
    
    decoder = MESSAGE_DECODERS[msg_type]
    try:
        message, msg_consumed = decoder.decode(data, offset + consumed)
        return message, consumed + msg_consumed
    except ValueError as e:
        logger.warning(f"Failed to decode message {hex(msg_type)}: {e}")
        return None, 0
