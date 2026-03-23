"""
MOQ Protocol Constants and Message Types
Implements draft-ietf-moq-transport-17 message types and constants
"""

from enum import IntEnum
from typing import Final


class MOQMessageType(IntEnum):
    """MOQ Control Message Types (draft-ietf-moq-transport-17 Section 6)"""
    
    # Setup Messages
    CLIENT_SETUP = 0x40
    SERVER_SETUP = 0x41
    
    # Announcement Namespace Messages
    ANNOUNCE = 0xC0
    ANNOUNCE_OK = 0xC1
    ANNOUNCE_ERROR = 0xC2
    ANNOUNCE_CANCEL = 0xC3
    UNANNOUNCE = 0xC4
    
    # Namespace Subscription Messages
    SUBSCRIBE_NAMESPACE = 0xD0
    SUBSCRIBE_NAMESPACE_OK = 0xD1
    SUBSCRIBE_NAMESPACE_ERROR = 0xD2
    UNSUBSCRIBE_NAMESPACE = 0xD3
    
    # Track Subscription Messages
    SUBSCRIBE = 0x80
    SUBSCRIBE_OK = 0x81
    SUBSCRIBE_ERROR = 0x82
    UNSUBSCRIBE = 0x83
    SUBSCRIBE_UPDATE = 0x84
    SUBSCRIBE_DONE = 0x85
    
    # Fetch Messages
    FETCH = 0xE0
    FETCH_OK = 0xE1
    FETCH_ERROR = 0xE2
    FETCH_CANCEL = 0xE3
    
    # Request Processing Messages
    MAX_SUBSCRIBE_ID = 0x90
    SUBSCRIBES_BLOCKED = 0x91
    
    # Object Stream Headers
    OBJECT_DATAGRAM = 0x01
    STREAM_HEADER_TRACK = 0x50
    STREAM_HEADER_SUBGROUP = 0x51
    
    # Session Termination
    GOAWAY = 0x10
    CLOSE_WEBTRANSPORT_SESSION = 0x11


class MOQErrorCode(IntEnum):
    """MOQ Error Codes (draft-ietf-moq-transport-17 Section 6.2)"""
    
    # Setup Errors
    INTERNAL_ERROR = 0x0000
    UNAUTHORIZED = 0x0001
    PROTOCOL_VIOLATION = 0x0002
    DUP_TRACK_ALIAS = 0x0003
    PARAM_LENGTH_MISMATCH = 0x0004
    
    # Subscription Errors
    SUBSCRIPTION_EXISTS = 0x0100
    INVALID_RANGE = 0x0101
    SUBSCRIBE_RETRY = 0x0102
    SUBSCRIBE_GONE = 0x0103
    SUBSCRIBE_CLOSED = 0x0104
    SUBSCRIBE_REJECTED = 0x0105
    
    # Announce Errors
    ANNOUNCE_EXISTS = 0x0200
    INVALID_TRACK_ALIAS = 0x0201
    ANNOUNCE_GONE = 0x0202
    
    # Fetch Errors
    FETCH_EXCEEDED = 0x0300
    FETCH_NOT_FOUND = 0x0301
    FETCH_PREVIOUS_RANGE = 0x0302


class MOQRole(IntEnum):
    """MOQ Role Parameter (draft-ietf-moq-transport-17 Section 8.1)"""
    
    PUBLISHER = 0x01
    SUBSCRIBER = 0x02
    PUB_SUB = 0x03


class MOQDeliveryPreference(IntEnum):
    """Object delivery preferences"""
    
    DATAGRAM = 0x01      # Unreliable datagram delivery
    STREAM = 0x02        # Reliable stream delivery
    SUBGROUP = 0x03      # Per-subgroup stream delivery


class MOQFilterType(IntEnum):
    """Subscription filter types"""
    
    LATEST_GROUP = 0x01
    LATEST_OBJECT = 0x02
    ABSOLUTE_START = 0x03
    ABSOLUTE_RANGE = 0x04


# Version constants
MOQ_VERSION_DRAFT_17: Final[int] = 0xff000011
MOQ_VERSIONS_SUPPORTED: Final[list] = [MOQ_VERSION_DRAFT_17]

# Protocol defaults
MOQ_DEFAULT_MAX_SUBSCRIBE_ID: Final[int] = 100
MOQ_DEFAULT_MAX_CACHE_SIZE: Final[int] = 1024 * 1024 * 100  # 100MB
MOQ_DEFAULT_CACHE_DIR: Final[str] = ".moq_cache"
MOQ_MAX_OBJECT_SIZE: Final[int] = 65535
MOQ_MAX_NAMESPACE_LENGTH: Final[int] = 256
MOQ_MAX_TRACK_NAME_LENGTH: Final[int] = 256

# Stream types for object delivery
OBJECT_STREAM_HEADER_DATAGRAM: Final[int] = 0x01
OBJECT_STREAM_HEADER_TRACK: Final[int] = 0x50
OBJECT_STREAM_HEADER_SUBGROUP: Final[int] = 0x51
