"""
MOQ Protocol Variable-Length Integer Encoding/Decoding
Implements QUIC-style variable-length integer encoding
"""

import struct
from typing import Union


def encode_varint(value: int) -> bytes:
    """
    Encode an integer as a variable-length integer.
    
    Args:
        value: Integer to encode (0 to 2^62-1)
        
    Returns:
        Encoded bytes
        
    Raises:
        ValueError: If value is negative or too large
    """
    if value < 0:
        raise ValueError("Value must be non-negative")
    if value > (1 << 62) - 1:
        raise ValueError("Value too large for varint")
    
    if value < (1 << 6):
        # 1 byte: 00xxxxxx (6 bits)
        return struct.pack("!B", value)
    elif value < (1 << 14):
        # 2 bytes: 01xxxxxx xxxxxxxx (14 bits)
        return struct.pack("!H", value | 0x4000)
    elif value < (1 << 30):
        # 4 bytes: 10xxxxxx xxxxxxxx xxxxxxxx xxxxxxxx (30 bits)
        return struct.pack("!I", value | 0x80000000)
    else:
        # 8 bytes: 11xxxxxx xxxxxxxx ... (62 bits)
        return struct.pack("!Q", value | 0xC000000000000000)


def decode_varint(data: bytes, offset: int = 0) -> tuple[int, int]:
    """
    Decode a variable-length integer from bytes.
    
    Args:
        data: Bytes to decode from
        offset: Starting offset in data
        
    Returns:
        Tuple of (decoded value, bytes consumed)
        
    Raises:
        ValueError: If data is too short or invalid
    """
    if offset >= len(data):
        raise ValueError("Insufficient data for varint")
    
    first_byte = data[offset]
    prefix = (first_byte & 0xC0) >> 6
    
    if prefix == 0:
        # 1 byte
        return first_byte, 1
    elif prefix == 1:
        # 2 bytes
        if offset + 2 > len(data):
            raise ValueError("Insufficient data for 2-byte varint")
        value = struct.unpack("!H", data[offset:offset+2])[0] & 0x3FFF
        return value, 2
    elif prefix == 2:
        # 4 bytes
        if offset + 4 > len(data):
            raise ValueError("Insufficient data for 4-byte varint")
        value = struct.unpack("!I", data[offset:offset+4])[0] & 0x3FFFFFFF
        return value, 4
    else:
        # 8 bytes
        if offset + 8 > len(data):
            raise ValueError("Insufficient data for 8-byte varint")
        value = struct.unpack("!Q", data[offset:offset+8])[0] & 0x3FFFFFFFFFFFFFFF
        return value, 8


def encode_bytes(data: bytes) -> bytes:
    """Encode bytes with length prefix."""
    return encode_varint(len(data)) + data


def decode_bytes(data: bytes, offset: int = 0) -> tuple[bytes, int]:
    """Decode bytes with length prefix."""
    length, consumed = decode_varint(data, offset)
    if offset + consumed + length > len(data):
        raise ValueError("Insufficient data for bytes")
    return data[offset+consumed:offset+consumed+length], consumed + length


def encode_string(s: str) -> bytes:
    """Encode a string with length prefix."""
    return encode_bytes(s.encode('utf-8'))


def decode_string(data: bytes, offset: int = 0) -> tuple[str, int]:
    """Decode a string with length prefix."""
    b, consumed = decode_bytes(data, offset)
    return b.decode('utf-8'), consumed


def encode_tuple(t: tuple) -> bytes:
    """Encode a tuple as varint count followed by elements."""
    result = encode_varint(len(t))
    for item in t:
        if isinstance(item, str):
            result += encode_string(item)
        elif isinstance(item, bytes):
            result += encode_bytes(item)
        elif isinstance(item, int):
            result += encode_varint(item)
        else:
            raise TypeError(f"Unsupported tuple element type: {type(item)}")
    return result


def decode_tuple(data: bytes, offset: int = 0) -> tuple[tuple, int]:
    """Decode a tuple from bytes."""
    count, consumed = decode_varint(data, offset)
    elements = []
    total_consumed = consumed
    
    for _ in range(count):
        # Try string first
        try:
            s, s_consumed = decode_string(data, offset + total_consumed)
            elements.append(s)
            total_consumed += s_consumed
            continue
        except (ValueError, UnicodeDecodeError):
            pass
        
        # Try varint
        try:
            v, v_consumed = decode_varint(data, offset + total_consumed)
            elements.append(v)
            total_consumed += v_consumed
            continue
        except ValueError:
            pass
        
        raise ValueError("Could not decode tuple element")
    
    return tuple(elements), total_consumed


# Type alias for encoded data
MOQEncodedData = Union[bytes, bytearray]
