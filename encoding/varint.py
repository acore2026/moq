"""
MOQ Transport - Base encoding utilities for varint and data types.
Implements variable-length integer encoding as per draft-ietf-moq-transport-17.
"""

from typing import Optional, Tuple
import struct


class VarInt:
    """Variable-length integer encoding for MOQT."""
    
    @staticmethod
    def encode(value: int) -> bytes:
        """
        Encode an integer using variable-length encoding.
        
        Args:
            value: The integer to encode (0 to 2^64-1)
            
        Returns:
            Encoded bytes
            
        Raises:
            ValueError: If value is negative or exceeds 64-bit range
        """
        if value < 0:
            raise ValueError(f"Cannot encode negative value: {value}")
        if value > 0xFFFFFFFFFFFFFFFF:
            raise ValueError(f"Value exceeds 64-bit range: {value}")
        
        if value <= 0x7F:  # 1 byte: 0xxxxxxx
            return bytes([value])
        elif value <= 0x3FFF:  # 2 bytes: 10xxxxxx + 1 byte
            return bytes([0x80 | (value >> 8), value & 0xFF])
        elif value <= 0x1FFFFF:  # 3 bytes: 110xxxxx + 2 bytes
            return bytes([0xC0 | (value >> 16), (value >> 8) & 0xFF, value & 0xFF])
        elif value <= 0x0FFFFFFF:  # 4 bytes: 1110xxxx + 3 bytes
            return bytes([0xE0 | (value >> 24), (value >> 16) & 0xFF, 
                         (value >> 8) & 0xFF, value & 0xFF])
        elif value <= 0x7FFFFFFFF:  # 5 bytes: 11110xxx + 4 bytes
            return bytes([0xF0 | (value >> 32), (value >> 24) & 0xFF,
                         (value >> 16) & 0xFF, (value >> 8) & 0xFF, value & 0xFF])
        elif value <= 0x3FFFFFFFFFF:  # 6 bytes: 111110xx + 5 bytes
            return bytes([0xF8 | (value >> 40), (value >> 32) & 0xFF,
                         (value >> 24) & 0xFF, (value >> 16) & 0xFF,
                         (value >> 8) & 0xFF, value & 0xFF])
        elif value <= 0x00FFFFFFFFFFFFFF:  # 8 bytes: 11111110 + 7 bytes
            return bytes([0xFE]) + struct.pack('>Q', value)[1:]
        else:  # 9 bytes: 11111111 + 8 bytes
            return bytes([0xFF]) + struct.pack('>Q', value)
    
    @staticmethod
    def decode(data: bytes, offset: int = 0) -> Tuple[int, int]:
        """
        Decode a variable-length integer from bytes.
        
        Args:
            data: The bytes to decode from
            offset: Starting offset in the data
            
        Returns:
            Tuple of (decoded_value, bytes_consumed)
            
        Raises:
            ValueError: If data is invalid or incomplete
        """
        if offset >= len(data):
            raise ValueError("Insufficient data for varint")
        
        first_byte = data[offset]
        
        # Determine encoding length from leading bits
        if (first_byte & 0x80) == 0:  # 1 byte
            return first_byte, 1
        elif (first_byte & 0xC0) == 0x80:  # 2 bytes: 10xxxxxx
            if offset + 2 > len(data):
                raise ValueError("Insufficient data for 2-byte varint")
            return ((first_byte & 0x3F) << 8) | data[offset + 1], 2
        elif (first_byte & 0xE0) == 0xC0:  # 3 bytes: 110xxxxx
            if offset + 3 > len(data):
                raise ValueError("Insufficient data for 3-byte varint")
            return ((first_byte & 0x1F) << 16) | (data[offset + 1] << 8) | data[offset + 2], 3
        elif (first_byte & 0xF0) == 0xE0:  # 4 bytes: 1110xxxx
            if offset + 4 > len(data):
                raise ValueError("Insufficient data for 4-byte varint")
            return ((first_byte & 0x0F) << 24) | (data[offset + 1] << 16) | \
                   (data[offset + 2] << 8) | data[offset + 3], 4
        elif (first_byte & 0xF8) == 0xF0:  # 5 bytes: 11110xxx
            if offset + 5 > len(data):
                raise ValueError("Insufficient data for 5-byte varint")
            return ((first_byte & 0x07) << 32) | (data[offset + 1] << 24) | \
                   (data[offset + 2] << 16) | (data[offset + 3] << 8) | data[offset + 4], 5
        elif (first_byte & 0xFC) == 0xF8:  # 6 bytes: 111110xx
            if offset + 6 > len(data):
                raise ValueError("Insufficient data for 6-byte varint")
            return ((first_byte & 0x03) << 40) | (data[offset + 1] << 32) | \
                   (data[offset + 2] << 24) | (data[offset + 3] << 16) | \
                   (data[offset + 4] << 8) | data[offset + 5], 6
        elif first_byte == 0xFE:  # 8 bytes: 11111110
            if offset + 8 > len(data):
                raise ValueError("Insufficient data for 8-byte varint")
            return (data[offset + 1] << 48) | (data[offset + 2] << 40) | \
                   (data[offset + 3] << 32) | (data[offset + 4] << 24) | \
                   (data[offset + 5] << 16) | (data[offset + 6] << 8) | data[offset + 7], 8
        elif first_byte == 0xFF:  # 9 bytes: 11111111
            if offset + 9 > len(data):
                raise ValueError("Insufficient data for 9-byte varint")
            return struct.unpack('>Q', data[offset + 1:offset + 9])[0], 9
        elif first_byte == 0xFC:
            raise ValueError("Invalid varint code point: 0xFC")
        else:
            raise ValueError(f"Unknown varint prefix: {first_byte:02x}")


def encode_bytes(data: bytes) -> bytes:
    """
    Encode bytes with length prefix.
    
    Args:
        data: The bytes to encode
        
    Returns:
        Length-prefixed bytes
    """
    return VarInt.encode(len(data)) + data


def decode_bytes(data: bytes, offset: int = 0) -> Tuple[bytes, int]:
    """
    Decode bytes with length prefix.
    
    Args:
        data: The bytes to decode from
        offset: Starting offset in the data
        
    Returns:
        Tuple of (decoded_bytes, bytes_consumed)
    """
    length, consumed = VarInt.decode(data, offset)
    if offset + consumed + length > len(data):
        raise ValueError("Insufficient data for bytes field")
    return data[offset + consumed:offset + consumed + length], consumed + length
