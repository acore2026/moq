"""
MOQ Transport - Key-Value Pair encoding as per draft-ietf-moq-transport-17.
"""

from typing import List, Dict, Tuple, Optional, Union
from .varint import VarInt


class KeyValuePair:
    """
    Key-Value-Pair structure for MOQT.
    Used for parameters, properties, and extensions.
    """
    
    def __init__(self, key_type: int, value: Union[int, bytes]):
        """
        Initialize a key-value pair.
        
        Args:
            key_type: The parameter/property type (even for varint, odd for bytes)
            value: The value (int for even types, bytes for odd types)
        """
        self.key_type = key_type
        self.value = value
    
    def encode(self, prev_type: int = 0) -> bytes:
        """
        Encode the key-value pair with delta encoding.
        
        Args:
            prev_type: The previous type value for delta encoding
            
        Returns:
            Encoded bytes
        """
        delta = self.key_type - prev_type
        if delta < 0:
            raise ValueError(f"Delta type must be non-negative: {delta}")
        
        encoded = VarInt.encode(delta)
        
        if self.key_type % 2 == 0:  # Even type: varint value
            if not isinstance(self.value, int):
                raise ValueError(f"Even type {self.key_type} requires int value")
            encoded += VarInt.encode(self.value)
        else:  # Odd type: bytes value with length
            if not isinstance(self.value, bytes):
                raise ValueError(f"Odd type {self.key_type} requires bytes value")
            if len(self.value) > 65535:
                raise ValueError(f"Value too long: {len(self.value)} > 65535")
            encoded += VarInt.encode(len(self.value)) + self.value
        
        return encoded
    
    @staticmethod
    def decode(data: bytes, offset: int = 0, prev_type: int = 0) -> Tuple['KeyValuePair', int]:
        """
        Decode a key-value pair from bytes.
        
        Args:
            data: The bytes to decode from
            offset: Starting offset
            prev_type: Previous type for delta decoding
            
        Returns:
            Tuple of (KeyValuePair, bytes_consumed)
        """
        delta, consumed = VarInt.decode(data, offset)
        key_type = prev_type + delta
        
        if key_type % 2 == 0:  # Even type: varint value
            value, value_consumed = VarInt.decode(data, offset + consumed)
            consumed += value_consumed
        else:  # Odd type: bytes value
            length, length_consumed = VarInt.decode(data, offset + consumed)
            if length > 65535:
                raise ValueError(f"Value length too large: {length}")
            value = data[offset + consumed + length_consumed:offset + consumed + length_consumed + length]
            consumed += length_consumed + length
        
        return KeyValuePair(key_type, value), consumed
    
    def __repr__(self):
        return f"KeyValuePair(type={self.key_type}, value={self.value})"


class Parameters:
    """Collection of key-value parameters."""
    
    def __init__(self):
        self.params: Dict[int, Union[int, bytes]] = {}
    
    def set(self, key_type: int, value: Union[int, bytes]) -> None:
        """Set a parameter value."""
        self.params[key_type] = value
    
    def get(self, key_type: int, default=None) -> Optional[Union[int, bytes]]:
        """Get a parameter value."""
        return self.params.get(key_type, default)
    
    def encode(self) -> bytes:
        """Encode all parameters."""
        if not self.params:
            return VarInt.encode(0)
        
        # Sort by type for consistent encoding
        sorted_types = sorted(self.params.keys())
        
        encoded = VarInt.encode(len(self.params))
        prev_type = 0
        for key_type in sorted_types:
            kvp = KeyValuePair(key_type, self.params[key_type])
            encoded += kvp.encode(prev_type)
            prev_type = key_type
        
        return encoded
    
    @staticmethod
    def decode(data: bytes, offset: int = 0) -> Tuple['Parameters', int]:
        """Decode parameters from bytes."""
        count, consumed = VarInt.decode(data, offset)
        params = Parameters()
        prev_type = 0
        
        for _ in range(count):
            kvp, kvp_consumed = KeyValuePair.decode(data, offset + consumed, prev_type)
            params.params[kvp.key_type] = kvp.value
            prev_type = kvp.key_type
            consumed += kvp_consumed
        
        return params, consumed
    
    def __repr__(self):
        return f"Parameters({self.params})"
