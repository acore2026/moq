#!/usr/bin/env python3
"""
Unit tests for MOQ Transport encoding module.
"""

import unittest
import sys
sys.path.insert(0, '/home/acn/cxr/moq-py')

from moq.encoding import VarInt, encode_bytes, decode_bytes
from moq.encoding import KeyValuePair, Parameters
from moq.encoding import Location, FullTrackName, TrackAlias


class TestVarInt(unittest.TestCase):
    """Test variable-length integer encoding."""
    
    def test_encode_decode_small(self):
        """Test encoding/decoding small values (1 byte)."""
        for value in [0, 1, 127]:
            encoded = VarInt.encode(value)
            self.assertEqual(len(encoded), 1)
            decoded, consumed = VarInt.decode(encoded)
            self.assertEqual(decoded, value)
            self.assertEqual(consumed, 1)
    
    def test_encode_decode_medium(self):
        """Test encoding/decoding medium values (2 bytes)."""
        for value in [128, 16383]:
            encoded = VarInt.encode(value)
            self.assertEqual(len(encoded), 2)
            decoded, consumed = VarInt.decode(encoded)
            self.assertEqual(decoded, value)
            self.assertEqual(consumed, 2)
    
    def test_encode_decode_large(self):
        """Test encoding/decoding large values."""
        test_values = [
            (16384, 3),
            (2097151, 3),
            (2097152, 4),
            (268435455, 4),
            (34359738367, 5),
            (4398046511103, 6),
            (72057594037927935, 8),
            (18446744073709551615, 9),  # max 64-bit
        ]
        
        for value, expected_bytes in test_values:
            encoded = VarInt.encode(value)
            self.assertEqual(len(encoded), expected_bytes)
            decoded, consumed = VarInt.decode(encoded)
            self.assertEqual(decoded, value)
            self.assertEqual(consumed, expected_bytes)
    
    def test_invalid_values(self):
        """Test handling of invalid values."""
        with self.assertRaises(ValueError):
            VarInt.encode(-1)
        
        with self.assertRaises(ValueError):
            VarInt.encode(2**64)  # Too large


class TestBytesEncoding(unittest.TestCase):
    """Test bytes encoding with length prefix."""
    
    def test_encode_decode_bytes(self):
        """Test encoding/decoding bytes."""
        test_data = [
            b"",
            b"Hello",
            b"\x00\x01\x02\x03",
            b"x" * 1000,
        ]
        
        for data in test_data:
            encoded = encode_bytes(data)
            decoded, consumed = decode_bytes(encoded)
            self.assertEqual(decoded, data)
    
    def test_decode_bytes_with_offset(self):
        """Test decoding bytes with offset."""
        prefix = b"prefix"
        data = b"test data"
        
        combined = prefix + encode_bytes(data)
        decoded, consumed = decode_bytes(combined, offset=len(prefix))
        
        self.assertEqual(decoded, data)


class TestKeyValuePair(unittest.TestCase):
    """Test Key-Value Pair encoding."""
    
    def test_encode_decode_varint_value(self):
        """Test KVP with varint value."""
        kvp = KeyValuePair(key_type=2, value=42)
        encoded = kvp.encode()
        
        decoded, consumed = KeyValuePair.decode(encoded)
        self.assertEqual(decoded.key_type, 2)
        self.assertEqual(decoded.value, 42)
    
    def test_encode_decode_bytes_value(self):
        """Test KVP with bytes value."""
        kvp = KeyValuePair(key_type=3, value=b"test data")
        encoded = kvp.encode()
        
        decoded, consumed = KeyValuePair.decode(encoded)
        self.assertEqual(decoded.key_type, 3)
        self.assertEqual(decoded.value, b"test data")
    
    def test_delta_encoding(self):
        """Test delta encoding of multiple KVPs."""
        kvp1 = KeyValuePair(key_type=2, value=100)
        kvp2 = KeyValuePair(key_type=4, value=200)  # delta = 2 (even type for varint)
        
        encoded = kvp1.encode(prev_type=0) + kvp2.encode(prev_type=2)
        
        # Decode
        d1, c1 = KeyValuePair.decode(encoded, offset=0, prev_type=0)
        d2, c2 = KeyValuePair.decode(encoded, offset=c1, prev_type=d1.key_type)
        
        self.assertEqual(d1.key_type, 2)
        self.assertEqual(d2.key_type, 4)


class TestParameters(unittest.TestCase):
    """Test Parameters collection."""
    
    def test_empty_parameters(self):
        """Test empty parameters."""
        params = Parameters()
        encoded = params.encode()
        
        decoded, consumed = Parameters.decode(encoded)
        self.assertEqual(len(decoded.params), 0)
    
    def test_parameters_with_values(self):
        """Test parameters with mixed values."""
        params = Parameters()
        params.set(2, 42)  # varint
        params.set(3, b"test")  # bytes
        params.set(10, 999)
        
        encoded = params.encode()
        decoded, consumed = Parameters.decode(encoded)
        
        self.assertEqual(decoded.get(2), 42)
        self.assertEqual(decoded.get(3), b"test")
        self.assertEqual(decoded.get(10), 999)


class TestLocation(unittest.TestCase):
    """Test Location structure."""
    
    def test_encode_decode(self):
        """Test encoding/decoding location."""
        loc = Location(group=5, object_id=10)
        encoded = loc.encode()
        
        decoded, consumed = Location.decode(encoded)
        self.assertEqual(decoded.group, 5)
        self.assertEqual(decoded.object_id, 10)
    
    def test_location_comparison(self):
        """Test location comparison."""
        loc1 = Location(group=1, object_id=1)
        loc2 = Location(group=1, object_id=2)
        loc3 = Location(group=2, object_id=1)
        
        self.assertTrue(loc1 < loc2)
        self.assertTrue(loc2 < loc3)
        self.assertTrue(loc1 < loc3)
        
        self.assertEqual(loc1, Location(group=1, object_id=1))


class TestFullTrackName(unittest.TestCase):
    """Test FullTrackName structure."""
    
    def test_encode_decode(self):
        """Test encoding/decoding track name."""
        namespace = [b"example", b"namespace"]
        name = FullTrackName(namespace, b"track")
        
        encoded = name.encode()
        decoded, consumed = FullTrackName.decode(encoded)
        
        self.assertEqual(decoded.namespace, namespace)
        self.assertEqual(decoded.track_name, b"track")
    
    def test_empty_namespace(self):
        """Test track with empty namespace."""
        name = FullTrackName([], b"track")
        
        encoded = name.encode()
        decoded, consumed = FullTrackName.decode(encoded)
        
        self.assertEqual(decoded.namespace, [])
        self.assertEqual(decoded.track_name, b"track")
    
    def test_invalid_namespace_field(self):
        """Test validation of empty namespace field."""
        with self.assertRaises(ValueError):
            FullTrackName([b""], b"track")  # Empty field
    
    def test_too_many_namespace_fields(self):
        """Test validation of too many namespace fields."""
        with self.assertRaises(ValueError):
            FullTrackName([b"x"] * 33, b"track")  # Too many fields
    
    def test_track_too_long(self):
        """Test validation of track name too long."""
        with self.assertRaises(ValueError):
            FullTrackName([b"x" * 4096], b"track")  # Exceeds max length
    
    def test_to_string(self):
        """Test safe string representation."""
        name = FullTrackName([b"example_net", b"team2"], b"project_x")
        str_repr = name.to_string()
        
        # Should contain namespace and track name
        self.assertIn("example_net", str_repr)
        self.assertIn("team2", str_repr)


class TestTrackAlias(unittest.TestCase):
    """Test TrackAlias structure."""
    
    def test_encode_decode(self):
        """Test encoding/decoding track alias."""
        alias = TrackAlias(123)
        encoded = alias.encode()
        
        decoded, consumed = TrackAlias.decode(encoded)
        self.assertEqual(decoded.alias, 123)


if __name__ == '__main__':
    unittest.main()
