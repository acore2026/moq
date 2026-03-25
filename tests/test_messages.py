#!/usr/bin/env python3
"""
Unit tests for MOQ Transport messages module.
"""

import unittest

from _path_helper import ensure_repo_root

ensure_repo_root()

from moq.messages import (
    SetupMessage, GoAwayMessage,
    SubscribeMessage, SubscribeOkMessage,
    PublishMessage, PublishOkMessage, PublishDoneMessage,
    FetchMessage, FetchOkMessage,
    RequestErrorMessage,
    ErrorCode, GroupOrder, SubscribeFilter
)
from moq.encoding import FullTrackName, Parameters


class TestSetupMessage(unittest.TestCase):
    """Test SETUP message."""
    
    def test_encode_decode(self):
        """Test encoding/decoding SETUP message."""
        params = Parameters()
        msg = SetupMessage(version=0xFF000011, role=0x03, parameters=params)
        
        encoded = msg.encode()
        decoded, consumed = SetupMessage.decode(encoded, 2)  # Skip message type and length
        
        self.assertEqual(decoded.version, 0xFF000011)
        self.assertEqual(decoded.role, 0x03)


class TestSubscribeMessage(unittest.TestCase):
    """Test SUBSCRIBE message."""
    
    def test_encode_decode_latest_object(self):
        """Test SUBSCRIBE with LATEST_OBJECT filter."""
        track_name = FullTrackName([b"test"], b"track")
        
        msg = SubscribeMessage(
            request_id=1,
            track_alias=10,
            full_track_name=track_name,
            subscriber_priority=128,
            group_order=GroupOrder.ASCENDING,
            filter_type=SubscribeFilter.LATEST_OBJECT
        )
        
        encoded = msg.encode()
        # Skip message type and length for decoding test
        length_bytes = encoded[1:2]  # Usually 1 byte for small messages
        decoded, consumed = SubscribeMessage.decode(encoded, 2)
        
        self.assertEqual(decoded.request_id, 1)
        self.assertEqual(decoded.track_alias, 10)
        self.assertEqual(decoded.subscriber_priority, 128)
        self.assertEqual(decoded.group_order, GroupOrder.ASCENDING)
        self.assertEqual(decoded.filter_type, SubscribeFilter.LATEST_OBJECT)
    
    def test_encode_decode_absolute_start(self):
        """Test SUBSCRIBE with ABSOLUTE_START filter."""
        track_name = FullTrackName([b"test"], b"track")
        
        msg = SubscribeMessage(
            request_id=2,
            track_alias=20,
            full_track_name=track_name,
            subscriber_priority=64,
            group_order=GroupOrder.DESCENDING,
            filter_type=SubscribeFilter.ABSOLUTE_START,
            start_group=5,
            start_object=10
        )
        
        encoded = msg.encode()
        decoded, consumed = SubscribeMessage.decode(encoded, 2)
        
        self.assertEqual(decoded.filter_type, SubscribeFilter.ABSOLUTE_START)
        self.assertEqual(decoded.start_group, 5)
        self.assertEqual(decoded.start_object, 10)


class TestSubscribeOkMessage(unittest.TestCase):
    """Test SUBSCRIBE_OK message."""
    
    def test_encode_decode(self):
        """Test encoding/decoding SUBSCRIBE_OK."""
        msg = SubscribeOkMessage(
            request_id=1,
            expires=0,
            group_order=GroupOrder.ASCENDING
        )
        
        encoded = msg.encode()
        decoded, consumed = SubscribeOkMessage.decode(encoded, 2)
        
        self.assertEqual(decoded.request_id, 1)
        self.assertEqual(decoded.expires, 0)
        self.assertEqual(decoded.group_order, GroupOrder.ASCENDING)


class TestPublishMessage(unittest.TestCase):
    """Test PUBLISH message."""
    
    def test_encode_decode(self):
        """Test encoding/decoding PUBLISH."""
        track_name = FullTrackName([b"test"], b"track")
        
        msg = PublishMessage(
            request_id=1,
            track_alias=10,
            full_track_name=track_name
        )
        
        encoded = msg.encode()
        decoded, consumed = PublishMessage.decode(encoded, 2)
        
        self.assertEqual(decoded.request_id, 1)
        self.assertEqual(decoded.track_alias, 10)


class TestFetchMessage(unittest.TestCase):
    """Test FETCH message."""
    
    def test_encode_decode(self):
        """Test encoding/decoding FETCH."""
        track_name = FullTrackName([b"test"], b"track")
        
        msg = FetchMessage(
            request_id=1,
            full_track_name=track_name,
            subscriber_priority=128,
            group_order=GroupOrder.ASCENDING,
            start_group=1,
            start_object=1,
            end_group=10,
            end_object=100
        )
        
        encoded = msg.encode()
        decoded, consumed = FetchMessage.decode(encoded, 2)
        
        self.assertEqual(decoded.request_id, 1)
        self.assertEqual(decoded.start_group, 1)
        self.assertEqual(decoded.start_object, 1)
        self.assertEqual(decoded.end_group, 10)
        self.assertEqual(decoded.end_object, 100)


class TestRequestErrorMessage(unittest.TestCase):
    """Test REQUEST_ERROR message."""
    
    def test_encode_decode(self):
        """Test encoding/decoding REQUEST_ERROR."""
        msg = RequestErrorMessage(
            request_id=1,
            error_code=ErrorCode.UNAUTHORIZED,
            reason="Authentication failed"
        )
        
        encoded = msg.encode()
        decoded, consumed = RequestErrorMessage.decode(encoded, 2)
        
        self.assertEqual(decoded.request_id, 1)
        self.assertEqual(decoded.error_code, ErrorCode.UNAUTHORIZED)
        self.assertEqual(decoded.reason, "Authentication failed")


class TestGoAwayMessage(unittest.TestCase):
    """Test GOAWAY message."""
    
    def test_encode_decode(self):
        """Test encoding/decoding GOAWAY."""
        msg = GoAwayMessage(new_session_uri="moq://new.example.com")
        
        encoded = msg.encode()
        decoded, consumed = GoAwayMessage.decode(encoded, 2)
        
        self.assertEqual(decoded.new_session_uri, "moq://new.example.com")


if __name__ == '__main__':
    unittest.main()
