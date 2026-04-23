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
    RequestOkMessage, RequestErrorMessage, RequestUpdateMessage,
    ErrorCode, GroupOrder, SubscribeFilter, SubscriptionFilterValue,
    UNKNOWN_PUBLISH_DONE_STREAM_COUNT,
    group_order_from_parameter_value, group_order_to_parameter_value,
    decode_control_message
)
from moq.encoding import FullTrackName, Parameters
from moq.encoding import Location
from moq.messages.data import ObjectHeader, ObjectDatagram, FetchObject, SubgroupObject, ObjectStatus


class TestSetupMessage(unittest.TestCase):
    """Test SETUP message."""
    
    def test_encode_decode(self):
        """Test encoding/decoding SETUP message."""
        params = Parameters()
        msg = SetupMessage(version=0xFF000011, role=0x03, parameters=params)
        
        encoded = msg.encode()
        decoded, consumed = decode_control_message(encoded)
        
        self.assertEqual(decoded.version, 0xFF000011)
        self.assertEqual(decoded.role, 0x03)


class TestSubscribeMessage(unittest.TestCase):
    """Test SUBSCRIBE message."""
    
    def test_encode_decode_latest_object(self):
        """Test SUBSCRIBE with LATEST_OBJECT filter."""
        track_name = FullTrackName([b"test"], b"track")
        
        msg = SubscribeMessage(
            request_id=1,
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
        self.assertEqual(decoded.required_request_id_delta, 0)
        self.assertEqual(decoded.subscriber_priority, 128)
        self.assertEqual(decoded.group_order, GroupOrder.ASCENDING)
        self.assertEqual(decoded.filter_type, SubscribeFilter.LATEST_OBJECT)
    
    def test_encode_decode_absolute_start(self):
        """Test SUBSCRIBE with ABSOLUTE_START filter."""
        track_name = FullTrackName([b"test"], b"track")
        
        msg = SubscribeMessage(
            request_id=2,
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
            track_alias=9,
        )
        
        encoded = msg.encode()
        decoded, consumed = SubscribeOkMessage.decode(encoded, 2)
        
        self.assertEqual(decoded.request_id, 1)
        self.assertEqual(decoded.track_alias, 9)

    def test_decode_without_request_id_uses_stream_context(self):
        msg = SubscribeOkMessage(
            request_id=1,
            track_alias=9,
        )

        encoded = msg.encode(include_request_id=False)
        decoded, consumed = decode_control_message(encoded, response_request_id=1)

        self.assertEqual(decoded.request_id, 1)
        self.assertEqual(decoded.track_alias, 9)
        self.assertEqual(consumed, len(encoded))


class TestPublishMessage(unittest.TestCase):
    """Test PUBLISH message."""
    
    def test_encode_decode(self):
        """Test encoding/decoding PUBLISH."""
        track_name = FullTrackName([b"test"], b"track")
        
        msg = PublishMessage(
            request_id=1,
            required_request_id_delta=0,
            track_alias=10,
            full_track_name=track_name
        )
        
        encoded = msg.encode()
        decoded, consumed = PublishMessage.decode(encoded, 2)
        
        self.assertEqual(decoded.request_id, 1)
        self.assertEqual(decoded.required_request_id_delta, 0)
        self.assertEqual(decoded.track_alias, 10)


class TestPublishOkMessage(unittest.TestCase):
    """Test PUBLISH_OK message."""

    def test_encode_decode(self):
        params = Parameters()
        params.set(0x10, 1)
        msg = PublishOkMessage(
            request_id=3,
            parameters=params,
        )

        encoded = msg.encode()
        decoded, consumed = PublishOkMessage.decode(encoded, 2)

        self.assertEqual(decoded.request_id, 3)
        self.assertEqual(decoded.parameters.get(0x10), 1)

    def test_decode_without_request_id_uses_stream_context(self):
        params = Parameters()
        params.set(0x10, 1)
        msg = PublishOkMessage(
            request_id=3,
            parameters=params,
        )

        encoded = msg.encode(include_request_id=False)
        decoded, consumed = decode_control_message(encoded, response_request_id=3)

        self.assertEqual(decoded.request_id, 3)
        self.assertEqual(decoded.parameters.get(0x10), 1)
        self.assertEqual(consumed, len(encoded))


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
        self.assertEqual(decoded.required_request_id_delta, 0)


class TestFetchOkMessage(unittest.TestCase):
    """Test FETCH_OK message."""

    def test_encode_decode(self):
        params = Parameters()
        params.set(0x08, 30)
        msg = FetchOkMessage(
            request_id=6,
            end_of_track=True,
            end_location=Location(10, 101),
            parameters=params,
            track_properties=b"props",
        )

        encoded = msg.encode()
        decoded, consumed = FetchOkMessage.decode(encoded, 2)

        self.assertEqual(decoded.request_id, 6)
        self.assertTrue(decoded.end_of_track)
        self.assertEqual(decoded.end_location.group, 10)
        self.assertEqual(decoded.end_location.object_id, 101)
        self.assertEqual(decoded.parameters.get(0x08), 30)
        self.assertEqual(decoded.track_properties, b"props")

    def test_decode_without_request_id_uses_stream_context(self):
        msg = FetchOkMessage(
            request_id=6,
            end_of_track=False,
            end_location=Location(1, 2),
        )

        encoded = msg.encode(include_request_id=False)
        decoded, consumed = decode_control_message(encoded, response_request_id=6)

        self.assertEqual(decoded.request_id, 6)
        self.assertFalse(decoded.end_of_track)
        self.assertEqual(decoded.end_location.group, 1)
        self.assertEqual(decoded.end_location.object_id, 2)
        self.assertEqual(consumed, len(encoded))


class TestRequestUpdateMessage(unittest.TestCase):
    """Test REQUEST_UPDATE message."""

    def test_encode_decode(self):
        params = Parameters()
        params.set(0x20, 7)
        msg = RequestUpdateMessage(
            request_id=5,
            required_request_id_delta=2,
            parameters=params,
        )

        encoded = msg.encode()
        decoded, consumed = RequestUpdateMessage.decode(encoded, 2)

        self.assertEqual(decoded.request_id, 5)
        self.assertEqual(decoded.required_request_id_delta, 2)
        self.assertEqual(decoded.parameters.get(0x20), 7)


class TestSubscriptionFilterValue(unittest.TestCase):
    """Test SUBSCRIPTION_FILTER parameter encoding."""

    def test_absolute_range_encode_decode(self):
        value = SubscriptionFilterValue(
            filter_type=SubscribeFilter.ABSOLUTE_RANGE,
            start_group=3,
            start_object=5,
            end_group_delta=2,
        )

        encoded = value.encode()
        decoded, consumed = SubscriptionFilterValue.decode(encoded)

        self.assertEqual(decoded.filter_type, SubscribeFilter.ABSOLUTE_RANGE)
        self.assertEqual(decoded.start_group, 3)
        self.assertEqual(decoded.start_object, 5)
        self.assertEqual(decoded.end_group_delta, 2)
        self.assertEqual(consumed, len(encoded))

    def test_group_order_parameter_mapping(self):
        self.assertEqual(group_order_to_parameter_value(GroupOrder.ASCENDING), 0x1)
        self.assertEqual(group_order_to_parameter_value(GroupOrder.DESCENDING), 0x2)
        self.assertEqual(group_order_from_parameter_value(0x1), GroupOrder.ASCENDING)
        self.assertEqual(group_order_from_parameter_value(0x2), GroupOrder.DESCENDING)


class TestRequestOkMessage(unittest.TestCase):
    """Test REQUEST_OK message."""

    def test_encode_decode(self):
        params = Parameters()
        params.set(0x20, 9)
        msg = RequestOkMessage(
            request_id=4,
            parameters=params,
        )

        encoded = msg.encode()
        decoded, consumed = RequestOkMessage.decode(encoded, 2)

        self.assertEqual(decoded.request_id, 4)
        self.assertEqual(decoded.parameters.get(0x20), 9)

    def test_decode_without_request_id_uses_stream_context(self):
        params = Parameters()
        params.set(0x20, 9)
        msg = RequestOkMessage(
            request_id=4,
            parameters=params,
        )

        encoded = msg.encode(include_request_id=False)
        decoded, consumed = decode_control_message(encoded, response_request_id=4)

        self.assertEqual(decoded.request_id, 4)
        self.assertEqual(decoded.parameters.get(0x20), 9)
        self.assertEqual(consumed, len(encoded))


class TestDataMessages(unittest.TestCase):
    """Test data message encoding/decoding."""

    def test_object_datagram_encode_decode(self):
        header = ObjectHeader(
            track_alias=7,
            group_id=3,
            object_id=9,
            publisher_priority=128,
        )
        msg = ObjectDatagram(header=header, payload=b"payload")

        encoded = msg.encode()
        decoded, consumed = ObjectDatagram.decode(encoded)

        self.assertEqual(decoded.header.track_alias, 7)
        self.assertEqual(decoded.header.group_id, 3)
        self.assertEqual(decoded.header.object_id, 9)
        self.assertEqual(decoded.payload, b"payload")
        self.assertEqual(consumed, len(encoded))

    def test_fetch_object_encode_decode(self):
        msg = FetchObject(
            group_id=5,
            object_id=11,
            publisher_priority=200,
            payload=b"cached",
        )

        encoded = msg.encode()
        decoded, consumed = FetchObject.decode(encoded)

        self.assertEqual(decoded.group_id, 5)
        self.assertEqual(decoded.object_id, 11)
        self.assertEqual(decoded.publisher_priority, 200)
        self.assertEqual(decoded.payload, b"cached")
        self.assertEqual(consumed, len(encoded))

    def test_subgroup_object_delta_encode_decode(self):
        first = SubgroupObject(object_id=7, payload=b"first")
        second = SubgroupObject(object_id=8, payload=b"second")
        status = SubgroupObject(object_id=10, payload=b"", object_status=ObjectStatus.END_OF_GROUP)

        encoded_first = first.encode()
        encoded_second = second.encode(previous_object_id=first.object_id)
        encoded_status = status.encode(previous_object_id=second.object_id)

        decoded_first, consumed_first = SubgroupObject.decode(encoded_first)
        decoded_second, consumed_second = SubgroupObject.decode(
            encoded_second,
            previous_object_id=decoded_first.object_id,
        )
        decoded_status, consumed_status = SubgroupObject.decode(
            encoded_status,
            previous_object_id=decoded_second.object_id,
        )

        self.assertEqual(decoded_first.object_id, 7)
        self.assertEqual(decoded_first.payload, b"first")
        self.assertEqual(decoded_second.object_id, 8)
        self.assertEqual(decoded_second.payload, b"second")
        self.assertEqual(decoded_status.object_id, 10)
        self.assertEqual(decoded_status.object_status, ObjectStatus.END_OF_GROUP)
        self.assertEqual(consumed_first, len(encoded_first))
        self.assertEqual(consumed_second, len(encoded_second))
        self.assertEqual(consumed_status, len(encoded_status))


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
        self.assertEqual(decoded.retry_interval, 0)
        self.assertEqual(decoded.reason, "Authentication failed")

    def test_decode_without_request_id_uses_stream_context(self):
        msg = RequestErrorMessage(
            request_id=1,
            error_code=ErrorCode.UNAUTHORIZED,
            reason="Authentication failed"
        )

        encoded = msg.encode(include_request_id=False)
        decoded, consumed = decode_control_message(encoded, response_request_id=1)

        self.assertEqual(decoded.request_id, 1)
        self.assertEqual(decoded.error_code, ErrorCode.UNAUTHORIZED)
        self.assertEqual(decoded.retry_interval, 0)
        self.assertEqual(decoded.reason, "Authentication failed")
        self.assertEqual(consumed, len(encoded))


class TestPublishDoneMessage(unittest.TestCase):
    """Test PUBLISH_DONE message."""

    def test_encode_decode(self):
        msg = PublishDoneMessage(
            request_id=1,
            status_code=6,
            stream_count=42,
            reason="queue full",
        )

        encoded = msg.encode()
        decoded, consumed = PublishDoneMessage.decode(encoded, 2)

        self.assertEqual(decoded.request_id, 1)
        self.assertEqual(decoded.status_code, 6)
        self.assertEqual(decoded.stream_count, 42)
        self.assertEqual(decoded.reason, "queue full")

    def test_decode_without_request_id_uses_stream_context(self):
        msg = PublishDoneMessage(
            request_id=1,
            status_code=6,
            stream_count=UNKNOWN_PUBLISH_DONE_STREAM_COUNT,
            reason="queue full",
        )

        encoded = msg.encode(include_request_id=False)
        decoded, consumed = decode_control_message(encoded, response_request_id=1)

        self.assertEqual(decoded.request_id, 1)
        self.assertEqual(decoded.status_code, 6)
        self.assertEqual(decoded.stream_count, UNKNOWN_PUBLISH_DONE_STREAM_COUNT)
        self.assertEqual(decoded.reason, "queue full")
        self.assertEqual(consumed, len(encoded))


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
