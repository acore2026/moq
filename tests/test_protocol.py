"""
Basic tests for MOQ protocol
"""

import pytest
import asyncio

from moq.protocol.varint import encode_varint, decode_varint
from moq.protocol.constants import MOQMessageType, MOQFilterType
from moq.protocol.objects import MOQObject, MOQTrack, MOQObjectBuilder
from moq.protocol.subscription import MOQSubscription, SubscriptionBuilder


class TestVarint:
    """Test variable-length integer encoding"""
    
    def test_encode_decode_small(self):
        """Test encoding/decoding small values"""
        for value in [0, 1, 63, 64, 100, 255]:
            encoded = encode_varint(value)
            decoded, consumed = decode_varint(encoded)
            assert decoded == value
            assert consumed == len(encoded)
    
    def test_encode_decode_medium(self):
        """Test encoding/decoding medium values"""
        for value in [256, 1000, 16383, 16384, 65535]:
            encoded = encode_varint(value)
            decoded, consumed = decode_varint(encoded)
            assert decoded == value
    
    def test_encode_decode_large(self):
        """Test encoding/decoding large values"""
        for value in [65536, 1000000, 1073741823, 1073741824]:
            encoded = encode_varint(value)
            decoded, consumed = decode_varint(encoded)
            assert decoded == value
    
    def test_encode_negative_raises(self):
        """Test that encoding negative values raises error"""
        with pytest.raises(ValueError):
            encode_varint(-1)


class TestMOQObject:
    """Test MOQ Object"""
    
    def test_object_creation(self):
        """Test creating MOQObject"""
        obj = MOQObject(
            track_alias=1,
            group_id=2,
            object_id=3,
            payload=b"test data"
        )
        
        assert obj.track_alias == 1
        assert obj.group_id == 2
        assert obj.object_id == 3
        assert obj.payload == b"test data"
    
    def test_object_to_dict(self):
        """Test converting MOQObject to dict"""
        obj = MOQObject(
            track_alias=1,
            group_id=2,
            object_id=3,
            payload=b"test"
        )
        
        d = obj.to_dict()
        assert d['track_alias'] == 1
        assert d['group_id'] == 2
        assert d['object_id'] == 3
        assert d['payload_length'] == 4


class TestMOQTrack:
    """Test MOQ Track"""
    
    def test_track_creation(self):
        """Test creating MOQTrack"""
        track = MOQTrack(
            namespace=("example", "live"),
            name="video"
        )
        
        assert track.namespace == ("example", "live")
        assert track.name == "video"
        assert track.full_name() == "example/live/video"
    
    def test_track_equality(self):
        """Test track equality"""
        track1 = MOQTrack(namespace=("a",), name="b")
        track2 = MOQTrack(namespace=("a",), name="b")
        track3 = MOQTrack(namespace=("a",), name="c")
        
        assert track1 == track2
        assert track1 != track3


class TestMOQSubscription:
    """Test MOQ Subscription"""
    
    @pytest.mark.asyncio
    async def test_subscription_creation(self):
        """Test creating MOQSubscription"""
        sub = MOQSubscription(
            id=1,
            track_namespace=("example",),
            track_name="video"
        )
        
        assert sub.id == 1
        assert sub.track_namespace == ("example",)
        assert sub.track_name == "video"
        assert sub.full_track_name() == "example/video"
    
    @pytest.mark.asyncio
    async def test_subscription_state(self):
        """Test subscription state changes"""
        from moq.protocol.subscription import SubscriptionState
        
        sub = MOQSubscription(
            id=1,
            track_namespace=("example",),
            track_name="video"
        )
        
        assert sub.state == SubscriptionState.PENDING
        
        sub.update_state(SubscriptionState.ACTIVE)
        assert sub.state == SubscriptionState.ACTIVE


class TestBuilder:
    """Test builder pattern"""
    
    def test_object_builder(self):
        """Test MOQObjectBuilder"""
        obj = MOQObjectBuilder() \
            .track_alias(1) \
            .group_id(2) \
            .object_id(3) \
            .payload(b"test") \
            .build()
        
        assert obj.track_alias == 1
        assert obj.group_id == 2
        assert obj.object_id == 3
        assert obj.payload == b"test"
    
    def test_subscription_builder(self):
        """Test SubscriptionBuilder"""
        from moq.protocol.subscription import SubscriptionBuilder
        
        sub = SubscriptionBuilder() \
            .track_namespace("example") \
            .track_name("video") \
            .filter_latest_group() \
            .priority(100) \
            .build(1)
        
        assert sub.id == 1
        assert sub.track_namespace == ("example",)
        assert sub.track_name == "video"
        assert sub.filter_type == MOQFilterType.LATEST_GROUP
        assert sub.priority == 100


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
