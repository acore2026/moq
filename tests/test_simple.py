"""
Simple test to verify basic MOQ functionality.
"""

import asyncio
import logging
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from moq import MOQPublisher, MOQSubscriber, MOQRelay
from moq.encoding import FullTrackName
from moq.pub import PublishedObject
from moq.sub import ReceivedObject

logging.basicConfig(
    level=logging.DEBUG, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class TestSimple(unittest.IsolatedAsyncioTestCase):
    """Simple test case."""

    async def asyncSetUp(self):
        self.relay: MOQRelay = None
        self.publisher: MOQPublisher = None
        self.subscriber: MOQSubscriber = None

    async def asyncTearDown(self):
        if self.subscriber:
            self.subscriber.disconnect()
        if self.publisher:
            self.publisher.disconnect()
        if self.relay:
            await self.relay.stop()

    async def test_basic_stream(self):
        """Test basic stream transmission."""
        logger.info("=" * 60)
        logger.info("TEST: Basic Stream Transmission")
        logger.info("=" * 60)

        # Start relay
        self.relay = MOQRelay(host="127.0.0.1", port=0)
        await self.relay.start()
        port = self.relay._quic_server.actual_port or 4433
        logger.info(f"Relay started on port {port}")

        # Create publisher
        pub_connected = asyncio.Event()
        self.publisher = MOQPublisher("127.0.0.1", port)
        self.publisher.set_handlers(on_connected=lambda: pub_connected.set())
        success = await self.publisher.connect(agent_id="test-pub")
        self.assertTrue(success, "Publisher connection failed")
        await asyncio.wait_for(pub_connected.wait(), timeout=5.0)
        logger.info("Publisher connected")

        # Create subscriber
        sub_connected = asyncio.Event()
        received_objects = []

        def on_object(obj):
            logger.info(
                f"Received object: group={obj.group_id}, object={obj.object_id}, size={len(obj.payload)}"
            )
            received_objects.append(obj)

        self.subscriber = MOQSubscriber("127.0.0.1", port)
        self.subscriber.set_handlers(
            on_connected=lambda: sub_connected.set(), on_object_received=on_object
        )
        success = await self.subscriber.connect(agent_id="test-sub")
        self.assertTrue(success, "Subscriber connection failed")
        await asyncio.wait_for(sub_connected.wait(), timeout=5.0)
        logger.info("Subscriber connected")

        # Publish track
        track_name = FullTrackName(namespace=[b"test"], track_name=b"basic-stream")
        pub_success = await self.publisher.publish(track_name)
        self.assertTrue(pub_success, "Publish failed")
        logger.info(f"Published track: {track_name}")

        # Subscribe to track
        sub_success = await self.subscriber.subscribe(track_name)
        self.assertTrue(sub_success, "Subscribe failed")
        logger.info(f"Subscribed to track: {track_name}")

        await asyncio.sleep(0.5)

        # Send object
        obj = PublishedObject(
            group_id=0,
            object_id=0,
            payload=b"Hello, MOQ!",
            publisher_priority=128,
            subgroup_id=0,
            use_datagram=False,
        )
        await self.publisher.send_object(track_name, obj)
        logger.info("Sent object via stream")

        # Wait for reception
        await asyncio.sleep(2.0)

        logger.info(f"Objects received: {len(received_objects)}")
        self.assertEqual(len(received_objects), 1, "Object not received")

    async def test_basic_datagram(self):
        """Test basic datagram transmission."""
        logger.info("=" * 60)
        logger.info("TEST: Basic Datagram Transmission")
        logger.info("=" * 60)

        # Start relay
        self.relay = MOQRelay(host="127.0.0.1", port=0)
        await self.relay.start()
        port = self.relay._quic_server.actual_port or 4433
        logger.info(f"Relay started on port {port}")

        # Create publisher
        pub_connected = asyncio.Event()
        self.publisher = MOQPublisher("127.0.0.1", port)
        self.publisher.set_handlers(on_connected=lambda: pub_connected.set())
        success = await self.publisher.connect(agent_id="test-pub")
        self.assertTrue(success, "Publisher connection failed")
        await asyncio.wait_for(pub_connected.wait(), timeout=5.0)
        logger.info("Publisher connected")

        # Create subscriber
        sub_connected = asyncio.Event()
        received_objects = []

        def on_object(obj):
            logger.info(
                f"Received datagram: group={obj.group_id}, object={obj.object_id}, size={len(obj.payload)}"
            )
            received_objects.append(obj)

        self.subscriber = MOQSubscriber("127.0.0.1", port)
        self.subscriber.set_handlers(
            on_connected=lambda: sub_connected.set(), on_object_received=on_object
        )
        success = await self.subscriber.connect(agent_id="test-sub")
        self.assertTrue(success, "Subscriber connection failed")
        await asyncio.wait_for(sub_connected.wait(), timeout=5.0)
        logger.info("Subscriber connected")

        # Publish track
        track_name = FullTrackName(namespace=[b"test"], track_name=b"basic-datagram")
        pub_success = await self.publisher.publish(track_name)
        self.assertTrue(pub_success, "Publish failed")
        logger.info(f"Published track: {track_name}")

        # Subscribe to track
        sub_success = await self.subscriber.subscribe(track_name)
        self.assertTrue(sub_success, "Subscribe failed")
        logger.info(f"Subscribed to track: {track_name}")

        await asyncio.sleep(0.5)

        # Send object via datagram
        obj = PublishedObject(
            group_id=0,
            object_id=0,
            payload=b"Hello, MOQ Datagram!",
            publisher_priority=128,
            subgroup_id=0,
            use_datagram=True,
        )
        await self.publisher.send_object(track_name, obj)
        logger.info("Sent object via datagram")

        # Wait for reception
        await asyncio.sleep(2.0)

        logger.info(f"Objects received: {len(received_objects)}")
        self.assertGreaterEqual(len(received_objects), 1, "Datagram not received")


if __name__ == "__main__":
    unittest.main()
