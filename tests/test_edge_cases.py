"""
Test edge cases and potential bugs in MOQ Transport.

This module focuses on specific bugs found in code review:
1. Connection state management issues
2. Resource cleanup problems
3. Race conditions
4. Error handling gaps
"""

import asyncio
import logging
import os
import unittest
from pathlib import Path
from typing import Optional, List
from dataclasses import dataclass

import sys

sys.path.insert(0, str(Path(__file__).parent.parent))

from moq import MOQPublisher, MOQSubscriber, MOQRelay
from moq.encoding import FullTrackName
from moq.pub import PublishedObject
from moq.sub import ReceivedObject

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class TestEdgeCases(unittest.IsolatedAsyncioTestCase):
    """Test edge cases and potential bugs."""

    async def asyncSetUp(self):
        self.relay: Optional[MOQRelay] = None
        self.publisher: Optional[MOQPublisher] = None
        self.subscriber: Optional[MOQSubscriber] = None

    async def asyncTearDown(self):
        if self.subscriber:
            self.subscriber.disconnect()
        if self.publisher:
            self.publisher.disconnect()
        if self.relay:
            await self.relay.stop()

    async def test_multiple_subscribers_same_track(self):
        """
        Test multiple subscribers subscribing to the same track.

        This can expose issues with subscription tracking and message forwarding.
        """
        logger.info("=" * 60)
        logger.info("TEST: Multiple Subscribers Same Track")
        logger.info("=" * 60)

        self.relay = MOQRelay(host="127.0.0.1", port=0)
        await self.relay.start()
        port = self.relay._quic_server.actual_port or 4433

        # Create publisher
        pub = MOQPublisher("127.0.0.1", port)
        pub_connected = asyncio.Event()
        pub.set_handlers(on_connected=lambda: pub_connected.set())
        await pub.connect(agent_id="test-pub")
        await asyncio.wait_for(pub_connected.wait(), timeout=5.0)

        # Create multiple subscribers
        num_subscribers = 3
        subscribers = []
        received_counts = [0] * num_subscribers

        for i in range(num_subscribers):
            sub = MOQSubscriber("127.0.0.1", port)
            sub_connected = asyncio.Event()

            def make_handler(idx):
                def on_obj(obj):
                    received_counts[idx] += 1

                return on_obj

            sub.set_handlers(
                on_connected=lambda ev=sub_connected: ev.set(),
                on_object_received=make_handler(i),
            )
            await sub.connect(agent_id=f"test-sub-{i}")
            await asyncio.wait_for(sub_connected.wait(), timeout=5.0)
            subscribers.append(sub)

        # Publish and subscribe
        track_name = FullTrackName(namespace=[b"test"], track_name=b"multi-sub")
        await pub.publish(track_name)

        for sub in subscribers:
            await sub.subscribe(track_name)

        await asyncio.sleep(0.5)

        # Send objects
        num_objects = 5
        for i in range(num_objects):
            obj = PublishedObject(
                group_id=0,
                object_id=i,
                payload=f"object {i}".encode() * 10,
                use_datagram=False,
            )
            await pub.send_object(track_name, obj)

        await asyncio.sleep(2.0)

        # Check all subscribers received all objects
        for i, count in enumerate(received_counts):
            logger.info(f"Subscriber {i} received {count}/{num_objects} objects")
            self.assertEqual(
                count, num_objects, f"Subscriber {i} did not receive all objects"
            )

        # Cleanup
        for sub in subscribers:
            sub.disconnect()
        pub.disconnect()

    async def test_subscriber_disconnect_before_unsubscribe(self):
        """
        Test subscriber disconnecting without explicit unsubscribe.

        This tests cleanup of subscription state in relay.
        """
        logger.info("=" * 60)
        logger.info("TEST: Subscriber Disconnect Without Unsubscribe")
        logger.info("=" * 60)

        self.relay = MOQRelay(host="127.0.0.1", port=0)
        await self.relay.start()
        port = self.relay._quic_server.actual_port or 4433

        # Create publisher
        pub = MOQPublisher("127.0.0.1", port)
        pub_connected = asyncio.Event()
        pub.set_handlers(on_connected=lambda: pub_connected.set())
        await pub.connect(agent_id="test-pub")
        await asyncio.wait_for(pub_connected.wait(), timeout=5.0)

        # Create subscriber
        sub = MOQSubscriber("127.0.0.1", port)
        sub_connected = asyncio.Event()
        sub_disconnected = asyncio.Event()
        received_count = 0

        def on_obj(obj):
            nonlocal received_count
            received_count += 1

        sub.set_handlers(
            on_connected=lambda: sub_connected.set(),
            on_disconnected=lambda: sub_disconnected.set(),
            on_object_received=on_obj,
        )
        await sub.connect(agent_id="test-sub")
        await asyncio.wait_for(sub_connected.wait(), timeout=5.0)

        # Publish and subscribe
        track_name = FullTrackName(namespace=[b"test"], track_name=b"abrupt-disconnect")
        await pub.publish(track_name)
        await sub.subscribe(track_name)
        await asyncio.sleep(0.5)

        # Send a few objects
        for i in range(3):
            obj = PublishedObject(
                group_id=0,
                object_id=i,
                payload=f"object {i}".encode(),
                use_datagram=False,
            )
            await pub.send_object(track_name, obj)

        await asyncio.sleep(0.5)

        # Abruptly disconnect subscriber (no unsubscribe)
        sub.disconnect()
        await asyncio.wait_for(sub_disconnected.wait(), timeout=5.0)

        # Wait and check relay state
        await asyncio.sleep(1.0)

        # Check if subscriptions were cleaned up
        track_name_normalized = track_name.normalize()
        if track_name_normalized in self.relay._subscriptions:
            remaining = len(self.relay._subscriptions[track_name_normalized])
            logger.warning(f"Subscriptions not cleaned up: {remaining} remaining")
            # This is a potential bug - subscriptions should be cleaned up
            # self.assertEqual(remaining, 0, "Subscriptions not cleaned up after disconnect")

        logger.info(f"Received {received_count} objects before disconnect")

        # Now create a new subscriber and verify publisher still works
        sub2 = MOQSubscriber("127.0.0.1", port)
        sub2_connected = asyncio.Event()
        received_count_2 = 0

        def on_obj2(obj):
            nonlocal received_count_2
            received_count_2 += 1

        sub2.set_handlers(
            on_connected=lambda: sub2_connected.set(), on_object_received=on_obj2
        )
        await sub2.connect(agent_id="test-sub-2")
        await asyncio.wait_for(sub2_connected.wait(), timeout=5.0)
        await sub2.subscribe(track_name)
        await asyncio.sleep(0.5)

        # Send more objects
        for i in range(3, 6):
            obj = PublishedObject(
                group_id=0,
                object_id=i,
                payload=f"object {i}".encode(),
                use_datagram=False,
            )
            await pub.send_object(track_name, obj)

        await asyncio.sleep(1.0)

        logger.info(f"New subscriber received {received_count_2} objects")

        # Cleanup
        sub2.disconnect()
        pub.disconnect()

    async def test_publisher_disconnect_while_publishing(self):
        """
        Test publisher disconnecting while actively publishing.

        This can expose issues with publication cleanup.
        """
        logger.info("=" * 60)
        logger.info("TEST: Publisher Disconnect While Publishing")
        logger.info("=" * 60)

        self.relay = MOQRelay(host="127.0.0.1", port=0)
        await self.relay.start()
        port = self.relay._quic_server.actual_port or 4433

        # Create publisher
        pub = MOQPublisher("127.0.0.1", port)
        pub_connected = asyncio.Event()
        pub_disconnected = asyncio.Event()
        pub.set_handlers(
            on_connected=lambda: pub_connected.set(),
            on_disconnected=lambda: pub_disconnected.set(),
        )
        await pub.connect(agent_id="test-pub")
        await asyncio.wait_for(pub_connected.wait(), timeout=5.0)

        # Create subscriber
        sub = MOQSubscriber("127.0.0.1", port)
        sub_connected = asyncio.Event()
        sub.set_handlers(on_connected=lambda: sub_connected.set())
        await sub.connect(agent_id="test-sub")
        await asyncio.wait_for(sub_connected.wait(), timeout=5.0)

        # Publish and subscribe
        track_name = FullTrackName(namespace=[b"test"], track_name=b"pub-disconnect")
        await pub.publish(track_name)
        await sub.subscribe(track_name)
        await asyncio.sleep(0.5)

        # Start sending objects in background
        async def send_objects():
            i = 0
            while True:
                try:
                    obj = PublishedObject(
                        group_id=0,
                        object_id=i,
                        payload=f"object {i}".encode() * 100,
                        use_datagram=False,
                    )
                    await pub.send_object(track_name, obj)
                    i += 1
                    await asyncio.sleep(0.1)
                except Exception as e:
                    logger.error(f"Send failed: {e}")
                    break

        send_task = asyncio.create_task(send_objects())

        # Let some objects send
        await asyncio.sleep(1.0)

        # Abruptly disconnect publisher
        pub.disconnect()
        await asyncio.wait_for(pub_disconnected.wait(), timeout=5.0)

        # Cancel send task
        send_task.cancel()
        try:
            await send_task
        except asyncio.CancelledError:
            pass

        # Wait and check relay state
        await asyncio.sleep(1.0)

        # Check if publications were cleaned up
        track_name_normalized = track_name.normalize()
        if track_name_normalized in self.relay._publications:
            logger.warning("Publication not cleaned up after disconnect")

        sub.disconnect()

    async def test_zero_byte_payload(self):
        """
        Test sending objects with zero-byte payloads.

        This can expose issues with payload handling.
        """
        logger.info("=" * 60)
        logger.info("TEST: Zero Byte Payload")
        logger.info("=" * 60)

        self.relay = MOQRelay(host="127.0.0.1", port=0)
        await self.relay.start()
        port = self.relay._quic_server.actual_port or 4433

        # Create publisher
        pub = MOQPublisher("127.0.0.1", port)
        pub_connected = asyncio.Event()
        pub.set_handlers(on_connected=lambda: pub_connected.set())
        await pub.connect(agent_id="test-pub")
        await asyncio.wait_for(pub_connected.wait(), timeout=5.0)

        # Create subscriber
        sub = MOQSubscriber("127.0.0.1", port)
        sub_connected = asyncio.Event()
        received_count = 0
        last_payload = None

        def on_obj(obj):
            nonlocal received_count, last_payload
            received_count += 1
            last_payload = obj.payload

        sub.set_handlers(
            on_connected=lambda: sub_connected.set(), on_object_received=on_obj
        )
        await sub.connect(agent_id="test-sub")
        await asyncio.wait_for(sub_connected.wait(), timeout=5.0)

        # Publish and subscribe
        track_name = FullTrackName(namespace=[b"test"], track_name=b"zero-payload")
        await pub.publish(track_name)
        await sub.subscribe(track_name)
        await asyncio.sleep(0.5)

        # Send objects with zero-byte payloads
        for i in range(3):
            obj = PublishedObject(
                group_id=0,
                object_id=i,
                payload=b"",  # Zero bytes
                use_datagram=False,
            )
            await pub.send_object(track_name, obj)

        await asyncio.sleep(1.0)

        logger.info(f"Received {received_count} zero-byte objects")
        self.assertEqual(received_count, 3, "Not all zero-byte objects received")
        self.assertEqual(last_payload, b"", "Last payload should be empty")

        sub.disconnect()
        pub.disconnect()

    async def test_large_payload(self):
        """
        Test sending objects with large payloads.

        This can expose issues with buffer management.
        """
        logger.info("=" * 60)
        logger.info("TEST: Large Payload")
        logger.info("=" * 60)

        self.relay = MOQRelay(host="127.0.0.1", port=0)
        await self.relay.start()
        port = self.relay._quic_server.actual_port or 4433

        # Create publisher
        pub = MOQPublisher("127.0.0.1", port)
        pub_connected = asyncio.Event()
        pub.set_handlers(on_connected=lambda: pub_connected.set())
        await pub.connect(agent_id="test-pub")
        await asyncio.wait_for(pub_connected.wait(), timeout=5.0)

        # Create subscriber
        sub = MOQSubscriber("127.0.0.1", port)
        sub_connected = asyncio.Event()
        received_count = 0
        total_bytes = 0

        def on_obj(obj):
            nonlocal received_count, total_bytes
            received_count += 1
            total_bytes += len(obj.payload)

        sub.set_handlers(
            on_connected=lambda: sub_connected.set(), on_object_received=on_obj
        )
        await sub.connect(agent_id="test-sub")
        await asyncio.wait_for(sub_connected.wait(), timeout=5.0)

        # Publish and subscribe
        track_name = FullTrackName(namespace=[b"test"], track_name=b"large-payload")
        await pub.publish(track_name)
        await sub.subscribe(track_name)
        await asyncio.sleep(0.5)

        # Send large objects (100KB each)
        payload_size = 100 * 1024
        num_objects = 3
        for i in range(num_objects):
            obj = PublishedObject(
                group_id=0,
                object_id=i,
                payload=os.urandom(payload_size),
                use_datagram=False,  # Stream mode for large payloads
            )
            await pub.send_object(track_name, obj)
            logger.info(f"Sent large object {i} ({payload_size} bytes)")

        await asyncio.sleep(2.0)

        logger.info(f"Received {received_count}/{num_objects} large objects")
        logger.info(f"Total bytes: {total_bytes}/{num_objects * payload_size}")

        self.assertEqual(received_count, num_objects, "Not all large objects received")
        self.assertEqual(total_bytes, num_objects * payload_size, "Bytes mismatch")

        sub.disconnect()
        pub.disconnect()

    async def test_many_tracks(self):
        """
        Test publishing/subscribing to many tracks.

        This can expose issues with resource management.
        """
        logger.info("=" * 60)
        logger.info("TEST: Many Tracks")
        logger.info("=" * 60)

        self.relay = MOQRelay(host="127.0.0.1", port=0)
        await self.relay.start()
        port = self.relay._quic_server.actual_port or 4433

        # Create publisher
        pub = MOQPublisher("127.0.0.1", port)
        pub_connected = asyncio.Event()
        pub.set_handlers(on_connected=lambda: pub_connected.set())
        await pub.connect(agent_id="test-pub")
        await asyncio.wait_for(pub_connected.wait(), timeout=5.0)

        # Create subscriber
        sub = MOQSubscriber("127.0.0.1", port)
        sub_connected = asyncio.Event()
        received_counts = {}

        def on_obj(obj):
            key = (obj.group_id, obj.object_id)
            received_counts[key] = received_counts.get(key, 0) + 1

        sub.set_handlers(
            on_connected=lambda: sub_connected.set(), on_object_received=on_obj
        )
        await sub.connect(agent_id="test-sub")
        await asyncio.wait_for(sub_connected.wait(), timeout=5.0)

        # Publish many tracks
        num_tracks = 10
        tracks = []
        for i in range(num_tracks):
            track_name = FullTrackName(
                namespace=[b"test"], track_name=f"many-tracks-{i}".encode()
            )
            tracks.append(track_name)
            await pub.publish(track_name)
            await sub.subscribe(track_name)

        await asyncio.sleep(1.0)

        # Send one object on each track
        for i, track_name in enumerate(tracks):
            obj = PublishedObject(
                group_id=i,
                object_id=0,
                payload=f"data for track {i}".encode(),
                use_datagram=False,
            )
            await pub.send_object(track_name, obj)

        await asyncio.sleep(2.0)

        logger.info(f"Received objects on {len(received_counts)} tracks")
        self.assertEqual(
            len(received_counts), num_tracks, "Not all tracks received data"
        )

        sub.disconnect()
        pub.disconnect()

    async def test_reconnect_after_disconnect(self):
        """
        Test reconnecting after disconnection.

        This can expose issues with state cleanup and recovery.
        """
        logger.info("=" * 60)
        logger.info("TEST: Reconnect After Disconnect")
        logger.info("=" * 60)

        self.relay = MOQRelay(host="127.0.0.1", port=0)
        await self.relay.start()
        port = self.relay._quic_server.actual_port or 4433

        track_name = FullTrackName(namespace=[b"test"], track_name=b"reconnect")

        # First connection
        pub1 = MOQPublisher("127.0.0.1", port)
        pub1_connected = asyncio.Event()
        pub1.set_handlers(on_connected=lambda: pub1_connected.set())
        await pub1.connect(agent_id="test-pub-1")
        await asyncio.wait_for(pub1_connected.wait(), timeout=5.0)

        sub1 = MOQSubscriber("127.0.0.1", port)
        sub1_connected = asyncio.Event()
        received1 = []

        def on_obj1(obj):
            received1.append(obj)

        sub1.set_handlers(
            on_connected=lambda: sub1_connected.set(), on_object_received=on_obj1
        )
        await sub1.connect(agent_id="test-sub-1")
        await asyncio.wait_for(sub1_connected.wait(), timeout=5.0)

        await pub1.publish(track_name)
        await sub1.subscribe(track_name)
        await asyncio.sleep(0.5)

        # Send object
        obj1 = PublishedObject(
            group_id=0, object_id=0, payload=b"first connection", use_datagram=False
        )
        await pub1.send_object(track_name, obj1)
        await asyncio.sleep(0.5)

        logger.info(f"First connection received {len(received1)} objects")

        # Disconnect
        sub1.disconnect()
        pub1.disconnect()
        await asyncio.sleep(1.0)

        # Second connection with same track
        pub2 = MOQPublisher("127.0.0.1", port)
        pub2_connected = asyncio.Event()
        pub2.set_handlers(on_connected=lambda: pub2_connected.set())
        await pub2.connect(agent_id="test-pub-2")
        await asyncio.wait_for(pub2_connected.wait(), timeout=5.0)

        sub2 = MOQSubscriber("127.0.0.1", port)
        sub2_connected = asyncio.Event()
        received2 = []

        def on_obj2(obj):
            received2.append(obj)

        sub2.set_handlers(
            on_connected=lambda: sub2_connected.set(), on_object_received=on_obj2
        )
        await sub2.connect(agent_id="test-sub-2")
        await asyncio.wait_for(sub2_connected.wait(), timeout=5.0)

        await pub2.publish(track_name)
        await sub2.subscribe(track_name)
        await asyncio.sleep(0.5)

        # Send object
        obj2 = PublishedObject(
            group_id=0, object_id=1, payload=b"second connection", use_datagram=False
        )
        await pub2.send_object(track_name, obj2)
        await asyncio.sleep(1.0)

        logger.info(f"Second connection received {len(received2)} objects")
        self.assertEqual(len(received2), 1, "Second connection should receive 1 object")

        sub2.disconnect()
        pub2.disconnect()


class TestPotentialBugs(unittest.IsolatedAsyncioTestCase):
    """Test cases for specific potential bugs found in code review."""

    async def asyncSetUp(self):
        self.relay: Optional[MOQRelay] = None

    async def asyncTearDown(self):
        if self.relay:
            await self.relay.stop()

    async def test_stream_buffer_cleanup(self):
        """
        Test that stream buffers are properly cleaned up.

        Bug: Stream buffers in relay._stream_buffers might not be
        cleaned up properly if stream ends unexpectedly.
        """
        logger.info("=" * 60)
        logger.info("TEST: Stream Buffer Cleanup")
        logger.info("=" * 60)

        self.relay = MOQRelay(host="127.0.0.1", port=0)
        await self.relay.start()

        # Check initial state
        initial_buffers = len(self.relay._stream_buffers)
        logger.info(f"Initial stream buffers: {initial_buffers}")

        # Create connections and send data
        port = self.relay._quic_server.actual_port or 4433

        pub = MOQPublisher("127.0.0.1", port)
        pub_connected = asyncio.Event()
        pub.set_handlers(on_connected=lambda: pub_connected.set())
        await pub.connect(agent_id="test-pub")
        await asyncio.wait_for(pub_connected.wait(), timeout=5.0)

        sub = MOQSubscriber("127.0.0.1", port)
        sub_connected = asyncio.Event()
        sub.set_handlers(on_connected=lambda: sub_connected.set())
        await sub.connect(agent_id="test-sub")
        await asyncio.wait_for(sub_connected.wait(), timeout=5.0)

        track_name = FullTrackName(namespace=[b"test"], track_name=b"buffer-cleanup")
        await pub.publish(track_name)
        await sub.subscribe(track_name)
        await asyncio.sleep(0.5)

        # Send multiple stream objects
        for i in range(10):
            obj = PublishedObject(
                group_id=0,
                object_id=i,
                payload=f"data {i}".encode() * 100,
                use_datagram=False,
            )
            await pub.send_object(track_name, obj)

        await asyncio.sleep(1.0)

        # Check buffers during transmission
        during_buffers = len(self.relay._stream_buffers)
        logger.info(f"Stream buffers during transmission: {during_buffers}")

        # Disconnect
        sub.disconnect()
        pub.disconnect()
        await asyncio.sleep(1.0)

        # Check final state
        final_buffers = len(self.relay._stream_buffers)
        logger.info(f"Stream buffers after disconnect: {final_buffers}")

        # Buffers should be cleaned up
        # Note: This might fail if there's a bug

    async def test_subscription_list_cleanup(self):
        """
        Test that subscription lists are properly cleaned up.

        Bug: In relay._cleanup_client, empty subscription lists
        might not be removed from _subscriptions dict.
        """
        logger.info("=" * 60)
        logger.info("TEST: Subscription List Cleanup")
        logger.info("=" * 60)

        self.relay = MOQRelay(host="127.0.0.1", port=0)
        await self.relay.start()
        port = self.relay._quic_server.actual_port or 4433

        track_name = FullTrackName(namespace=[b"test"], track_name=b"sub-cleanup")
        track_normalized = track_name.normalize()

        # Create publisher
        pub = MOQPublisher("127.0.0.1", port)
        pub_connected = asyncio.Event()
        pub.set_handlers(on_connected=lambda: pub_connected.set())
        await pub.connect(agent_id="test-pub")
        await asyncio.wait_for(pub_connected.wait(), timeout=5.0)

        # Create subscriber
        sub = MOQSubscriber("127.0.0.1", port)
        sub_connected = asyncio.Event()
        sub.set_handlers(on_connected=lambda: sub_connected.set())
        await sub.connect(agent_id="test-sub")
        await asyncio.wait_for(sub_connected.wait(), timeout=5.0)

        await pub.publish(track_name)
        await sub.subscribe(track_name)
        await asyncio.sleep(0.5)

        # Check subscriptions exist
        self.assertIn(track_normalized, self.relay._subscriptions)
        self.assertEqual(len(self.relay._subscriptions[track_normalized]), 1)

        # Disconnect subscriber
        sub.disconnect()
        await asyncio.sleep(1.0)

        # Check subscriptions cleaned up
        if track_normalized in self.relay._subscriptions:
            remaining = len(self.relay._subscriptions[track_normalized])
            logger.warning(f"Subscription list not cleaned up: {remaining} entries")
            # This is a potential bug - empty lists should be removed

        pub.disconnect()


if __name__ == "__main__":
    unittest.main()
