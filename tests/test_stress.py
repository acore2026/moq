"""
Stress tests for MOQ Transport.

Tests for:
- High message rates
- Large number of connections
- Network degradation simulation
- Burst traffic handling
"""

import asyncio
import logging
import os
import random
import time
import unittest
from pathlib import Path
from typing import List, Optional, Dict
import sys

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from moq import MOQPublisher, MOQSubscriber, MOQRelay
from moq.encoding import FullTrackName
from moq.pub import PublishedObject
from moq.sub import ReceivedObject

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class TestStress(unittest.IsolatedAsyncioTestCase):
    """Stress tests for MOQ Transport."""

    async def asyncSetUp(self):
        self.relay: Optional[MOQRelay] = None
        self.publishers: List[MOQPublisher] = []
        self.subscribers: List[MOQSubscriber] = []

    async def asyncTearDown(self):
        for sub in self.subscribers:
            sub.disconnect()
        for pub in self.publishers:
            pub.disconnect()
        if self.relay:
            await self.relay.stop()

    async def _start_relay(self, cache_mb: int = 500) -> int:
        """Start relay with specified cache."""
        self.relay = MOQRelay(
            host="127.0.0.1",
            port=0,
            max_memory_cache=cache_mb * 1024 * 1024,
        )
        await self.relay.start()
        port = self.relay._quic_server.actual_port or 4433
        return port

    async def _create_publisher(self, port: int, agent_id: str) -> MOQPublisher:
        """Create and connect publisher."""
        pub = MOQPublisher("127.0.0.1", port)
        connected = asyncio.Event()
        pub.set_handlers(on_connected=lambda: connected.set())
        success = await pub.connect(agent_id=agent_id)
        if not success:
            raise ConnectionError(f"Failed to connect publisher {agent_id}")
        await asyncio.wait_for(connected.wait(), timeout=5.0)
        return pub

    async def _create_subscriber(
        self, port: int, agent_id: str
    ) -> Tuple[MOQSubscriber, List]:
        """Create and connect subscriber."""
        sub = MOQSubscriber("127.0.0.1", port)
        connected = asyncio.Event()
        received = []

        def on_object(obj):
            received.append({"time": time.time(), "size": len(obj.payload)})

        sub.set_handlers(
            on_connected=lambda: connected.set(), on_object_received=on_object
        )
        success = await sub.connect(agent_id=agent_id)
        if not success:
            raise ConnectionError(f"Failed to connect subscriber {agent_id}")
        await asyncio.wait_for(connected.wait(), timeout=5.0)
        return sub, received

    async def test_01_high_throughput_single_stream(self):
        """Test high throughput on single stream."""
        logger.info("=" * 60)
        logger.info("TEST: High Throughput Single Stream")
        logger.info("=" * 60)

        duration = 10  # seconds
        target_rate = 1000  # objects per second
        object_size = 1024  # 1KB

        port = await self._start_relay()
        pub = await self._create_publisher(port, "stress-pub")
        sub, received = await self._create_subscriber(port, "stress-sub")
        self.publishers.append(pub)
        self.subscribers.append(sub)

        track_name = FullTrackName(namespace=[b"stress"], track_name=b"high-throughput")
        await pub.publish(track_name)
        await sub.subscribe(track_name)
        await asyncio.sleep(0.5)

        start_time = time.time()
        objects_sent = 0
        target_interval = 1.0 / target_rate

        while time.time() - start_time < duration:
            obj = PublishedObject(
                group_id=objects_sent // 10000,
                object_id=objects_sent % 10000,
                payload=os.urandom(object_size),
                use_datagram=False,
            )
            await pub.send_object(track_name, obj)
            objects_sent += 1

            # Maintain rate
            expected_time = start_time + (objects_sent * target_interval)
            sleep_time = expected_time - time.time()
            if sleep_time > 0:
                await asyncio.sleep(sleep_time)

        await asyncio.sleep(3.0)

        elapsed = time.time() - start_time
        actual_rate = objects_sent / elapsed
        logger.info(f"Sent: {objects_sent}, Rate: {actual_rate:.1f} obj/s")
        logger.info(f"Received: {len(received)}")
        logger.info(f"Throughput: {actual_rate * object_size / (1024 * 1024):.2f} MB/s")

        # Should achieve reasonable rate
        self.assertGreater(actual_rate, 500, "Throughput too low")
        self.assertGreaterEqual(
            len(received), objects_sent * 0.8, "Too much loss at high throughput"
        )

    async def test_02_burst_traffic_handling(self):
        """Test handling of burst traffic."""
        logger.info("=" * 60)
        logger.info("TEST: Burst Traffic Handling")
        logger.info("=" * 60)

        burst_size = 1000
        num_bursts = 5
        burst_interval = 2.0  # seconds between bursts

        port = await self._start_relay()
        pub = await self._create_publisher(port, "burst-pub")
        sub, received = await self._create_subscriber(port, "burst-sub")
        self.publishers.append(pub)
        self.subscribers.append(sub)

        track_name = FullTrackName(namespace=[b"stress"], track_name=b"burst-test")
        await pub.publish(track_name)
        await sub.subscribe(track_name)
        await asyncio.sleep(0.5)

        total_sent = 0

        for burst_idx in range(num_bursts):
            logger.info(f"Sending burst {burst_idx + 1}/{num_bursts}")
            start = time.time()

            # Send burst as fast as possible
            for i in range(burst_size):
                obj = PublishedObject(
                    group_id=burst_idx,
                    object_id=i,
                    payload=os.urandom(4096),
                    use_datagram=False,
                )
                await pub.send_object(track_name, obj)
                total_sent += 1

            burst_time = time.time() - start
            logger.info(f"Burst sent in {burst_time:.3f}s")

            # Wait for reception
            await asyncio.sleep(burst_interval)

        await asyncio.sleep(3.0)

        logger.info(f"Total sent: {total_sent}, received: {len(received)}")
        loss_rate = (total_sent - len(received)) / total_sent if total_sent > 0 else 0
        logger.info(f"Loss rate: {loss_rate:.2%}")

        self.assertGreaterEqual(
            len(received), total_sent * 0.75, "Burst traffic lost too much data"
        )

    async def test_03_concurrent_connections(self):
        """Test with multiple concurrent connections."""
        logger.info("=" * 60)
        logger.info("TEST: Concurrent Connections")
        logger.info("=" * 60)

        num_pairs = 5  # Number of pub/sub pairs
        duration = 10  # seconds

        port = await self._start_relay(cache_mb=1000)

        all_received = []
        tracks = []

        # Create pairs
        for i in range(num_pairs):
            pub = await self._create_publisher(port, f"concurrent-pub-{i}")
            sub, received = await self._create_subscriber(port, f"concurrent-sub-{i}")
            self.publishers.append(pub)
            self.subscribers.append(sub)
            all_received.append(received)

            track_name = FullTrackName(
                namespace=[b"concurrent"], track_name=f"track_{i}".encode()
            )
            tracks.append(track_name)
            await pub.publish(track_name)
            await sub.subscribe(track_name)

        await asyncio.sleep(0.5)

        # Send on all tracks concurrently
        async def send_on_track(idx: int):
            pub = self.publishers[idx]
            track_name = tracks[idx]
            objects = 0
            start = time.time()

            while time.time() - start < duration:
                obj = PublishedObject(
                    group_id=objects // 1000,
                    object_id=objects % 1000,
                    payload=os.urandom(2048),
                    use_datagram=False,
                )
                await pub.send_object(track_name, obj)
                objects += 1
                await asyncio.sleep(0.01)  # 100 objects per second per track

            return objects

        results = await asyncio.gather(*[send_on_track(i) for i in range(num_pairs)])
        total_sent = sum(results)

        await asyncio.sleep(3.0)

        total_received = sum(len(r) for r in all_received)
        logger.info(f"Total sent: {total_sent}, received: {total_received}")
        logger.info(f"Per track: {results}")

        # Each track should have reasonable reception
        for i, received in enumerate(all_received):
            expected = results[i]
            self.assertGreaterEqual(
                len(received), expected * 0.75, f"Track {i} lost too much data"
            )

    async def test_04_mixed_payload_sizes(self):
        """Test with mixed payload sizes."""
        logger.info("=" * 60)
        logger.info("TEST: Mixed Payload Sizes")
        logger.info("=" * 60)

        port = await self._start_relay()
        pub = await self._create_publisher(port, "mixed-pub")
        sub, received = await self._create_subscriber(port, "mixed-sub")
        self.publishers.append(pub)
        self.subscribers.append(sub)

        track_name = FullTrackName(namespace=[b"stress"], track_name=b"mixed-sizes")
        await pub.publish(track_name)
        await sub.subscribe(track_name)
        await asyncio.sleep(0.5)

        # Payload sizes: 100B, 1KB, 10KB, 50KB, 100KB
        sizes = [100, 1024, 10240, 51200, 102400]
        objects_per_size = 20

        total_sent = 0
        for size in sizes:
            logger.info(f"Sending {objects_per_size} objects of {size} bytes")
            for i in range(objects_per_size):
                obj = PublishedObject(
                    group_id=size,
                    object_id=i,
                    payload=os.urandom(size),
                    use_datagram=False,
                )
                await pub.send_object(track_name, obj)
                total_sent += 1
            await asyncio.sleep(0.5)

        await asyncio.sleep(3.0)

        logger.info(f"Total sent: {total_sent}, received: {len(received)}")

        self.assertGreaterEqual(
            len(received), total_sent * 0.85, "Mixed sizes lost too much data"
        )

    async def test_05_rapid_subscribe_unsubscribe(self):
        """Test rapid subscribe/unsubscribe cycles."""
        logger.info("=" * 60)
        logger.info("TEST: Rapid Subscribe/Unsubscribe")
        logger.info("=" * 60)

        cycles = 10
        port = await self._start_relay()
        pub = await self._create_publisher(port, "rapid-pub")
        self.publishers.append(pub)

        track_name = FullTrackName(namespace=[b"stress"], track_name=b"rapid-sub-test")
        await pub.publish(track_name)

        total_received = 0

        for cycle in range(cycles):
            # Create subscriber
            sub, received = await self._create_subscriber(port, f"rapid-sub-{cycle}")
            await sub.subscribe(track_name)
            await asyncio.sleep(0.2)

            # Send data
            for i in range(5):
                obj = PublishedObject(
                    group_id=cycle,
                    object_id=i,
                    payload=os.urandom(1024),
                    use_datagram=False,
                )
                await pub.send_object(track_name, obj)

            await asyncio.sleep(0.5)

            cycle_received = len(received)
            total_received += cycle_received
            logger.info(f"Cycle {cycle + 1}: received {cycle_received}/5")

            # Cleanup
            sub.disconnect()

        logger.info(f"Total received across all cycles: {total_received}")

        # Should have received data in most cycles
        avg_per_cycle = total_received / cycles
        self.assertGreater(
            avg_per_cycle, 3, f"Low average reception: {avg_per_cycle:.1f} per cycle"
        )


if __name__ == "__main__":
    unittest.main()
