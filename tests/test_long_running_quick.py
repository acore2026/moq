"""
Quick version of long running tests for verification.
"""

import asyncio
import gc
import logging
import os
import sys
import time
import unittest
from pathlib import Path
from typing import List

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from moq import MOQPublisher, MOQSubscriber, MOQRelay
from moq.encoding import FullTrackName
from moq.pub import PublishedObject

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class TestLongRunningQuick(unittest.IsolatedAsyncioTestCase):
    """Quick long running tests (shortened versions)."""

    async def asyncSetUp(self):
        self.relay = None
        self.publisher = None
        self.subscriber = None

    async def asyncTearDown(self):
        if self.subscriber:
            self.subscriber.disconnect()
        if self.publisher:
            self.publisher.disconnect()
        if self.relay:
            await self.relay.stop()

    async def _start_relay(self):
        self.relay = MOQRelay(
            host="127.0.0.1",
            port=0,
            max_memory_cache=50 * 1024 * 1024,
        )
        await self.relay.start()
        return self.relay._quic_server.actual_port or 4433

    async def _create_publisher(self, port: int, agent_id: str):
        pub = MOQPublisher("127.0.0.1", port)
        connected = asyncio.Event()
        pub.set_handlers(on_connected=lambda: connected.set())
        success = await pub.connect(agent_id=agent_id)
        self.assertTrue(success)
        await asyncio.wait_for(connected.wait(), timeout=5.0)
        return pub

    async def _create_subscriber(self, port: int, agent_id: str):
        sub = MOQSubscriber("127.0.0.1", port)
        connected = asyncio.Event()
        received = []

        def on_object(obj):
            received.append({"time": time.time(), "size": len(obj.payload)})

        sub.set_handlers(
            on_connected=lambda: connected.set(), on_object_received=on_object
        )
        success = await sub.connect(agent_id=agent_id)
        self.assertTrue(success)
        await asyncio.wait_for(connected.wait(), timeout=5.0)
        return sub, received

    async def test_quick_continuous_transmission(self):
        """Quick continuous transmission test (30 seconds)."""
        logger.info("=" * 60)
        logger.info("TEST: Quick Continuous Transmission (30s)")
        logger.info("=" * 60)

        duration = 30  # seconds
        interval = 0.1  # 100ms between objects

        port = await self._start_relay()

        self.publisher = await self._create_publisher(port, "quick-pub")
        self.subscriber, received = await self._create_subscriber(port, "quick-sub")

        track_name = FullTrackName(namespace=[b"quick"], track_name=b"continuous")
        await self.publisher.publish(track_name)
        await self.subscriber.subscribe(track_name)
        await asyncio.sleep(0.5)

        start_time = time.time()
        objects_sent = 0
        last_status_time = start_time

        while time.time() - start_time < duration:
            obj = PublishedObject(
                group_id=objects_sent // 1000,
                object_id=objects_sent % 1000,
                payload=os.urandom(1024),
                use_datagram=False,
            )
            await self.publisher.send_object(track_name, obj)
            objects_sent += 1

            current_time = time.time()
            if current_time - last_status_time >= 10:  # Log every 10s
                elapsed = current_time - start_time
                rate = objects_sent / elapsed
                logger.info(
                    f"Progress: {elapsed:.0f}s, sent: {objects_sent}, "
                    f"rate: {rate:.1f} obj/s"
                )
                last_status_time = current_time

            await asyncio.sleep(interval)

        await asyncio.sleep(3.0)

        total_time = time.time() - start_time
        logger.info(f"Total time: {total_time:.1f}s")
        logger.info(f"Objects sent: {objects_sent}")
        logger.info(f"Objects received: {len(received)}")
        logger.info(f"Send rate: {objects_sent / total_time:.1f} obj/s")

        loss_rate = (
            (objects_sent - len(received)) / objects_sent if objects_sent > 0 else 0
        )
        logger.info(f"Loss rate: {loss_rate:.2%}")

        # Should have high success rate
        self.assertGreaterEqual(
            len(received),
            objects_sent * 0.95,
            f"Too many objects lost: {loss_rate:.2%}",
        )

    async def test_quick_idle_stability(self):
        """Quick idle stability test (20 seconds total)."""
        logger.info("=" * 60)
        logger.info("TEST: Quick Idle Stability")
        logger.info("=" * 60)

        port = await self._start_relay()
        self.publisher = await self._create_publisher(port, "idle-pub")
        self.subscriber, received = await self._create_subscriber(port, "idle-sub")

        track_name = FullTrackName(namespace=[b"quick"], track_name=b"idle-test")
        await self.publisher.publish(track_name)
        await self.subscriber.subscribe(track_name)

        # Active period
        logger.info("Active period: sending 20 objects...")
        for i in range(20):
            obj = PublishedObject(
                group_id=0,
                object_id=i,
                payload=os.urandom(1024),
                use_datagram=False,
            )
            await self.publisher.send_object(track_name, obj)
            await asyncio.sleep(0.05)

        await asyncio.sleep(1.0)
        active_received = len(received)
        logger.info(f"Active period received: {active_received}")

        # Idle period
        logger.info("Idle period: waiting 5 seconds...")
        await asyncio.sleep(5.0)

        # Send again after idle
        logger.info("Sending after idle...")
        for i in range(20, 30):
            obj = PublishedObject(
                group_id=1,
                object_id=i,
                payload=os.urandom(1024),
                use_datagram=False,
            )
            await self.publisher.send_object(track_name, obj)

        await asyncio.sleep(2.0)
        total_received = len(received)
        logger.info(f"Total received: {total_received}")

        # Should receive data after idle period
        self.assertGreaterEqual(
            total_received - active_received, 8, "Should receive data after idle period"
        )


if __name__ == "__main__":
    unittest.main()
