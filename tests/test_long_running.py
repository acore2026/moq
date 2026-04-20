"""
Long running tests for MOQ Transport stability.

Tests for:
- Extended duration (minutes to hours)
- Memory leak detection
- Connection stability over time
- Resource exhaustion handling
- Automatic recovery
"""

import asyncio
import gc
import logging
import os
import resource
import sys
import time
import tracemalloc
import unittest
from pathlib import Path
from typing import Optional, List, Dict, Tuple
import psutil

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from moq import MOQPublisher, MOQSubscriber, MOQRelay
from moq.encoding import FullTrackName
from moq.pub import PublishedObject
from moq.sub import ReceivedObject

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class MemoryTracker:
    """Track memory usage over time."""

    def __init__(self):
        self.measurements: List[Dict] = []
        self.process = psutil.Process()
        tracemalloc.start()

    def snapshot(self, label: str = ""):
        """Take a memory snapshot."""
        gc.collect()
        current, peak = tracemalloc.get_traced_memory()
        mem_info = self.process.memory_info()

        measurement = {
            "timestamp": time.time(),
            "label": label,
            "rss_mb": mem_info.rss / (1024 * 1024),
            "vms_mb": mem_info.vms / (1024 * 1024),
            "traced_current_mb": current / (1024 * 1024),
            "traced_peak_mb": peak / (1024 * 1024),
        }
        self.measurements.append(measurement)
        return measurement

    def get_growth(self) -> float:
        """Get memory growth from first to last measurement."""
        if len(self.measurements) < 2:
            return 0.0
        first = self.measurements[0]["rss_mb"]
        last = self.measurements[-1]["rss_mb"]
        return last - first

    def print_report(self):
        """Print memory usage report."""
        logger.info("=" * 60)
        logger.info("Memory Usage Report")
        logger.info("=" * 60)
        for m in self.measurements:
            logger.info(
                f"{m['label']:30s} RSS: {m['rss_mb']:8.2f} MB, "
                f"Traced: {m['traced_current_mb']:8.2f} MB"
            )

        if len(self.measurements) >= 2:
            growth = self.get_growth()
            logger.info(f"Memory growth: {growth:+.2f} MB")
            if growth > 100:  # More than 100MB growth
                logger.warning("Significant memory growth detected!")

    def stop(self):
        tracemalloc.stop()


class TestLongRunning(unittest.IsolatedAsyncioTestCase):
    """Long running stability tests."""

    async def asyncSetUp(self):
        self.relay: Optional[MOQRelay] = None
        self.publisher: Optional[MOQPublisher] = None
        self.subscriber: Optional[MOQSubscriber] = None
        self.memory_tracker = MemoryTracker()

    async def asyncTearDown(self):
        if self.subscriber:
            self.subscriber.disconnect()
        if self.publisher:
            self.publisher.disconnect()
        if self.relay:
            await self.relay.stop()
        self.memory_tracker.stop()

    async def _start_relay(self) -> int:
        """Start relay and return port."""
        self.relay = MOQRelay(
            host="127.0.0.1",
            port=0,
            max_memory_cache=100 * 1024 * 1024,
        )
        await self.relay.start()
        port = self.relay._quic_server.actual_port or 4433
        return port

    async def _create_publisher(self, port: int) -> MOQPublisher:
        """Create and connect publisher."""
        pub = MOQPublisher("127.0.0.1", port)
        connected = asyncio.Event()
        pub.set_handlers(on_connected=lambda: connected.set())
        success = await pub.connect(agent_id="long-pub")
        self.assertTrue(success)
        await asyncio.wait_for(connected.wait(), timeout=5.0)
        return pub

    async def _create_subscriber(self, port: int) -> Tuple[MOQSubscriber, List]:
        """Create and connect subscriber."""
        sub = MOQSubscriber("127.0.0.1", port)
        connected = asyncio.Event()
        received = []

        def on_object(obj):
            received.append({"time": time.time(), "size": len(obj.payload)})

        sub.set_handlers(
            on_connected=lambda: connected.set(), on_object_received=on_object
        )
        success = await sub.connect(agent_id="long-sub")
        self.assertTrue(success)
        await asyncio.wait_for(connected.wait(), timeout=5.0)
        return sub, received

    async def test_01_continuous_transmission_5_minutes(self):
        """Test continuous data transmission for 5 minutes."""
        logger.info("=" * 60)
        logger.info("TEST: Continuous Transmission (5 minutes)")
        logger.info("=" * 60)

        duration = 300  # 5 minutes
        interval = 0.1  # 100ms between objects

        port = await self._start_relay()
        self.memory_tracker.snapshot("start")

        self.publisher = await self._create_publisher(port)
        self.subscriber, received = await self._create_subscriber(port)

        track_name = FullTrackName(namespace=[b"long"], track_name=b"continuous")
        await self.publisher.publish(track_name)
        await self.subscriber.subscribe(track_name)
        await asyncio.sleep(0.5)

        start_time = time.time()
        objects_sent = 0
        last_status_time = start_time

        while time.time() - start_time < duration:
            obj = PublishedObject(
                group_id=objects_sent // 10000,
                object_id=objects_sent % 10000,
                payload=os.urandom(1024),  # 1KB objects
                use_datagram=False,
            )
            await self.publisher.send_object(track_name, obj)
            objects_sent += 1

            # Status update every 30 seconds
            current_time = time.time()
            if current_time - last_status_time >= 30:
                elapsed = current_time - start_time
                rate = objects_sent / elapsed
                mem = self.memory_tracker.snapshot(f"{elapsed:.0f}s")
                logger.info(
                    f"Progress: {elapsed:.0f}s, sent: {objects_sent}, "
                    f"rate: {rate:.1f} obj/s, RSS: {mem['rss_mb']:.1f} MB"
                )
                last_status_time = current_time

            await asyncio.sleep(interval)

        await asyncio.sleep(3.0)  # Allow final objects to be received

        total_time = time.time() - start_time
        self.memory_tracker.snapshot("end")

        # Verify
        logger.info(f"Total time: {total_time:.1f}s")
        logger.info(f"Objects sent: {objects_sent}")
        logger.info(f"Objects received: {len(received)}")
        logger.info(f"Send rate: {objects_sent / total_time:.1f} obj/s")

        loss_rate = (
            (objects_sent - len(received)) / objects_sent if objects_sent > 0 else 0
        )
        logger.info(f"Loss rate: {loss_rate:.2%}")

        self.memory_tracker.print_report()

        # Check for significant memory growth (leak)
        growth = self.memory_tracker.get_growth()
        logger.info(f"Memory growth: {growth:.2f} MB")

        # Allow some loss in long-running test
        self.assertGreaterEqual(
            len(received),
            objects_sent * 0.85,
            f"Too many objects lost: {loss_rate:.2%}",
        )

        # Check for memory leak (>200MB growth considered significant)
        if growth > 200:
            self.fail(f"Possible memory leak: {growth:.1f} MB growth")

    async def test_02_connection_stability_idle_periods(self):
        """Test connection stability during idle periods."""
        logger.info("=" * 60)
        logger.info("TEST: Connection Stability with Idle Periods")
        logger.info("=" * 60)

        port = await self._start_relay()
        self.memory_tracker.snapshot("start")

        self.publisher = await self._create_publisher(port)
        self.subscriber, received = await self._create_subscriber(port)

        track_name = FullTrackName(namespace=[b"long"], track_name=b"idle-test")
        await self.publisher.publish(track_name)
        await self.subscriber.subscribe(track_name)

        # Test sequence: active -> idle -> active -> idle
        phases = [
            ("active_1", 10, 0.1),  # 10s active
            ("idle_1", 30, None),  # 30s idle
            ("active_2", 10, 0.1),  # 10s active
            ("idle_2", 60, None),  # 60s idle
            ("active_3", 10, 0.1),  # 10s active
        ]

        total_sent = 0

        for phase_name, duration, interval in phases:
            logger.info(f"Phase: {phase_name} ({duration}s)")
            self.memory_tracker.snapshot(phase_name)

            if interval:
                # Active phase - send data
                start = time.time()
                while time.time() - start < duration:
                    obj = PublishedObject(
                        group_id=total_sent // 1000,
                        object_id=total_sent % 1000,
                        payload=os.urandom(1024),
                        use_datagram=False,
                    )
                    await self.publisher.send_object(track_name, obj)
                    total_sent += 1
                    await asyncio.sleep(interval)
            else:
                # Idle phase - just wait
                await asyncio.sleep(duration)

            # Send a marker object after each phase
            marker = PublishedObject(
                group_id=9999,
                object_id=hash(phase_name) % 10000,
                payload=f"MARKER:{phase_name}".encode(),
                use_datagram=False,
            )
            await self.publisher.send_object(track_name, marker)
            total_sent += 1

        await asyncio.sleep(3.0)

        self.memory_tracker.snapshot("end")
        self.memory_tracker.print_report()

        logger.info(f"Total sent: {total_sent}, received: {len(received)}")

        # Should have received marker objects after idle periods
        markers_received = sum(1 for r in received if r["size"] < 100)
        logger.info(f"Markers received: {markers_received}")

        self.assertGreaterEqual(
            markers_received, 3, "Should receive markers after idle periods"
        )

    async def test_03_repeated_connect_disconnect(self):
        """Test repeated connect/disconnect cycles."""
        logger.info("=" * 60)
        logger.info("TEST: Repeated Connect/Disconnect")
        logger.info("=" * 60)

        cycles = 10
        port = await self._start_relay()

        track_name = FullTrackName(namespace=[b"long"], track_name=b"reconnect-test")

        for cycle in range(cycles):
            logger.info(f"Cycle {cycle + 1}/{cycles}")
            self.memory_tracker.snapshot(f"cycle_{cycle}_start")

            # Create publisher
            self.publisher = await self._create_publisher(port)
            await self.publisher.publish(track_name)

            # Create subscriber
            self.subscriber, received = await self._create_subscriber(port)
            await self.subscriber.subscribe(track_name)
            await asyncio.sleep(0.5)

            # Send data
            for i in range(10):
                obj = PublishedObject(
                    group_id=cycle,
                    object_id=i,
                    payload=os.urandom(1024),
                    use_datagram=False,
                )
                await self.publisher.send_object(track_name, obj)

            await asyncio.sleep(1.0)

            logger.info(f"  Received: {len(received)}/10")

            # Cleanup
            self.subscriber.disconnect()
            self.publisher.disconnect()
            self.subscriber = None
            self.publisher = None

            self.memory_tracker.snapshot(f"cycle_{cycle}_end")

            # Small delay between cycles
            await asyncio.sleep(0.5)

        self.memory_tracker.snapshot("final")
        self.memory_tracker.print_report()

        # Check memory hasn't grown excessively
        growth = self.memory_tracker.get_growth()
        logger.info(f"Total memory growth: {growth:.2f} MB over {cycles} cycles")

        # Allow some growth but not excessive
        self.assertLess(growth, 100, f"Memory growth too high: {growth:.1f} MB")

    async def test_04_multiple_tracks_stress(self):
        """Test stress with many concurrent tracks."""
        logger.info("=" * 60)
        logger.info("TEST: Multiple Tracks Stress Test")
        logger.info("=" * 60)

        num_tracks = 20
        duration = 60  # 1 minute

        port = await self._start_relay()
        self.memory_tracker.snapshot("start")

        self.publisher = await self._create_publisher(port)
        self.subscriber, received = await self._create_subscriber(port)

        # Create tracks
        tracks = []
        for i in range(num_tracks):
            track_name = FullTrackName(
                namespace=[b"stress"], track_name=f"track_{i}".encode()
            )
            tracks.append(track_name)
            await self.publisher.publish(track_name)
            await self.subscriber.subscribe(track_name)

        await asyncio.sleep(0.5)

        start_time = time.time()
        objects_sent = 0

        while time.time() - start_time < duration:
            # Send on random track
            track_idx = objects_sent % num_tracks
            obj = PublishedObject(
                group_id=objects_sent // 1000,
                object_id=objects_sent % 1000,
                payload=os.urandom(512),  # Smaller objects
                use_datagram=False,
            )
            await self.publisher.send_object(tracks[track_idx], obj)
            objects_sent += 1

            if objects_sent % 100 == 0:
                elapsed = time.time() - start_time
                logger.info(f"Progress: {elapsed:.0f}s, sent: {objects_sent}")

            await asyncio.sleep(0.01)  # 100 objects per second

        await asyncio.sleep(3.0)

        self.memory_tracker.snapshot("end")

        logger.info(f"Total sent: {objects_sent}, received: {len(received)}")
        self.memory_tracker.print_report()

        growth = self.memory_tracker.get_growth()
        self.assertLess(
            growth, 150, f"Memory growth too high with many tracks: {growth:.1f} MB"
        )

    async def test_05_large_object_churn(self):
        """Test with large objects being created and discarded."""
        logger.info("=" * 60)
        logger.info("TEST: Large Object Churn")
        logger.info("=" * 60)

        iterations = 50
        objects_per_iteration = 10
        object_size = 100 * 1024  # 100KB

        port = await self._start_relay()
        self.memory_tracker.snapshot("start")

        self.publisher = await self._create_publisher(port)
        self.subscriber, received = await self._create_subscriber(port)

        track_name = FullTrackName(namespace=[b"long"], track_name=b"churn-test")
        await self.publisher.publish(track_name)
        await self.subscriber.subscribe(track_name)

        for iteration in range(iterations):
            # Send batch of large objects
            for i in range(objects_per_iteration):
                obj = PublishedObject(
                    group_id=iteration,
                    object_id=i,
                    payload=os.urandom(object_size),
                    use_datagram=False,
                )
                await self.publisher.send_object(track_name, obj)

            # Wait for reception
            await asyncio.sleep(1.0)

            # Force garbage collection
            gc.collect()

            if (iteration + 1) % 10 == 0:
                mem = self.memory_tracker.snapshot(f"iter_{iteration + 1}")
                total_sent = (iteration + 1) * objects_per_iteration
                logger.info(
                    f"Iteration {iteration + 1}: sent {total_sent} large objects, "
                    f"RSS: {mem['rss_mb']:.1f} MB"
                )

        await asyncio.sleep(5.0)
        self.memory_tracker.snapshot("end")

        logger.info(
            f"Total sent: {iterations * objects_per_iteration}, "
            f"received: {len(received)}"
        )
        self.memory_tracker.print_report()

        # Memory should stabilize, not grow indefinitely
        growth = self.memory_tracker.get_growth()
        self.assertLess(growth, 300, f"Memory growth too high: {growth:.1f} MB")


if __name__ == "__main__":
    unittest.main()
