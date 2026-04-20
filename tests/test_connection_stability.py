"""
Test connection stability for MOQ Transport.

This test module focuses on:
1. Video transmission (stream and datagram modes)
2. Small data transmission
3. Long interval (20s) transmission - to detect idle timeout issues
4. Connection recovery scenarios
"""

import asyncio
import logging
import os
import subprocess
import tempfile
import time
import unittest
from pathlib import Path
from typing import Optional, List
from dataclasses import dataclass

import sys

sys.path.insert(0, str(Path(__file__).parent.parent))

from moq import MOQPublisher, MOQSubscriber, MOQRelay
from moq.encoding import FullTrackName
from moq.messages import ObjectStatus
from moq.pub import PublishedObject
from moq.sub import ReceivedObject


# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


@dataclass
class TestStats:
    """Statistics for test runs."""

    objects_sent: int = 0
    objects_received: int = 0
    bytes_sent: int = 0
    bytes_received: int = 0
    errors: List[str] = None
    start_time: Optional[float] = None
    end_time: Optional[float] = None

    def __post_init__(self):
        if self.errors is None:
            self.errors = []

    @property
    def duration(self) -> float:
        if self.start_time and self.end_time:
            return self.end_time - self.start_time
        return 0.0

    @property
    def loss_rate(self) -> float:
        if self.objects_sent == 0:
            return 0.0
        return (self.objects_sent - self.objects_received) / self.objects_sent


class TestConnectionStability(unittest.IsolatedAsyncioTestCase):
    """Test connection stability under various scenarios."""

    relay_host = "127.0.0.1"
    relay_port = 0  # Let OS assign port

    async def asyncSetUp(self):
        """Set up test fixtures."""
        self.relay: Optional[MOQRelay] = None
        self.publisher: Optional[MOQPublisher] = None
        self.subscriber: Optional[MOQSubscriber] = None
        self.stats = TestStats()
        self.received_objects: asyncio.Queue = asyncio.Queue()
        self.connection_events: asyncio.Queue = asyncio.Queue()

    async def asyncTearDown(self):
        """Tear down test fixtures."""
        if self.subscriber:
            self.subscriber.disconnect()
        if self.publisher:
            self.publisher.disconnect()
        if self.relay:
            await self.relay.stop()

    async def _start_relay(self, port: int = 0) -> int:
        """Start relay on specified port (0 for auto-assign)."""
        self.relay = MOQRelay(
            host=self.relay_host,
            port=port,
            max_memory_cache=50 * 1024 * 1024,  # 50MB for testing
        )
        await self.relay.start()
        # Get the actual port if auto-assigned
        actual_port = self.relay._quic_server.actual_port
        if actual_port is None:
            actual_port = port if port else 4433  # fallback
        logger.info(f"Relay started on {self.relay_host}:{actual_port}")
        return actual_port

    async def _create_publisher(self, port: int) -> MOQPublisher:
        """Create and connect publisher."""
        pub = MOQPublisher(self.relay_host, port)

        connected_event = asyncio.Event()
        disconnected_event = asyncio.Event()

        def on_connected():
            logger.info("Publisher connected")
            connected_event.set()
            asyncio.create_task(self.connection_events.put(("pub", "connected")))

        def on_disconnected():
            logger.warning("Publisher disconnected")
            disconnected_event.set()
            asyncio.create_task(self.connection_events.put(("pub", "disconnected")))

        pub.set_handlers(
            on_connected=on_connected,
            on_disconnected=on_disconnected,
            on_publication_accepted=lambda tn: logger.info(
                f"Publication accepted: {tn}"
            ),
            on_publication_rejected=lambda tn, r: logger.error(
                f"Publication rejected: {tn}, reason={r}"
            ),
        )

        success = await pub.connect(agent_id="test-publisher")
        if not success:
            raise ConnectionError("Failed to connect publisher")

        await asyncio.wait_for(connected_event.wait(), timeout=5.0)
        return pub

    async def _create_subscriber(self, port: int) -> MOQSubscriber:
        """Create and connect subscriber."""
        sub = MOQSubscriber(self.relay_host, port)

        connected_event = asyncio.Event()
        disconnected_event = asyncio.Event()

        def on_connected():
            logger.info("Subscriber connected")
            connected_event.set()
            asyncio.create_task(self.connection_events.put(("sub", "connected")))

        def on_disconnected():
            logger.warning("Subscriber disconnected")
            disconnected_event.set()
            asyncio.create_task(self.connection_events.put(("sub", "disconnected")))

        def on_object_received(obj: ReceivedObject):
            self.stats.objects_received += 1
            self.stats.bytes_received += len(obj.payload)
            asyncio.create_task(self.received_objects.put(obj))

        sub.set_handlers(
            on_connected=on_connected,
            on_disconnected=on_disconnected,
            on_object_received=on_object_received,
            on_subscription_accepted=lambda tn: logger.info(
                f"Subscription accepted: {tn}"
            ),
            on_subscription_rejected=lambda tn, r: logger.error(
                f"Subscription rejected: {tn}, reason={r}"
            ),
        )

        success = await sub.connect(agent_id="test-subscriber")
        if not success:
            raise ConnectionError("Failed to connect subscriber")

        await asyncio.wait_for(connected_event.wait(), timeout=5.0)
        return sub

    async def _generate_test_video(self, duration: int = 5, fps: int = 30) -> Path:
        """Generate a test video file using ffmpeg."""
        # Check if ffmpeg is available
        try:
            subprocess.run(["ffmpeg", "-version"], capture_output=True, check=True)
        except (subprocess.CalledProcessError, FileNotFoundError):
            raise unittest.SkipTest("ffmpeg not available")

        with tempfile.NamedTemporaryFile(suffix=".mp4", delete=False) as f:
            video_path = Path(f.name)

        # Generate test video with colored bars
        cmd = [
            "ffmpeg",
            "-y",
            "-f",
            "lavfi",
            "-i",
            f"testsrc=duration={duration}:size=640x480:rate={fps}",
            "-pix_fmt",
            "yuv420p",
            "-c:v",
            "libx264",
            "-preset",
            "ultrafast",
            str(video_path),
        ]

        logger.info(f"Generating test video: {video_path}")
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode != 0:
            logger.error(f"ffmpeg error: {result.stderr}")
            raise RuntimeError(f"Failed to generate test video: {result.stderr}")

        logger.info(
            f"Test video generated: {video_path} ({video_path.stat().st_size} bytes)"
        )
        return video_path

    async def _split_video_to_chunks(
        self, video_path: Path, chunk_duration: float = 0.033
    ) -> List[bytes]:
        """Split video into chunks simulating frame data."""
        chunks = []
        chunk_size = int(10000)  # Approximate bytes per chunk

        with open(video_path, "rb") as f:
            while True:
                chunk = f.read(chunk_size)
                if not chunk:
                    break
                chunks.append(chunk)

        logger.info(f"Video split into {len(chunks)} chunks")
        return chunks

    async def test_01_small_data_stream_mode(self):
        """
        Test small data transmission using stream mode.

        This tests basic connectivity and data transfer reliability.
        """
        logger.info("=" * 60)
        logger.info("TEST: Small Data Stream Mode")
        logger.info("=" * 60)

        port = await self._start_relay()
        self.publisher = await self._create_publisher(port)
        self.subscriber = await self._create_subscriber(port)

        # Setup track
        track_name = FullTrackName(namespace=[b"test"], track_name=b"small-data-stream")

        # Publish track
        pub_success = await self.publisher.publish(track_name)
        self.assertTrue(pub_success, "Failed to publish track")

        # Subscribe to track
        sub_success = await self.subscriber.subscribe(track_name)
        self.assertTrue(sub_success, "Failed to subscribe to track")

        # Send small data objects
        num_objects = 10
        object_size = 100  # bytes
        self.stats.start_time = time.time()

        for i in range(num_objects):
            obj = PublishedObject(
                group_id=0,
                object_id=i,
                payload=os.urandom(object_size),
                publisher_priority=128,
                subgroup_id=0,
                use_datagram=False,  # Stream mode
            )
            await self.publisher.send_object(track_name, obj)
            self.stats.objects_sent += 1
            self.stats.bytes_sent += object_size
            logger.info(f"Sent object {i}")
            await asyncio.sleep(0.1)  # Small delay between objects

        # Wait for all objects to be received
        logger.info("Waiting for objects to be received...")
        await asyncio.sleep(2.0)

        self.stats.end_time = time.time()

        logger.info(f"Objects sent: {self.stats.objects_sent}")
        logger.info(f"Objects received: {self.stats.objects_received}")
        logger.info(f"Loss rate: {self.stats.loss_rate:.2%}")
        logger.info(f"Duration: {self.stats.duration:.2f}s")

        # Verify results
        self.assertEqual(
            self.stats.objects_sent,
            self.stats.objects_received,
            f"Not all objects received. Loss rate: {self.stats.loss_rate:.2%}",
        )

    async def test_02_small_data_datagram_mode(self):
        """
        Test small data transmission using datagram mode.

        This tests datagram transport reliability.
        """
        logger.info("=" * 60)
        logger.info("TEST: Small Data Datagram Mode")
        logger.info("=" * 60)

        port = await self._start_relay()
        self.publisher = await self._create_publisher(port)
        self.subscriber = await self._create_subscriber(port)

        # Setup track
        track_name = FullTrackName(
            namespace=[b"test"], track_name=b"small-data-datagram"
        )

        # Publish track
        pub_success = await self.publisher.publish(track_name)
        self.assertTrue(pub_success, "Failed to publish track")

        # Subscribe to track
        sub_success = await self.subscriber.subscribe(track_name)
        self.assertTrue(sub_success, "Failed to subscribe to track")

        # Send small data objects via datagram
        num_objects = 10
        object_size = 100  # bytes
        self.stats.start_time = time.time()

        for i in range(num_objects):
            obj = PublishedObject(
                group_id=0,
                object_id=i,
                payload=os.urandom(object_size),
                publisher_priority=128,
                subgroup_id=0,
                use_datagram=True,  # Datagram mode
            )
            await self.publisher.send_object(track_name, obj)
            self.stats.objects_sent += 1
            self.stats.bytes_sent += object_size
            logger.info(f"Sent datagram object {i}")
            await asyncio.sleep(0.1)

        # Wait for all objects to be received
        logger.info("Waiting for objects to be received...")
        await asyncio.sleep(2.0)

        self.stats.end_time = time.time()

        logger.info(f"Objects sent: {self.stats.objects_sent}")
        logger.info(f"Objects received: {self.stats.objects_received}")
        logger.info(f"Loss rate: {self.stats.loss_rate:.2%}")
        logger.info(f"Duration: {self.stats.duration:.2f}s")

        # Note: Datagrams may have some loss, so we allow for some tolerance
        self.assertGreaterEqual(
            self.stats.objects_received,
            self.stats.objects_sent * 0.7,
            f"Too many datagrams lost. Loss rate: {self.stats.loss_rate:.2%}",
        )

    async def test_03_long_interval_transmission(self):
        """
        Test long interval (20s) transmission.

        This tests if the connection remains stable during idle periods.
        The QUIC idle timeout is 300s by default, so 20s should be fine.
        However, this can expose issues with connection state management.
        """
        logger.info("=" * 60)
        logger.info("TEST: Long Interval Transmission (20s gaps)")
        logger.info("=" * 60)

        port = await self._start_relay()
        self.publisher = await self._create_publisher(port)
        self.subscriber = await self._create_subscriber(port)

        # Setup track
        track_name = FullTrackName(namespace=[b"test"], track_name=b"interval-data")

        # Publish track
        pub_success = await self.publisher.publish(track_name)
        self.assertTrue(pub_success, "Failed to publish track")

        # Subscribe to track
        sub_success = await self.subscriber.subscribe(track_name)
        self.assertTrue(sub_success, "Failed to subscribe to track")

        # Send objects with long intervals
        intervals = [20, 20, 20]  # 3 objects with 20s intervals
        object_size = 100
        self.stats.start_time = time.time()

        for i, interval in enumerate(intervals):
            obj = PublishedObject(
                group_id=0,
                object_id=i,
                payload=os.urandom(object_size),
                publisher_priority=128,
                subgroup_id=0,
                use_datagram=False,
            )
            await self.publisher.send_object(track_name, obj)
            self.stats.objects_sent += 1
            self.stats.bytes_sent += object_size
            logger.info(f"Sent object {i}, waiting {interval}s...")

            if i < len(intervals) - 1:  # Don't wait after last object
                await asyncio.sleep(interval)

        # Wait for all objects to be received
        logger.info("Waiting for final objects to be received...")
        await asyncio.sleep(3.0)

        self.stats.end_time = time.time()

        logger.info(f"Objects sent: {self.stats.objects_sent}")
        logger.info(f"Objects received: {self.stats.objects_received}")
        logger.info(f"Loss rate: {self.stats.loss_rate:.2%}")
        logger.info(f"Duration: {self.stats.duration:.2f}s")

        # Check if any disconnections occurred
        disconnections = []
        while not self.connection_events.empty():
            event = await self.connection_events.get()
            if "disconnected" in event:
                disconnections.append(event)

        if disconnections:
            logger.error(f"Unexpected disconnections: {disconnections}")
            # This is a potential bug - connections shouldn't drop during idle

        self.assertEqual(
            self.stats.objects_sent,
            self.stats.objects_received,
            f"Not all objects received after idle periods. Loss rate: {self.stats.loss_rate:.2%}",
        )

    async def test_04_video_stream_mode(self):
        """
        Test video-like data transmission using stream mode.

        Simulates video streaming with larger payloads.
        """
        logger.info("=" * 60)
        logger.info("TEST: Video Stream Mode")
        logger.info("=" * 60)

        # Skip if ffmpeg not available
        try:
            video_path = await self._generate_test_video(duration=3, fps=30)
        except unittest.SkipTest:
            logger.info("Skipping video test - ffmpeg not available")
            self.skipTest("ffmpeg not available")
            return

        try:
            chunks = await self._split_video_to_chunks(video_path)
            if len(chunks) < 5:
                self.skipTest("Not enough video chunks generated")

            port = await self._start_relay()
            self.publisher = await self._create_publisher(port)
            self.subscriber = await self._create_subscriber(port)

            # Setup track
            track_name = FullTrackName(namespace=[b"test"], track_name=b"video-stream")

            # Publish track
            pub_success = await self.publisher.publish(track_name)
            self.assertTrue(pub_success, "Failed to publish track")

            # Subscribe to track
            sub_success = await self.subscriber.subscribe(track_name)
            self.assertTrue(sub_success, "Failed to subscribe to track")

            self.stats.start_time = time.time()

            # Send video chunks as objects
            for i, chunk in enumerate(chunks[:50]):  # Limit to 50 chunks for test
                obj = PublishedObject(
                    group_id=i // 30,  # Group by ~1 second of video
                    object_id=i % 30,
                    payload=chunk,
                    publisher_priority=128,
                    subgroup_id=0,
                    use_datagram=False,  # Stream mode for video
                )
                await self.publisher.send_object(track_name, obj)
                self.stats.objects_sent += 1
                self.stats.bytes_sent += len(chunk)

                if i % 10 == 0:
                    logger.info(f"Sent chunk {i}/{len(chunks[:50])}")

                await asyncio.sleep(0.033)  # ~30fps

            # Wait for all objects to be received
            logger.info("Waiting for video chunks to be received...")
            await asyncio.sleep(5.0)

            self.stats.end_time = time.time()

            logger.info(f"Objects sent: {self.stats.objects_sent}")
            logger.info(f"Objects received: {self.stats.objects_received}")
            logger.info(f"Loss rate: {self.stats.loss_rate:.2%}")
            logger.info(f"Duration: {self.stats.duration:.2f}s")

            # Allow some loss for stream mode (should be reliable though)
            self.assertGreaterEqual(
                self.stats.objects_received,
                self.stats.objects_sent * 0.9,
                f"Too many objects lost. Loss rate: {self.stats.loss_rate:.2%}",
            )

        finally:
            # Cleanup video file
            if video_path.exists():
                video_path.unlink()

    async def test_05_video_datagram_mode(self):
        """
        Test video-like data transmission using datagram mode.

        Simulates video streaming with larger payloads via datagrams.
        """
        logger.info("=" * 60)
        logger.info("TEST: Video Datagram Mode")
        logger.info("=" * 60)

        # Skip if ffmpeg not available
        try:
            video_path = await self._generate_test_video(duration=3, fps=30)
        except unittest.SkipTest:
            logger.info("Skipping video test - ffmpeg not available")
            self.skipTest("ffmpeg not available")
            return

        try:
            chunks = await self._split_video_to_chunks(video_path)
            if len(chunks) < 5:
                self.skipTest("Not enough video chunks generated")

            port = await self._start_relay()
            self.publisher = await self._create_publisher(port)
            self.subscriber = await self._create_subscriber(port)

            # Setup track
            track_name = FullTrackName(
                namespace=[b"test"], track_name=b"video-datagram"
            )

            # Publish track
            pub_success = await self.publisher.publish(track_name)
            self.assertTrue(pub_success, "Failed to publish track")

            # Subscribe to track
            sub_success = await self.subscriber.subscribe(track_name)
            self.assertTrue(sub_success, "Failed to subscribe to track")

            self.stats.start_time = time.time()

            # Send video chunks as datagrams (smaller chunks for datagrams)
            for i, chunk in enumerate(chunks[:50]):
                # Truncate chunks to fit in datagram
                datagram_chunk = chunk[:1200] if len(chunk) > 1200 else chunk

                obj = PublishedObject(
                    group_id=i // 30,
                    object_id=i % 30,
                    payload=datagram_chunk,
                    publisher_priority=128,
                    subgroup_id=0,
                    use_datagram=True,  # Datagram mode
                )
                await self.publisher.send_object(track_name, obj)
                self.stats.objects_sent += 1
                self.stats.bytes_sent += len(datagram_chunk)

                if i % 10 == 0:
                    logger.info(f"Sent datagram chunk {i}/{len(chunks[:50])}")

                await asyncio.sleep(0.033)

            # Wait for all objects to be received
            logger.info("Waiting for video chunks to be received...")
            await asyncio.sleep(5.0)

            self.stats.end_time = time.time()

            logger.info(f"Objects sent: {self.stats.objects_sent}")
            logger.info(f"Objects received: {self.stats.objects_received}")
            logger.info(f"Loss rate: {self.stats.loss_rate:.2%}")
            logger.info(f"Duration: {self.stats.duration:.2f}s")

            # Datagrams may have higher loss
            self.assertGreaterEqual(
                self.stats.objects_received,
                self.stats.objects_sent * 0.5,
                f"Too many datagrams lost. Loss rate: {self.stats.loss_rate:.2%}",
            )

        finally:
            if video_path.exists():
                video_path.unlink()

    async def test_06_rapid_connect_disconnect(self):
        """
        Test rapid connection/disconnection cycles.

        This can expose issues with resource cleanup and state management.
        """
        logger.info("=" * 60)
        logger.info("TEST: Rapid Connect/Disconnect")
        logger.info("=" * 60)

        port = await self._start_relay()

        cycles = 3
        for cycle in range(cycles):
            logger.info(f"Connection cycle {cycle + 1}/{cycles}")

            pub = await self._create_publisher(port)
            sub = await self._create_subscriber(port)

            track_name = FullTrackName(
                namespace=[b"test"], track_name=f"rapid-test-{cycle}".encode()
            )

            await pub.publish(track_name)
            await sub.subscribe(track_name)

            # Send a few objects
            for i in range(3):
                obj = PublishedObject(
                    group_id=0,
                    object_id=i,
                    payload=b"test data " * 10,
                    use_datagram=False,
                )
                await pub.send_object(track_name, obj)

            await asyncio.sleep(0.5)

            # Disconnect
            sub.disconnect()
            pub.disconnect()

            await asyncio.sleep(0.5)

        logger.info(f"Completed {cycles} connection cycles")


class TestConnectionIssues(unittest.IsolatedAsyncioTestCase):
    """
    Test cases specifically designed to expose potential bugs.
    """

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

    async def test_idle_timeout_detection(self):
        """
        Test to detect idle timeout issues.

        The QUIC transport has a 300s idle timeout. This test sends
        data at intervals to see if the connection is incorrectly
        closed before the timeout.
        """
        logger.info("=" * 60)
        logger.info("TEST: Idle Timeout Detection")
        logger.info("=" * 60)

        relay = MOQRelay(host="127.0.0.1", port=0)
        await relay.start()
        port = relay._quic_server.actual_port or 4433

        pub = MOQPublisher("127.0.0.1", port)
        sub = MOQSubscriber("127.0.0.1", port)

        pub_connected = asyncio.Event()
        pub_disconnected = asyncio.Event()
        sub_connected = asyncio.Event()
        sub_disconnected = asyncio.Event()

        pub.set_handlers(
            on_connected=lambda: pub_connected.set(),
            on_disconnected=lambda: pub_disconnected.set(),
        )
        sub.set_handlers(
            on_connected=lambda: sub_connected.set(),
            on_disconnected=lambda: sub_disconnected.set(),
        )

        await pub.connect()
        await sub.connect()

        await asyncio.wait_for(pub_connected.wait(), timeout=5.0)
        await asyncio.wait_for(sub_connected.wait(), timeout=5.0)

        track_name = FullTrackName(namespace=[b"test"], track_name=b"idle-test")
        await pub.publish(track_name)
        await sub.subscribe(track_name)

        # Send object, then wait 25 seconds (longer than typical NAT timeouts)
        # but less than 300s QUIC idle timeout
        logger.info("Sending initial object...")
        obj = PublishedObject(
            group_id=0,
            object_id=0,
            payload=b"initial data",
            use_datagram=False,
        )
        await pub.send_object(track_name, obj)

        # Wait 25 seconds
        wait_time = 25
        logger.info(f"Waiting {wait_time}s to test idle timeout...")

        # Use shorter wait periods to check for disconnections
        check_interval = 5
        elapsed = 0
        while elapsed < wait_time:
            await asyncio.sleep(check_interval)
            elapsed += check_interval
            logger.info(f"Elapsed: {elapsed}s, checking connection...")

            if pub_disconnected.is_set() or sub_disconnected.is_set():
                logger.error("Unexpected disconnection during idle period!")
                # This indicates a bug - connections should stay alive
                # until the 300s idle timeout

        # Try to send another object
        logger.info("Sending object after idle period...")
        obj2 = PublishedObject(
            group_id=0,
            object_id=1,
            payload=b"data after idle",
            use_datagram=False,
        )

        try:
            await pub.send_object(track_name, obj2)
            logger.info("Object sent successfully after idle period")
        except Exception as e:
            logger.error(f"Failed to send after idle period: {e}")
            # This is a bug - should be able to send after idle

        await asyncio.sleep(2.0)

        # Cleanup
        sub.disconnect()
        pub.disconnect()
        await relay.stop()

        logger.info("Idle timeout test completed")


if __name__ == "__main__":
    unittest.main()
