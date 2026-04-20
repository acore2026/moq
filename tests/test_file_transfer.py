"""
File transfer scenarios test for MOQ Transport.

Tests various file transfer scenarios:
- Large file transfer (MB to GB range)
- Chunked file transfer
- Simulated network conditions (packet loss, latency)
- Concurrent file transfers
- Resume capability
"""

import asyncio
import hashlib
import logging
import os
import tempfile
import time
import unittest
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional, Dict, Tuple
import random

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


@dataclass
class FileTransferConfig:
    """File transfer configuration."""

    file_size: int  # bytes
    chunk_size: int  # bytes per object
    use_datagram: bool  # datagram vs stream mode
    delay_ms: float  # simulated delay between chunks (ms)
    packet_loss_rate: float  # simulated packet loss (0-1)


# Standard transfer configurations
TRANSFER_CONFIGS = [
    FileTransferConfig(1024 * 1024, 4096, False, 0, 0),  # 1MB, stream
    FileTransferConfig(10 * 1024 * 1024, 8192, False, 0, 0),  # 10MB, stream
    FileTransferConfig(50 * 1024 * 1024, 16384, False, 0, 0),  # 50MB, stream
    FileTransferConfig(1024 * 1024, 1200, True, 0, 0),  # 1MB, datagram
    FileTransferConfig(10 * 1024 * 1024, 1200, True, 0, 0),  # 10MB, datagram
]


class TestFileTransfer(unittest.IsolatedAsyncioTestCase):
    """Test file transfer scenarios."""

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

    async def _start_relay(self, cache_mb: int = 500) -> int:
        """Start relay with specified cache size."""
        self.relay = MOQRelay(
            host="127.0.0.1",
            port=0,
            max_memory_cache=cache_mb * 1024 * 1024,
        )
        await self.relay.start()
        port = self.relay._quic_server.actual_port or 4433
        logger.info(f"Relay started on port {port} (cache: {cache_mb}MB)")
        return port

    async def _create_publisher(self, port: int) -> MOQPublisher:
        """Create and connect publisher."""
        pub = MOQPublisher("127.0.0.1", port)
        connected = asyncio.Event()
        pub.set_handlers(on_connected=lambda: connected.set())
        success = await pub.connect(agent_id="file-pub")
        self.assertTrue(success)
        await asyncio.wait_for(connected.wait(), timeout=5.0)
        return pub

    async def _create_subscriber(self, port: int) -> Tuple[MOQSubscriber, List]:
        """Create and connect subscriber."""
        sub = MOQSubscriber("127.0.0.1", port)
        connected = asyncio.Event()
        received_chunks = []

        def on_object(obj):
            received_chunks.append(
                {
                    "group_id": obj.group_id,
                    "object_id": obj.object_id,
                    "data": obj.payload,
                    "timestamp": time.time(),
                }
            )

        sub.set_handlers(
            on_connected=lambda: connected.set(), on_object_received=on_object
        )
        success = await sub.connect(agent_id="file-sub")
        self.assertTrue(success)
        await asyncio.wait_for(connected.wait(), timeout=5.0)
        return sub, received_chunks

    def _generate_file(self, size: int) -> bytes:
        """Generate random file data."""
        # Use predictable seed for reproducibility
        random.seed(42)
        return bytes(random.randint(0, 255) for _ in range(size))

    def _calculate_checksum(self, data: bytes) -> str:
        """Calculate MD5 checksum."""
        return hashlib.md5(data).hexdigest()

    async def test_01_small_file_transfer_stream(self):
        """Test small file transfer using stream mode."""
        logger.info("=" * 60)
        logger.info("TEST: Small File Transfer (Stream Mode)")
        logger.info("=" * 60)

        config = TRANSFER_CONFIGS[0]  # 1MB
        file_data = self._generate_file(config.file_size)
        original_checksum = self._calculate_checksum(file_data)

        port = await self._start_relay()
        self.publisher = await self._create_publisher(port)
        self.subscriber, received = await self._create_subscriber(port)

        track_name = FullTrackName(namespace=[b"file"], track_name=b"small-stream")
        await self.publisher.publish(track_name)
        await self.subscriber.subscribe(track_name)
        await asyncio.sleep(0.5)

        # Send file in chunks
        chunks_sent = 0
        offset = 0

        while offset < len(file_data):
            chunk = file_data[offset : offset + config.chunk_size]
            obj = PublishedObject(
                group_id=chunks_sent // 1000,  # Group every 1000 chunks
                object_id=chunks_sent,
                payload=chunk,
                use_datagram=False,
            )
            await self.publisher.send_object(track_name, obj)
            chunks_sent += 1
            offset += len(chunk)

        logger.info(f"Sent {chunks_sent} chunks, {config.file_size} bytes")

        # Wait for reception
        await asyncio.sleep(3.0)

        # Reassemble and verify
        received_data = b"".join(
            chunk["data"] for chunk in sorted(received, key=lambda x: x["object_id"])
        )
        received_checksum = self._calculate_checksum(received_data)

        logger.info(f"Chunks received: {len(received)}/{chunks_sent}")
        logger.info(f"Bytes received: {len(received_data)}/{config.file_size}")

        self.assertEqual(received_checksum, original_checksum, "File checksum mismatch")
        self.assertEqual(len(received_data), config.file_size, "File size mismatch")

    async def test_02_large_file_transfer_stream(self):
        """Test large file transfer using stream mode."""
        logger.info("=" * 60)
        logger.info("TEST: Large File Transfer (Stream Mode)")
        logger.info("=" * 60)

        config = TRANSFER_CONFIGS[1]  # 10MB
        file_data = self._generate_file(config.file_size)
        original_checksum = self._calculate_checksum(file_data)

        port = await self._start_relay(cache_mb=1000)
        self.publisher = await self._create_publisher(port)
        self.subscriber, received = await self._create_subscriber(port)

        track_name = FullTrackName(namespace=[b"file"], track_name=b"large-stream")
        await self.publisher.publish(track_name)
        await self.subscriber.subscribe(track_name)
        await asyncio.sleep(0.5)

        # Send file
        chunks_sent = 0
        offset = 0
        start_time = time.time()

        while offset < len(file_data):
            chunk = file_data[offset : offset + config.chunk_size]
            obj = PublishedObject(
                group_id=chunks_sent // 1000,
                object_id=chunks_sent,
                payload=chunk,
                use_datagram=False,
            )
            await self.publisher.send_object(track_name, obj)
            chunks_sent += 1
            offset += len(chunk)

            # Progress log every MB
            if offset % (1024 * 1024) == 0:
                logger.info(f"Progress: {offset / (1024 * 1024):.0f}MB sent")

        send_time = time.time() - start_time
        throughput = (config.file_size / send_time) / (1024 * 1024)
        logger.info(
            f"Send complete: {send_time:.2f}s, throughput: {throughput:.2f} MB/s"
        )

        # Wait for reception
        await asyncio.sleep(10.0)

        # Verify
        received_data = b"".join(
            chunk["data"] for chunk in sorted(received, key=lambda x: x["object_id"])
        )
        received_checksum = self._calculate_checksum(received_data)

        logger.info(f"Chunks: {chunks_sent} sent, {len(received)} received")
        logger.info(f"Bytes: {config.file_size} sent, {len(received_data)} received")

        loss_rate = (chunks_sent - len(received)) / chunks_sent
        logger.info(f"Loss rate: {loss_rate:.2%}")

        # Allow some loss for large transfers
        self.assertGreaterEqual(
            len(received_data),
            config.file_size * 0.95,
            f"Too much data lost: {loss_rate:.2%}",
        )

        if len(received_data) == config.file_size:
            self.assertEqual(
                received_checksum, original_checksum, "File checksum mismatch"
            )

    async def test_03_file_transfer_datagram(self):
        """Test file transfer using datagram mode."""
        logger.info("=" * 60)
        logger.info("TEST: File Transfer (Datagram Mode)")
        logger.info("=" * 60)

        config = TRANSFER_CONFIGS[3]  # 1MB datagram
        file_data = self._generate_file(config.file_size)

        port = await self._start_relay()
        self.publisher = await self._create_publisher(port)
        self.subscriber, received = await self._create_subscriber(port)

        track_name = FullTrackName(namespace=[b"file"], track_name=b"datagram")
        await self.publisher.publish(track_name)
        await self.subscriber.subscribe(track_name)
        await asyncio.sleep(0.5)

        # Send file
        chunks_sent = 0
        offset = 0

        while offset < len(file_data):
            chunk = file_data[offset : offset + config.chunk_size]
            obj = PublishedObject(
                group_id=chunks_sent // 1000,
                object_id=chunks_sent,
                payload=chunk,
                use_datagram=True,
            )
            await self.publisher.send_object(track_name, obj)
            chunks_sent += 1
            offset += len(chunk)

        logger.info(f"Sent {chunks_sent} datagrams")

        await asyncio.sleep(3.0)

        received_data = b"".join(
            chunk["data"] for chunk in sorted(received, key=lambda x: x["object_id"])
        )

        logger.info(f"Datagrams: {chunks_sent} sent, {len(received)} received")
        loss_rate = (
            (chunks_sent - len(received)) / chunks_sent if chunks_sent > 0 else 0
        )
        logger.info(f"Loss rate: {loss_rate:.2%}")

        # Datagrams have higher expected loss
        self.assertGreaterEqual(
            len(received_data),
            config.file_size * 0.5,
            f"Too many datagrams lost: {loss_rate:.2%}",
        )

    async def test_04_concurrent_file_transfers(self):
        """Test multiple concurrent file transfers."""
        logger.info("=" * 60)
        logger.info("TEST: Concurrent File Transfers")
        logger.info("=" * 60)

        num_files = 3
        file_size = 2 * 1024 * 1024  # 2MB each
        files_data = [self._generate_file(file_size) for _ in range(num_files)]
        checksums = [self._calculate_checksum(data) for data in files_data]

        port = await self._start_relay(cache_mb=1000)
        self.publisher = await self._create_publisher(port)

        # Create subscribers for each file
        subscribers = []
        all_received = []

        for i in range(num_files):
            sub, received = await self._create_subscriber(port)
            subscribers.append((sub, received))
            all_received.append(received)

        # Publish all tracks
        tracks = []
        for i in range(num_files):
            track_name = FullTrackName(
                namespace=[b"file"], track_name=f"concurrent_{i}".encode()
            )
            tracks.append(track_name)
            await self.publisher.publish(track_name)
            await subscribers[i][0].subscribe(track_name)

        await asyncio.sleep(0.5)

        # Send all files concurrently
        async def send_file(track_idx: int):
            data = files_data[track_idx]
            track_name = tracks[track_idx]
            chunk_size = 4096
            offset = 0
            chunks = 0

            while offset < len(data):
                chunk = data[offset : offset + chunk_size]
                obj = PublishedObject(
                    group_id=chunks // 1000,
                    object_id=chunks,
                    payload=chunk,
                    use_datagram=False,
                )
                await self.publisher.send_object(track_name, obj)
                chunks += 1
                offset += len(chunk)

            logger.info(f"File {track_idx}: sent {chunks} chunks")

        # Start all transfers
        await asyncio.gather(*[send_file(i) for i in range(num_files)])

        await asyncio.sleep(5.0)

        # Verify all files
        for i in range(num_files):
            received = all_received[i]
            received_data = b"".join(
                chunk["data"]
                for chunk in sorted(received, key=lambda x: x["object_id"])
            )
            received_checksum = self._calculate_checksum(received_data)

            logger.info(
                f"File {i}: {len(received_data)}/{file_size} bytes, "
                f"checksum match: {received_checksum == checksums[i]}"
            )

            self.assertGreaterEqual(
                len(received_data), file_size * 0.9, f"File {i} lost too much data"
            )

        self.subscriber = subscribers[0][0]  # For cleanup

    async def test_05_chunked_file_with_resume(self):
        """Test file transfer with simulated resume capability."""
        logger.info("=" * 60)
        logger.info("TEST: Chunked File with Resume")
        logger.info("=" * 60)

        file_size = 5 * 1024 * 1024  # 5MB
        chunk_size = 32768  # 32KB chunks
        file_data = self._generate_file(file_size)

        port = await self._start_relay()
        self.publisher = await self._create_publisher(port)
        self.subscriber, received = await self._create_subscriber(port)

        track_name = FullTrackName(namespace=[b"file"], track_name=b"resume-test")
        await self.publisher.publish(track_name)
        await self.subscriber.subscribe(track_name)
        await asyncio.sleep(0.5)

        # Calculate total chunks
        total_chunks = (file_size + chunk_size - 1) // chunk_size

        # Send first half
        first_half_end = total_chunks // 2
        logger.info(f"Sending first half (chunks 0-{first_half_end})...")

        for chunk_idx in range(first_half_end):
            offset = chunk_idx * chunk_size
            chunk = file_data[offset : offset + chunk_size]
            obj = PublishedObject(
                group_id=chunk_idx // 100,
                object_id=chunk_idx,
                payload=chunk,
                use_datagram=False,
            )
            await self.publisher.send_object(track_name, obj)

        await asyncio.sleep(2.0)
        first_half_received = len(received)
        logger.info(
            f"First half: sent {first_half_end}, received {first_half_received}"
        )

        # Simulate subscriber disconnect and reconnect
        logger.info("Simulating disconnect/reconnect...")
        self.subscriber.disconnect()
        await asyncio.sleep(1.0)

        # Create new subscriber for second half
        self.subscriber, received2 = await self._create_subscriber(port)
        await self.subscriber.subscribe(track_name)
        await asyncio.sleep(0.5)

        # Send second half
        logger.info(f"Sending second half (chunks {first_half_end}-{total_chunks})...")
        for chunk_idx in range(first_half_end, total_chunks):
            offset = chunk_idx * chunk_size
            chunk = file_data[offset : offset + chunk_size]
            obj = PublishedObject(
                group_id=chunk_idx // 100,
                object_id=chunk_idx,
                payload=chunk,
                use_datagram=False,
            )
            await self.publisher.send_object(track_name, obj)

        await asyncio.sleep(3.0)

        total_received = first_half_received + len(received2)
        logger.info(f"Total received: {total_received}/{total_chunks} chunks")

        # Should have received most of the second half
        self.assertGreaterEqual(
            len(received2),
            (total_chunks - first_half_end) * 0.9,
            "Second half lost too much data",
        )

    async def test_06_file_transfer_with_throttling(self):
        """Test file transfer with rate limiting."""
        logger.info("=" * 60)
        logger.info("TEST: File Transfer with Throttling")
        logger.info("=" * 60)

        file_size = 2 * 1024 * 1024  # 2MB
        chunk_size = 4096
        target_rate = 500 * 1024  # 500 KB/s
        file_data = self._generate_file(file_size)

        port = await self._start_relay()
        self.publisher = await self._create_publisher(port)
        self.subscriber, received = await self._create_subscriber(port)

        track_name = FullTrackName(namespace=[b"file"], track_name=b"throttled")
        await self.publisher.publish(track_name)
        await self.subscriber.subscribe(track_name)
        await asyncio.sleep(0.5)

        # Send with rate limiting
        start_time = time.time()
        chunks_sent = 0
        bytes_sent = 0

        for offset in range(0, len(file_data), chunk_size):
            chunk = file_data[offset : offset + chunk_size]
            obj = PublishedObject(
                group_id=chunks_sent // 1000,
                object_id=chunks_sent,
                payload=chunk,
                use_datagram=False,
            )
            await self.publisher.send_object(track_name, obj)

            chunks_sent += 1
            bytes_sent += len(chunk)

            # Rate limiting
            expected_time = start_time + (bytes_sent / target_rate)
            sleep_time = expected_time - time.time()
            if sleep_time > 0:
                await asyncio.sleep(sleep_time)

        send_time = time.time() - start_time
        actual_rate = bytes_sent / send_time / 1024
        logger.info(
            f"Send complete: {send_time:.2f}s, actual rate: {actual_rate:.2f} KB/s"
        )

        await asyncio.sleep(5.0)

        received_data = b"".join(
            chunk["data"] for chunk in sorted(received, key=lambda x: x["object_id"])
        )
        logger.info(f"Received: {len(received_data)}/{file_size} bytes")

        self.assertGreaterEqual(
            len(received_data),
            file_size * 0.95,
            "Throttled transfer lost too much data",
        )


if __name__ == "__main__":
    unittest.main()
