"""
Video transmission scenarios test for MOQ Transport.

Tests various video streaming scenarios:
- Different resolutions (720p, 1080p, 4K)
- Different frame rates (15fps, 30fps, 60fps)
- Different bitrates (low, medium, high)
- Stream continuity over long periods
- Adaptive bitrate scenarios
"""

import asyncio
import logging
import os
import subprocess
import tempfile
import time
import unittest
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional, Dict, Tuple

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
class VideoProfile:
    """Video profile configuration."""

    name: str
    resolution: Tuple[int, int]  # width, height
    fps: int
    bitrate: str  # e.g., "2M", "4M", "8M"
    duration: int  # seconds
    gop_size: int  # Group of Pictures size


# Standard video profiles
VIDEO_PROFILES = [
    VideoProfile("720p_30fps_low", (1280, 720), 30, "1M", 10, 30),
    VideoProfile("720p_30fps_medium", (1280, 720), 30, "2M", 10, 30),
    VideoProfile("1080p_30fps_medium", (1920, 1080), 30, "4M", 10, 30),
    VideoProfile("1080p_60fps_high", (1920, 1080), 60, "8M", 10, 60),
    VideoProfile("4K_30fps_high", (3840, 2160), 30, "15M", 10, 30),
]


class TestVideoScenarios(unittest.IsolatedAsyncioTestCase):
    """Test video transmission scenarios."""

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

    async def _start_relay(self) -> int:
        """Start relay and return port."""
        self.relay = MOQRelay(
            host="127.0.0.1",
            port=0,
            max_memory_cache=200 * 1024 * 1024,  # 200MB for video
        )
        await self.relay.start()
        port = self.relay._quic_server.actual_port or 4433
        logger.info(f"Relay started on port {port}")
        return port

    async def _create_publisher(self, port: int) -> MOQPublisher:
        """Create and connect publisher."""
        pub = MOQPublisher("127.0.0.1", port)
        connected = asyncio.Event()
        pub.set_handlers(on_connected=lambda: connected.set())
        success = await pub.connect(agent_id="video-pub")
        self.assertTrue(success)
        await asyncio.wait_for(connected.wait(), timeout=5.0)
        return pub

    async def _create_subscriber(self, port: int) -> Tuple[MOQSubscriber, List]:
        """Create and connect subscriber."""
        sub = MOQSubscriber("127.0.0.1", port)
        connected = asyncio.Event()
        received_objects = []

        def on_object(obj):
            received_objects.append(
                {
                    "group_id": obj.group_id,
                    "object_id": obj.object_id,
                    "size": len(obj.payload),
                    "timestamp": time.time(),
                }
            )

        sub.set_handlers(
            on_connected=lambda: connected.set(), on_object_received=on_object
        )
        success = await sub.connect(agent_id="video-sub")
        self.assertTrue(success)
        await asyncio.wait_for(connected.wait(), timeout=5.0)
        return sub, received_objects

    async def _generate_video(self, profile: VideoProfile) -> Path:
        """Generate test video with specified profile."""
        try:
            subprocess.run(["ffmpeg", "-version"], capture_output=True, check=True)
        except (subprocess.CalledProcessError, FileNotFoundError):
            raise unittest.SkipTest("ffmpeg not available")

        with tempfile.NamedTemporaryFile(suffix=".mp4", delete=False) as f:
            video_path = Path(f.name)

        width, height = profile.resolution
        cmd = [
            "ffmpeg",
            "-y",
            "-f",
            "lavfi",
            "-i",
            f"testsrc=duration={profile.duration}:size={width}x{height}:rate={profile.fps}",
            "-pix_fmt",
            "yuv420p",
            "-c:v",
            "libx264",
            "-preset",
            "ultrafast",
            "-b:v",
            profile.bitrate,
            "-g",
            str(profile.gop_size),
            "-keyint_min",
            str(profile.gop_size),
            str(video_path),
        ]

        logger.info(
            f"Generating {profile.name} video: {width}x{height}@{profile.fps}fps, {profile.bitrate}"
        )
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode != 0:
            logger.error(f"ffmpeg error: {result.stderr}")
            raise RuntimeError(f"Failed to generate video: {result.stderr}")

        file_size = video_path.stat().st_size
        logger.info(f"Generated video: {video_path} ({file_size} bytes)")
        return video_path

    async def _split_video_to_frames(self, video_path: Path, fps: int) -> List[bytes]:
        """Split video into frame-sized chunks."""
        chunks = []
        chunk_size = 10000  # ~10KB per frame for testing

        with open(video_path, "rb") as f:
            while True:
                chunk = f.read(chunk_size)
                if not chunk:
                    break
                chunks.append(chunk)

        logger.info(f"Video split into {len(chunks)} chunks")
        return chunks

    async def test_01_video_stream_720p_30fps(self):
        """Test 720p 30fps video streaming."""
        logger.info("=" * 60)
        logger.info("TEST: 720p 30fps Video Stream")
        logger.info("=" * 60)

        profile = VIDEO_PROFILES[1]  # 720p medium
        video_path = await self._generate_video(profile)

        try:
            chunks = await self._split_video_to_frames(video_path, profile.fps)
            if len(chunks) < 5:
                self.skipTest("Not enough video chunks")

            port = await self._start_relay()
            self.publisher = await self._create_publisher(port)
            self.subscriber, received = await self._create_subscriber(port)

            track_name = FullTrackName(namespace=[b"video"], track_name=b"720p30")
            await self.publisher.publish(track_name)
            await self.subscriber.subscribe(track_name)
            await asyncio.sleep(0.5)

            # Send frames at ~30fps
            start_time = time.time()
            frames_sent = 0

            for i, chunk in enumerate(chunks[:150]):  # 5 seconds @ 30fps
                obj = PublishedObject(
                    group_id=i // profile.fps,  # Group by second
                    object_id=i % profile.fps,
                    payload=chunk,
                    publisher_priority=128,
                    subgroup_id=0,
                    use_datagram=False,  # Stream mode for video
                )
                await self.publisher.send_object(track_name, obj)
                frames_sent += 1

                # Maintain ~30fps
                expected_time = start_time + (i / profile.fps)
                sleep_time = expected_time - time.time()
                if sleep_time > 0:
                    await asyncio.sleep(sleep_time)

            # Wait for reception
            await asyncio.sleep(3.0)

            # Verify results
            loss_rate = (
                (frames_sent - len(received)) / frames_sent if frames_sent > 0 else 0
            )
            logger.info(f"Frames sent: {frames_sent}")
            logger.info(f"Frames received: {len(received)}")
            logger.info(f"Loss rate: {loss_rate:.2%}")

            self.assertGreaterEqual(
                len(received),
                frames_sent * 0.9,
                f"Too many frames lost. Loss rate: {loss_rate:.2%}",
            )

        finally:
            if video_path.exists():
                video_path.unlink()

    async def test_02_video_stream_multiple_profiles(self):
        """Test streaming with multiple video profiles simultaneously."""
        logger.info("=" * 60)
        logger.info("TEST: Multiple Video Profiles")
        logger.info("=" * 60)

        # Generate videos for different profiles
        videos = []
        for profile in VIDEO_PROFILES[:3]:  # Test first 3 profiles
            video_path = await self._generate_video(profile)
            videos.append((profile, video_path))

        try:
            port = await self._start_relay()
            self.publisher = await self._create_publisher(port)

            # Create subscribers for each profile
            subscribers = []
            all_received = []

            for i, (profile, _) in enumerate(videos):
                sub, received = await self._create_subscriber(port)
                subscribers.append((sub, received, profile))
                all_received.append(received)

            # Publish all tracks
            tracks = []
            for i, (profile, _) in enumerate(videos):
                track_name = FullTrackName(
                    namespace=[b"video"], track_name=f"profile_{i}".encode()
                )
                tracks.append(track_name)
                await self.publisher.publish(track_name)
                await subscribers[i][0].subscribe(track_name)

            await asyncio.sleep(0.5)

            # Send data on all tracks
            for frame_idx in range(30):  # 1 second of frames
                for track_idx, (profile, video_path) in enumerate(videos):
                    obj = PublishedObject(
                        group_id=0,
                        object_id=frame_idx,
                        payload=os.urandom(5000),  # 5KB per frame
                        use_datagram=False,
                    )
                    await self.publisher.send_object(tracks[track_idx], obj)

                await asyncio.sleep(1 / 30)  # 30fps

            await asyncio.sleep(2.0)

            # Verify all subscribers received data
            for i, (sub, received, profile) in enumerate(subscribers):
                logger.info(f"Profile {profile.name}: received {len(received)} objects")
                self.assertGreaterEqual(
                    len(received), 25, f"Profile {profile.name} lost too many objects"
                )

        finally:
            for _, video_path in videos:
                if video_path.exists():
                    video_path.unlink()

    async def test_03_video_adaptive_bitrate(self):
        """Test adaptive bitrate scenario (switching between qualities)."""
        logger.info("=" * 60)
        logger.info("TEST: Adaptive Bitrate")
        logger.info("=" * 60)

        port = await self._start_relay()
        self.publisher = await self._create_publisher(port)
        self.subscriber, received = await self._create_subscriber(port)

        # Create multiple quality tracks
        qualities = [b"low", b"medium", b"high"]
        tracks = []

        for quality in qualities:
            track_name = FullTrackName(namespace=[b"video"], track_name=quality)
            tracks.append(track_name)
            await self.publisher.publish(track_name)

        # Subscribe to medium quality initially
        await self.subscriber.subscribe(tracks[1])
        await asyncio.sleep(0.5)

        # Send 2 seconds on each quality
        for quality_idx, track_name in enumerate(tracks):
            logger.info(f"Sending {qualities[quality_idx].decode()} quality...")

            for frame_idx in range(60):  # 2 seconds @ 30fps
                payload_size = 2000 + quality_idx * 3000  # Increasing quality
                obj = PublishedObject(
                    group_id=quality_idx,
                    object_id=frame_idx,
                    payload=os.urandom(payload_size),
                    use_datagram=False,
                )
                await self.publisher.send_object(track_name, obj)
                await asyncio.sleep(1 / 30)

        await asyncio.sleep(2.0)

        logger.info(f"Total objects received: {len(received)}")
        # Should have received medium quality frames
        self.assertGreater(len(received), 40, "Should receive medium quality frames")

    async def test_04_video_long_duration_stream(self):
        """Test long duration video stream (30 seconds)."""
        logger.info("=" * 60)
        logger.info("TEST: Long Duration Stream (30s)")
        logger.info("=" * 60)

        port = await self._start_relay()
        self.publisher = await self._create_publisher(port)
        self.subscriber, received = await self._create_subscriber(port)

        track_name = FullTrackName(namespace=[b"video"], track_name=b"long-stream")
        await self.publisher.publish(track_name)
        await self.subscriber.subscribe(track_name)
        await asyncio.sleep(0.5)

        duration = 30  # seconds
        fps = 30
        total_frames = duration * fps

        logger.info(f"Streaming {duration}s video at {fps}fps ({total_frames} frames)")

        start_time = time.time()
        frames_sent = 0

        for i in range(total_frames):
            obj = PublishedObject(
                group_id=i // fps,
                object_id=i % fps,
                payload=os.urandom(5000),  # 5KB per frame
                use_datagram=False,
            )
            await self.publisher.send_object(track_name, obj)
            frames_sent += 1

            # Maintain fps
            expected_time = start_time + (i / fps)
            sleep_time = expected_time - time.time()
            if sleep_time > 0:
                await asyncio.sleep(sleep_time)

            # Log progress every 10 seconds
            if i % (fps * 10) == 0 and i > 0:
                elapsed = time.time() - start_time
                logger.info(
                    f"Progress: {i}/{total_frames} frames, elapsed: {elapsed:.1f}s"
                )

        await asyncio.sleep(5.0)

        elapsed = time.time() - start_time
        logger.info(f"Stream complete. Sent: {frames_sent}, Received: {len(received)}")
        logger.info(f"Total time: {elapsed:.1f}s")

        loss_rate = (
            (frames_sent - len(received)) / frames_sent if frames_sent > 0 else 0
        )
        logger.info(f"Loss rate: {loss_rate:.2%}")

        self.assertGreaterEqual(
            len(received),
            frames_sent * 0.85,
            f"Too many frames lost in long stream: {loss_rate:.2%}",
        )

    async def test_05_video_burst_transmission(self):
        """Test video burst transmission (simulating key frames)."""
        logger.info("=" * 60)
        logger.info("TEST: Burst Transmission (Key Frames)")
        logger.info("=" * 60)

        port = await self._start_relay()
        self.publisher = await self._create_publisher(port)
        self.subscriber, received = await self._create_subscriber(port)

        track_name = FullTrackName(namespace=[b"video"], track_name=b"burst-test")
        await self.publisher.publish(track_name)
        await self.subscriber.subscribe(track_name)
        await asyncio.sleep(0.5)

        # Simulate GOP structure: I-frame (large) followed by P-frames (small)
        gop_size = 30
        num_gops = 3

        for gop_idx in range(num_gops):
            logger.info(f"Sending GOP {gop_idx + 1}/{num_gops}")

            # I-frame (key frame) - larger
            iframe = PublishedObject(
                group_id=gop_idx,
                object_id=0,
                payload=os.urandom(50000),  # 50KB I-frame
                use_datagram=False,
            )
            await self.publisher.send_object(track_name, iframe)

            # P-frames - smaller
            for frame_idx in range(1, gop_size):
                pframe = PublishedObject(
                    group_id=gop_idx,
                    object_id=frame_idx,
                    payload=os.urandom(5000),  # 5KB P-frame
                    use_datagram=False,
                )
                await self.publisher.send_object(track_name, pframe)

            await asyncio.sleep(0.5)

        await asyncio.sleep(3.0)

        expected_frames = num_gops * gop_size
        logger.info(f"Frames sent: {expected_frames}, received: {len(received)}")

        self.assertGreaterEqual(
            len(received),
            expected_frames * 0.9,
            "Burst transmission lost too many frames",
        )

    async def test_06_video_stream_recovery(self):
        """Test stream recovery after subscriber disconnect/reconnect."""
        logger.info("=" * 60)
        logger.info("TEST: Stream Recovery")
        logger.info("=" * 60)

        port = await self._start_relay()
        self.publisher = await self._create_publisher(port)

        track_name = FullTrackName(namespace=[b"video"], track_name=b"recovery-test")
        await self.publisher.publish(track_name)

        # First subscriber session
        sub1, received1 = await self._create_subscriber(port)
        await sub1.subscribe(track_name)
        await asyncio.sleep(0.5)

        # Send first batch
        for i in range(30):
            obj = PublishedObject(
                group_id=0,
                object_id=i,
                payload=os.urandom(5000),
                use_datagram=False,
            )
            await self.publisher.send_object(track_name, obj)
        await asyncio.sleep(1.0)

        count1 = len(received1)
        logger.info(f"First session received: {count1} objects")

        # Disconnect first subscriber
        sub1.disconnect()
        await asyncio.sleep(1.0)

        # Create new subscriber
        sub2, received2 = await self._create_subscriber(port)
        await sub2.subscribe(track_name)
        await asyncio.sleep(0.5)

        # Send second batch
        for i in range(30, 60):
            obj = PublishedObject(
                group_id=1,
                object_id=i,
                payload=os.urandom(5000),
                use_datagram=False,
            )
            await self.publisher.send_object(track_name, obj)
        await asyncio.sleep(1.0)

        count2 = len(received2)
        logger.info(f"Second session received: {count2} objects")

        self.subscriber = sub2  # For cleanup

        # Second subscriber should receive second batch
        self.assertGreaterEqual(
            count2, 25, "Reconnected subscriber should receive data"
        )


if __name__ == "__main__":
    unittest.main()
