#!/usr/bin/env python3
"""
MOQ视频传输简单集成测试

演示如何使用MOQVideoService进行视频传输。
"""

import asyncio
import hashlib
import logging
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from moq_video_service import MOQVideoPublisher, MOQVideoSubscriber, MOQVideoRelay

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


async def create_test_video(path: Path, duration: int = 3):
    """创建测试视频"""
    import subprocess

    cmd = [
        "ffmpeg",
        "-y",
        "-f",
        "lavfi",
        "-i",
        f"testsrc=duration={duration}:size=1280x720:rate=30",
        "-f",
        "lavfi",
        "-i",
        f"sine=frequency=1000:duration={duration}",
        "-pix_fmt",
        "yuv420p",
        "-c:v",
        "libx264",
        "-preset",
        "ultrafast",
        "-b:v",
        "2M",
        "-c:a",
        "aac",
        "-b:a",
        "128k",
        str(path),
    ]

    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        raise RuntimeError(f"Failed to create video: {result.stderr}")

    return path.stat().st_size


async def test_video_transfer():
    """测试视频传输"""
    logger.info("=" * 60)
    logger.info("MOQ Video Transfer Integration Test")
    logger.info("=" * 60)

    # 1. 启动Relay
    logger.info("\n[1/5] Starting MOQ Relay...")
    relay = MOQVideoRelay(host="127.0.0.1", port=0)
    relay_port = await relay.start()
    logger.info(f"Relay started on port {relay_port}")

    # 2. 创建测试视频
    logger.info("\n[2/5] Creating test video...")
    with tempfile.NamedTemporaryFile(suffix=".mp4", delete=False) as f:
        video_path = Path(f.name)

    video_size = await create_test_video(video_path, duration=3)
    logger.info(f"Video created: {video_path} ({video_size} bytes)")

    # 计算原始hash
    with open(video_path, "rb") as f:
        original_data = f.read()
    original_hash = hashlib.sha256(original_data).hexdigest()
    logger.info(f"Original hash: {original_hash}")

    # 3 & 4. 同时启动Publisher和Subscriber进行传输
    logger.info("\n[3&4] Publishing and subscribing video...")

    with tempfile.NamedTemporaryFile(suffix=".mp4", delete=False) as f:
        output_path = Path(f.name)

    pub = MOQVideoPublisher("127.0.0.1", relay_port)
    sub = MOQVideoSubscriber("127.0.0.1", relay_port)

    # 先连接Subscriber
    if not await sub.connect():
        logger.error("Failed to connect subscriber")
        await relay.stop()
        return False

    # 再连接Publisher
    if not await pub.connect():
        logger.error("Failed to connect publisher")
        sub.disconnect()
        await relay.stop()
        return False

    try:
        # 同时发布和订阅
        sub_stats = None

        async def subscribe_task():
            nonlocal sub_stats
            sub_stats = await sub.subscribe(
                "test-stream", str(output_path), wait_time=10.0
            )

        # 启动订阅任务
        sub_task = asyncio.create_task(subscribe_task())

        # 等待订阅建立
        await asyncio.sleep(0.5)

        # 发布视频
        pub_stats = await pub.publish_video(video_path, "test-stream")
        logger.info(
            f"Published: {pub_stats.chunks_sent} chunks, {pub_stats.bytes_sent} bytes"
        )

        # 等待订阅完成
        await sub_task

        if sub_stats:
            logger.info(
                f"Received: {sub_stats.chunks_received} chunks, {sub_stats.bytes_received} bytes"
            )
            logger.info(f"Transfer time: {sub_stats.duration:.3f}s")
            logger.info(f"Throughput: {sub_stats.throughput_mbps:.2f} Mbps")

    finally:
        pub.disconnect()
        sub.disconnect()

    # 5. 验证结果
    logger.info("\n[5/5] Verifying results...")

    if output_path.exists():
        with open(output_path, "rb") as f:
            received_data = f.read()
        received_hash = hashlib.sha256(received_data).hexdigest()

        logger.info(f"Received hash: {received_hash}")
        logger.info(f"Hash match: {original_hash == received_hash}")

        if original_hash == received_hash:
            logger.info("\n" + "=" * 60)
            logger.info("✅ TEST PASSED: Video transmitted successfully!")
            logger.info("=" * 60)
            success = True
        else:
            logger.error("\n" + "=" * 60)
            logger.error("❌ TEST FAILED: Hash mismatch!")
            logger.error(f"Original: {original_hash}")
            logger.error(f"Received: {received_hash}")
            logger.info("=" * 60)
            success = False
    else:
        logger.error("Output file not created!")
        success = False

    # 清理
    await relay.stop()
    video_path.unlink(missing_ok=True)
    output_path.unlink(missing_ok=True)

    return success


async def main():
    """主函数"""
    try:
        success = await test_video_transfer()
        sys.exit(0 if success else 1)
    except Exception as e:
        logger.error(f"Test failed with error: {e}")
        import traceback

        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
