#!/usr/bin/env python3
"""
MOQ视频传输使用示例

演示如何使用MOQVideoService进行视频传输。

运行方式:
    # 1. 先启动Relay
    python video_transfer_example.py relay

    # 2. 在另一个终端发布视频
    python video_transfer_example.py pub /path/to/video.mp4 my-video

    # 3. 在另一个终端订阅视频
    python video_transfer_example.py sub my-video /path/to/output.mp4
"""

import asyncio
import argparse
import logging
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from moq_video_service import (
    MOQVideoPublisher,
    MOQVideoSubscriber,
    MOQVideoRelay,
    MOQVideoServiceAPI,
    VideoTransferStats,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


async def run_relay(host: str = "127.0.0.1", port: int = 4433):
    """运行Relay服务"""
    relay = MOQVideoRelay(host=host, port=port)
    actual_port = await relay.start()

    logger.info(f"Relay running on {host}:{actual_port}")
    logger.info("Press Ctrl+C to stop")

    try:
        while True:
            await asyncio.sleep(1)
    except KeyboardInterrupt:
        logger.info("Stopping relay...")
    finally:
        await relay.stop()


async def run_publisher(
    video_path: str,
    track_name: str,
    relay_host: str = "127.0.0.1",
    relay_port: int = 4433,
):
    """运行Publisher示例"""
    logger.info(f"Connecting to relay at {relay_host}:{relay_port}")

    pub = MOQVideoPublisher(relay_host, relay_port)

    if not await pub.connect():
        logger.error("Failed to connect to relay")
        return

    logger.info(f"Connected to relay, publishing video: {video_path}")

    # 设置进度回调
    def on_progress(stats: VideoTransferStats):
        logger.info(
            f"Progress: {stats.chunks_sent} chunks sent, "
            f"{stats.bytes_sent} bytes, "
            f"{stats.throughput_mbps:.2f} Mbps"
        )

    pub.set_progress_callback(on_progress)

    try:
        stats = await pub.publish_video(video_path, track_name)

        logger.info("=" * 60)
        logger.info("Publish completed!")
        logger.info(f"Track: {stats.track_name}")
        logger.info(f"Chunks: {stats.chunks_sent}")
        logger.info(f"Bytes: {stats.bytes_sent}")
        logger.info(f"Duration: {stats.duration:.3f}s")
        logger.info(f"Throughput: {stats.throughput_mbps:.2f} Mbps")
        logger.info(f"Hash: {stats.original_hash}")
        logger.info("=" * 60)

    except Exception as e:
        logger.error(f"Error publishing video: {e}")
    finally:
        pub.disconnect()


async def run_subscriber(
    track_name: str,
    output_path: str,
    relay_host: str = "127.0.0.1",
    relay_port: int = 4433,
    wait_time: float = 15.0,
):
    """运行Subscriber示例"""
    logger.info(f"Connecting to relay at {relay_host}:{relay_port}")

    sub = MOQVideoSubscriber(relay_host, relay_port)

    if not await sub.connect():
        logger.error("Failed to connect to relay")
        return

    logger.info(f"Connected to relay, subscribing to: {track_name}")

    # 设置进度回调
    def on_progress(stats: VideoTransferStats):
        logger.info(
            f"Progress: {stats.chunks_received} chunks received, "
            f"{stats.bytes_received} bytes, "
            f"{stats.throughput_mbps:.2f} Mbps"
        )

    sub.set_progress_callback(on_progress)

    try:
        stats = await sub.subscribe(track_name, output_path, wait_time=wait_time)

        logger.info("=" * 60)
        logger.info("Subscribe completed!")
        logger.info(f"Track: {stats.track_name}")
        logger.info(f"Status: {stats.status.value}")
        logger.info(f"Chunks: {stats.chunks_received}/{stats.chunks_sent}")
        logger.info(f"Bytes: {stats.bytes_received}")
        logger.info(f"Duration: {stats.duration:.3f}s")
        logger.info(f"Throughput: {stats.throughput_mbps:.2f} Mbps")
        logger.info(f"Loss rate: {stats.loss_rate * 100:.2f}%")
        logger.info(f"Original hash: {stats.original_hash}")
        logger.info(f"Received hash: {stats.received_hash}")
        logger.info(
            f"Hash match: {stats.original_hash == stats.received_hash if stats.original_hash and stats.received_hash else False}"
        )
        logger.info(f"Output: {output_path}")
        logger.info("=" * 60)

    except Exception as e:
        logger.error(f"Error subscribing to video: {e}")
    finally:
        sub.disconnect()


async def run_auto_test(duration: int = 5, resolution: str = "1080p"):
    """
    自动运行完整测试（生成视频、发布、订阅、验证）

    Args:
        duration: 视频时长（秒）
        resolution: 分辨率 (720p, 1080p, 4k)
    """
    import subprocess
    import tempfile
    import hashlib

    # 解析分辨率
    res_map = {
        "720p": (1280, 720),
        "1080p": (1920, 1080),
        "4k": (3840, 2160),
    }
    width, height = res_map.get(resolution, (1920, 1080))

    logger.info("=" * 60)
    logger.info(f"Auto Test: {resolution} ({width}x{height}), {duration}s")
    logger.info("=" * 60)

    # 1. 启动Relay
    relay = MOQVideoRelay(host="127.0.0.1", port=0)
    relay_port = await relay.start()
    logger.info(f"Relay started on port {relay_port}")

    # 2. 生成测试视频
    with tempfile.NamedTemporaryFile(suffix=".mp4", delete=False) as f:
        video_path = f.name

    logger.info(f"Generating test video...")
    cmd = [
        "ffmpeg",
        "-y",
        "-f",
        "lavfi",
        "-i",
        f"testsrc=duration={duration}:size={width}x{height}:rate=30",
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
        "4M",
        "-c:a",
        "aac",
        "-b:a",
        "128k",
        video_path,
    ]

    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        logger.error(f"Failed to generate video: {result.stderr}")
        await relay.stop()
        return

    video_size = Path(video_path).stat().st_size
    logger.info(f"Video generated: {video_path} ({video_size} bytes)")

    # 计算原始hash
    with open(video_path, "rb") as f:
        original_hash = hashlib.sha256(f.read()).hexdigest()
    logger.info(f"Original hash: {original_hash}")

    # 3. 发布视频
    with tempfile.NamedTemporaryFile(suffix=".mp4", delete=False) as f:
        output_path = f.name

    track_name = f"test-video-{resolution}"

    pub = MOQVideoPublisher("127.0.0.1", relay_port)
    sub = MOQVideoSubscriber("127.0.0.1", relay_port)

    # 先启动订阅（非阻塞）
    sub_stats_result = None

    async def subscribe_task():
        nonlocal sub_stats_result
        if not await sub.connect():
            logger.error("Subscriber failed to connect")
            return None
        try:
            # 等待更长时间以确保接收所有数据
            sub_stats_result = await sub.subscribe(
                track_name, output_path, wait_time=duration + 5
            )
            return sub_stats_result
        finally:
            sub.disconnect()

    # 启动订阅任务
    sub_task = asyncio.create_task(subscribe_task())

    # 等待订阅建立
    await asyncio.sleep(0.5)

    # 发布视频
    if not await pub.connect():
        logger.error("Publisher failed to connect")
        await relay.stop()
        return

    try:
        pub_stats = await pub.publish_video(video_path, track_name)
        logger.info(f"Published: {pub_stats.chunks_sent} chunks")
    finally:
        pub.disconnect()

    # 等待订阅完成
    sub_stats = await sub_task

    if sub_stats:
        logger.info("=" * 60)
        logger.info("Test Results:")
        logger.info("=" * 60)
        logger.info(f"Resolution: {resolution} ({width}x{height})")
        logger.info(f"Duration: {duration}s")
        logger.info(f"Video size: {video_size} bytes")
        logger.info(f"Chunks sent: {pub_stats.chunks_sent}")
        logger.info(f"Chunks received: {sub_stats.chunks_received}")
        logger.info(f"Loss rate: {sub_stats.loss_rate * 100:.2f}%")
        logger.info(f"Transfer time: {sub_stats.duration:.3f}s")
        logger.info(f"Throughput: {sub_stats.throughput_mbps:.2f} Mbps")
        logger.info(f"Original hash: {original_hash}")
        logger.info(f"Received hash: {sub_stats.received_hash}")
        logger.info(f"Hash match: {original_hash == sub_stats.received_hash}")

        if original_hash == sub_stats.received_hash:
            logger.info("✅ TEST PASSED: Video transmitted successfully!")
        else:
            logger.error("❌ TEST FAILED: Hash mismatch!")
        logger.info("=" * 60)

    # 清理
    await relay.stop()
    Path(video_path).unlink(missing_ok=True)
    Path(output_path).unlink(missing_ok=True)


def main():
    parser = argparse.ArgumentParser(description="MOQ Video Transfer Example")
    parser.add_argument(
        "mode",
        choices=["relay", "pub", "sub", "test"],
        help="运行模式: relay (启动中继), pub (发布), sub (订阅), test (自动测试)",
    )
    parser.add_argument("--host", default="127.0.0.1", help="Relay主机地址")
    parser.add_argument("--port", type=int, default=4433, help="Relay端口")
    parser.add_argument("--video", help="视频文件路径 (pub模式)")
    parser.add_argument("--track", help="轨道名称 (pub/sub模式)")
    parser.add_argument("--output", help="输出文件路径 (sub模式)")
    parser.add_argument("--wait", type=float, default=15.0, help="订阅等待时间")
    parser.add_argument("--duration", type=int, default=5, help="测试视频时长")
    parser.add_argument(
        "--resolution",
        default="1080p",
        choices=["720p", "1080p", "4k"],
        help="测试分辨率",
    )

    args = parser.parse_args()

    if args.mode == "relay":
        asyncio.run(run_relay(args.host, args.port))
    elif args.mode == "pub":
        if not args.video or not args.track:
            print("Error: --video and --track required for pub mode")
            sys.exit(1)
        asyncio.run(run_publisher(args.video, args.track, args.host, args.port))
    elif args.mode == "sub":
        if not args.track or not args.output:
            print("Error: --track and --output required for sub mode")
            sys.exit(1)
        asyncio.run(
            run_subscriber(args.track, args.output, args.host, args.port, args.wait)
        )
    elif args.mode == "test":
        asyncio.run(run_auto_test(args.duration, args.resolution))


if __name__ == "__main__":
    main()
