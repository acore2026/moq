#!/usr/bin/env python3
"""
真实视频传输稳定性测试

使用ffmpeg生成本地测试视频，通过MOQ的pub-relay-sub架构传输，
对比原始视频和接收后视频的hash值以验证传输稳定性。

流程:
1. 使用ffmpeg生成测试视频(testsrc)
2. 将视频分块并通过MOQ publisher发送
3. 通过relay转发
4. 通过subscriber接收并重组视频
5. 对比原始视频和接收视频的hash值
"""

import asyncio
import hashlib
import logging
import os
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from typing import List, Optional, Tuple

# 添加项目路径
sys.path.insert(0, str(Path(__file__).parent.parent))
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from moq import MOQPublisher, MOQSubscriber, MOQRelay
from moq.encoding import FullTrackName
from moq.pub import PublishedObject
from moq.sub import ReceivedObject

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)

# 测试配置 - 1080p 30fps
TEST_CONFIG = {
    "relay_host": "127.0.0.1",
    "relay_port": 0,  # 0表示自动分配
    "video_duration": 10,  # 视频时长(秒)
    "video_resolution": (1920, 1080),  # 1080p分辨率
    "video_fps": 30,  # 帧率
    "video_bitrate": "4M",  # 1080p推荐码率
    "chunk_size": 16384,  # 增大chunk大小到16KB以适应更大的视频
}


class VideoTransferTest:
    """真实视频传输测试类"""

    def __init__(self):
        self.relay: Optional[MOQRelay] = None
        self.publisher: Optional[MOQPublisher] = None
        self.subscriber: Optional[MOQSubscriber] = None
        self.video_path: Optional[Path] = None
        self.received_video_path: Optional[Path] = None
        self.relay_port: int = 0
        self.received_objects: List[bytes] = []
        self.transmission_stats = {
            "chunks_sent": 0,
            "chunks_received": 0,
            "bytes_sent": 0,
            "bytes_received": 0,
            "start_time": 0,
            "end_time": 0,
        }

    def generate_test_video(self, output_path: Path) -> bool:
        """
        使用ffmpeg生成测试视频

        Args:
            output_path: 输出视频文件路径

        Returns:
            bool: 是否成功生成视频
        """
        width, height = TEST_CONFIG["video_resolution"]
        duration = TEST_CONFIG["video_duration"]
        fps = TEST_CONFIG["video_fps"]
        bitrate = TEST_CONFIG["video_bitrate"]

        # 使用testsrc生成测试视频，包含时间戳和移动图形
        cmd = [
            "ffmpeg",
            "-y",  # 覆盖已存在的文件
            "-f",
            "lavfi",
            "-i",
            f"testsrc=duration={duration}:size={width}x{height}:rate={fps}",
            "-f",
            "lavfi",
            "-i",
            f"sine=frequency=1000:duration={duration}",  # 添加音频
            "-pix_fmt",
            "yuv420p",
            "-c:v",
            "libx264",
            "-preset",
            "ultrafast",
            "-b:v",
            bitrate,
            "-c:a",
            "aac",
            "-b:a",
            "128k",
            "-movflags",
            "+faststart",
            str(output_path),
        ]

        logger.info(
            f"生成测试视频: {width}x{height}@{fps}fps, {duration}秒, 码率{bitrate}"
        )
        logger.info(f"FFmpeg命令: {' '.join(cmd)}")

        try:
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=60)

            if result.returncode != 0:
                logger.error(f"FFmpeg错误: {result.stderr}")
                return False

            file_size = output_path.stat().st_size
            logger.info(f"视频生成成功: {output_path} ({file_size} bytes)")
            return True

        except subprocess.TimeoutExpired:
            logger.error("FFmpeg执行超时")
            return False
        except Exception as e:
            logger.error(f"生成视频时发生错误: {e}")
            return False

    def split_video_to_chunks(self, video_path: Path, chunk_size: int) -> List[bytes]:
        """
        将视频文件分割成多个chunk

        Args:
            video_path: 视频文件路径
            chunk_size: 每个chunk的大小

        Returns:
            List[bytes]: 视频数据块列表
        """
        chunks = []

        with open(video_path, "rb") as f:
            while True:
                chunk = f.read(chunk_size)
                if not chunk:
                    break
                chunks.append(chunk)

        logger.info(
            f"视频分割完成: {len(chunks)} 个chunks, 每个chunk最大 {chunk_size} bytes"
        )
        return chunks

    def calculate_file_hash(self, file_path: Path) -> str:
        """
        计算文件的SHA256哈希值

        Args:
            file_path: 文件路径

        Returns:
            str: SHA256哈希值(十六进制)
        """
        hash_obj = hashlib.sha256()

        with open(file_path, "rb") as f:
            while True:
                data = f.read(65536)  # 64KB chunks
                if not data:
                    break
                hash_obj.update(data)

        return hash_obj.hexdigest()

    def calculate_data_hash(self, data: bytes) -> str:
        """
        计算字节数据的SHA256哈希值

        Args:
            data: 字节数据

        Returns:
            str: SHA256哈希值(十六进制)
        """
        return hashlib.sha256(data).hexdigest()

    async def start_relay(self) -> bool:
        """启动MOQ relay"""
        try:
            self.relay = MOQRelay(
                host=TEST_CONFIG["relay_host"],
                port=TEST_CONFIG["relay_port"],
                max_memory_cache=200 * 1024 * 1024,  # 200MB
            )
            await self.relay.start()
            self.relay_port = self.relay._quic_server.actual_port or 4433
            logger.info(f"Relay启动成功: 127.0.0.1:{self.relay_port}")
            return True
        except Exception as e:
            logger.error(f"启动Relay失败: {e}")
            return False

    async def setup_publisher(self) -> bool:
        """设置并连接publisher"""
        try:
            self.publisher = MOQPublisher(TEST_CONFIG["relay_host"], self.relay_port)

            connected = asyncio.Event()
            self.publisher.set_handlers(on_connected=lambda: connected.set())

            success = await self.publisher.connect(agent_id="video-publisher")
            if not success:
                logger.error("Publisher连接失败")
                return False

            await asyncio.wait_for(connected.wait(), timeout=5.0)
            logger.info("Publisher连接成功")
            return True
        except Exception as e:
            logger.error(f"设置Publisher失败: {e}")
            return False

    async def setup_subscriber(self) -> bool:
        """设置并连接subscriber"""
        try:
            self.subscriber = MOQSubscriber(TEST_CONFIG["relay_host"], self.relay_port)

            connected = asyncio.Event()
            self.received_objects = []

            def on_object(obj: ReceivedObject):
                """接收object的回调函数"""
                # 存储为tuple以便后续排序 (group_id, object_id, payload)
                self.received_objects.append((obj.group_id, obj.object_id, obj.payload))
                self.transmission_stats["chunks_received"] += 1
                self.transmission_stats["bytes_received"] += len(obj.payload)
                logger.debug(
                    f"Received object: group={obj.group_id}, object={obj.object_id}, size={len(obj.payload)}"
                )

            self.subscriber.set_handlers(
                on_connected=lambda: connected.set(), on_object_received=on_object
            )

            success = await self.subscriber.connect(agent_id="video-subscriber")
            if not success:
                logger.error("Subscriber连接失败")
                return False

            await asyncio.wait_for(connected.wait(), timeout=5.0)
            logger.info("Subscriber连接成功")
            return True
        except Exception as e:
            logger.error(f"设置Subscriber失败: {e}")
            return False

    async def run_transmission_test(self) -> bool:
        """
        执行视频传输测试

        Returns:
            bool: 测试是否成功
        """
        # 1. 生成测试视频
        with tempfile.NamedTemporaryFile(suffix=".mp4", delete=False) as f:
            self.video_path = Path(f.name)

        if not self.generate_test_video(self.video_path):
            return False

        # 2. 分割视频
        chunks = self.split_video_to_chunks(self.video_path, TEST_CONFIG["chunk_size"])

        # 3. 启动relay
        if not await self.start_relay():
            return False

        # 4. 设置publisher和subscriber
        if not await self.setup_publisher():
            return False

        if not await self.setup_subscriber():
            return False

        # 5. 发布和订阅track
        track_name = FullTrackName(
            namespace=[b"video", b"test"], track_name=b"real-video-stream"
        )

        try:
            await self.publisher.publish(track_name)
            await self.subscriber.subscribe(track_name)
            await asyncio.sleep(0.5)  # 等待订阅建立

            # 6. 传输数据
            logger.info("=" * 60)
            logger.info("开始视频传输...")
            logger.info("=" * 60)

            self.transmission_stats["start_time"] = time.time()
            self.transmission_stats["chunks_sent"] = len(chunks)
            self.transmission_stats["bytes_sent"] = sum(len(c) for c in chunks)

            for i, chunk in enumerate(chunks):
                obj = PublishedObject(
                    group_id=i // 30,  # 每30个chunks一个group
                    object_id=i % 30,
                    payload=chunk,
                    publisher_priority=128,
                    subgroup_id=0,
                    use_datagram=False,  # 使用stream模式
                )
                await self.publisher.send_object(track_name, obj)

                if (i + 1) % 10 == 0:
                    logger.info(f"传输进度: {i + 1}/{len(chunks)} chunks")

            self.transmission_stats["end_time"] = time.time()

            # 7. 等待接收完成
            logger.info("等待接收完成...")
            await asyncio.sleep(3.0)

            return True

        except Exception as e:
            logger.error(f"传输过程中发生错误: {e}")
            return False

    def analyze_results(self) -> dict:
        """
        分析传输结果

        Returns:
            dict: 分析结果
        """
        results = {
            "success": False,
            "original_hash": None,
            "received_hash": None,
            "hash_match": False,
            "transmission_stats": self.transmission_stats,
            "loss_rate": 0.0,
            "duration": 0.0,
            "throughput_mbps": 0.0,
        }

        # 计算原始视频hash
        if self.video_path and self.video_path.exists():
            results["original_hash"] = self.calculate_file_hash(self.video_path)
            logger.info(f"原始视频SHA256: {results['original_hash']}")

        # 重组接收的数据 (按group_id和object_id排序)
        if self.received_objects:
            # 按 (group_id, object_id) 排序
            sorted_objects = sorted(self.received_objects, key=lambda x: (x[0], x[1]))
            received_data = b"".join([obj[2] for obj in sorted_objects])
            results["received_hash"] = self.calculate_data_hash(received_data)
            logger.info(f"接收数据SHA256: {results['received_hash']}")
            logger.info(f"接收objects数量: {len(self.received_objects)}, 排序后重组")

            # 保存接收的视频文件用于验证
            with tempfile.NamedTemporaryFile(suffix="_received.mp4", delete=False) as f:
                self.received_video_path = Path(f.name)
                f.write(received_data)
            logger.info(f"接收视频已保存到: {self.received_video_path}")

            # 对比hash
            if results["original_hash"] and results["received_hash"]:
                results["hash_match"] = (
                    results["original_hash"] == results["received_hash"]
                )

        # 计算传输统计
        if self.transmission_stats["chunks_sent"] > 0:
            results["loss_rate"] = (
                self.transmission_stats["chunks_sent"]
                - self.transmission_stats["chunks_received"]
            ) / self.transmission_stats["chunks_sent"]

        if self.transmission_stats["end_time"] > self.transmission_stats["start_time"]:
            results["duration"] = (
                self.transmission_stats["end_time"]
                - self.transmission_stats["start_time"]
            )

        if results["duration"] > 0:
            results["throughput_mbps"] = (
                self.transmission_stats["bytes_received"] * 8
            ) / (results["duration"] * 1000 * 1000)

        results["success"] = results["hash_match"]

        return results

    def print_results(self, results: dict):
        """打印测试结果"""
        logger.info("=" * 60)
        logger.info("传输测试结果")
        logger.info("=" * 60)

        logger.info(f"原始视频Hash (SHA256): {results['original_hash']}")
        logger.info(f"接收视频Hash (SHA256): {results['received_hash']}")
        logger.info(f"Hash匹配: {'✓ 成功' if results['hash_match'] else '✗ 失败'}")

        logger.info("-" * 60)
        logger.info("传输统计:")
        logger.info(f"  - Chunks发送: {results['transmission_stats']['chunks_sent']}")
        logger.info(
            f"  - Chunks接收: {results['transmission_stats']['chunks_received']}"
        )
        logger.info(f"  - 丢失率: {results['loss_rate']:.2%}")
        logger.info(
            f"  - 数据发送: {results['transmission_stats']['bytes_sent']:,} bytes"
        )
        logger.info(
            f"  - 数据接收: {results['transmission_stats']['bytes_received']:,} bytes"
        )
        logger.info(f"  - 传输时间: {results['duration']:.3f} 秒")
        logger.info(f"  - 吞吐量: {results['throughput_mbps']:.2f} Mbps")

        logger.info("=" * 60)

        if results["success"]:
            logger.info("✓ 测试通过: 视频传输完整且稳定")
        else:
            logger.error("✗ 测试失败: 视频数据在传输过程中损坏或丢失")

    async def cleanup(self):
        """清理资源"""
        logger.info("清理资源...")

        if self.subscriber:
            self.subscriber.disconnect()
            self.subscriber = None

        if self.publisher:
            self.publisher.disconnect()
            self.publisher = None

        if self.relay:
            await self.relay.stop()
            self.relay = None

        # 清理临时文件
        if self.video_path and self.video_path.exists():
            self.video_path.unlink()
            logger.info(f"已删除临时文件: {self.video_path}")

        if self.received_video_path and self.received_video_path.exists():
            self.received_video_path.unlink()
            logger.info(f"已删除临时文件: {self.received_video_path}")

    async def run(self) -> bool:
        """
        运行完整测试

        Returns:
            bool: 测试是否成功
        """
        try:
            success = await self.run_transmission_test()

            if success:
                results = self.analyze_results()
                self.print_results(results)
                return results["success"]
            else:
                logger.error("传输测试执行失败")
                return False

        except Exception as e:
            logger.error(f"测试执行异常: {e}")
            import traceback

            traceback.print_exc()
            return False

        finally:
            await self.cleanup()


async def main():
    """主函数"""
    logger.info("=" * 60)
    logger.info("MOQ真实视频传输稳定性测试")
    logger.info("=" * 60)
    logger.info(f"配置:")
    logger.info(f"  - 视频时长: {TEST_CONFIG['video_duration']}秒")
    logger.info(f"  - 分辨率: {TEST_CONFIG['video_resolution']}")
    logger.info(f"  - 帧率: {TEST_CONFIG['video_fps']}fps")
    logger.info(f"  - 码率: {TEST_CONFIG['video_bitrate']}")
    logger.info(f"  - Chunk大小: {TEST_CONFIG['chunk_size']} bytes")
    logger.info("=" * 60)

    test = VideoTransferTest()
    success = await test.run()

    return 0 if success else 1


if __name__ == "__main__":
    exit_code = asyncio.run(main())
    sys.exit(exit_code)
