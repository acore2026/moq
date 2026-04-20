#!/usr/bin/env python3
"""
MOQ Video Transport Service

提供供其他应用调用的视频传输接口，封装了MOQ pub/sub的复杂性。

使用方法:
    # Publisher端
    from moq_video_service import MOQVideoPublisher

    pub = MOQVideoPublisher("127.0.0.1", 4433)
    await pub.connect()
    await pub.publish_video("/path/to/video.mp4", track_name="my-video")

    # Subscriber端
    from moq_video_service import MOQVideoSubscriber

    sub = MOQVideoSubscriber("127.0.0.1", 4433)
    await sub.connect()
    await sub.subscribe(track_name="my-video", output_path="/path/to/output.mp4")

支持的接口:
    - HTTP REST API (通过aiohttp)
    - Python API (直接导入使用)
    - WebSocket (实时流传输)
"""

import asyncio
import hashlib
import json
import logging
import os
import tempfile
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Callable, List, Dict, Any, Union
from enum import Enum
import threading

from moq import MOQPublisher, MOQSubscriber, MOQRelay
from moq.encoding import FullTrackName
from moq.pub import PublishedObject
from moq.sub import ReceivedObject

logger = logging.getLogger(__name__)


class VideoTransferStatus(Enum):
    """视频传输状态"""

    PENDING = "pending"
    CONNECTING = "connecting"
    CONNECTED = "connected"
    PUBLISHING = "publishing"
    SUBSCRIBING = "subscribing"
    TRANSFERRING = "transferring"
    COMPLETED = "completed"
    FAILED = "failed"
    DISCONNECTED = "disconnected"


@dataclass
class VideoTransferStats:
    """视频传输统计信息"""

    track_name: str
    status: VideoTransferStatus
    bytes_sent: int = 0
    bytes_received: int = 0
    chunks_sent: int = 0
    chunks_received: int = 0
    start_time: Optional[float] = None
    end_time: Optional[float] = None
    error_message: Optional[str] = None
    original_hash: Optional[str] = None
    received_hash: Optional[str] = None

    @property
    def duration(self) -> float:
        """传输持续时间"""
        if self.start_time and self.end_time:
            return self.end_time - self.start_time
        elif self.start_time:
            return time.time() - self.start_time
        return 0.0

    @property
    def throughput_mbps(self) -> float:
        """吞吐量(Mbps)"""
        duration = self.duration
        if duration > 0 and self.bytes_received > 0:
            return (self.bytes_received * 8) / (duration * 1000 * 1000)
        return 0.0

    @property
    def loss_rate(self) -> float:
        """丢包率"""
        if self.chunks_sent > 0:
            return (self.chunks_sent - self.chunks_received) / self.chunks_sent
        return 0.0

    def to_dict(self) -> Dict[str, Any]:
        """转换为字典"""
        return {
            "track_name": self.track_name,
            "status": self.status.value,
            "bytes_sent": self.bytes_sent,
            "bytes_received": self.bytes_received,
            "chunks_sent": self.chunks_sent,
            "chunks_received": self.chunks_received,
            "duration": self.duration,
            "throughput_mbps": self.throughput_mbps,
            "loss_rate": self.loss_rate,
            "original_hash": self.original_hash,
            "received_hash": self.received_hash,
            "hash_match": self.original_hash == self.received_hash
            if self.original_hash and self.received_hash
            else None,
            "error_message": self.error_message,
        }


class MOQVideoPublisher:
    """
    MOQ视频发布者

    用于发布视频流到MOQ Relay。
    """

    def __init__(
        self,
        relay_host: str,
        relay_port: int,
        chunk_size: int = 16384,
        agent_id: Optional[str] = None,
    ):
        self.relay_host = relay_host
        self.relay_port = relay_port
        self.chunk_size = chunk_size
        self.agent_id = agent_id or f"video-pub-{id(self)}"

        self._publisher: Optional[MOQPublisher] = None
        self._connected = False
        self._stats: Dict[str, VideoTransferStats] = {}
        self._on_progress: Optional[Callable[[VideoTransferStats], None]] = None

    async def connect(self) -> bool:
        """连接到MOQ Relay"""
        try:
            self._publisher = MOQPublisher(self.relay_host, self.relay_port)

            connected_event = asyncio.Event()
            self._publisher.set_handlers(on_connected=lambda: connected_event.set())

            success = await self._publisher.connect(agent_id=self.agent_id)
            if not success:
                logger.error("Failed to connect to relay")
                return False

            await asyncio.wait_for(connected_event.wait(), timeout=10.0)
            self._connected = True
            logger.info(
                f"VideoPublisher connected to {self.relay_host}:{self.relay_port}"
            )
            return True

        except Exception as e:
            logger.error(f"Error connecting to relay: {e}")
            return False

    def disconnect(self):
        """断开连接"""
        if self._publisher:
            self._publisher.disconnect()
            self._connected = False
            logger.info("VideoPublisher disconnected")

    def set_progress_callback(self, callback: Callable[[VideoTransferStats], None]):
        """设置进度回调函数"""
        self._on_progress = callback

    async def publish_video(
        self,
        video_path: Union[str, Path],
        track_name: str,
        namespace: Optional[List[str]] = None,
    ) -> VideoTransferStats:
        """
        发布视频文件

        Args:
            video_path: 视频文件路径
            track_name: 轨道名称
            namespace: 命名空间列表（可选）

        Returns:
            VideoTransferStats: 传输统计信息
        """
        video_path = Path(video_path)
        if not video_path.exists():
            raise FileNotFoundError(f"Video file not found: {video_path}")

        # 创建统计对象
        stats = VideoTransferStats(
            track_name=track_name,
            status=VideoTransferStatus.PENDING,
        )
        self._stats[track_name] = stats

        try:
            # 读取视频文件
            with open(video_path, "rb") as f:
                video_data = f.read()

            stats.original_hash = hashlib.sha256(video_data).hexdigest()
            stats.status = VideoTransferStatus.PUBLISHING
            stats.start_time = time.time()

            # 分割成chunks
            chunks = []
            for i in range(0, len(video_data), self.chunk_size):
                chunks.append(video_data[i : i + self.chunk_size])

            stats.chunks_sent = len(chunks)
            stats.bytes_sent = len(video_data)

            # 创建FullTrackName
            ns_bytes = [ns.encode() for ns in (namespace or ["video", "default"])]
            full_track_name = FullTrackName(
                namespace=ns_bytes, track_name=track_name.encode()
            )

            # 发布track
            await self._publisher.publish(full_track_name)
            logger.info(
                f"Publishing video: {track_name}, {len(chunks)} chunks, {len(video_data)} bytes"
            )

            # 发送chunks
            stats.status = VideoTransferStatus.TRANSFERRING
            for i, chunk in enumerate(chunks):
                obj = PublishedObject(
                    group_id=i // 30,  # 每30个chunks一个group
                    object_id=i % 30,
                    payload=chunk,
                    publisher_priority=128,
                    subgroup_id=0,
                    use_datagram=False,
                )
                await self._publisher.send_object(full_track_name, obj)

                if self._on_progress and i % 10 == 0:
                    self._on_progress(stats)

            stats.end_time = time.time()
            stats.status = VideoTransferStatus.COMPLETED

            logger.info(f"Video published successfully: {track_name}")
            logger.info(
                f"  Duration: {stats.duration:.3f}s, Throughput: {stats.throughput_mbps:.2f} Mbps"
            )

            return stats

        except Exception as e:
            stats.status = VideoTransferStatus.FAILED
            stats.error_message = str(e)
            logger.error(f"Error publishing video: {e}")
            raise

    async def publish_bytes(
        self,
        data: bytes,
        track_name: str,
        namespace: Optional[List[str]] = None,
    ) -> VideoTransferStats:
        """
        发布字节数据（视频数据）

        Args:
            data: 视频字节数据
            track_name: 轨道名称
            namespace: 命名空间列表（可选）

        Returns:
            VideoTransferStats: 传输统计信息
        """
        # 创建统计对象
        stats = VideoTransferStats(
            track_name=track_name,
            status=VideoTransferStatus.PENDING,
        )
        self._stats[track_name] = stats

        try:
            stats.original_hash = hashlib.sha256(data).hexdigest()
            stats.status = VideoTransferStatus.PUBLISHING
            stats.start_time = time.time()

            # 分割成chunks
            chunks = []
            for i in range(0, len(data), self.chunk_size):
                chunks.append(data[i : i + self.chunk_size])

            stats.chunks_sent = len(chunks)
            stats.bytes_sent = len(data)

            # 创建FullTrackName
            ns_bytes = [ns.encode() for ns in (namespace or ["video", "default"])]
            full_track_name = FullTrackName(
                namespace=ns_bytes, track_name=track_name.encode()
            )

            # 发布track
            await self._publisher.publish(full_track_name)
            logger.info(
                f"Publishing data: {track_name}, {len(chunks)} chunks, {len(data)} bytes"
            )

            # 发送chunks
            stats.status = VideoTransferStatus.TRANSFERRING
            for i, chunk in enumerate(chunks):
                obj = PublishedObject(
                    group_id=i // 30,
                    object_id=i % 30,
                    payload=chunk,
                    publisher_priority=128,
                    subgroup_id=0,
                    use_datagram=False,
                )
                await self._publisher.send_object(full_track_name, obj)

                if self._on_progress and i % 10 == 0:
                    self._on_progress(stats)

            stats.end_time = time.time()
            stats.status = VideoTransferStatus.COMPLETED

            logger.info(f"Data published successfully: {track_name}")
            logger.info(
                f"  Duration: {stats.duration:.3f}s, Throughput: {stats.throughput_mbps:.2f} Mbps"
            )

            return stats

        except Exception as e:
            stats.status = VideoTransferStatus.FAILED
            stats.error_message = str(e)
            logger.error(f"Error publishing data: {e}")
            raise

    def get_stats(self, track_name: str) -> Optional[VideoTransferStats]:
        """获取传输统计信息"""
        return self._stats.get(track_name)


class MOQVideoSubscriber:
    """
    MOQ视频订阅者

    用于从MOQ Relay订阅视频流。
    """

    def __init__(
        self,
        relay_host: str,
        relay_port: int,
        agent_id: Optional[str] = None,
    ):
        self.relay_host = relay_host
        self.relay_port = relay_port
        self.agent_id = agent_id or f"video-sub-{id(self)}"

        self._subscriber: Optional[MOQSubscriber] = None
        self._connected = False
        self._stats: Dict[str, VideoTransferStats] = {}
        self._received_objects: Dict[
            str, List[tuple]
        ] = {}  # track_name -> [(group_id, object_id, payload)]
        self._on_progress: Optional[Callable[[VideoTransferStats], None]] = None
        self._on_complete: Optional[Callable[[str, bytes], None]] = None

    async def connect(self) -> bool:
        """连接到MOQ Relay"""
        try:
            self._subscriber = MOQSubscriber(self.relay_host, self.relay_port)

            connected_event = asyncio.Event()
            self._subscriber.set_handlers(on_connected=lambda: connected_event.set())

            success = await self._subscriber.connect(agent_id=self.agent_id)
            if not success:
                logger.error("Failed to connect to relay")
                return False

            await asyncio.wait_for(connected_event.wait(), timeout=10.0)
            self._connected = True
            logger.info(
                f"VideoSubscriber connected to {self.relay_host}:{self.relay_port}"
            )
            return True

        except Exception as e:
            logger.error(f"Error connecting to relay: {e}")
            return False

    def disconnect(self):
        """断开连接"""
        if self._subscriber:
            self._subscriber.disconnect()
            self._connected = False
            logger.info("VideoSubscriber disconnected")

    def set_progress_callback(self, callback: Callable[[VideoTransferStats], None]):
        """设置进度回调函数"""
        self._on_progress = callback

    def set_complete_callback(self, callback: Callable[[str, bytes], None]):
        """
        设置接收完成回调函数

        Args:
            callback: 接收完成时的回调函数，参数为 (track_name, data)
        """
        self._on_complete = callback

    async def subscribe(
        self,
        track_name: str,
        output_path: Optional[Union[str, Path]] = None,
        namespace: Optional[List[str]] = None,
        wait_time: float = 10.0,
    ) -> VideoTransferStats:
        """
        订阅视频流

        Args:
            track_name: 轨道名称
            output_path: 输出文件路径（可选）
            namespace: 命名空间列表（可选）
            wait_time: 等待接收完成的最大时间（秒）

        Returns:
            VideoTransferStats: 传输统计信息
        """
        # 创建统计对象
        stats = VideoTransferStats(
            track_name=track_name,
            status=VideoTransferStatus.PENDING,
        )
        self._stats[track_name] = stats
        self._received_objects[track_name] = []

        try:
            # 创建FullTrackName
            ns_bytes = [ns.encode() for ns in (namespace or ["video", "default"])]
            full_track_name = FullTrackName(
                namespace=ns_bytes, track_name=track_name.encode()
            )

            # 设置接收回调
            def on_object(obj: ReceivedObject):
                self._received_objects[track_name].append(
                    (obj.group_id, obj.object_id, obj.payload)
                )
                stats.chunks_received += 1
                stats.bytes_received += len(obj.payload)

                if self._on_progress and stats.chunks_received % 10 == 0:
                    self._on_progress(stats)

            self._subscriber.set_handlers(on_object_received=on_object)

            # 订阅track
            stats.status = VideoTransferStatus.SUBSCRIBING
            stats.start_time = time.time()

            await self._subscriber.subscribe(full_track_name)
            logger.info(f"Subscribed to video: {track_name}")

            stats.status = VideoTransferStatus.TRANSFERRING

            # 等待接收完成
            await asyncio.sleep(wait_time)

            # 重组数据
            if self._received_objects[track_name]:
                # 按(group_id, object_id)排序
                sorted_objects = sorted(
                    self._received_objects[track_name], key=lambda x: (x[0], x[1])
                )
                received_data = b"".join([obj[2] for obj in sorted_objects])

                stats.received_hash = hashlib.sha256(received_data).hexdigest()
                stats.end_time = time.time()

                # 保存到文件
                if output_path:
                    output_path = Path(output_path)
                    output_path.parent.mkdir(parents=True, exist_ok=True)
                    with open(output_path, "wb") as f:
                        f.write(received_data)
                    logger.info(f"Video saved to: {output_path}")

                # 调用完成回调
                if self._on_complete:
                    self._on_complete(track_name, received_data)

                stats.status = VideoTransferStatus.COMPLETED

                logger.info(f"Video received successfully: {track_name}")
                logger.info(
                    f"  Chunks: {stats.chunks_received}, Bytes: {stats.bytes_received}"
                )
                logger.info(
                    f"  Duration: {stats.duration:.3f}s, Throughput: {stats.throughput_mbps:.2f} Mbps"
                )

            else:
                stats.status = VideoTransferStatus.FAILED
                stats.error_message = "No data received"
                logger.warning(f"No data received for track: {track_name}")

            return stats

        except Exception as e:
            stats.status = VideoTransferStatus.FAILED
            stats.error_message = str(e)
            logger.error(f"Error subscribing to video: {e}")
            raise

    def get_stats(self, track_name: str) -> Optional[VideoTransferStats]:
        """获取传输统计信息"""
        return self._stats.get(track_name)


class MOQVideoRelay:
    """
    MOQ视频Relay服务

    提供独立的Relay服务，供Publisher和Subscriber连接。
    """

    def __init__(
        self,
        host: str = "0.0.0.0",
        port: int = 4433,
        cert_file: Optional[str] = None,
        key_file: Optional[str] = None,
        max_memory_cache: int = 500 * 1024 * 1024,  # 500MB
    ):
        self.host = host
        self.port = port
        self.cert_file = cert_file
        self.key_file = key_file
        self.max_memory_cache = max_memory_cache

        self._relay: Optional[MOQRelay] = None
        self._running = False

    async def start(self) -> int:
        """
        启动Relay服务

        Returns:
            int: 实际端口号
        """
        self._relay = MOQRelay(
            host=self.host,
            port=self.port,
            max_memory_cache=self.max_memory_cache,
            cert_file=self.cert_file,
            key_file=self.key_file,
        )

        await self._relay.start()
        self._running = True

        actual_port = self._relay._quic_server.actual_port or self.port
        logger.info(f"VideoRelay started on {self.host}:{actual_port}")
        return actual_port

    async def stop(self):
        """停止Relay服务"""
        if self._relay:
            await self._relay.stop()
            self._running = False
            logger.info("VideoRelay stopped")

    def is_running(self) -> bool:
        """检查Relay是否正在运行"""
        return self._running


# ============ HTTP REST API (可选) ============

try:
    from aiohttp import web

    AIOHTTP_AVAILABLE = True
except ImportError:
    AIOHTTP_AVAILABLE = False
    logger.warning("aiohttp not available. HTTP API will not be available.")


class MOQVideoServiceAPI:
    """
    MOQ视频传输HTTP REST API

    提供HTTP接口供其他应用调用。

    启动服务:
        service = MOQVideoServiceAPI("127.0.0.1", 4433)
        await service.start_api_server(host="0.0.0.0", port=8080)

    API端点:
        - POST /api/publish - 发布视频
        - POST /api/subscribe - 订阅视频
        - GET /api/status/{track_name} - 获取传输状态
    """

    def __init__(self, relay_host: str, relay_port: int):
        if not AIOHTTP_AVAILABLE:
            raise RuntimeError("aiohttp is required for HTTP API")

        self.relay_host = relay_host
        self.relay_port = relay_port
        self._app: Optional[web.Application] = None
        self._runner: Optional[web.AppRunner] = None
        self._transfers: Dict[str, VideoTransferStats] = {}

    async def start_api_server(self, host: str = "0.0.0.0", port: int = 8080):
        """启动HTTP API服务器"""
        self._app = web.Application()

        # 注册路由
        self._app.router.add_post("/api/publish", self._handle_publish)
        self._app.router.add_post("/api/subscribe", self._handle_subscribe)
        self._app.router.add_get("/api/status/{track_name}", self._handle_status)
        self._app.router.add_get("/api/health", self._handle_health)

        self._runner = web.AppRunner(self._app)
        await self._runner.setup()

        site = web.TCPSite(self._runner, host, port)
        await site.start()

        logger.info(f"HTTP API server started on http://{host}:{port}")

    async def stop_api_server(self):
        """停止HTTP API服务器"""
        if self._runner:
            await self._runner.cleanup()
            logger.info("HTTP API server stopped")

    async def _handle_publish(self, request: web.Request) -> web.Response:
        """处理发布视频请求"""
        try:
            data = await request.json()
            video_path = data.get("video_path")
            track_name = data.get("track_name")

            if not video_path or not track_name:
                return web.json_response(
                    {"error": "Missing video_path or track_name"}, status=400
                )

            # 创建Publisher并发布
            pub = MOQVideoPublisher(self.relay_host, self.relay_port)
            if not await pub.connect():
                return web.json_response(
                    {"error": "Failed to connect to relay"}, status=500
                )

            try:
                stats = await pub.publish_video(video_path, track_name)
                self._transfers[track_name] = stats
                return web.json_response(stats.to_dict())
            finally:
                pub.disconnect()

        except Exception as e:
            logger.error(f"Error handling publish request: {e}")
            return web.json_response({"error": str(e)}, status=500)

    async def _handle_subscribe(self, request: web.Request) -> web.Response:
        """处理订阅视频请求"""
        try:
            data = await request.json()
            track_name = data.get("track_name")
            output_path = data.get("output_path")
            wait_time = data.get("wait_time", 10.0)

            if not track_name:
                return web.json_response({"error": "Missing track_name"}, status=400)

            # 创建Subscriber并订阅
            sub = MOQVideoSubscriber(self.relay_host, self.relay_port)
            if not await sub.connect():
                return web.json_response(
                    {"error": "Failed to connect to relay"}, status=500
                )

            try:
                stats = await sub.subscribe(
                    track_name, output_path, wait_time=wait_time
                )
                self._transfers[track_name] = stats
                return web.json_response(stats.to_dict())
            finally:
                sub.disconnect()

        except Exception as e:
            logger.error(f"Error handling subscribe request: {e}")
            return web.json_response({"error": str(e)}, status=500)

    async def _handle_status(self, request: web.Request) -> web.Response:
        """处理获取状态请求"""
        track_name = request.match_info.get("track_name")
        stats = self._transfers.get(track_name)

        if not stats:
            return web.json_response({"error": "Track not found"}, status=404)

        return web.json_response(stats.to_dict())

    async def _handle_health(self, request: web.Request) -> web.Response:
        """处理健康检查请求"""
        return web.json_response({"status": "healthy"})


# ============ 使用示例 ============


async def example_usage():
    """使用示例"""

    # 1. 启动Relay（通常在单独进程中）
    relay = MOQVideoRelay(host="127.0.0.1", port=4433)
    relay_port = await relay.start()

    # 2. Publisher发布视频
    pub = MOQVideoPublisher("127.0.0.1", relay_port)
    await pub.connect()

    def on_progress(stats: VideoTransferStats):
        print(f"Progress: {stats.chunks_sent} chunks sent")

    pub.set_progress_callback(on_progress)

    stats = await pub.publish_video(
        video_path="/path/to/video.mp4", track_name="my-video-1080p"
    )
    print(f"Published: {stats.to_dict()}")

    pub.disconnect()

    # 3. Subscriber订阅视频
    sub = MOQVideoSubscriber("127.0.0.1", relay_port)
    await sub.connect()

    def on_complete(track_name: str, data: bytes):
        print(f"Received complete video: {track_name}, {len(data)} bytes")

    sub.set_complete_callback(on_complete)

    stats = await sub.subscribe(
        track_name="my-video-1080p", output_path="/path/to/output.mp4", wait_time=15.0
    )
    print(f"Subscribed: {stats.to_dict()}")

    sub.disconnect()

    # 4. 停止Relay
    await relay.stop()


if __name__ == "__main__":
    # 运行示例
    logging.basicConfig(level=logging.INFO)
    asyncio.run(example_usage())
