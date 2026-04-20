#!/usr/bin/env python3
"""
MOQ视频WebSocket实时流传输

支持通过WebSocket进行实时视频流传输。

使用方法:
    # 启动WebSocket服务器
    python websocket_streaming.py --mode server

    # 启动推流客户端（推送视频到服务器）
    python websocket_streaming.py --mode push --video /path/to/video.mp4 --stream mystream

    # 启动播放客户端（从服务器拉流）
    python websocket_streaming.py --mode pull --stream mystream --output /path/to/output.mp4

或者使用网页播放器:
    打开 websocket_player.html 在浏览器中观看
"""

import asyncio
import argparse
import json
import logging
import sys
from pathlib import Path
from typing import Optional, Set, Dict

sys.path.insert(0, str(Path(__file__).parent.parent))
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from aiohttp import web, WSMsgType
from moq_video_service import MOQVideoPublisher, MOQVideoSubscriber, MOQVideoRelay

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


class WebSocketStreamingServer:
    """WebSocket流媒体服务器"""

    def __init__(
        self,
        relay_host: str = "127.0.0.1",
        relay_port: int = 4433,
        ws_host: str = "0.0.0.0",
        ws_port: int = 8765,
    ):
        self.relay_host = relay_host
        self.relay_port = relay_port
        self.ws_host = ws_host
        self.ws_port = ws_port

        self._relay: Optional[MOQVideoRelay] = None
        self._app: Optional[web.Application] = None
        self._runner: Optional[web.AppRunner] = None

        # 存储活跃的WebSocket连接
        self._subscribers: Dict[str, Set[web.WebSocketResponse]] = {}
        self._publishers: Dict[str, web.WebSocketResponse] = {}

    async def start(self):
        """启动服务器"""
        # 1. 启动MOQ Relay
        self._relay = MOQVideoRelay(host=self.relay_host, port=self.relay_port)
        actual_port = await self._relay.start()
        logger.info(f"MOQ Relay started on port {actual_port}")

        # 2. 启动WebSocket服务器
        self._app = web.Application()
        self._app.router.add_get("/ws", self._handle_websocket)
        self._app.router.add_get("/api/streams", self._handle_list_streams)

        self._runner = web.AppRunner(self._app)
        await self._runner.setup()

        site = web.TCPSite(self._runner, self.ws_host, self.ws_port)
        await site.start()

        logger.info("=" * 60)
        logger.info("WebSocket Streaming Server started")
        logger.info(f"  MOQ Relay: {self.relay_host}:{actual_port}")
        logger.info(f"  WebSocket: ws://{self.ws_host}:{self.ws_port}/ws")
        logger.info(f"  HTTP API: http://{self.ws_host}:{self.ws_port}/api/streams")
        logger.info("=" * 60)
        logger.info("Press Ctrl+C to stop")

    async def stop(self):
        """停止服务器"""
        if self._runner:
            await self._runner.cleanup()
        if self._relay:
            await self._relay.stop()
        logger.info("Server stopped")

    async def _handle_websocket(self, request: web.Request):
        """处理WebSocket连接"""
        ws = web.WebSocketResponse()
        await ws.prepare(request)

        logger.info(f"WebSocket client connected: {request.remote}")

        try:
            async for msg in ws:
                if msg.type == WSMsgType.TEXT:
                    data = json.loads(msg.data)
                    await self._handle_message(ws, data)
                elif msg.type == WSMsgType.BINARY:
                    await self._handle_binary_data(ws, msg.data)
                elif msg.type == WSMsgType.ERROR:
                    logger.error(f"WebSocket error: {ws.exception()}")
        finally:
            # 清理连接
            await self._cleanup_websocket(ws)
            logger.info(f"WebSocket client disconnected: {request.remote}")

        return ws

    async def _handle_message(self, ws: web.WebSocketResponse, data: dict):
        """处理WebSocket消息"""
        msg_type = data.get("type")

        if msg_type == "publish":
            # 发布者注册
            stream_id = data.get("stream_id")
            self._publishers[ws] = stream_id
            logger.info(f"Publisher registered for stream: {stream_id}")
            await ws.send_json({"type": "publish_ack", "stream_id": stream_id})

        elif msg_type == "subscribe":
            # 订阅者注册
            stream_id = data.get("stream_id")
            if stream_id not in self._subscribers:
                self._subscribers[stream_id] = set()
            self._subscribers[stream_id].add(ws)
            logger.info(f"Subscriber registered for stream: {stream_id}")
            await ws.send_json({"type": "subscribe_ack", "stream_id": stream_id})

        elif msg_type == "unpublish":
            # 取消发布
            stream_id = self._publishers.pop(ws, None)
            if stream_id:
                logger.info(f"Publisher unregistered for stream: {stream_id}")
                # 通知所有订阅者
                await self._notify_subscribers(
                    stream_id, {"type": "stream_end", "stream_id": stream_id}
                )

        elif msg_type == "unsubscribe":
            # 取消订阅
            stream_id = data.get("stream_id")
            if stream_id in self._subscribers:
                self._subscribers[stream_id].discard(ws)
                logger.info(f"Subscriber unregistered for stream: {stream_id}")

    async def _handle_binary_data(self, ws: web.WebSocketResponse, data: bytes):
        """处理二进制视频数据"""
        stream_id = self._publishers.get(ws)
        if stream_id and stream_id in self._subscribers:
            # 转发给所有订阅者
            dead_ws = set()
            for sub_ws in self._subscribers[stream_id]:
                try:
                    await sub_ws.send_bytes(data)
                except:
                    dead_ws.add(sub_ws)

            # 清理断开的连接
            for dead in dead_ws:
                self._subscribers[stream_id].discard(dead)

    async def _cleanup_websocket(self, ws: web.WebSocketResponse):
        """清理WebSocket连接"""
        # 如果是发布者
        stream_id = self._publishers.pop(ws, None)
        if stream_id:
            await self._notify_subscribers(
                stream_id, {"type": "stream_end", "stream_id": stream_id}
            )

        # 如果是订阅者
        for stream_id, subs in self._subscribers.items():
            subs.discard(ws)

    async def _notify_subscribers(self, stream_id: str, message: dict):
        """通知所有订阅者"""
        if stream_id in self._subscribers:
            dead_ws = set()
            for ws in self._subscribers[stream_id]:
                try:
                    await ws.send_json(message)
                except:
                    dead_ws.add(ws)

            for dead in dead_ws:
                self._subscribers[stream_id].discard(dead)

    async def _handle_list_streams(self, request: web.Request):
        """列出活跃的流"""
        streams = []
        for stream_id in self._subscribers.keys():
            pub_ws = None
            for ws, sid in self._publishers.items():
                if sid == stream_id:
                    pub_ws = ws
                    break

            streams.append(
                {
                    "stream_id": stream_id,
                    "has_publisher": pub_ws is not None,
                    "subscriber_count": len(self._subscribers.get(stream_id, set())),
                }
            )

        return web.json_response({"streams": streams})


async def run_server(
    relay_host: str = "127.0.0.1",
    relay_port: int = 4433,
    ws_host: str = "0.0.0.0",
    ws_port: int = 8765,
):
    """运行WebSocket服务器"""
    server = WebSocketStreamingServer(relay_host, relay_port, ws_host, ws_port)
    await server.start()

    try:
        while True:
            await asyncio.sleep(1)
    except KeyboardInterrupt:
        logger.info("\nStopping server...")
    finally:
        await server.stop()


def main():
    parser = argparse.ArgumentParser(description="MOQ WebSocket Streaming")
    parser.add_argument("--relay-host", default="127.0.0.1", help="Relay host")
    parser.add_argument("--relay-port", type=int, default=4433, help="Relay port")
    parser.add_argument("--ws-host", default="0.0.0.0", help="WebSocket bind host")
    parser.add_argument("--ws-port", type=int, default=8765, help="WebSocket port")

    args = parser.parse_args()

    asyncio.run(
        run_server(
            args.relay_host,
            args.relay_port,
            args.ws_host,
            args.ws_port,
        )
    )


if __name__ == "__main__":
    main()
