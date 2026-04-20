#!/usr/bin/env python3
"""
MOQ视频传输HTTP API服务

提供REST API供其他应用调用。

启动服务:
    python http_api_server.py

API端点:
    - POST /api/publish - 发布视频
    - POST /api/subscribe - 订阅视频
    - GET /api/status/{track_name} - 获取传输状态
    - GET /api/health - 健康检查

示例请求:
    # 发布视频
    curl -X POST http://localhost:8080/api/publish \
        -H "Content-Type: application/json" \
        -d '{"video_path": "/path/to/video.mp4", "track_name": "my-video"}'
    
    # 订阅视频
    curl -X POST http://localhost:8080/api/subscribe \
        -H "Content-Type: application/json" \
        -d '{"track_name": "my-video", "output_path": "/path/to/output.mp4"}'
    
    # 获取状态
    curl http://localhost:8080/api/status/my-video
"""

import asyncio
import argparse
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from aiohttp import web
from moq_video_service import MOQVideoServiceAPI, MOQVideoRelay

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


async def start_servers(
    relay_host: str = "127.0.0.1",
    relay_port: int = 4433,
    api_host: str = "0.0.0.0",
    api_port: int = 8080,
):
    """启动Relay和HTTP API服务"""

    # 1. 启动MOQ Relay
    logger.info(f"Starting MOQ Relay on {relay_host}:{relay_port}")
    relay = MOQVideoRelay(host=relay_host, port=relay_port)
    actual_relay_port = await relay.start()
    logger.info(f"MOQ Relay started on port {actual_relay_port}")

    # 2. 启动HTTP API服务
    logger.info(f"Starting HTTP API on http://{api_host}:{api_port}")
    api = MOQVideoServiceAPI(relay_host, actual_relay_port)
    await api.start_api_server(host=api_host, port=api_port)

    logger.info("=" * 60)
    logger.info("Services started:")
    logger.info(f"  MOQ Relay: {relay_host}:{actual_relay_port}")
    logger.info(f"  HTTP API: http://{api_host}:{api_port}")
    logger.info("=" * 60)
    logger.info("API Endpoints:")
    logger.info(f"  POST http://{api_host}:{api_port}/api/publish")
    logger.info(f"  POST http://{api_host}:{api_port}/api/subscribe")
    logger.info(f"  GET  http://{api_host}:{api_port}/api/status/<track_name>")
    logger.info(f"  GET  http://{api_host}:{api_port}/api/health")
    logger.info("=" * 60)
    logger.info("Press Ctrl+C to stop")

    try:
        while True:
            await asyncio.sleep(1)
    except KeyboardInterrupt:
        logger.info("\nStopping services...")
    finally:
        await api.stop_api_server()
        await relay.stop()
        logger.info("Services stopped")


def main():
    parser = argparse.ArgumentParser(description="MOQ Video HTTP API Server")
    parser.add_argument("--relay-host", default="127.0.0.1", help="Relay bind host")
    parser.add_argument(
        "--relay-port", type=int, default=4433, help="Relay port (0 for auto)"
    )
    parser.add_argument("--api-host", default="0.0.0.0", help="API bind host")
    parser.add_argument("--api-port", type=int, default=8080, help="API port")

    args = parser.parse_args()

    asyncio.run(
        start_servers(
            args.relay_host,
            args.relay_port,
            args.api_host,
            args.api_port,
        )
    )


if __name__ == "__main__":
    main()
