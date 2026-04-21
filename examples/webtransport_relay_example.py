#!/usr/bin/env python3
"""
MOQ Relay Example over WebTransport.

This starts the relay on an HTTPS + WebTransport endpoint instead of native QUIC.

Usage:
    python examples/webtransport_relay_example.py
"""

import asyncio
import logging

try:
    from examples._bootstrap import ensure_repo_root, setup_logging
except ImportError:  # pragma: no cover - direct script execution fallback
    from _bootstrap import ensure_repo_root, setup_logging

ensure_repo_root()
setup_logging()

from moq import MOQRelay

logger = logging.getLogger(__name__)

RELAY_HOST = "127.0.0.1"
RELAY_PORT = 4433
WEBTRANSPORT_PATH = "/moq"


async def main():
    relay = MOQRelay(
        host=RELAY_HOST,
        port=RELAY_PORT,
        cache_dir="/tmp/moq_relay_cache_wt",
        max_memory_cache=100 * 1024 * 1024,
        max_disk_cache=1024 * 1024 * 1024,
        transport="webtransport",
        webtransport_path=WEBTRANSPORT_PATH,
    )

    logger.info(
        "Starting MOQ relay over WebTransport at https://%s:%d%s",
        RELAY_HOST,
        RELAY_PORT,
        WEBTRANSPORT_PATH,
    )

    try:
        await relay.start()
        while True:
            await asyncio.sleep(1)
    except KeyboardInterrupt:
        logger.info("Relay stopped by user")
    finally:
        await relay.stop()


if __name__ == "__main__":
    asyncio.run(main())
