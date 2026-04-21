#!/usr/bin/env python3
"""
MOQ Relay Example with both native QUIC and WebTransport enabled.

This starts one relay instance with two listeners sharing the same cache,
publications, and subscriptions on the same UDP port:
  - native QUIC via ALPN `moq-00`
  - WebTransport via HTTP/3 extended CONNECT on `https://127.0.0.1:4443/moq`

Usage:
    python examples/dual_transport_relay_example.py
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
QUIC_PORT = 4443
WEBTRANSPORT_PATH = "/moq"


async def main():
    relay = MOQRelay(
        host=RELAY_HOST,
        port=QUIC_PORT,
        cache_dir="/tmp/moq_relay_cache_dual",
        max_memory_cache=100 * 1024 * 1024,
        max_disk_cache=1024 * 1024 * 1024,
        transport="both",
        webtransport_path=WEBTRANSPORT_PATH,
    )

    logger.info("Starting dual-transport relay")
    logger.info("Native QUIC + WebTransport: %s:%d", RELAY_HOST, QUIC_PORT)
    logger.info(
        "WebTransport: https://%s:%d%s",
        RELAY_HOST,
        QUIC_PORT,
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
