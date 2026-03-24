#!/usr/bin/env python3
"""
MOQ Relay Example - Demonstrates how to use the MOQRelay class.

This example shows how to run a MOQ relay server using the public interface
from the moq package. The relay uses QUIC as the underlying transport.

Usage:
    python relay_example.py

The relay listens on 127.0.0.1:4443 and acts as an intermediary
between publishers and subscribers.
"""

import asyncio
import logging

from _bootstrap import ensure_repo_root, setup_logging

ensure_repo_root()
setup_logging()

# Import MOQRelay from the public moq interface
from moq import MOQRelay
logger = logging.getLogger(__name__)

RELAY_HOST = "127.0.0.1"
RELAY_PORT = 4443


async def main():
    """Run the MOQ relay server."""
    logger.info("Starting MOQ Relay Example")
    
    # Create relay instance using the public interface
    relay = MOQRelay(
        host=RELAY_HOST,
        port=RELAY_PORT,
        cache_dir="/tmp/moq_relay_cache",
        max_memory_cache=100 * 1024 * 1024,  # 100MB
        max_disk_cache=1024 * 1024 * 1024     # 1GB
    )
    
    logger.info(f"Relay configured: {RELAY_HOST}:{RELAY_PORT}")
    logger.info(f"Cache directory: /tmp/moq_relay_cache")
    
    try:
        # Start the relay - this uses QUIC transport
        await relay.start()
        logger.info("Relay started")

        # Keep the process alive until interrupted.
        while True:
            await asyncio.sleep(1)
    except KeyboardInterrupt:
        logger.info("Relay stopped by user")
    except Exception as e:
        logger.error(f"Relay error: {e}")
    finally:
        await relay.stop()
        logger.info("Relay shutdown complete")


if __name__ == "__main__":
    asyncio.run(main())
