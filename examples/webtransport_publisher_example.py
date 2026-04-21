#!/usr/bin/env python3
"""
MOQ Publisher Example over WebTransport.

Run with:
    1. python examples/webtransport_relay_example.py
    2. python examples/webtransport_publisher_example.py
"""

import asyncio
import logging
from datetime import datetime

try:
    from examples._bootstrap import ensure_repo_root, setup_logging
except ImportError:  # pragma: no cover - direct script execution fallback
    from _bootstrap import ensure_repo_root, setup_logging

ensure_repo_root()
setup_logging()

from moq import MOQPublisher, FullTrackName, PublishedObject

logger = logging.getLogger(__name__)

RELAY_HOST = "127.0.0.1"
RELAY_PORT = 4433
WEBTRANSPORT_PATH = "/moq"


async def main():
    publisher = MOQPublisher(
        relay_host=RELAY_HOST,
        relay_port=RELAY_PORT,
        transport="webtransport",
        webtransport_path=WEBTRANSPORT_PATH,
    )

    publisher.set_handlers(
        on_connected=lambda: logger.info("Publisher connected"),
        on_disconnected=lambda: logger.info("Publisher disconnected"),
        on_publication_accepted=lambda track_name: logger.info("Publication accepted: %s", track_name),
        on_publication_rejected=lambda track_name, reason: logger.warning(
            "Publication rejected: %s - %s",
            track_name,
            reason,
        ),
    )

    if not await publisher.connect():
        logger.error("Failed to connect to WebTransport relay")
        return

    track_name = FullTrackName([b"time"], b"updates-wt")
    if not await publisher.publish(track_name):
        logger.error("Failed to publish track: %s", track_name)
        publisher.disconnect()
        return

    group_id = 1
    object_id = 1

    try:
        while True:
            payload = datetime.now().isoformat().encode("utf-8")
            await publisher.send_object(
                track_name,
                PublishedObject(
                    group_id=group_id,
                    object_id=object_id,
                    payload=payload,
                    use_datagram=True,
                ),
            )
            logger.info("Sent object group=%d object=%d", group_id, object_id)
            object_id += 1
            await asyncio.sleep(1)
    except KeyboardInterrupt:
        logger.info("Publisher stopped by user")
    finally:
        try:
            await publisher.unpublish(track_name)
        except Exception:
            logger.debug("Failed to unpublish cleanly", exc_info=True)
        publisher.disconnect()


if __name__ == "__main__":
    asyncio.run(main())
