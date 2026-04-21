#!/usr/bin/env python3
"""
MOQ Subscriber Example over WebTransport.

Run with:
    1. python examples/webtransport_relay_example.py
    2. python examples/webtransport_publisher_example.py
    3. python examples/webtransport_subscriber_example.py
"""

import asyncio
import logging

try:
    from examples._bootstrap import ensure_repo_root, setup_logging
except ImportError:  # pragma: no cover - direct script execution fallback
    from _bootstrap import ensure_repo_root, setup_logging

ensure_repo_root()
setup_logging()

from moq import MOQSubscriber, FullTrackName, ReceivedObject

logger = logging.getLogger(__name__)

RELAY_HOST = "127.0.0.1"
RELAY_PORT = 4433
WEBTRANSPORT_PATH = "/moq"


async def main():
    subscriber = MOQSubscriber(
        relay_host=RELAY_HOST,
        relay_port=RELAY_PORT,
        transport="webtransport",
        webtransport_path=WEBTRANSPORT_PATH,
    )

    def on_object_received(obj: ReceivedObject):
        try:
            payload = obj.payload.decode("utf-8")
        except UnicodeDecodeError:
            payload = obj.payload.hex()
        logger.info(
            "Received group=%d object=%d payload=%s",
            obj.group_id,
            obj.object_id,
            payload,
        )

    subscriber.set_handlers(
        on_connected=lambda: logger.info("Subscriber connected"),
        on_disconnected=lambda: logger.info("Subscriber disconnected"),
        on_object_received=on_object_received,
        on_subscription_accepted=lambda track_name: logger.info("Subscription accepted: %s", track_name),
        on_subscription_rejected=lambda track_name, reason: logger.warning(
            "Subscription rejected: %s - %s",
            track_name,
            reason,
        ),
    )

    if not await subscriber.connect():
        logger.error("Failed to connect to WebTransport relay")
        return

    track_name = FullTrackName([b"time"], b"updates-wt")
    await subscriber.subscribe(track_name)

    try:
        while True:
            await asyncio.sleep(1)
    except KeyboardInterrupt:
        logger.info("Subscriber stopped by user")
    finally:
        try:
            await subscriber.unsubscribe(track_name)
        except Exception:
            logger.debug("Failed to unsubscribe cleanly", exc_info=True)
        subscriber.disconnect()


if __name__ == "__main__":
    asyncio.run(main())
