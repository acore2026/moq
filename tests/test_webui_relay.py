#!/usr/bin/env python3
"""
Test video transfer through agent_gw relay (port 9003)
"""

import asyncio
import logging
import sys
from pathlib import Path

# Add project path
sys.path.insert(0, str(Path(__file__).parent.parent))

from moq import MOQPublisher, MOQSubscriber, FullTrackName, PublishedObject
from moq.sub.subscriber import ReceivedObject

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

RELAY_HOST = "127.0.0.1"
RELAY_PORT = 9003


async def test_video_transfer():
    # Test data
    test_data = b"TEST_VIDEO_DATA_CHUNK_1234567890" * 1000  # ~30KB

    # Create publisher
    publisher = MOQPublisher(RELAY_HOST, RELAY_PORT)
    pub_connected = asyncio.Event()
    publisher.set_handlers(on_connected=lambda: pub_connected.set())

    logger.info("Connecting publisher...")
    if not await publisher.connect(agent_id="video-test-publisher"):
        logger.error("Publisher connection failed")
        return False

    await asyncio.wait_for(pub_connected.wait(), timeout=5.0)
    logger.info("Publisher connected")

    # Publish track
    track_name = FullTrackName(namespace=[b"test", b"video"], track_name=b"test-stream")
    await publisher.publish(track_name)
    logger.info(f"Published track: {track_name}")

    await asyncio.sleep(0.5)

    # Create subscriber
    subscriber = MOQSubscriber(RELAY_HOST, RELAY_PORT)
    sub_connected = asyncio.Event()
    received_objects = []

    def on_object(obj: ReceivedObject):
        received_objects.append(obj)
        logger.info(
            f"[MOQ DEBUG] Received object: group={obj.group_id}, obj={obj.object_id}, size={len(obj.payload)}"
        )

    subscriber.set_handlers(
        on_connected=lambda: sub_connected.set(), on_object_received=on_object
    )

    logger.info("Connecting subscriber...")
    if not await subscriber.connect(agent_id="video-test-subscriber"):
        logger.error("Subscriber connection failed")
        return False

    await asyncio.wait_for(sub_connected.wait(), timeout=5.0)
    logger.info("Subscriber connected")

    # Subscribe
    await subscriber.subscribe(track_name)
    logger.info(f"Subscribed to track: {track_name}")

    await asyncio.sleep(0.5)

    # Send objects
    logger.info(f"Sending {len(test_data)} bytes in chunks...")
    chunk_size = 16384
    for i in range(0, len(test_data), chunk_size):
        chunk = test_data[i : i + chunk_size]
        obj = PublishedObject(
            group_id=0, object_id=i // chunk_size, payload=chunk, use_datagram=False
        )
        await publisher.send_object(track_name, obj)
        logger.info(f"Sent chunk {i // chunk_size}: {len(chunk)} bytes")

    logger.info("Waiting for objects to be received...")
    await asyncio.sleep(3.0)

    logger.info(f"Received {len(received_objects)} objects")

    # Cleanup
    publisher.disconnect()
    subscriber.disconnect()

    return len(received_objects) > 0


if __name__ == "__main__":
    result = asyncio.run(test_video_transfer())
    logger.info(f"Test result: {'PASS' if result else 'FAIL'}")
