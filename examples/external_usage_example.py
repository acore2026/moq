#!/usr/bin/env python3
"""
Example: Using MOQ Transport in Another Project

This example shows how to import and use MOQ Transport interfaces
from an external project after installing the package.

Installation:
    pip install /path/to/moq-py
    
    Or for development:
    pip install -e /path/to/moq-py
"""

import asyncio
import logging

# Option 1: Import everything from the main package
from moq import (
    MOQPublisher,
    MOQSubscriber,
    FullTrackName,
    ReceivedObject,
    PublishedObject,
)

# Option 2: Import from specific submodules
# from moq.pub import MOQPublisher, PublishedObject
# from moq.sub import MOQSubscriber, ReceivedObject
# from moq.encoding import FullTrackName

# Option 3: Import transport classes for advanced usage
# from moq.transport import QUICClient, QUICServer

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def run_publisher():
    """Example: Publisher usage"""
    # Create publisher instance
    publisher = MOQPublisher(
        host="127.0.0.1",
        port=4443
    )
    
    # Connect to relay
    connected = await publisher.connect()
    if not connected:
        logger.error("Failed to connect")
        return
    
    logger.info("Publisher connected!")
    
    # Define track name
    track_name = FullTrackName(
        namespace=[b"example", b"namespace"],
        name=b"my-track"
    )
    
    # Publish track
    success = await publisher.publish(track_name)
    if not success:
        logger.error("Failed to publish")
        return
    
    logger.info(f"Publishing track: {track_name}")
    
    # Send objects
    for i in range(10):
        obj = PublishedObject(
            group_id=i,
            object_id=0,
            payload=f"Message {i}".encode()
        )
        await publisher.send_object(track_name, obj)
        logger.info(f"Sent object: group={i}, object=0")
        await asyncio.sleep(1)
    
    # Unpublish
    await publisher.unpublish(track_name)
    publisher.disconnect()


async def run_subscriber():
    """Example: Subscriber usage"""
    # Create subscriber instance
    subscriber = MOQSubscriber(
        host="127.0.0.1",
        port=4443
    )
    
    # Set up handlers
    def on_connected():
        logger.info("Subscriber connected!")
    
    def on_disconnected():
        logger.info("Subscriber disconnected!")
    
    def on_object_received(obj: ReceivedObject):
        try:
            message = obj.payload.decode('utf-8')
            logger.info(f"Received: group={obj.group_id}, object={obj.object_id}, data={message}")
        except Exception as e:
            logger.error(f"Failed to decode: {e}")
    
    subscriber.set_handlers(
        on_connected=on_connected,
        on_disconnected=on_disconnected,
        on_object_received=on_object_received
    )
    
    # Connect to relay
    connected = await subscriber.connect()
    if not connected:
        logger.error("Failed to connect")
        return
    
    # Define track name
    track_name = FullTrackName(
        namespace=[b"example", b"namespace"],
        name=b"my-track"
    )
    
    # Subscribe to track
    success = await subscriber.subscribe(track_name)
    if not success:
        logger.error("Failed to subscribe")
        return
    
    logger.info(f"Subscribed to track: {track_name}")
    
    # Keep receiving for 30 seconds
    await asyncio.sleep(30)
    
    # Unsubscribe
    await subscriber.unsubscribe(track_name)
    subscriber.disconnect()


async def run_fetch_example():
    """Example: Fetch mode usage"""
    subscriber = MOQSubscriber(
        host="127.0.0.1",
        port=4443
    )
    
    def on_object_received(obj: ReceivedObject):
        try:
            message = obj.payload.decode('utf-8')
            logger.info(f"Fetched: group={obj.group_id}, object={obj.object_id}, data={message}")
        except Exception as e:
            logger.error(f"Failed to decode: {e}")
    
    subscriber.set_handlers(on_object_received=on_object_received)
    
    connected = await subscriber.connect()
    if not connected:
        logger.error("Failed to connect")
        return
    
    track_name = FullTrackName(
        namespace=[b"example", b"namespace"],
        name=b"my-track"
    )
    
    # Example 1: Fetch from beginning (0, 0) until latest
    logger.info("Fetching from beginning until latest...")
    request_id = await subscriber.fetch(track_name=track_name)
    
    # Example 2: Fetch from specific start until latest
    # request_id = await subscriber.fetch(
    #     track_name=track_name,
    #     start_group=5,
    #     start_object=0
    # )
    
    # Example 3: Fetch specific range
    # request_id = await subscriber.fetch(
    #     track_name=track_name,
    #     start_group=0,
    #     start_object=0,
    #     end_group=10,
    #     end_object=100
    # )
    
    if request_id < 0:
        logger.error("Failed to initiate fetch")
        return
    
    logger.info(f"Fetch initiated with request_id={request_id}")
    
    # Wait for fetch to complete
    await asyncio.sleep(10)
    
    subscriber.disconnect()


if __name__ == "__main__":
    import sys
    
    if len(sys.argv) < 2:
        print("Usage: python external_usage_example.py [publisher|subscriber|fetch]")
        sys.exit(1)
    
    mode = sys.argv[1]
    
    if mode == "publisher":
        asyncio.run(run_publisher())
    elif mode == "subscriber":
        asyncio.run(run_subscriber())
    elif mode == "fetch":
        asyncio.run(run_fetch_example())
    else:
        print(f"Unknown mode: {mode}")
        print("Usage: python external_usage_example.py [publisher|subscriber|fetch]")
        sys.exit(1)
