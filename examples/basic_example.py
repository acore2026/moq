#!/usr/bin/env python3
"""
Basic MOQ Publish-Subscribe Example

This example demonstrates basic publish and subscribe functionality.
Run this example to see how publisher and subscriber interact through a relay.

Usage:
    Terminal 1: python basic_example.py relay
    Terminal 2: python basic_example.py publisher
    Terminal 3: python basic_example.py subscriber
"""

import asyncio
import sys
import logging
from typing import Optional

from _bootstrap import ensure_repo_root, setup_logging

ensure_repo_root()
setup_logging()
logger = logging.getLogger(__name__)

from moq.encoding import FullTrackName
from moq.pub import MOQPublisher, PublishedObject
from moq.sub import MOQSubscriber, ReceivedObject
from moq.relay import MOQRelay


# Configuration
RELAY_HOST = "127.0.0.1"
RELAY_PORT = 4443

# Track configuration
TRACK_NAMESPACE = [b"example", b"test"]
TRACK_NAME = b"video"


async def run_relay():
    """Run the relay server."""
    logger.info("Starting relay...")
    
    relay = MOQRelay(
        host=RELAY_HOST,
        port=RELAY_PORT,
        cache_dir="/tmp/moq_cache",
        max_memory_cache=50 * 1024 * 1024,  # 50MB
        max_disk_cache=200 * 1024 * 1024    # 200MB
    )
    
    await relay.start()
    logger.info(f"Relay running on {RELAY_HOST}:{RELAY_PORT}")
    
    try:
        # Keep running
        while True:
            await asyncio.sleep(1)
            
            # Print cache stats periodically
            stats = relay.get_cache_stats()
            if stats['memory_objects'] > 0:
                logger.info(f"Cache stats: {stats}")
                
    except KeyboardInterrupt:
        logger.info("Shutting down relay...")
    finally:
        await relay.stop()


async def run_publisher():
    """Run the publisher."""
    logger.info("Starting publisher...")
    
    publisher = MOQPublisher(RELAY_HOST, RELAY_PORT)
    
    # Set handlers
    def on_connected():
        logger.info("Publisher connected to relay")
    
    def on_disconnected():
        logger.info("Publisher disconnected from relay")
    
    def on_publication_accepted(track_name: FullTrackName):
        logger.info(f"Publication accepted: {track_name}")
    
    def on_publication_rejected(track_name: FullTrackName, reason: str):
        logger.warning(f"Publication rejected: {track_name}, reason: {reason}")
    
    publisher.set_handlers(
        on_connected=on_connected,
        on_disconnected=on_disconnected,
        on_publication_accepted=on_publication_accepted,
        on_publication_rejected=on_publication_rejected
    )
    
    # Connect to relay
    if not await publisher.connect():
        logger.error("Failed to connect to relay")
        return
    
    # Create track name
    track_name = FullTrackName(TRACK_NAMESPACE, TRACK_NAME)
    
    # Publish the track
    if not await publisher.publish(track_name):
        logger.error("Failed to publish track")
        return
    
    logger.info(f"Publishing track: {track_name}")
    
    # Send objects
    try:
        for group_id in range(1, 6):  # 5 groups
            for object_id in range(1, 4):  # 3 objects per group
                # Create object payload
                payload = f"Object from group {group_id}, object {object_id}".encode()
                
                obj = PublishedObject(
                    group_id=group_id,
                    object_id=object_id,
                    payload=payload,
                    publisher_priority=128,
                    use_datagram=(object_id % 2 == 0)  # Alternate between datagram and stream
                )
                
                await publisher.send_object(track_name, obj)
                logger.info(f"Sent: group={group_id}, object={object_id}")
                
                await asyncio.sleep(0.5)  # Small delay between objects
            
            logger.info(f"Completed group {group_id}")
        
        logger.info("All objects sent")
        
        # Keep publishing for a while
        await asyncio.sleep(5)
        
    except KeyboardInterrupt:
        logger.info("Publisher interrupted")
    finally:
        await publisher.unpublish(track_name, "Finished")
        publisher.disconnect()


async def run_subscriber():
    """Run the subscriber."""
    logger.info("Starting subscriber...")
    
    subscriber = MOQSubscriber(RELAY_HOST, RELAY_PORT)
    
    received_objects: list = []
    
    # Set handlers
    def on_connected():
        logger.info("Subscriber connected to relay")
    
    def on_disconnected():
        logger.info("Subscriber disconnected from relay")
    
    def on_object_received(obj: ReceivedObject):
        received_objects.append(obj)
        logger.info(f"Received: group={obj.group_id}, object={obj.object_id}, "
                   f"size={len(obj.payload)}, status={obj.object_status.name}")
        
        # Print payload for small objects
        if len(obj.payload) < 100:
            logger.info(f"  Payload: {obj.payload.decode('utf-8', errors='replace')}")
    
    def on_subscription_accepted(track_name: FullTrackName):
        logger.info(f"Subscription accepted: {track_name}")
    
    def on_subscription_rejected(track_name: FullTrackName, reason: str):
        logger.warning(f"Subscription rejected: {track_name}, reason: {reason}")
    
    subscriber.set_handlers(
        on_connected=on_connected,
        on_disconnected=on_disconnected,
        on_object_received=on_object_received,
        on_subscription_accepted=on_subscription_accepted,
        on_subscription_rejected=on_subscription_rejected
    )
    
    # Connect to relay
    if not await subscriber.connect():
        logger.error("Failed to connect to relay")
        return
    
    # Create track name
    track_name = FullTrackName(TRACK_NAMESPACE, TRACK_NAME)
    
    # Subscribe to the track
    if not await subscriber.subscribe(track_name):
        logger.error("Failed to subscribe to track")
        return
    
    logger.info(f"Subscribed to track: {track_name}")
    
    # Keep receiving for a while
    try:
        await asyncio.sleep(15)
    except KeyboardInterrupt:
        logger.info("Subscriber interrupted")
    finally:
        await subscriber.unsubscribe(track_name)
        subscriber.disconnect()
    
    # Print summary
    logger.info(f"\nSummary: Received {len(received_objects)} objects")
    groups = {}
    for obj in received_objects:
        if obj.group_id not in groups:
            groups[obj.group_id] = 0
        groups[obj.group_id] += 1
    
    for group_id, count in sorted(groups.items()):
        logger.info(f"  Group {group_id}: {count} objects")


async def run_demo():
    """Run a demo with publisher and subscriber in the same process."""
    logger.info("Running demo mode...")
    
    # Start relay
    relay = MOQRelay(
        host=RELAY_HOST,
        port=RELAY_PORT,
        cache_dir="/tmp/moq_cache_demo"
    )
    await relay.start()
    logger.info("Relay started")
    
    await asyncio.sleep(1)
    
    # Create track name
    track_name = FullTrackName(TRACK_NAMESPACE, TRACK_NAME)
    
    # Create publisher
    publisher = MOQPublisher(RELAY_HOST, RELAY_PORT)
    
    # Create subscriber
    subscriber = MOQSubscriber(RELAY_HOST, RELAY_PORT)
    
    received_count = [0]  # Use list to allow modification in closure
    
    def on_object_received(obj: ReceivedObject):
        received_count[0] += 1
        logger.info(f"[Subscriber] Received: group={obj.group_id}, object={obj.object_id}")
    
    subscriber.set_handlers(on_object_received=on_object_received)
    
    # Connect both
    if not await publisher.connect():
        logger.error("Publisher failed to connect")
        return
    logger.info("Publisher connected")
    
    if not await subscriber.connect():
        logger.error("Subscriber failed to connect")
        return
    logger.info("Subscriber connected")
    
    # Publish track
    await publisher.publish(track_name)
    logger.info("Track published")
    
    await asyncio.sleep(0.5)
    
    # Subscribe
    await subscriber.subscribe(track_name)
    logger.info("Subscribed to track")
    
    await asyncio.sleep(0.5)
    
    # Send some objects
    for i in range(1, 6):
        obj = PublishedObject(
            group_id=1,
            object_id=i,
            payload=f"Demo object {i}".encode()
        )
        await publisher.send_object(track_name, obj)
        logger.info(f"[Publisher] Sent object {i}")
        await asyncio.sleep(0.2)
    
    # Wait for all objects to be received
    await asyncio.sleep(2)
    
    logger.info(f"\nDemo complete! Received {received_count[0]} objects")
    
    # Cleanup
    publisher.disconnect()
    subscriber.disconnect()
    await relay.stop()


def main():
    """Main entry point."""
    if len(sys.argv) < 2:
        print("Usage:")
        print("  python basic_example.py relay      - Run relay")
        print("  python basic_example.py publisher  - Run publisher")
        print("  python basic_example.py subscriber - Run subscriber")
        print("  python basic_example.py demo       - Run demo (all in one)")
        sys.exit(1)
    
    mode = sys.argv[1].lower()
    
    if mode == "relay":
        asyncio.run(run_relay())
    elif mode == "publisher":
        asyncio.run(run_publisher())
    elif mode == "subscriber":
        asyncio.run(run_subscriber())
    elif mode == "demo":
        asyncio.run(run_demo())
    else:
        print(f"Unknown mode: {mode}")
        sys.exit(1)


if __name__ == "__main__":
    main()
