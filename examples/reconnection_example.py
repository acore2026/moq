#!/usr/bin/env python3
"""
Reconnection Example

This example demonstrates how the MOQ protocol handles connection migration
and reconnection with data continuation.

Usage:
    Terminal 1: python reconnection_example.py relay
    Terminal 2: python reconnection_example.py publisher
    Terminal 3: python reconnection_example.py subscriber
"""

import asyncio
import logging
import random

try:
    from examples._bootstrap import ensure_repo_root, setup_logging
except ImportError:  # pragma: no cover - direct script execution fallback
    from _bootstrap import ensure_repo_root, setup_logging

ensure_repo_root()
setup_logging()
logger = logging.getLogger(__name__)

from moq.encoding import FullTrackName
from moq.pub import MOQPublisher, PublishedObject
from moq.sub import MOQSubscriber, ReceivedObject
from moq.relay import MOQRelay


RELAY_HOST = "127.0.0.1"
RELAY_PORT = 4444

TRACK_NAMESPACE = [b"reconnect", b"test"]
TRACK_NAME = b"stream"


async def run_relay():
    """Run relay with larger cache for reconnection testing."""
    logger.info("Starting relay with reconnection support...")
    
    relay = MOQRelay(
        host=RELAY_HOST,
        port=RELAY_PORT,
        cache_dir="/tmp/moq_reconnect_cache",
        max_memory_cache=200 * 1024 * 1024,  # 200MB
        max_disk_cache=500 * 1024 * 1024     # 500MB
    )
    
    await relay.start()
    logger.info(f"Relay running on {RELAY_HOST}:{RELAY_PORT}")
    
    try:
        while True:
            await asyncio.sleep(5)
            stats = relay.get_cache_stats()
            logger.info(f"Cache stats: hits={stats['hits']}, misses={stats['misses']}, "
                       f"hit_rate={stats['hit_rate']:.2%}")
    except KeyboardInterrupt:
        logger.info("Shutting down relay...")
    finally:
        await relay.stop()


async def run_publisher():
    """Run publisher that sends continuous stream."""
    logger.info("Starting publisher...")
    
    publisher = MOQPublisher(RELAY_HOST, RELAY_PORT)
    
    if not await publisher.connect():
        logger.error("Failed to connect")
        return
    
    track_name = FullTrackName(TRACK_NAMESPACE, TRACK_NAME)
    
    if not await publisher.publish(track_name):
        logger.error("Failed to publish")
        return
    
    logger.info("Publishing started, sending continuous stream...")
    
    group_id = 1
    object_id = 1
    
    try:
        while True:
            # Create object with sequence number for tracking
            payload = f"Group:{group_id},Object:{object_id},Seq:{group_id * 1000 + object_id}".encode()
            
            obj = PublishedObject(
                group_id=group_id,
                object_id=object_id,
                payload=payload,
                publisher_priority=128
            )
            
            await publisher.send_object(track_name, obj)
            logger.info(f"Published: group={group_id}, object={object_id}")
            
            object_id += 1
            if object_id > 100:  # New group every 100 objects
                object_id = 1
                group_id += 1
            
            await asyncio.sleep(1)  # 1 object per second
            
    except KeyboardInterrupt:
        logger.info("Publisher stopping...")
    finally:
        await publisher.unpublish(track_name)
        publisher.disconnect()


async def run_subscriber():
    """
    Run subscriber with simulated disconnections.
    Demonstrates connection migration and data continuation.
    """
    logger.info("Starting subscriber with reconnection simulation...")
    
    track_name = FullTrackName(TRACK_NAMESPACE, TRACK_NAME)
    
    # Track received objects
    received_objects = set()
    last_sequence = 0
    
    async def subscribe_and_receive(duration: float, disconnect_after: bool = False):
        """Subscribe and receive for a duration, optionally disconnecting."""
        nonlocal last_sequence
        
        subscriber = MOQSubscriber(RELAY_HOST, RELAY_PORT)
        
        def on_object_received(obj: ReceivedObject):
            nonlocal last_sequence
            
            # Parse sequence number from payload
            try:
                payload_str = obj.payload.decode('utf-8')
                parts = payload_str.split(',')
                seq = int(parts[2].split(':')[1])
                
                received_objects.add(seq)
                
                # Check for gaps (lost objects during disconnect)
                if seq > last_sequence + 1 and last_sequence > 0:
                    gap = seq - last_sequence - 1
                    logger.warning(f"GAP DETECTED: Missing {gap} objects between {last_sequence} and {seq}")
                
                last_sequence = max(last_sequence, seq)
                
                logger.info(f"[Received] group={obj.group_id}, object={obj.object_id}, seq={seq}, "
                           f"total_unique={len(received_objects)}")
                
            except Exception as e:
                logger.error(f"Failed to parse object: {e}")
        
        subscriber.set_handlers(on_object_received=on_object_received)
        
        if not await subscriber.connect():
            logger.error("Failed to connect")
            return False
        
        # Subscribe - request from last known position
        start_group = (last_sequence // 1000) + 1 if last_sequence > 0 else None
        start_object = (last_sequence % 1000) + 1 if last_sequence > 0 else None
        
        logger.info(f"Subscribing from group={start_group}, object={start_object}")
        
        if not await subscriber.subscribe(track_name, start_group=start_group, start_object=start_object):
            logger.error("Failed to subscribe")
            subscriber.disconnect()
            return False
        
        try:
            await asyncio.sleep(duration)
        except asyncio.CancelledError:
            pass
        
        if disconnect_after:
            logger.info("Simulating disconnect...")
            subscriber.disconnect()
            return True
        else:
            # Keep receiving
            while True:
                await asyncio.sleep(1)
    
    try:
        # First connection - receive for 5 seconds
        logger.info("=== First Connection ===")
        await subscribe_and_receive(5, disconnect_after=True)
        
        logger.info("Disconnected. Waiting 3 seconds before reconnect...")
        await asyncio.sleep(3)
        
        # Reconnect - should continue from where we left off
        logger.info("=== Reconnecting ===")
        await subscribe_and_receive(10, disconnect_after=True)
        
        logger.info("Disconnected. Waiting 5 seconds before reconnect...")
        await asyncio.sleep(5)
        
        # Second reconnect
        logger.info("=== Second Reconnect ===")
        await subscribe_and_receive(0, disconnect_after=False)  # Run until interrupted
        
    except KeyboardInterrupt:
        logger.info("Subscriber stopping...")
    
    logger.info(f"\nFinal Statistics:")
    logger.info(f"  Total unique objects received: {len(received_objects)}")
    logger.info(f"  Highest sequence number: {last_sequence}")
    if last_sequence > 0:
        expected_count = last_sequence
        loss_rate = 1 - (len(received_objects) / expected_count)
        logger.info(f"  Loss rate: {loss_rate:.2%}")


async def run_demo():
    """Run a quick demo showing reconnection."""
    logger.info("Running reconnection demo...")
    
    # Start relay
    relay = MOQRelay(
        host=RELAY_HOST,
        port=RELAY_PORT,
        cache_dir="/tmp/moq_reconnect_demo",
        max_memory_cache=100 * 1024 * 1024,
        max_disk_cache=200 * 1024 * 1024
    )
    await relay.start()
    logger.info("Relay started")
    
    await asyncio.sleep(1)
    
    track_name = FullTrackName(TRACK_NAMESPACE, TRACK_NAME)
    
    # Create and connect publisher
    publisher = MOQPublisher(RELAY_HOST, RELAY_PORT)
    await publisher.connect()
    await publisher.publish(track_name)
    logger.info("Publisher connected and publishing")
    
    # Start publisher task
    async def publish_loop():
        seq = 1
        while True:
            obj = PublishedObject(
                group_id=seq // 10 + 1,
                object_id=seq % 10 + 1,
                payload=f"Object sequence {seq}".encode()
            )
            await publisher.send_object(track_name, obj)
            seq += 1
            await asyncio.sleep(0.5)
    
    publish_task = asyncio.create_task(publish_loop())
    
    # First subscriber connection
    logger.info("\n=== First Connection ===")
    subscriber1 = MOQSubscriber(RELAY_HOST, RELAY_PORT)
    
    received_1 = []
    def on_obj_1(obj: ReceivedObject):
        received_1.append(obj)
        logger.info(f"[Conn1] Received object {len(received_1)}")
    
    subscriber1.set_handlers(on_object_received=on_obj_1)
    await subscriber1.connect()
    await subscriber1.subscribe(track_name)
    
    await asyncio.sleep(3)
    
    # Disconnect
    logger.info("\n=== Disconnecting ===")
    subscriber1.disconnect()
    logger.info(f"First connection received {len(received_1)} objects")
    
    await asyncio.sleep(2)
    
    # Reconnect
    logger.info("\n=== Reconnecting ===")
    subscriber2 = MOQSubscriber(RELAY_HOST, RELAY_PORT)
    
    received_2 = []
    def on_obj_2(obj: ReceivedObject):
        received_2.append(obj)
        logger.info(f"[Conn2] Received object {len(received_2)}")
    
    subscriber2.set_handlers(on_object_received=on_obj_2)
    await subscriber2.connect()
    await subscriber2.subscribe(track_name)
    
    await asyncio.sleep(3)
    
    logger.info("\n=== Demo Results ===")
    logger.info(f"First connection objects: {len(received_1)}")
    logger.info(f"Second connection objects: {len(received_2)}")
    logger.info(f"Total unique objects received: {len(received_1) + len(received_2)}")
    
    # Cleanup
    publish_task.cancel()
    try:
        await publish_task
    except asyncio.CancelledError:
        pass
    
    publisher.disconnect()
    subscriber2.disconnect()
    await relay.stop()


def main():
    if len(sys.argv) < 2:
        print("Usage:")
        print("  python reconnection_example.py relay      - Run relay")
        print("  python reconnection_example.py publisher  - Run publisher")
        print("  python reconnection_example.py subscriber - Run subscriber with reconnections")
        print("  python reconnection_example.py demo       - Run demo")
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
