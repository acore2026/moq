#!/usr/bin/env python3
"""
MOQ Fetch Example - Fetches historical objects from a track

This example demonstrates how to use the fetch mode to retrieve
specific objects from a track within a given range.

Usage:
    python fetch_example.py

The subscriber connects to the relay at 127.0.0.1:4443 and fetches
objects from the track "time/updates" within a specified range.
"""

import asyncio
import logging

try:
    from examples._bootstrap import ensure_repo_root, setup_logging
except ImportError:  # pragma: no cover - direct script execution fallback
    from _bootstrap import ensure_repo_root, setup_logging

ensure_repo_root()
setup_logging()
from moq.sub import MOQSubscriber
from moq.encoding import FullTrackName
logger = logging.getLogger(__name__)

RELAY_HOST = "127.0.0.1"
RELAY_PORT = 4443
TRACK_NAMESPACE = [b"time"]
TRACK_NAME = b"updates"


async def main():
    """Main function demonstrating fetch mode."""
    logger.info("Starting fetch example...")
    
    # Create subscriber
    subscriber = MOQSubscriber(RELAY_HOST, RELAY_PORT)
    
    # Set up handlers
    def on_connected():
        logger.info("Connected to relay!")
    
    def on_disconnected():
        logger.info("Disconnected from relay")
    
    def on_object_received(obj):
        """Handle received object from fetch."""
        try:
            message = obj.payload.decode('utf-8')
            print(f"[Fetched] Group={obj.group_id}, Object={obj.object_id}: {message}")
        except Exception as e:
            logger.error(f"Failed to decode object: {e}")
    
    subscriber.set_handlers(
        on_connected=on_connected,
        on_disconnected=on_disconnected,
        on_object_received=on_object_received
    )
    
    # Connect to relay
    if not await subscriber.connect():
        logger.error("Failed to connect to relay")
        return
    
    logger.info("Connected! Fetching objects...")
    
    # Define track name
    track_name = FullTrackName(TRACK_NAMESPACE, TRACK_NAME)
    
    # Fetch objects from group 0, object 0 to group 5, object 10
    # This requests a specific range of historical objects
    start_group = 0
    start_object = 0
    end_group = 5
    end_object = 10
    
    logger.info(f"Fetching objects from group {start_group}:{start_object} to {end_group}:{end_object}")
    
    request_id = await subscriber.fetch(
        track_name=track_name,
        start_group=start_group,
        start_object=start_object,
        end_group=end_group,
        end_object=end_object,
        subscriber_priority=128
    )
    
    if request_id < 0:
        logger.error("Failed to initiate fetch")
        subscriber.disconnect()
        return
    
    logger.info(f"Fetch initiated with request_id={request_id}")
    logger.info("Waiting for fetch response... (Press Ctrl+C to stop)")
    
    # Keep the connection alive to receive fetch response
    try:
        while True:
            await asyncio.sleep(1)
    except KeyboardInterrupt:
        logger.info("Fetch stopped by user")
    finally:
        subscriber.disconnect()
        logger.info("Fetch example complete")


if __name__ == "__main__":
    asyncio.run(main())
