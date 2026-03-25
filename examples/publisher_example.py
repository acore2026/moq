#!/usr/bin/env python3
"""
MOQ Publisher Example - Demonstrates how to use the MOQPublisher class.

This example shows how to publish time updates using the public interface
from the moq package. The publisher uses QUIC datagrams for each timestamp.

Usage:
    python publisher_example.py

The publisher connects to the relay at 127.0.0.1:4443 and publishes
time updates on the track "time/updates".
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

# Import from the public moq interface
from moq import MOQPublisher, FullTrackName, PublishedObject
logger = logging.getLogger(__name__)

RELAY_HOST = "127.0.0.1"
RELAY_PORT = 4443


async def main():
    """Run the MOQ publisher example."""
    logger.info("Starting MOQ Publisher Example")
    
    # Create publisher using the public interface
    publisher = MOQPublisher(
        relay_host=RELAY_HOST,
        relay_port=RELAY_PORT
    )
    
    # Set up event handlers
    def on_connected():
        logger.info("Publisher connected to relay")
    
    def on_disconnected():
        logger.info("Publisher disconnected from relay")
    
    def on_publication_accepted(track_name):
        logger.info(f"Publication accepted: {track_name}")
    
    def on_publication_rejected(track_name, reason):
        logger.warning(f"Publication rejected: {track_name} - {reason}")
    
    publisher.set_handlers(
        on_connected=on_connected,
        on_disconnected=on_disconnected,
        on_publication_accepted=on_publication_accepted,
        on_publication_rejected=on_publication_rejected
    )
    
    # Connect to relay
    connected = await publisher.connect()
    if not connected:
        logger.error("Failed to connect to relay")
        return
    
    logger.info(f"Connected to relay at {RELAY_HOST}:{RELAY_PORT}")
    
    # Define track
    track_name = FullTrackName([b"time"], b"updates")
    
    # Publish the track
    try:
        published = await publisher.publish(track_name)
        if not published:
            logger.error(f"Failed to activate publication: {track_name}")
            publisher.disconnect()
            return
        logger.info(f"Publishing on track: {track_name}")
    except Exception as e:
        logger.error(f"Failed to publish: {e}")
        publisher.disconnect()
        return
    
    # Send time updates every second
    group_id = 1
    object_id = 1
    
    try:
        while True:
            # Create time message
            current_time = datetime.now().isoformat().encode()
            
            # Create published object
            obj = PublishedObject(
                group_id=group_id,
                object_id=object_id,
                payload=current_time,
                use_datagram=True
            )
            
            # Send the object
            await publisher.send_object(track_name, obj)
            logger.info(f"Sent time update: group={group_id}, object={object_id}")
            
            # Increment IDs
            object_id += 1
            if object_id > 10:
                object_id = 1
                group_id += 1
            
            # Wait before next update
            await asyncio.sleep(1.0)
            
    except KeyboardInterrupt:
        logger.info("Publisher stopped by user")
    except Exception as e:
        logger.error(f"Publisher error: {e}")
    finally:
        # Unpublish and disconnect
        try:
            await publisher.unpublish(track_name)
            logger.info(f"Unpublished track: {track_name}")
        except Exception as e:
            logger.error(f"Error unpublishing: {e}")
        
        publisher.disconnect()
        logger.info("Publisher disconnected")


if __name__ == "__main__":
    asyncio.run(main())
