#!/usr/bin/env python3
"""
MOQ Subscriber Example - Demonstrates how to use the MOQSubscriber class.

This example shows how to subscribe to tracks and receive objects using
the public interface from the moq package. The subscriber uses QUIC as
the underlying transport.

Usage:
    python subscriber_example.py

The subscriber connects to the relay at 127.0.0.1:4443 and subscribes
to the track "time/updates", printing all received messages.
"""

import asyncio
import logging

try:
    from examples._bootstrap import ensure_repo_root, setup_logging
except ImportError:  # pragma: no cover - direct script execution fallback
    from _bootstrap import ensure_repo_root, setup_logging

ensure_repo_root()
setup_logging()

# Import from the public moq interface
from moq import MOQSubscriber, FullTrackName, ReceivedObject
logger = logging.getLogger(__name__)

RELAY_HOST = "127.0.0.1"
RELAY_PORT = 4443


async def main():
    """Run the MOQ subscriber example."""
    logger.info("Starting MOQ Subscriber Example")
    
    # Create subscriber using the public interface
    subscriber = MOQSubscriber(
        relay_host=RELAY_HOST,
        relay_port=RELAY_PORT
    )
    
    # Set up event handlers
    def on_connected():
        logger.info("Subscriber connected to relay")
    
    def on_disconnected():
        logger.info("Subscriber disconnected from relay")
    
    def on_object_received(obj: ReceivedObject):
        """Handle received object."""
        try:
            payload_str = obj.payload.decode('utf-8')
            logger.info(f"Received: group={obj.group_id}, object={obj.object_id}, "
                       f"data='{payload_str}'")
        except:
            logger.info(f"Received: group={obj.group_id}, object={obj.object_id}, "
                       f"data={obj.payload.hex()}")
    
    def on_subscription_accepted(track_name):
        logger.info(f"Subscription accepted: {track_name}")
    
    def on_subscription_rejected(track_name, reason):
        logger.warning(f"Subscription rejected: {track_name} - {reason}")
    
    subscriber.set_handlers(
        on_connected=on_connected,
        on_disconnected=on_disconnected,
        on_object_received=on_object_received,
        on_subscription_accepted=on_subscription_accepted,
        on_subscription_rejected=on_subscription_rejected
    )
    
    # Connect to relay
    connected = await subscriber.connect()
    if not connected:
        logger.error("Failed to connect to relay")
        return
    
    logger.info(f"Connected to relay at {RELAY_HOST}:{RELAY_PORT}")
    
    # Define track
    track_name = FullTrackName([b"time"], b"updates")
    
    # Subscribe to the track
    try:
        await subscriber.subscribe(track_name)
        logger.info(f"Subscribed to track: {track_name}")
    except Exception as e:
        logger.error(f"Failed to subscribe: {e}")
        subscriber.disconnect()
        return
    
    # Keep running until interrupted
    try:
        logger.info("Listening for messages... (Press Ctrl+C to stop)")
        while True:
            await asyncio.sleep(1.0)
    except KeyboardInterrupt:
        logger.info("Subscriber stopped by user")
    except Exception as e:
        logger.error(f"Subscriber error: {e}")
    finally:
        # Unsubscribe and disconnect
        try:
            await subscriber.unsubscribe(track_name)
            logger.info(f"Unsubscribed from track: {track_name}")
        except Exception as e:
            logger.error(f"Error unsubscribing: {e}")
        
        subscriber.disconnect()
        logger.info("Subscriber disconnected")


if __name__ == "__main__":
    asyncio.run(main())
