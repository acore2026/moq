#!/usr/bin/env python3
"""
Complete Integration Example

This example demonstrates a complete workflow using MOQ Transport
as an external library. It shows:
1. How to install and import the package
2. How to create a complete publisher-subscriber workflow
3. How to use fetch mode
4. Error handling and best practices

Prerequisites:
    pip install /path/to/moq-py

Usage:
    # Terminal 1: Start relay
    python -m cases.relay
    
    # Terminal 2: Run this example
    python examples/integration_example.py
"""

import asyncio
import logging
import sys
from typing import Optional

# Import MOQ Transport as an external library would
from moq import MOQPublisher, MOQSubscriber, FullTrackName
from moq import PublishedObject, ReceivedObject
from moq import FetchMessage, FetchOkMessage  # For advanced usage

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class MOQApplication:
    """
    Example application showing how to integrate MOQ Transport
    into your own project.
    """
    
    def __init__(self, relay_host: str = "127.0.0.1", relay_port: int = 4443):
        self.relay_host = relay_host
        self.relay_port = relay_port
        self.publisher: Optional[MOQPublisher] = None
        self.subscriber: Optional[MOQSubscriber] = None
        self.track_name = FullTrackName(
            namespace=[b"demo", b"app"],
            name=b"messages"
        )
        self._received_count = 0
        self._published_count = 0
    
    async def setup_publisher(self) -> bool:
        """Initialize and connect the publisher."""
        logger.info("Setting up publisher...")
        
        try:
            self.publisher = MOQPublisher(
                host=self.relay_host,
                port=self.relay_port
            )
            
            connected = await self.publisher.connect()
            if not connected:
                logger.error("Failed to connect publisher")
                return False
            
            # Publish track
            success = await self.publisher.publish(self.track_name)
            if not success:
                logger.error("Failed to publish track")
                return False
            
            logger.info("Publisher ready!")
            return True
            
        except Exception as e:
            logger.error(f"Publisher setup error: {e}")
            return False
    
    async def setup_subscriber(self) -> bool:
        """Initialize and connect the subscriber."""
        logger.info("Setting up subscriber...")
        
        try:
            self.subscriber = MOQSubscriber(
                host=self.relay_host,
                port=self.relay_port
            )
            
            # Set up handlers
            self.subscriber.set_handlers(
                on_connected=self._on_subscriber_connected,
                on_disconnected=self._on_subscriber_disconnected,
                on_object_received=self._on_object_received
            )
            
            connected = await self.subscriber.connect()
            if not connected:
                logger.error("Failed to connect subscriber")
                return False
            
            # Subscribe to track
            success = await self.subscriber.subscribe(self.track_name)
            if not success:
                logger.error("Failed to subscribe")
                return False
            
            logger.info("Subscriber ready!")
            return True
            
        except Exception as e:
            logger.error(f"Subscriber setup error: {e}")
            return False
    
    def _on_subscriber_connected(self):
        """Handler for subscriber connection."""
        logger.info("✓ Subscriber connected to relay")
    
    def _on_subscriber_disconnected(self):
        """Handler for subscriber disconnection."""
        logger.info("✗ Subscriber disconnected from relay")
    
    def _on_object_received(self, obj: ReceivedObject):
        """Handler for received objects."""
        try:
            message = obj.payload.decode('utf-8')
            self._received_count += 1
            logger.info(f"📥 Received [{self._received_count}]: "
                       f"group={obj.group_id}, object={obj.object_id}, "
                       f"data='{message}'")
        except Exception as e:
            logger.error(f"Failed to process received object: {e}")
    
    async def publish_messages(self, count: int = 10, delay: float = 1.0):
        """Publish messages to the track."""
        if not self.publisher:
            logger.error("Publisher not initialized")
            return
        
        logger.info(f"Publishing {count} messages...")
        
        for i in range(count):
            try:
                obj = PublishedObject(
                    group_id=i,
                    object_id=0,
                    payload=f"Hello from message {i}!".encode('utf-8'),
                    publisher_priority=128
                )
                
                await self.publisher.send_object(self.track_name, obj)
                self._published_count += 1
                logger.info(f"📤 Published [{self._published_count}]: group={i}")
                
                await asyncio.sleep(delay)
                
            except Exception as e:
                logger.error(f"Failed to publish message {i}: {e}")
    
    async def fetch_historical_messages(self):
        """Fetch historical messages using fetch mode."""
        if not self.subscriber:
            logger.error("Subscriber not initialized")
            return
        
        logger.info("Fetching historical messages...")
        
        try:
            # Fetch from beginning (0, 0) until latest
            request_id = await self.subscriber.fetch(
                track_name=self.track_name
                # Using defaults: start_group=0, start_object=0, 
                # end_group=None, end_object=None (until latest)
            )
            
            if request_id < 0:
                logger.error("Failed to initiate fetch")
                return
            
            logger.info(f"Fetch initiated with request_id={request_id}")
            
            # Wait a bit for fetch to complete
            await asyncio.sleep(3)
            
        except Exception as e:
            logger.error(f"Fetch error: {e}")
    
    async def run_demo(self):
        """Run a complete demo workflow."""
        logger.info("=" * 60)
        logger.info("MOQ Transport Integration Demo")
        logger.info("=" * 60)
        
        # Setup phase
        pub_ok = await self.setup_publisher()
        sub_ok = await self.setup_subscriber()
        
        if not pub_ok or not sub_ok:
            logger.error("Setup failed!")
            return
        
        logger.info("\n" + "=" * 60)
        logger.info("Starting message exchange...")
        logger.info("=" * 60)
        
        # Publish messages
        publish_task = asyncio.create_task(
            self.publish_messages(count=5, delay=1.0)
        )
        
        # Wait for publishing to complete
        await publish_task
        
        # Give some time for messages to be received
        await asyncio.sleep(2)
        
        logger.info("\n" + "=" * 60)
        logger.info("Fetching historical messages...")
        logger.info("=" * 60)
        
        # Fetch historical messages
        await self.fetch_historical_messages()
        
        # Give time for fetch to complete
        await asyncio.sleep(3)
        
        # Summary
        logger.info("\n" + "=" * 60)
        logger.info("Demo Summary")
        logger.info("=" * 60)
        logger.info(f"Total published: {self._published_count}")
        logger.info(f"Total received: {self._received_count}")
        
        # Cleanup
        logger.info("\nCleaning up...")
        await self.cleanup()
        logger.info("Demo complete!")
    
    async def cleanup(self):
        """Clean up resources."""
        if self.publisher:
            try:
                await self.publisher.unpublish(self.track_name)
                self.publisher.disconnect()
                logger.info("Publisher disconnected")
            except Exception as e:
                logger.error(f"Publisher cleanup error: {e}")
        
        if self.subscriber:
            try:
                await self.subscriber.unsubscribe(self.track_name)
                self.subscriber.disconnect()
                logger.info("Subscriber disconnected")
            except Exception as e:
                logger.error(f"Subscriber cleanup error: {e}")


async def main():
    """Main entry point."""
    app = MOQApplication(
        relay_host="127.0.0.1",
        relay_port=4443
    )
    
    try:
        await app.run_demo()
    except KeyboardInterrupt:
        logger.info("\nInterrupted by user")
    finally:
        await app.cleanup()


if __name__ == "__main__":
    asyncio.run(main())
