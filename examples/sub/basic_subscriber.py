"""
Example: Basic Subscriber
Demonstrates basic subscription functionality
"""

import asyncio
import logging

from moq.transport.subscriber import MOQSubscriber, SubscriberConfig
from moq.transport.session import SessionConfig
from moq.protocol.constants import MOQFilterType


async def on_object_received(obj):
    """Callback for received objects"""
    print(f"Received: track={obj.track_alias}, group={obj.group_id}, object={obj.object_id}, size={len(obj.payload)}")


async def main():
    # Setup logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Create subscriber configuration
    session_config = SessionConfig(
        role=0x02,  # SUBSCRIBER
        enable_cache=True,
        cache_dir=".subscriber_cache"
    )
    
    subscriber_config = SubscriberConfig(
        subscriber_priority=128,
        auto_resume=True
    )
    
    # Create subscriber
    subscriber = MOQSubscriber(
        session_config=session_config,
        subscriber_config=subscriber_config,
        on_object=on_object_received,
        on_status=lambda event, data: print(f"Status: {event} - {data}")
    )
    
    # Connect to relay
    await subscriber.connect("127.0.0.1", 4433)
    print("Connected to relay")
    
    # Subscribe to track
    subscription = await subscriber.subscribe(
        "example", "live",
        track_name="video",
        filter_type=MOQFilterType.LATEST_GROUP,
        on_object=on_object_received
    )
    
    print(f"Subscribed with ID: {subscription.id}")
    
    # Receive objects for 30 seconds
    print("Receiving objects...")
    
    try:
        async for obj in subscriber.receive_objects(subscription):
            print(f"Stream: group={obj.group_id}, object={obj.object_id}")
            
    except asyncio.TimeoutError:
        print("Receive timeout")
    
    # Get stats
    stats = await subscriber.get_stats()
    print(f"Subscriber stats: {stats}")
    
    # Unsubscribe
    await subscriber.unsubscribe(subscription)
    
    # Close
    await subscriber.close()
    print("Subscriber closed")


if __name__ == "__main__":
    asyncio.run(main())
