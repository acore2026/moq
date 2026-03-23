"""
Example: Reconnection and Resume
Demonstrates subscriber reconnection with resume capability
"""

import asyncio
import logging

from moq.transport.subscriber import MOQSubscriber, SubscriberConfig
from moq.transport.session import SessionConfig
from moq.protocol.constants import MOQFilterType


async def main():
    logging.basicConfig(level=logging.INFO)
    
    # Create subscriber with resume enabled
    session_config = SessionConfig(
        role=0x02,
        enable_cache=True,
        cache_dir=".resume_cache"
    )
    
    subscriber_config = SubscriberConfig(
        auto_resume=True,
        resume_buffer_size=1000
    )
    
    subscriber = MOQSubscriber(
        session_config=session_config,
        subscriber_config=subscriber_config
    )
    
    await subscriber.connect("127.0.0.1", 4433)
    
    # Subscribe with ABSOLUTE_START filter
    subscription = await subscriber.subscribe(
        "example", "live",
        track_name="video",
        filter_type=MOQFilterType.ABSOLUTE_START,
        start_group=0,
        start_object=0
    )
    
    print(f"Subscribed with ID: {subscription.id}")
    
    # Simulate: receive some objects
    received_count = 0
    async for obj in subscriber.receive_objects(subscription):
        print(f"Received: group={obj.group_id}, object={obj.object_id}")
        received_count += 1
        
        # Simulate disconnect after receiving 10 objects
        if received_count >= 10:
            print("Simulating disconnect...")
            break
    
    # Unsubscribe (saves resume state)
    await subscriber.unsubscribe(subscription)
    
    # Wait a bit
    await asyncio.sleep(2)
    
    print("Reconnecting...")
    
    # Reconnect and subscribe again (should resume from where we left off)
    subscription2 = await subscriber.subscribe(
        "example", "live",
        track_name="video",
        filter_type=MOQFilterType.LATEST_GROUP
    )
    
    print(f"Resumed subscription with ID: {subscription2.id}")
    
    # Continue receiving
    async for obj in subscriber.receive_objects(subscription2):
        print(f"Received (resumed): group={obj.group_id}, object={obj.object_id}")
        
        if obj.group_id > 5:
            break
    
    await subscriber.unsubscribe(subscription2)
    await subscriber.close()
    print("Done")


if __name__ == "__main__":
    asyncio.run(main())
