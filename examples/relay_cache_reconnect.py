"""
Example: Relay Cache Reconnection
Demonstrates relay cache with subscriber reconnection
"""

import asyncio
import logging

from moq.transport.publisher import MOQPublisher
from moq.transport.subscriber import MOQSubscriber
from moq.transport.relay import MOQRelay
from moq.transport.session import SessionConfig
from moq.protocol.objects import MOQTrack
from moq.protocol.constants import MOQFilterType


async def run_relay():
    """Run relay in background"""
    session_config = SessionConfig(
        role=0x03,
        enable_cache=True,
        max_cache_memory=50 * 1024 * 1024,
        max_cache_disk=200 * 1024 * 1024,
        cache_dir=".test_relay_cache"
    )
    
    from moq.transport.relay import RelayConfig
    relay_config = RelayConfig(
        host="127.0.0.1",
        port=4433,
        cache_enabled=True
    )
    
    relay = MOQRelay(
        session_config=session_config,
        relay_config=relay_config
    )
    
    await relay.start()
    print("Relay started")
    
    # Keep running
    try:
        while True:
            await asyncio.sleep(1)
            stats = await relay.get_stats()
            print(f"Relay: {stats}")
    except asyncio.CancelledError:
        await relay.stop()


async def run_publisher():
    """Run publisher"""
    await asyncio.sleep(1)  # Wait for relay
    
    session_config = SessionConfig(
        role=0x01,
        enable_cache=True,
        cache_dir=".test_pub_cache"
    )
    
    publisher = MOQPublisher(session_config=session_config)
    await publisher.connect("127.0.0.1", 4433)
    await publisher.announce_namespace("test", "stream")
    
    track = MOQTrack(
        namespace=("test", "stream"),
        name="video"
    )
    
    # Publish objects continuously
    group_id = 0
    while True:
        for object_id in range(5):
            data = f"Frame G{group_id}O{object_id}".encode() * 100  # Make it bigger
            await publisher.publish_object(
                track=track,
                group_id=group_id,
                object_id=object_id,
                data=data
            )
            print(f"Published: G{group_id}O{object_id}")
            await asyncio.sleep(0.2)
        
        group_id += 1


async def run_subscriber():
    """Run subscriber with reconnect"""
    await asyncio.sleep(2)  # Wait for publisher
    
    session_config = SessionConfig(
        role=0x02,
        enable_cache=True,
        cache_dir=".test_sub_cache"
    )
    
    from moq.transport.subscriber import SubscriberConfig
    subscriber_config = SubscriberConfig(
        auto_resume=True
    )
    
    subscriber = MOQSubscriber(
        session_config=session_config,
        subscriber_config=subscriber_config
    )
    
    await subscriber.connect("127.0.0.1", 4433)
    
    # First subscription
    subscription = await subscriber.subscribe(
        "test", "stream",
        track_name="video",
        filter_type=MOQFilterType.ABSOLUTE_START,
        start_group=0,
        start_object=0
    )
    
    print("Subscribed (first)")
    
    count = 0
    async for obj in subscriber.receive_objects(subscription):
        print(f"First sub: G{obj.group_id}O{obj.object_id}")
        count += 1
        
        if count >= 8:
            print("Disconnecting for reconnection test...")
            break
    
    await subscriber.unsubscribe(subscription)
    await asyncio.sleep(3)
    
    # Reconnect and resume
    print("Reconnecting...")
    subscription2 = await subscriber.subscribe(
        "test", "stream",
        track_name="video",
        filter_type=MOQFilterType.LATEST_GROUP
    )
    
    print("Subscribed (resumed)")
    
    count = 0
    async for obj in subscriber.receive_objects(subscription2):
        print(f"Resumed sub: G{obj.group_id}O{obj.object_id}")
        count += 1
        
        if count >= 10:
            break
    
    await subscriber.unsubscribe(subscription2)
    await subscriber.close()
    print("Subscriber done")


async def main():
    logging.basicConfig(level=logging.INFO)
    
    # Run all components
    relay_task = asyncio.create_task(run_relay())
    pub_task = asyncio.create_task(run_publisher())
    sub_task = asyncio.create_task(run_subscriber())
    
    # Wait for subscriber to complete
    await sub_task
    
    # Cancel others
    relay_task.cancel()
    pub_task.cancel()
    
    try:
        await relay_task
        await pub_task
    except asyncio.CancelledError:
        pass
    
    print("Test completed")


if __name__ == "__main__":
    asyncio.run(main())
