"""
Example: Basic Publisher
Demonstrates basic publishing functionality
"""

import asyncio
import logging

from moq.transport.publisher import MOQPublisher, PublisherConfig
from moq.transport.session import SessionConfig
from moq.protocol.objects import MOQTrack


async def main():
    # Setup logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Create publisher configuration
    session_config = SessionConfig(
        role=0x01,  # PUBLISHER
        enable_cache=True,
        cache_dir=".publisher_cache"
    )
    
    publisher_config = PublisherConfig(
        delivery_preference=0x03,  # SUBGROUP
        publisher_priority=128
    )
    
    # Create publisher
    publisher = MOQPublisher(
        session_config=session_config,
        publisher_config=publisher_config,
        on_status=lambda event, data: print(f"Status: {event} - {data}")
    )
    
    # Connect to relay
    await publisher.connect("127.0.0.1", 4433)
    print("Connected to relay")
    
    # Announce namespace
    await publisher.announce_namespace("example", "live")
    print("Announced namespace: example/live")
    
    # Create track
    track = MOQTrack(
        namespace=("example", "live"),
        name="video",
        publisher_priority=128
    )
    
    # Publish some objects
    for group_id in range(5):
        for object_id in range(10):
            data = f"Video frame group={group_id}, object={object_id}".encode()
            
            success = await publisher.publish_object(
                track=track,
                group_id=group_id,
                object_id=object_id,
                data=data,
                send_order=group_id * 10 + object_id
            )
            
            if success:
                print(f"Published: group={group_id}, object={object_id}")
            
            await asyncio.sleep(0.1)
    
    # Get stats
    stats = await publisher.get_stats()
    print(f"Publisher stats: {stats}")
    
    # Unannounce
    await publisher.unannounce_namespace("example", "live")
    
    # Close
    await publisher.close()
    print("Publisher closed")


if __name__ == "__main__":
    asyncio.run(main())
