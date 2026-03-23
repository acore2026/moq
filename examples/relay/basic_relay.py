"""
Example: Basic Relay Server
Demonstrates relay functionality
"""

import asyncio
import logging
import signal

from moq.transport.relay import MOQRelay, RelayConfig
from moq.transport.session import SessionConfig


async def on_relay_event(event: str, data: dict):
    """Callback for relay events"""
    print(f"Relay event: {event} - {data}")


async def main():
    # Setup logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Create relay configuration
    session_config = SessionConfig(
        role=0x03,  # PUB_SUB
        enable_cache=True,
        max_cache_memory=100 * 1024 * 1024,  # 100MB
        max_cache_disk=1024 * 1024 * 1024,    # 1GB
        cache_dir=".relay_cache"
    )
    
    relay_config = RelayConfig(
        host="0.0.0.0",
        port=4433,
        max_connections=1000,
        max_subscriptions_per_track=100,
        cache_enabled=True
    )
    
    # Create relay
    relay = MOQRelay(
        session_config=session_config,
        relay_config=relay_config,
        on_event=on_relay_event
    )
    
    # Start relay
    await relay.start()
    
    print(f"Relay started on {relay_config.host}:{relay_config.port}")
    print("Press Ctrl+C to stop")
    
    # Setup signal handler
    loop = asyncio.get_event_loop()
    
    def signal_handler():
        print("\nShutting down...")
        asyncio.create_task(relay.stop())
    
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, signal_handler)
    
    # Print stats periodically
    try:
        while True:
            await asyncio.sleep(10)
            stats = await relay.get_stats()
            print(f"Relay stats: {stats}")
    except asyncio.CancelledError:
        pass
    
    await relay.stop()
    print("Relay stopped")


if __name__ == "__main__":
    asyncio.run(main())
