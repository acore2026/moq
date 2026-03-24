#!/usr/bin/env python3
"""
MOQ Publisher Case - Sends current system time

This publisher sends the current system time to a relay every second.

Usage:
    python pub.py

The publisher connects to the relay at 127.0.0.1:4443 and publishes
time updates on the track "time/updates".
"""

import asyncio
import sys
import logging
from datetime import datetime

sys.path.insert(0, '/home/acn/cxr/moq-py')
from moq.encoding import FullTrackName
from moq.messages import (
    PublishMessage, PublishOkMessage, ObjectDatagram, ObjectHeader, SubscribeFilter, GroupOrder
)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

RELAY_HOST = "127.0.0.1"
RELAY_PORT = 4443
TRACK_NAMESPACE = [b"time"]
TRACK_NAME = b"updates"


class SimplePublisher:
    """Simple TCP-based MOQ publisher."""
    
    def __init__(self, host: str, port: int):
        self.host = host
        self.port = port
        self.reader: asyncio.StreamReader = None
        self.writer: asyncio.StreamWriter = None
        self.track_alias_counter = 1
        self.request_id_counter = 1
        self.publications: dict = {}
    
    async def connect(self) -> bool:
        """Connect to the relay."""
        try:
            self.reader, self.writer = await asyncio.open_connection(
                self.host, self.port
            )
            logger.info(f"Connected to relay at {self.host}:{self.port}")
            return True
        except Exception as e:
            logger.error(f"Failed to connect: {e}")
            return False
    
    async def publish(self, track_name: FullTrackName) -> bool:
        """Publish a track."""
        track_alias = self.track_alias_counter
        self.track_alias_counter += 1
        
        request_id = self.request_id_counter
        self.request_id_counter += 1
        
        # Send PUBLISH message
        msg = PublishMessage(
            request_id=request_id,
            track_alias=track_alias,
            full_track_name=track_name,
            parameters=None
        )
        
        msg_data = msg.encode()
        logger.debug(f"Sending PUBLISH: {len(msg_data)} bytes")
        await self._send_message(msg_data)
        
        # Wait for PUBLISH_OK
        try:
            response_data = await asyncio.wait_for(self._read_message(), timeout=5.0)
            if response_data:
                logger.debug(f"Received response: {len(response_data)} bytes")
                try:
                    from moq.messages import decode_control_message
                    response, _ = decode_control_message(response_data)
                    logger.debug(f"Response type: {type(response).__name__}")
                    if isinstance(response, PublishOkMessage) and response.request_id == request_id:
                        self.publications[track_name] = {
                            'track_alias': track_alias,
                            'request_id': request_id
                        }
                        logger.info(f"Publication accepted: {track_name}")
                        return True
                except Exception as e:
                    logger.error(f"Error decoding response: {e}")
        except asyncio.TimeoutError:
            logger.error("Timeout waiting for PUBLISH_OK")
        
        return False
    
    async def send_object(self, track_name: FullTrackName, payload: bytes, 
                          group_id: int = 1, object_id: int = 1):
        """Send an object."""
        if track_name not in self.publications:
            logger.error(f"Not publishing track: {track_name}")
            return
        
        track_alias = self.publications[track_name]['track_alias']
        
        header = ObjectHeader(
            track_alias=track_alias,
            group_id=group_id,
            object_id=object_id,
            publisher_priority=128
        )
        
        obj = ObjectDatagram(header=header, payload=payload)
        await self._send_message(obj.encode())
    
    async def _send_message(self, data: bytes):
        """Send a message with length prefix."""
        length = len(data).to_bytes(4, 'big')
        self.writer.write(length + data)
        await self.writer.drain()
    
    async def _read_message(self) -> bytes:
        """Read a message with length prefix."""
        length_data = await self.reader.read(4)
        if not length_data or len(length_data) < 4:
            return None
        
        msg_length = int.from_bytes(length_data, 'big')
        data = await self.reader.read(msg_length)
        return data if len(data) == msg_length else None
    
    def disconnect(self):
        """Disconnect from the relay."""
        if self.writer:
            self.writer.close()
        logger.info("Disconnected from relay")


async def main():
    logger.info("Starting time publisher...")
    
    publisher = SimplePublisher(RELAY_HOST, RELAY_PORT)
    
    if not await publisher.connect():
        logger.error("Failed to connect to relay")
        return
    
    track_name = FullTrackName(TRACK_NAMESPACE, TRACK_NAME)
    
    if not await publisher.publish(track_name):
        logger.error("Failed to publish track")
        return
    
    logger.info(f"Publishing time updates on: {track_name}")
    
    try:
        group_id = 1
        object_id = 1
        
        while True:
            current_time = datetime.now().isoformat()
            payload = current_time.encode('utf-8')
            
            await publisher.send_object(
                track_name, 
                payload, 
                group_id=group_id, 
                object_id=object_id
            )
            logger.info(f"Sent time: {current_time}")
            
            object_id += 1
            if object_id > 1000:
                group_id += 1
                object_id = 1
            
            await asyncio.sleep(1)
            
    except KeyboardInterrupt:
        logger.info("Publisher stopped by user")
    finally:
        publisher.disconnect()
        logger.info("Publisher shutdown complete")


if __name__ == "__main__":
    asyncio.run(main())
