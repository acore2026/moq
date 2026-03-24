#!/usr/bin/env python3
"""
MOQ Subscriber Case - Prints received messages

This subscriber connects to a relay and subscribes to time updates,
printing each received message to the console.

Usage:
    python sub.py

The subscriber connects to the relay at 127.0.0.1:4443 and subscribes
to the track "time/updates", printing all received time messages.
"""

import asyncio
import sys
import logging

sys.path.insert(0, '/home/acn/cxr/moq-py')
from moq.encoding import FullTrackName
from moq.messages import (
    SubscribeMessage, SubscribeOkMessage, ObjectDatagram, decode_control_message,
    SubscribeFilter, GroupOrder
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


class SimpleSubscriber:
    """Simple TCP-based MOQ subscriber."""
    
    def __init__(self, host: str, port: int):
        self.host = host
        self.port = port
        self.reader: asyncio.StreamReader = None
        self.writer: asyncio.StreamWriter = None
        self.track_alias_counter = 1
        self.request_id_counter = 1
        self.subscriptions: dict = {}
        self._running = False
        self._receive_task = None
    
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
    
    async def subscribe(self, track_name: FullTrackName) -> bool:
        """Subscribe to a track."""
        track_alias = self.track_alias_counter
        self.track_alias_counter += 1
        
        request_id = self.request_id_counter
        self.request_id_counter += 1
        
        # Send SUBSCRIBE message
        msg = SubscribeMessage(
            request_id=request_id,
            track_alias=track_alias,
            full_track_name=track_name,
            subscriber_priority=128,
            group_order=GroupOrder.ASCENDING,
            filter_type=SubscribeFilter.LATEST_OBJECT
        )
        
        msg_data = msg.encode()
        logger.debug(f"Sending SUBSCRIBE: {len(msg_data)} bytes")
        await self._send_message(msg_data)
        
        # Wait for SUBSCRIBE_OK
        try:
            response_data = await asyncio.wait_for(self._read_message(), timeout=5.0)
            if response_data:
                logger.debug(f"Received response: {len(response_data)} bytes")
                try:
                    from moq.messages import decode_control_message
                    response, _ = decode_control_message(response_data)
                    logger.debug(f"Response type: {type(response).__name__}")
                    if isinstance(response, SubscribeOkMessage) and response.request_id == request_id:
                        self.subscriptions[track_name] = {
                            'track_alias': track_alias,
                            'request_id': request_id
                        }
                        logger.info(f"Subscription accepted: {track_name}")
                        
                        # Start receiving messages
                        self._running = True
                        self._receive_task = asyncio.create_task(self._receive_loop())
                        return True
                except Exception as e:
                    logger.error(f"Error decoding response: {e}")
        except asyncio.TimeoutError:
            logger.error("Timeout waiting for SUBSCRIBE_OK")
        
        return False
    
    async def _receive_loop(self):
        """Loop to receive messages."""
        while self._running:
            try:
                data = await self._read_message()
                if not data:
                    break
                
                await self._handle_message(data)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error receiving message: {e}")
                break
        
        logger.info("Receive loop ended")
    
    async def _handle_message(self, data: bytes):
        """Handle a received message."""
        try:
            # Try to decode as object datagram
            try:
                obj, _ = ObjectDatagram.decode(data)
                await self._handle_object(obj)
                return
            except Exception:
                pass
            
            # Try to decode as control message
            try:
                msg, _ = decode_control_message(data)
                logger.debug(f"Received control message: {type(msg).__name__}")
            except Exception:
                # Raw data - try to decode as string
                try:
                    message = data.decode('utf-8')
                    print(f"[Received] {message}")
                except:
                    logger.debug(f"Received raw data: {len(data)} bytes")
        
        except Exception as e:
            logger.error(f"Error handling message: {e}")
    
    async def _handle_object(self, obj: ObjectDatagram):
        """Handle a received object."""
        try:
            message = obj.payload.decode('utf-8')
            print(f"[Received] {message}")
            logger.info(f"Object: group={obj.header.group_id}, object={obj.header.object_id}, size={len(obj.payload)}")
        except Exception as e:
            logger.error(f"Failed to decode object payload: {e}")
    
    async def _send_message(self, data: bytes):
        """Send a message with length prefix."""
        length = len(data).to_bytes(4, 'big')
        self.writer.write(length + data)
        await self.writer.drain()
    
    async def _read_message(self) -> bytes:
        """Read a message with length prefix."""
        try:
            length_data = await self.reader.read(4)
            if not length_data or len(length_data) < 4:
                return None
            
            msg_length = int.from_bytes(length_data, 'big')
            data = await self.reader.read(msg_length)
            return data if len(data) == msg_length else None
        except Exception:
            return None
    
    async def unsubscribe(self, track_name: FullTrackName):
        """Unsubscribe from a track."""
        if track_name in self.subscriptions:
            del self.subscriptions[track_name]
            logger.info(f"Unsubscribed from: {track_name}")
    
    def disconnect(self):
        """Disconnect from the relay."""
        self._running = False
        if self._receive_task:
            self._receive_task.cancel()
        if self.writer:
            self.writer.close()
        logger.info("Disconnected from relay")


async def main():
    logger.info("Starting time subscriber...")
    
    subscriber = SimpleSubscriber(RELAY_HOST, RELAY_PORT)
    
    if not await subscriber.connect():
        logger.error("Failed to connect to relay")
        return
    
    track_name = FullTrackName(TRACK_NAMESPACE, TRACK_NAME)
    
    if not await subscriber.subscribe(track_name):
        logger.error("Failed to subscribe to track")
        return
    
    logger.info(f"Subscribed to: {track_name}")
    logger.info("Waiting for messages... (Press Ctrl+C to stop)")
    
    try:
        while True:
            await asyncio.sleep(1)
    except KeyboardInterrupt:
        logger.info("Subscriber stopped by user")
    finally:
        await subscriber.unsubscribe(track_name)
        subscriber.disconnect()
        logger.info("Subscriber shutdown complete")


if __name__ == "__main__":
    asyncio.run(main())
