#!/usr/bin/env python3
"""
MOQ Fetch Subscriber Case - Tests fetch mode functionality using TCP

This subscriber connects to a relay, fetches specific objects from a track,
and prints each received message to the console.

Usage:
    python sub_fetch.py [--host HOST] [--port PORT] [--start-group N] [--start-object N] [--end-group N] [--end-object N]

The subscriber connects to the relay and fetches objects from the track
"time/updates" within the specified range.
"""

import asyncio
import sys
import argparse
import logging

sys.path.insert(0, '/home/acn/cxr/moq-py')
from moq.encoding import FullTrackName, VarInt
from moq.messages import (
    FetchMessage, FetchOkMessage, ObjectDatagram, decode_control_message,
    RequestErrorMessage, GroupOrder, ObjectHeader
)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORT = 4443
TRACK_NAMESPACE = [b"time"]
TRACK_NAME = b"updates"


class SimpleFetchSubscriber:
    """Simple TCP-based MOQ fetch subscriber."""
    
    def __init__(self, host: str, port: int):
        self.host = host
        self.port = port
        self.reader: asyncio.StreamReader = None
        self.writer: asyncio.StreamWriter = None
        self.request_id_counter = 1
        self.fetches: dict = {}
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
    
    async def fetch(self, track_name: FullTrackName, 
                    start_group: int, start_object: int,
                    end_group: int, end_object: int,
                    priority: int = 128) -> bool:
        """Fetch objects from a track."""
        request_id = self.request_id_counter
        self.request_id_counter += 1
        
        # Send FETCH message
        msg = FetchMessage(
            request_id=request_id,
            full_track_name=track_name,
            subscriber_priority=priority,
            group_order=GroupOrder.ASCENDING,
            start_group=start_group,
            start_object=start_object,
            end_group=end_group,
            end_object=end_object
        )
        
        msg_data = msg.encode()
        logger.debug(f"Sending FETCH: {len(msg_data)} bytes")
        await self._send_message(msg_data)
        
        # Wait for FETCH_OK
        try:
            response_data = await asyncio.wait_for(self._read_message(), timeout=5.0)
            if response_data:
                logger.debug(f"Received response: {len(response_data)} bytes")
                try:
                    response, _ = decode_control_message(response_data)
                    logger.debug(f"Response type: {type(response).__name__}")
                    
                    if isinstance(response, FetchOkMessage) and response.request_id == request_id:
                        self.fetches[request_id] = {
                            'track_name': track_name,
                            'start_group': start_group,
                            'start_object': start_object,
                            'end_group': end_group,
                            'end_object': end_object
                        }
                        logger.info(f"Fetch accepted: {track_name}")
                        
                        # Start receiving messages
                        self._running = True
                        self._receive_task = asyncio.create_task(self._receive_loop())
                        return True
                    elif isinstance(response, RequestErrorMessage):
                        logger.error(f"Fetch rejected: {response.reason}")
                        return False
                        
                except Exception as e:
                    logger.error(f"Error decoding response: {e}")
        except asyncio.TimeoutError:
            logger.error("Timeout waiting for FETCH_OK")
        
        return False
    
    async def _receive_loop(self):
        """Loop to receive fetched messages."""
        received_count = 0
        
        while self._running:
            try:
                data = await self._read_message()
                if not data:
                    break
                
                handled = await self._handle_message(data)
                if handled:
                    received_count += 1
                    
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error receiving message: {e}")
                break
        
        logger.info(f"Receive loop ended. Total objects received: {received_count}")
    
    async def _handle_message(self, data: bytes) -> bool:
        """Handle a received message. Returns True if it was an object."""
        try:
            # Try to decode as object datagram
            try:
                obj, _ = ObjectDatagram.decode(data)
                await self._handle_object(obj)
                return True
            except Exception:
                pass
            
            # Try to decode as control message
            try:
                msg, _ = decode_control_message(data)
                logger.debug(f"Received control message: {type(msg).__name__}")
                if isinstance(msg, RequestErrorMessage):
                    logger.error(f"Request error: {msg.reason}")
                return False
            except Exception:
                pass
            
            # Try to decode as raw object data with header
            try:
                await self._handle_raw_object(data)
                return True
            except Exception:
                pass
            
            # Raw data - try to decode as string
            try:
                message = data.decode('utf-8')
                print(f"[Received] {message}")
                return True
            except:
                logger.debug(f"Received raw data: {len(data)} bytes")
                return False
        
        except Exception as e:
            logger.error(f"Error handling message: {e}")
            return False
    
    async def _handle_object(self, obj: ObjectDatagram):
        """Handle a received object."""
        try:
            message = obj.payload.decode('utf-8')
            print(f"[Fetched] Group={obj.header.group_id}, Object={obj.header.object_id}: {message}")
            logger.info(f"Object: group={obj.header.group_id}, object={obj.header.object_id}, size={len(obj.payload)}")
        except Exception as e:
            logger.error(f"Failed to decode object payload: {e}")
    
    async def _handle_raw_object(self, data: bytes):
        """Handle raw object data that may not be wrapped in ObjectDatagram."""
        try:
            offset = 0
            
            # Try to read as ObjectHeader + payload
            # First byte might be track_alias (varint)
            track_alias, consumed = VarInt.decode(data, offset)
            offset += consumed
            
            group_id, consumed = VarInt.decode(data, offset)
            offset += consumed
            
            object_id, consumed = VarInt.decode(data, offset)
            offset += consumed
            
            publisher_priority = data[offset]
            offset += 1
            
            object_status = data[offset]
            offset += 1
            
            # Remaining is payload
            payload = data[offset:]
            
            message = payload.decode('utf-8')
            print(f"[Fetched] Group={group_id}, Object={object_id}: {message}")
            logger.info(f"Raw object: group={group_id}, object={object_id}, size={len(payload)}")
            
        except Exception as e:
            logger.debug(f"Failed to decode as raw object: {e}")
            raise
    
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
    
    def disconnect(self):
        """Disconnect from the relay."""
        self._running = False
        if self._receive_task:
            self._receive_task.cancel()
        if self.writer:
            self.writer.close()
        logger.info("Disconnected from relay")


async def main():
    parser = argparse.ArgumentParser(description='MOQ Fetch Subscriber')
    parser.add_argument('--host', default=DEFAULT_HOST, help='Relay host')
    parser.add_argument('--port', type=int, default=DEFAULT_PORT, help='Relay port')
    parser.add_argument('--start-group', type=int, default=0, help='Start group ID (default: 0)')
    parser.add_argument('--start-object', type=int, default=0, help='Start object ID (default: 0)')
    parser.add_argument('--end-group', type=int, default=None, help='End group ID (default: fetch until latest)')
    parser.add_argument('--end-object', type=int, default=None, help='End object ID (default: fetch until latest)')
    parser.add_argument('--priority', type=int, default=128, help='Subscriber priority (0-255)')
    
    args = parser.parse_args()
    
    logger.info("Starting fetch subscriber...")
    logger.info(f"Connecting to relay at {args.host}:{args.port}")
    
    # Create fetch subscriber
    subscriber = SimpleFetchSubscriber(args.host, args.port)
    
    # Connect to relay
    if not await subscriber.connect():
        logger.error("Failed to connect to relay")
        return 1
    
    logger.info("Connected to relay successfully!")
    
    # Define track name
    track_name = FullTrackName(TRACK_NAMESPACE, TRACK_NAME)
    
    logger.info(f"Track: {track_name}")
    logger.info(f"Fetch range: group {args.start_group}:{args.start_object} to {args.end_group}:{args.end_object}")
    logger.info(f"Priority: {args.priority}")
    
    # Initiate fetch
    success = await subscriber.fetch(
        track_name=track_name,
        start_group=args.start_group,
        start_object=args.start_object,
        end_group=args.end_group,
        end_object=args.end_object,
        priority=args.priority
    )
    
    if not success:
        logger.error("Failed to initiate fetch")
        subscriber.disconnect()
        return 1
    
    logger.info("Fetch initiated successfully!")
    logger.info("Waiting for fetch response and objects... (Press Ctrl+C to stop)")
    
    # Wait for fetch to complete or user interrupt
    try:
        while subscriber._running:
            await asyncio.sleep(1)
    except KeyboardInterrupt:
        logger.info("Fetch stopped by user")
    finally:
        subscriber.disconnect()
        logger.info("Fetch subscriber shutdown complete")
    
    return 0


if __name__ == "__main__":
    exit_code = asyncio.run(main())
    sys.exit(exit_code)
