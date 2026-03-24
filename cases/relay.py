#!/usr/bin/env python3
"""
MOQ Relay Case - Forwards messages between pub and sub

This relay accepts connections from publishers and subscribers,
caching and forwarding messages between them.

Usage:
    python relay.py

The relay listens on 127.0.0.1:4443 and acts as an intermediary
between publishers and subscribers.
"""

import asyncio
import sys
import logging
import ssl
from typing import Dict, Optional, Tuple
from dataclasses import dataclass

sys.path.insert(0, '/home/acn/cxr/moq-py')
from moq.encoding import FullTrackName
from moq.messages import (
    SubscribeMessage, SubscribeOkMessage, PublishMessage, PublishOkMessage,
    ObjectHeader, ObjectDatagram, decode_control_message, GroupOrder,
    FetchMessage, FetchOkMessage
)
from moq.session import MOQSession, Role

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

RELAY_HOST = "127.0.0.1"
RELAY_PORT = 4443


@dataclass
class ClientSession:
    """Represents a connected client session."""
    session_id: str
    reader: asyncio.StreamReader
    writer: asyncio.StreamWriter
    role: Optional[Role] = None
    subscriptions: Dict[FullTrackName, dict] = None
    publications: Dict[FullTrackName, dict] = None
    
    def __post_init__(self):
        if self.subscriptions is None:
            self.subscriptions = {}
        if self.publications is None:
            self.publications = {}


class MOQRelayServer:
    """
    Simple MOQ Relay Server using TCP (for demonstration).
    In production, this would use QUIC.
    """
    
    def __init__(self, host: str, port: int):
        self.host = host
        self.port = port
        self.clients: Dict[str, ClientSession] = {}
        self.publications: Dict[FullTrackName, ClientSession] = {}
        self.subscriptions: Dict[FullTrackName, list] = {}
        self.server = None
        self._running = False
        # Object cache: track_name -> [(group_id, object_id, payload), ...]
        self.object_cache: Dict[FullTrackName, list] = {}
        self.max_cached_objects = 1000  # Limit cache size
    
    async def start(self):
        """Start the relay server."""
        self.server = await asyncio.start_server(
            self._handle_client,
            self.host,
            self.port
        )
        self._running = True
        
        addr = self.server.sockets[0].getsockname()
        logger.info(f"MOQ Relay running on {addr}")
        logger.info("Waiting for connections... (Press Ctrl+C to stop)")
        
        async with self.server:
            await self.server.serve_forever()
    
    async def stop(self):
        """Stop the relay server."""
        self._running = False
        if self.server:
            self.server.close()
            await self.server.wait_closed()
        
        # Close all client connections
        for client in list(self.clients.values()):
            client.writer.close()
            await client.writer.wait_closed()
        
        logger.info("Relay server stopped")
    
    async def _handle_client(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        """Handle a new client connection."""
        addr = writer.get_extra_info('peername')
        session_id = f"{addr[0]}:{addr[1]}"
        
        client = ClientSession(
            session_id=session_id,
            reader=reader,
            writer=writer
        )
        self.clients[session_id] = client
        
        logger.info(f"Client connected: {session_id}")
        
        try:
            # Handle client messages
            await self._process_client_messages(client)
        except asyncio.CancelledError:
            pass
        except Exception as e:
            logger.error(f"Error handling client {session_id}: {e}")
        finally:
            # Cleanup
            await self._cleanup_client(client)
    
    async def _process_client_messages(self, client: ClientSession):
        """Process messages from a client."""
        while self._running:
            try:
                # Read message length (4 bytes)
                length_data = await client.reader.read(4)
                if not length_data or len(length_data) < 4:
                    break
                
                msg_length = int.from_bytes(length_data, 'big')
                
                # Read the full message
                data = await client.reader.read(msg_length)
                if not data or len(data) < msg_length:
                    break
                
                # Handle the message
                await self._handle_message(client, data)
                
            except asyncio.IncompleteReadError:
                break
            except ConnectionResetError:
                break
            except Exception as e:
                logger.error(f"Error reading from client {client.session_id}: {e}")
                break
    
    async def _handle_message(self, client: ClientSession, data: bytes):
        """Handle a message from a client."""
        try:
            # Try to decode as control message first
            try:
                msg, _ = decode_control_message(data)
                
                if isinstance(msg, PublishMessage):
                    await self._handle_publish(client, msg)
                elif isinstance(msg, SubscribeMessage):
                    await self._handle_subscribe(client, msg)
                elif isinstance(msg, FetchMessage):
                    await self._handle_fetch(client, msg)
                else:
                    logger.debug(f"Received control message type: {type(msg).__name__}")
                return
            except Exception:
                pass  # Not a control message, try data message
            
            # Try to decode as ObjectDatagram (data message)
            try:
                obj, _ = ObjectDatagram.decode(data)
                await self._handle_object(client, obj)
                return
            except Exception:
                pass  # Not an ObjectDatagram either
            
            # Treat as raw data
            logger.debug(f"Received raw data: {len(data)} bytes")
            await self._forward_raw_data(client, data)
                
        except Exception as e:
            logger.error(f"Error handling message: {e}")
    
    async def _handle_publish(self, client: ClientSession, msg: PublishMessage):
        """Handle a publish request."""
        track_name = msg.full_track_name
        logger.info(f"Client {client.session_id} publishing: {track_name}")
        
        # Store publication
        self.publications[track_name] = client
        client.publications[track_name] = {
            'track_alias': msg.track_alias,
            'request_id': msg.request_id
        }
        
        # Send PUBLISH_OK
        response = PublishOkMessage(request_id=msg.request_id)
        response_data = response.encode()
        logger.debug(f"Sending PUBLISH_OK: {len(response_data)} bytes")
        await self._send_message(client, response_data)
        logger.info(f"Publication accepted: {track_name}")
    
    async def _handle_subscribe(self, client: ClientSession, msg: SubscribeMessage):
        """Handle a subscribe request."""
        track_name = msg.full_track_name
        logger.info(f"Client {client.session_id} subscribing to: {track_name}")
        
        # Store subscription
        if track_name not in self.subscriptions:
            self.subscriptions[track_name] = []
        self.subscriptions[track_name].append(client)
        client.subscriptions[track_name] = {
            'track_alias': msg.track_alias,
            'request_id': msg.request_id
        }
        
        # Send SUBSCRIBE_OK
        response = SubscribeOkMessage(
            request_id=msg.request_id,
            expires=0,
            group_order=GroupOrder.ASCENDING
        )
        response_data = response.encode()
        logger.debug(f"Sending SUBSCRIBE_OK: {len(response_data)} bytes")
        await self._send_message(client, response_data)
        logger.info(f"Subscription accepted: {track_name}")
    
    async def _handle_fetch(self, client: ClientSession, msg: FetchMessage):
        """Handle a fetch request."""
        track_name = msg.full_track_name
        logger.info(f"Client {client.session_id} fetching from: {track_name}, "
                   f"range=[{msg.start_group}:{msg.start_object} to {msg.end_group}:{msg.end_object}]")
        
        # Check if track exists (has a publisher)
        if track_name not in self.publications:
            logger.warning(f"Fetch requested for unknown track: {track_name}")
            from moq.messages import RequestErrorMessage, ErrorCode
            response = RequestErrorMessage(
                request_id=msg.request_id,
                error_code=ErrorCode.INTERNAL_ERROR,
                reason="Track not found"
            )
            await self._send_message(client, response.encode())
            return
        
        # Store fetch request
        if track_name not in self.subscriptions:
            self.subscriptions[track_name] = []
        self.subscriptions[track_name].append(client)
        
        # Send FETCH_OK
        response = FetchOkMessage(
            request_id=msg.request_id,
            group_order=GroupOrder.ASCENDING,
            end_of_track=False
        )
        response_data = response.encode()
        logger.debug(f"Sending FETCH_OK: {len(response_data)} bytes")
        await self._send_message(client, response_data)
        logger.info(f"Fetch accepted: {track_name}")
        
        # Send cached objects that match the fetch range
        await self._send_cached_objects(client, track_name, msg)

    async def _send_cached_objects(self, client: ClientSession, track_name: FullTrackName, msg: FetchMessage):
        """Send cached objects that match the fetch range to the client."""
        cached_objects = self.object_cache.get(track_name, [])
        if not cached_objects:
            logger.info(f"No cached objects for track: {track_name}")
            return
        
        sent_count = 0
        for obj_data in cached_objects:
            # Check if object is within fetch range
            # If end_group/end_object is None, fetch until the latest message
            end_group_limit = msg.end_group if msg.end_group is not None else float('inf')
            end_object_limit = msg.end_object if msg.end_object is not None else float('inf')
            
            if (msg.start_group <= obj_data['group_id'] <= end_group_limit and
                msg.start_object <= obj_data['object_id'] <= end_object_limit):
                
                # Create ObjectDatagram and send
                header = ObjectHeader(
                    track_alias=0,  # Fetch doesn't use track_alias in the same way
                    group_id=obj_data['group_id'],
                    object_id=obj_data['object_id'],
                    publisher_priority=obj_data['publisher_priority'],
                    object_status=obj_data['object_status']
                )
                obj = ObjectDatagram(header=header, payload=obj_data['payload'])
                
                try:
                    await self._send_message(client, obj.encode())
                    sent_count += 1
                    logger.debug(f"Sent cached object: group={obj_data['group_id']}, object={obj_data['object_id']}")
                except Exception as e:
                    logger.error(f"Error sending cached object to {client.session_id}: {e}")
        
        logger.info(f"Sent {sent_count} cached objects to {client.session_id} for fetch request")

    async def _handle_object(self, client: ClientSession, obj: ObjectDatagram):
        """Handle an object from a publisher."""
        # Find the track name from the client's publications
        track_name = None
        for tn, pub_info in client.publications.items():
            if pub_info['track_alias'] == obj.header.track_alias:
                track_name = tn
                break
        
        if not track_name:
            logger.warning(f"Received object for unknown track alias: {obj.header.track_alias}")
            return
        
        # Cache the object for future fetches
        await self._cache_object(track_name, obj)
        
        # Forward to all subscribers
        await self._forward_object(track_name, obj)
    
    async def _cache_object(self, track_name: FullTrackName, obj: ObjectDatagram):
        """Cache an object for future fetch requests."""
        if track_name not in self.object_cache:
            self.object_cache[track_name] = []
        
        # Store object data
        self.object_cache[track_name].append({
            'group_id': obj.header.group_id,
            'object_id': obj.header.object_id,
            'publisher_priority': obj.header.publisher_priority,
            'object_status': obj.header.object_status,
            'payload': obj.payload
        })
        
        # Limit cache size
        if len(self.object_cache[track_name]) > self.max_cached_objects:
            self.object_cache[track_name].pop(0)
        
        logger.debug(f"Cached object for {track_name}: group={obj.header.group_id}, object={obj.header.object_id}")
    
    async def _forward_raw_data(self, sender: ClientSession, data: bytes):
        """Forward raw data to subscribers."""
        # Try to find which track this data belongs to
        for track_name, subscribers in self.subscriptions.items():
            # Check if sender is the publisher for this track
            if track_name in self.publications and self.publications[track_name] == sender:
                # Forward to all subscribers except sender
                for subscriber in subscribers:
                    if subscriber.session_id != sender.session_id:
                        try:
                            await self._send_message(subscriber, data)
                        except Exception as e:
                            logger.error(f"Error forwarding to {subscriber.session_id}: {e}")
                break
    
    async def _forward_object(self, track_name: FullTrackName, obj: ObjectDatagram):
        """Forward an object to all subscribers of a track."""
        subscribers = self.subscriptions.get(track_name, [])
        if not subscribers:
            return
        
        data = obj.encode()
        forwarded = 0
        
        for subscriber in subscribers:
            try:
                await self._send_message(subscriber, data)
                forwarded += 1
            except Exception as e:
                logger.error(f"Error forwarding to {subscriber.session_id}: {e}")
        
        if forwarded > 0:
            logger.debug(f"Forwarded object to {forwarded} subscribers")
    
    async def _send_message(self, client: ClientSession, data: bytes):
        """Send a message to a client."""
        # Send length prefix + data
        length = len(data).to_bytes(4, 'big')
        client.writer.write(length + data)
        await client.writer.drain()
    
    async def _cleanup_client(self, client: ClientSession):
        """Clean up when a client disconnects."""
        logger.info(f"Client disconnected: {client.session_id}")
        
        # Remove from clients
        if client.session_id in self.clients:
            del self.clients[client.session_id]
        
        # Remove publications
        for track_name in list(client.publications.keys()):
            if track_name in self.publications and self.publications[track_name].session_id == client.session_id:
                del self.publications[track_name]
                logger.info(f"Publication removed: {track_name}")
        
        # Remove subscriptions
        for track_name in list(client.subscriptions.keys()):
            if track_name in self.subscriptions:
                self.subscriptions[track_name] = [
                    s for s in self.subscriptions[track_name] 
                    if s.session_id != client.session_id
                ]
                if not self.subscriptions[track_name]:
                    del self.subscriptions[track_name]
        
        # Clean up object cache if no more publishers/subscribers for track
        for track_name in list(self.object_cache.keys()):
            if track_name not in self.publications and track_name not in self.subscriptions:
                del self.object_cache[track_name]
                logger.debug(f"Cleaned up cache for: {track_name}")
        
        # Close connection
        try:
            client.writer.close()
            await client.writer.wait_closed()
        except:
            pass


async def main():
    relay = MOQRelayServer(RELAY_HOST, RELAY_PORT)
    
    try:
        await relay.start()
    except KeyboardInterrupt:
        logger.info("Relay stopped by user")
    finally:
        await relay.stop()
        logger.info("Relay shutdown complete")


if __name__ == "__main__":
    asyncio.run(main())
