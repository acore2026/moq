"""
MOQ Transport - Relay Implementation
Implements a caching relay for MOQT with memory and disk caching.
Uses QUIC as the underlying transport protocol.
"""

import os
import json
import asyncio
import logging
import hashlib
import shutil
from typing import Dict, List, Optional, Set, Tuple, Callable
from dataclasses import dataclass, field, asdict
from datetime import datetime, timedelta
from pathlib import Path
import threading

from moq.session import MOQSession, Role, Subscription, Publication
from moq.messages import (
    SubscribeMessage, PublishMessage, ObjectHeader, ObjectDatagram,
    SubscribeOkMessage, PublishOkMessage, PublishDoneMessage,
    decode_control_message, GroupOrder, FetchMessage, FetchOkMessage,
    RequestErrorMessage, ErrorCode
)
from moq.encoding import FullTrackName, Location, VarInt
from moq.transport import QUICServer, StreamData, DatagramData, is_quic_available

logger = logging.getLogger(__name__)


@dataclass
class CachedObject:
    """Cached object with metadata."""
    track_alias: int
    group_id: int
    object_id: int
    publisher_priority: int
    payload: bytes
    timestamp: datetime = field(default_factory=datetime.now)
    access_count: int = 0
    
    def to_bytes(self) -> bytes:
        """Serialize to bytes."""
        header = ObjectHeader(
            track_alias=self.track_alias,
            group_id=self.group_id,
            object_id=self.object_id,
            publisher_priority=self.publisher_priority
        )
        datagram = ObjectDatagram(header=header, payload=self.payload)
        return datagram.encode()
    
    @staticmethod
    def from_bytes(data: bytes) -> 'CachedObject':
        """Deserialize from bytes."""
        datagram, _ = ObjectDatagram.decode(data)
        return CachedObject(
            track_alias=datagram.header.track_alias,
            group_id=datagram.header.group_id,
            object_id=datagram.header.object_id,
            publisher_priority=datagram.header.publisher_priority,
            payload=datagram.payload
        )
    
    def get_location(self) -> Location:
        """Get object location."""
        return Location(self.group_id, self.object_id)


@dataclass
class ClientSession:
    """Represents a connected client session over QUIC."""
    session_id: str
    protocol: any  # MOQQuicProtocol instance
    quic_connection: any  # QuicConnection instance
    role: Optional[Role] = None
    subscriptions: Dict[FullTrackName, dict] = None
    publications: Dict[FullTrackName, dict] = None
    control_stream_id: Optional[int] = None
    control_buffer: bytes = b""
    
    def __post_init__(self):
        if self.subscriptions is None:
            self.subscriptions = {}
        if self.publications is None:
            self.publications = {}


class ObjectCache:
    """
    Cache for MOQT objects with memory and disk backing.
    """
    
    def __init__(self, max_memory_size: int = 100 * 1024 * 1024,  # 100MB default
                 disk_cache_dir: Optional[str] = None,
                 max_disk_size: int = 1024 * 1024 * 1024):  # 1GB default
        self.max_memory_size = max_memory_size
        self.max_disk_size = max_disk_size
        self.disk_cache_dir = Path(disk_cache_dir) if disk_cache_dir else None
        
        # Memory cache: track_name -> {location -> CachedObject}
        self._memory_cache: Dict[FullTrackName, Dict[Location, CachedObject]] = {}
        self._memory_size = 0
        self._lock = threading.RLock()
        
        # Disk cache index
        self._disk_index: Dict[str, Dict[Location, str]] = {}  # track_id -> {location -> file_path}
        self._disk_size = 0
        
        # Statistics
        self._hits = 0
        self._misses = 0
        
        # Initialize disk cache
        if self.disk_cache_dir:
            self._init_disk_cache()
    
    def _init_disk_cache(self):
        """Initialize disk cache directory."""
        self.disk_cache_dir.mkdir(parents=True, exist_ok=True)
        
        # Load existing cache index
        index_file = self.disk_cache_dir / "cache_index.json"
        if index_file.exists():
            try:
                with open(index_file, 'r') as f:
                    index_data = json.load(f)
                    self._disk_index = index_data
                    self._disk_size = sum(
                        os.path.getsize(f) for f in self.disk_cache_dir.glob("**/*")
                        if f.is_file() and f.name != "cache_index.json"
                    )
                logger.info(f"Loaded disk cache index: {len(self._disk_index)} tracks")
            except Exception as e:
                logger.warning(f"Failed to load disk cache index: {e}")
    
    def _get_track_id(self, track_name: FullTrackName) -> str:
        """Get unique track ID."""
        track_str = track_name.to_string()
        return hashlib.sha256(track_str.encode()).hexdigest()[:16]
    
    def _get_object_path(self, track_id: str, location: Location) -> Path:
        """Get file path for cached object."""
        group_dir = self.disk_cache_dir / track_id / str(location.group)
        group_dir.mkdir(parents=True, exist_ok=True)
        return group_dir / f"{location.object_id}.obj"
    
    def _save_disk_index(self):
        """Save disk cache index."""
        if not self.disk_cache_dir:
            return
        
        index_file = self.disk_cache_dir / "cache_index.json"
        try:
            with open(index_file, 'w') as f:
                json.dump(self._disk_index, f)
        except Exception as e:
            logger.warning(f"Failed to save disk cache index: {e}")

    def clear_disk_cache(self):
        """Remove all cached objects and metadata from disk."""
        if not self.disk_cache_dir:
            return

        with self._lock:
            try:
                if self.disk_cache_dir.exists():
                    for path in self.disk_cache_dir.iterdir():
                        if path.is_dir():
                            shutil.rmtree(path)
                        else:
                            path.unlink()
                else:
                    self.disk_cache_dir.mkdir(parents=True, exist_ok=True)

                self._disk_index.clear()
                self._disk_size = 0
                self._save_disk_index()
                logger.info(f"Cleared disk cache: {self.disk_cache_dir}")
            except Exception as e:
                logger.warning(f"Failed to clear disk cache: {e}")
    
    def put(self, track_name: FullTrackName, obj: CachedObject):
        """Add object to cache."""
        location = obj.get_location()
        
        with self._lock:
            # Add to memory cache
            if track_name not in self._memory_cache:
                self._memory_cache[track_name] = {}
            
            # Remove old object if exists
            if location in self._memory_cache[track_name]:
                old_obj = self._memory_cache[track_name][location]
                self._memory_size -= len(old_obj.payload)
            
            # Add new object
            self._memory_cache[track_name][location] = obj
            self._memory_size += len(obj.payload)
            
            # Evict from memory if needed
            self._evict_memory_if_needed()
            
            # Also persist to disk if enabled
            if self.disk_cache_dir:
                self._persist_to_disk(track_name, obj)
    
    def _evict_memory_if_needed(self):
        """Evict objects from memory cache if size exceeds limit."""
        if self._memory_size <= self.max_memory_size:
            return
        
        # Simple LRU eviction
        all_objects = []
        for track_name, objects in self._memory_cache.items():
            for location, obj in objects.items():
                all_objects.append((track_name, location, obj))
        
        # Sort by access time (oldest first)
        all_objects.sort(key=lambda x: x[2].timestamp)
        
        # Evict oldest objects
        while self._memory_size > self.max_memory_size * 0.8 and all_objects:
            track_name, location, obj = all_objects.pop(0)
            if location in self._memory_cache.get(track_name, {}):
                del self._memory_cache[track_name][location]
                self._memory_size -= len(obj.payload)
                logger.debug(f"Evicted from memory: {track_name} @ {location}")
    
    def _persist_to_disk(self, track_name: FullTrackName, obj: CachedObject):
        """Persist object to disk cache."""
        track_id = self._get_track_id(track_name)
        location = obj.get_location()
        
        try:
            file_path = self._get_object_path(track_id, location)
            
            with open(file_path, 'wb') as f:
                f.write(obj.to_bytes())
            
            # Update index
            if track_id not in self._disk_index:
                self._disk_index[track_id] = {}
            self._disk_index[track_id][str(location)] = str(file_path)
            
            self._disk_size += len(obj.payload)
            self._save_disk_index()
            
            logger.debug(f"Persisted to disk: {track_name} @ {location}")
            
        except Exception as e:
            logger.warning(f"Failed to persist to disk: {e}")
    
    def get(self, track_name: FullTrackName, location: Location) -> Optional[CachedObject]:
        """Get object from cache."""
        with self._lock:
            # Try memory cache first
            if track_name in self._memory_cache:
                if location in self._memory_cache[track_name]:
                    obj = self._memory_cache[track_name][location]
                    obj.access_count += 1
                    obj.timestamp = datetime.now()
                    self._hits += 1
                    logger.debug(f"Memory cache hit: {track_name} @ {location}")
                    return obj
            
            # Try disk cache
            if self.disk_cache_dir:
                track_id = self._get_track_id(track_name)
                if track_id in self._disk_index:
                    if str(location) in self._disk_index[track_id]:
                        try:
                            file_path = Path(self._disk_index[track_id][str(location)])
                            with open(file_path, 'rb') as f:
                                data = f.read()
                            
                            obj = CachedObject.from_bytes(data)
                            obj.access_count += 1
                            obj.timestamp = datetime.now()
                            
                            # Also add to memory cache
                            if track_name not in self._memory_cache:
                                self._memory_cache[track_name] = {}
                            self._memory_cache[track_name][location] = obj
                            self._memory_size += len(obj.payload)
                            
                            self._hits += 1
                            logger.debug(f"Disk cache hit: {track_name} @ {location}")
                            return obj
                        except Exception as e:
                            logger.warning(f"Failed to load from disk cache: {e}")
            
            self._misses += 1
            return None
    
    def get_range(self, track_name: FullTrackName, 
                  start: Location, end: Location) -> List[CachedObject]:
        """Get all objects in range from cache."""
        objects = []
        
        with self._lock:
            if track_name not in self._memory_cache:
                return objects
            
            for location, obj in self._memory_cache[track_name].items():
                if start <= location <= end:
                    obj.access_count += 1
                    objects.append(obj)
        
        # Sort by location
        objects.sort(key=lambda o: o.get_location())
        return objects
    
    def get_statistics(self) -> dict:
        """Get cache statistics."""
        total_requests = self._hits + self._misses
        hit_rate = self._hits / total_requests if total_requests > 0 else 0
        
        return {
            'memory_size': self._memory_size,
            'memory_objects': sum(len(objs) for objs in self._memory_cache.values()),
            'disk_size': self._disk_size,
            'hits': self._hits,
            'misses': self._misses,
            'hit_rate': hit_rate
        }
    
    def get_cache_stats(self) -> dict:
        """Alias for get_statistics for backward compatibility."""
        return self.get_statistics()


class MOQRelay:
    """
    MOQ Relay with caching support.
    Acts as both publisher and subscriber, forwarding and caching content.
    Uses QUIC as the underlying transport protocol.
    """
    
    def __init__(self, host: str, port: int, 
                 cache_dir: Optional[str] = None,
                 max_memory_cache: int = 100 * 1024 * 1024,
                 max_disk_cache: int = 1024 * 1024 * 1024,
                 cert_file: Optional[str] = None,
                 key_file: Optional[str] = None):
        self.host = host
        self.port = port
        
        # Check QUIC availability
        if not is_quic_available():
            raise RuntimeError("QUIC is not available. Please install aioquic.")
        
        # Cache
        self.cache = ObjectCache(
            max_memory_size=max_memory_cache,
            disk_cache_dir=cache_dir,
            max_disk_size=max_disk_cache
        )
        
        # Sessions
        self.sessions: Dict[str, MOQSession] = {}
        self.publisher_sessions: Dict[FullTrackName, List[MOQSession]] = {}
        self.subscriber_sessions: Dict[FullTrackName, List[MOQSession]] = {}
        
        # Track management
        self._track_publications: Dict[FullTrackName, int] = {}  # track -> request_id for local pub
        
        # Event handlers
        self._on_object_received: Optional[Callable] = None
        self._on_object_forwarded: Optional[Callable] = None
        
        # QUIC Server
        self._quic_server = QUICServer(
            host=host,
            port=port,
            use_datagrams=True,
            cert_file=cert_file,
            key_file=key_file
        )
        
        # Client management
        self._clients: Dict[str, ClientSession] = {}
        self._publications: Dict[FullTrackName, ClientSession] = {}
        self._subscriptions: Dict[FullTrackName, list] = {}
        self._object_cache: Dict[FullTrackName, list] = {}
        self._max_cached_objects = 1000  # Limit cache size
        self._running = False
        
        logger.info(f"MOQRelay initialized: {host}:{port} (QUIC)")
    
    async def start(self):
        """Start the relay server using QUIC transport."""
        self._running = True

        # Start each relay process with a clean on-disk cache.
        self.cache.clear_disk_cache()
        
        # Set up QUIC server handlers
        self._quic_server.set_handlers(
            on_client_connect=self._on_quic_client_connect,
            on_stream_data=self._on_quic_stream_data,
            on_datagram=self._on_quic_datagram,
            on_client_disconnect=self._on_quic_client_disconnect
        )
        
        # Start QUIC server
        await self._quic_server.start()
        
        logger.info(f"MOQ Relay running on {self.host}:{self.port} (QUIC)")
        logger.info("Waiting for connections... (Press Ctrl+C to stop)")
    
    async def stop(self):
        """Stop the relay server."""
        self._running = False
        
        # Stop the QUIC server
        if self._quic_server:
            await self._quic_server.stop()
        
        # Close all client connections
        for client in list(self._clients.values()):
            try:
                if hasattr(client.protocol, 'close'):
                    client.protocol.close()
            except:
                pass
        self._clients.clear()
        
        # Close all MOQ sessions
        for session in self.sessions.values():
            session.close()
        self.sessions.clear()
        
        logger.info("Relay server stopped")
    
    async def _on_quic_client_connect(self, protocol):
        """Handle new QUIC client connection."""
        client = self._get_or_create_client(protocol)
        logger.info(f"QUIC client connected: {client.session_id}")
    
    async def _on_quic_client_disconnect(self, protocol, error_code, reason):
        """Handle QUIC client disconnection."""
        # Find client by protocol
        for session_id, client in list(self._clients.items()):
            if client.protocol == protocol:
                await self._cleanup_client(client)
                break
        
        logger.info(f"QUIC client disconnected: error_code={error_code}, reason={reason}")
    
    async def _on_quic_stream_data(self, protocol, stream_data: StreamData):
        """Handle data received on a QUIC stream."""
        client = self._get_or_create_client(protocol)
        
        # Set control stream if not set
        if client.control_stream_id is None:
            client.control_stream_id = stream_data.stream_id
        
        if stream_data.stream_id == client.control_stream_id:
            await self._handle_control_stream_data(client, stream_data.data, end_stream=stream_data.end_stream)
        else:
            await self._handle_message(client, stream_data.data)
    
    async def _on_quic_datagram(self, protocol, datagram_data: DatagramData):
        """Handle data received as QUIC datagram."""
        client = self._get_or_create_client(protocol)
        
        # Process the message
        await self._handle_message(client, datagram_data.data)
    
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
            except Exception as e:
                logger.debug(f"Not a control message: {e}")
                pass  # Not a control message, try data message
            
            # Try to decode as ObjectDatagram (data message)
            try:
                obj, _ = ObjectDatagram.decode(data)
                await self._handle_object(client, obj)
                return
            except Exception as e:
                logger.debug(f"Not an ObjectDatagram: {e}")
                pass  # Not an ObjectDatagram either
            
            # Treat as raw data
            logger.debug(f"Received raw data: {len(data)} bytes")
            await self._forward_raw_data(client, data)
                
        except Exception as e:
            logger.error(f"Error handling message: {e}")

    async def _handle_control_stream_data(self, client: ClientSession, data: bytes, end_stream: bool = False):
        """Handle buffered control stream data from a client."""
        client.control_buffer += data

        while client.control_buffer:
            try:
                msg, consumed = decode_control_message(client.control_buffer)
            except Exception as e:
                if end_stream:
                    logger.warning(f"Failed to decode control message from {client.session_id}: {e}")
                    client.control_buffer = b""
                break

            client.control_buffer = client.control_buffer[consumed:]
            await self._dispatch_control_message(client, msg)

    async def _dispatch_control_message(self, client: ClientSession, msg: object):
        """Dispatch a decoded control message."""
        if isinstance(msg, PublishMessage):
            await self._handle_publish(client, msg)
        elif isinstance(msg, SubscribeMessage):
            await self._handle_subscribe(client, msg)
        elif isinstance(msg, FetchMessage):
            await self._handle_fetch(client, msg)
        else:
            logger.debug(f"Received control message type: {type(msg).__name__}")

    def _get_or_create_client(self, protocol) -> ClientSession:
        """Find the client session for a protocol, creating it if needed.

        QUIC connection callbacks are scheduled asynchronously, so the first
        stream or datagram can arrive before `_on_quic_client_connect` runs.
        Creating the session lazily here avoids dropping that initial message.
        """
        for client in self._clients.values():
            if client.protocol == protocol:
                return client

        session_id = f"{protocol._quic.host_cid}"
        client = ClientSession(
            session_id=session_id,
            protocol=protocol,
            quic_connection=protocol._quic
        )
        self._clients[session_id] = client
        logger.info(f"QUIC client registered lazily: {session_id}")
        return client
    
    async def _handle_publish(self, client: ClientSession, msg: PublishMessage):
        """Handle a publish request."""
        track_name = msg.full_track_name
        logger.info(f"Client {client.session_id} publishing: {track_name}")
        
        # Store publication
        self._publications[track_name] = client
        client.publications[track_name] = {
            'track_alias': msg.track_alias,
            'request_id': msg.request_id
        }
        
        # Update publisher sessions
        if track_name not in self.publisher_sessions:
            self.publisher_sessions[track_name] = []
        
        # Send PUBLISH_OK
        response = PublishOkMessage(request_id=msg.request_id)
        response_data = response.encode()
        logger.debug(f"Sending PUBLISH_OK: {len(response_data)} bytes")
        await self._send_control_message(client, response_data)
        logger.info(f"Publication accepted: {track_name}")
    
    async def _handle_subscribe(self, client: ClientSession, msg: SubscribeMessage):
        """Handle a subscribe request."""
        track_name = msg.full_track_name
        logger.info(f"Client {client.session_id} subscribing to: {track_name}")
        
        # Store subscription
        if track_name not in self._subscriptions:
            self._subscriptions[track_name] = []
        self._subscriptions[track_name].append(client)
        client.subscriptions[track_name] = {
            'track_alias': msg.track_alias,
            'request_id': msg.request_id
        }
        
        # Update subscriber sessions
        if track_name not in self.subscriber_sessions:
            self.subscriber_sessions[track_name] = []
        
        # Send SUBSCRIBE_OK
        response = SubscribeOkMessage(
            request_id=msg.request_id,
            expires=0,
            group_order=GroupOrder.ASCENDING
        )
        response_data = response.encode()
        logger.debug(f"Sending SUBSCRIBE_OK: {len(response_data)} bytes")
        await self._send_control_message(client, response_data)
        logger.info(f"Subscription accepted: {track_name}")
    
    async def _handle_fetch(self, client: ClientSession, msg: FetchMessage):
        """Handle a fetch request."""
        track_name = msg.full_track_name
        logger.info(f"Client {client.session_id} fetching from: {track_name}, "
                   f"range=[{msg.start_group}:{msg.start_object} to {msg.end_group}:{msg.end_object}]")
        
        # Check if track exists (has a publisher)
        has_cached_objects = track_name in self._object_cache and len(self._object_cache[track_name]) > 0
        if track_name not in self._publications and not has_cached_objects:
            logger.warning(f"Fetch requested for unknown track: {track_name}")
            response = RequestErrorMessage(
                request_id=msg.request_id,
                error_code=ErrorCode.INTERNAL_ERROR,
                reason="Track not found"
            )
            await self._send_control_message(client, response.encode())
            return
        
        # Store fetch request
        if track_name not in self._subscriptions:
            self._subscriptions[track_name] = []
        self._subscriptions[track_name].append(client)
        
        # Send FETCH_OK
        response = FetchOkMessage(
            request_id=msg.request_id,
            group_order=GroupOrder.ASCENDING,
            end_of_track=False
        )
        response_data = response.encode()
        logger.debug(f"Sending FETCH_OK: {len(response_data)} bytes")
        await self._send_control_message(client, response_data)
        if track_name in self._publications:
            logger.info(f"Fetch accepted: {track_name}")
        else:
            logger.info(f"Fetch accepted from cache: {track_name}")
        
        # Send cached objects that match the fetch range
        await self._send_cached_objects(client, track_name, msg)
    
    async def _send_cached_objects(self, client: ClientSession, track_name: FullTrackName, msg: FetchMessage):
        """Send cached objects that match the fetch range to the client."""
        cached_objects = self._object_cache.get(track_name, [])
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
                    track_alias=obj_data['track_alias'],
                    group_id=obj_data['group_id'],
                    object_id=obj_data['object_id'],
                    publisher_priority=obj_data['publisher_priority'],
                    object_status=obj_data['object_status']
                )
                obj = ObjectDatagram(header=header, payload=obj_data['payload'])
                
                try:
                    await self._send_datagram(client, obj.encode())
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
        
        # Also cache in the ObjectCache for advanced caching features
        cached_obj = CachedObject(
            track_alias=obj.header.track_alias,
            group_id=obj.header.group_id,
            object_id=obj.header.object_id,
            publisher_priority=obj.header.publisher_priority,
            payload=obj.payload
        )
        self.cache.put(track_name, cached_obj)
        
        # Forward to all subscribers
        await self._forward_object(track_name, obj)
    
    async def _cache_object(self, track_name: FullTrackName, obj: ObjectDatagram):
        """Cache an object for future fetch requests."""
        if track_name not in self._object_cache:
            self._object_cache[track_name] = []
        
        # Store object data
        self._object_cache[track_name].append({
            'track_alias': obj.header.track_alias,
            'group_id': obj.header.group_id,
            'object_id': obj.header.object_id,
            'publisher_priority': obj.header.publisher_priority,
            'object_status': obj.header.object_status,
            'payload': obj.payload
        })
        
        # Limit cache size
        if len(self._object_cache[track_name]) > self._max_cached_objects:
            self._object_cache[track_name].pop(0)
        
        logger.debug(f"Cached object for {track_name}: group={obj.header.group_id}, object={obj.header.object_id}")
    
    async def _forward_raw_data(self, sender: ClientSession, data: bytes):
        """Forward raw data to subscribers."""
        # Try to find which track this data belongs to
        for track_name, subscribers in self._subscriptions.items():
            # Check if sender is the publisher for this track
            if track_name in self._publications and self._publications[track_name] == sender:
                # Forward to all subscribers except sender
                for subscriber in subscribers:
                    if subscriber.session_id != sender.session_id:
                        try:
                            await self._send_data_stream(subscriber, data)
                        except Exception as e:
                            logger.error(f"Error forwarding to {subscriber.session_id}: {e}")
                break
    
    async def _forward_object(self, track_name: FullTrackName, obj: ObjectDatagram):
        """Forward an object to all subscribers of a track."""
        subscribers = self._subscriptions.get(track_name, [])
        if not subscribers:
            return

        data = obj.encode()
        forwarded = 0

        for subscriber in subscribers:
            try:
                await self._send_datagram(subscriber, data)
                forwarded += 1
            except Exception as e:
                logger.error(f"Error forwarding to {subscriber.session_id}: {e}")
        
        if forwarded > 0:
            logger.debug(f"Forwarded object to {forwarded} subscribers")
        
        # Call event handler if set
        if self._on_object_forwarded:
            try:
                self._on_object_forwarded(track_name, obj)
            except Exception as e:
                logger.error(f"Error in forward handler: {e}")
    
    async def _send_control_message(self, client: ClientSession, data: bytes):
        """Send a message to a client over QUIC."""
        try:
            if client.control_stream_id is not None:
                # Send on control stream
                client.quic_connection.send_stream_data(client.control_stream_id, data)
            else:
                # Open a new stream or use datagram
                stream_id = client.quic_connection.get_next_available_stream_id(is_unidirectional=False)
                client.quic_connection.send_stream_data(stream_id, data)
            
            # Transmit the data
            if hasattr(client.protocol, 'transmit'):
                client.protocol.transmit()
            
            logger.debug(f"Sent {len(data)} bytes to {client.session_id}")
        except Exception as e:
            logger.error(f"Error sending message to {client.session_id}: {e}")
            raise

    async def _send_datagram(self, client: ClientSession, data: bytes):
        """Send a datagram to a client over QUIC."""
        try:
            client.quic_connection.send_datagram_frame(data)
            if hasattr(client.protocol, 'transmit'):
                client.protocol.transmit()
            logger.debug(f"Sent datagram {len(data)} bytes to {client.session_id}")
        except Exception as e:
            logger.error(f"Error sending datagram to {client.session_id}: {e}")
            raise

    async def _send_data_stream(self, client: ClientSession, data: bytes):
        """Send opaque raw data on a fresh unidirectional stream."""
        try:
            stream_id = client.quic_connection.get_next_available_stream_id(is_unidirectional=True)
            client.quic_connection.send_stream_data(stream_id, data, end_stream=True)
            if hasattr(client.protocol, 'transmit'):
                client.protocol.transmit()
            logger.debug(f"Sent raw stream {len(data)} bytes to {client.session_id} on stream {stream_id}")
        except Exception as e:
            logger.error(f"Error sending raw stream data to {client.session_id}: {e}")
            raise
    
    async def _cleanup_client(self, client: ClientSession):
        """Clean up when a client disconnects."""
        logger.info(f"Client disconnected: {client.session_id}")
        
        # Remove from clients
        if client.session_id in self._clients:
            del self._clients[client.session_id]
        
        # Remove publications
        for track_name in list(client.publications.keys()):
            if track_name in self._publications and self._publications[track_name].session_id == client.session_id:
                del self._publications[track_name]
                logger.info(f"Publication removed: {track_name}")
        
        # Remove subscriptions
        for track_name in list(client.subscriptions.keys()):
            if track_name in self._subscriptions:
                self._subscriptions[track_name] = [
                    s for s in self._subscriptions[track_name] 
                    if s.session_id != client.session_id
                ]
                if not self._subscriptions[track_name]:
                    del self._subscriptions[track_name]
        
        # Keep cached objects available for later FETCH requests.
    
    def register_session(self, session: MOQSession):
        """Register a new session."""
        self.sessions[session.session_id] = session
        
        # Set up session handlers
        session.set_handlers(
            on_subscribe=self._on_subscribe,
            on_publish=self._on_publish
        )
        
        logger.info(f"Registered session: {session.session_id}")
    
    def unregister_session(self, session: MOQSession):
        """Unregister a session."""
        if session.session_id in self.sessions:
            del self.sessions[session.session_id]
            logger.info(f"Unregistered session: {session.session_id}")
    
    def _on_subscribe(self, msg: SubscribeMessage):
        """Handle incoming subscription."""
        track_name = msg.full_track_name
        logger.info(f"Subscription request: {track_name}")
        
        session = self.sessions.get(msg.subscriber.session_id) if msg.subscriber else None
        if not session:
            logger.warning(f"Unknown session for subscription")
            return
        
        # Register subscriber
        if track_name not in self.subscriber_sessions:
            self.subscriber_sessions[track_name] = []
        if session not in self.subscriber_sessions[track_name]:
            self.subscriber_sessions[track_name].append(session)
        
        # Forward subscription upstream if needed
        self._forward_subscribe(msg)
    
    def _on_publish(self, msg: PublishMessage):
        """Handle incoming publication."""
        track_name = msg.full_track_name
        logger.info(f"Publication received: {track_name}")
        
        session = self.sessions.get(msg.publisher.session_id) if msg.publisher else None
        if not session:
            logger.warning(f"Unknown session for publication")
            return
        
        # Register publisher
        if track_name not in self.publisher_sessions:
            self.publisher_sessions[track_name] = []
        if session not in self.publisher_sessions[track_name]:
            self.publisher_sessions[track_name].append(session)
    
    def _forward_subscribe(self, msg: SubscribeMessage):
        """Forward subscription to appropriate publisher."""
        track_name = msg.full_track_name
        
        # Find publisher session
        publishers = self.publisher_sessions.get(track_name, [])
        if not publishers:
            logger.info(f"No publisher found for {track_name}, waiting...")
            return
        
        # Forward to first available publisher
        publisher = publishers[0]
        logger.info(f"Forwarding subscription for {track_name} to publisher")
        
        # Create subscription on publisher session
        # This would create a subscription from relay to publisher
    
    def cache_object(self, track_name: FullTrackName, obj: CachedObject):
        """Cache an object."""
        self.cache.put(track_name, obj)
        logger.debug(f"Cached object: {track_name} @ group={obj.group_id}, object={obj.object_id}")
    
    async def serve_cached_objects(self, session: MOQSession, track_name: FullTrackName,
                                   start: Location, end: Location):
        """Serve cached objects to a subscriber."""
        objects = self.cache.get_range(track_name, start, end)
        
        if not objects:
            logger.info(f"No cached objects for {track_name} in range [{start}, {end}]")
            return
        
        logger.info(f"Serving {len(objects)} cached objects for {track_name}")
        
        for obj in objects:
            # TODO: Send object to subscriber via data stream
            pass
    
    def get_cache_stats(self) -> dict:
        """Get cache statistics."""
        return self.cache.get_statistics()
    
    def set_object_handler(self, handler: Callable):
        """Set handler for received objects."""
        self._on_object_received = handler
    
    def set_forward_handler(self, handler: Callable):
        """Set handler for forwarded objects."""
        self._on_object_forwarded = handler
