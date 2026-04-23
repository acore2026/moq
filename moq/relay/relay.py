"""
MOQ Transport - Relay Implementation
Implements a caching relay for MOQT with memory and disk caching.
Uses QUIC or WebTransport as the underlying transport protocol.
"""

import os
import json
import asyncio
import inspect
import logging
import hashlib
import shutil
from typing import Dict, List, Optional, Set, Tuple, Callable, Awaitable
from dataclasses import dataclass, field, asdict
from datetime import datetime, timedelta
from pathlib import Path
import threading

from moq.session import MOQSession, Role, Subscription, Publication
from moq.messages import (
    SubscribeMessage, PublishMessage, ObjectHeader, ObjectDatagram,
    SubscribeOkMessage, PublishOkMessage, PublishDoneMessage,
    decode_control_message, GroupOrder, FetchMessage, FetchOkMessage,
    ParameterType, RequestOkMessage, RequestErrorMessage, RequestUpdateMessage,
    ErrorCode, SubgroupHeader, SubgroupObject, SubscriptionFilterValue,
    StreamType, ObjectStatus, SubscribeFilter, FetchHeader, FetchObject,
    group_order_from_parameter_value,
    is_subgroup_stream_type,
    PublishDoneStatus,
    StreamResetCode,
    UNKNOWN_PUBLISH_DONE_STREAM_COUNT,
)
from moq.encoding import FullTrackName, Location, Parameters, VarInt
from moq.transport import (
    CombinedTransportServer,
    QUICServer,
    WebTransportServer,
    StreamData,
    StreamResetData,
    DatagramData,
    is_unidirectional_stream_id,
    is_quic_available,
    is_webtransport_available,
)

logger = logging.getLogger(__name__)

CLIENT_SEND_QUEUE_MAX_ITEMS = 256
SEND_PRIORITY_CONTROL = 0
SEND_PRIORITY_FETCH = 5
SEND_PRIORITY_DATA = 10


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
    """Represents a connected client session over QUIC or WebTransport."""
    session_id: str
    protocol: any  # MOQQuicProtocol instance
    quic_connection: any  # QuicConnection or WebTransport session adapter
    role: Optional[Role] = None
    subscriptions: Dict[FullTrackName, dict] = None
    publications: Dict[FullTrackName, dict] = None
    control_stream_id: Optional[int] = None
    local_control_stream_id: Optional[int] = None
    control_buffer: bytes = b""
    request_stream_buffers: Dict[int, bytearray] = None
    data_streams: Dict[int, dict] = None
    outbound_send_queue: Optional[asyncio.PriorityQueue] = None
    outbound_send_task: Optional[asyncio.Task] = None
    outbound_send_sequence: int = 0
    next_track_alias: int = 0
    closing: bool = False
    accepting_data_sends: bool = True
    
    def __post_init__(self):
        if self.subscriptions is None:
            self.subscriptions = {}
        if self.publications is None:
            self.publications = {}
        if self.request_stream_buffers is None:
            self.request_stream_buffers = {}
        if self.data_streams is None:
            self.data_streams = {}


@dataclass
class InboundDataStream:
    """Incremental parser and forwarding state for an incoming data stream."""
    parse_buffer: bytearray = field(default_factory=bytearray)
    forward_objects: List[SubgroupObject] = field(default_factory=list)
    buffered_forward_bytes: int = 0
    stream_type: Optional[int] = None
    subgroup_header: Optional[object] = None
    track_name: Optional[FullTrackName] = None
    downstream_streams: Dict[str, int] = field(default_factory=dict)
    last_object_id: Optional[int] = None
    downstream_last_object_ids: Dict[str, Optional[int]] = field(default_factory=dict)


@dataclass
class OutboundSendOperation:
    """One serialized outbound send for a specific client."""
    priority: int
    sequence: int
    execute: Callable[[], Awaitable[None]]
    description: str
    future: Optional[asyncio.Future] = None


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

    def clear_track(self, track_name: FullTrackName):
        """Remove one track from memory and disk cache."""
        track_id = self._get_track_id(track_name)

        with self._lock:
            cached_track = self._memory_cache.pop(track_name, None)
            if cached_track is not None:
                self._memory_size -= sum(len(obj.payload) for obj in cached_track.values())

            if not self.disk_cache_dir:
                return

            track_dir = self.disk_cache_dir / track_id
            try:
                if track_dir.exists():
                    removed_size = sum(
                        path.stat().st_size
                        for path in track_dir.rglob("*")
                        if path.is_file()
                    )
                    shutil.rmtree(track_dir)
                    self._disk_size = max(0, self._disk_size - removed_size)
                self._disk_index.pop(track_id, None)
                self._save_disk_index()
                logger.info("Cleared cached track %s from disk cache", track_name)
            except Exception as e:
                logger.warning("Failed to clear cached track %s: %s", track_name, e)
    
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
                 transport: str = "quic",
                 webtransport_port: Optional[int] = None,
                 webtransport_path: str = "/moq",
                 cert_file: Optional[str] = None,
                 key_file: Optional[str] = None):
        self.host = host
        self.port = port
        self.transport = transport.lower()
        self.webtransport_port = (
            webtransport_port
            if webtransport_port is not None
            else port
        )
        self.webtransport_path = webtransport_path
        
        if self.transport == "quic":
            if not is_quic_available():
                raise RuntimeError("QUIC is not available. Please install aioquic.")
        elif self.transport == "webtransport":
            if not is_webtransport_available():
                raise RuntimeError("WebTransport is not available. Please install aioquic.")
        elif self.transport == "both":
            if not is_quic_available():
                raise RuntimeError("QUIC is not available. Please install aioquic.")
            if not is_webtransport_available():
                raise RuntimeError("WebTransport is not available. Please install aioquic.")
        else:
            raise ValueError(f"Unsupported transport: {transport}")
        
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
        
        self._quic_server = None
        self._webtransport_server = None
        self._combined_server = None

        if self.transport == "both" and self.webtransport_port == port:
            self._combined_server = CombinedTransportServer(
                host=host,
                port=port,
                path=webtransport_path,
                use_datagrams=True,
                cert_file=cert_file,
                key_file=key_file,
            )
            self._quic_server = self._combined_server
            self._webtransport_server = self._combined_server
        elif self.transport in {"quic", "both"}:
            self._quic_server = QUICServer(
                host=host,
                port=port,
                use_datagrams=True,
                cert_file=cert_file,
                key_file=key_file
            )
        if self.transport == "webtransport" or (
            self.transport == "both" and self.webtransport_port != port
        ):
            self._webtransport_server = WebTransportServer(
                host=host,
                port=self.webtransport_port,
                path=webtransport_path,
                use_datagrams=True,
                cert_file=cert_file,
                key_file=key_file,
            )
        
        # Client management
        self._clients: Dict[str, ClientSession] = {}
        self._publications: Dict[FullTrackName, ClientSession] = {}
        self._subscriptions: Dict[FullTrackName, list] = {}
        self._object_cache: Dict[FullTrackName, list] = {}
        self._max_cached_objects = 1000  # Limit cache size
        self._running = False
        
        logger.info(
            "MOQRelay initialized: host=%s quic_port=%s webtransport_port=%s transport=%s",
            host,
            port if self._quic_server is not None else None,
            self.webtransport_port if self._webtransport_server is not None else None,
            self.transport,
        )
    
    async def start(self):
        """Start the relay server using the configured transport."""
        self._running = True

        # Start each relay process with a clean on-disk cache.
        self.cache.clear_disk_cache()
        
        for server in self._iter_transport_servers():
            server.set_handlers(
                on_client_connect=self._on_quic_client_connect,
                on_stream_data=self._on_quic_stream_data,
                on_stream_reset=self._on_quic_stream_reset,
                on_datagram=self._on_quic_datagram,
                on_client_disconnect=self._on_quic_client_disconnect
            )
            await server.start()

        if self._quic_server is not None:
            logger.info("MOQ Relay QUIC listener on %s:%d", self.host, self.port)
        if self._webtransport_server is not None:
            logger.info(
                "MOQ Relay WebTransport listener on https://%s:%d%s",
                self.host,
                self.webtransport_port,
                self.webtransport_path,
            )
        logger.info(f"MOQ Relay running ({self.transport})")
        logger.info("Waiting for connections... (Press Ctrl+C to stop)")
    
    async def stop(self):
        """Stop the relay server."""
        self._running = False
        
        for server in self._iter_transport_servers():
            await server.stop()
        
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

    def _iter_transport_servers(self):
        """Yield configured listener instances once each."""
        seen = set()
        for server in (self._quic_server, self._webtransport_server):
            if server is None:
                continue
            marker = id(server)
            if marker in seen:
                continue
            seen.add(marker)
            yield server
    
    async def _on_quic_client_connect(self, protocol):
        """Handle new transport client connection."""
        client = self._get_or_create_client(protocol)
        logger.info(f"Client connected: {client.session_id}")
    
    async def _on_quic_client_disconnect(self, protocol, error_code, reason):
        """Handle transport client disconnection."""
        for session_id, client in list(self._clients.items()):
            if client.protocol == self._resolve_protocol(protocol):
                await self._cleanup_client(client)
                break
        
        logger.info(f"Client disconnected: error_code={error_code}, reason={reason}")
    
    async def _on_quic_stream_data(self, protocol, stream_data: StreamData):
        """Handle data received on a transport stream."""
        client = self._get_or_create_client(protocol)

        if stream_data.stream_id == client.control_stream_id:
            await self._handle_control_stream_data(client, stream_data.data, end_stream=stream_data.end_stream)
        elif client.control_stream_id is None and is_unidirectional_stream_id(stream_data.stream_id):
            client.control_stream_id = stream_data.stream_id
            await self._handle_control_stream_data(client, stream_data.data, end_stream=stream_data.end_stream)
        elif not is_unidirectional_stream_id(stream_data.stream_id):
            await self._handle_request_stream_data(
                client,
                stream_data.stream_id,
                stream_data.data,
                end_stream=stream_data.end_stream,
            )
        else:
            await self._handle_data_stream(client, stream_data)

    async def _on_quic_stream_reset(self, protocol, stream_reset: StreamResetData):
        """Handle peer-initiated stream reset / STOP_SENDING."""
        client = self._get_or_create_client(protocol)
        logger.info(
            "Client %s stream termination: stream_id=%s type=%s error=%s",
            client.session_id,
            stream_reset.stream_id,
            stream_reset.event_type,
            stream_reset.error_code,
        )

        client.request_stream_buffers.pop(stream_reset.stream_id, None)
        if client.control_stream_id == stream_reset.stream_id:
            client.control_stream_id = None
            client.control_buffer = b""
        client.data_streams.pop(stream_reset.stream_id, None)

        for track_name, publication in list(client.publications.items()):
            if publication.get("request_stream_id") == stream_reset.stream_id:
                status_code, reason = self._map_stream_termination_to_publish_done(stream_reset)
                logger.info(
                    "Cancelling publication for %s on %s due to request stream %s %s",
                    track_name,
                    client.session_id,
                    stream_reset.event_type,
                    stream_reset.stream_id,
                )
                await self._notify_track_subscriptions_ended(
                    track_name,
                    status_code,
                    reason,
                )
                self._remove_client_publication(client, track_name)

        for track_name, subscription in list(client.subscriptions.items()):
            if subscription.get("request_stream_id") == stream_reset.stream_id:
                logger.info(
                    "Cancelling subscription for %s on %s due to request stream %s %s",
                    track_name,
                    client.session_id,
                    stream_reset.event_type,
                    stream_reset.stream_id,
                )
                self._remove_client_subscription(client, track_name)
    
    async def _on_quic_datagram(self, protocol, datagram_data: DatagramData):
        """Handle data received as a transport datagram."""
        client = self._get_or_create_client(protocol)
        
        # Process the message
        await self._handle_message(client, datagram_data.data)

    async def _handle_data_stream(self, client: ClientSession, stream_data: StreamData):
        """Handle an incoming QUIC data stream incrementally."""
        state = client.data_streams.get(stream_data.stream_id)
        if state is None:
            state = InboundDataStream()
            client.data_streams[stream_data.stream_id] = state

        state.parse_buffer.extend(stream_data.data)

        try:
            await self._parse_data_stream(client, state, end_stream=stream_data.end_stream)
        except ValueError:
            pass
        except Exception as e:
            logger.warning(
                f"Failed to parse data stream {stream_data.stream_id} from {client.session_id}: {e}"
            )

        if state.track_name and state.forward_objects:
            await self._flush_forward_buffer(
                state.track_name,
                state,
                end_stream=stream_data.end_stream,
            )

        if stream_data.end_stream:
            if state.parse_buffer:
                logger.warning(
                    f"Data stream {stream_data.stream_id} from {client.session_id} ended "
                    f"with {len(state.parse_buffer)} buffered bytes"
                )
            client.data_streams.pop(stream_data.stream_id, None)

    async def _parse_data_stream(
        self,
        client: ClientSession,
        state: InboundDataStream,
        end_stream: bool = False
    ):
        """Parse incoming stream bytes far enough to identify and cache subgroup objects."""
        buffer = state.parse_buffer

        if state.stream_type is None:
            stream_type, consumed = VarInt.decode(buffer, 0)
            state.stream_type = stream_type
            del buffer[:consumed]

        if not is_subgroup_stream_type(state.stream_type):
            raise RuntimeError(f"unsupported stream type {state.stream_type}")

        if state.subgroup_header is None:
            header, consumed = SubgroupHeader.decode(buffer, 0)
            state.subgroup_header = header
            del buffer[:consumed]

            for track_name, publication in client.publications.items():
                if publication["track_alias"] == header.track_alias:
                    state.track_name = track_name
                    break

            if state.track_name is None:
                raise RuntimeError(f"unknown track alias {header.track_alias}")

        header = state.subgroup_header

        while buffer:
            try:
                subgroup_obj, consumed = SubgroupObject.decode(
                    buffer,
                    0,
                    previous_object_id=state.last_object_id,
                )
            except ValueError:
                break

            del buffer[:consumed]
            state.last_object_id = subgroup_obj.object_id
            await self._store_object(
                state.track_name,
                ObjectHeader(
                    track_alias=header.track_alias,
                    group_id=header.group_id,
                    object_id=subgroup_obj.object_id,
                    publisher_priority=header.publisher_priority,
                    object_status=subgroup_obj.object_status,
                ),
                subgroup_obj.payload,
            )
            self._append_forward_subgroup_object(state, subgroup_obj)

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
                elif isinstance(msg, RequestUpdateMessage):
                    await self._handle_request_update(client, msg)
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

    async def _handle_request_stream_data(
        self,
        client: ClientSession,
        stream_id: int,
        data: bytes,
        end_stream: bool = False,
    ) -> None:
        """Handle control messages arriving on a bidirectional request stream."""
        buffer = client.request_stream_buffers.setdefault(stream_id, bytearray())
        buffer.extend(data)

        while buffer:
            try:
                msg, consumed = decode_control_message(bytes(buffer))
            except Exception as e:
                if end_stream:
                    logger.warning(
                        "Failed to decode request stream %s from %s: %s",
                        stream_id,
                        client.session_id,
                        e,
                    )
                    buffer.clear()
                break

            del buffer[:consumed]
            await self._dispatch_control_message(client, msg, response_stream_id=stream_id)

        if end_stream:
            client.request_stream_buffers.pop(stream_id, None)

    async def _dispatch_control_message(
        self,
        client: ClientSession,
        msg: object,
        response_stream_id: Optional[int] = None,
    ):
        """Dispatch a decoded control message."""
        if isinstance(msg, PublishMessage):
            await self._call_control_handler(
                self._handle_publish,
                client,
                msg,
                response_stream_id=response_stream_id,
            )
        elif isinstance(msg, SubscribeMessage):
            await self._call_control_handler(
                self._handle_subscribe,
                client,
                msg,
                response_stream_id=response_stream_id,
            )
        elif isinstance(msg, FetchMessage):
            await self._call_control_handler(
                self._handle_fetch,
                client,
                msg,
                response_stream_id=response_stream_id,
            )
        elif isinstance(msg, RequestUpdateMessage):
            await self._call_control_handler(
                self._handle_request_update,
                client,
                msg,
                response_stream_id=response_stream_id,
            )
        else:
            logger.debug(f"Received control message type: {type(msg).__name__}")

    async def _call_control_handler(
        self,
        handler: Callable,
        client: ClientSession,
        msg: object,
        response_stream_id: Optional[int] = None,
    ) -> None:
        """Call a control handler, tolerating legacy two-argument test stubs."""
        try:
            await handler(client, msg, response_stream_id=response_stream_id)
        except TypeError:
            await handler(client, msg)

    def _merge_parameters(
        self,
        current: Optional[Parameters],
        updates: Optional[Parameters],
    ) -> Parameters:
        """Merge request parameters for REQUEST_UPDATE semantics."""
        merged = Parameters()
        if current is not None:
            merged.params.update(current.params)
        if updates is not None:
            merged.params.update(updates.params)
        return merged

    def _largest_known_location(self, track_name: FullTrackName) -> Optional[Tuple[int, int]]:
        """Return the largest cached location for a track."""
        cached_objects = self._object_cache.get(track_name, [])
        if not cached_objects:
            return None
        largest = max(
            cached_objects,
            key=lambda obj: (obj['group_id'], obj['object_id']),
        )
        return largest['group_id'], largest['object_id']

    def _resolve_fetch_end_location(
        self,
        track_name: FullTrackName,
        msg: FetchMessage,
    ) -> Tuple[bool, Location]:
        """Resolve a draft-17 style FETCH_OK end location from current cache state."""
        largest_known = self._largest_known_location(track_name)

        if msg.end_group is not None and msg.end_object is not None:
            return False, Location(msg.end_group, msg.end_object)

        if largest_known is None:
            return False, Location(msg.start_group, msg.start_object)

        largest_group, largest_object = largest_known
        end_of_track = track_name not in self._publications
        return end_of_track, Location(largest_group, largest_object + 1)

    def _subscription_stream_count(self, subscription: dict) -> int:
        """Return the number of data streams opened for a live subscription."""
        opened_stream_ids = subscription.get('opened_data_stream_ids')
        if not opened_stream_ids:
            return 0
        return len(opened_stream_ids)

    def _apply_subscription_parameter_state(
        self,
        track_name: FullTrackName,
        subscription: dict,
        parameters: Optional[Parameters],
    ) -> bool:
        """Apply supported draft-17 subscription parameters onto internal state."""
        if parameters is None:
            return False

        filter_updated = False

        subscriber_priority = parameters.get(ParameterType.SUBSCRIBER_PRIORITY)
        if subscriber_priority is not None:
            subscription['subscriber_priority'] = subscriber_priority

        group_order = parameters.get(ParameterType.GROUP_ORDER)
        if group_order is not None:
            subscription['group_order'] = group_order_from_parameter_value(group_order)

        filter_value = parameters.get(ParameterType.SUBSCRIPTION_FILTER)
        if filter_value is not None:
            filter_updated = True
            decoded_filter, _ = SubscriptionFilterValue.decode(filter_value)
            largest_location = self._largest_known_location(track_name)

            subscription['filter_type'] = decoded_filter.filter_type
            subscription['end_object'] = None

            if decoded_filter.filter_type == SubscribeFilter.LATEST_GROUP:
                if largest_location is None:
                    subscription['start_group'] = 0
                    subscription['start_object'] = 0
                else:
                    subscription['start_group'] = largest_location[0] + 1
                    subscription['start_object'] = 0
                subscription['end_group'] = None
            elif decoded_filter.filter_type == SubscribeFilter.LATEST_OBJECT:
                if largest_location is None:
                    subscription['start_group'] = 0
                    subscription['start_object'] = 0
                else:
                    subscription['start_group'] = largest_location[0]
                    subscription['start_object'] = largest_location[1] + 1
                subscription['end_group'] = None
            elif decoded_filter.filter_type == SubscribeFilter.ABSOLUTE_START:
                subscription['start_group'] = decoded_filter.start_group
                subscription['start_object'] = decoded_filter.start_object
                subscription['end_group'] = None
            elif decoded_filter.filter_type == SubscribeFilter.ABSOLUTE_RANGE:
                subscription['start_group'] = decoded_filter.start_group
                subscription['start_object'] = decoded_filter.start_object
                subscription['end_group'] = (
                    decoded_filter.start_group + decoded_filter.end_group_delta
                    if decoded_filter.end_group_delta is not None
                    else None
                )
            else:
                subscription['start_group'] = None
                subscription['start_object'] = None
                subscription['end_group'] = None

        return filter_updated

    def _resolve_protocol(self, protocol):
        """Normalize callback objects to the owning protocol."""
        return getattr(protocol, "protocol", protocol)

    def _resolve_connection(self, protocol):
        """Normalize callback objects to a connection adapter."""
        if hasattr(protocol, "open_stream") and hasattr(protocol, "send_stream_data"):
            return protocol
        resolved_protocol = self._resolve_protocol(protocol)
        return getattr(resolved_protocol, "_quic", resolved_protocol)

    def _derive_session_id(self, protocol) -> str:
        """Build a stable session identifier from the underlying QUIC connection."""
        resolved_protocol = self._resolve_protocol(protocol)
        quic = getattr(resolved_protocol, "_quic", None)
        host_cid = getattr(quic, "host_cid", None)
        if host_cid is not None:
            return f"{host_cid}"
        return f"client-{id(protocol)}"

    async def _open_stream(self, client: ClientSession, unidirectional: bool) -> int:
        """Open a stream on either a native QUIC or WebTransport connection."""
        open_stream = getattr(client.quic_connection, "open_stream", None)
        if callable(open_stream):
            result = open_stream(unidirectional=unidirectional)
            if inspect.isawaitable(result):
                return await result
            return result
        get_next_stream_id = getattr(client.quic_connection, "get_next_available_stream_id", None)
        if callable(get_next_stream_id):
            return get_next_stream_id(is_unidirectional=unidirectional)
        if client.control_stream_id is not None:
            return client.control_stream_id
        raise AttributeError("connection does not support opening streams")

    async def _send_stream_bytes(
        self,
        client: ClientSession,
        stream_id: int,
        data: bytes,
        end_stream: bool = False,
    ) -> None:
        """Send bytes on a stream regardless of connection type."""
        result = client.quic_connection.send_stream_data(stream_id, data, end_stream=end_stream)
        if inspect.isawaitable(result):
            await result
        elif hasattr(client.protocol, "transmit"):
            client.protocol.transmit()

    async def _send_datagram_bytes(self, client: ClientSession, data: bytes) -> None:
        """Send a datagram regardless of connection type."""
        send_datagram = getattr(client.quic_connection, "send_datagram", None)
        if callable(send_datagram):
            result = send_datagram(data)
        else:
            result = client.quic_connection.send_datagram_frame(data)
        if inspect.isawaitable(result):
            await result
        elif hasattr(client.protocol, "transmit"):
            client.protocol.transmit()

    def _ensure_client_send_worker(self, client: ClientSession) -> None:
        """Create the outbound worker for a client if it does not exist yet."""
        if client.outbound_send_queue is None:
            client.outbound_send_queue = asyncio.PriorityQueue(maxsize=CLIENT_SEND_QUEUE_MAX_ITEMS)

        if client.outbound_send_task is None or client.outbound_send_task.done():
            client.outbound_send_task = asyncio.create_task(
                self._client_send_worker(client),
                name=f"relay-send-{client.session_id}",
            )

    async def _client_send_worker(self, client: ClientSession) -> None:
        """Serialize all outbound writes for one client while allowing cross-client parallelism."""
        assert client.outbound_send_queue is not None

        while True:
            operation: Optional[OutboundSendOperation] = None
            try:
                _, _, operation = await client.outbound_send_queue.get()
                await operation.execute()
                if operation.future is not None and not operation.future.done():
                    operation.future.set_result(None)
            except asyncio.CancelledError:
                if operation is not None and operation.future is not None and not operation.future.done():
                    operation.future.set_exception(asyncio.CancelledError())
                raise
            except Exception as e:
                logger.error(
                    "Outbound send failed for %s (%s): %s",
                    client.session_id,
                    operation.description if operation is not None else "unknown",
                    e,
                )
                if operation is not None and operation.future is not None and not operation.future.done():
                    operation.future.set_exception(e)
                self._schedule_client_disconnect(client, f"send failure: {e}")
            finally:
                if operation is not None and client.outbound_send_queue is not None:
                    client.outbound_send_queue.task_done()

    async def _enqueue_client_send(
        self,
        client: ClientSession,
        execute: Callable[[], Awaitable[None]],
        description: str,
        priority: int,
        wait: bool = True,
        allow_while_closing: bool = False,
        is_data_send: bool = True,
    ) -> None:
        """Queue an outbound send for a client."""
        if client.closing and not allow_while_closing:
            raise RuntimeError(f"client {client.session_id} is closing")
        if is_data_send and not client.accepting_data_sends:
            raise RuntimeError(f"client {client.session_id} is not accepting data sends")

        self._ensure_client_send_worker(client)
        assert client.outbound_send_queue is not None

        future: Optional[asyncio.Future] = None
        if wait:
            future = asyncio.get_running_loop().create_future()

        operation = OutboundSendOperation(
            priority=priority,
            sequence=client.outbound_send_sequence,
            execute=execute,
            description=description,
            future=future,
        )
        client.outbound_send_sequence += 1

        try:
            client.outbound_send_queue.put_nowait((operation.priority, operation.sequence, operation))
        except asyncio.QueueFull as e:
            logger.warning(
                "Outbound send queue is full for %s while scheduling %s",
                client.session_id,
                description,
            )
            if future is not None and not future.done():
                future.set_exception(e)
            self._schedule_client_disconnect(client, "outbound send queue full")
            raise

        if future is not None:
            await future

    def _cancel_client_send_worker(self, client: ClientSession) -> None:
        """Cancel the worker and fail any queued outbound operations."""
        self._drain_client_send_queue(client, exc=asyncio.CancelledError())

        if client.outbound_send_task is not None and not client.outbound_send_task.done():
            client.outbound_send_task.cancel()

        client.outbound_send_task = None
        client.outbound_send_queue = None

    def _drain_client_send_queue(self, client: ClientSession, exc: BaseException) -> None:
        """Fail and discard all queued outbound operations for a client."""
        queue = client.outbound_send_queue
        if queue is None:
            return

        while not queue.empty():
            try:
                _, _, operation = queue.get_nowait()
            except asyncio.QueueEmpty:
                break
            if operation.future is not None and not operation.future.done():
                operation.future.set_exception(exc)
            queue.task_done()

    def _schedule_client_disconnect(self, client: ClientSession, reason: str) -> None:
        """Disconnect a client once when it falls behind or its sends fail."""
        if client.closing:
            return

        client.closing = True
        client.accepting_data_sends = False
        asyncio.create_task(self._disconnect_client(client, reason))

    async def _disconnect_client(self, client: ClientSession, reason: str) -> None:
        """Disconnect and clean up a client session."""
        logger.warning("Disconnecting client %s: %s", client.session_id, reason)
        self._drain_client_send_queue(client, exc=RuntimeError(reason))
        try:
            await asyncio.wait_for(
                self._notify_client_subscriptions_ended(
                    client,
                    PublishDoneStatus.TOO_FAR_BEHIND,
                    reason,
                ),
                timeout=0.15,
            )
        except asyncio.TimeoutError:
            logger.warning("Timed out draining control notifications for %s", client.session_id)
        finally:
            self._cancel_client_send_worker(client)
        await self._cleanup_client(client)

        close = getattr(client.protocol, "close", None)
        if callable(close):
            result = close()
            if inspect.isawaitable(result):
                await result

    async def _notify_client_subscriptions_ended(
        self,
        client: ClientSession,
        status_code: PublishDoneStatus,
        reason: str,
    ) -> None:
        """Best-effort notification that all live subscriptions for a client are ending."""
        if not client.subscriptions:
            return

        if client.local_control_stream_id is None:
            try:
                client.local_control_stream_id = await self._open_stream(
                    client,
                    unidirectional=True,
                )
            except Exception as e:
                logger.warning(
                    "Failed to open control stream while ending subscriptions for %s: %s",
                    client.session_id,
                    e,
                )
                return

        for subscription in client.subscriptions.values():
            message = PublishDoneMessage(
                request_id=subscription['request_id'],
                status_code=int(status_code),
                stream_count=self._subscription_stream_count(subscription),
                reason=reason,
            )
            try:
                await self._enqueue_client_send(
                    client,
                    lambda control_stream_id=client.local_control_stream_id, payload=message.encode(): self._send_stream_bytes(
                        client,
                        control_stream_id,
                        payload,
                    ),
                    description=f"publish_done request {subscription['request_id']}",
                    priority=SEND_PRIORITY_CONTROL,
                    wait=True,
                    allow_while_closing=True,
                    is_data_send=False,
                )
            except Exception as e:
                logger.warning(
                    "Failed to send PUBLISH_DONE to %s for request %s: %s",
                    client.session_id,
                    subscription['request_id'],
                    e,
                )

    def _remove_client_publication(
        self,
        client: ClientSession,
        track_name: FullTrackName,
    ) -> None:
        """Remove one publication from relay bookkeeping."""
        publication = client.publications.pop(track_name, None)
        if publication is None:
            return
        if self._publications.get(track_name) is client:
            del self._publications[track_name]
        sessions = self.publisher_sessions.get(track_name)
        if sessions is not None:
            self.publisher_sessions[track_name] = [
                session for session in sessions if session.session_id != client.session_id
            ]
            if not self.publisher_sessions[track_name]:
                del self.publisher_sessions[track_name]

    def _remove_client_subscription(
        self,
        client: ClientSession,
        track_name: FullTrackName,
    ) -> None:
        """Remove one live subscription from relay bookkeeping."""
        removed = client.subscriptions.pop(track_name, None)
        if removed is None:
            return
        subscribers = self._subscriptions.get(track_name)
        if subscribers is not None:
            self._subscriptions[track_name] = [
                subscriber for subscriber in subscribers if subscriber.session_id != client.session_id
            ]
            if not self._subscriptions[track_name]:
                del self._subscriptions[track_name]
        sessions = self.subscriber_sessions.get(track_name)
        if sessions is not None:
            self.subscriber_sessions[track_name] = [
                session for session in sessions if session.session_id != client.session_id
            ]
            if not self.subscriber_sessions[track_name]:
                del self.subscriber_sessions[track_name]

    def _map_stream_termination_to_publish_done(
        self,
        stream_reset: StreamResetData,
    ) -> Tuple[PublishDoneStatus, str]:
        """Translate request-stream termination into a downstream PUBLISH_DONE."""
        reason = f"publication request stream {stream_reset.event_type}"
        try:
            reset_code = StreamResetCode(stream_reset.error_code)
        except ValueError:
            return PublishDoneStatus.TRACK_ENDED, reason

        status_map = {
            StreamResetCode.INTERNAL_ERROR: PublishDoneStatus.INTERNAL_ERROR,
            StreamResetCode.SESSION_CLOSED: PublishDoneStatus.GOING_AWAY,
            StreamResetCode.TOO_FAR_BEHIND: PublishDoneStatus.TOO_FAR_BEHIND,
            StreamResetCode.EXCESSIVE_LOAD: PublishDoneStatus.EXCESSIVE_LOAD,
            StreamResetCode.MALFORMED_TRACK: PublishDoneStatus.MALFORMED_TRACK,
        }
        status = status_map.get(reset_code, PublishDoneStatus.TRACK_ENDED)
        if reset_code != StreamResetCode.CANCELLED:
            reason = f"{reason}: {reset_code.name}"
        return status, reason

    async def _notify_track_subscriptions_ended(
        self,
        track_name: FullTrackName,
        status_code: PublishDoneStatus,
        reason: str,
    ) -> None:
        """Notify and retire every live downstream subscription for one track."""
        subscribers = list(self._subscriptions.get(track_name, ()))
        if not subscribers:
            return

        for subscriber in subscribers:
            subscription = subscriber.subscriptions.get(track_name)
            if subscription is None:
                continue

            message = PublishDoneMessage(
                request_id=subscription["request_id"],
                status_code=int(status_code),
                stream_count=self._subscription_stream_count(subscription),
                reason=reason,
            )
            try:
                await self._send_control_message(subscriber, message.encode())
            except Exception as e:
                logger.warning(
                    "Failed to send track-end PUBLISH_DONE to %s for %s: %s",
                    subscriber.session_id,
                    track_name,
                    e,
                )
            finally:
                self._remove_client_subscription(subscriber, track_name)

    def _get_or_create_client(self, protocol) -> ClientSession:
        """Find the client session for a transport callback, creating it if needed."""
        resolved_protocol = self._resolve_protocol(protocol)
        for client in self._clients.values():
            if client.protocol == resolved_protocol:
                return client

        session_id = self._derive_session_id(protocol)
        client = ClientSession(
            session_id=session_id,
            protocol=resolved_protocol,
            quic_connection=self._resolve_connection(protocol),
        )
        self._clients[session_id] = client
        logger.info(f"Client registered lazily: {session_id}")
        return client

    def _reset_track_cache(self, track_name: FullTrackName) -> None:
        """Drop cached objects for one track before starting a fresh publication."""
        self._object_cache.pop(track_name, None)
        self.cache.clear_track(track_name)
    
    async def _handle_publish(
        self,
        client: ClientSession,
        msg: PublishMessage,
        response_stream_id: Optional[int] = None,
    ):
        """Handle a publish request."""
        track_name = msg.full_track_name
        logger.info(f"Client {client.session_id} publishing: {track_name}")

        self._reset_track_cache(track_name)
        
        # Store publication
        self._publications[track_name] = client
        client.publications[track_name] = {
            'track_alias': msg.track_alias,
            'request_id': msg.request_id,
            'request_stream_id': response_stream_id,
            'parameters': msg.parameters,
            'track_properties': msg.track_properties,
        }
        
        # Update publisher sessions
        if track_name not in self.publisher_sessions:
            self.publisher_sessions[track_name] = []
        if client.session_id not in {session.session_id for session in self.publisher_sessions[track_name]}:
            self.publisher_sessions[track_name].append(client)
        
        # Send PUBLISH_OK
        response = PublishOkMessage(
            request_id=msg.request_id,
            parameters=Parameters(),
        )
        response_data = response.encode(include_request_id=response_stream_id is None)
        logger.debug(f"Sending PUBLISH_OK: {len(response_data)} bytes")
        await self._send_control_message(client, response_data, stream_id=response_stream_id)
        logger.info(f"Publication accepted: {track_name}")
    
    async def _handle_subscribe(
        self,
        client: ClientSession,
        msg: SubscribeMessage,
        response_stream_id: Optional[int] = None,
    ):
        """Handle a subscribe request."""
        track_name = msg.full_track_name
        logger.info(f"Client {client.session_id} subscribing to: {track_name}")

        existing_subscription = client.subscriptions.get(track_name)
        if existing_subscription:
            track_alias = existing_subscription['track_alias']
        else:
            track_alias = client.next_track_alias
            client.next_track_alias += 1
        
        # Store subscription
        if track_name not in self._subscriptions:
            self._subscriptions[track_name] = []
        self._subscriptions[track_name] = [
            subscriber
            for subscriber in self._subscriptions[track_name]
            if subscriber.session_id != client.session_id
        ]
        self._subscriptions[track_name].append(client)
        client.subscriptions[track_name] = {
            'track_alias': track_alias,
            'request_id': msg.request_id,
            'request_stream_id': response_stream_id,
            'subscriber_priority': msg.subscriber_priority,
            'group_order': msg.group_order,
            'filter_type': msg.filter_type,
            'parameters': msg.parameters,
            'start_group': msg.start_group,
            'start_object': msg.start_object,
            'end_group': msg.end_group,
            'end_object': msg.end_object,
            'opened_data_stream_ids': set(),
        }
        self._apply_subscription_parameter_state(
            track_name,
            client.subscriptions[track_name],
            msg.parameters,
        )
        
        # Update subscriber sessions
        if track_name not in self.subscriber_sessions:
            self.subscriber_sessions[track_name] = []
        if client.session_id not in {session.session_id for session in self.subscriber_sessions[track_name]}:
            self.subscriber_sessions[track_name].append(client)

        # Send SUBSCRIBE_OK
        response = SubscribeOkMessage(
            request_id=msg.request_id,
            track_alias=track_alias,
        )
        response_data = response.encode(include_request_id=response_stream_id is None)
        logger.debug(f"Sending SUBSCRIBE_OK: {len(response_data)} bytes")
        await self._send_control_message(client, response_data, stream_id=response_stream_id)
        logger.info(f"Subscription accepted: {track_name}")

        if msg.filter_type in (
            SubscribeFilter.ABSOLUTE_START,
            SubscribeFilter.ABSOLUTE_RANGE,
        ):
            await self._send_cached_objects_for_subscription(client, track_name)
    
    async def _handle_fetch(
        self,
        client: ClientSession,
        msg: FetchMessage,
        response_stream_id: Optional[int] = None,
    ):
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
            await self._send_control_message(
                client,
                response.encode(include_request_id=response_stream_id is None),
                stream_id=response_stream_id,
            )
            return
        
        # Send FETCH_OK
        end_of_track, end_location = self._resolve_fetch_end_location(track_name, msg)
        response = FetchOkMessage(
            request_id=msg.request_id,
            end_of_track=end_of_track,
            end_location=end_location,
            parameters=Parameters(),
        )
        response_data = response.encode(include_request_id=response_stream_id is None)
        logger.debug(f"Sending FETCH_OK: {len(response_data)} bytes")
        await self._send_control_message(client, response_data, stream_id=response_stream_id)
        if track_name in self._publications:
            logger.info(f"Fetch accepted: {track_name}")
        else:
            logger.info(f"Fetch accepted from cache: {track_name}")
        
        # Send cached objects that match the fetch range
        await self._send_cached_objects(client, track_name, msg)

    async def _handle_request_update(
        self,
        client: ClientSession,
        msg: RequestUpdateMessage,
        response_stream_id: Optional[int] = None,
    ):
        """Handle REQUEST_UPDATE for an existing request."""
        logger.info("Client %s updating request %s", client.session_id, msg.request_id)

        updated_parameters = None
        updated_subscription_track_name = None

        for track_name, subscription in client.subscriptions.items():
            if subscription['request_id'] != msg.request_id:
                continue
            updated_parameters = self._merge_parameters(subscription.get('parameters'), msg.parameters)
            subscription['parameters'] = updated_parameters
            filter_updated = self._apply_subscription_parameter_state(
                track_name,
                subscription,
                updated_parameters,
            )
            if filter_updated:
                updated_subscription_track_name = track_name
            break

        if updated_parameters is None:
            for publication in client.publications.values():
                if publication['request_id'] != msg.request_id:
                    continue
                updated_parameters = self._merge_parameters(publication.get('parameters'), msg.parameters)
                publication['parameters'] = updated_parameters
                break

        if updated_parameters is None:
            response = RequestErrorMessage(
                request_id=msg.request_id,
                error_code=ErrorCode.INTERNAL_ERROR,
                reason="Unknown request",
                retry_interval=0,
            )
            await self._send_control_message(
                client,
                response.encode(include_request_id=response_stream_id is None),
                stream_id=response_stream_id,
            )
            return

        response = RequestOkMessage(
            request_id=msg.request_id,
            parameters=updated_parameters,
        )
        await self._send_control_message(
            client,
            response.encode(include_request_id=response_stream_id is None),
            stream_id=response_stream_id,
        )
        if updated_subscription_track_name is not None:
            await self._send_cached_objects_for_subscription(client, updated_subscription_track_name)
    
    async def _send_cached_objects(self, client: ClientSession, track_name: FullTrackName, msg: FetchMessage):
        """Send cached objects that match the fetch range to the client."""
        cached_objects = self._object_cache.get(track_name, [])
        if not cached_objects:
            logger.info(f"No cached objects for track: {track_name}")
            return

        stream_payload = bytearray()
        stream_payload.extend(VarInt.encode(StreamType.FETCH_HEADER))
        stream_payload.extend(FetchHeader(msg.request_id).encode())
        sent_count = 0
        for obj_data in cached_objects:
            # Check if object is within fetch range
            # If end_group/end_object is None, fetch until the latest message
            end_group_limit = msg.end_group if msg.end_group is not None else float('inf')
            end_object_limit = msg.end_object if msg.end_object is not None else float('inf')
            
            if (msg.start_group <= obj_data['group_id'] <= end_group_limit and
                msg.start_object <= obj_data['object_id'] <= end_object_limit):
                
                # Create ObjectDatagram and send
                if obj_data['object_status'] != ObjectStatus.NORMAL:
                    continue
                obj = FetchObject(
                    group_id=obj_data['group_id'],
                    object_id=obj_data['object_id'],
                    publisher_priority=obj_data['publisher_priority'],
                    payload=obj_data['payload'],
                )
                
                try:
                    stream_payload.extend(obj.encode())
                    sent_count += 1
                    logger.debug(f"Sent cached object: group={obj_data['group_id']}, object={obj_data['object_id']}")
                except Exception as e:
                    logger.error(f"Error sending cached object to {client.session_id}: {e}")

        if sent_count == 0:
            logger.info(f"Sent 0 cached objects to {client.session_id} for fetch request")
            return

        stream_id = await self._open_stream(client, unidirectional=True)
        await self._enqueue_client_send(
            client,
            lambda: self._send_stream_bytes(client, stream_id, bytes(stream_payload), end_stream=True),
            description=f"fetch stream {stream_id}",
            priority=SEND_PRIORITY_FETCH,
            wait=True,
            is_data_send=False,
        )
        logger.info(f"Sent {sent_count} cached objects to {client.session_id} for fetch request")

    async def _send_cached_objects_for_subscription(
        self,
        client: ClientSession,
        track_name: FullTrackName,
    ) -> None:
        """Backfill cached objects that match a live subscription filter."""
        subscription = client.subscriptions.get(track_name)
        if not subscription:
            return

        cached_objects = self._object_cache.get(track_name, [])
        if not cached_objects:
            return

        sent_count = 0
        for obj_data in cached_objects:
            if not self._subscription_matches_object(
                subscription,
                obj_data['group_id'],
                obj_data['object_id'],
            ):
                continue

            rewritten = ObjectDatagram(
                header=ObjectHeader(
                    track_alias=subscription['track_alias'],
                    group_id=obj_data['group_id'],
                    object_id=obj_data['object_id'],
                    publisher_priority=obj_data['publisher_priority'],
                    object_status=obj_data['object_status'],
                ),
                payload=obj_data['payload'],
            )

            try:
                await self._send_datagram(client, rewritten.encode())
                sent_count += 1
            except Exception as e:
                logger.error(
                    "Error sending cached subscription object to %s: %s",
                    client.session_id,
                    e,
                )

        if sent_count:
            logger.info(
                "Sent %d cached subscription objects to %s for %s",
                sent_count,
                client.session_id,
                track_name,
            )
    
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
        
        await self._store_object(track_name, obj.header, obj.payload)
        
        # Forward to all subscribers
        await self._forward_object(track_name, obj)

    async def _store_object(self, track_name: FullTrackName, header: ObjectHeader, payload: bytes):
        """Persist a parsed object in the relay caches."""
        datagram = ObjectDatagram(header=header, payload=payload)
        await self._cache_object(track_name, datagram)

        cached_obj = CachedObject(
            track_alias=header.track_alias,
            group_id=header.group_id,
            object_id=header.object_id,
            publisher_priority=header.publisher_priority,
            payload=payload
        )
        self.cache.put(track_name, cached_obj)

        if self._on_object_received:
            try:
                self._on_object_received(track_name, datagram)
            except Exception as e:
                logger.error(f"Error in object handler: {e}")
    
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
                            subscription = subscriber.subscriptions.get(track_name)
                            stream_id = await self._open_stream(subscriber, unidirectional=True)
                            if subscription is not None:
                                subscription.setdefault('opened_data_stream_ids', set()).add(stream_id)
                            await self._enqueue_client_send(
                                subscriber,
                                lambda client=subscriber, sid=stream_id, payload=data: self._send_stream_bytes(
                                    client,
                                    sid,
                                    payload,
                                    end_stream=True,
                                ),
                                description=f"broadcast raw stream {stream_id}",
                                priority=SEND_PRIORITY_DATA,
                                wait=False,
                            )
                        except Exception as e:
                            logger.error(f"Error forwarding to {subscriber.session_id}: {e}")
                break

    async def _flush_forward_buffer(
        self,
        track_name: FullTrackName,
        state: InboundDataStream,
        end_stream: bool = False
    ):
        """Forward buffered stream bytes to all subscribers in larger batches."""
        subscribers = self._subscriptions.get(track_name, [])
        if not subscribers:
            state.forward_objects.clear()
            state.buffered_forward_bytes = 0
            return

        buffered_objects = list(state.forward_objects)
        state.forward_objects.clear()
        state.buffered_forward_bytes = 0

        for subscriber in subscribers:
            subscription = subscriber.subscriptions.get(track_name)
            if not subscription:
                continue

            stream_id = state.downstream_streams.get(subscriber.session_id)
            payload_chunks = []
            previous_object_id = state.downstream_last_object_ids.get(subscriber.session_id)
            for subgroup_obj in buffered_objects:
                if not self._subscription_matches_object(
                    subscription,
                    state.subgroup_header.group_id if state.subgroup_header else 0,
                    subgroup_obj.object_id,
                ):
                    continue
                payload_chunks.append(subgroup_obj.encode(previous_object_id=previous_object_id))
                previous_object_id = subgroup_obj.object_id
            payload = b"".join(payload_chunks)

            payload_to_send = payload
            if stream_id is None:
                if not payload:
                    continue
                stream_id = await self._open_stream(subscriber, unidirectional=True)
                state.downstream_streams[subscriber.session_id] = stream_id
                subscription.setdefault('opened_data_stream_ids', set()).add(stream_id)
                if state.subgroup_header is None:
                    raise RuntimeError("subgroup header missing while opening downstream stream")

                rewritten_header = SubgroupHeader(
                    track_alias=subscription['track_alias'],
                    group_id=state.subgroup_header.group_id,
                    subgroup_id=state.subgroup_header.subgroup_id,
                    publisher_priority=state.subgroup_header.publisher_priority,
                )
                payload_to_send = (
                    VarInt.encode(StreamType.SUBGROUP_HEADER)
                    + rewritten_header.encode()
                    + payload
                )
            elif not payload and not end_stream:
                continue

            state.downstream_last_object_ids[subscriber.session_id] = previous_object_id

            try:
                await self._enqueue_client_send(
                    subscriber,
                    lambda client=subscriber, sid=stream_id, payload=payload_to_send, finish=end_stream: self._send_stream_bytes(
                        client,
                        sid,
                        payload,
                        end_stream=finish,
                    ),
                    description=f"fanout subgroup stream {stream_id}",
                    priority=SEND_PRIORITY_DATA,
                    wait=False,
                )
            except Exception as e:
                logger.error(f"Error forwarding stream buffer to {subscriber.session_id}: {e}")

    def _append_forward_subgroup_object(
        self,
        state: InboundDataStream,
        subgroup_obj: SubgroupObject,
    ):
        """Append a fully parsed subgroup object to the downstream forward buffer."""
        state.forward_objects.append(subgroup_obj)
        previous_object_id = state.forward_objects[-2].object_id if len(state.forward_objects) > 1 else None
        state.buffered_forward_bytes += len(subgroup_obj.encode(previous_object_id=previous_object_id))
    
    async def _forward_object(self, track_name: FullTrackName, obj: ObjectDatagram):
        """Forward an object to all subscribers of a track."""
        subscribers = self._subscriptions.get(track_name, [])
        if not subscribers:
            return

        forwarded = 0

        for subscriber in subscribers:
            subscription = subscriber.subscriptions.get(track_name)
            if not subscription:
                continue
            if not self._subscription_matches_object(
                subscription,
                obj.header.group_id,
                obj.header.object_id,
            ):
                continue

            rewritten = ObjectDatagram(
                header=ObjectHeader(
                    track_alias=subscription['track_alias'],
                    group_id=obj.header.group_id,
                    object_id=obj.header.object_id,
                    publisher_priority=obj.header.publisher_priority,
                    object_status=obj.header.object_status,
                ),
                payload=obj.payload,
            )
            try:
                encoded = rewritten.encode()
                await self._enqueue_client_send(
                    subscriber,
                    lambda client=subscriber, payload=encoded: self._send_datagram_bytes(client, payload),
                    description=f"fanout datagram group={obj.header.group_id} object={obj.header.object_id}",
                    priority=SEND_PRIORITY_DATA,
                    wait=False,
                )
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
    
    async def _send_control_message(
        self,
        client: ClientSession,
        data: bytes,
        stream_id: Optional[int] = None,
    ):
        """Send a message to a client over the negotiated transport."""
        try:
            target_stream_id = stream_id
            if target_stream_id is None and client.local_control_stream_id is None:
                client.local_control_stream_id = await self._open_stream(
                    client,
                    unidirectional=True,
                )
            if target_stream_id is None:
                target_stream_id = client.local_control_stream_id

            await self._enqueue_client_send(
                client,
                lambda: self._send_stream_bytes(
                    client,
                    target_stream_id,
                    data,
                ),
                description=f"control stream {target_stream_id}",
                priority=SEND_PRIORITY_CONTROL,
                wait=True,
                is_data_send=False,
            )
            
            logger.debug(f"Sent {len(data)} bytes to {client.session_id}")
        except Exception as e:
            logger.error(f"Error sending message to {client.session_id}: {e}")
            raise

    async def _send_datagram(self, client: ClientSession, data: bytes):
        """Send a datagram to a client over the negotiated transport."""
        try:
            await self._enqueue_client_send(
                client,
                lambda: self._send_datagram_bytes(client, data),
                description=f"datagram ({len(data)} bytes)",
                priority=SEND_PRIORITY_DATA,
                wait=True,
            )
            logger.debug(f"Sent datagram {len(data)} bytes to {client.session_id}")
        except Exception as e:
            logger.error(f"Error sending datagram to {client.session_id}: {e}")
            raise

    async def _send_data_stream(self, client: ClientSession, data: bytes):
        """Send opaque raw data on a fresh unidirectional stream."""
        try:
            stream_id = await self._open_stream(client, unidirectional=True)
            await self._enqueue_client_send(
                client,
                lambda: self._send_stream_bytes(client, stream_id, data, end_stream=True),
                description=f"raw data stream {stream_id}",
                priority=SEND_PRIORITY_DATA,
                wait=True,
            )
            logger.debug(f"Sent raw stream {len(data)} bytes to {client.session_id} on stream {stream_id}")
        except Exception as e:
            logger.error(f"Error sending raw stream data to {client.session_id}: {e}")
            raise
    
    async def _cleanup_client(self, client: ClientSession):
        """Clean up when a client disconnects."""
        logger.info(f"Client disconnected: {client.session_id}")
        client.closing = True
        self._cancel_client_send_worker(client)
        
        # Remove from clients
        if client.session_id in self._clients:
            del self._clients[client.session_id]
        
        # Remove publications
        for track_name in list(client.publications.keys()):
            await self._notify_track_subscriptions_ended(
                track_name,
                PublishDoneStatus.GOING_AWAY,
                "publisher disconnected",
            )
            self._remove_client_publication(client, track_name)
            logger.info(f"Publication removed: {track_name}")

        # Remove subscriptions
        for track_name in list(client.subscriptions.keys()):
            self._remove_client_subscription(client, track_name)
        
        # Keep cached objects available for later FETCH requests.

    def _get_largest_cached_location(
        self,
        track_name: FullTrackName,
    ) -> Tuple[Optional[int], Optional[int]]:
        """Return the largest cached group/object location for a track."""
        cached_objects = self._object_cache.get(track_name, [])
        if not cached_objects:
            return None, None

        largest = max(
            cached_objects,
            key=lambda obj: (obj['group_id'], obj['object_id']),
        )
        return largest['group_id'], largest['object_id']

    def _subscription_matches_object(
        self,
        subscription: dict,
        group_id: int,
        object_id: int,
    ) -> bool:
        """Check whether an object should be forwarded to a subscriber."""
        filter_type = subscription.get('filter_type')
        start_group = subscription.get('start_group')
        start_object = subscription.get('start_object')
        end_group = subscription.get('end_group')
        end_object = subscription.get('end_object')

        location = (group_id, object_id)
        if filter_type in (SubscribeFilter.ABSOLUTE_START, SubscribeFilter.ABSOLUTE_RANGE):
            if start_group is not None and start_object is not None:
                if location < (start_group, start_object):
                    return False

        if filter_type == SubscribeFilter.ABSOLUTE_RANGE:
            if end_group is not None:
                if end_object is None:
                    if group_id > end_group:
                        return False
                elif location > (end_group, end_object):
                    return False

        return True
    
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
