"""
MOQ Transport - Relay Implementation
Implements a caching relay for MOQT with memory and disk caching.
"""

import os
import json
import asyncio
import logging
import hashlib
from typing import Dict, List, Optional, Set, Tuple, Callable
from dataclasses import dataclass, field, asdict
from datetime import datetime, timedelta
from pathlib import Path
import threading

from moq.session import MOQSession, Role, Subscription, Publication
from moq.messages import (
    SubscribeMessage, PublishMessage, ObjectHeader, ObjectDatagram,
    SubscribeOkMessage, PublishOkMessage, PublishDoneMessage
)
from moq.encoding import FullTrackName, Location, VarInt

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
    """
    
    def __init__(self, host: str, port: int, 
                 cache_dir: Optional[str] = None,
                 max_memory_cache: int = 100 * 1024 * 1024,
                 max_disk_cache: int = 1024 * 1024 * 1024):
        self.host = host
        self.port = port
        
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
        
        logger.info(f"MOQRelay initialized: {host}:{port}")
    
    async def start(self):
        """Start the relay."""
        logger.info(f"Starting relay on {self.host}:{self.port}")
        # Server implementation would go here
    
    async def stop(self):
        """Stop the relay."""
        logger.info("Stopping relay")
        for session in self.sessions.values():
            session.close()
        self.sessions.clear()
    
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
        
        # Check if we have cached content to serve
        # TODO: Serve cached content based on subscription filter
        
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
