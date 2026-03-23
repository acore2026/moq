"""
MOQ Cache Management System
Supports both memory and disk caching for relay functionality
"""

import os
import json
import sqlite3
import logging
import hashlib
import asyncio
from pathlib import Path
from typing import Optional, Dict, List, Tuple, Any, AsyncIterator
from dataclasses import dataclass, field, asdict
from datetime import datetime, timedelta
from collections import OrderedDict
import aiofiles

from moq.protocol.objects import MOQObject

logger = logging.getLogger(__name__)


@dataclass
class CacheEntry:
    """Cache entry metadata"""
    track_alias: int
    group_id: int
    object_id: int
    data_size: int
    timestamp: float
    storage_type: str  # 'memory' or 'disk'
    disk_path: Optional[str] = None
    
    @property
    def cache_key(self) -> str:
        """Generate unique cache key"""
        return f"{self.track_alias}/{self.group_id}/{self.object_id}"


class MemoryCache:
    """LRU Memory Cache for MOQ objects"""
    
    def __init__(self, max_size_bytes: int = 100 * 1024 * 1024):  # 100MB default
        self.max_size = max_size_bytes
        self.current_size = 0
        self._cache: OrderedDict[str, bytes] = OrderedDict()
        self._lock = asyncio.Lock()
    
    async def put(self, key: str, data: bytes) -> bool:
        """Store data in memory cache"""
        async with self._lock:
            data_size = len(data)
            
            # Evict entries if necessary
            while self.current_size + data_size > self.max_size and self._cache:
                oldest_key, oldest_data = self._cache.popitem(last=False)
                self.current_size -= len(oldest_data)
                logger.debug(f"Evicted from memory cache: {oldest_key}")
            
            # Store new data
            self._cache[key] = data
            self._cache.move_to_end(key)
            self.current_size += data_size
            logger.debug(f"Stored in memory cache: {key} ({data_size} bytes)")
            return True
    
    async def get(self, key: str) -> Optional[bytes]:
        """Retrieve data from memory cache"""
        async with self._lock:
            if key in self._cache:
                self._cache.move_to_end(key)
                logger.debug(f"Cache hit (memory): {key}")
                return self._cache[key]
            logger.debug(f"Cache miss (memory): {key}")
            return None
    
    async def delete(self, key: str) -> bool:
        """Delete entry from memory cache"""
        async with self._lock:
            if key in self._cache:
                data = self._cache.pop(key)
                self.current_size -= len(data)
                logger.debug(f"Deleted from memory cache: {key}")
                return True
            return False
    
    async def clear(self) -> None:
        """Clear all entries from memory cache"""
        async with self._lock:
            self._cache.clear()
            self.current_size = 0
            logger.info("Memory cache cleared")
    
    def get_stats(self) -> Dict[str, Any]:
        """Get cache statistics"""
        return {
            'entries': len(self._cache),
            'current_size_bytes': self.current_size,
            'max_size_bytes': self.max_size,
            'usage_percent': (self.current_size / self.max_size * 100) if self.max_size > 0 else 0
        }


class DiskCache:
    """Disk-based cache for MOQ objects"""
    
    def __init__(self, cache_dir: str = ".moq_cache", max_size_bytes: int = 1024 * 1024 * 1024):  # 1GB
        self.cache_dir = Path(cache_dir)
        self.max_size = max_size_bytes
        self.current_size = 0
        self._metadata_db: Optional[sqlite3.Connection] = None
        self._lock = asyncio.Lock()
        
    async def initialize(self) -> None:
        """Initialize disk cache"""
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        
        # Initialize SQLite metadata database
        db_path = self.cache_dir / "metadata.db"
        self._metadata_db = sqlite3.connect(str(db_path), check_same_thread=False)
        
        cursor = self._metadata_db.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS cache_entries (
                cache_key TEXT PRIMARY KEY,
                track_alias INTEGER,
                group_id INTEGER,
                object_id INTEGER,
                data_size INTEGER,
                timestamp REAL,
                file_path TEXT,
                access_count INTEGER DEFAULT 0
            )
        """)
        
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_track ON cache_entries(track_alias, group_id, object_id)
        """)
        
        self._metadata_db.commit()
        
        # Calculate current size
        cursor.execute("SELECT SUM(data_size) FROM cache_entries")
        result = cursor.fetchone()
        self.current_size = result[0] or 0
        
        logger.info(f"Disk cache initialized at {self.cache_dir}")
    
    def _get_file_path(self, key: str) -> Path:
        """Generate file path for cache entry"""
        # Create subdirectory based on hash to avoid too many files in one directory
        hash_prefix = hashlib.md5(key.encode()).hexdigest()[:2]
        subdir = self.cache_dir / hash_prefix
        subdir.mkdir(exist_ok=True)
        return subdir / f"{hashlib.md5(key.encode()).hexdigest()}.dat"
    
    async def put(self, key: str, data: bytes, track_alias: int, group_id: int, object_id: int) -> bool:
        """Store data in disk cache"""
        async with self._lock:
            try:
                # Evict entries if necessary
                while self.current_size + len(data) > self.max_size:
                    cursor = self._metadata_db.cursor()
                    cursor.execute(
                        "SELECT cache_key, file_path, data_size FROM cache_entries ORDER BY access_count, timestamp LIMIT 1"
                    )
                    result = cursor.fetchone()
                    
                    if not result:
                        break
                    
                    old_key, old_path, old_size = result
                    
                    # Delete file
                    try:
                        os.remove(old_path)
                    except FileNotFoundError:
                        pass
                    
                    # Delete from database
                    cursor.execute("DELETE FROM cache_entries WHERE cache_key = ?", (old_key,))
                    self._metadata_db.commit()
                    self.current_size -= old_size
                    logger.debug(f"Evicted from disk cache: {old_key}")
                
                # Write data to file
                file_path = self._get_file_path(key)
                async with aiofiles.open(file_path, 'wb') as f:
                    await f.write(data)
                
                # Store metadata
                cursor = self._metadata_db.cursor()
                cursor.execute(
                    """INSERT OR REPLACE INTO cache_entries 
                       (cache_key, track_alias, group_id, object_id, data_size, timestamp, file_path)
                       VALUES (?, ?, ?, ?, ?, ?, ?)""",
                    (key, track_alias, group_id, object_id, len(data), 
                     datetime.now().timestamp(), str(file_path))
                )
                self._metadata_db.commit()
                self.current_size += len(data)
                
                logger.debug(f"Stored in disk cache: {key} ({len(data)} bytes)")
                return True
                
            except Exception as e:
                logger.error(f"Failed to store in disk cache: {e}")
                return False
    
    async def get(self, key: str) -> Optional[bytes]:
        """Retrieve data from disk cache"""
        async with self._lock:
            try:
                cursor = self._metadata_db.cursor()
                cursor.execute("SELECT file_path FROM cache_entries WHERE cache_key = ?", (key,))
                result = cursor.fetchone()
                
                if not result:
                    logger.debug(f"Cache miss (disk): {key}")
                    return None
                
                file_path = result[0]
                
                # Update access count
                cursor.execute(
                    "UPDATE cache_entries SET access_count = access_count + 1 WHERE cache_key = ?",
                    (key,)
                )
                self._metadata_db.commit()
                
                async with aiofiles.open(file_path, 'rb') as f:
                    data = await f.read()
                
                logger.debug(f"Cache hit (disk): {key}")
                return data
                
            except Exception as e:
                logger.error(f"Failed to read from disk cache: {e}")
                return None
    
    async def delete(self, key: str) -> bool:
        """Delete entry from disk cache"""
        async with self._lock:
            try:
                cursor = self._metadata_db.cursor()
                cursor.execute("SELECT file_path, data_size FROM cache_entries WHERE cache_key = ?", (key,))
                result = cursor.fetchone()
                
                if not result:
                    return False
                
                file_path, data_size = result
                
                # Delete file
                try:
                    os.remove(file_path)
                except FileNotFoundError:
                    pass
                
                # Delete from database
                cursor.execute("DELETE FROM cache_entries WHERE cache_key = ?", (key,))
                self._metadata_db.commit()
                self.current_size -= data_size
                
                logger.debug(f"Deleted from disk cache: {key}")
                return True
                
            except Exception as e:
                logger.error(f"Failed to delete from disk cache: {e}")
                return False
    
    async def get_objects_for_track(
        self,
        track_alias: int,
        start_group: Optional[int] = None,
        start_object: Optional[int] = None
    ) -> List[Tuple[str, bytes]]:
        """Get all cached objects for a track"""
        async with self._lock:
            cursor = self._metadata_db.cursor()
            
            if start_group is not None and start_object is not None:
                cursor.execute(
                    """SELECT cache_key, file_path FROM cache_entries 
                       WHERE track_alias = ? AND (group_id > ? OR (group_id = ? AND object_id >= ?))
                       ORDER BY group_id, object_id""",
                    (track_alias, start_group, start_group, start_object)
                )
            else:
                cursor.execute(
                    "SELECT cache_key, file_path FROM cache_entries WHERE track_alias = ? ORDER BY group_id, object_id",
                    (track_alias,)
                )
            
            results = []
            for row in cursor.fetchall():
                key, file_path = row
                try:
                    async with aiofiles.open(file_path, 'rb') as f:
                        data = await f.read()
                    results.append((key, data))
                except Exception as e:
                    logger.error(f"Failed to read cached object {key}: {e}")
            
            return results
    
    async def clear(self) -> None:
        """Clear all entries from disk cache"""
        async with self._lock:
            try:
                # Delete all files
                for file_path in self.cache_dir.rglob("*.dat"):
                    try:
                        file_path.unlink()
                    except Exception as e:
                        logger.warning(f"Failed to delete {file_path}: {e}")
                
                # Clear database
                cursor = self._metadata_db.cursor()
                cursor.execute("DELETE FROM cache_entries")
                self._metadata_db.commit()
                self.current_size = 0
                
                logger.info("Disk cache cleared")
                
            except Exception as e:
                logger.error(f"Failed to clear disk cache: {e}")
    
    def get_stats(self) -> Dict[str, Any]:
        """Get cache statistics"""
        cursor = self._metadata_db.cursor()
        cursor.execute("SELECT COUNT(*), SUM(data_size) FROM cache_entries")
        result = cursor.fetchone()
        
        return {
            'entries': result[0] or 0,
            'current_size_bytes': result[1] or 0,
            'max_size_bytes': self.max_size,
            'usage_percent': ((result[1] or 0) / self.max_size * 100) if self.max_size > 0 else 0
        }
    
    async def close(self) -> None:
        """Close disk cache and cleanup resources"""
        if self._metadata_db:
            self._metadata_db.close()
            self._metadata_db = None
            logger.info("Disk cache closed")


class MOQCacheManager:
    """
    Unified cache manager supporting both memory and disk caching.
    Provides intelligent tiered caching for MOQ objects.
    """
    
    def __init__(
        self,
        memory_cache_size: int = 100 * 1024 * 1024,  # 100MB
        disk_cache_size: int = 1024 * 1024 * 1024,   # 1GB
        disk_cache_dir: str = ".moq_cache",
        use_disk_cache: bool = True
    ):
        self.memory_cache = MemoryCache(memory_cache_size)
        self.disk_cache = DiskCache(disk_cache_dir, disk_cache_size)
        self.use_disk_cache = use_disk_cache
        self._initialized = False
    
    async def initialize(self) -> None:
        """Initialize cache manager"""
        if self.use_disk_cache:
            await self.disk_cache.initialize()
        self._initialized = True
        logger.info("Cache manager initialized")
    
    def _get_cache_key(self, track_alias: int, group_id: int, object_id: int) -> str:
        """Generate cache key for an object"""
        return f"{track_alias}/{group_id}/{object_id}"
    
    async def store(
        self,
        obj: MOQObject,
        serialized_data: bytes,
        prefer_memory: bool = True
    ) -> bool:
        """
        Store an object in cache.
        
        Args:
            obj: MOQ object
            serialized_data: Serialized object data
            prefer_memory: If True, try memory first, else disk
        
        Returns:
            True if stored successfully
        """
        if not self._initialized:
            raise RuntimeError("Cache manager not initialized")
        
        key = self._get_cache_key(obj.track_alias, obj.group_id, obj.object_id)
        
        if prefer_memory:
            # Try memory first, fall back to disk
            success = await self.memory_cache.put(key, serialized_data)
            if not success and self.use_disk_cache:
                success = await self.disk_cache.put(
                    key, serialized_data, obj.track_alias, obj.group_id, obj.object_id
                )
        else:
            # Store in both for redundancy
            if self.use_disk_cache:
                await self.disk_cache.put(
                    key, serialized_data, obj.track_alias, obj.group_id, obj.object_id
                )
            success = await self.memory_cache.put(key, serialized_data)
        
        return success
    
    async def retrieve(
        self,
        track_alias: int,
        group_id: int,
        object_id: int
    ) -> Optional[bytes]:
        """
        Retrieve an object from cache.
        Checks memory first, then disk.
        
        Returns:
            Cached data or None if not found
        """
        if not self._initialized:
            raise RuntimeError("Cache manager not initialized")
        
        key = self._get_cache_key(track_alias, group_id, object_id)
        
        # Try memory first
        data = await self.memory_cache.get(key)
        if data is not None:
            return data
        
        # Try disk
        if self.use_disk_cache:
            data = await self.disk_cache.get(key)
            if data is not None:
                # Promote to memory cache
                await self.memory_cache.put(key, data)
                return data
        
        return None
    
    async def get_track_objects(
        self,
        track_alias: int,
        start_group: Optional[int] = None,
        start_object: Optional[int] = None
    ) -> AsyncIterator[Tuple[int, int, bytes]]:
        """
        Get all cached objects for a track starting from a position.
        Used for resuming subscriptions after reconnection.
        """
        if self.use_disk_cache:
            objects = await self.disk_cache.get_objects_for_track(
                track_alias, start_group, start_object
            )
            for key, data in objects:
                parts = key.split("/")
                if len(parts) == 3:
                    group_id = int(parts[1])
                    object_id = int(parts[2])
                    yield group_id, object_id, data
    
    async def delete(self, track_alias: int, group_id: int, object_id: int) -> bool:
        """Delete a specific object from cache"""
        if not self._initialized:
            raise RuntimeError("Cache manager not initialized")
        
        key = self._get_cache_key(track_alias, group_id, object_id)
        
        success = await self.memory_cache.delete(key)
        if self.use_disk_cache:
            success = success or await self.disk_cache.delete(key)
        
        return success
    
    async def clear(self) -> None:
        """Clear all caches"""
        if not self._initialized:
            return
        
        await self.memory_cache.clear()
        if self.use_disk_cache:
            await self.disk_cache.clear()
        
        logger.info("All caches cleared")
    
    def get_stats(self) -> Dict[str, Any]:
        """Get cache statistics"""
        stats = {
            'memory': self.memory_cache.get_stats(),
        }
        
        if self.use_disk_cache:
            stats['disk'] = self.disk_cache.get_stats()
        
        return stats
    
    async def close(self) -> None:
        """Close cache manager and cleanup resources"""
        if self.use_disk_cache:
            await self.disk_cache.close()
        await self.memory_cache.clear()
        self._initialized = False
        logger.info("Cache manager closed")
