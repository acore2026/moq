"""
MOQ Subscription Management
Implements subscription model for MOQ protocol
"""

from dataclasses import dataclass, field
from enum import IntEnum
from typing import Optional, Callable, Any, AsyncIterator
import asyncio
import logging


logger = logging.getLogger(__name__)


class SubscriptionState(IntEnum):
    """Subscription states"""
    
    PENDING = 0
    ACTIVE = 1
    PAUSED = 2
    CANCELLED = 3
    COMPLETED = 4
    ERROR = 5


@dataclass
class MOQSubscription:
    """
    MOQ Subscription representing a subscription request.
    
    Attributes:
        id: Unique subscription ID
        track_namespace: Track namespace tuple
        track_name: Track name
        track_alias: Track alias for efficient reference
        filter_type: Filter type for subscription
        start_group: Starting group (for absolute filters)
        start_object: Starting object (for absolute filters)
        end_group: Ending group (for range filters)
        end_object: Ending object (for range filters)
        priority: Subscription priority
        state: Current subscription state
        callback: Optional callback for received objects
    """
    
    id: int
    track_namespace: tuple[str, ...]
    track_name: str
    track_alias: Optional[int] = None
    filter_type: int = 0x01  # LATEST_GROUP
    start_group: Optional[int] = None
    start_object: Optional[int] = None
    end_group: Optional[int] = None
    end_object: Optional[int] = None
    priority: int = 128
    state: SubscriptionState = SubscriptionState.PENDING
    callback: Optional[Callable] = None
    
    # Internal fields
    _queue: asyncio.Queue = field(default_factory=asyncio.Queue)
    _lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    _received_objects: int = 0
    _bytes_received: int = 0
    
    def __post_init__(self):
        """Validate subscription fields."""
        if not self.track_name:
            raise ValueError("track_name cannot be empty")
        if self.id < 0:
            raise ValueError("id must be non-negative")
    
    def full_track_name(self) -> str:
        """Return full track name with namespace."""
        return "/".join(self.track_namespace) + "/" + self.track_name
    
    def is_active(self) -> bool:
        """Check if subscription is active."""
        return self.state == SubscriptionState.ACTIVE
    
    def update_state(self, state: SubscriptionState) -> None:
        """Update subscription state with logging."""
        old_state = self.state
        self.state = state
        logger.info(f"Subscription {self.id} state: {old_state.name} -> {state.name}")
    
    async def add_object(self, obj: Any) -> None:
        """Add received object to queue."""
        async with self._lock:
            self._received_objects += 1
            self._bytes_received += len(obj.payload) if hasattr(obj, 'payload') else 0
            await self._queue.put(obj)
            
            if self.callback:
                try:
                    if asyncio.iscoroutinefunction(self.callback):
                        await self.callback(obj)
                    else:
                        self.callback(obj)
                except Exception as e:
                    logger.error(f"Callback error for subscription {self.id}: {e}")
    
    async def get_object(self, timeout: Optional[float] = None) -> Optional[Any]:
        """Get object from queue with optional timeout."""
        try:
            return await asyncio.wait_for(self._queue.get(), timeout=timeout)
        except asyncio.TimeoutError:
            return None
    
    def get_stats(self) -> dict:
        """Get subscription statistics."""
        return {
            'subscription_id': self.id,
            'track': self.full_track_name(),
            'state': self.state.name,
            'objects_received': self._received_objects,
            'bytes_received': self._bytes_received,
        }


@dataclass
class MOQAnnouncement:
    """
    MOQ Namespace Announcement.
    
    Attributes:
        namespace: Announced namespace
        state: Current announcement state
        parameters: Optional parameters
    """
    
    namespace: tuple[str, ...]
    state: str = "pending"  # pending, active, error, cancelled
    error_code: Optional[int] = None
    error_reason: str = ""
    parameters: dict = field(default_factory=dict)


class SubscriptionManager:
    """
    Manager for handling multiple subscriptions.
    """
    
    def __init__(self):
        self._subscriptions: dict[int, MOQSubscription] = {}
        self._next_id: int = 0
        self._lock: asyncio.Lock = asyncio.Lock()
    
    async def create_subscription(
        self,
        track_namespace: tuple[str, ...],
        track_name: str,
        **kwargs
    ) -> MOQSubscription:
        """Create a new subscription."""
        async with self._lock:
            sub_id = self._next_id
            self._next_id += 1
            
            sub = MOQSubscription(
                id=sub_id,
                track_namespace=track_namespace,
                track_name=track_name,
                **kwargs
            )
            self._subscriptions[sub_id] = sub
            logger.info(f"Created subscription {sub_id} for {sub.full_track_name()}")
            return sub
    
    async def get_subscription(self, sub_id: int) -> Optional[MOQSubscription]:
        """Get subscription by ID."""
        return self._subscriptions.get(sub_id)
    
    async def remove_subscription(self, sub_id: int) -> bool:
        """Remove a subscription."""
        async with self._lock:
            if sub_id in self._subscriptions:
                sub = self._subscriptions[sub_id]
                sub.update_state(SubscriptionState.CANCELLED)
                del self._subscriptions[sub_id]
                logger.info(f"Removed subscription {sub_id}")
                return True
            return False
    
    async def get_all_subscriptions(self) -> list[MOQSubscription]:
        """Get all active subscriptions."""
        return list(self._subscriptions.values())
    
    def get_subscriptions_for_track(
        self,
        namespace: tuple[str, ...],
        name: str
    ) -> list[MOQSubscription]:
        """Get subscriptions for a specific track."""
        return [
            sub for sub in self._subscriptions.values()
            if sub.track_namespace == namespace and sub.track_name == name
        ]
    
    async def close_all(self) -> None:
        """Close all subscriptions."""
        async with self._lock:
            for sub in self._subscriptions.values():
                sub.update_state(SubscriptionState.CANCELLED)
            self._subscriptions.clear()
            logger.info("All subscriptions closed")


class SubscriptionBuilder:
    """
    Builder for constructing MOQ Subscriptions.
    """
    
    def __init__(self):
        self._track_namespace: tuple[str, ...] = ()
        self._track_name: str = ""
        self._filter_type: int = 0x01
        self._start_group: Optional[int] = None
        self._start_object: Optional[int] = None
        self._end_group: Optional[int] = None
        self._end_object: Optional[int] = None
        self._priority: int = 128
        self._callback: Optional[Callable] = None
    
    def track_namespace(self, *namespace: str) -> 'SubscriptionBuilder':
        """Set track namespace."""
        self._track_namespace = namespace
        return self
    
    def track_name(self, name: str) -> 'SubscriptionBuilder':
        """Set track name."""
        self._track_name = name
        return self
    
    def filter_latest_group(self) -> 'SubscriptionBuilder':
        """Filter: latest group."""
        self._filter_type = 0x01
        return self
    
    def filter_latest_object(self) -> 'SubscriptionBuilder':
        """Filter: latest object."""
        self._filter_type = 0x02
        return self
    
    def filter_absolute_start(
        self,
        start_group: int,
        start_object: int = 0
    ) -> 'SubscriptionBuilder':
        """Filter: absolute start position."""
        self._filter_type = 0x03
        self._start_group = start_group
        self._start_object = start_object
        return self
    
    def filter_absolute_range(
        self,
        start_group: int,
        end_group: int,
        start_object: int = 0,
        end_object: int = 0
    ) -> 'SubscriptionBuilder':
        """Filter: absolute range."""
        self._filter_type = 0x04
        self._start_group = start_group
        self._start_object = start_object
        self._end_group = end_group
        self._end_object = end_object
        return self
    
    def priority(self, priority: int) -> 'SubscriptionBuilder':
        """Set priority."""
        self._priority = priority
        return self
    
    def on_object(self, callback: Callable) -> 'SubscriptionBuilder':
        """Set object callback."""
        self._callback = callback
        return self
    
    def build(self, sub_id: int) -> MOQSubscription:
        """Build subscription."""
        if not self._track_name:
            raise ValueError("track_name is required")
        
        return MOQSubscription(
            id=sub_id,
            track_namespace=self._track_namespace,
            track_name=self._track_name,
            filter_type=self._filter_type,
            start_group=self._start_group,
            start_object=self._start_object,
            end_group=self._end_group,
            end_object=self._end_object,
            priority=self._priority,
            callback=self._callback,
        )
