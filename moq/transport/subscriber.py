"""
MOQ Subscriber Implementation
Handles subscribing to and receiving media streams
"""

import asyncio
import logging
from typing import Optional, Callable, Any, AsyncIterator
from dataclasses import dataclass

from moq.protocol.constants import (
    MOQMessageType, MOQRole, MOQFilterType
)
from moq.protocol.objects import MOQObject
from moq.protocol.messages import (
    SubscribeMessage, SubscribeOkMessage, SubscribeErrorMessage, UnsubscribeMessage
)
from moq.protocol.subscription import (
    MOQSubscription, SubscriptionState, SubscriptionManager, SubscriptionBuilder
)
from moq.transport.session import MOQSession, SessionConfig, MOQClientSession

logger = logging.getLogger(__name__)


@dataclass
class SubscriberConfig:
    """Subscriber configuration"""
    subscriber_priority: int = 128
    group_order: int = 0  # 0=original, 1=ascending, 2=descending
    auto_resume: bool = True
    resume_buffer_size: int = 1000


class MOQSubscriber(MOQClientSession):
    """
    MOQ Subscriber for receiving media streams.
    
    Usage:
        subscriber = MOQSubscriber()
        await subscriber.connect("relay.example.com", 4433)
        subscription = await subscriber.subscribe("example", "stream", "video")
        async for obj in subscription:
            process(obj)
    """
    
    def __init__(
        self,
        session_config: Optional[SessionConfig] = None,
        subscriber_config: Optional[SubscriberConfig] = None,
        on_object: Optional[Callable[[MOQObject], None]] = None,
        on_status: Optional[Callable[[str, Any], None]] = None
    ):
        session_config = session_config or SessionConfig()
        session_config.role = MOQRole.SUBSCRIBER
        
        super().__init__(session_config)
        
        self.sub_config = subscriber_config or SubscriberConfig()
        self._on_object = on_object
        self._on_status = on_status
        
        # Resume state
        self._resume_state: dict[int, tuple[int, int]] = {}  # sub_id -> (last_group, last_object)
        
        # Register message handlers
        self.register_message_handler(MOQMessageType.SUBSCRIBE_OK, self._handle_subscribe_ok)
        self.register_message_handler(MOQMessageType.SUBSCRIBE_ERROR, self._handle_subscribe_error)
        self.register_message_handler(MOQMessageType.OBJECT_DATAGRAM, self._handle_object)
        
        logger.info("MOQSubscriber initialized")
    
    async def connect(self, host: str, port: int, **kwargs) -> None:
        """
        Connect to relay server.
        
        Args:
            host: Relay hostname or IP
            port: Relay port
            **kwargs: Additional connection options
        """
        try:
            self._host = host
            self._port = port
            logger.info(f"Connecting to relay at {host}:{port}")
            
            # TODO: Implement actual QUIC connection
            
        except Exception as e:
            logger.error(f"Connection failed: {e}")
            raise
    
    async def subscribe(
        self,
        *namespace: str,
        track_name: str,
        filter_type: int = MOQFilterType.LATEST_GROUP,
        start_group: Optional[int] = None,
        start_object: Optional[int] = None,
        end_group: Optional[int] = None,
        end_object: Optional[int] = None,
        on_object: Optional[Callable[[MOQObject], None]] = None
    ) -> MOQSubscription:
        """
        Subscribe to a track.
        
        Args:
            *namespace: Track namespace tuple
            track_name: Track name
            filter_type: Filter type (LATEST_GROUP, LATEST_OBJECT, etc.)
            start_group: Starting group for absolute filters
            start_object: Starting object for absolute filters
            end_group: Ending group for range filters
            end_object: Ending object for range filters
            on_object: Callback for received objects
        
        Returns:
            MOQSubscription object
        """
        if not self.is_setup:
            raise RuntimeError("Session not set up")
        
        # Check for resume state
        track_key = "/".join(namespace) + "/" + track_name
        sub_id = hash(track_key) % (2**31)
        
        if self.sub_config.auto_resume and sub_id in self._resume_state:
            last_group, last_object = self._resume_state[sub_id]
            filter_type = MOQFilterType.ABSOLUTE_START
            start_group = last_group
            start_object = last_object + 1
            logger.info(f"Resuming subscription from group={last_group}, object={last_object + 1}")
        
        # Create subscription
        subscription = await self.subscription_manager.create_subscription(
            track_namespace=namespace,
            track_name=track_name,
            track_alias=sub_id,
            filter_type=filter_type,
            start_group=start_group,
            start_object=start_object,
            end_group=end_group,
            end_object=end_object,
            priority=self.sub_config.subscriber_priority,
            callback=on_object or self._on_object
        )
        
        # Send SUBSCRIBE message
        msg = SubscribeMessage(
            subscribe_id=subscription.id,
            track_alias=subscription.track_alias or subscription.id,
            track_namespace=namespace,
            track_name=track_name,
            filter_type=filter_type,
            start_group=start_group,
            start_object=start_object,
            end_group=end_group,
            end_object=end_object,
            subscriber_priority=self.sub_config.subscriber_priority,
            group_order=self.sub_config.group_order
        )
        
        try:
            await self.send_message(msg)
            subscription.update_state(SubscriptionState.PENDING)
            logger.info(f"Sent SUBSCRIBE for {track_key}")
            
        except Exception as e:
            logger.error(f"Failed to subscribe: {e}")
            subscription.update_state(SubscriptionState.ERROR)
            raise
        
        return subscription
    
    async def unsubscribe(self, subscription: MOQSubscription) -> bool:
        """
        Unsubscribe from a track.
        
        Args:
            subscription: Subscription to cancel
        
        Returns:
            True if unsubscribed successfully
        """
        # Save resume state
        self._resume_state[subscription.id] = (
            subscription.start_group or 0,
            subscription.end_object or 0
        )
        
        # Send UNSUBSCRIBE message
        msg = UnsubscribeMessage(subscribe_id=subscription.id)
        
        try:
            await self.send_message(msg)
            await self.subscription_manager.remove_subscription(subscription.id)
            logger.info(f"Unsubscribed from subscription {subscription.id}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to unsubscribe: {e}")
            return False
    
    async def _handle_subscribe_ok(self, message: SubscribeOkMessage) -> None:
        """Handle SUBSCRIBE_OK message"""
        subscription = await self.subscription_manager.get_subscription(message.subscribe_id)
        
        if subscription:
            subscription.update_state(SubscriptionState.ACTIVE)
            logger.info(f"Subscription {message.subscribe_id} activated")
            
            if self._on_status:
                self._on_status("subscribed", {
                    'subscription_id': message.subscribe_id,
                    'expires': message.expires,
                    'largest_group': message.largest_group_id,
                    'largest_object': message.largest_object_id
                })
        else:
            logger.warning(f"Received SUBSCRIBE_OK for unknown subscription {message.subscribe_id}")
    
    async def _handle_subscribe_error(self, message: SubscribeErrorMessage) -> None:
        """Handle SUBSCRIBE_ERROR message"""
        subscription = await self.subscription_manager.get_subscription(message.subscribe_id)
        
        if subscription:
            subscription.update_state(SubscriptionState.ERROR)
            logger.error(f"Subscription {message.subscribe_id} failed: {message.reason} (code={message.error_code})")
            
            if self._on_status:
                self._on_status("subscribe_error", {
                    'subscription_id': message.subscribe_id,
                    'error_code': message.error_code,
                    'reason': message.reason
                })
    
    async def _handle_object(self, data: bytes) -> None:
        """Handle incoming object"""
        try:
            obj = self._deserialize_object(data)
            
            # Find matching subscription
            subscriptions = self.subscription_manager.get_subscriptions_for_track(
                namespace=(),  # TODO: Track namespace mapping
                name=str(obj.track_alias)
            )
            
            for subscription in subscriptions:
                await subscription.add_object(obj)
                
                # Update resume state
                self._resume_state[subscription.id] = (obj.group_id, obj.object_id)
            
            if self._on_object:
                await self._on_object(obj)
                
        except Exception as e:
            logger.error(f"Failed to handle object: {e}")
    
    def _deserialize_object(self, data: bytes) -> MOQObject:
        """Deserialize MOQ object from bytes"""
        from moq.protocol.varint import decode_varint
        
        offset = 0
        
        track_alias, consumed = decode_varint(data, offset)
        offset += consumed
        
        group_id, consumed = decode_varint(data, offset)
        offset += consumed
        
        object_id, consumed = decode_varint(data, offset)
        offset += consumed
        
        send_order, consumed = decode_varint(data, offset)
        offset += consumed
        
        payload_len, consumed = decode_varint(data, offset)
        offset += consumed
        
        payload = data[offset:offset + payload_len]
        
        return MOQObject(
            track_alias=track_alias,
            group_id=group_id,
            object_id=object_id,
            send_order=send_order,
            payload=payload
        )
    
    async def receive_objects(self, subscription: MOQSubscription) -> AsyncIterator[MOQObject]:
        """
        Async iterator for receiving objects from a subscription.
        
        Args:
            subscription: Active subscription
        
        Yields:
            MOQObject instances
        """
        while subscription.is_active():
            obj = await subscription.get_object(timeout=1.0)
            if obj:
                yield obj
    
    async def get_stats(self) -> dict:
        """Get subscriber statistics"""
        subscriptions = await self.subscription_manager.get_all_subscriptions()
        
        stats = {
            'active_subscriptions': len([s for s in subscriptions if s.is_active()]),
            'total_subscriptions': len(subscriptions),
            'resume_points': len(self._resume_state),
        }
        
        return stats
    
    async def close(self) -> None:
        """Close subscriber and cleanup"""
        # Unsubscribe from all
        subscriptions = await self.subscription_manager.get_all_subscriptions()
        for subscription in subscriptions:
            await self.unsubscribe(subscription)
        
        await super().close()
        logger.info("MOQSubscriber closed")
