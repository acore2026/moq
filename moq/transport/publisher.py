"""
MOQ Publisher Implementation
Handles publishing media streams to relay or subscribers
"""

import asyncio
import logging
import ssl
from typing import Optional, Callable, Any, AsyncIterator
from dataclasses import dataclass

from aioquic.asyncio.client import connect
from aioquic.quic.configuration import QuicConfiguration

from moq.protocol.constants import (
    MOQMessageType, MOQRole, MOQDeliveryPreference
)
from moq.protocol.objects import MOQObject, MOQTrack
from moq.protocol.messages import (
    AnnounceMessage, AnnounceOkMessage, AnnounceErrorMessage, UnannounceMessage
)
from moq.protocol.subscription import MOQAnnouncement
from moq.transport.session import MOQSession, SessionConfig, MOQClientSession

logger = logging.getLogger(__name__)


@dataclass
class PublisherConfig:
    """Publisher configuration"""
    delivery_preference: int = MOQDeliveryPreference.SUBGROUP
    publisher_priority: int = 128
    enable_announce: bool = True


class MOQPublisher(MOQClientSession):
    """
    MOQ Publisher for publishing media streams.
    
    Usage:
        publisher = MOQPublisher()
        await publisher.connect("relay.example.com", 4433)
        await publisher.announce_namespace("example", "stream")
        await publisher.publish_object(track_alias, group_id, object_id, data)
    """
    
    def __init__(
        self,
        session_config: Optional[SessionConfig] = None,
        publisher_config: Optional[PublisherConfig] = None,
        on_status: Optional[Callable[[str, Any], None]] = None
    ):
        session_config = session_config or SessionConfig()
        session_config.role = MOQRole.PUBLISHER
        
        super().__init__(session_config)
        
        self.pub_config = publisher_config or PublisherConfig()
        self._on_status = on_status
        
        # State
        self._announced_namespaces: dict[tuple[str, ...], MOQAnnouncement] = {}
        self._track_aliases: dict[str, int] = {}
        self._next_alias: int = 0
        
        # Streams
        self._streams: dict[int, Any] = {}
        
        logger.info("MOQPublisher initialized")
    
    async def connect(self, host: str, port: int, **kwargs) -> None:
        """
        Connect to relay server using QUIC.
        
        Args:
            host: Relay hostname or IP
            port: Relay port
            **kwargs: Additional connection options
        """
        try:
            self._host = host
            self._port = port
            logger.info(f"Connecting to relay at {host}:{port}")
            
            # Create QUIC configuration
            configuration = QuicConfiguration(
                is_client=True,
                verify_mode=kwargs.get('verify_mode', ssl.CERT_NONE),
                alpn_protocols=kwargs.get('alpn_protocols', ["moq-17"]),
            )
            
            # Connect and store context manager
            self._connection_context = connect(host, port, configuration=configuration)
            self._protocol = await self._connection_context.__aenter__()
            self._quic = self._protocol._quic
            self._connection = self._protocol  # Protocol acts as connection
            
            # Perform setup (this sets is_setup = True)
            await self._perform_setup()
            
            # Start connection handler in background
            self._connection_task = asyncio.create_task(
                self._handle_connection_loop(),
                name=f"publisher-connection-{host}:{port}"
            )
            
            logger.info(f"Connected and session established with {host}:{port}")
            
        except Exception as e:
            logger.error(f"Connection failed: {e}")
            raise
    
    async def _handle_connection_loop(self) -> None:
        """Handle incoming streams in the background"""
        try:
            await self._handle_streams()
        except Exception as e:
            logger.error(f"Connection handler error: {e}")
        finally:
            await self.close()
    
    async def disconnect(self) -> None:
        """Disconnect from relay"""
        if self._connection_task and not self._connection_task.done():
            self._connection_task.cancel()
            try:
                await self._connection_task
            except asyncio.CancelledError:
                pass
        
        if hasattr(self, '_connection_context') and self._connection_context:
            await self._connection_context.__aexit__(None, None, None)
        
        await self.close()
    
    async def announce_namespace(self, *namespace: str) -> bool:
        """
        Announce a namespace to the relay.
        
        Args:
            *namespace: Namespace tuple (e.g., "example", "live")
        
        Returns:
            True if announcement succeeded
        """
        if not self.is_setup:
            raise RuntimeError("Session not set up")
        
        namespace_tuple = namespace
        
        # Check if already announced
        if namespace_tuple in self._announced_namespaces:
            logger.warning(f"Namespace already announced: {namespace_tuple}")
            return True
        
        # Create announcement
        announcement = MOQAnnouncement(namespace=namespace_tuple)
        self._announced_namespaces[namespace_tuple] = announcement
        
        # Send ANNOUNCE message
        msg = AnnounceMessage(track_namespace=namespace_tuple)
        
        try:
            await self.send_message(msg)
            logger.info(f"Sent ANNOUNCE for namespace: {namespace_tuple}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to announce namespace: {e}")
            announcement.state = "error"
            return False
    
    async def unannounce_namespace(self, *namespace: str) -> bool:
        """
        Unannounce a previously announced namespace.
        
        Args:
            *namespace: Namespace tuple
        """
        namespace_tuple = namespace
        
        if namespace_tuple not in self._announced_namespaces:
            logger.warning(f"Namespace not announced: {namespace_tuple}")
            return False
        
        # Send UNANNOUNCE message
        msg = UnannounceMessage(track_namespace=namespace_tuple)
        
        try:
            await self.send_message(msg)
            del self._announced_namespaces[namespace_tuple]
            logger.info(f"Sent UNANNOUNCE for namespace: {namespace_tuple}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to unannounce namespace: {e}")
            return False
    
    def _get_track_alias(self, track: MOQTrack) -> int:
        """Get or create track alias"""
        track_key = track.full_name()
        
        if track_key not in self._track_aliases:
            self._track_aliases[track_key] = self._next_alias
            track.alias = self._next_alias
            self._next_alias += 1
            logger.debug(f"Created track alias {track.alias} for {track_key}")
        
        return self._track_aliases[track_key]
    
    async def publish_object(
        self,
        track: MOQTrack,
        group_id: int,
        object_id: int,
        data: bytes,
        send_order: int = 0,
        extensions: Optional[dict] = None
    ) -> bool:
        """
        Publish a media object.
        
        Args:
            track: Track to publish to
            group_id: Group identifier
            object_id: Object identifier within group
            data: Object payload
            send_order: Priority (lower = higher priority)
            extensions: Optional extension data
        
        Returns:
            True if published successfully
        """
        if not self.is_setup:
            raise RuntimeError("Session not set up")
        
        # Get track alias
        track_alias = self._get_track_alias(track)
        
        # Create object
        obj = MOQObject(
            track_alias=track_alias,
            group_id=group_id,
            object_id=object_id,
            send_order=send_order,
            payload=data,
            extensions=extensions or {}
        )
        
        try:
            # Serialize and send object
            # TODO: Implement actual object sending based on delivery preference
            logger.debug(f"Publishing object: track={track_alias}, group={group_id}, object={object_id}")
            
            # Cache object if enabled
            if self.cache_manager:
                serialized = self._serialize_object(obj)
                await self.cache_manager.store(obj, serialized, prefer_memory=False)
            
            if self._on_status:
                self._on_status("object_published", {
                    'track_alias': track_alias,
                    'group_id': group_id,
                    'object_id': object_id,
                    'size': len(data)
                })
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to publish object: {e}")
            return False
    
    def _serialize_object(self, obj: MOQObject) -> bytes:
        """Serialize MOQ object to bytes"""
        from moq.protocol.varint import encode_varint
        
        result = encode_varint(obj.track_alias)
        result += encode_varint(obj.group_id)
        result += encode_varint(obj.object_id)
        result += encode_varint(obj.send_order)
        result += encode_varint(len(obj.payload))
        result += obj.payload
        
        return result
    
    async def publish_stream(
        self,
        track: MOQTrack,
        objects: AsyncIterator[MOQObject]
    ) -> None:
        """
        Publish a stream of objects.
        
        Args:
            track: Track to publish to
            objects: Async iterator of MOQObject
        """
        track_alias = self._get_track_alias(track)
        
        async for obj in objects:
            obj.track_alias = track_alias
            await self.publish_object(
                track=track,
                group_id=obj.group_id,
                object_id=obj.object_id,
                data=obj.payload,
                send_order=obj.send_order
            )
    
    async def get_stats(self) -> dict:
        """Get publisher statistics"""
        stats = {
            'announced_namespaces': len(self._announced_namespaces),
            'active_tracks': len(self._track_aliases),
        }
        
        if self.cache_manager:
            stats['cache'] = self.cache_manager.get_stats()
        
        return stats
    
    async def close(self) -> None:
        """Close publisher and cleanup"""
        # Unannounce all namespaces
        for namespace in list(self._announced_namespaces.keys()):
            await self.unannounce_namespace(*namespace)
        
        await super().close()
        logger.info("MOQPublisher closed")
