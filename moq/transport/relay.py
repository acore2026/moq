"""
MOQ Relay Implementation
Routes media streams between publishers and subscribers with caching
"""

import asyncio
import logging
import os
import ssl
import tempfile
from typing import Optional, Dict, List, Callable, Any
from dataclasses import dataclass

from aioquic.asyncio.server import serve
from aioquic.quic.configuration import QuicConfiguration

from moq.protocol.constants import MOQMessageType, MOQRole
from moq.protocol.messages import (
    SubscribeMessage, SubscribeOkMessage, SubscribeErrorMessage,
    AnnounceMessage, AnnounceOkMessage, AnnounceErrorMessage, UnannounceMessage
)
from moq.protocol.subscription import MOQSubscription, MOQAnnouncement
from moq.transport.session import MOQSession, SessionConfig, MOQServerSession

logger = logging.getLogger(__name__)


def generate_self_signed_cert(cert_path: str, key_path: str) -> None:
    """Generate self-signed certificate for development/testing"""
    from cryptography import x509
    from cryptography.x509.oid import NameOID
    from cryptography.hazmat.primitives import hashes, serialization
    from cryptography.hazmat.primitives.asymmetric import rsa
    import datetime

    # Generate private key
    key = rsa.generate_private_key(
        public_exponent=65537,
        key_size=2048,
    )

    # Generate certificate
    subject = issuer = x509.Name([
        x509.NameAttribute(NameOID.COUNTRY_NAME, u"US"),
        x509.NameAttribute(NameOID.STATE_OR_PROVINCE_NAME, u"CA"),
        x509.NameAttribute(NameOID.LOCALITY_NAME, u"San Francisco"),
        x509.NameAttribute(NameOID.ORGANIZATION_NAME, u"MOQ Relay"),
        x509.NameAttribute(NameOID.COMMON_NAME, u"localhost"),
    ])

    cert = x509.CertificateBuilder().subject_name(
        subject
    ).issuer_name(
        issuer
    ).public_key(
        key.public_key()
    ).serial_number(
        x509.random_serial_number()
    ).not_valid_before(
        datetime.datetime.utcnow()
    ).not_valid_after(
        datetime.datetime.utcnow() + datetime.timedelta(days=365)
    ).add_extension(
        x509.SubjectAlternativeName([x509.DNSName(u"localhost")]),
        critical=False,
    ).sign(key, hashes.SHA256())

    # Write certificate and key to files
    with open(cert_path, "wb") as f:
        f.write(cert.public_bytes(serialization.Encoding.PEM))

    with open(key_path, "wb") as f:
        f.write(key.private_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PrivateFormat.TraditionalOpenSSL,
            encryption_algorithm=serialization.NoEncryption()
        ))


@dataclass
class RelayConfig:
    host: str = "0.0.0.0"
    port: int = 4433
    max_connections: int = 1000
    max_subscriptions_per_track: int = 100
    cache_enabled: bool = True
    cert_path: Optional[str] = None  # Path to SSL certificate
    key_path: Optional[str] = None   # Path to SSL private key


class RelayClientSession(MOQServerSession):
    """Session handler for each incoming client connection"""

    def __init__(self, relay: 'MOQRelay', connection_id: str):
        super().__init__(relay.config)
        self._relay = relay
        self._connection_id = connection_id

        # Register message handlers
        self.register_message_handler(MOQMessageType.ANNOUNCE, self._handle_announce)
        self.register_message_handler(MOQMessageType.UNANNOUNCE, self._handle_unannounce)
        self.register_message_handler(MOQMessageType.SUBSCRIBE, self._handle_subscribe)

    async def _handle_streams(self) -> None:
        """Handle incoming streams - server just waits for client streams"""
        logger.info(f"Server stream handler started for {self._connection_id}")
        try:
            while not self._is_closed:
                await asyncio.sleep(1)
        except asyncio.CancelledError:
            logger.info(f"Server stream handler cancelled for {self._connection_id}")
            raise

    async def _perform_setup_with_data(self, data: bytes) -> None:
        """Perform setup when CLIENT_SETUP data is already received"""
        from moq.protocol.messages import decode_message, ServerSetupMessage
        from moq.protocol.constants import MOQMessageType, MOQRole

        logger.info(f"Processing CLIENT_SETUP data ({len(data)} bytes)")

        message, _ = decode_message(data)

        if message and message.msg_type == MOQMessageType.CLIENT_SETUP:
            logger.info(f"Received CLIENT_SETUP: versions={[hex(v) for v in message.versions]}")

            # Select version
            selected_version = None
            for v in message.versions:
                if v == self.config.version:
                    selected_version = v
                    break

            if selected_version is None:
                raise RuntimeError("No compatible version found")

            # Send SERVER_SETUP
            response = ServerSetupMessage(
                selected_version=selected_version,
                role=MOQRole.PUB_SUB
            )

            await self.send_message(response)
            self._is_setup = True
            logger.info(f"Session setup complete, version={hex(selected_version)}")

            # IMPORTANT: Explicitly trigger transmission
            protocol = self._relay._connections.get(self._connection_id)
            if protocol and hasattr(protocol, 'transmit'):
                protocol.transmit()
                logger.info("Explicitly triggered transmission")

            # Give time for response to be transmitted over the network
            await asyncio.sleep(1.0)

            # Start stream handler
            asyncio.create_task(self._handle_streams())
        else:
            raise RuntimeError(f"Unexpected setup message: {message}")

    async def _handle_announce(self, message: AnnounceMessage) -> None:
        """Handle ANNOUNCE message from client"""
        await self._relay._handle_announce(message, self._connection_id)
        # Send ANNOUNCE_OK response
        response = AnnounceOkMessage(track_namespace=message.track_namespace)
        await self.send_message(response)

    async def _handle_unannounce(self, message: UnannounceMessage) -> None:
        """Handle UNANNOUNCE message from client"""
        await self._relay._handle_unannounce(message, self._connection_id)

    async def _handle_subscribe(self, message: SubscribeMessage) -> None:
        """Handle SUBSCRIBE message from client"""
        await self._relay._handle_subscribe(message, self._connection_id)
        # For now, always send SUBSCRIBE_OK
        # In a full implementation, this would check if track exists
        response = SubscribeOkMessage(
            subscribe_id=message.subscribe_id,
            expires=0,
            group_order=0,
            content_exists=False,
            track_alias=message.track_alias
        )
        await self.send_message(response)


class MOQRelay:
    """MOQ Relay that manages multiple client connections"""
    
    def __init__(
        self,
        session_config: Optional[SessionConfig] = None,
        relay_config: Optional[RelayConfig] = None,
        on_event: Optional[Callable[[str, Any], None]] = None
    ):
        self.config = session_config or SessionConfig()
        self.config.role = MOQRole.PUB_SUB
        self.config.enable_cache = True
        
        self.relay_config = relay_config or RelayConfig()
        self._on_event = on_event
        
        self._announced_namespaces: Dict[tuple, MOQAnnouncement] = {}
        self._active_tracks: Dict[str, dict] = {}
        self._subscriptions_by_track: Dict[str, List[MOQSubscription]] = {}
        self._connections: Dict[str, Any] = {}
        self._server: Optional[Any] = None
        self._sessions: Dict[str, RelayClientSession] = {}
        self._connection_counter: int = 0
        
        logger.info("MOQRelay initialized")
    
    async def initialize(self) -> None:
        """Initialize relay components"""
        from moq.cache.manager import MOQCacheManager
        if self.config.enable_cache:
            self.cache_manager = MOQCacheManager(
                memory_cache_size=self.config.max_cache_memory,
                disk_cache_size=self.config.max_cache_disk,
                disk_cache_dir=self.config.cache_dir,
                use_disk_cache=True
            )
            await self.cache_manager.initialize()
            logger.info("Relay cache manager initialized")
    
    async def close(self) -> None:
        """Close relay and cleanup"""
        logger.info("Closing relay")
        # Close all sessions
        for session_id, session in list(self._sessions.items()):
            try:
                await session.close()
            except Exception as e:
                logger.error(f"Error closing session {session_id}: {e}")
        self._sessions.clear()
        
        # Close cache manager
        if hasattr(self, 'cache_manager') and self.cache_manager:
            await self.cache_manager.close()
        
        logger.info("Relay closed")
    
    async def _handle_connection(self, connection_id: str, protocol, quic) -> None:
        """Handle an incoming client connection - now handled via protocol events"""
        # Connection handling is now done through protocol.quic_event_received
        # This method is kept for backwards compatibility but doesn't do anything
        pass
    
    def _create_protocol(self, *args, **kwargs):
        """Create protocol handler for incoming connections"""
        from aioquic.asyncio.protocol import QuicConnectionProtocol, QuicStreamAdapter
        from aioquic.quic import events
        relay = self
        
        class RelayProtocol(QuicConnectionProtocol):
            def __init__(self, *args, **kwargs):
                super().__init__(*args, **kwargs)
                self._relay = relay
                self._connection_id = None
                self._session = None
            
            def connection_made(self, transport):
                super().connection_made(transport)
                self._connection_id = f"conn_{relay._connection_counter}"
                relay._connection_counter += 1
                relay._connections[self._connection_id] = self
                logger.info(f"New connection: {self._connection_id}")
                
                # Create session for this connection
                self._session = RelayClientSession(relay, self._connection_id)
                relay._sessions[self._connection_id] = self._session
                self._session._connection = self
                self._session._quic = self._quic
                
                # Set up stream handler
                self._stream_handler = self._handle_new_stream
                logger.info(f"Stream handler set: {self._stream_handler is not None}")
            
            def quic_event_received(self, event):
                """Handle QUIC events"""
                from aioquic.quic import events
                if isinstance(event, events.StreamDataReceived):
                    logger.info(f"DEBUG: StreamDataReceived for stream {event.stream_id}, {len(event.data)} bytes")
                super().quic_event_received(event)
            
            def _handle_new_stream(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
                """Handle a new stream - called by aioquic when stream data arrives"""
                logger.info(f"New stream handler called for connection {self._connection_id}")
                
                # Get stream ID from writer
                stream_id = writer.get_extra_info("stream_id")
                logger.info(f"Handling stream {stream_id}")
                
                # Set up session streams
                self._session._control_stream = writer
                self._session._control_reader = reader
                
                # Handle the stream data asynchronously
                asyncio.create_task(self._handle_stream(reader, writer))
            
            async def _handle_stream(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
                """Handle stream data"""
                try:
                    # Read the CLIENT_SETUP message (don't read to EOF, just get the available data)
                    # MOQ messages have a length prefix, so we need to read at least that
                    # For now, read a reasonable amount
                    data = await reader.read(1024)  # Read up to 1KB, don't wait for EOF
                    if data:
                        logger.info(f"Received {len(data)} bytes on stream")
                        await self._session._perform_setup_with_data(data)
                except Exception as e:
                    logger.error(f"Error handling stream: {e}")
                    import traceback
                    traceback.print_exc()
            
            def connection_lost(self, exc):
                if self._connection_id and self._connection_id in relay._connections:
                    del relay._connections[self._connection_id]
                logger.info(f"Connection lost: {self._connection_id}")
                super().connection_lost(exc)
        
        return RelayProtocol(*args, **kwargs)
    
    async def start(self, host: Optional[str] = None, port: Optional[int] = None) -> None:
        host = host or self.relay_config.host
        port = port or self.relay_config.port
        
        logger.info(f"Starting relay on {host}:{port}")
        await self.initialize()
        
        # Handle SSL certificates
        if self.relay_config.cert_path and self.relay_config.key_path:
            # Use provided certificates
            cert_path = self.relay_config.cert_path
            key_path = self.relay_config.key_path
        else:
            # Generate self-signed certificates for development
            cert_dir = tempfile.gettempdir()
            cert_path = os.path.join(cert_dir, "moq_relay_cert.pem")
            key_path = os.path.join(cert_dir, "moq_relay_key.pem")
            
            if not os.path.exists(cert_path) or not os.path.exists(key_path):
                logger.info("Generating self-signed SSL certificate for development")
                generate_self_signed_cert(cert_path, key_path)
                logger.info(f"Certificate saved to: {cert_path}")
        
        # Create QUIC server configuration with certificates
        configuration = QuicConfiguration(
            is_client=False,
            verify_mode=ssl.CERT_NONE,
            alpn_protocols=["moq-17"],
        )
        configuration.load_cert_chain(cert_path, key_path)
        
        # Store cert paths for client reference
        self._cert_path = cert_path
        
        # Start QUIC server
        self._server = await serve(
            host,
            port,
            configuration=configuration,
            create_protocol=self._create_protocol,
        )
        
        logger.info(f"Relay started on {host}:{port}")
    
    async def stop(self) -> None:
        logger.info("Stopping relay")
        for conn_id, connection in list(self._connections.items()):
            try:
                connection.close()
            except Exception as e:
                logger.error(f"Error closing connection {conn_id}: {e}")
        
        self._connections.clear()
        if self._server:
            self._server.close()
            # Wait a moment for server to close
            await asyncio.sleep(0.1)
        
        await self.close()
        logger.info("Relay stopped")
    
    async def _handle_unannounce(self, message: UnannounceMessage, session_id: str = "") -> None:
        namespace = message.track_namespace
        if namespace in self._announced_namespaces:
            del self._announced_namespaces[namespace]
            logger.info(f"Namespace unannounced: {namespace}")
        
        if self._on_event:
            self._on_event("unannounce", {"namespace": namespace, "session_id": session_id})
    
    async def _handle_subscribe(self, message: SubscribeMessage, session_id: str = "") -> None:
        track_key = "/".join(message.track_namespace) + "/" + message.track_name
        
        if track_key not in self._active_tracks:
            response = SubscribeErrorMessage(
                subscribe_id=message.subscribe_id,
                error_code=0x0301,
                reason="Track not found",
                track_alias=message.track_alias
            )
            logger.warning(f"Subscribe request for unknown track: {track_key}")
            return
        
        subscription = MOQSubscription(
            id=message.subscribe_id,
            track_namespace=message.track_namespace,
            track_name=message.track_name,
            track_alias=message.track_alias,
            filter_type=message.filter_type,
            start_group=message.start_group,
            start_object=message.start_object,
            end_group=message.end_group,
            end_object=message.end_object,
            priority=message.subscriber_priority
        )
        
        if track_key not in self._subscriptions_by_track:
            self._subscriptions_by_track[track_key] = []
        
        if len(self._subscriptions_by_track[track_key]) >= self.relay_config.max_subscriptions_per_track:
            response = SubscribeErrorMessage(
                subscribe_id=message.subscribe_id,
                error_code=0x0105,
                reason="Too many subscriptions for this track",
                track_alias=message.track_alias
            )
            logger.warning(f"Too many subscriptions for track: {track_key}")
            return
        
        self._subscriptions_by_track[track_key].append(subscription)
        
        # Send cached objects if resuming
        await self._send_cached_objects(message)
        
        response = SubscribeOkMessage(
            subscribe_id=message.subscribe_id,
            expires=0,
            group_order=message.subscriber_priority,
            content_exists=True,
            largest_group_id=0,
            largest_object_id=0
        )
        
        subscription.update_state(subscription.state.ACTIVE)
        logger.info(f"Subscription {message.subscribe_id} activated for {track_key}")
        
        if self._on_event:
            self._on_event("subscribe", {"track": track_key, "session_id": session_id})
    
    async def _send_cached_objects(self, message: SubscribeMessage) -> None:
        if not self.cache_manager or not (message.start_group or message.start_object):
            return
        
        track_alias = message.track_alias or message.subscribe_id
        start_group = message.start_group or 0
        start_object = message.start_object or 0
        
        async for group_id, object_id, data in self.cache_manager.get_track_objects(
            track_alias, start_group, start_object
        ):
            logger.debug(f"Sending cached object: group={group_id}, object={object_id}")
            # TODO: Send to subscriber
    
    async def forward_object(self, obj: Any, publisher_session: str) -> None:
        track_alias = obj.track_alias
        track_key = str(track_alias)
        
        # Cache object
        if self.cache_manager:
            serialized = self._serialize_object(obj)
            await self.cache_manager.store(obj, serialized, prefer_memory=False)
        
        # Forward to subscribers
        if track_key in self._subscriptions_by_track:
            for subscription in self._subscriptions_by_track[track_key]:
                if subscription.is_active():
                    await subscription.add_object(obj)
                    logger.debug(f"Forwarded object to subscription {subscription.id}")
    
    def _serialize_object(self, obj: Any) -> bytes:
        from moq.protocol.varint import encode_varint
        result = encode_varint(obj.track_alias)
        result += encode_varint(obj.group_id)
        result += encode_varint(obj.object_id)
        result += encode_varint(obj.send_order)
        result += encode_varint(len(obj.payload))
        result += obj.payload
        return result
    
    async def get_stats(self) -> dict:
        stats = {
            "announced_namespaces": len(self._announced_namespaces),
            "active_tracks": len(self._active_tracks),
            "total_subscriptions": sum(len(subs) for subs in self._subscriptions_by_track.values()),
            "connections": len(self._connections),
        }
        if self.cache_manager:
            stats["cache"] = self.cache_manager.get_stats()
        return stats
