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


class MOQRelay(MOQServerSession):
    def __init__(
        self,
        session_config: Optional[SessionConfig] = None,
        relay_config: Optional[RelayConfig] = None,
        on_event: Optional[Callable[[str, Any], None]] = None
    ):
        session_config = session_config or SessionConfig()
        session_config.role = MOQRole.PUB_SUB
        session_config.enable_cache = True
        
        super().__init__(session_config)
        
        self.relay_config = relay_config or RelayConfig()
        self._on_event = on_event
        
        self._announced_namespaces: Dict[tuple, MOQAnnouncement] = {}
        self._active_tracks: Dict[str, dict] = {}
        self._subscriptions_by_track: Dict[str, List[MOQSubscription]] = {}
        self._connections: Dict[str, Any] = {}
        self._server: Optional[Any] = None
        
        self.register_message_handler(MOQMessageType.ANNOUNCE, self._handle_announce)
        self.register_message_handler(MOQMessageType.UNANNOUNCE, self._handle_unannounce)
        self.register_message_handler(MOQMessageType.SUBSCRIBE, self._handle_subscribe)
        
        logger.info("MOQRelay initialized")
    
    def _create_protocol(self, *args, **kwargs):
        """Create protocol handler for incoming connections"""
        from aioquic.asyncio.protocol import QuicConnectionProtocol
        
        class RelayProtocol(QuicConnectionProtocol):
            def __init__(self, *args, **kwargs):
                super().__init__(*args, **kwargs)
                self.relay = self
            
            def quic_event_received(self, event):
                # Handle QUIC events
                pass
        
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
    
    async def _handle_announce(self, message: AnnounceMessage, session_id: str = "") -> None:
        namespace = message.track_namespace
        
        if namespace in self._announced_namespaces:
            response = AnnounceErrorMessage(
                track_namespace=namespace,
                error_code=0x0200,
                reason="Namespace already announced"
            )
            logger.warning(f"Namespace already announced: {namespace}")
        else:
            announcement = MOQAnnouncement(namespace=namespace, state="active")
            self._announced_namespaces[namespace] = announcement
            response = AnnounceOkMessage(track_namespace=namespace)
            logger.info(f"Namespace announced: {namespace}")
        
        if self._on_event:
            self._on_event("announce", {"namespace": namespace, "session_id": session_id})
    
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
