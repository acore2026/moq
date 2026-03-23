"""
MOQ Session Management
Handles QUIC connection and MOQ protocol session lifecycle
"""

import asyncio
import logging
from typing import Optional, Callable, Any, Dict
from dataclasses import dataclass

from moq.protocol.constants import MOQMessageType, MOQRole, MOQ_VERSION_DRAFT_17
from moq.protocol.messages import (
    ClientSetupMessage, ServerSetupMessage, decode_message
)
from moq.protocol.subscription import SubscriptionManager, MOQSubscription
from moq.cache.manager import MOQCacheManager

logger = logging.getLogger(__name__)


@dataclass
class SessionConfig:
    """MOQ Session configuration"""
    role: int = MOQRole.PUB_SUB
    version: int = MOQ_VERSION_DRAFT_17
    max_subscribe_id: int = 100
    enable_cache: bool = True
    cache_dir: str = ".moq_cache"
    max_cache_memory: int = 100 * 1024 * 1024  # 100MB
    max_cache_disk: int = 1024 * 1024 * 1024   # 1GB


class MOQSession:
    """
    MOQ Session handles QUIC connection and MOQ protocol state.
    
    This is the base class for publisher, subscriber, and relay sessions.
    """
    
    def __init__(
        self,
        config: Optional[SessionConfig] = None,
        on_message: Optional[Callable] = None
    ):
        self.config = config or SessionConfig()
        self._on_message = on_message
        self._connection: Optional[Any] = None
        self._quic: Optional[Any] = None
        self._is_setup = False
        self._is_closed = False
        
        # Managers
        self.subscription_manager = SubscriptionManager()
        self.cache_manager: Optional[MOQCacheManager] = None
        
        # Message handlers
        self._message_handlers: Dict[int, Callable] = {}
        
        # Connection state
        self._control_stream: Optional[Any] = None
        self._datagram_queue: asyncio.Queue = asyncio.Queue()
        
        logger.info(f"MOQSession created with role={self.config.role}")
    
    async def initialize(self) -> None:
        """Initialize session components"""
        if self.config.enable_cache:
            self.cache_manager = MOQCacheManager(
                memory_cache_size=self.config.max_cache_memory,
                disk_cache_size=self.config.max_cache_disk,
                disk_cache_dir=self.config.cache_dir,
                use_disk_cache=True
            )
            await self.cache_manager.initialize()
            logger.info("Cache manager initialized")
    
    def register_message_handler(self, msg_type: int, handler: Callable) -> None:
        """Register a handler for a specific message type"""
        self._message_handlers[msg_type] = handler
        logger.debug(f"Registered handler for message type {hex(msg_type)}")
    
    async def handle_connection(self, connection: Any, quic: Any) -> None:
        """
        Handle an incoming QUIC connection.
        
        Args:
            connection: aioquic connection object
            quic: aioquic protocol object
        """
        self._connection = connection
        self._quic = quic
        
        try:
            # Perform setup
            await self._perform_setup()
            
            # Handle incoming streams and datagrams
            await self._handle_streams()
            
        except Exception as e:
            logger.error(f"Session error: {e}")
        finally:
            await self.close()
    
    async def _perform_setup(self) -> None:
        """Perform MOQ session setup handshake"""
        raise NotImplementedError("Subclasses must implement _perform_setup")
    
    async def _handle_streams(self) -> None:
        """Handle incoming QUIC streams"""
        raise NotImplementedError("Subclasses must implement _handle_streams")
    
    async def send_message(self, message: Any) -> None:
        """Send a MOQ control message"""
        if not self._control_stream:
            raise RuntimeError("Control stream not established")

        try:
            data = message.encode()

            # Try different write methods depending on stream type
            if hasattr(self._control_stream, 'write'):
                # It's a StreamWriter
                self._control_stream.write(data)
                await self._control_stream.drain()
            elif hasattr(self._control_stream, 'send'):
                # It's a QuicStream sender
                self._control_stream.send(data)
            else:
                # Fallback: use QUIC connection directly
                quic = self._quic if hasattr(self, '_quic') else None
                if quic and hasattr(quic, 'send_stream_data'):
                    # Find stream ID by checking which stream matches _control_stream
                    stream_id = None
                    for sid, stream in quic._streams.items():
                        if stream == self._control_stream or \
                           (hasattr(self._control_stream, 'stream_id') and stream.stream_id == self._control_stream.stream_id):
                            stream_id = sid
                            break
                    
                    if stream_id is not None:
                        quic.send_stream_data(stream_id, data, end_stream=False)
                        logger.debug(f"Sent {len(data)} bytes on stream {stream_id}")
                    else:
                        raise RuntimeError("Could not find stream ID for sending")

            # Trigger transmission via protocol
            if self._connection:
                if hasattr(self._connection, 'transmit'):
                    self._connection.transmit()
                elif hasattr(self._connection, '_transmit_soon'):
                    self._connection._transmit_soon()

            # Give time for transmission
            await asyncio.sleep(0.05)

            logger.debug(f"Sent message: {type(message).__name__}")
        except Exception as e:
            logger.error(f"Failed to send message: {e}")
            raise
    
    async def _process_message(self, data: bytes) -> None:
        """Process incoming MOQ message"""
        offset = 0
        while offset < len(data):
            message, consumed = decode_message(data, offset)
            
            if message is None:
                break
            
            offset += consumed
            
            # Call specific handler if registered
            if message.msg_type in self._message_handlers:
                try:
                    await self._message_handlers[message.msg_type](message)
                except Exception as e:
                    logger.error(f"Message handler error: {e}")
            
            # Call general message callback
            if self._on_message:
                try:
                    await self._on_message(message)
                except Exception as e:
                    logger.error(f"Message callback error: {e}")
    
    async def close(self) -> None:
        """Close the session"""
        if self._is_closed:
            return
        
        self._is_closed = True
        logger.info("Closing MOQ session")
        
        # Close all subscriptions
        await self.subscription_manager.close_all()
        
        # Close cache manager
        if self.cache_manager:
            await self.cache_manager.close()
        
        # Close QUIC connection
        if self._connection:
            self._connection.close()
        
        logger.info("MOQ session closed")
    
    @property
    def is_setup(self) -> bool:
        """Check if session is set up"""
        return self._is_setup
    
    @property
    def is_closed(self) -> bool:
        """Check if session is closed"""
        return self._is_closed


class MOQClientSession(MOQSession):
    """MOQ Client Session (Publisher or Subscriber)"""
    
    def __init__(
        self,
        config: Optional[SessionConfig] = None,
        on_message: Optional[Callable] = None
    ):
        super().__init__(config, on_message)
    
    async def _handle_streams(self) -> None:
        """Handle incoming QUIC streams - keep connection alive"""
        logger.info("Client stream handler started")
        try:
            while not self._is_closed:
                # Keep connection alive - actual stream handling would go here
                await asyncio.sleep(1)
        except asyncio.CancelledError:
            logger.info("Client stream handler cancelled")
            raise
    
    async def _perform_setup(self) -> None:
        """Send CLIENT_SETUP message"""
        if not self._connection:
            raise RuntimeError("No connection available")

        # Create control stream
        stream_result = self._connection.create_stream()
        if asyncio.iscoroutine(stream_result):
            reader, writer = await stream_result
        else:
            reader, writer = stream_result

        self._control_stream = writer
        self._control_reader = reader

        # Send setup message
        setup = ClientSetupMessage(
            versions=[self.config.version],
            role=self.config.role
        )

        await self.send_message(setup)
        logger.info("Sent CLIENT_SETUP message")

        # Wait for SERVER_SETUP response
        logger.info("Waiting for SERVER_SETUP response...")
        try:
            response = await asyncio.wait_for(self._control_reader.read(), timeout=10.0)
        except asyncio.TimeoutError:
            logger.error("Timeout waiting for SERVER_SETUP")
            raise RuntimeError("Timeout waiting for SERVER_SETUP")

        if not response:
            logger.error("Empty response from server")
            raise RuntimeError("Empty response from server")

        message, _ = decode_message(response)

        if message and message.msg_type == MOQMessageType.SERVER_SETUP:
            self._is_setup = True
            logger.info(f"Session setup complete, version={hex(message.selected_version)}")
        else:
            raise RuntimeError(f"Unexpected setup response: {message}")


class MOQServerSession(MOQSession):
    """MOQ Server Session (for Relay)"""
    
    def __init__(
        self,
        config: Optional[SessionConfig] = None,
        on_message: Optional[Callable] = None
    ):
        super().__init__(config, on_message)
    
    async def _perform_setup(self) -> None:
        """Handle CLIENT_SETUP and respond with SERVER_SETUP"""
        if not self._connection:
            raise RuntimeError("No connection available")

        logger.info("Server accepting client control stream...")

        # Wait for the client to create its stream and send data
        await asyncio.sleep(0.5)

        # Create a bidirectional stream - server creates its own stream
        # In QUIC, bidirectional streams are separate - server creates stream 1
        logger.info("Creating server control stream...")
        stream_result = self._connection.create_stream()
        if asyncio.iscoroutine(stream_result):
            reader, writer = await stream_result
        else:
            reader, writer = stream_result

        self._control_stream = writer
        self._control_reader = reader

        logger.info("Server reading CLIENT_SETUP...")

        # Read CLIENT_SETUP with timeout
        try:
            data = await asyncio.wait_for(self._control_reader.read(), timeout=5.0)
        except asyncio.TimeoutError:
            raise RuntimeError("Timeout waiting for CLIENT_SETUP")

        if not data:
            raise RuntimeError("No data received from client")

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
                role=MOQRole.PUB_SUB  # Relay acts as both
            )

            await self.send_message(response)
            self._is_setup = True
            logger.info(f"Session setup complete, version={hex(selected_version)}")
        else:
            raise RuntimeError(f"Unexpected setup message: {message}")
