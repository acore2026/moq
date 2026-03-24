"""
MOQ Transport - QUIC Transport layer using aioquic.
Provides QUIC connection management with connection migration support.
"""

import asyncio
import logging
from typing import Optional, Callable, Dict, Set, Tuple, Any
from dataclasses import dataclass

logger = logging.getLogger(__name__)

# Try to import aioquic, provide helpful error if not available
try:
    from aioquic.asyncio import QuicConnectionProtocol, serve
    from aioquic.quic.configuration import QuicConfiguration
    from aioquic.quic.connection import QuicConnection
    from aioquic.quic.events import (
        QuicEvent, StreamDataReceived, StreamReset, ConnectionTerminated,
        DatagramFrameReceived
    )
    AIOQUIC_AVAILABLE = True
except ImportError:
    AIOQUIC_AVAILABLE = False
    logger.warning("aioquic not available. QUIC transport will not function.")


@dataclass
class StreamData:
    """Data received on a stream."""
    stream_id: int
    data: bytes
    end_stream: bool = False


@dataclass
class DatagramData:
    """Data received as datagram."""
    data: bytes


class MOQQuicProtocol(QuicConnectionProtocol):
    """QUIC protocol handler for MOQ Transport."""
    
    def __init__(self, *args, on_stream_data: Optional[Callable] = None,
                 on_datagram: Optional[Callable] = None,
                 on_connection_close: Optional[Callable] = None,
                 **kwargs):
        super().__init__(*args, **kwargs)
        self._on_stream_data = on_stream_data
        self._on_datagram = on_datagram
        self._on_connection_close = on_connection_close
        self._stream_buffers: Dict[int, bytes] = {}
        logger.info("MOQQuicProtocol initialized")
    
    def quic_event_received(self, event: QuicEvent) -> None:
        """Handle QUIC events."""
        if isinstance(event, StreamDataReceived):
            logger.debug(f"Stream data received: stream_id={event.stream_id}, length={len(event.data)}, end={event.end_stream}")
            
            # Buffer the data
            if event.stream_id not in self._stream_buffers:
                self._stream_buffers[event.stream_id] = b''
            self._stream_buffers[event.stream_id] += event.data
            
            # Notify handler
            if self._on_stream_data:
                data = StreamData(
                    stream_id=event.stream_id,
                    data=event.data,
                    end_stream=event.end_stream
                )
                asyncio.create_task(self._on_stream_data(self, data))
            
            # Clean up if stream ended
            if event.end_stream and event.stream_id in self._stream_buffers:
                del self._stream_buffers[event.stream_id]
        
        elif isinstance(event, StreamReset):
            logger.warning(f"Stream reset: stream_id={event.stream_id}, error_code={event.error_code}")
            if event.stream_id in self._stream_buffers:
                del self._stream_buffers[event.stream_id]
        
        elif isinstance(event, DatagramFrameReceived):
            logger.debug(f"Datagram received: length={len(event.data)}")
            if self._on_datagram:
                data = DatagramData(data=event.data)
                asyncio.create_task(self._on_datagram(self, data))
        
        elif isinstance(event, ConnectionTerminated):
            logger.info(f"Connection terminated: error_code={event.error_code}, reason={event.reason_phrase}")
            if self._on_connection_close:
                asyncio.create_task(self._on_connection_close(self, event.error_code, event.reason_phrase))


class QUICClient:
    """QUIC client for MOQ Transport."""
    
    def __init__(self, host: str, port: int, use_datagrams: bool = True):
        if not AIOQUIC_AVAILABLE:
            raise RuntimeError("aioquic is required for QUIC transport")
        
        self.host = host
        self.port = port
        self.use_datagrams = use_datagrams
        self.protocol: Optional[MOQQuicProtocol] = None
        self._connection: Optional[QuicConnection] = None
        self._on_stream_data: Optional[Callable] = None
        self._on_datagram: Optional[Callable] = None
        self._on_close: Optional[Callable] = None
        
        # Configuration
        self._config = QuicConfiguration(
            alpn_protocols=["moq-00"],
            is_client=True,
            max_datagram_frame_size=65536 if use_datagrams else None,
        )
    
    def set_handlers(self, 
                     on_stream_data: Optional[Callable] = None,
                     on_datagram: Optional[Callable] = None,
                     on_close: Optional[Callable] = None):
        """Set event handlers."""
        self._on_stream_data = on_stream_data
        self._on_datagram = on_datagram
        self._on_close = on_close
    
    async def connect(self) -> bool:
        """Connect to QUIC server."""
        logger.info(f"Connecting to {self.host}:{self.port}")
        
        try:
            loop = asyncio.get_event_loop()
            
            # Create connection
            from aioquic.asyncio.client import connect
            
            async with connect(
                self.host,
                self.port,
                configuration=self._config,
                create_protocol=lambda *args, **kwargs: MOQQuicProtocol(
                    *args,
                    on_stream_data=self._on_stream_data,
                    on_datagram=self._on_datagram,
                    on_connection_close=self._on_close,
                    **kwargs
                )
            ) as protocol:
                self.protocol = protocol
                self._connection = protocol._quic
                logger.info("QUIC connection established")
                return True
                
        except Exception as e:
            logger.error(f"Failed to connect: {e}")
            return False
    
    async def open_stream(self, unidirectional: bool = False) -> int:
        """Open a new stream."""
        if not self.protocol:
            raise RuntimeError("Not connected")
        
        stream_id = self._connection.get_next_available_stream_id(is_unidirectional=unidirectional)
        logger.debug(f"Opened stream: {stream_id}, unidirectional={unidirectional}")
        return stream_id
    
    async def send_stream_data(self, stream_id: int, data: bytes, end_stream: bool = False):
        """Send data on a stream."""
        if not self.protocol:
            raise RuntimeError("Not connected")
        
        self._connection.send_stream_data(stream_id, data, end_stream)
        self.protocol.transmit()
        logger.debug(f"Sent {len(data)} bytes on stream {stream_id}")
    
    async def send_datagram(self, data: bytes):
        """Send datagram."""
        if not self.protocol:
            raise RuntimeError("Not connected")
        
        if not self.use_datagrams:
            raise RuntimeError("Datagrams not enabled")
        
        self._connection.send_datagram_frame(data)
        self.protocol.transmit()
        logger.debug(f"Sent datagram: {len(data)} bytes")
    
    def close(self):
        """Close the connection."""
        if self.protocol:
            self.protocol.close()
            logger.info("QUIC connection closed")


class QUICServer:
    """QUIC server for MOQ Transport."""
    
    def __init__(self, host: str, port: int, use_datagrams: bool = True, cert_file: Optional[str] = None, key_file: Optional[str] = None):
        if not AIOQUIC_AVAILABLE:
            raise RuntimeError("aioquic is required for QUIC transport")
        
        self.host = host
        self.port = port
        self.use_datagrams = use_datagrams
        self.cert_file = cert_file
        self.key_file = key_file
        self._server = None
        self._on_client_connect: Optional[Callable] = None
        self._on_stream_data: Optional[Callable] = None
        self._on_datagram: Optional[Callable] = None
        self._on_client_disconnect: Optional[Callable] = None
        
        # Configuration
        self._config = QuicConfiguration(
            alpn_protocols=["moq-00"],
            is_client=False,
            max_datagram_frame_size=65536 if use_datagrams else None,
        )
        
        if cert_file and key_file:
            self._config.load_cert_chain(cert_file, key_file)
    
    def set_handlers(self,
                     on_client_connect: Optional[Callable] = None,
                     on_stream_data: Optional[Callable] = None,
                     on_datagram: Optional[Callable] = None,
                     on_client_disconnect: Optional[Callable] = None):
        """Set event handlers."""
        self._on_client_connect = on_client_connect
        self._on_stream_data = on_stream_data
        self._on_datagram = on_datagram
        self._on_client_disconnect = on_client_disconnect
    
    def _create_protocol(self) -> MOQQuicProtocol:
        """Create protocol instance for new connection."""
        return MOQQuicProtocol(
            quic=None,  # Will be set by aioquic
            on_stream_data=self._on_stream_data,
            on_datagram=self._on_datagram,
            on_connection_close=self._on_client_disconnect
        )
    
    async def start(self):
        """Start the QUIC server."""
        logger.info(f"Starting QUIC server on {self.host}:{self.port}")
        
        self._server = await serve(
            self.host,
            self.port,
            configuration=self._config,
            create_protocol=self._create_protocol
        )
        
        logger.info("QUIC server started")
    
    async def stop(self):
        """Stop the QUIC server."""
        if self._server:
            self._server.close()
            await self._server.wait_closed()
            logger.info("QUIC server stopped")


def is_quic_available() -> bool:
    """Check if QUIC is available."""
    return AIOQUIC_AVAILABLE
