"""
MOQ Transport - QUIC Transport layer using aioquic.
Provides QUIC connection management with connection migration support.
"""

import asyncio
import ipaddress
import logging
import ssl
import tempfile
from collections import deque
from datetime import datetime, timedelta, timezone
from typing import Optional, Callable, Dict, Deque, Set, Tuple, Any
from dataclasses import dataclass

logger = logging.getLogger(__name__)

DEFAULT_QUIC_MAX_DATA = 64 * 1024 * 1024
DEFAULT_QUIC_MAX_STREAM_DATA = 64 * 1024 * 1024
DEFAULT_QUIC_CONGESTION_CONTROL = "cubic"
DEFAULT_QUIC_IDLE_TIMEOUT = 300.0
DEFAULT_QUIC_KEEPALIVE_INTERVAL = 20.0

# Try to import aioquic, provide helpful error if not available
try:
    from aioquic.asyncio import QuicConnectionProtocol, serve
    from aioquic.quic.configuration import QuicConfiguration
    from aioquic.quic.connection import QuicConnection
    from aioquic.quic.events import (
        QuicEvent, StreamDataReceived, StreamReset, StopSendingReceived, ConnectionTerminated,
        DatagramFrameReceived
    )
    AIOQUIC_AVAILABLE = True
except ImportError:
    AIOQUIC_AVAILABLE = False
    logger.warning("aioquic not available. QUIC transport will not function.")

try:
    from cryptography import x509
    from cryptography.hazmat.primitives import hashes, serialization
    from cryptography.hazmat.primitives.asymmetric import rsa
    from cryptography.x509.oid import NameOID
    CRYPTOGRAPHY_AVAILABLE = True
except ImportError:
    CRYPTOGRAPHY_AVAILABLE = False


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


@dataclass
class StreamResetData:
    """Peer-initiated stream termination signal."""
    stream_id: int
    error_code: int
    event_type: str = "reset"


def is_unidirectional_stream_id(stream_id: int) -> bool:
    """Return True when a QUIC/WebTransport stream id is unidirectional."""
    return bool(stream_id & 0x02)


class _OrderedCallbackDispatcher:
    """Run async callbacks serially in enqueue order."""

    def __init__(self, logger: logging.Logger):
        self._logger = logger
        self._queue: Deque[tuple[Callable, tuple[Any, ...]]] = deque()
        self._drain_task: Optional[asyncio.Task] = None

    def enqueue(self, callback: Optional[Callable], *args: Any) -> None:
        if callback is None:
            return
        self._queue.append((callback, args))
        if self._drain_task is None or self._drain_task.done():
            self._drain_task = asyncio.create_task(self._drain())

    async def _drain(self) -> None:
        while self._queue:
            callback, args = self._queue.popleft()
            try:
                await callback(*args)
            except Exception:
                self._logger.exception("Ordered callback failed")


class _KeepAliveProtocolMixin:
    """Send periodic QUIC PING frames to prevent idle timeout closure."""

    def _init_keepalive(
        self,
        keepalive_logger: logging.Logger,
        interval: float = DEFAULT_QUIC_KEEPALIVE_INTERVAL,
    ) -> None:
        self._keepalive_logger = keepalive_logger
        self._keepalive_interval = interval
        self._keepalive_task: Optional[asyncio.Task] = None
        self._keepalive_ping_uid = 0

    def _start_keepalive(self) -> None:
        if self._keepalive_interval <= 0:
            return
        if self._keepalive_task is None or self._keepalive_task.done():
            self._keepalive_task = asyncio.create_task(self._keepalive_loop())

    def _stop_keepalive(self) -> None:
        task = self._keepalive_task
        self._keepalive_task = None
        if task is not None and not task.done():
            task.cancel()

    async def _keepalive_loop(self) -> None:
        try:
            while True:
                await asyncio.sleep(self._keepalive_interval)
                self._keepalive_ping_uid += 1
                self._quic.send_ping(self._keepalive_ping_uid)
                self.transmit()
                self._keepalive_logger.debug(
                    "Sent QUIC keepalive PING uid=%d",
                    self._keepalive_ping_uid,
                )
        except asyncio.CancelledError:
            pass
        except Exception:
            self._keepalive_logger.exception("QUIC keepalive loop failed")


class MOQQuicProtocol(_KeepAliveProtocolMixin, QuicConnectionProtocol):
    """QUIC protocol handler for MOQ Transport."""
    
    def __init__(self, *args, on_stream_data: Optional[Callable] = None,
                 on_stream_reset: Optional[Callable] = None,
                 on_datagram: Optional[Callable] = None,
                 on_connection_open: Optional[Callable] = None,
                 on_connection_close: Optional[Callable] = None,
                 **kwargs):
        super().__init__(*args, **kwargs)
        self._on_stream_data = on_stream_data
        self._on_stream_reset = on_stream_reset
        self._on_datagram = on_datagram
        self._on_connection_open = on_connection_open
        self._on_connection_close = on_connection_close
        self._stream_buffers: Dict[int, bytes] = {}
        self._dispatcher = _OrderedCallbackDispatcher(logger)
        self._init_keepalive(logger)
        logger.info("MOQQuicProtocol initialized")

    def connection_made(self, transport):
        """Handle new QUIC connection."""
        super().connection_made(transport)
        self._start_keepalive()
        self._dispatcher.enqueue(self._on_connection_open, self)

    def connection_lost(self, exc):
        """Stop connection-local background tasks."""
        self._stop_keepalive()
    
    def quic_event_received(self, event: QuicEvent) -> None:
        """Handle QUIC events."""
        if isinstance(event, StreamDataReceived):
            logger.debug(f"Stream data received: stream_id={event.stream_id}, length={len(event.data)}, end={event.end_stream}")
            
            # Buffer the data
            if event.stream_id not in self._stream_buffers:
                self._stream_buffers[event.stream_id] = b''
            self._stream_buffers[event.stream_id] += event.data
            
            # Notify handler
            data = StreamData(
                stream_id=event.stream_id,
                data=event.data,
                end_stream=event.end_stream
            )
            self._dispatcher.enqueue(self._on_stream_data, self, data)
            
            # Clean up if stream ended
            if event.end_stream and event.stream_id in self._stream_buffers:
                del self._stream_buffers[event.stream_id]
        
        elif isinstance(event, StreamReset):
            logger.warning(f"Stream reset: stream_id={event.stream_id}, error_code={event.error_code}")
            if event.stream_id in self._stream_buffers:
                del self._stream_buffers[event.stream_id]
            self._dispatcher.enqueue(
                self._on_stream_reset,
                self,
                StreamResetData(
                    stream_id=event.stream_id,
                    error_code=event.error_code,
                    event_type="reset",
                ),
            )
        
        elif isinstance(event, StopSendingReceived):
            logger.warning(f"STOP_SENDING received: stream_id={event.stream_id}, error_code={event.error_code}")
            self._dispatcher.enqueue(
                self._on_stream_reset,
                self,
                StreamResetData(
                    stream_id=event.stream_id,
                    error_code=event.error_code,
                    event_type="stop_sending",
                ),
            )
        
        elif isinstance(event, DatagramFrameReceived):
            logger.debug(f"Datagram received: length={len(event.data)}")
            data = DatagramData(data=event.data)
            self._dispatcher.enqueue(self._on_datagram, self, data)
        
        elif isinstance(event, ConnectionTerminated):
            self._stop_keepalive()
            logger.info(f"Connection terminated: error_code={event.error_code}, reason={event.reason_phrase}")
            self._dispatcher.enqueue(
                self._on_connection_close,
                self,
                event.error_code,
                event.reason_phrase,
            )


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
        self._connection_cm = None
        self._on_stream_data: Optional[Callable] = None
        self._on_datagram: Optional[Callable] = None
        self._on_close: Optional[Callable] = None
        
        # Configuration
        self._config = QuicConfiguration(
            alpn_protocols=["moq-00"],
            is_client=True,
            congestion_control_algorithm=DEFAULT_QUIC_CONGESTION_CONTROL,
            idle_timeout=DEFAULT_QUIC_IDLE_TIMEOUT,
            max_data=DEFAULT_QUIC_MAX_DATA,
            max_stream_data=DEFAULT_QUIC_MAX_STREAM_DATA,
            max_datagram_frame_size=65536 if use_datagrams else None,
        )
        # Local relay examples use a self-signed certificate.
        self._config.verify_mode = ssl.CERT_NONE
        logger.info(
            "QUIC client config: congestion=%s max_data=%d max_stream_data=%d",
            DEFAULT_QUIC_CONGESTION_CONTROL,
            DEFAULT_QUIC_MAX_DATA,
            DEFAULT_QUIC_MAX_STREAM_DATA,
        )
    
    def set_handlers(self, 
                     on_stream_data: Optional[Callable] = None,
                     on_stream_reset: Optional[Callable] = None,
                     on_datagram: Optional[Callable] = None,
                     on_close: Optional[Callable] = None):
        """Set event handlers."""
        self._on_stream_data = on_stream_data
        self._on_stream_reset = on_stream_reset
        self._on_datagram = on_datagram
        self._on_close = on_close
    
    async def connect(self) -> bool:
        """Connect to QUIC server."""
        logger.info(f"Connecting to {self.host}:{self.port}")
        
        try:
            # Create connection
            from aioquic.asyncio.client import connect

            self._connection_cm = connect(
                self.host,
                self.port,
                configuration=self._config,
                create_protocol=lambda *args, **kwargs: MOQQuicProtocol(
                    *args,
                    on_stream_data=self._on_stream_data,
                    on_stream_reset=self._on_stream_reset,
                    on_datagram=self._on_datagram,
                    on_connection_close=self._on_close,
                    **kwargs
                )
            )

            self.protocol = await self._connection_cm.__aenter__()
            self._connection = self.protocol._quic
            logger.info("QUIC connection established")
            return True
                
        except Exception as e:
            if self._connection_cm is not None:
                try:
                    await self._connection_cm.__aexit__(type(e), e, e.__traceback__)
                except Exception:
                    pass
                self._connection_cm = None
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

    async def stop_stream(self, stream_id: int, error_code: int = 0):
        """Send STOP_SENDING for a peer-initiated stream."""
        if not self.protocol:
            raise RuntimeError("Not connected")

        stop_stream = getattr(self._connection, "stop_stream", None)
        if not callable(stop_stream):
            raise RuntimeError("Underlying QUIC connection does not support STOP_SENDING")
        stop_stream(stream_id, error_code)
        self.protocol.transmit()
        logger.debug(f"Sent STOP_SENDING on stream {stream_id} error={error_code}")

    async def reset_stream(self, stream_id: int, error_code: int = 0):
        """Reset a locally initiated stream."""
        if not self.protocol:
            raise RuntimeError("Not connected")

        reset_stream = getattr(self._connection, "reset_stream", None)
        if not callable(reset_stream):
            raise RuntimeError("Underlying QUIC connection does not support RESET_STREAM")
        reset_stream(stream_id, error_code)
        self.protocol.transmit()
        logger.debug(f"Sent RESET_STREAM on stream {stream_id} error={error_code}")
    
    def close(self):
        """Close the connection."""
        if self.protocol:
            self.protocol.close()
            logger.info("QUIC connection closed")
        if self._connection_cm is not None:
            try:
                loop = asyncio.get_running_loop()
                loop.create_task(self._connection_cm.__aexit__(None, None, None))
            except RuntimeError:
                pass
            self._connection_cm = None


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
        self._temp_cert_dir = None
        self._server = None
        self._on_client_connect: Optional[Callable] = None
        self._on_stream_data: Optional[Callable] = None
        self._on_stream_reset: Optional[Callable] = None
        self._on_datagram: Optional[Callable] = None
        self._on_client_disconnect: Optional[Callable] = None
        
        # Configuration
        self._config = QuicConfiguration(
            alpn_protocols=["moq-00"],
            is_client=False,
            congestion_control_algorithm=DEFAULT_QUIC_CONGESTION_CONTROL,
            idle_timeout=DEFAULT_QUIC_IDLE_TIMEOUT,
            max_data=DEFAULT_QUIC_MAX_DATA,
            max_stream_data=DEFAULT_QUIC_MAX_STREAM_DATA,
            max_datagram_frame_size=65536 if use_datagrams else None,
        )
        logger.info(
            "QUIC server config: congestion=%s max_data=%d max_stream_data=%d",
            DEFAULT_QUIC_CONGESTION_CONTROL,
            DEFAULT_QUIC_MAX_DATA,
            DEFAULT_QUIC_MAX_STREAM_DATA,
        )
        
        if cert_file and key_file:
            self._config.load_cert_chain(cert_file, key_file)
        else:
            self._ensure_self_signed_cert()
    
    def set_handlers(self,
                     on_client_connect: Optional[Callable] = None,
                     on_stream_data: Optional[Callable] = None,
                     on_stream_reset: Optional[Callable] = None,
                     on_datagram: Optional[Callable] = None,
                     on_client_disconnect: Optional[Callable] = None):
        """Set event handlers."""
        self._on_client_connect = on_client_connect
        self._on_stream_data = on_stream_data
        self._on_stream_reset = on_stream_reset
        self._on_datagram = on_datagram
        self._on_client_disconnect = on_client_disconnect
    
    def _create_protocol(self, *args, **kwargs) -> MOQQuicProtocol:
        """Create protocol instance for new connection."""
        return MOQQuicProtocol(
            *args,
            on_stream_data=self._on_stream_data,
            on_stream_reset=self._on_stream_reset,
            on_datagram=self._on_datagram,
            on_connection_open=self._on_client_connect,
            on_connection_close=self._on_client_disconnect,
            **kwargs
        )

    def _ensure_self_signed_cert(self):
        """Generate a self-signed certificate for local development if needed."""
        if self._config.certificate and self._config.private_key:
            return

        if not CRYPTOGRAPHY_AVAILABLE:
            raise RuntimeError(
                "SSL certificate is required for a server and cryptography is not available "
                "to generate a self-signed certificate."
            )

        self._temp_cert_dir = tempfile.TemporaryDirectory(prefix="moq-quic-")
        cert_path = f"{self._temp_cert_dir.name}/cert.pem"
        key_path = f"{self._temp_cert_dir.name}/key.pem"

        key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
        subject = issuer = x509.Name([
            x509.NameAttribute(NameOID.COMMON_NAME, self.host),
        ])

        san_values = [x509.DNSName("localhost")]
        try:
            san_values.append(x509.IPAddress(ipaddress.ip_address(self.host)))
        except ValueError:
            san_values.append(x509.DNSName(self.host))

        cert = (
            x509.CertificateBuilder()
            .subject_name(subject)
            .issuer_name(issuer)
            .public_key(key.public_key())
            .serial_number(x509.random_serial_number())
            .not_valid_before(datetime.now(timezone.utc) - timedelta(minutes=1))
            .not_valid_after(datetime.now(timezone.utc) + timedelta(days=30))
            .add_extension(x509.SubjectAlternativeName(san_values), critical=False)
            .sign(key, hashes.SHA256())
        )

        with open(cert_path, "wb") as cert_file:
            cert_file.write(cert.public_bytes(serialization.Encoding.PEM))

        with open(key_path, "wb") as key_file:
            key_file.write(
                key.private_bytes(
                    encoding=serialization.Encoding.PEM,
                    format=serialization.PrivateFormat.TraditionalOpenSSL,
                    encryption_algorithm=serialization.NoEncryption(),
                )
            )

        self._config.load_cert_chain(cert_path, key_path)
        logger.info("Generated temporary self-signed certificate for QUIC server")
    
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
            wait_closed = getattr(self._server, "wait_closed", None)
            if callable(wait_closed):
                await wait_closed()
            else:
                # aioquic's server object does not expose wait_closed().
                await asyncio.sleep(0)
            logger.info("QUIC server stopped")
        if self._temp_cert_dir is not None:
            self._temp_cert_dir.cleanup()
            self._temp_cert_dir = None


def is_quic_available() -> bool:
    """Check if QUIC is available."""
    return AIOQUIC_AVAILABLE
