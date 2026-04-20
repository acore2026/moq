"""
MOQ Transport - QUIC Transport layer using aioquic.
Provides QUIC connection management with connection migration support.
"""

import asyncio
import ipaddress
import logging
import ssl
import tempfile
from datetime import datetime, timedelta
from typing import Optional, Callable, Dict, Set, Tuple, Any
from dataclasses import dataclass

logger = logging.getLogger(__name__)

# Try to import aioquic, provide helpful error if not available
try:
    from aioquic.asyncio import QuicConnectionProtocol, serve
    from aioquic.quic.configuration import QuicConfiguration
    from aioquic.quic.connection import QuicConnection
    from aioquic.quic.events import (
        QuicEvent,
        StreamDataReceived,
        StreamReset,
        ConnectionTerminated,
        DatagramFrameReceived,
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


class MOQQuicProtocol(QuicConnectionProtocol):
    """QUIC protocol handler for MOQ Transport."""

    def __init__(
        self,
        *args,
        on_stream_data: Optional[Callable] = None,
        on_datagram: Optional[Callable] = None,
        on_connection_open: Optional[Callable] = None,
        on_connection_close: Optional[Callable] = None,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self._on_stream_data = on_stream_data
        self._on_datagram = on_datagram
        self._on_connection_open = on_connection_open
        self._on_connection_close = on_connection_close
        self._stream_buffers: Dict[int, bytes] = {}
        logger.info("MOQQuicProtocol initialized")

    def connection_made(self, transport):
        """Handle new QUIC connection."""
        super().connection_made(transport)
        if self._on_connection_open:
            asyncio.create_task(self._on_connection_open(self))

    def quic_event_received(self, event: QuicEvent) -> None:
        """Handle QUIC events."""
        logger.debug(f"QUIC event received: {type(event).__name__}")
        if isinstance(event, StreamDataReceived):
            logger.debug(
                f"Stream data received: stream_id={event.stream_id}, length={len(event.data)}, end={event.end_stream}"
            )

            # Buffer the data
            if event.stream_id not in self._stream_buffers:
                self._stream_buffers[event.stream_id] = b""
            self._stream_buffers[event.stream_id] += event.data

            # Notify handler
            if self._on_stream_data:
                data = StreamData(
                    stream_id=event.stream_id,
                    data=event.data,
                    end_stream=event.end_stream,
                )
                asyncio.create_task(self._on_stream_data(self, data))

            # Clean up if stream ended
            if event.end_stream and event.stream_id in self._stream_buffers:
                del self._stream_buffers[event.stream_id]

        elif isinstance(event, StreamReset):
            logger.warning(
                f"Stream reset: stream_id={event.stream_id}, error_code={event.error_code}"
            )
            if event.stream_id in self._stream_buffers:
                del self._stream_buffers[event.stream_id]

        elif isinstance(event, DatagramFrameReceived):
            logger.debug(
                f"Datagram received: length={len(event.data)}, raw_hex={event.data.hex()[:60]}..."
            )
            if self._on_datagram:
                data = DatagramData(data=event.data)
                asyncio.create_task(self._on_datagram(self, data))

        elif isinstance(event, ConnectionTerminated):
            logger.info(
                f"Connection terminated: error_code={event.error_code}, reason={event.reason_phrase}"
            )
            if self._on_connection_close:
                asyncio.create_task(
                    self._on_connection_close(
                        self, event.error_code, event.reason_phrase
                    )
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
            max_datagram_frame_size=65536 if use_datagrams else None,
            idle_timeout=300.0,  # 300 seconds (updated parameter name for aioquic compatibility)
        )
        # Local relay examples use a self-signed certificate.
        self._config.verify_mode = ssl.CERT_NONE

    def set_handlers(
        self,
        on_stream_data: Optional[Callable] = None,
        on_datagram: Optional[Callable] = None,
        on_close: Optional[Callable] = None,
    ):
        """Set event handlers."""
        self._on_stream_data = on_stream_data
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
                    on_datagram=self._on_datagram,
                    on_connection_close=self._on_close,
                    **kwargs,
                ),
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

        stream_id = self._connection.get_next_available_stream_id(
            is_unidirectional=unidirectional
        )
        logger.debug(f"Opened stream: {stream_id}, unidirectional={unidirectional}")
        return stream_id

    async def send_stream_data(
        self, stream_id: int, data: bytes, end_stream: bool = False
    ):
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

    async def send_ping(self):
        """Send QUIC PING frame to keep connection alive."""
        if not self.protocol:
            raise RuntimeError("Not connected")

        # Send QUIC PING frame - this is explicitly treated as connection activity
        # Generate a unique uid for the ping
        import time

        uid = int(time.time() * 1000) % 0xFFFFFFFF
        self._connection.send_ping(uid)
        self.protocol.transmit()
        logger.debug("Sent QUIC PING frame for keepalive")

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

    async def aclose(self):
        """Close the connection and wait for aioquic resources to shut down."""
        if self.protocol:
            self.protocol.close()
            logger.info("QUIC connection closed")
        if self._connection_cm is not None:
            try:
                await self._connection_cm.__aexit__(None, None, None)
            finally:
                self._connection_cm = None
                self.protocol = None
                self._connection = None


class QUICServer:
    """QUIC server for MOQ Transport."""

    def __init__(
        self,
        host: str,
        port: int,
        use_datagrams: bool = True,
        cert_file: Optional[str] = None,
        key_file: Optional[str] = None,
    ):
        if not AIOQUIC_AVAILABLE:
            raise RuntimeError("aioquic is required for QUIC transport")

        self.host = host
        self.port = port
        self._actual_port: Optional[int] = None
        self.use_datagrams = use_datagrams
        self.cert_file = cert_file
        self.key_file = key_file
        self._temp_cert_dir = None
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
            idle_timeout=300.0,  # 300 seconds
        )

        if cert_file and key_file:
            self._config.load_cert_chain(cert_file, key_file)
        else:
            self._ensure_self_signed_cert()

    @property
    def actual_port(self) -> Optional[int]:
        """Get the actual port the server is listening on."""
        return self._actual_port

    def set_handlers(
        self,
        on_client_connect: Optional[Callable] = None,
        on_stream_data: Optional[Callable] = None,
        on_datagram: Optional[Callable] = None,
        on_client_disconnect: Optional[Callable] = None,
    ):
        """Set event handlers."""
        self._on_client_connect = on_client_connect
        self._on_stream_data = on_stream_data
        self._on_datagram = on_datagram
        self._on_client_disconnect = on_client_disconnect

    def _create_protocol(self, *args, **kwargs) -> MOQQuicProtocol:
        """Create protocol instance for new connection."""
        return MOQQuicProtocol(
            *args,
            on_stream_data=self._on_stream_data,
            on_datagram=self._on_datagram,
            on_connection_open=self._on_client_connect,
            on_connection_close=self._on_client_disconnect,
            **kwargs,
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
        subject = issuer = x509.Name(
            [
                x509.NameAttribute(NameOID.COMMON_NAME, self.host),
            ]
        )

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
            .not_valid_before(datetime.utcnow() - timedelta(minutes=1))
            .not_valid_after(datetime.utcnow() + timedelta(days=30))
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
            create_protocol=self._create_protocol,
        )

        # Get actual port if auto-assigned (port=0)
        if self._server and hasattr(self._server, "_transport"):
            transport = self._server._transport
            if transport and hasattr(transport, "get_extra_info"):
                sockname = transport.get_extra_info("sockname")
                if sockname:
                    self._actual_port = sockname[1]
                    logger.info(
                        f"QUIC server started on {self.host}:{self._actual_port}"
                    )
                    return

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
