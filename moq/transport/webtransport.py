"""
MOQ Transport - WebTransport transport layer using aioquic HTTP/3.
Provides WebTransport session management with stream / datagram callbacks.
"""

import asyncio
import ipaddress
import logging
import ssl
import tempfile
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Callable, Dict, Optional

from .quic_transport import (
    AIOQUIC_AVAILABLE,
    CRYPTOGRAPHY_AVAILABLE,
    DEFAULT_QUIC_CONGESTION_CONTROL,
    DEFAULT_QUIC_IDLE_TIMEOUT,
    DEFAULT_QUIC_KEEPALIVE_INTERVAL,
    DEFAULT_QUIC_MAX_DATA,
    DEFAULT_QUIC_MAX_STREAM_DATA,
    _KeepAliveProtocolMixin,
    _OrderedCallbackDispatcher,
    DatagramData,
    StreamData,
    StreamResetData,
)

logger = logging.getLogger(__name__)

if AIOQUIC_AVAILABLE:
    from aioquic.asyncio import QuicConnectionProtocol, serve
    from aioquic.asyncio.client import connect
    from aioquic.h3.connection import H3_ALPN, H3Connection
    from aioquic.h3.events import (
        DatagramReceived,
        H3Event,
        HeadersReceived,
        WebTransportStreamDataReceived,
    )
    from aioquic.quic.configuration import QuicConfiguration
    from aioquic.quic.events import (
        ConnectionTerminated,
        ProtocolNegotiated,
        QuicEvent,
        StopSendingReceived,
        StreamReset,
    )

if CRYPTOGRAPHY_AVAILABLE:
    from cryptography import x509
    from cryptography.hazmat.primitives import hashes, serialization
    from cryptography.hazmat.primitives.asymmetric import rsa
    from cryptography.x509.oid import NameOID


WEBTRANSPORT_DRAFT_HEADER = b"draft02"


class WebTransportError(RuntimeError):
    """Raised when a WebTransport session cannot be established."""


@dataclass
class WebTransportSessionConnection:
    """A WebTransport session wrapper exposing a QUIC-like API."""

    protocol: "MOQWebTransportProtocolBase"
    session_id: int

    async def open_stream(self, unidirectional: bool = False) -> int:
        stream_id = self.protocol.http.create_webtransport_stream(
            session_id=self.session_id,
            is_unidirectional=unidirectional,
        )
        self.protocol.transmit()
        return stream_id

    async def send_stream_data(
        self,
        stream_id: int,
        data: bytes,
        end_stream: bool = False,
    ) -> None:
        self.protocol.http._quic.send_stream_data(stream_id, data, end_stream=end_stream)
        self.protocol.transmit()

    async def send_datagram(self, data: bytes) -> None:
        self.protocol.http.send_datagram(self.session_id, data)
        self.protocol.transmit()

    async def stop_stream(self, stream_id: int, error_code: int = 0) -> None:
        stop_stream = getattr(self.protocol.http._quic, "stop_stream", None)
        if not callable(stop_stream):
            raise RuntimeError("Underlying QUIC connection does not support STOP_SENDING")
        stop_stream(stream_id, error_code)
        self.protocol.transmit()

    async def reset_stream(self, stream_id: int, error_code: int = 0) -> None:
        reset_stream = getattr(self.protocol.http._quic, "reset_stream", None)
        if not callable(reset_stream):
            raise RuntimeError("Underlying QUIC connection does not support RESET_STREAM")
        reset_stream(stream_id, error_code)
        self.protocol.transmit()

    def close(self, error_code: int = 0, reason: str = "") -> None:
        self.protocol.close(error_code=error_code, reason_phrase=reason)


class MOQWebTransportProtocolBase(_KeepAliveProtocolMixin, QuicConnectionProtocol):
    """Shared HTTP/3 + WebTransport protocol logic."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._http: Optional[H3Connection] = None
        self._init_keepalive(logger, interval=DEFAULT_QUIC_KEEPALIVE_INTERVAL)

    def connection_made(self, transport) -> None:
        super().connection_made(transport)
        self._start_keepalive()

    def connection_lost(self, exc) -> None:
        self._stop_keepalive()

    @property
    def http(self) -> H3Connection:
        if self._http is None:
            self._http = H3Connection(self._quic, enable_webtransport=True)
        return self._http

    def quic_event_received(self, event: QuicEvent) -> None:
        if isinstance(event, ProtocolNegotiated) and event.alpn_protocol in H3_ALPN:
            self._http = H3Connection(self._quic, enable_webtransport=True)
        elif isinstance(event, ConnectionTerminated):
            self._stop_keepalive()

        self._handle_quic_lifecycle_event(event)

        if self._http is None:
            return

        for http_event in self._http.handle_event(event):
            self._handle_http_event(http_event)

    def _handle_quic_lifecycle_event(self, event: QuicEvent) -> None:
        """Hook for subclasses."""

    def _handle_http_event(self, event: H3Event) -> None:
        """Hook for subclasses."""


class MOQWebTransportClientProtocol(MOQWebTransportProtocolBase):
    """Client-side WebTransport session handler."""

    def __init__(
        self,
        *args,
        on_stream_data: Optional[Callable] = None,
        on_stream_reset: Optional[Callable] = None,
        on_datagram: Optional[Callable] = None,
        on_connection_close: Optional[Callable] = None,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self._on_stream_data = on_stream_data
        self._on_stream_reset = on_stream_reset
        self._on_datagram = on_datagram
        self._on_connection_close = on_connection_close
        self._dispatcher = _OrderedCallbackDispatcher(logger)
        self._session_future: asyncio.Future = asyncio.get_running_loop().create_future()
        self._session_id: Optional[int] = None
        self.session: Optional[WebTransportSessionConnection] = None
        self._stream_sessions: Dict[int, WebTransportSessionConnection] = {}

    async def establish_session(
        self,
        host: str,
        port: int,
        path: str,
        authority: Optional[str] = None,
        headers: Optional[list[tuple[bytes, bytes]]] = None,
        timeout: float = 10.0,
    ) -> WebTransportSessionConnection:
        connect_stream_id = self._quic.get_next_available_stream_id()
        self._session_id = connect_stream_id
        authority_value = authority or f"{host}:{port}"
        request_headers = [
            (b":method", b"CONNECT"),
            (b":scheme", b"https"),
            (b":authority", authority_value.encode("utf-8")),
            (b":path", path.encode("utf-8")),
            (b":protocol", b"webtransport"),
            (b"sec-webtransport-http3-draft", WEBTRANSPORT_DRAFT_HEADER),
        ]
        if headers:
            request_headers.extend(headers)
        self.http.send_headers(connect_stream_id, request_headers, end_stream=False)
        self.transmit()
        return await asyncio.wait_for(self._session_future, timeout=timeout)

    def _handle_quic_lifecycle_event(self, event: QuicEvent) -> None:
        if isinstance(event, ConnectionTerminated):
            if not self._session_future.done():
                self._session_future.set_exception(
                    WebTransportError(
                        f"connection terminated before session establishment: "
                        f"{event.error_code} {event.reason_phrase}"
                    )
                )
            self._dispatcher.enqueue(
                self._on_connection_close,
                self,
                event.error_code,
                event.reason_phrase,
            )
        elif isinstance(event, (StreamReset, StopSendingReceived)):
            session = self._stream_sessions.get(event.stream_id)
            if session is not None:
                self._dispatcher.enqueue(
                    self._on_stream_reset,
                    session,
                    StreamResetData(
                        stream_id=event.stream_id,
                        error_code=event.error_code,
                        event_type="reset" if isinstance(event, StreamReset) else "stop_sending",
                    ),
                )

    def _handle_http_event(self, event: H3Event) -> None:
        if isinstance(event, HeadersReceived) and event.stream_id == self._session_id:
            headers = {name: value for name, value in event.headers}
            status = headers.get(b":status", b"").decode("ascii", errors="ignore")
            if status == "200":
                self.session = WebTransportSessionConnection(
                    protocol=self,
                    session_id=event.stream_id,
                )
                if not self._session_future.done():
                    self._session_future.set_result(self.session)
            elif not self._session_future.done():
                self._session_future.set_exception(
                    WebTransportError(f"webtransport CONNECT rejected with status {status or 'unknown'}")
                )
        elif isinstance(event, DatagramReceived) and event.stream_id == self._session_id:
            self._dispatcher.enqueue(self._on_datagram, self.session, DatagramData(data=event.data))
        elif (
            isinstance(event, WebTransportStreamDataReceived)
            and event.session_id == self._session_id
        ):
            if self.session is not None:
                self._stream_sessions[event.stream_id] = self.session
            self._dispatcher.enqueue(
                self._on_stream_data,
                self.session,
                StreamData(
                    stream_id=event.stream_id,
                    data=event.data,
                    end_stream=event.stream_ended,
                )
            )


class MOQWebTransportServerProtocol(MOQWebTransportProtocolBase):
    """Server-side WebTransport protocol handler."""

    def __init__(
        self,
        *args,
        session_path: str,
        on_client_connect: Optional[Callable] = None,
        on_stream_data: Optional[Callable] = None,
        on_stream_reset: Optional[Callable] = None,
        on_datagram: Optional[Callable] = None,
        on_client_disconnect: Optional[Callable] = None,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self._session_path = session_path
        self._on_client_connect = on_client_connect
        self._on_stream_data = on_stream_data
        self._on_stream_reset = on_stream_reset
        self._on_datagram = on_datagram
        self._on_client_disconnect = on_client_disconnect
        self._dispatcher = _OrderedCallbackDispatcher(logger)
        self._sessions: Dict[int, WebTransportSessionConnection] = {}
        self._stream_sessions: Dict[int, WebTransportSessionConnection] = {}

    def _handle_quic_lifecycle_event(self, event: QuicEvent) -> None:
        if isinstance(event, ConnectionTerminated):
            for session in tuple(self._sessions.values()):
                self._dispatcher.enqueue(
                    self._on_client_disconnect,
                    session,
                    event.error_code,
                    event.reason_phrase,
                )
            self._sessions.clear()
            self._stream_sessions.clear()
        elif isinstance(event, (StreamReset, StopSendingReceived)):
            session = self._stream_sessions.get(event.stream_id)
            if session is not None:
                self._dispatcher.enqueue(
                    self._on_stream_reset,
                    session,
                    StreamResetData(
                        stream_id=event.stream_id,
                        error_code=event.error_code,
                        event_type="reset" if isinstance(event, StreamReset) else "stop_sending",
                    ),
                )

    def _handle_http_event(self, event: H3Event) -> None:
        if isinstance(event, HeadersReceived):
            self._handle_headers(event)
        elif isinstance(event, DatagramReceived):
            session = self._sessions.get(event.stream_id)
            if session:
                self._dispatcher.enqueue(self._on_datagram, session, DatagramData(data=event.data))
        elif isinstance(event, WebTransportStreamDataReceived):
            session = self._sessions.get(event.session_id)
            if session:
                self._stream_sessions[event.stream_id] = session
                self._dispatcher.enqueue(
                    self._on_stream_data,
                    session,
                    StreamData(
                        stream_id=event.stream_id,
                        data=event.data,
                        end_stream=event.stream_ended,
                    ),
                )

    def _handle_headers(self, event: HeadersReceived) -> None:
        headers = {name: value for name, value in event.headers}
        method = headers.get(b":method", b"").decode("utf-8", errors="ignore")
        path = headers.get(b":path", b"").decode("utf-8", errors="ignore")
        protocol = headers.get(b":protocol", b"").decode("utf-8", errors="ignore")

        if method != "CONNECT" or protocol not in {"webtransport", "webtransport-h3"}:
            self._reject_session(event.stream_id, status=400)
            return
        if path != self._session_path:
            self._reject_session(event.stream_id, status=404)
            return

        self.http.send_headers(
            stream_id=event.stream_id,
            headers=[
                (b":status", b"200"),
                (b"server", b"moq-webtransport"),
                (b"sec-webtransport-http3-draft", WEBTRANSPORT_DRAFT_HEADER),
            ],
            end_stream=False,
        )
        self.transmit()

        session = WebTransportSessionConnection(protocol=self, session_id=event.stream_id)
        self._sessions[event.stream_id] = session
        self._dispatcher.enqueue(self._on_client_connect, session)

    def _reject_session(self, stream_id: int, status: int) -> None:
        self.http.send_headers(
            stream_id=stream_id,
            headers=[(b":status", str(status).encode("ascii"))],
            end_stream=True,
        )
        self.transmit()


class WebTransportClient:
    """WebTransport client for MOQ Transport."""

    def __init__(
        self,
        host: str,
        port: int,
        path: str = "/moq",
        authority: Optional[str] = None,
        use_datagrams: bool = True,
    ):
        if not AIOQUIC_AVAILABLE:
            raise RuntimeError("aioquic is required for WebTransport transport")

        self.host = host
        self.port = port
        self.path = path
        self.authority = authority
        self.use_datagrams = use_datagrams
        self.protocol: Optional[MOQWebTransportClientProtocol] = None
        self.session: Optional[WebTransportSessionConnection] = None
        self._connection_cm = None
        self._on_stream_data: Optional[Callable] = None
        self._on_stream_reset: Optional[Callable] = None
        self._on_datagram: Optional[Callable] = None
        self._on_close: Optional[Callable] = None

        self._config = QuicConfiguration(
            alpn_protocols=H3_ALPN,
            is_client=True,
            congestion_control_algorithm=DEFAULT_QUIC_CONGESTION_CONTROL,
            idle_timeout=DEFAULT_QUIC_IDLE_TIMEOUT,
            max_data=DEFAULT_QUIC_MAX_DATA,
            max_stream_data=DEFAULT_QUIC_MAX_STREAM_DATA,
            max_datagram_frame_size=65536 if use_datagrams else None,
        )
        self._config.verify_mode = ssl.CERT_NONE

    def set_handlers(
        self,
        on_stream_data: Optional[Callable] = None,
        on_stream_reset: Optional[Callable] = None,
        on_datagram: Optional[Callable] = None,
        on_close: Optional[Callable] = None,
    ) -> None:
        self._on_stream_data = on_stream_data
        self._on_stream_reset = on_stream_reset
        self._on_datagram = on_datagram
        self._on_close = on_close

    async def connect(self) -> bool:
        logger.info("Connecting to WebTransport server at https://%s:%d%s", self.host, self.port, self.path)
        try:
            self._connection_cm = connect(
                self.host,
                self.port,
                configuration=self._config,
                create_protocol=lambda *args, **kwargs: MOQWebTransportClientProtocol(
                    *args,
                    on_stream_data=self._on_stream_data,
                    on_stream_reset=self._on_stream_reset,
                    on_datagram=self._on_datagram,
                    on_connection_close=self._on_close,
                    **kwargs,
                ),
            )
            self.protocol = await self._connection_cm.__aenter__()
            self.session = await self.protocol.establish_session(
                host=self.host,
                port=self.port,
                path=self.path,
                authority=self.authority,
            )
            logger.info("WebTransport session established")
            return True
        except Exception as exc:
            logger.error("Failed to establish WebTransport session: %s", exc)
            if self._connection_cm is not None:
                try:
                    await self._connection_cm.__aexit__(type(exc), exc, exc.__traceback__)
                except Exception:
                    logger.debug("Failed to clean up WebTransport connection after error", exc_info=True)
                self._connection_cm = None
            return False

    async def open_stream(self, unidirectional: bool = False) -> int:
        if self.session is None:
            raise RuntimeError("Not connected")
        return await self.session.open_stream(unidirectional=unidirectional)

    async def send_stream_data(self, stream_id: int, data: bytes, end_stream: bool = False) -> None:
        if self.session is None:
            raise RuntimeError("Not connected")
        await self.session.send_stream_data(stream_id=stream_id, data=data, end_stream=end_stream)

    async def send_datagram(self, data: bytes) -> None:
        if self.session is None:
            raise RuntimeError("Not connected")
        if not self.use_datagrams:
            raise RuntimeError("Datagrams not enabled")
        await self.session.send_datagram(data)

    async def stop_stream(self, stream_id: int, error_code: int = 0) -> None:
        if self.session is None:
            raise RuntimeError("Not connected")
        await self.session.stop_stream(stream_id, error_code=error_code)

    async def reset_stream(self, stream_id: int, error_code: int = 0) -> None:
        if self.session is None:
            raise RuntimeError("Not connected")
        await self.session.reset_stream(stream_id, error_code=error_code)

    def close(self) -> None:
        if self.session is not None:
            self.session.close()
        if self._connection_cm is not None:
            try:
                loop = asyncio.get_running_loop()
                loop.create_task(self._connection_cm.__aexit__(None, None, None))
            except RuntimeError:
                pass
            self._connection_cm = None


class WebTransportServer:
    """WebTransport server for MOQ Transport."""

    def __init__(
        self,
        host: str,
        port: int,
        path: str = "/moq",
        use_datagrams: bool = True,
        cert_file: Optional[str] = None,
        key_file: Optional[str] = None,
    ):
        if not AIOQUIC_AVAILABLE:
            raise RuntimeError("aioquic is required for WebTransport transport")

        self.host = host
        self.port = port
        self.path = path
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

        self._config = QuicConfiguration(
            alpn_protocols=H3_ALPN,
            is_client=False,
            congestion_control_algorithm=DEFAULT_QUIC_CONGESTION_CONTROL,
            idle_timeout=DEFAULT_QUIC_IDLE_TIMEOUT,
            max_data=DEFAULT_QUIC_MAX_DATA,
            max_stream_data=DEFAULT_QUIC_MAX_STREAM_DATA,
            max_datagram_frame_size=65536 if use_datagrams else None,
        )

        if cert_file and key_file:
            self._config.load_cert_chain(cert_file, key_file)
        else:
            self._ensure_self_signed_cert()

    def set_handlers(
        self,
        on_client_connect: Optional[Callable] = None,
        on_stream_data: Optional[Callable] = None,
        on_stream_reset: Optional[Callable] = None,
        on_datagram: Optional[Callable] = None,
        on_client_disconnect: Optional[Callable] = None,
    ) -> None:
        self._on_client_connect = on_client_connect
        self._on_stream_data = on_stream_data
        self._on_stream_reset = on_stream_reset
        self._on_datagram = on_datagram
        self._on_client_disconnect = on_client_disconnect

    def _create_protocol(self, *args, **kwargs) -> MOQWebTransportServerProtocol:
        return MOQWebTransportServerProtocol(
            *args,
            session_path=self.path,
            on_client_connect=self._on_client_connect,
            on_stream_data=self._on_stream_data,
            on_stream_reset=self._on_stream_reset,
            on_datagram=self._on_datagram,
            on_client_disconnect=self._on_client_disconnect,
            **kwargs,
        )

    def _ensure_self_signed_cert(self) -> None:
        if self._config.certificate and self._config.private_key:
            return

        if not CRYPTOGRAPHY_AVAILABLE:
            raise RuntimeError(
                "SSL certificate is required for WebTransport server and cryptography "
                "is not available to generate a self-signed certificate."
            )

        self._temp_cert_dir = tempfile.TemporaryDirectory(prefix="moq-wt-")
        cert_path = f"{self._temp_cert_dir.name}/cert.pem"
        key_path = f"{self._temp_cert_dir.name}/key.pem"

        key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
        subject = issuer = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, self.host)])

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

        with open(cert_path, "wb") as cert_fp:
            cert_fp.write(cert.public_bytes(serialization.Encoding.PEM))
        with open(key_path, "wb") as key_fp:
            key_fp.write(
                key.private_bytes(
                    encoding=serialization.Encoding.PEM,
                    format=serialization.PrivateFormat.TraditionalOpenSSL,
                    encryption_algorithm=serialization.NoEncryption(),
                )
            )

        self._config.load_cert_chain(cert_path, key_path)
        logger.info("Generated temporary self-signed certificate for WebTransport server")

    async def start(self) -> None:
        logger.info("Starting WebTransport server on https://%s:%d%s", self.host, self.port, self.path)
        self._server = await serve(
            self.host,
            self.port,
            configuration=self._config,
            create_protocol=self._create_protocol,
        )

    async def stop(self) -> None:
        if self._server:
            self._server.close()
            wait_closed = getattr(self._server, "wait_closed", None)
            if callable(wait_closed):
                await wait_closed()
            else:
                await asyncio.sleep(0)
            self._server = None
        if self._temp_cert_dir is not None:
            self._temp_cert_dir.cleanup()
            self._temp_cert_dir = None


def is_webtransport_available() -> bool:
    """Check if WebTransport support is available."""
    return AIOQUIC_AVAILABLE
