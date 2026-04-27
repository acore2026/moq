"""
MOQ Transport - Combined native QUIC + WebTransport server on one UDP port.
"""

import asyncio
import ipaddress
import logging
import tempfile
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
from .webtransport import WEBTRANSPORT_DRAFT_HEADER, WebTransportSessionConnection

logger = logging.getLogger(__name__)

if AIOQUIC_AVAILABLE:
    from aioquic.asyncio import QuicConnectionProtocol, serve
    from aioquic.h3.connection import H3_ALPN, H3Connection
    from aioquic.h3.events import DatagramReceived, H3Event, HeadersReceived, WebTransportStreamDataReceived
    from aioquic.quic.configuration import QuicConfiguration
    from aioquic.quic.events import (
        ConnectionTerminated,
        DatagramFrameReceived,
        ProtocolNegotiated,
        QuicEvent,
        StreamDataReceived,
        StopSendingReceived,
        StreamReset,
    )

if CRYPTOGRAPHY_AVAILABLE:
    from cryptography import x509
    from cryptography.hazmat.primitives import hashes, serialization
    from cryptography.hazmat.primitives.asymmetric import rsa
    from cryptography.x509.oid import NameOID


class MOQCombinedServerProtocol(_KeepAliveProtocolMixin, QuicConnectionProtocol):
    """One QUIC protocol that accepts either native MOQ or WebTransport."""

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
        self._mode: Optional[str] = None
        self._http: Optional[H3Connection] = None
        self._stream_buffers: Dict[int, bytes] = {}
        self._wt_sessions: Dict[int, WebTransportSessionConnection] = {}
        self._wt_stream_sessions: Dict[int, WebTransportSessionConnection] = {}
        self._dispatcher = _OrderedCallbackDispatcher(logger)
        self._init_keepalive(logger, interval=DEFAULT_QUIC_KEEPALIVE_INTERVAL)

    def connection_made(self, transport) -> None:
        super().connection_made(transport)
        self._start_keepalive()

    def connection_lost(self, exc) -> None:
        self._stop_keepalive()

    def quic_event_received(self, event: QuicEvent) -> None:
        if isinstance(event, ConnectionTerminated):
            self._stop_keepalive()

        if isinstance(event, ProtocolNegotiated):
            if event.alpn_protocol in H3_ALPN:
                self._mode = "webtransport"
                self._http = H3Connection(self._quic, enable_webtransport=True)
            else:
                self._mode = "quic"
                self._dispatcher.enqueue(self._on_client_connect, self)

        if self._mode == "webtransport":
            self._handle_webtransport_event(event)
        elif self._mode == "quic":
            self._handle_native_quic_event(event)

    def _handle_native_quic_event(self, event: QuicEvent) -> None:
        if isinstance(event, StreamDataReceived):
            if event.stream_id not in self._stream_buffers:
                self._stream_buffers[event.stream_id] = b""
            self._stream_buffers[event.stream_id] += event.data
            self._dispatcher.enqueue(
                self._on_stream_data,
                self,
                StreamData(
                    stream_id=event.stream_id,
                    data=event.data,
                    end_stream=event.end_stream,
                ),
            )
            if event.end_stream:
                self._stream_buffers.pop(event.stream_id, None)
        elif isinstance(event, StreamReset):
            self._stream_buffers.pop(event.stream_id, None)
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
            self._dispatcher.enqueue(self._on_datagram, self, DatagramData(data=event.data))
        elif isinstance(event, ConnectionTerminated):
            self._dispatcher.enqueue(
                self._on_client_disconnect,
                self,
                event.error_code,
                event.reason_phrase,
            )

    def _handle_webtransport_event(self, event: QuicEvent) -> None:
        if isinstance(event, ConnectionTerminated):
            for session in tuple(self._wt_sessions.values()):
                self._dispatcher.enqueue(
                    self._on_client_disconnect,
                    session,
                    event.error_code,
                    event.reason_phrase,
                )
            self._wt_sessions.clear()
            self._wt_stream_sessions.clear()
            return

        if isinstance(event, (StreamReset, StopSendingReceived)):
            session = self._wt_stream_sessions.get(event.stream_id)
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

        if self._http is None:
            return

        for http_event in self._http.handle_event(event):
            self._handle_http_event(http_event)

    def _handle_http_event(self, event: H3Event) -> None:
        if isinstance(event, HeadersReceived):
            self._handle_headers(event)
        elif isinstance(event, DatagramReceived):
            session = self._wt_sessions.get(event.stream_id)
            if session:
                self._dispatcher.enqueue(self._on_datagram, session, DatagramData(data=event.data))
        elif isinstance(event, WebTransportStreamDataReceived):
            session = self._wt_sessions.get(event.session_id)
            if session:
                self._wt_stream_sessions[event.stream_id] = session
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

        assert self._http is not None
        self._http.send_headers(
            stream_id=event.stream_id,
            headers=[
                (b":status", b"200"),
                (b"server", b"moq-combined"),
                (b"sec-webtransport-http3-draft", WEBTRANSPORT_DRAFT_HEADER),
            ],
            end_stream=False,
        )
        self.transmit()

        session = WebTransportSessionConnection(protocol=self, session_id=event.stream_id)
        self._wt_sessions[event.stream_id] = session
        self._dispatcher.enqueue(self._on_client_connect, session)

    def _reject_session(self, stream_id: int, status: int) -> None:
        assert self._http is not None
        self._http.send_headers(
            stream_id=stream_id,
            headers=[(b":status", str(status).encode("ascii"))],
            end_stream=True,
        )
        self.transmit()


class CombinedTransportServer:
    """Single-port server accepting native MOQ and WebTransport."""

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
            raise RuntimeError("aioquic is required for combined transport")

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
            alpn_protocols=["moq-00", *H3_ALPN],
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

    def _create_protocol(self, *args, **kwargs) -> MOQCombinedServerProtocol:
        return MOQCombinedServerProtocol(
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
                "SSL certificate is required for a combined server and cryptography "
                "is not available to generate a self-signed certificate."
            )

        self._temp_cert_dir = tempfile.TemporaryDirectory(prefix="moq-combined-")
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

    async def start(self) -> None:
        logger.info("Starting combined MOQ transport server on %s:%d", self.host, self.port)
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
