"""One ACN Publisher or Subscriber connection, reusable across multiple groups."""

import asyncio
import inspect
import socket
import ssl
from dataclasses import dataclass

from aioquic.quic.configuration import QuicConfiguration
from aioquic.quic.connection import QuicConnection
from aioquic.quic.events import DatagramFrameReceived, StreamDataReceived, StreamReset, StopSendingReceived

from moq.encoding import Parameters, VarInt
from moq.messages import (SetupMessage, PublishMessage, PublishOkMessage, SubscribeMessage,
                          SubscribeOkMessage, PublishDoneMessage, RequestErrorMessage,
                          SubscribeFilter, GroupOrder)
from moq.messages.data import SubgroupHeader, SubgroupObject

from .contracts import (ACN_AGENT_BINDING, ACN_AGENT_BINDING_RESULT, MOQ_VERSION,
                        Limits, encode_json, strict_json)
from .transport import AcnProtocol
from .wire import ControlReader, ObjectReader, json_object


class RequestFailed(Exception):
    def __init__(self, code, reason, retry_interval=0):
        super().__init__(reason)
        self.code, self.retry_interval = code, retry_interval


@dataclass
class Request:
    identifier: int
    alias: int
    track: object
    accepted: object
    stream_id: object = None
    next_object: int = 0
    lock: object = None


class AcnClient:
    def __init__(self, host, port, agent_id, role, *, source_ip=None, limits=Limits(),
                 on_object=None, on_failure=None, on_close=None):
        if role not in ("publisher", "subscriber"):
            raise ValueError("expected Publisher or Subscriber")
        self.host, self.port, self.agent_id, self.role = host, port, agent_id, role
        self.source_ip, self.limits = source_ip, limits
        self.on_object, self.on_failure, self.on_close = on_object, on_failure, on_close
        self.protocol = self.transport = None
        self.bound = False
        self.closed = False
        self.reader = ControlReader()
        self.binding_result = None
        self.requests = {}
        self.streams = {}
        self.timers = {}
        self.sequence = 0

    async def connect(self, timeout=None):
        timeout = min(timeout or self.limits.local_timeout, self.limits.local_timeout)
        self.binding_result = asyncio.get_running_loop().create_future()
        self.binding_result.add_done_callback(lambda done: done.exception() if not done.cancelled() else None)
        config = QuicConfiguration(is_client=True, alpn_protocols=["moq-00"], max_datagram_frame_size=65536)
        config.verify_mode = ssl.CERT_NONE
        quic = QuicConnection(configuration=config)
        packet = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        packet.setblocking(False)
        packet.bind((self.source_ip or "0.0.0.0", 0))
        try:
            self.transport, self.protocol = await asyncio.get_running_loop().create_datagram_endpoint(
                lambda: AcnProtocol(quic, owner=self), sock=packet)
            self.protocol.connect((self.host, self.port), transmit=True)
            await asyncio.wait_for(self.protocol.wait_connected(), timeout)
            parameters = Parameters()
            parameters.set(ACN_AGENT_BINDING, encode_json({"binding_version": 1, "agent_id": self.agent_id, "role": self.role}))
            self._send(SetupMessage(MOQ_VERSION, 1 if self.role == "publisher" else 2, parameters))
            await asyncio.wait_for(self.binding_result, timeout)
        except BaseException:
            packet.close()
            await self.close()
            raise

    def _send(self, message):
        self.protocol._quic.send_stream_data(0, message.encode())
        self.protocol.transmit()

    async def request(self, track, timeout=None, on_created=None):
        if not self.bound or self.closed:
            raise ConnectionError("ACN role is not bound")
        identifier = self.sequence
        self.sequence += 1
        pending = asyncio.get_running_loop().create_future()
        request = Request(identifier, identifier, track, pending, lock=asyncio.Lock())
        self.requests[identifier] = request
        if on_created:
            on_created(request)
        message = (PublishMessage(identifier, identifier, track) if self.role == "publisher"
                   else SubscribeMessage(identifier, identifier, track, 128, GroupOrder.ASCENDING, SubscribeFilter.LATEST_OBJECT))
        self._send(message)
        try:
            await asyncio.wait_for(pending, min(timeout or self.limits.local_timeout, self.limits.local_timeout))
        except BaseException:
            await self.cancel(request)
            raise
        return request

    async def send(self, request, value, *, datagram=False, timeout=None):
        if self.role != "publisher" or self.requests.get(request.identifier) is not request:
            raise ValueError("active Publisher request required")
        payload = json_object(value, self.limits.object_bytes)
        async with request.lock:
            oid = request.next_object
            request.next_object += 1
            if datagram:
                if request.track.track_name == b"group_catalog":
                    raise ValueError("catalog requires reliable Stream")
                data = b"".join(VarInt.encode(x) for x in (request.alias, 0, oid)) + b"\x80" + VarInt.encode(len(payload)) + payload
                remote = self.protocol._quic._remote_max_datagram_frame_size
                path = self.protocol._quic._max_datagram_size
                if not remote or len(data) + 64 > min(remote, path):
                    raise ValueError("Datagram does not fit negotiated/path packet size")
                self.protocol._quic.send_datagram_frame(data)
                self.protocol.transmit()
                return
            if request.stream_id is None:
                request.stream_id = self.protocol._quic.get_next_available_stream_id(is_unidirectional=True)
                header = VarInt.encode(1) + SubgroupHeader(request.alias, 0, 0, 128).encode()
                self.protocol._quic.send_stream_data(request.stream_id, header)
            await self.protocol.write(request.stream_id, SubgroupObject(oid, payload).encode(),
                                      min(timeout or self.limits.local_timeout, self.limits.local_timeout))

    async def cancel(self, request):
        if self.requests.pop(request.identifier, None) is not request:
            return
        if self.closed or self.protocol is None:
            return
        if self.role == "publisher":
            self._send(PublishDoneMessage(request.identifier, 0, "publisher ended"))
            if request.stream_id is not None:
                self.protocol._quic.reset_stream(request.stream_id, 0)
        elif request.stream_id is not None:
            self.protocol._quic.stop_stream(request.stream_id, 0)
        self.protocol.transmit()

    def _callback(self, callback, *args):
        if callback:
            result = callback(*args)
            if inspect.isawaitable(result):
                # Application callbacks must not block serial protocol dispatch.
                task = asyncio.create_task(result)
                task.add_done_callback(lambda done: done.exception() if not done.cancelled() else None)

    def _failure(self, request, failure):
        if self.requests.pop(request.identifier, None) is request:
            if not request.accepted.done():
                request.accepted.set_exception(failure)
            else:
                self._callback(self.on_failure, request, failure)

    async def event(self, protocol, event):
        if protocol is not self.protocol or self.closed:
            return
        if isinstance(event, StreamDataReceived):
            if event.stream_id == 0:
                for message in self.reader.feed(event.data):
                    self._control(message)
            else:
                try:
                    self._receive_objects(event)
                except ValueError as exc:
                    reader = self.streams.get(event.stream_id)
                    if reader and reader.header:
                        for request in list(self.requests.values()):
                            if request.alias == reader.header.track_alias:
                                self._failure(request, RequestFailed(2, str(exc)))
                    self.protocol._quic.stop_stream(event.stream_id, 2)
                    self.protocol.transmit()
                    self.streams.pop(event.stream_id, None)
        elif isinstance(event, (StreamReset, StopSendingReceived)):
            for request in list(self.requests.values()):
                if request.stream_id == event.stream_id:
                    self._failure(request, RequestFailed(event.error_code, "Track Stream terminated"))
        elif isinstance(event, DatagramFrameReceived):
            raise ValueError("ACN Relay downstream uses reliable Stream")

    def _control(self, message):
        if isinstance(message, SetupMessage):
            if self.binding_result.done() or message.version != MOQ_VERSION:
                raise ValueError("invalid or repeated Relay SETUP")
            result = strict_json(message.parameters.get(ACN_AGENT_BINDING_RESULT), self.limits.binding_bytes)
            if (type(result.get("binding_version")) is not int or result["binding_version"] != 1
                    or result.get("agent_id") != self.agent_id or result.get("role") != self.role):
                self.binding_result.set_exception(ValueError("Relay binding result mismatch"))
            elif result.get("status") != "ACCEPTED":
                code = result.get("code", "INVALID_BINDING")
                self.binding_result.set_exception(RequestFailed(code, "ACN binding rejected", 501 if code == "AGENT_NOT_REGISTERED" else 0))
            else:
                self.bound = True
                self.binding_result.set_result(True)
            return
        if not self.bound:
            raise ValueError("business response before ACN binding")
        request = self.requests.get(getattr(message, "request_id", None))
        if request is None:
            return  # Retired/expired request, never reuse its alias.
        if isinstance(message, (PublishOkMessage, SubscribeOkMessage)):
            if (self.role == "publisher") != isinstance(message, PublishOkMessage):
                raise ValueError("response direction mismatch")
            if not request.accepted.done():
                request.accepted.set_result(True)
        elif isinstance(message, RequestErrorMessage):
            self._failure(request, RequestFailed(message.error_code, message.reason, message.retry_interval or 0))
        elif isinstance(message, PublishDoneMessage):
            self._failure(request, RequestFailed(message.status_code, message.reason))

    def _receive_objects(self, event):
        if self.role != "subscriber" or not self.bound or event.stream_id % 4 != 3:
            raise ValueError("unexpected data Stream")
        reader = self.streams.setdefault(event.stream_id, ObjectReader(self.limits.object_bytes))
        objects = reader.feed(event.data, event.end_stream)
        if reader.header is not None:
            request = next((r for r in self.requests.values() if r.alias == reader.header.track_alias), None)
            if request is None:
                self.protocol._quic.stop_stream(event.stream_id, 0)
                self.protocol.transmit()
                self.streams.pop(event.stream_id, None)
                return
            if request.stream_id not in (None, event.stream_id):
                raise ValueError("multiple Streams for one ACN subscription")
            request.stream_id = event.stream_id
            for _, payload in objects:
                self._callback(self.on_object, request, strict_json(payload, self.limits.object_bytes))
            if reader.ended:
                self._failure(request, RequestFailed(0, "Track Stream ended"))
                self.streams.pop(event.stream_id, None)
        timer = self.timers.pop(event.stream_id, None) if objects or not reader.buffer else None
        if timer:
            timer.cancel()
        if reader.buffer and event.stream_id not in self.timers:
            self.timers[event.stream_id] = asyncio.get_running_loop().call_later(self.limits.local_timeout,
                self._assembly_timeout, event.stream_id)

    def _assembly_timeout(self, stream_id):
        for request in list(self.requests.values()):
            if request.stream_id == stream_id:
                self._failure(request, TimeoutError("Object assembly timed out"))
        self.protocol._quic.stop_stream(stream_id, 2)
        self.protocol.transmit()
        self.streams.pop(stream_id, None)
        self.timers.pop(stream_id, None)

    def disconnected(self, protocol, code=0):
        if protocol is not self.protocol:
            return
        self.bound = False
        if self.binding_result is not None and not self.binding_result.done():
            self.binding_result.set_exception(ConnectionError("connection closed during binding"))
        for request in list(self.requests.values()):
            self._failure(request, ConnectionError("MOQT role disconnected"))
        if not self.closed:
            self._callback(self.on_close, code)

    async def close(self):
        self.closed = True
        self.bound = False
        for timer in self.timers.values():
            timer.cancel()
        self.timers.clear()
        if self.protocol:
            self.protocol.retired = True
            self.protocol.close()
            self.disconnected(self.protocol)
            self.protocol.worker.cancel()
            if self.protocol.worker is not asyncio.current_task():
                await asyncio.gather(self.protocol.worker, return_exceptions=True)
        if self.transport:
            self.transport.close()
