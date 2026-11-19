"""Opt-in ACN Relay, isolated from ordinary MOQ routing and cache state."""

import asyncio
import logging
import time
from dataclasses import dataclass, field

from aioquic.quic.events import DatagramFrameReceived, StreamDataReceived, StreamReset, StopSendingReceived

from moq.encoding import Parameters, VarInt
from moq.messages import (ErrorCode, GroupOrder, SetupMessage, PublishMessage,
                          PublishOkMessage, SubscribeMessage, SubscribeOkMessage,
                          PublishDoneMessage, RequestErrorMessage, FetchMessage)
from moq.messages.data import SubgroupHeader, SubgroupObject

from .contracts import (ACN_AGENT_BINDING, ACN_AGENT_BINDING_RESULT, MOQ_VERSION,
                        SESSION_REPLACED, CATALOG, Limits, binding, catalog,
                        encode_json, identifier_key, parse_track, strict_json)
from .policy import Policy, PolicyError
from .transport import AcnServer
from .wire import ControlReader, ObjectReader

logger = logging.getLogger(__name__)


@dataclass(eq=False)
class Client:
    protocol: object
    reader: ControlReader = field(default_factory=ControlReader)
    agent_id: str = ""
    role: str = ""
    bound: bool = False
    setup: bool = False
    aliases: set = field(default_factory=set)
    requests: dict = field(default_factory=dict)
    streams: dict = field(default_factory=dict)
    timers: dict = field(default_factory=dict)
    revisions: dict = field(default_factory=dict)


@dataclass(eq=False)
class Published:
    client: Client
    request_id: int
    alias: int
    track: object
    latest: object = None
    tokens: float = 4.0
    refill: float = field(default_factory=time.monotonic)
    stream_id: object = None


@dataclass(eq=False)
class Subscribed:
    client: Client
    request_id: int
    alias: int
    track: object
    stream_id: int
    queue: object
    bytes_pending: int = 0
    objects_pending: int = 0
    worker: object = None
    active: bool = True


class AcnRelay:
    def __init__(self, host="0.0.0.0", port=4433, limits=Limits(), cert_file=None, key_file=None):
        self.limits = limits
        self.policy = Policy()
        self.clients = {}
        self.bindings = {}
        self.publications = {}
        self.subscriptions = {}
        self.server = AcnServer(self, host, port, cert_file, key_file)

    async def start(self):
        await self.server.start()

    async def stop(self):
        self.reset()
        await self.server.stop()

    def reset(self):
        for client in list(self.clients.values()):
            self._close(client, 0, "ACN management initialization")
        self.bindings.clear()
        self.publications.clear()
        self.subscriptions.clear()

    def open_management(self, session):
        # No await between fencing authorization and clearing communication.
        self.policy.open(session)
        self.reset()
        logger.info("[ACN-L5][MANAGE] event=state_cleared")

    def apply(self, session, group_id, version, phase, members):
        if len(members) > 64:
            raise PolicyError("RESOURCE_EXHAUSTED", "group member limit exceeded")
        changed = not self.policy.apply(session, group_id, version, phase, members)
        if changed:
            self._reconcile()
        return not changed

    def delete(self, session, group_id, version):
        absent = self.policy.delete(session, group_id, version)
        self._reconcile()
        return absent

    def _reconcile(self):
        for pub in list(self.publications.values()):
            try:
                self._authorize(pub.client, pub.track, "publish")
            except PolicyError:
                self._unpublish(pub, int(ErrorCode.UNAUTHORIZED), "group authorization changed")
        for subscribers in list(self.subscriptions.values()):
            for sub in list(subscribers):
                try:
                    self._authorize(sub.client, sub.track, "subscribe")
                except PolicyError:
                    self._unsubscribe(sub, int(ErrorCode.UNAUTHORIZED), "group authorization changed")
        for client in list(self.clients.values()):
            for scope in list(client.revisions):
                group = self.policy.groups.get(scope[3].decode("ascii"))
                if group is None or group.version != int(scope[5]) or client.agent_id not in group.members:
                    del client.revisions[scope]
            if client.bound and not self.policy.contains_agent(client.agent_id):
                self._close(client, int(ErrorCode.UNAUTHORIZED), "no remaining group membership")

    def _authorize(self, client, track, operation):
        if not client.bound or self.bindings.get((client.agent_id, client.role)) is not client:
            raise PolicyError("UNAUTHORIZED", "Agent role is not bound")
        return self.policy.authorize(client.agent_id, client.role, track, operation, self.limits)

    def _close(self, client, code, reason):
        client.protocol.retired = True
        client.protocol.close(error_code=code, reason_phrase=reason)
        self.disconnected(client.protocol, code)

    def disconnected(self, protocol, code=0):
        client = self.clients.pop(protocol, None)
        if not client:
            return
        key = (client.agent_id, client.role)
        if self.bindings.get(key) is client:
            del self.bindings[key]
        client.bound = False
        for timer in client.timers.values():
            timer.cancel()
        for request in list(client.requests.values()):
            if isinstance(request, Published):
                self._unpublish(request, 0, "publisher ended")
            else:
                self._unsubscribe(request, 0, "subscriber ended", notify=False)
        logger.info("[ACN-L5][SESSION] agent_id=%s role=%s event=closed code=%d", client.agent_id, client.role, code)

    async def event(self, protocol, event):
        if getattr(protocol, "retired", False):
            return
        client = self.clients.setdefault(protocol, Client(protocol))
        if isinstance(event, StreamDataReceived):
            if event.stream_id == 0:
                for message in client.reader.feed(event.data):
                    await self._control(client, message)
                if event.end_stream:
                    self._close(client, 2, "control Stream ended")
            elif event.stream_id % 4 == 2:
                self._objects(client, event)
            else:
                self._close(client, 2, "unexpected Stream direction")
        elif isinstance(event, DatagramFrameReceived):
            self._datagram(client, event.data)
        elif isinstance(event, StreamReset):
            self._stream_failed(client, event.stream_id, "upstream Stream reset")
        elif isinstance(event, StopSendingReceived):
            for request in list(client.requests.values()):
                if isinstance(request, Subscribed) and request.stream_id == event.stream_id:
                    self._unsubscribe(request, 0, "subscriber cancelled", notify=False)

    def _send_control(self, client, message):
        client.protocol._quic.send_stream_data(0, message.encode())
        client.protocol.transmit()

    async def _control(self, client, message):
        if isinstance(message, SetupMessage):
            if client.setup or message.version != MOQ_VERSION:
                self._close(client, 2, "invalid or repeated SETUP")
                return
            client.setup = True
            result = {"binding_version": 1, "status": "REJECTED"}
            try:
                raw = message.parameters.get(ACN_AGENT_BINDING)
                if raw is None:
                    raise ValueError("BINDING_REQUIRED")
                parsed = strict_json(raw, self.limits.binding_bytes)
                for field in ("agent_id", "role"):
                    if isinstance(parsed.get(field), str) and parsed[field]:
                        result[field] = parsed[field]
                agent, role = binding(raw, message.role, self.limits)
                result.update(agent_id=agent, role=role)
                if not self.policy.contains_agent(agent):
                    raise ValueError("AGENT_NOT_REGISTERED")
                old = self.bindings.get((agent, role))
                if old is not None:
                    self._close(old, SESSION_REPLACED, "same Agent role replaced")
                client.agent_id, client.role, client.bound = agent, role, True
                self.bindings[(agent, role)] = client
                result["status"] = "ACCEPTED"
                logger.info("[ACN-L5][BIND] agent_id=%s role=%s result=ACCEPTED", agent, role)
            except (ValueError, TypeError, UnicodeError) as exc:
                code = str(exc)
                result["code"] = code if code in ("AGENT_NOT_REGISTERED", "BINDING_REQUIRED", "BINDING_VERSION_UNSUPPORTED") else "BINDING_INVALID"
                logger.warning("[ACN-L5][BIND] result=REJECTED reason=%s", exc)
            parameters = Parameters()
            parameters.set(ACN_AGENT_BINDING_RESULT, encode_json(result))
            self._send_control(client, SetupMessage(MOQ_VERSION, 3, parameters))
            if not client.bound:
                asyncio.create_task(self._finish_rejection(client))
            return
        if not client.bound:
            self._close(client, 1, "ACN binding required")
            return
        if isinstance(message, (PublishMessage, SubscribeMessage, FetchMessage)):
            try:
                if isinstance(message, FetchMessage):
                    self._authorize(client, message.full_track_name, "subscribe")
                    raise PolicyError("UNAUTHORIZED", "ACN history reads not enabled")
                self._request(client, message)
            except PolicyError as exc:
                self._send_control(client, RequestErrorMessage(message.request_id,
                    int(getattr(ErrorCode, exc.code, ErrorCode.INTERNAL_ERROR)), str(exc), exc.retry_interval))
        elif isinstance(message, PublishDoneMessage):
            request = client.requests.get(message.request_id)
            if isinstance(request, Published):
                self._unpublish(request, 0, "publisher ended")
        else:
            self._close(client, 2, "unsupported ACN control message")

    async def _finish_rejection(self, client):
        try:
            await client.protocol.write(0, b"", self.limits.local_timeout)
        except (ConnectionError, TimeoutError):
            pass
        finally:
            if self.clients.get(client.protocol) is client:
                self._close(client, 1, "ACN binding rejected")

    def _request(self, client, message):
        track = message.full_track_name
        publishing = isinstance(message, PublishMessage)
        self._authorize(client, track, "publish" if publishing else "subscribe")
        previous = client.requests.get(message.request_id)
        if previous:
            if previous.track != track or previous.alias != message.track_alias or isinstance(previous, Published) != publishing:
                raise PolicyError("UNAUTHORIZED", "conflicting request ID")
            self._send_control(client, PublishOkMessage(message.request_id) if publishing else SubscribeOkMessage(message.request_id, 0, GroupOrder.ASCENDING))
            return
        if message.track_alias in client.aliases:
            raise PolicyError("UNAUTHORIZED", "Track alias cannot be reused")
        if publishing:
            if track in self.publications:
                raise PolicyError("UNAUTHORIZED", "Track already registered under another request")
            namespace_tracks = [p for p in self.publications.values() if p.track.namespace == track.namespace and p.track.track_name != CATALOG]
            if track.track_name != CATALOG and len(namespace_tracks) >= self.limits.business_tracks:
                raise PolicyError("INTERNAL_ERROR", "business Track limit exceeded")
            request = Published(client, message.request_id, message.track_alias, track, tokens=float(self.limits.directory_burst))
            self.publications[track] = request
            self._send_control(client, PublishOkMessage(message.request_id))
        else:
            if track not in self.publications:
                raise PolicyError("INTERNAL_ERROR", "Track not yet published", 501)
            if any(sub.client is client for sub in self.subscriptions.get(track, [])):
                raise PolicyError("UNAUTHORIZED", "Track already subscribed under another request")
            stream = client.protocol._quic.get_next_available_stream_id(is_unidirectional=True)
            request = Subscribed(client, message.request_id, message.track_alias, track, stream, asyncio.Queue())
            self.subscriptions.setdefault(track, []).append(request)
            self._send_control(client, SubscribeOkMessage(message.request_id, 0, GroupOrder.ASCENDING))
            header = VarInt.encode(1) + SubgroupHeader(request.alias, 0, 0, 128).encode()
            client.protocol._quic.send_stream_data(stream, header)
            client.protocol.transmit()
            request.worker = asyncio.create_task(self._deliver(request))
            cached = self.publications[track].latest
            if track.track_name == CATALOG and cached is not None:
                self._enqueue(request, *cached)
        client.requests[message.request_id] = request
        client.aliases.add(message.track_alias)
        logger.info("[ACN-L5][REQUEST] agent_id=%s role=%s operation=%s request_id=%s track=%s", client.agent_id, client.role, "publish" if publishing else "subscribe", message.request_id, track.to_string())

    def _objects(self, client, event):
        reader = client.streams.get(event.stream_id)
        if reader is None:
            if len(client.streams) >= (self.limits.business_tracks + 1) * 64:
                self._close(client, 2, "data Stream limit exceeded")
                return
            reader = client.streams[event.stream_id] = ObjectReader(self.limits.object_bytes)
        try:
            objects = reader.feed(event.data, event.end_stream)
            for oid, payload in objects:
                pub = self._publication(client, reader.header.track_alias)
                if pub.stream_id not in (None, event.stream_id):
                    raise ValueError("one ordered Subgroup Stream required")
                pub.stream_id = event.stream_id
                self._object(pub, oid, payload)
            timer = client.timers.pop(event.stream_id, None) if not reader.buffer else None
            if timer:
                timer.cancel()
            if reader.buffer and event.stream_id not in client.timers:
                client.timers[event.stream_id] = asyncio.get_running_loop().call_later(
                    self.limits.local_timeout, self._stream_failed, client, event.stream_id, "Object assembly timed out")
            if not reader.buffer and objects:
                timer = client.timers.pop(event.stream_id, None)
                if timer:
                    timer.cancel()
            if reader.ended:
                self._stream_failed(client, event.stream_id, "publication Stream ended", code=0)
        except (ValueError, PolicyError) as exc:
            self._stream_failed(client, event.stream_id, str(exc))

    def _publication(self, client, alias):
        for request in client.requests.values():
            if isinstance(request, Published) and request.alias == alias and self.publications.get(request.track) is request:
                return request
        raise ValueError("unknown publication alias")

    def _object(self, pub, oid, payload):
        group, publisher = self._authorize(pub.client, pub.track, "publish")
        if pub.track.track_name == CATALOG:
            value = catalog(payload, group.group_id, group.version, publisher, self.limits)
            scope = tuple(pub.track.namespace)
            previous = pub.client.revisions.get(scope)
            import json
            semantic = json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(",", ":")).encode()
            if previous:
                revision, content = previous
                if value["directory_revision"] < revision or (value["directory_revision"] == revision and content != semantic):
                    raise ValueError("catalog revision conflict")
                if value["directory_revision"] == revision:
                    return
            now = time.monotonic()
            pub.tokens = min(self.limits.directory_burst, pub.tokens + (now - pub.refill) * self.limits.directory_rate)
            pub.refill = now
            if pub.tokens < 1:
                raise ValueError("catalog rate exceeded")
            pub.tokens -= 1
            pub.client.revisions[scope] = value["directory_revision"], semantic
            pub.latest = oid, payload
        else:
            strict_json(payload, self.limits.object_bytes)
        for sub in list(self.subscriptions.get(pub.track, [])):
            self._enqueue(sub, oid, payload)

    def _enqueue(self, sub, oid, payload):
        try:
            self._authorize(sub.client, sub.track, "subscribe")
        except PolicyError:
            self._unsubscribe(sub, 1, "authorization revoked")
            return
        if sub.objects_pending + 1 > self.limits.queue_objects or sub.bytes_pending + len(payload) > self.limits.queue_bytes:
            self._unsubscribe(sub, 0, "subscription backlog exceeded")
            return
        sub.bytes_pending += len(payload)
        sub.objects_pending += 1
        sub.queue.put_nowait((oid, payload, time.monotonic()))

    async def _deliver(self, sub):
        try:
            while sub.active:
                oid, payload, queued = await sub.queue.get()
                self._authorize(sub.client, sub.track, "subscribe")
                remaining = self.limits.local_timeout - (time.monotonic() - queued)
                if remaining <= 0:
                    raise TimeoutError("subscription delivery timed out")
                await sub.client.protocol.write(sub.stream_id, SubgroupObject(oid, payload).encode(), remaining)
                sub.bytes_pending -= len(payload)
                sub.objects_pending -= 1
        except asyncio.CancelledError:
            pass
        except Exception as exc:
            self._unsubscribe(sub, 0, str(exc))

    def _unsubscribe(self, sub, code, reason, notify=True):
        if not sub.active:
            return
        sub.active = False
        if sub.worker and sub.worker is not asyncio.current_task():
            sub.worker.cancel()
        subscribers = self.subscriptions.get(sub.track, [])
        if sub in subscribers:
            subscribers.remove(sub)
        if not subscribers:
            self.subscriptions.pop(sub.track, None)
        if sub.client.requests.get(sub.request_id) is sub:
            del sub.client.requests[sub.request_id]
        sub.client.protocol._quic.reset_stream(sub.stream_id, int(code))
        if notify and sub.client.bound:
            self._send_control(sub.client, PublishDoneMessage(sub.request_id, code, reason[:256]))
        sub.queue = asyncio.Queue()
        sub.bytes_pending = sub.objects_pending = 0

    def _unpublish(self, pub, code, reason):
        if self.publications.get(pub.track) is not pub:
            return
        del self.publications[pub.track]
        pub.client.requests.pop(pub.request_id, None)
        pub.latest = None
        if pub.stream_id is not None:
            pub.client.protocol._quic.stop_stream(pub.stream_id, int(code))
            pub.client.protocol.transmit()
        for sub in list(self.subscriptions.get(pub.track, [])):
            self._unsubscribe(sub, code, reason)
        logger.info("[ACN-L5][PUBLICATION] event=ended agent_id=%s track=%s reason=%s", pub.client.agent_id, pub.track.to_string(), reason)

    def _stream_failed(self, client, stream_id, reason, code=2):
        reader = client.streams.pop(stream_id, None)
        timer = client.timers.pop(stream_id, None)
        if timer:
            timer.cancel()
        if reader and reader.header:
            try:
                self._unpublish(self._publication(client, reader.header.track_alias), code, reason)
            except ValueError:
                pass
        try:
            client.protocol._quic.stop_stream(stream_id, int(code))
            client.protocol.transmit()
        except ValueError:
            pass

    def _datagram(self, client, data):
        # Header's normal status is implicit in the retained wire format.
        try:
            offset, values = 0, []
            for _ in range(3):
                value, used = VarInt.decode(data, offset)
                values.append(value)
                offset += used
            offset += 1  # Publisher priority.
            size, used = VarInt.decode(data, offset)
            offset += used
            if size > self.limits.object_bytes or offset + size != len(data):
                raise ValueError("invalid Datagram Object size")
            pub = self._publication(client, values[0])
            if pub.track.track_name == CATALOG:
                raise ValueError("catalog requires reliable Stream")
            strict_json(data[offset:], self.limits.object_bytes)
            self._object(pub, values[2], data[offset:])
        except (ValueError, PolicyError):
            logger.warning("[ACN-L5][DATAGRAM] event=rejected agent_id=%s", client.agent_id)
