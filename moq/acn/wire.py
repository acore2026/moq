"""Bounded incremental parsing for ACN sessions on the existing MOQ encoding."""

from moq.encoding import VarInt, KeyValuePair
from moq.messages import MessageType, decode_control_message
from moq.messages.data import SubgroupHeader

from .contracts import ACN_AGENT_BINDING, ACN_AGENT_BINDING_RESULT, encode_json, strict_json


def json_object(value, maximum):
    data = encode_json(value)
    strict_json(data, maximum)
    # Existing SubgroupObject reserves lengths 1..4 for ObjectStatus.
    # JSON whitespace is semantically inert and preserves the existing codec.
    return data + b" " * max(0, 5 - len(data))


class ControlReader:
    def __init__(self, maximum=131072):
        self.buffer = bytearray()
        self.maximum = maximum

    def feed(self, data):
        self.buffer.extend(data)
        result = []
        while self.buffer:
            try:
                kind, offset = VarInt.decode(self.buffer)
                size, used = VarInt.decode(self.buffer, offset)
            except ValueError:
                break
            offset += used
            if size > self.maximum:
                raise ValueError("control message size exceeded")
            if offset + size > len(self.buffer):
                break
            raw = bytes(self.buffer[:offset + size])
            payload = raw[offset:]
            if kind == MessageType.SETUP:
                self._setup(payload)
            msg, consumed = decode_control_message(raw)
            if consumed != len(raw):
                raise ValueError("control message length mismatch")
            result.append(msg)
            del self.buffer[:len(raw)]
        if len(self.buffer) > self.maximum + 20:
            raise ValueError("control receive buffer exceeded")
        return result

    @staticmethod
    def _setup(payload):
        _, offset = VarInt.decode(payload)
        _, used = VarInt.decode(payload, offset)
        offset += used
        count, used = VarInt.decode(payload, offset)
        offset += used
        previous, seen = 0, set()
        for _ in range(count):
            kv, used = KeyValuePair.decode(payload, offset, previous)
            offset += used
            if offset > len(payload):
                raise ValueError("truncated SETUP parameter")
            if kv.key_type in (ACN_AGENT_BINDING, ACN_AGENT_BINDING_RESULT):
                if kv.key_type in seen:
                    raise ValueError("duplicate ACN SETUP parameter")
                seen.add(kv.key_type)
            previous = kv.key_type
        if offset != len(payload):
            raise ValueError("SETUP parameter length mismatch")


class ObjectReader:
    """Single ordered Subgroup Stream; emits only complete bounded Objects."""

    def __init__(self, maximum=65536):
        self.buffer = bytearray()
        self.maximum = maximum
        self.header = None
        self.ended = False
        self.last_object = -1

    def feed(self, data, end=False):
        if self.ended and data:
            raise ValueError("data after subgroup end")
        self.buffer.extend(data)
        if self.header is None:
            try:
                kind, offset = VarInt.decode(self.buffer)
                if kind != 1:
                    raise ValueError("unsupported data Stream type")
                header, used = SubgroupHeader.decode(self.buffer, offset)
            except (ValueError, IndexError):
                if end or len(self.buffer) > 40:
                    raise ValueError("invalid Subgroup header")
                return []
            self.header = header
            del self.buffer[:offset + used]
        objects = []
        while self.buffer:
            try:
                oid, offset = VarInt.decode(self.buffer)
                size, used = VarInt.decode(self.buffer, offset)
            except ValueError:
                break
            offset += used
            if size in (1, 2, 3, 4):
                del self.buffer[:offset]
                if size in (3, 4):
                    self.ended = True
                if self.buffer:
                    raise ValueError("unexpected data after Object status")
                break
            if size > self.maximum:
                raise ValueError("Object payload size exceeded")
            if offset + size > len(self.buffer):
                break
            if oid <= self.last_object:
                raise ValueError("Object ID must increase within an ACN Subgroup Stream")
            payload = bytes(self.buffer[offset:offset + size])
            strict_json(payload, self.maximum)
            objects.append((oid, payload))
            self.last_object = oid
            del self.buffer[:offset + size]
        if len(self.buffer) > self.maximum + 20:
            raise ValueError("Object receive buffer exceeded")
        if end and self.buffer:
            raise ValueError("truncated Object")
        if end:
            self.ended = True
        return objects
