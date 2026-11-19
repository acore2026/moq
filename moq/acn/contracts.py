"""ACN-specific names, limits and validation, independent of ordinary MOQ policy."""

import base64
import hashlib
import json
import re
import math
from dataclasses import dataclass

from moq.encoding import FullTrackName

ACN_AGENT_BINDING = 0x3F01
ACN_AGENT_BINDING_RESULT = 0x3F03
SESSION_REPLACED = 0xAC10
BINDING_VERSION = 1
MOQ_VERSION = 0xFF000011
CATALOG = b"group_catalog"
MAX_UINT64 = (1 << 64) - 1


@dataclass(frozen=True)
class Limits:
    binding_bytes: int = 4096
    track_name_bytes: int = 128
    business_tracks: int = 64
    object_bytes: int = 65536
    queue_objects: int = 8
    queue_bytes: int = 262144
    directory_rate: float = 2.0
    directory_burst: int = 4
    local_timeout: float = 5.0
    recovery_timeout: float = 30.0

    def __post_init__(self):
        for name, value in vars(self).items():
            if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value) or value <= 0:
                raise ValueError("invalid L5 limit: " + name)


def identifier_key(value):
    if not isinstance(value, str) or not value:
        raise ValueError("identifier must be a nonempty string")
    return base64.b32encode(hashlib.sha256(value.encode("utf-8")).digest()).decode("ascii").lower().rstrip("=")


def positive_uint64(value):
    if type(value) is not int or not 0 < value <= MAX_UINT64:
        raise ValueError("expected positive uint64")
    return value


def namespace(group_id, version, agent_id):
    positive_uint64(version)
    return [part.encode("utf-8") for part in (
        "acn", "v1", "group", identifier_key(group_id), "version", str(version),
        "agent", identifier_key(agent_id),
    )]


def full_track(group_id, version, agent_id, name):
    if isinstance(name, str):
        name = name.encode("utf-8")
    return FullTrackName(namespace(group_id, version, agent_id), name)


def parse_track(track, limits=Limits()):
    parts = track.namespace
    if (len(parts) != 8 or parts[0:3] != [b"acn", b"v1", b"group"]
            or parts[4] != b"version" or parts[6] != b"agent"):
        raise ValueError("invalid ACN namespace")
    if any(not re.fullmatch(b"[a-z2-7]{52}", parts[i]) for i in (3, 7)):
        raise ValueError("invalid identifier key")
    if not re.fullmatch(b"[1-9][0-9]{0,19}", parts[5]):
        raise ValueError("invalid namespace version")
    version = positive_uint64(int(parts[5]))
    name = track.track_name.decode("utf-8")
    if not name or len(track.track_name) > limits.track_name_bytes:
        raise ValueError("invalid Track Name length")
    return parts[3].decode("ascii"), version, parts[7].decode("ascii"), name


def strict_json(payload, maximum):
    if not isinstance(payload, bytes) or len(payload) > maximum:
        raise ValueError("JSON size limit exceeded")

    def pairs(items):
        result = {}
        for key, value in items:
            if key in result:
                raise ValueError("duplicate JSON field: " + key)
            result[key] = value
        return result

    def constant(value):
        raise ValueError("invalid JSON number: " + value)

    result = json.loads(payload.decode("utf-8"), object_pairs_hook=pairs, parse_constant=constant)
    if not isinstance(result, dict):
        raise ValueError("expected JSON Object")
    return result


def encode_json(value):
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), allow_nan=False).encode("utf-8")


def binding(payload, numeric_role, limits=Limits()):
    value = strict_json(payload, limits.binding_bytes)
    if type(value.get("binding_version")) is not int or value["binding_version"] != BINDING_VERSION:
        raise ValueError("BINDING_VERSION_UNSUPPORTED")
    expected = {1: "publisher", 2: "subscriber"}.get(numeric_role)
    if expected is None or value.get("role") != expected:
        raise ValueError("BINDING_INVALID")
    identifier_key(value.get("agent_id"))
    return value["agent_id"], expected


def catalog(payload, group_id, version, publisher, limits=Limits()):
    value = strict_json(payload, limits.object_bytes)
    if (value.get("group_id") != group_id or type(value.get("group_version")) is not int
            or value["group_version"] != version or value.get("publisher_agent_id") != publisher):
        raise ValueError("catalog identity/version mismatch")
    positive_uint64(value.get("directory_revision"))
    tracks = value.get("tracks")
    if not isinstance(tracks, list) or len(tracks) > limits.business_tracks:
        raise ValueError("invalid catalog track count")
    names = set()
    for item in tracks:
        if not isinstance(item, dict):
            raise ValueError("invalid catalog entry")
        name, content_type = item.get("name"), item.get("content_type")
        if (not isinstance(name, str) or not name or name == CATALOG.decode()
                or len(name.encode("utf-8")) > limits.track_name_bytes or name in names
                or not isinstance(content_type, str) or not content_type):
            raise ValueError("invalid or duplicate catalog Track")
        names.add(name)
    return value
