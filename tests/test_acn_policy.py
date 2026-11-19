"""ACN ACL isolation and version/identity contract tests."""

import pytest

from moq.acn.contracts import Limits, binding, catalog, encode_json, full_track, namespace
from moq.acn.policy import Policy, PolicyError


def expect(code, action):
    with pytest.raises(PolicyError) as error:
        action()
    assert error.value.code == code
    return error.value


def test_prepared_active_and_version_fencing():
    p, stream, limits = Policy(), object(), Limits()
    p.open(stream)
    a = full_track("group-g", 1, "Agent-A", "status")
    p.apply(stream, "group-g", 1, "PREPARED", ["Agent-A", "Agent-B"])
    assert expect("UNAUTHORIZED", lambda: p.authorize("Agent-A", "publisher", a, "publish", limits)).retry_interval == 501
    p.apply(stream, "group-g", 1, "ACTIVE", ["Agent-B", "Agent-A"])
    assert p.apply(stream, "group-g", 1, "ACTIVE", ["Agent-A", "Agent-B"])
    p.authorize("Agent-B", "subscriber", a, "subscribe", limits)
    expect("PHASE_REGRESSION", lambda: p.apply(stream, "group-g", 1, "PREPARED", ["Agent-A", "Agent-B"]))
    expect("CONTENT_CONFLICT", lambda: p.apply(stream, "group-g", 1, "ACTIVE", ["Agent-A"]))
    expect("PREPARED_REQUIRED", lambda: p.apply(stream, "group-g", 2, "ACTIVE", ["Agent-A"]))
    p.apply(stream, "group-g", 2, "PREPARED", ["Agent-A", "Agent-B", "Agent-C"])
    expect("UNAUTHORIZED", lambda: p.authorize("Agent-B", "subscriber", a, "subscribe", limits))
    expect("STALE_VERSION", lambda: p.delete(stream, "group-g", 1))
    assert next(iter(p.groups.values())).version == 2
    p.delete(stream, "group-g", 2)
    expect("GROUP_DELETED", lambda: p.apply(stream, "group-g", 2, "PREPARED", ["Agent-A"]))


def test_management_open_clears_state_and_fences_old_writer():
    p, old, new = Policy(), object(), object()
    p.open(old)
    p.apply(old, "g", 1, "PREPARED", ["A"])
    p.delete(old, "g", 1)
    p.open(new)
    assert not p.groups and not p.deleted
    expect("INVALID_ARGUMENT", lambda: p.apply(old, "g", 2, "PREPARED", ["A"]))
    assert not p.groups


def test_unknown_authorization_is_retryable_but_wrong_role_and_namespace_are_not():
    p, limits = Policy(), Limits()
    track = full_track("unknown", 1, "A", "status")
    assert expect("INTERNAL_ERROR", lambda: p.authorize("A", "publisher", track, "publish", limits)).retry_interval == 501
    assert expect("UNAUTHORIZED", lambda: p.authorize("A", "subscriber", track, "publish", limits)).retry_interval == 0
    track.namespace[5] = b"01"
    expect("UNAUTHORIZED", lambda: p.authorize("A", "publisher", track, "publish", limits))


def test_binding_and_catalog_validate_identity_and_duplicate_json():
    assert binding(encode_json({"binding_version": 1, "agent_id": "A", "role": "publisher"}), 1) == ("A", "publisher")
    for payload in (b'{"binding_version":1,"binding_version":1}', b'[]', b'{"binding_version":true}'):
        with pytest.raises(ValueError):
            binding(payload, 1)
    value = {"group_id": "g", "group_version": 1, "publisher_agent_id": "A", "directory_revision": 1, "tracks": []}
    assert catalog(encode_json(value), "g", 1, "A")["tracks"] == []
    with pytest.raises(ValueError):
        catalog(encode_json(value), "g", 1, "B")
    value["tracks"] = [{"name": "group_catalog", "content_type": "application/json"}]
    with pytest.raises(ValueError):
        catalog(encode_json(value), "g", 1, "A")
    assert len(namespace("g", (1 << 64) - 1, "A")) == 8
