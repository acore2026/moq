"""Volatile, versioned ACL authority projection; no Agent credential database."""

from dataclasses import dataclass

from .contracts import identifier_key, parse_track, positive_uint64


class PolicyError(Exception):
    def __init__(self, code, message, retry_interval=0):
        super().__init__(message)
        self.code = code
        self.retry_interval = retry_interval


@dataclass(frozen=True)
class Group:
    group_id: str
    version: int
    phase: str
    members: frozenset


class Policy:
    """Mutated only on the Relay event loop, including management stream opens."""

    def __init__(self):
        self.groups = {}
        self.deleted = {}
        self.management = None

    def open(self, session):
        self.management = session
        self.groups.clear()
        self.deleted.clear()

    def require_management(self, session):
        if self.management is None or session is not self.management:
            raise PolicyError("INVALID_ARGUMENT", "management stream has been superseded")

    def apply(self, session, group_id, version, phase, members):
        self.require_management(session)
        try:
            key = identifier_key(group_id)
            positive_uint64(version)
            if (phase not in ("PREPARED", "ACTIVE") or not members
                    or any(not isinstance(x, str) or not x for x in members)
                    or len(set(members)) != len(members)):
                raise ValueError("invalid phase or members")
        except (ValueError, TypeError) as exc:
            raise PolicyError("INVALID_ARGUMENT", str(exc)) from exc
        current = self.groups.get(key)
        if version <= self.deleted.get(key, 0):
            raise PolicyError("GROUP_DELETED", "version covered by deletion")
        proposed = Group(group_id, version, phase, frozenset(members))
        if current:
            if version < current.version:
                raise PolicyError("STALE_VERSION", "older than current group")
            if version == current.version:
                if proposed.members != current.members:
                    raise PolicyError("CONTENT_CONFLICT", "same version has different members")
                if phase == current.phase:
                    return True
                if current.phase == "ACTIVE":
                    raise PolicyError("PHASE_REGRESSION", "ACTIVE cannot become PREPARED")
            elif phase != "PREPARED":
                raise PolicyError("PREPARED_REQUIRED", "new version requires PREPARED")
        elif phase != "PREPARED":
            raise PolicyError("PREPARED_REQUIRED", "initial snapshot requires PREPARED")
        self.groups[key] = proposed
        return False

    def delete(self, session, group_id, version):
        self.require_management(session)
        try:
            key = identifier_key(group_id)
            positive_uint64(version)
        except ValueError as exc:
            raise PolicyError("INVALID_ARGUMENT", str(exc)) from exc
        current = self.groups.get(key)
        if (current and current.version > version) or self.deleted.get(key, 0) > version:
            raise PolicyError("STALE_VERSION", "delete would precede current version")
        absent = current is None
        if current and current.version <= version:
            del self.groups[key]
        self.deleted[key] = max(version, self.deleted.get(key, 0))
        return absent

    def contains_agent(self, agent_id):
        return any(agent_id in group.members for group in self.groups.values())

    def authorize(self, agent_id, role, track, operation, limits):
        if role != {"publish": "publisher", "subscribe": "subscriber"}.get(operation):
            raise PolicyError("UNAUTHORIZED", "role does not permit operation")
        try:
            key, version, publisher_key, _ = parse_track(track, limits)
        except (ValueError, UnicodeError, TypeError) as exc:
            raise PolicyError("UNAUTHORIZED", "invalid ACN namespace") from exc
        current = self.groups.get(key)
        if version <= self.deleted.get(key, 0) or (current and version < current.version):
            raise PolicyError("UNAUTHORIZED", "group version has been revoked")
        if current is None or version > current.version:
            raise PolicyError("INTERNAL_ERROR", "authorization not synchronized", 501)
        publishers = {identifier_key(member): member for member in current.members}
        if (agent_id not in current.members or publisher_key not in publishers
                or (operation == "publish" and publisher_key != identifier_key(agent_id))
                or (operation == "subscribe" and publisher_key == identifier_key(agent_id))):
            raise PolicyError("UNAUTHORIZED", "not permitted in current member set")
        if current.phase != "ACTIVE":
            raise PolicyError("UNAUTHORIZED", "group is PREPARED", 501)
        return current, publishers[publisher_key]
