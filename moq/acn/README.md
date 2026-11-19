# Optional ACN L5 extension

`AcnRelay` and `AcnClient` implement the ACN-specific policy and catalog profile
on the existing MOQ wire encoding. `MOQRelay`, `Publisher`, `Subscriber`, their
entry points, TLS profile, cache and ordinary authorization behavior are unchanged.
Importing the base package does not activate ACN policy.

Install the matching source/wheel with the `acn` extra, which pins the tested
`aioquic==1.3.0` transport progress hooks:

```sh
python3 -m pip install -e '.[acn]'
python3 -m pytest -q tests/test_acn_policy.py tests/test_acn_transport.py
```

The modules are split by responsibility:

- `contracts.py`: ACN constants, SHA256/Base32 namespaces, strict JSON and limits.
- `policy.py`: volatile full ACL snapshots, PREPARED/ACTIVE, tombstones and writer fencing.
- `wire.py`: bounded incremental ACN parsing and the retained Subgroup encoding.
- `transport.py`: ACN-only serialized QUIC callbacks and bounded send progress.
- `relay.py`: role binding/replacement, permission checks, catalog cache/replay,
  ordered per-subscription queues, resource limits and revocation.
- `client.py`: one role connection serving multiple groups, explicit binding,
  request results, reliable JSON Objects and failure callbacks.

The EC-owned gRPC contract and AGW entry point are in the sibling Free6GC
repository at `ec/acn/agw/`; the Relay library does not duplicate that source
contract. Normal MOQ instances do not register its management service or share
ACN cache/authorization state. Enable ACN through that explicit entry point,
not by switching an ordinary instance's permissions in response to client input.

The profile retains `moq-00`, SETUP version `0xFF000011`, TLS encryption with
client service-certificate checks disabled, and distinct Publisher/Subscriber
connections. SETUP parameters `0x3F01` and `0x3F03` carry ACN binding JSON;
`0xAC10` closes only the replaced role. This is the repository's existing
encoding plus application extensions, not a claim of MOQT draft conformance.

Only ACN sessions append optional `retry_interval` to REQUEST_ERROR; an ordinary
message constructed without it has the original bytes. ACN JSON Object payloads
shorter than five bytes are whitespace-padded to avoid existing Subgroup status
values 1–4, leaving ordinary encoding unchanged. Directory errors use existing
termination values and Stream cancellation, with no new directory ACK.

Tests open local QUIC sockets and do not require a live ACF, reset a running
core, create PDU sessions, or certify the physical CONNECT-IP/N6 deployment.
