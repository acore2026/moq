#!/usr/bin/env python3
"""
Relay implementation selection for Agent GW.
"""

import os
from typing import Any, List, Tuple


PYTHON_RELAY_IMPL = 'python'
RUST_RELAY_IMPL = 'rust'
BRIDGE_RELAY_IMPL = 'bridge'
SUPPORTED_RELAY_IMPLS = {
    PYTHON_RELAY_IMPL,
    RUST_RELAY_IMPL,
    BRIDGE_RELAY_IMPL,
}


def get_relay_impl() -> str:
    """Return the configured MOQ relay implementation."""
    relay_impl = os.environ.get('MOQ_RELAY_IMPL', PYTHON_RELAY_IMPL).strip().lower()
    if relay_impl not in SUPPORTED_RELAY_IMPLS:
        supported = ', '.join(sorted(SUPPORTED_RELAY_IMPLS))
        raise ValueError(f"Unsupported MOQ_RELAY_IMPL={relay_impl!r}; expected one of: {supported}")
    return relay_impl


def get_bridge_rust_port(public_port: int) -> int:
    """Return the internal Rust relay port used by bridge mode."""
    return int(os.environ.get('MOQ_RUST_RELAY_PORT', str(public_port + 10000)))


def get_relay_port_checks(host: str, port: int, relay_impl: str) -> List[Tuple[str, str, int, str]]:
    """Return startup port checks as tuples of name, host, port, and socket kind."""
    if relay_impl == PYTHON_RELAY_IMPL:
        return [('MOQT Relay server', host, port, 'udp')]
    if relay_impl == RUST_RELAY_IMPL:
        return [
            ('Rust MOQT Relay QUIC server', host, port, 'udp'),
            ('Rust MOQT Relay HTTP server', host, port, 'tcp'),
        ]

    rust_port = get_bridge_rust_port(port)
    return [
        ('Bridge Python MOQT Relay server', host, port, 'udp'),
        ('Bridge Rust MOQT Relay QUIC server', host, rust_port, 'udp'),
        ('Bridge Rust MOQT Relay HTTP server', host, rust_port, 'tcp'),
    ]


def create_moq_relay(
    *,
    relay_impl: str,
    host: str,
    port: int,
    cache_dir: str,
    max_memory_cache: int,
    max_disk_cache: int,
) -> Any:
    """Create the configured relay implementation with a common lifecycle API."""
    if relay_impl == PYTHON_RELAY_IMPL:
        from moq import MOQRelay

        return MOQRelay(
            host=host,
            port=port,
            cache_dir=cache_dir,
            max_memory_cache=max_memory_cache,
            max_disk_cache=max_disk_cache,
    )

    if relay_impl == RUST_RELAY_IMPL:
        from .moq_official_relay import RustMOQRelay

        return RustMOQRelay(
            host=host,
            port=port,
            cache_dir=cache_dir,
            max_memory_cache=max_memory_cache,
            max_disk_cache=max_disk_cache,
        )

    if relay_impl == BRIDGE_RELAY_IMPL:
        from .moq_official_relay import BridgeMOQRelay

        return BridgeMOQRelay(
            host=host,
            port=port,
            rust_port=get_bridge_rust_port(port),
            cache_dir=cache_dir,
            max_memory_cache=max_memory_cache,
            max_disk_cache=max_disk_cache,
        )

    supported = ', '.join(sorted(SUPPORTED_RELAY_IMPLS))
    raise ValueError(f"Unsupported relay implementation {relay_impl!r}; expected one of: {supported}")
