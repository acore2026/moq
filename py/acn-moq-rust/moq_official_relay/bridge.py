#!/usr/bin/env python3
"""
Bridge-mode relay lifecycle.
"""

import logging
import os
from pathlib import Path
from typing import Dict, Optional

from .relay import RustMOQRelay

logger = logging.getLogger(__name__)


class BridgeMOQRelay:
    """
    Run the legacy Python relay plus an official Rust relay sidecar.

    The public port remains compatible with existing Python pub/sub clients. The
    sidecar gives the process a managed official Rust relay endpoint while the
    protocol translation layer is developed and validated.
    """

    def __init__(
        self,
        host: str,
        port: int,
        rust_port: int,
        cache_dir: Optional[str] = None,
        max_memory_cache: int = 100 * 1024 * 1024,
        max_disk_cache: int = 1024 * 1024 * 1024,
    ):
        from moq import MOQRelay

        self.host = host
        self.port = port
        self.rust_port = rust_port
        self.cache_dir = Path(cache_dir or '.relay_cache')
        self.python_relay = MOQRelay(
            host=host,
            port=port,
            cache_dir=str(self.cache_dir / 'python'),
            max_memory_cache=max_memory_cache,
            max_disk_cache=max_disk_cache,
        )
        self.rust_relay = RustMOQRelay(
            host=host,
            port=rust_port,
            cache_dir=str(self.cache_dir / 'rust'),
            max_memory_cache=max_memory_cache,
            max_disk_cache=max_disk_cache,
        )
        self._running = False

    async def start(self):
        """Start public legacy relay and optional official Rust sidecar."""
        if self._running:
            return

        await self.python_relay.start()

        if self._should_start_rust_sidecar():
            try:
                await self.rust_relay.start()
            except Exception:
                await self.python_relay.stop()
                raise
        else:
            logger.info(
                "Bridge Rust sidecar disabled; set MOQ_BRIDGE_START_RUST=true "
                "after configuring MOQ_OFFICIAL_RELAY_BIN or MOQ_OFFICIAL_RELAY_SOURCE"
            )

        self._running = True

    async def stop(self):
        """Stop both bridge relay processes."""
        self._running = False
        await self.rust_relay.stop()
        await self.python_relay.stop()

    def get_cache_stats(self) -> Dict[str, int]:
        """Return public legacy relay cache stats."""
        return self.python_relay.get_cache_stats()

    def _should_start_rust_sidecar(self) -> bool:
        configured = (
            os.environ.get('MOQ_OFFICIAL_RELAY_BIN')
            or os.environ.get('MOQ_OFFICIAL_RELAY_SOURCE')
        )
        value = os.environ.get('MOQ_BRIDGE_START_RUST')
        if value is None:
            return bool(configured)
        value = value.strip().lower()
        return value in ('1', 'true', 'yes', 'on')
