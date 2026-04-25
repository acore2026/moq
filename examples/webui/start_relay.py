#!/usr/bin/env python3
"""Start only the WebUI-local MOQ relay."""

from __future__ import annotations

import argparse
import asyncio
import os
import signal
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

try:
    from examples._bootstrap import ensure_repo_root, setup_logging
except ImportError:  # pragma: no cover - direct script execution fallback
    from _bootstrap import ensure_repo_root, setup_logging

ensure_repo_root()
setup_logging()

from moq import MOQRelay

RELAY_HOST = os.environ.get("MOQ_RELAY_HOST", "127.0.0.1")
RELAY_PORT = int(os.environ.get("MOQ_RELAY_PORT", "28446"))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Start the WebUI-local MOQ relay")
    parser.add_argument("--host", default=RELAY_HOST, help="Relay bind host")
    parser.add_argument("--port", type=int, default=RELAY_PORT, help="Relay QUIC port")
    parser.add_argument("--cache-dir", default="/tmp/moq_relay_cache", help="Relay cache directory")
    return parser.parse_args()


async def main_async() -> None:
    args = parse_args()
    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()
    for signame in ("SIGINT", "SIGTERM"):
        try:
            loop.add_signal_handler(getattr(signal, signame), stop_event.set)
        except NotImplementedError:
            pass

    relay = MOQRelay(
        host=args.host,
        port=args.port,
        cache_dir=args.cache_dir,
        max_memory_cache=100 * 1024 * 1024,
        max_disk_cache=1024 * 1024 * 1024,
    )
    try:
        await relay.start()
        print(f"relay started: {args.host}:{args.port}", flush=True)
        print("press Ctrl+C to stop relay", flush=True)
        await stop_event.wait()
    finally:
        await relay.stop()
        print("relay stopped", flush=True)


def main() -> None:
    asyncio.run(main_async())


if __name__ == "__main__":
    main()
