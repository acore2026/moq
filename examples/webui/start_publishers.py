#!/usr/bin/env python3
"""Start WebUI-local synthetic video publishers without starting relay."""

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

from examples.webui.video_publisher import TEST_SOURCES

WEBUI_ROOT = Path(__file__).resolve().parent
RELAY_HOST = os.environ.get("MOQ_RELAY_HOST", "127.0.0.1")
RELAY_PORT = int(os.environ.get("MOQ_RELAY_PORT", "28446"))
DEFAULT_SOURCES = ("testsrc2", "testsrc", "smptebars")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Start WebUI synthetic video publishers")
    parser.add_argument("--relay-host", default=RELAY_HOST, help="Relay host")
    parser.add_argument("--relay-port", type=int, default=RELAY_PORT, help="Relay QUIC port")
    parser.add_argument(
        "--sources",
        default=",".join(DEFAULT_SOURCES),
        help=f"Comma-separated publisher sources, or 'all'. Available: {','.join(TEST_SOURCES)}",
    )
    return parser.parse_args()


def resolve_sources(raw_sources: str) -> list[str]:
    if raw_sources.strip().lower() == "all":
        return list(TEST_SOURCES)
    sources = [source.strip() for source in raw_sources.split(",") if source.strip()]
    unknown = [source for source in sources if source not in TEST_SOURCES]
    if unknown:
        raise ValueError(f"Unknown source(s): {', '.join(unknown)}")
    return sources or list(DEFAULT_SOURCES)


async def start_publisher(source: str, host: str, port: int) -> asyncio.subprocess.Process:
    env = {
        **os.environ,
        "MOQ_RELAY_HOST": host,
        "MOQ_RELAY_PORT": str(port),
        "MOQ_TEST_SOURCE": source,
    }
    process = await asyncio.create_subprocess_exec(
        sys.executable,
        str(WEBUI_ROOT / "video_publisher.py"),
        cwd=str(PROJECT_ROOT),
        env=env,
    )
    track = f"video/{TEST_SOURCES[source]['track']}"
    print(f"publisher started: source={source} track={track} pid={process.pid}", flush=True)
    return process


async def terminate_process(process: asyncio.subprocess.Process) -> None:
    if process.returncode is not None:
        return
    process.terminate()
    try:
        await asyncio.wait_for(process.wait(), timeout=4)
    except asyncio.TimeoutError:
        process.kill()
        await process.wait()


async def main_async() -> None:
    args = parse_args()
    sources = resolve_sources(args.sources)
    stop_event = asyncio.Event()
    publishers: list[asyncio.subprocess.Process] = []

    loop = asyncio.get_running_loop()
    for signame in ("SIGINT", "SIGTERM"):
        try:
            loop.add_signal_handler(getattr(signal, signame), stop_event.set)
        except NotImplementedError:
            pass

    try:
        publishers = [await start_publisher(source, args.relay_host, args.relay_port) for source in sources]
        print("press Ctrl+C to stop publishers", flush=True)
        await stop_event.wait()
    finally:
        for process in publishers:
            await terminate_process(process)
        print("publishers stopped", flush=True)


def main() -> None:
    asyncio.run(main_async())


if __name__ == "__main__":
    main()
