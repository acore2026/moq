#!/usr/bin/env python3
"""
MOQ Relay Example - Demonstrates how to use the MOQRelay class.

This example shows how to run a MOQ relay server using the public interface
from the moq package. The relay uses QUIC as the underlying transport.

Usage:
    python relay_example.py

The relay listens on 127.0.0.1:4443 and acts as an intermediary
between publishers and subscribers.
"""

import asyncio
import logging
import os
import platform
import re
import shutil
import signal
import subprocess

try:
    from examples._bootstrap import ensure_repo_root, setup_logging
except ImportError:  # pragma: no cover - direct script execution fallback
    from _bootstrap import ensure_repo_root, setup_logging

ensure_repo_root()
setup_logging()

# Import MOQRelay from the public moq interface
from moq import MOQRelay
logger = logging.getLogger(__name__)

RELAY_HOST = "127.0.0.1"
RELAY_PORT = 4443


def find_listener_pids(port: int) -> set[int]:
    """Return process IDs currently listening on the given TCP or UDP port."""
    if port <= 0:
        return set()

    system = platform.system()
    pids: set[int] = set()

    if system == "Windows":
        result = subprocess.run(
            ["netstat", "-ano"],
            check=False,
            capture_output=True,
            text=True,
        )
        if result.returncode != 0:
            return set()

        tcp_pattern = re.compile(rf"^\s*TCP\s+\S+:{port}\s+\S+\s+LISTENING\s+(\d+)\s*$")
        udp_pattern = re.compile(rf"^\s*UDP\s+\S+:{port}\s+\*:\*\s+(\d+)\s*$")
        for line in result.stdout.splitlines():
            match = tcp_pattern.match(line) or udp_pattern.match(line)
            if match:
                pids.add(int(match.group(1)))
        return pids

    if shutil.which("ss"):
        for args in (
            ["ss", "-ltnp", f"sport = :{port}"],
            ["ss", "-lunp", f"sport = :{port}"],
        ):
            result = subprocess.run(
                args,
                check=False,
                capture_output=True,
                text=True,
            )
            if result.returncode == 0:
                pids.update(int(pid) for pid in re.findall(r"pid=(\d+)", result.stdout))
        if pids:
            return pids

    if shutil.which("lsof"):
        for args in (
            ["lsof", f"-tiTCP:{port}", "-sTCP:LISTEN"],
            ["lsof", f"-tiUDP:{port}"],
        ):
            result = subprocess.run(
                args,
                check=False,
                capture_output=True,
                text=True,
            )
            if result.returncode == 0:
                for line in result.stdout.splitlines():
                    line = line.strip()
                    if line.isdigit():
                        pids.add(int(line))

    return pids


def stop_listener_processes(port: int) -> list[int]:
    """Best-effort terminate processes currently listening on the given TCP or UDP port."""
    stopped: list[int] = []
    current_pid = os.getpid()

    for pid in sorted(find_listener_pids(port)):
        if pid == current_pid:
            continue
        try:
            os.kill(pid, signal.SIGTERM)
            stopped.append(pid)
        except OSError:
            logger.debug("Failed to stop pid=%d on port %d", pid, port, exc_info=True)

    if stopped:
        logger.warning("Stopped existing listener(s) on port %d: %s", port, ", ".join(map(str, stopped)))

    return stopped


async def release_listener_port(port: int, label: str) -> bool:
    """Best-effort stop the current listener on a port and wait briefly for release."""
    stopped_pids = stop_listener_processes(port)
    if not stopped_pids:
        return False

    logger.warning("%s %d was already in use; attempting to reclaim it before startup", label, port)
    await asyncio.sleep(0.2)

    stubborn_pids = [pid for pid in sorted(find_listener_pids(port)) if pid in set(stopped_pids)]
    if stubborn_pids and hasattr(signal, "SIGKILL"):
        for pid in stubborn_pids:
            try:
                os.kill(pid, signal.SIGKILL)
            except OSError:
                logger.debug("Failed to force-stop pid=%d on port %d", pid, port, exc_info=True)
        logger.warning(
            "%s %d still had listener(s) after SIGTERM; force-stopped: %s",
            label,
            port,
            ", ".join(map(str, stubborn_pids)),
        )
        await asyncio.sleep(0.2)

    return True


async def main():
    """Run the MOQ relay server."""
    logger.info("Starting MOQ Relay Example")

    await release_listener_port(RELAY_PORT, "Relay port")
    
    # Create relay instance using the public interface
    relay = MOQRelay(
        host=RELAY_HOST,
        port=RELAY_PORT,
        cache_dir="/tmp/moq_relay_cache",
        max_memory_cache=100 * 1024 * 1024,  # 100MB
        max_disk_cache=1024 * 1024 * 1024     # 1GB
    )
    
    logger.info(f"Relay configured: {RELAY_HOST}:{RELAY_PORT}")
    logger.info(f"Cache directory: /tmp/moq_relay_cache")
    
    try:
        # Start the relay - this uses QUIC transport
        await relay.start()
        logger.info("Relay started")

        # Keep the process alive until interrupted.
        while True:
            await asyncio.sleep(1)
    except KeyboardInterrupt:
        logger.info("Relay stopped by user")
    except Exception as e:
        logger.error(f"Relay error: {e}")
    finally:
        await relay.stop()
        logger.info("Relay shutdown complete")


if __name__ == "__main__":
    asyncio.run(main())
