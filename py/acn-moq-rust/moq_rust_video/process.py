"""
Small process-management helpers for publisher/subscriber wrappers.
"""

from __future__ import annotations

import subprocess
import threading
from collections import deque
from dataclasses import dataclass, field
from typing import BinaryIO, Iterable, Sequence

from .errors import ProcessStartError


@dataclass
class LogBuffer:
    """Thread-safe fixed-size text log buffer."""

    max_lines: int = 400
    _lines: deque[str] = field(default_factory=deque, init=False)
    _lock: threading.Lock = field(default_factory=threading.Lock, init=False)

    def append(self, line: str) -> None:
        with self._lock:
            self._lines.append(line.rstrip())
            while len(self._lines) > self.max_lines:
                self._lines.popleft()

    def snapshot(self) -> list[str]:
        with self._lock:
            return list(self._lines)


def start_stderr_drain(process: subprocess.Popen, label: str, logs: LogBuffer) -> threading.Thread:
    """Start a background thread that records stderr lines from a process."""
    thread = threading.Thread(
        target=_drain_stderr,
        args=(process, label, logs),
        daemon=True,
    )
    thread.start()
    return thread


def _drain_stderr(process: subprocess.Popen, label: str, logs: LogBuffer) -> None:
    stderr = process.stderr
    if stderr is None:
        return
    for raw_line in iter(stderr.readline, b''):
        if not raw_line:
            break
        logs.append(f'{label}: {raw_line.decode("utf-8", errors="replace").rstrip()}')


def popen(
    command: Sequence[str],
    *,
    stdin: int | BinaryIO | None = None,
    stdout: int | BinaryIO | None = None,
    stderr: int | BinaryIO | None = subprocess.PIPE,
) -> subprocess.Popen:
    """Start a process and map OSError to a package exception."""
    try:
        return subprocess.Popen(
            list(command),
            stdin=stdin,
            stdout=stdout,
            stderr=stderr,
        )
    except OSError as exc:
        raise ProcessStartError(f'failed to start process: {_format_command(command)}') from exc


def _format_command(command: Iterable[str]) -> str:
    return ' '.join(str(part) for part in command)
