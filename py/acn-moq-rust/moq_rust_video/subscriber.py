"""
AVC3/H.264 subscription API backed by Rust moq-cli.
"""

from __future__ import annotations

import subprocess
import struct
import time
from dataclasses import dataclass
from typing import BinaryIO, Iterator

from .binaries import resolve_moq_cli, verify_moq_cli_supports_avc3
from .errors import FrameDecodeError, ProcessExitedError
from .process import LogBuffer, popen, start_stderr_drain

AVC3_FRAME_MAGIC = b'MAVC'
AVC3_TIMED_FRAME_MAGIC = b'MAVT'
AVC3_LEGACY_FRAME_HEADER = struct.Struct('!QBI')
AVC3_TIMED_FRAME_HEADER = struct.Struct('!QBQI')


@dataclass(frozen=True)
class Avc3Frame:
    """One H.264 Annex-B frame exported by patched moq-cli subscribe --output avc3."""

    payload: bytes
    timestamp_us: int
    keyframe: bool
    received_epoch_ms: float
    sent_epoch_ms: int = 0


class Avc3Subscriber:
    """Subscribe to a broadcast and yield H.264 Annex-B frames."""

    def __init__(
        self,
        relay_url: str,
        name: str = 'camera',
        *,
        moq_cli_path: str | None = None,
        client_bind: str = '0.0.0.0:0',
        max_latency_ms: int = 100,
        log_level: str = 'warn',
        max_frame_bytes: int = 2_000_000,
        verify_moq_cli: bool = False,
    ):
        self.relay_url = relay_url
        self.name = name
        self.moq_cli_path = moq_cli_path
        self.client_bind = client_bind
        self.max_latency_ms = max_latency_ms
        self.log_level = log_level
        self.max_frame_bytes = max_frame_bytes
        self.verify_moq_cli = verify_moq_cli

        self.logs_buffer = LogBuffer()
        self._process: subprocess.Popen | None = None

    def start(self) -> None:
        if self.is_running():
            return

        moq_bin = resolve_moq_cli(self.moq_cli_path)
        if self.verify_moq_cli:
            verify_moq_cli_supports_avc3(moq_bin)

        self._process = popen(
            self.build_moq_command(moq_bin),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        start_stderr_drain(self._process, 'moq-cli', self.logs_buffer)

    def stop(self, timeout: float = 5.0) -> None:
        process = self._process
        if process is None or process.poll() is not None:
            return
        process.terminate()
        try:
            process.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=timeout)

    def is_running(self) -> bool:
        return self._process is not None and self._process.poll() is None

    def frames(self) -> Iterator[Avc3Frame]:
        """Yield frames until the subscriber exits or the stream is stopped."""
        if self._process is None:
            self.start()
        assert self._process is not None
        if self._process.stdout is None:
            raise ProcessExitedError('moq-cli stdout pipe was not created')

        while True:
            try:
                yield read_avc3_frame(self._process.stdout, self.max_frame_bytes)
            except EOFError:
                return_code = self._process.poll()
                if return_code not in (None, 0):
                    tail = '\n'.join(self.logs()[-20:])
                    raise ProcessExitedError(
                        f'moq-cli subscriber exited rc={return_code}\n{tail}'
                    )
                return

    def read_frame(self) -> Avc3Frame:
        """Read exactly one frame, starting the process if needed."""
        if self._process is None:
            self.start()
        assert self._process is not None
        if self._process.stdout is None:
            raise ProcessExitedError('moq-cli stdout pipe was not created')
        return read_avc3_frame(self._process.stdout, self.max_frame_bytes)

    def logs(self) -> list[str]:
        return self.logs_buffer.snapshot()

    def build_moq_command(self, moq_cli_bin: str = 'moq-cli') -> list[str]:
        return [
            moq_cli_bin,
            '--log-level',
            self.log_level,
            '--iroh-enabled=false',
            'subscribe',
            '--client-bind',
            self.client_bind,
            '--url',
            self.relay_url,
            '--name',
            self.name,
            '--output',
            'avc3',
            '--max-latency',
            str(self.max_latency_ms),
        ]


def read_avc3_frame(stream: BinaryIO, max_frame_bytes: int = 2_000_000) -> Avc3Frame:
    """Read and decode one MAVC/MAVT-framed H.264 frame from a binary stream."""
    magic = _read_exact(stream, 4)
    if magic == AVC3_TIMED_FRAME_MAGIC:
        header = _read_exact(stream, AVC3_TIMED_FRAME_HEADER.size)
        timestamp_us, keyframe, sent_epoch_ms, payload_len = AVC3_TIMED_FRAME_HEADER.unpack(header)
    elif magic == AVC3_FRAME_MAGIC:
        header = _read_exact(stream, AVC3_LEGACY_FRAME_HEADER.size)
        timestamp_us, keyframe, payload_len = AVC3_LEGACY_FRAME_HEADER.unpack(header)
        sent_epoch_ms = 0
    else:
        raise FrameDecodeError(f'invalid AVC3 frame magic: {magic!r}')

    if payload_len <= 0 or payload_len > max_frame_bytes:
        raise FrameDecodeError(f'invalid AVC3 frame payload length: {payload_len}')

    payload = _read_exact(stream, payload_len)
    return Avc3Frame(
        payload=payload,
        timestamp_us=timestamp_us,
        keyframe=bool(keyframe),
        received_epoch_ms=time.time() * 1000,
        sent_epoch_ms=sent_epoch_ms,
    )


def _read_exact(stream: BinaryIO, length: int) -> bytes:
    data = stream.read(length)
    if data == b'':
        raise EOFError
    if len(data) != length:
        raise EOFError(f'expected {length} bytes, got {len(data)}')
    return data
