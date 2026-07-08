#!/usr/bin/env python3
"""
Shared helpers for live H.264 MOQ video test tools.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

try:
    from moq import FullTrackName
except ImportError:
    FullTrackName = None


DEFAULT_NAMESPACE = 'agent/video/windows-camera'
DEFAULT_VIDEO_NAME = 'h264'
DEFAULT_META_NAME = 'h264-meta'
VCL_NAL_TYPES = {1, 5}
IDR_NAL_TYPE = 5


def build_track(namespace: str, name: str) -> Any:
    if FullTrackName is None:
        raise RuntimeError(
            'Python moq package is required for the python subscriber mode; '
            'use --subscriber rust-avc3 for the Rust moq-cli video path.'
        )
    namespace_parts = [
        part.encode('utf-8')
        for part in namespace.strip('/').split('/')
        if part
    ]
    return FullTrackName(namespace_parts, name.encode('utf-8'))


def nal_type(nal: bytes) -> int | None:
    prefix_len = start_code_len(nal)
    if prefix_len is None or len(nal) <= prefix_len:
        return None
    return nal[prefix_len] & 0x1F


def start_code_len(data: bytes) -> int | None:
    if data.startswith(b'\x00\x00\x00\x01'):
        return 4
    if data.startswith(b'\x00\x00\x01'):
        return 3
    return None


def contains_idr(frame: bytes) -> bool:
    for nal in split_annexb_nals(frame):
        if nal_type(nal) == IDR_NAL_TYPE:
            return True
    return False


def split_annexb_nals(data: bytes) -> list[bytes]:
    starts = find_start_codes(data)
    nals = []
    for item_index, (start, prefix_len) in enumerate(starts):
        end = starts[item_index + 1][0] if item_index + 1 < len(starts) else len(data)
        if end > start + prefix_len:
            nals.append(data[start:end])
    return nals


def find_start_codes(data: bytes) -> list[tuple[int, int]]:
    starts = []
    index = 0
    length = len(data)
    while index < length - 3:
        if data[index:index + 3] == b'\x00\x00\x01':
            starts.append((index, 3))
            index += 3
            continue
        if index < length - 4 and data[index:index + 4] == b'\x00\x00\x00\x01':
            starts.append((index, 4))
            index += 4
            continue
        index += 1
    return starts


@dataclass
class AnnexBFrameSplitter:
    """
    Incrementally split an Annex-B H.264 byte stream into low-latency access units.

    The splitter is tuned for libx264 low-latency output with one VCL NAL per frame.
    It emits the previous frame when it sees the next VCL NAL or non-VCL prefix.
    """

    _buffer: bytearray = field(default_factory=bytearray)
    _pending_prefix: list[bytes] = field(default_factory=list)
    _current_frame: bytearray | None = None

    def feed(self, chunk: bytes) -> list[bytes]:
        if chunk:
            self._buffer.extend(chunk)
        frames = []
        for nal in self._pop_complete_nals():
            frames.extend(self._process_nal(nal))
        return frames

    def flush(self) -> list[bytes]:
        frames = []
        if self._buffer:
            nals = split_annexb_nals(bytes(self._buffer))
            self._buffer.clear()
            for nal in nals:
                frames.extend(self._process_nal(nal))
        if self._current_frame:
            frames.append(bytes(self._current_frame))
            self._current_frame = None
        return frames

    def _process_nal(self, nal: bytes) -> list[bytes]:
        kind = nal_type(nal)
        if kind is None:
            return []

        frames = []
        if kind in VCL_NAL_TYPES:
            if self._current_frame:
                frames.append(bytes(self._current_frame))
            self._current_frame = bytearray()
            for prefix in self._pending_prefix:
                self._current_frame.extend(prefix)
            self._pending_prefix.clear()
            self._current_frame.extend(nal)
            return frames

        if self._current_frame:
            frames.append(bytes(self._current_frame))
            self._current_frame = None
        self._pending_prefix.append(nal)
        return frames

    def _pop_complete_nals(self) -> list[bytes]:
        data = bytes(self._buffer)
        starts = find_start_codes(data)
        if len(starts) < 2:
            return []

        nals = []
        for item_index in range(len(starts) - 1):
            start, prefix_len = starts[item_index]
            end = starts[item_index + 1][0]
            if end > start + prefix_len:
                nals.append(data[start:end])

        last_start = starts[-1][0]
        del self._buffer[:last_start]
        return nals
