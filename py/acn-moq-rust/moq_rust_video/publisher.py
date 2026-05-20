"""
Camera publishing API backed by ffmpeg and Rust moq-cli.
"""

from __future__ import annotations

import platform
import subprocess
import time
from dataclasses import dataclass
from typing import Literal, Sequence

from .binaries import resolve_ffmpeg, resolve_moq_cli
from .errors import ProcessExitedError
from .process import LogBuffer, popen, start_stderr_drain

CaptureBackend = Literal['auto', 'dshow', 'v4l2', 'avfoundation', 'lavfi']


@dataclass(frozen=True)
class PublisherStatus:
    """Current publisher process status."""

    running: bool
    ffmpeg_returncode: int | None
    moq_returncode: int | None
    uptime_seconds: float


class CameraPublisher:
    """
    Publish a camera or test source to a Rust MoQ relay as AVC3/H.264.

    Python only controls process lifecycle. Video bytes flow directly from ffmpeg stdout
    to moq-cli stdin.
    """

    def __init__(
        self,
        relay_url: str,
        name: str = 'camera',
        camera_name: str | None = None,
        *,
        backend: CaptureBackend = 'auto',
        device: str | None = None,
        width: int = 1280,
        height: int = 720,
        fps: float = 30.0,
        bitrate: str = '2500k',
        maxrate: str | None = None,
        bufsize: str | None = None,
        keyint: int | None = None,
        ffmpeg_path: str | None = None,
        moq_cli_path: str | None = None,
        client_bind: str = '0.0.0.0:0',
        log_level: str = 'warn',
        ffmpeg_log_level: str = 'warning',
        extra_ffmpeg_args: Sequence[str] | None = None,
        test_source: bool = False,
        verify_moq_cli: bool = False,
    ):
        self.relay_url = relay_url
        self.name = name
        self.camera_name = camera_name
        self.backend = backend
        self.device = device
        self.width = width
        self.height = height
        self.fps = fps
        self.bitrate = bitrate
        self.maxrate = maxrate or bitrate
        self.bufsize = bufsize or _default_bufsize(bitrate)
        self.keyint = keyint or max(1, round(fps))
        self.ffmpeg_path = ffmpeg_path
        self.moq_cli_path = moq_cli_path
        self.client_bind = client_bind
        self.log_level = log_level
        self.ffmpeg_log_level = ffmpeg_log_level
        self.extra_ffmpeg_args = list(extra_ffmpeg_args or [])
        self.test_source = test_source
        self.verify_moq_cli = verify_moq_cli

        self.logs_buffer = LogBuffer()
        self._ffmpeg: subprocess.Popen | None = None
        self._moq: subprocess.Popen | None = None
        self._started_at: float | None = None

    @classmethod
    def test_source(
        cls,
        relay_url: str,
        name: str = 'camera',
        *,
        width: int = 1280,
        height: int = 720,
        fps: float = 30.0,
        bitrate: str = '2500k',
        **kwargs,
    ) -> 'CameraPublisher':
        """Create a publisher that uses ffmpeg's testsrc2 generator."""
        return cls(
            relay_url=relay_url,
            name=name,
            width=width,
            height=height,
            fps=fps,
            bitrate=bitrate,
            backend='lavfi',
            test_source=True,
            **kwargs,
        )

    def start(self) -> None:
        """Start ffmpeg and moq-cli."""
        if self.is_running():
            return

        ffmpeg_bin = resolve_ffmpeg(self.ffmpeg_path)
        moq_bin = resolve_moq_cli(self.moq_cli_path)

        if self.verify_moq_cli:
            from .binaries import verify_moq_cli_supports_avc3

            verify_moq_cli_supports_avc3(moq_bin)

        ffmpeg_cmd = self.build_ffmpeg_command(ffmpeg_bin)
        moq_cmd = self.build_moq_command(moq_bin)

        self._ffmpeg = popen(
            ffmpeg_cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        if self._ffmpeg.stdout is None:
            self._ffmpeg.kill()
            raise ProcessExitedError('ffmpeg stdout pipe was not created')

        self._moq = popen(
            moq_cmd,
            stdin=self._ffmpeg.stdout,
            stderr=subprocess.PIPE,
        )
        self._ffmpeg.stdout.close()
        self._started_at = time.monotonic()

        start_stderr_drain(self._ffmpeg, 'ffmpeg', self.logs_buffer)
        start_stderr_drain(self._moq, 'moq-cli', self.logs_buffer)

    def stop(self, timeout: float = 5.0) -> None:
        """Stop child processes, terminating moq-cli first to close the pipeline."""
        for process in (self._moq, self._ffmpeg):
            if process is None or process.poll() is not None:
                continue
            process.terminate()
            try:
                process.wait(timeout=timeout)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=timeout)

    def wait(self, timeout: float | None = None) -> int:
        """Wait for moq-cli to exit and return its exit code."""
        if self._moq is None:
            raise ProcessExitedError('publisher has not been started')
        return self._moq.wait(timeout=timeout)

    def is_running(self) -> bool:
        return bool(
            self._ffmpeg is not None
            and self._moq is not None
            and self._ffmpeg.poll() is None
            and self._moq.poll() is None
        )

    def status(self) -> PublisherStatus:
        started_at = self._started_at or time.monotonic()
        return PublisherStatus(
            running=self.is_running(),
            ffmpeg_returncode=self._ffmpeg.poll() if self._ffmpeg else None,
            moq_returncode=self._moq.poll() if self._moq else None,
            uptime_seconds=max(0.0, time.monotonic() - started_at),
        )

    def logs(self) -> list[str]:
        return self.logs_buffer.snapshot()

    def build_moq_command(self, moq_cli_bin: str = 'moq-cli') -> list[str]:
        command = [
            moq_cli_bin,
            '--log-level',
            self.log_level,
            '--iroh-enabled=false',
            'publish',
            '--client-bind',
            self.client_bind,
            '--url',
            self.relay_url,
            '--name',
            self.name,
            'avc3',
        ]
        return command

    def build_ffmpeg_command(self, ffmpeg_bin: str = 'ffmpeg') -> list[str]:
        backend = self._resolved_backend()
        command = [
            ffmpeg_bin,
            '-hide_banner',
            '-loglevel',
            self.ffmpeg_log_level,
        ]

        if backend == 'lavfi':
            command.extend([
                '-re',
                '-f',
                'lavfi',
                '-i',
                f'testsrc2=size={self.width}x{self.height}:rate={_format_fps(self.fps)}',
            ])
        elif backend == 'dshow':
            camera = self.camera_name or self.device
            if not camera:
                raise ValueError('camera_name or device is required for dshow capture')
            command.extend([
                '-f',
                'dshow',
                '-video_size',
                f'{self.width}x{self.height}',
                '-framerate',
                _format_fps(self.fps),
                '-i',
                f'video={camera}',
            ])
        elif backend == 'v4l2':
            device = self.device or self.camera_name or '/dev/video0'
            command.extend([
                '-f',
                'v4l2',
                '-video_size',
                f'{self.width}x{self.height}',
                '-framerate',
                _format_fps(self.fps),
                '-i',
                device,
            ])
        elif backend == 'avfoundation':
            device = self.device or self.camera_name or '0'
            command.extend([
                '-f',
                'avfoundation',
                '-framerate',
                _format_fps(self.fps),
                '-video_size',
                f'{self.width}x{self.height}',
                '-i',
                device,
            ])
        else:
            raise ValueError(f'unsupported capture backend: {backend}')

        command.extend([
            '-an',
            '-threads',
            '1',
            '-c:v',
            'libx264',
            '-preset',
            'ultrafast',
            '-tune',
            'zerolatency',
            '-profile:v',
            'baseline',
            '-level',
            '3.1',
            '-x264-params',
            (
                f'keyint={self.keyint}:min-keyint={self.keyint}:'
                'scenecut=0:repeat-headers=1'
            ),
            '-b:v',
            self.bitrate,
            '-maxrate',
            self.maxrate,
            '-bufsize',
            self.bufsize,
            '-pix_fmt',
            'yuv420p',
        ])
        command.extend(self.extra_ffmpeg_args)
        command.extend(['-f', 'h264', '-'])
        return command

    def _resolved_backend(self) -> str:
        if self.test_source:
            return 'lavfi'
        if self.backend != 'auto':
            return self.backend

        system = platform.system().lower()
        if system == 'windows':
            return 'dshow'
        if system == 'linux':
            return 'v4l2'
        if system == 'darwin':
            return 'avfoundation'
        raise ValueError(f'unsupported capture platform: {system}')


def _format_fps(fps: float) -> str:
    return str(int(fps)) if float(fps).is_integer() else str(fps)


def _default_bufsize(bitrate: str) -> str:
    if bitrate.endswith('k') and bitrate[:-1].isdigit():
        return f'{int(bitrate[:-1]) * 2}k'
    if bitrate.endswith('M') and bitrate[:-1].isdigit():
        return f'{int(bitrate[:-1]) * 2}M'
    return bitrate
