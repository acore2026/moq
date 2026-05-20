"""
Locate bundled and external binaries used by moq_rust_video.
"""

from __future__ import annotations

import os
import platform
import shutil
import subprocess
from pathlib import Path

from .errors import BinaryNotFoundError, ProcessStartError


PACKAGE_ROOT = Path(__file__).resolve().parent


def platform_key(system: str | None = None, machine: str | None = None) -> str:
    """Return the package-data platform key for the current machine."""
    system_name = (system or platform.system()).lower()
    machine_name = (machine or platform.machine()).lower()

    if system_name == 'windows' and machine_name in ('amd64', 'x86_64'):
        return 'win_amd64'
    if system_name == 'linux' and machine_name in ('amd64', 'x86_64'):
        return 'manylinux_x86_64'
    if system_name == 'darwin' and machine_name in ('arm64', 'aarch64'):
        return 'macosx_arm64'
    if system_name == 'darwin' and machine_name in ('amd64', 'x86_64'):
        return 'macosx_x86_64'

    normalized = f'{system_name}_{machine_name}'.replace('-', '_')
    raise BinaryNotFoundError(f'unsupported platform for bundled moq-cli: {normalized}')


def bundled_moq_cli_path(
    system: str | None = None,
    machine: str | None = None,
) -> Path:
    """Return the expected path of the bundled moq-cli binary for this platform."""
    key = platform_key(system, machine)
    exe_name = 'moq-cli.exe' if key == 'win_amd64' else 'moq-cli'
    return PACKAGE_ROOT / 'bin' / key / exe_name


def resolve_moq_cli(path: str | os.PathLike[str] | None = None) -> str:
    """
    Resolve moq-cli from an explicit path, environment variable, bundled binary, or PATH.
    """
    candidates = []
    if path:
        candidates.append(Path(path))

    env_path = os.environ.get('MOQ_RUST_VIDEO_MOQ_CLI')
    if env_path:
        candidates.append(Path(env_path))

    with_context = None
    try:
        with_context = bundled_moq_cli_path()
    except BinaryNotFoundError:
        with_context = None
    if with_context:
        candidates.append(with_context)

    for candidate in candidates:
        if candidate.exists() and os.access(candidate, os.X_OK):
            return str(candidate)

    from_path = shutil.which('moq-cli')
    if from_path:
        return from_path

    searched = ', '.join(str(item) for item in candidates) or 'no bundled candidate'
    raise BinaryNotFoundError(
        'moq-cli was not found. Provide moq_cli_path, set MOQ_RUST_VIDEO_MOQ_CLI, '
        f'or install moq-cli on PATH. Searched: {searched}'
    )


def resolve_ffmpeg(path: str | os.PathLike[str] | None = None) -> str:
    """Resolve ffmpeg from an explicit path, environment variable, or PATH."""
    candidates = []
    if path:
        candidates.append(Path(path))

    env_path = os.environ.get('MOQ_RUST_VIDEO_FFMPEG')
    if env_path:
        candidates.append(Path(env_path))

    for candidate in candidates:
        if candidate.exists() and os.access(candidate, os.X_OK):
            return str(candidate)

    from_path = shutil.which('ffmpeg')
    if from_path:
        return from_path

    searched = ', '.join(str(item) for item in candidates) or 'PATH'
    raise BinaryNotFoundError(
        f'ffmpeg was not found. Provide ffmpeg_path or set MOQ_RUST_VIDEO_FFMPEG. '
        f'Searched: {searched}'
    )


def verify_moq_cli_supports_avc3(moq_cli_path: str, timeout: float = 5.0) -> None:
    """Ensure the selected moq-cli supports the required AVC3 subscribe output."""
    try:
        completed = subprocess.run(
            [moq_cli_path, 'subscribe', '--help'],
            check=False,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            timeout=timeout,
        )
    except OSError as exc:
        raise ProcessStartError(f'failed to execute moq-cli: {moq_cli_path}') from exc
    except subprocess.TimeoutExpired as exc:
        raise ProcessStartError(f'moq-cli help timed out: {moq_cli_path}') from exc

    if completed.returncode != 0:
        raise ProcessStartError(
            f'moq-cli subscribe --help failed with rc={completed.returncode}: '
            f'{completed.stdout[-500:]}'
        )

    if 'avc3' not in completed.stdout:
        raise ProcessStartError(
            'moq-cli does not support subscribe --output avc3; use the patched '
            'moq-rust build shipped with this package.'
        )
