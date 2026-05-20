#!/usr/bin/env python3
"""
Async wrapper for the official Rust moq-relay process.
"""

import asyncio
import os
import shutil
import socket
import subprocess
import tempfile
from pathlib import Path
from typing import Dict, List, Optional


class RustRelayStartupError(RuntimeError):
    """Raised when the official Rust relay cannot be started."""


class RustMOQRelay:
    """
    Run the official moq-dev Rust relay behind the current Python lifecycle API.

    The wrapper starts either:
    - a `moq-relay` binary from `MOQ_OFFICIAL_RELAY_BIN` or PATH, or
    - `cargo run --release --package moq-relay` from `MOQ_OFFICIAL_RELAY_SOURCE`.
    """

    def __init__(
        self,
        host: str,
        port: int,
        cache_dir: Optional[str] = None,
        max_memory_cache: int = 100 * 1024 * 1024,
        max_disk_cache: int = 1024 * 1024 * 1024,
        config_path: Optional[str] = None,
        auth_public: str = '',
        startup_timeout: Optional[float] = None,
        group_cache_age_secs: Optional[int] = None,
    ):
        self.host = host
        self.port = port
        self.cache_dir = Path(cache_dir or tempfile.mkdtemp(prefix='moq-rust-relay-'))
        self.max_memory_cache = max_memory_cache
        self.max_disk_cache = max_disk_cache
        self.config_path = Path(config_path) if config_path else None
        self.auth_public = auth_public
        self.startup_timeout = startup_timeout or float(
            os.environ.get('MOQ_RUST_RELAY_STARTUP_TIMEOUT', '15.0')
        )
        self.group_cache_age_secs = group_cache_age_secs or self._default_group_cache_age_secs()
        self._process: Optional[asyncio.subprocess.Process] = None
        self._generated_config: Optional[Path] = None
        self._log_path: Optional[Path] = None
        self._log_file = None
        self._running = False

    async def start(self):
        """Start the official Rust relay and wait until its TCP sidecar is reachable."""
        if self._running:
            return

        self.cache_dir.mkdir(parents=True, exist_ok=True)
        config_path = self.config_path or self._write_config()
        command, cwd = self._build_command(config_path)
        env = self._build_env()
        self._log_path = self.cache_dir / 'official-relay' / f'moq-relay-{self.port}.log'
        self._log_path.parent.mkdir(parents=True, exist_ok=True)
        self._log_file = self._log_path.open('ab')

        self._process = await asyncio.create_subprocess_exec(
            *command,
            cwd=str(cwd) if cwd else None,
            env=env,
            stdout=self._log_file,
            stderr=self._log_file,
        )

        try:
            await self._wait_until_ready()
        except Exception:
            await self.stop()
            raise

        self._running = True

    async def stop(self):
        """Stop the official Rust relay process."""
        process = self._process
        self._process = None
        self._running = False

        if process is None:
            self._close_log_file()
            return

        if process.returncode is None:
            process.terminate()
            try:
                await asyncio.wait_for(process.wait(), timeout=5.0)
            except asyncio.TimeoutError:
                process.kill()
                await process.wait()
        self._close_log_file()

    def get_cache_stats(self) -> Dict[str, int]:
        """
        Return cache stats in the shape expected by existing tests.

        The official relay does not expose the legacy Python relay's object-cache counters,
        so these values only report that this wrapper has no local Python object cache.
        """
        return {
            'memory_objects': 0,
            'memory_size': 0,
            'disk_size': 0,
            'hits': 0,
            'misses': 0,
        }

    def _build_command(self, config_path: Path) -> tuple[List[str], Optional[Path]]:
        relay_bin = os.environ.get('MOQ_OFFICIAL_RELAY_BIN') or shutil.which('moq-relay')
        if relay_bin:
            return [relay_bin, str(config_path)], None

        source = os.environ.get('MOQ_OFFICIAL_RELAY_SOURCE')
        if source:
            source_path = Path(source)
            if not (source_path / 'Cargo.toml').exists():
                raise RustRelayStartupError(
                    f"MOQ_OFFICIAL_RELAY_SOURCE does not contain Cargo.toml: {source_path}"
                )
            return [
                'cargo',
                'run',
                '--release',
                '--package',
                'moq-relay',
                '--',
                str(config_path),
            ], source_path

        raise RustRelayStartupError(
            "Official Rust relay is not available. Set MOQ_OFFICIAL_RELAY_BIN to a "
            "moq-relay binary or MOQ_OFFICIAL_RELAY_SOURCE to a moq-dev/moq checkout."
        )

    def _build_env(self) -> Dict[str, str]:
        env = os.environ.copy()
        env.setdefault('RUST_LOG', os.environ.get('MOQ_RUST_LOG', 'info'))
        env['MOQ_LITE_MAX_GROUP_AGE_SECS'] = str(self.group_cache_age_secs)
        return env

    def _write_config(self) -> Path:
        config_dir = self.cache_dir / 'official-relay'
        config_dir.mkdir(parents=True, exist_ok=True)
        config_path = config_dir / f'moq-relay-{self.port}.toml'
        iroh_secret = config_dir / 'iroh-secret.key'
        listen = self._format_listen_addr(self.host, self.port)
        tls_names = self._tls_generate_names()

        config_path.write_text(
            "\n".join([
                "[log]",
                f'level = "{os.environ.get("MOQ_RUST_LOG_LEVEL", "info")}"',
                "",
                "[server]",
                f'listen = "{listen}"',
                'max_streams = 10000',
                f'tls.generate = {self._toml_array(tls_names)}',
                "",
                "[web.http]",
                f'listen = "{listen}"',
                "",
                "[auth]",
                f'public = "{self.auth_public}"',
                "",
                "[iroh]",
                "enabled = false",
                f'secret = "{iroh_secret}"',
                "",
            ]),
            encoding='utf-8',
        )
        self._generated_config = config_path
        return config_path

    def _format_listen_addr(self, host: str, port: int) -> str:
        if host in ('0.0.0.0', ''):
            return f'[::]:{port}'
        if ':' in host and not host.startswith('['):
            return f'[{host}]:{port}'
        return f'{host}:{port}'

    def _tls_generate_names(self) -> List[str]:
        names = ['localhost']
        if self.host not in ('0.0.0.0', '::', '[::]', ''):
            names.append(self.host.strip('[]'))
        if '127.0.0.1' not in names:
            names.append('127.0.0.1')
        return names

    def _toml_array(self, values: List[str]) -> str:
        quoted = [f'"{value}"' for value in values]
        return '[' + ', '.join(quoted) + ']'

    @staticmethod
    def _default_group_cache_age_secs() -> int:
        value = os.environ.get('MOQ_LITE_MAX_GROUP_AGE_SECS', '300').strip()
        try:
            return max(1, int(value))
        except ValueError:
            return 300

    async def _wait_until_ready(self):
        deadline = asyncio.get_running_loop().time() + self.startup_timeout
        connect_host = self._connect_host()

        while True:
            if self._process and self._process.returncode is not None:
                raise RustRelayStartupError(
                    "Official Rust relay exited during startup: "
                    f"rc={self._process.returncode}{self._log_tail()}"
                )
            if await asyncio.to_thread(self._can_connect_tcp, connect_host, self.port):
                return
            if asyncio.get_running_loop().time() >= deadline:
                raise RustRelayStartupError(
                    "Timed out waiting for official Rust relay on "
                    f"{connect_host}:{self.port}{self._log_tail()}"
                )
            await asyncio.sleep(0.1)

    def _connect_host(self) -> str:
        if self.host in ('0.0.0.0', '::', '[::]', ''):
            return '127.0.0.1'
        return self.host.strip('[]')

    def _can_connect_tcp(self, host: str, port: int) -> bool:
        try:
            with socket.create_connection((host, port), timeout=0.2):
                return True
        except OSError:
            return False

    def _close_log_file(self):
        if self._log_file is None:
            return
        self._log_file.close()
        self._log_file = None

    def _log_tail(self) -> str:
        if self._log_file is not None:
            self._log_file.flush()
        if self._log_path is None or not self._log_path.exists():
            return ''
        content = self._log_path.read_text(encoding='utf-8', errors='replace')
        lines = [line for line in content.splitlines() if line.strip()]
        if not lines:
            return f"; log={self._log_path}"
        return f"; log={self._log_path}; tail=" + "\n".join(lines[-12:])
