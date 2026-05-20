from __future__ import annotations

import logging
import os
import struct
import subprocess
import threading
from collections import defaultdict, deque
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, BinaryIO, Deque

from moq_rust_video.binaries import resolve_moq_cli
from moq_rust_video.errors import ProcessExitedError, ProcessStartError
from moq_rust_video.process import LogBuffer, start_stderr_drain


OBJECT_FRAME_MAGIC = b'MOBJ'
OBJECT_FRAME_VERSION = 1
OBJECT_OP_PUBLISH = 1
OBJECT_OP_UNPUBLISH = 2
OBJECT_OP_OBJECT = 3
OBJECT_FRAME_HEADER = struct.Struct('!4sBBHQQI')


@dataclass(frozen=True)
class ObjectEvent:
    namespace: str
    track: str
    group_id: int
    object_id: int
    payload: bytes


class RustCliMoQClient:
    """
    Drop-in ACN SDK MoQClient shape implemented through Rust moq-cli subprocesses.

    This class intentionally keeps the old synchronous API used by acn_sdk:
    connect, publish, send_object, subscribe, fetch, unsubscribe, disconnect.

    Runtime requirement: moq-cli must support the ACN object mode contract:

    - `moq-cli publish ... object`
    - `moq-cli subscribe ... --output object --track <track>`

    The Python layer only manages process lifecycle and object framing; MoQ
    transport and relay interoperability stay in Rust.
    """

    def __init__(
        self,
        host: str,
        remote_port: int,
        role: str,
        on_object_received: Callable[[str, str, bytes], None] | None = None,
        *,
        relay_url: str | None = None,
        moq_cli_path: str | Path | None = None,
        client_bind: str = '0.0.0.0:0',
        log_level: str = 'warn',
        max_latency_ms: int = 100,
        fetch_idle_timeout_ms: int = 250,
        verify_cli: bool | None = None,
        received_cache_size: int | None = None,
    ) -> None:
        self.host = host
        self.remote_port = remote_port
        self.role = role
        self.on_object_received = on_object_received
        self.relay_url = relay_url or self._build_relay_url(host, remote_port)
        self.moq_cli_path = moq_cli_path
        self.client_bind = client_bind
        self.log_level = log_level
        self.max_latency_ms = max_latency_ms
        self.fetch_idle_timeout_ms = fetch_idle_timeout_ms
        self.verify_cli = self._default_verify_cli() if verify_cli is None else verify_cli
        self.received_cache_size = (
            self._default_received_cache_size()
            if received_cache_size is None
            else received_cache_size
        )

        self._logger = logging.getLogger(self.__class__.__name__)
        self._moq_cli_bin: str | None = None
        self._connected = False
        self._subscriptions: dict[str, list[str]] = defaultdict(list)
        self._subscription_processes: dict[str, subprocess.Popen[bytes]] = {}
        self._subscription_threads: dict[str, threading.Thread] = {}
        self._subscription_ranges: dict[str, tuple[int, int, int | None, int | None]] = {}
        self._fetch_processes: dict[int, subprocess.Popen[bytes]] = {}
        self._fetch_threads: dict[int, threading.Thread] = {}
        self._published_tracks: set[str] = set()
        self._publisher_processes: dict[str, subprocess.Popen[bytes]] = {}
        self._object_counters: dict[str, int] = defaultdict(int)
        self._received_cache: dict[str, Deque[ObjectEvent]] = defaultdict(
            lambda: deque(maxlen=self.received_cache_size)
        )
        self._cache_lock = threading.RLock()
        self._fetch_counter = 0
        self.logs = LogBuffer()

    def connect(self) -> None:
        if self.role not in ('publisher', 'subscriber'):
            raise ValueError(f'Unsupported MoQ role: {self.role}')
        self._moq_cli_bin = resolve_moq_cli(self.moq_cli_path)
        if self.verify_cli:
            self._verify_object_mode(self._moq_cli_bin)
        self._connected = True
        self._logger.info('Rust MoQ client connected role=%s url=%s', self.role, self.relay_url)

    def publish(self, namespace: str, track: str) -> None:
        self._require_role('publisher')
        self._ensure_publisher(namespace)
        track_key = self._track_key(namespace, track)
        self._write_publisher_frame(namespace, track, OBJECT_OP_PUBLISH, 0, 0, b'')
        self._published_tracks.add(track_key)
        self._logger.info('Rust MoQ publish namespace=%s track=%s', namespace, track)

    def unpublish(self, namespace: str, track: str) -> None:
        self._require_role('publisher')
        track_key = self._track_key(namespace, track)
        if track_key not in self._published_tracks:
            return
        self._write_publisher_frame(namespace, track, OBJECT_OP_UNPUBLISH, 0, 0, b'')
        self._published_tracks.discard(track_key)
        self._object_counters.pop(track_key, None)
        self._logger.info('Rust MoQ unpublish namespace=%s track=%s', namespace, track)

    def send_object(self, namespace: str, track: str, payload: bytes) -> None:
        self._require_role('publisher')
        track_key = self._track_key(namespace, track)
        if track_key not in self._published_tracks:
            raise RuntimeError(f'Track is not published: {track_key}')
        restarted = self._ensure_publisher(namespace)
        if restarted:
            self._write_publisher_frame(namespace, track, OBJECT_OP_PUBLISH, 0, 0, b'')
        object_id = self._object_counters[track_key]
        self._object_counters[track_key] += 1
        self._write_publisher_frame(namespace, track, OBJECT_OP_OBJECT, 0, object_id, payload)
        self._logger.info(
            'Rust MoQ send object namespace=%s track=%s object_id=%s payload_size=%s',
            namespace,
            track,
            object_id,
            len(payload),
        )

    def subscribe(self, namespace: str, track: str, subscriber_id: str) -> None:
        self._require_role('subscriber')
        key = self._track_key(namespace, track)
        if not self._subscription_alive(key):
            self._start_subscriber(namespace, track)
        self._subscriptions[key].append(subscriber_id)
        self._logger.info(
            'Rust MoQ subscribe namespace=%s track=%s subscriber=%s',
            namespace,
            track,
            subscriber_id,
        )

    def fetch(
        self,
        namespace: str,
        track: str,
        start_group: int = 0,
        start_object: int = 0,
        end_group: int | None = None,
        end_object: int | None = None,
    ) -> int:
        self._require_role('subscriber')
        self._fetch_counter += 1
        self._start_fetch(
            namespace,
            track,
            start_group,
            start_object,
            end_group,
            end_object,
            self._fetch_counter,
        )
        self._logger.info(
            'Rust MoQ fetch requested namespace=%s track=%s request_id=%s '
            'range=[%s:%s to %s:%s]',
            namespace,
            track,
            self._fetch_counter,
            start_group,
            start_object,
            end_group,
            end_object,
        )
        return self._fetch_counter

    def unsubscribe(self, namespace: str, track: str, subscriber_id: str | None = None) -> None:
        self._require_role('subscriber')
        key = self._track_key(namespace, track)
        if subscriber_id is None:
            self._subscriptions.pop(key, None)
        else:
            subscribers = self._subscriptions.get(key, [])
            if subscriber_id in subscribers:
                subscribers.remove(subscriber_id)
            if not subscribers:
                self._subscriptions.pop(key, None)
        if key not in self._subscriptions:
            self._stop_subscription(key)
        self._logger.info(
            'Rust MoQ unsubscribe namespace=%s track=%s subscriber=%s',
            namespace,
            track,
            subscriber_id,
        )

    def simulate_incoming_object(self, namespace: str, track: str, payload: bytes) -> None:
        if self.on_object_received is not None:
            self.on_object_received(namespace, track, payload)

    def pump(self, duration: float = 0.1) -> None:
        threading.Event().wait(duration)

    def is_published(self, namespace: str, track: str) -> bool:
        return self._track_key(namespace, track) in self._published_tracks

    def is_subscribed(self, namespace: str, track: str, subscriber_id: str) -> bool:
        return subscriber_id in self._subscriptions.get(self._track_key(namespace, track), [])

    def disconnect(self) -> None:
        for namespace, process in list(self._publisher_processes.items()):
            self._stop_process(process, f'publisher {namespace}')
        self._publisher_processes.clear()
        for key in list(self._subscription_processes):
            self._stop_subscription(key)
        for request_id in list(self._fetch_processes):
            self._stop_fetch(request_id)
        self._subscriptions.clear()
        self._published_tracks.clear()
        self._object_counters.clear()
        self._clear_received_cache()
        self._connected = False
        self._logger.info('Rust MoQ client disconnected role=%s url=%s', self.role, self.relay_url)

    def close(self) -> None:
        self.disconnect()

    def build_publish_command(self, namespace: str) -> list[str]:
        moq_cli_bin = self._require_moq_cli()
        return [
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
            self._broadcast_name(namespace),
            'object',
        ]

    def build_subscribe_command(
        self,
        namespace: str,
        track: str,
        start_group: int | None = None,
        start_object: int | None = None,
        end_group: int | None = None,
        end_object: int | None = None,
    ) -> list[str]:
        moq_cli_bin = self._require_moq_cli()
        command = [
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
            self._broadcast_name(namespace),
            '--output',
            'object',
            '--track',
            track,
            '--max-latency',
            str(self.max_latency_ms),
        ]
        if start_group is not None:
            command.extend(['--start-group', str(start_group)])
        if start_object is not None:
            command.extend(['--start-object', str(start_object)])
        if end_group is not None:
            command.extend(['--end-group', str(end_group)])
        if end_object is not None:
            command.extend(['--end-object', str(end_object)])
        return command

    def build_fetch_command(
        self,
        namespace: str,
        track: str,
        start_group: int = 0,
        start_object: int = 0,
        end_group: int | None = None,
        end_object: int | None = None,
    ) -> list[str]:
        moq_cli_bin = self._require_moq_cli()
        command = [
            moq_cli_bin,
            '--log-level',
            self.log_level,
            '--iroh-enabled=false',
            'fetch',
            '--client-bind',
            self.client_bind,
            '--url',
            self.relay_url,
            '--name',
            self._broadcast_name(namespace),
            '--output',
            'object',
            '--track',
            track,
            '--start-group',
            str(start_group),
            '--start-object',
            str(start_object),
            '--idle-timeout-ms',
            str(self.fetch_idle_timeout_ms),
        ]
        if end_group is not None:
            command.extend(['--end-group', str(end_group)])
        if end_object is not None:
            command.extend(['--end-object', str(end_object)])
        return command

    def _ensure_publisher(self, namespace: str) -> bool:
        broadcast = self._broadcast_name(namespace)
        process = self._publisher_processes.get(broadcast)
        if process is not None and process.poll() is None:
            return False
        if process is not None:
            self._publisher_processes.pop(broadcast, None)
        command = self.build_publish_command(namespace)
        try:
            process = subprocess.Popen(
                command,
                stdin=subprocess.PIPE,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.PIPE,
            )
        except OSError as exc:
            raise ProcessStartError(f'failed to start moq-cli object publisher: {exc}') from exc
        start_stderr_drain(process, f'moq-cli-publish:{broadcast}', self.logs)
        self._publisher_processes[broadcast] = process
        return True

    def _write_publisher_frame(
        self,
        namespace: str,
        track: str,
        op: int,
        group_id: int,
        object_id: int,
        payload: bytes,
    ) -> None:
        process = self._publisher_processes.get(self._broadcast_name(namespace))
        if process is None or process.stdin is None:
            raise ProcessExitedError('moq-cli publisher stdin is not available')
        if process.poll() is not None:
            raise ProcessExitedError(
                f'moq-cli publisher exited rc={process.returncode}\n{self.logs.tail()}'
            )
        write_object_frame(process.stdin, op, track, group_id, object_id, payload)
        process.stdin.flush()

    def _start_subscriber(
        self,
        namespace: str,
        track: str,
        start_group: int | None = None,
        start_object: int | None = None,
        end_group: int | None = None,
        end_object: int | None = None,
    ) -> None:
        key = self._track_key(namespace, track)
        command = self.build_subscribe_command(
            namespace,
            track,
            start_group,
            start_object,
            end_group,
            end_object,
        )
        try:
            process = subprocess.Popen(
                command,
                stdin=subprocess.DEVNULL,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
        except OSError as exc:
            raise ProcessStartError(f'failed to start moq-cli object subscriber: {exc}') from exc
        start_stderr_drain(process, f'moq-cli-subscribe:{key}', self.logs)
        self._subscription_processes[key] = process
        self._subscription_ranges[key] = (
            start_group or 0,
            start_object or 0,
            end_group,
            end_object,
        )
        thread = threading.Thread(
            target=self._reader_loop,
            args=(namespace, key, process),
            name=f'RustMoQObjectReader-{key}',
            daemon=True,
        )
        self._subscription_threads[key] = thread
        thread.start()

    def _start_fetch(
        self,
        namespace: str,
        track: str,
        start_group: int,
        start_object: int,
        end_group: int | None,
        end_object: int | None,
        request_id: int,
    ) -> None:
        key = self._track_key(namespace, track)
        command = self.build_fetch_command(
            namespace,
            track,
            start_group,
            start_object,
            end_group,
            end_object,
        )
        try:
            process = subprocess.Popen(
                command,
                stdin=subprocess.DEVNULL,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
        except OSError as exc:
            raise ProcessStartError(f'failed to start moq-cli object fetch: {exc}') from exc
        start_stderr_drain(process, f'moq-cli-fetch:{request_id}', self.logs)
        self._fetch_processes[request_id] = process
        thread = threading.Thread(
            target=self._fetch_reader_loop,
            args=(namespace, key, request_id, process),
            name=f'RustMoQObjectFetch-{request_id}',
            daemon=True,
        )
        self._fetch_threads[request_id] = thread
        thread.start()

    def _reader_loop(self, namespace: str, key: str, process: subprocess.Popen[bytes]) -> None:
        if process.stdout is None:
            return
        while process.poll() is None:
            try:
                event = read_object_frame(process.stdout)
            except EOFError:
                return
            except Exception:
                self._logger.warning('Failed to read Rust MoQ object frame key=%s', key, exc_info=True)
                return
            if event['op'] != OBJECT_OP_OBJECT:
                continue
            if not self._event_in_range(key, event):
                continue
            self._handle_object_event(namespace, key, event)

    def _fetch_reader_loop(
        self,
        namespace: str,
        key: str,
        request_id: int,
        process: subprocess.Popen[bytes],
    ) -> None:
        try:
            if process.stdout is None:
                return
            while process.poll() is None:
                try:
                    event = read_object_frame(process.stdout)
                except EOFError:
                    return
                except Exception:
                    self._logger.warning(
                        'Failed to read Rust MoQ fetch object frame request_id=%s',
                        request_id,
                        exc_info=True,
                    )
                    return
                if event['op'] != OBJECT_OP_OBJECT:
                    continue
                self._handle_object_event(namespace, key, event)
        finally:
            self._fetch_processes.pop(request_id, None)
            thread = self._fetch_threads.get(request_id)
            if thread is threading.current_thread():
                self._fetch_threads.pop(request_id, None)

    def _stop_subscription(self, key: str) -> None:
        process = self._subscription_processes.pop(key, None)
        if process is not None:
            self._stop_process(process, f'subscriber {key}')
        self._subscription_ranges.pop(key, None)
        thread = self._subscription_threads.pop(key, None)
        if thread is not None and thread.is_alive():
            thread.join(timeout=1.0)

    def _stop_fetch(self, request_id: int) -> None:
        process = self._fetch_processes.pop(request_id, None)
        if process is not None:
            self._stop_process(process, f'fetch {request_id}')
        thread = self._fetch_threads.pop(request_id, None)
        if (
            thread is not None
            and thread is not threading.current_thread()
            and thread.is_alive()
        ):
            thread.join(timeout=1.0)

    def _subscription_alive(self, key: str) -> bool:
        process = self._subscription_processes.get(key)
        if process is not None and process.poll() is None:
            return True
        if process is not None:
            self._logger.warning('moq-cli subscriber exited key=%s rc=%s', key, process.returncode)
            self._subscription_processes.pop(key, None)
            self._subscription_ranges.pop(key, None)
            thread = self._subscription_threads.pop(key, None)
            if thread is not None and thread.is_alive():
                thread.join(timeout=1.0)
        return False

    def _handle_object_event(self, namespace: str, key: str, event: dict[str, object]) -> None:
        object_event = ObjectEvent(
            namespace=namespace,
            track=str(event['track']),
            group_id=int(event['group_id']),
            object_id=int(event['object_id']),
            payload=bytes(event['payload']),
        )
        self._cache_received_event(key, object_event)
        self._emit_object_event(object_event)

    def _cache_received_event(self, key: str, event: ObjectEvent) -> None:
        if self.received_cache_size <= 0:
            return
        with self._cache_lock:
            self._received_cache[key].append(event)

    def _emit_object_event(self, event: ObjectEvent) -> None:
        if self.on_object_received is not None:
            self.on_object_received(event.namespace, event.track, event.payload)

    def _clear_received_cache(self) -> None:
        with self._cache_lock:
            self._received_cache.clear()

    def _event_in_range(self, key: str, event: dict[str, object]) -> bool:
        start_group, start_object, end_group, end_object = self._subscription_ranges.get(
            key,
            (0, 0, None, None),
        )
        return self._object_in_range(
            int(event['group_id']),
            int(event['object_id']),
            start_group,
            start_object,
            end_group,
            end_object,
        )

    @staticmethod
    def _object_in_range(
        group_id: int,
        object_id: int,
        start_group: int,
        start_object: int,
        end_group: int | None,
        end_object: int | None,
    ) -> bool:
        if group_id < start_group or object_id < start_object:
            return False
        if end_group is not None and group_id > end_group:
            return False
        if end_object is not None and object_id > end_object:
            return False
        return True

    def _stop_process(self, process: subprocess.Popen[bytes], label: str) -> None:
        if process.poll() is not None:
            return
        process.terminate()
        try:
            process.wait(timeout=3.0)
        except subprocess.TimeoutExpired:
            self._logger.warning('Forcing moq-cli %s process to exit', label)
            process.kill()
            process.wait(timeout=3.0)

    def _require_role(self, role: str) -> None:
        if not self._connected:
            raise RuntimeError('MoQ client is not connected.')
        if self.role != role:
            raise RuntimeError(f'MoQ client role={self.role} cannot perform {role} operation.')

    def _require_moq_cli(self) -> str:
        if self._moq_cli_bin is None:
            self._moq_cli_bin = resolve_moq_cli(self.moq_cli_path)
        return self._moq_cli_bin

    def _verify_object_mode(self, moq_cli_bin: str) -> None:
        checks = [
            ([moq_cli_bin, 'publish', '--help'], 'object'),
            ([moq_cli_bin, 'subscribe', '--help'], 'object'),
            ([moq_cli_bin, 'subscribe', '--help'], '--start-group'),
            ([moq_cli_bin, 'subscribe', '--help'], '--end-object'),
            ([moq_cli_bin, 'fetch', '--help'], 'object'),
            ([moq_cli_bin, 'fetch', '--help'], '--idle-timeout-ms'),
        ]
        for command, needle in checks:
            completed = subprocess.run(
                command,
                check=False,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                timeout=5.0,
            )
            if completed.returncode != 0 or needle not in completed.stdout:
                raise ProcessStartError(
                    'moq-cli does not support ACN object mode yet. '
                    'Required CLI contract: publish ... object and '
                    'subscribe ... --output object --track <track>.'
                )

    @staticmethod
    def _default_verify_cli() -> bool:
        value = os.environ.get('ACN_MOQ_RUST_VERIFY_CLI', 'true').strip().lower()
        return value not in ('0', 'false', 'no', 'off')

    @staticmethod
    def _default_received_cache_size() -> int:
        value = os.environ.get('ACN_MOQ_RUST_RECEIVED_CACHE_SIZE', '1000').strip()
        try:
            return max(0, int(value))
        except ValueError:
            return 1000

    @staticmethod
    def _build_relay_url(host: str, remote_port: int) -> str:
        if host.startswith(('http://', 'https://', 'ws://', 'wss://')):
            return host
        return f'http://{host}:{remote_port}/'

    @staticmethod
    def _broadcast_name(namespace: str) -> str:
        normalized = namespace.strip('/')
        return normalized or '_root'

    @staticmethod
    def _track_key(namespace: str, track: str) -> str:
        return f'{namespace}::{track}'


def write_object_frame(
    stream: BinaryIO,
    op: int,
    track: str,
    group_id: int,
    object_id: int,
    payload: bytes,
) -> None:
    track_bytes = track.encode('utf-8')
    if len(track_bytes) > 65535:
        raise ValueError('track name is too long for object frame')
    header = OBJECT_FRAME_HEADER.pack(
        OBJECT_FRAME_MAGIC,
        OBJECT_FRAME_VERSION,
        op,
        len(track_bytes),
        group_id,
        object_id,
        len(payload),
    )
    stream.write(header)
    stream.write(track_bytes)
    stream.write(payload)


def read_object_frame(stream: BinaryIO) -> dict[str, object]:
    header = stream.read(OBJECT_FRAME_HEADER.size)
    if not header:
        raise EOFError
    if len(header) != OBJECT_FRAME_HEADER.size:
        raise EOFError('truncated object frame header')
    magic, version, op, track_len, group_id, object_id, payload_len = OBJECT_FRAME_HEADER.unpack(header)
    if magic != OBJECT_FRAME_MAGIC:
        raise ValueError('invalid object frame magic')
    if version != OBJECT_FRAME_VERSION:
        raise ValueError(f'unsupported object frame version: {version}')
    track_bytes = stream.read(track_len)
    if len(track_bytes) != track_len:
        raise EOFError('truncated object frame track')
    payload = stream.read(payload_len)
    if len(payload) != payload_len:
        raise EOFError('truncated object frame payload')
    return {
        'op': op,
        'track': track_bytes.decode('utf-8'),
        'group_id': group_id,
        'object_id': object_id,
        'payload': payload,
    }
