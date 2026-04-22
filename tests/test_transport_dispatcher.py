import asyncio
import logging

import pytest

from _path_helper import ensure_repo_root

ensure_repo_root()

from moq.transport.combined_transport import CombinedTransportServer
from moq.transport.quic_transport import (
    DEFAULT_QUIC_IDLE_TIMEOUT,
    _KeepAliveProtocolMixin,
    _OrderedCallbackDispatcher,
)
from moq.transport.webtransport import WebTransportClient, WebTransportServer


@pytest.mark.asyncio
async def test_ordered_callback_dispatcher_preserves_enqueue_order():
    dispatcher = _OrderedCallbackDispatcher(logging.getLogger(__name__))
    started: list[int] = []
    finished: list[int] = []
    gate = asyncio.Event()

    async def first(value: int):
        started.append(value)
        await gate.wait()
        finished.append(value)

    async def second(value: int):
        started.append(value)
        finished.append(value)

    dispatcher.enqueue(first, 1)
    dispatcher.enqueue(second, 2)
    await asyncio.sleep(0)

    assert started == [1]
    assert finished == []

    gate.set()
    await asyncio.sleep(0)
    await asyncio.sleep(0)

    assert started == [1, 2]
    assert finished == [1, 2]


@pytest.mark.asyncio
async def test_ordered_callback_dispatcher_continues_after_callback_error():
    dispatcher = _OrderedCallbackDispatcher(logging.getLogger(__name__))
    observed: list[str] = []

    async def failing():
        observed.append("fail")
        raise RuntimeError("boom")

    async def succeeding():
        observed.append("ok")

    dispatcher.enqueue(failing)
    dispatcher.enqueue(succeeding)
    await asyncio.sleep(0)
    await asyncio.sleep(0)

    assert observed == ["fail", "ok"]


@pytest.mark.asyncio
async def test_keepalive_mixin_sends_ping_and_stops_cleanly():
    class FakeQuic:
        def __init__(self):
            self.pings: list[int] = []

        def send_ping(self, uid: int) -> None:
            self.pings.append(uid)

    class FakeProtocol(_KeepAliveProtocolMixin):
        def __init__(self):
            self._quic = FakeQuic()
            self.transmits = 0
            self._init_keepalive(logging.getLogger(__name__), interval=0.01)

        def transmit(self) -> None:
            self.transmits += 1

    protocol = FakeProtocol()
    protocol._start_keepalive()
    await asyncio.sleep(0.035)
    protocol._stop_keepalive()
    await asyncio.sleep(0)

    ping_count = len(protocol._quic.pings)
    assert ping_count >= 2
    assert protocol.transmits == ping_count

    await asyncio.sleep(0.02)
    assert len(protocol._quic.pings) == ping_count


def test_transports_use_extended_idle_timeout():
    assert WebTransportClient("127.0.0.1", 4443)._config.idle_timeout == DEFAULT_QUIC_IDLE_TIMEOUT
    assert WebTransportServer("127.0.0.1", 4443)._config.idle_timeout == DEFAULT_QUIC_IDLE_TIMEOUT
    assert CombinedTransportServer("127.0.0.1", 4443)._config.idle_timeout == DEFAULT_QUIC_IDLE_TIMEOUT
