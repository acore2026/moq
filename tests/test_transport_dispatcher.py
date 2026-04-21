import asyncio
import logging

import pytest

from _path_helper import ensure_repo_root

ensure_repo_root()

from moq.transport.quic_transport import _OrderedCallbackDispatcher


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
