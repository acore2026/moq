import errno
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from _path_helper import ensure_repo_root

ensure_repo_root()

from examples.video_webtransport_subscriber_example import BrowserPageServer


@pytest.mark.asyncio
async def test_browser_page_server_falls_back_when_port_is_in_use(monkeypatch):
    preferred_port = 8080
    fallback_port = 18080
    fake_server = AsyncMock()
    fake_server.close = Mock()
    fake_server.sockets = [SimpleNamespace(getsockname=lambda: ("127.0.0.1", fallback_port))]

    start_server = AsyncMock(
        side_effect=[
            OSError(errno.EADDRINUSE, "address already in use"),
            fake_server,
        ]
    )
    monkeypatch.setattr("examples.video_webtransport_subscriber_example.asyncio.start_server", start_server)

    page_server = BrowserPageServer(b"<html></html>", port=preferred_port)

    await page_server.start()
    assert page_server.port == fallback_port
    assert start_server.await_args_list[0].args[1:] == ("127.0.0.1", preferred_port)
    assert start_server.await_args_list[1].args[1:] == ("127.0.0.1", 0)

    await page_server.stop()
    fake_server.close.assert_called_once()
    fake_server.wait_closed.assert_awaited_once()
