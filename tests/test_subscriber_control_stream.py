from unittest.mock import AsyncMock

import pytest

from _path_helper import ensure_repo_root

ensure_repo_root()

from moq.sub.subscriber import MOQSubscriber
from moq.transport.quic_transport import StreamData


@pytest.mark.asyncio
async def test_first_peer_stream_is_treated_as_control_stream():
    subscriber = MOQSubscriber("127.0.0.1", 4443)
    subscriber._handle_control_data = AsyncMock()
    subscriber._handle_data_stream = AsyncMock()

    stream_data = StreamData(stream_id=3, data=b"\x05\x01\x00", end_stream=False)
    await subscriber._handle_stream_data(None, stream_data)

    assert subscriber._peer_control_stream_id == 3
    subscriber._handle_control_data.assert_awaited_once_with(stream_data.data, end_stream=False)
    subscriber._handle_data_stream.assert_not_called()
