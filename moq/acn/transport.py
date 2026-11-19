"""ACN-only QUIC protocol with serialized bounded callbacks and write budgets."""

import asyncio
import logging

from aioquic.asyncio import QuicConnectionProtocol
from aioquic.quic.events import ConnectionTerminated, DatagramFrameReceived, StreamDataReceived, StreamReset, StopSendingReceived

from moq.transport.quic_transport import QUICServer

logger = logging.getLogger(__name__)


class AcnProtocol(QuicConnectionProtocol):
    def __init__(self, *args, owner, **kwargs):
        super().__init__(*args, **kwargs)
        self.owner = owner
        self.events = asyncio.Queue(maxsize=256)
        self.received_bytes = 0
        self.worker = asyncio.create_task(self._run())
        self.terminated = False

    def quic_event_received(self, event):
        if not isinstance(event, (ConnectionTerminated, DatagramFrameReceived, StreamDataReceived, StreamReset, StopSendingReceived)):
            return
        size = len(getattr(event, "data", b""))
        if self.events.full() or self.received_bytes + size > 1048576:
            self.close(error_code=2, reason_phrase="ACN receive budget exceeded")
            self.worker.cancel()
            self.owner.disconnected(self)
            return
        self.received_bytes += size
        self.events.put_nowait(event)

    async def _run(self):
        try:
            while True:
                event = await self.events.get()
                self.received_bytes -= len(getattr(event, "data", b""))
                if isinstance(event, ConnectionTerminated):
                    self.terminated = True
                    self.owner.disconnected(self, event.error_code)
                    return
                await self.owner.event(self, event)
        except asyncio.CancelledError:
            pass
        except Exception:
            logger.exception("[ACN-L5] protocol processing failed")
            self.close(error_code=2, reason_phrase="ACN protocol error")
            self.owner.disconnected(self)

    async def write(self, stream_id, data, timeout, end=False):
        """Wait for local QUIC send progress, never claim remote application ACK."""
        if self.terminated:
            raise ConnectionError("QUIC connection terminated")
        self._quic.send_stream_data(stream_id, data, end_stream=end)
        self.transmit()

        async def drain():
            # The pinned aioquic sender retains unacknowledged payloads. Keep
            # the application budget charged until this transport releases it.
            while not self.terminated:
                stream = self._quic._streams.get(stream_id)
                if stream is None or not stream.sender._buffer:
                    return
                await asyncio.sleep(.01)
            raise ConnectionError("QUIC connection terminated during write")

        await asyncio.wait_for(drain(), timeout)


class AcnServer(QUICServer):
    def __init__(self, owner, host, port, cert_file=None, key_file=None):
        self.owner = owner
        super().__init__(host, port, cert_file=cert_file, key_file=key_file)

    def _create_protocol(self, *args, **kwargs):
        return AcnProtocol(*args, owner=self.owner, **kwargs)
