"""
MOQ Transport - Publisher Implementation
Publisher for MOQT protocol.
"""

import asyncio
import logging
from typing import Optional, Callable, Dict, List
from dataclasses import dataclass

from moq.session import MOQSession, Role, Publication
from moq.messages import (
    PublishMessage, PublishOkMessage, PublishDoneMessage,
    ObjectHeader, ObjectDatagram, SubgroupHeader, SubgroupObject,
    ObjectStatus, StreamType
)
from moq.encoding import FullTrackName, VarInt
from moq.transport import QUICClient, StreamData, DatagramData

logger = logging.getLogger(__name__)


@dataclass
class PublishedObject:
    """Object ready for publishing."""
    group_id: int
    object_id: int
    payload: bytes
    publisher_priority: int = 128
    subgroup_id: int = 0
    use_datagram: bool = False


class MOQPublisher:
    """
    MOQ Publisher.
    Publishes tracks to a relay or subscriber.
    """
    
    def __init__(self, relay_host: str, relay_port: int):
        self.relay_host = relay_host
        self.relay_port = relay_port
        
        # Connection
        self._client: Optional[QUICClient] = None
        self._session: Optional[MOQSession] = None
        
        # Publications
        self._publications: Dict[FullTrackName, int] = {}  # track -> request_id
        self._active_tracks: Dict[int, FullTrackName] = {}  # request_id -> track
        self._publish_waiters: Dict[int, asyncio.Event] = {}
        self._control_buffer = b""
        
        # Stream management
        self._streams: Dict[int, int] = {}  # track_alias -> stream_id
        
        # Handlers
        self._on_connected: Optional[Callable] = None
        self._on_disconnected: Optional[Callable] = None
        self._on_publication_accepted: Optional[Callable[[FullTrackName], None]] = None
        self._on_publication_rejected: Optional[Callable[[FullTrackName, str], None]] = None
        
        logger.info(f"MOQPublisher initialized for {relay_host}:{relay_port}")
    
    def set_handlers(self,
                     on_connected: Optional[Callable] = None,
                     on_disconnected: Optional[Callable] = None,
                     on_publication_accepted: Optional[Callable[[FullTrackName], None]] = None,
                     on_publication_rejected: Optional[Callable[[FullTrackName, str], None]] = None):
        """Set event handlers."""
        self._on_connected = on_connected
        self._on_disconnected = on_disconnected
        self._on_publication_accepted = on_publication_accepted
        self._on_publication_rejected = on_publication_rejected
    
    async def connect(self) -> bool:
        """Connect to relay."""
        logger.info(f"Connecting to relay at {self.relay_host}:{self.relay_port}")
        
        try:
            # Create QUIC client
            self._client = QUICClient(self.relay_host, self.relay_port)
            self._client.set_handlers(
                on_stream_data=self._handle_stream_data,
                on_datagram=self._handle_datagram,
                on_close=self._handle_close
            )
            
            connected = await self._client.connect()
            if not connected:
                logger.error("Failed to connect to relay")
                return False
            
            # Create MOQ session
            self._session = MOQSession(
                session_id=f"pub-{id(self)}",
                role=Role.PUBLISHER
            )
            self._session.set_send_callback(self._send_data)
            self._session.set_handlers(
                on_publish=self._handle_publish_response
            )
            
            # Send SETUP
            await self._session.send_setup(Role.PUBLISHER)
            
            logger.info("Connected to relay")
            
            if self._on_connected:
                self._on_connected()
            
            return True
            
        except Exception as e:
            logger.error(f"Error connecting to relay: {e}")
            return False
    
    def disconnect(self):
        """Disconnect from relay."""
        logger.info("Disconnecting from relay")
        
        if self._session:
            self._session.close()
            self._session = None
        
        if self._client:
            self._client.close()
            self._client = None
        
        if self._on_disconnected:
            self._on_disconnected()
    
    async def publish(self, track_name: FullTrackName) -> bool:
        """
        Publish a track.
        
        Args:
            track_name: Full track name to publish
            
        Returns:
            True if publication initiated successfully
        """
        if not self._session:
            logger.error("Not connected")
            return False
        
        if track_name in self._publications:
            logger.warning(f"Already publishing track: {track_name}")
            return False
        
        logger.info(f"Publishing track: {track_name}")

        # Generate request ID and create waiter BEFORE sending the message
        # This prevents a race condition where the response arrives before the waiter is created
        request_id = self._session.get_next_request_id()
        self._publish_waiters[request_id] = asyncio.Event()

        try:
            # Send PUBLISH message
            await self._session.publish(track_name, request_id=request_id)
            self._publications[track_name] = request_id
            self._active_tracks[request_id] = track_name

            await asyncio.wait_for(self._publish_waiters[request_id].wait(), timeout=5.0)
        except asyncio.TimeoutError:
            logger.warning(f"Timed out waiting for PUBLISH_OK: {track_name}")
            return False
        finally:
            self._publish_waiters.pop(request_id, None)

        return True
    
    async def unpublish(self, track_name: FullTrackName, reason: str = ""):
        """Unpublish a track."""
        if track_name not in self._publications:
            logger.warning(f"Not publishing track: {track_name}")
            return
        
        request_id = self._publications[track_name]
        
        logger.info(f"Unpublishing track: {track_name}")
        
        # Send PUBLISH_DONE
        await self._session.send_publish_done(request_id, 0, reason)
        
        del self._publications[track_name]
        del self._active_tracks[request_id]
    
    async def send_object(self, track_name: FullTrackName, obj: PublishedObject):
        """
        Send an object on a published track.
        
        Args:
            track_name: Track to send on
            obj: Object to send
        """
        if track_name not in self._publications:
            logger.error(f"Not publishing track: {track_name}")
            return
        
        if not self._session:
            logger.error("Not connected")
            return
        
        request_id = self._publications[track_name]
        publication = self._session.get_publication(request_id)
        
        if not publication or not publication.active:
            logger.warning(f"Publication not active: {track_name}")
            return
        
        track_alias = publication.track_alias
        
        if obj.use_datagram:
            # Send as datagram
            await self._send_datagram_object(track_alias, obj)
        else:
            # Send on stream
            await self._send_stream_object(track_alias, obj)
    
    async def _send_datagram_object(self, track_alias: int, obj: PublishedObject):
        """Send object as datagram."""
        header = ObjectHeader(
            track_alias=track_alias,
            group_id=obj.group_id,
            object_id=obj.object_id,
            publisher_priority=obj.publisher_priority
        )
        
        datagram = ObjectDatagram(
            header=header,
            payload=obj.payload
        )
        
        data = datagram.encode()
        await self._client.send_datagram(data)
        
        logger.debug(f"Sent datagram object: group={obj.group_id}, object={obj.object_id}")
    
    async def _send_stream_object(self, track_alias: int, obj: PublishedObject):
        """Send object on subgroup stream."""
        # Get or create stream for this subgroup
        stream_key = (track_alias, obj.group_id, obj.subgroup_id)
        
        if stream_key not in self._streams:
            # Open new unidirectional stream
            stream_id = await self._client.open_stream(unidirectional=True)
            self._streams[stream_key] = stream_id
            
            # Send subgroup header
            subgroup_header = SubgroupHeader(
                track_alias=track_alias,
                group_id=obj.group_id,
                subgroup_id=obj.subgroup_id,
                publisher_priority=obj.publisher_priority
            )
            
            header_data = VarInt.encode(StreamType.SUBGROUP_HEADER) + subgroup_header.encode()
            await self._client.send_stream_data(stream_id, header_data)
        
        stream_id = self._streams[stream_key]
        
        # Send object
        subgroup_obj = SubgroupObject(
            object_id=obj.object_id,
            payload=obj.payload
        )
        
        await self._client.send_stream_data(stream_id, subgroup_obj.encode())
        
        logger.debug(f"Sent stream object: group={obj.group_id}, object={obj.object_id}")
    
    async def close_subgroup_stream(self, track_alias: int, group_id: int, subgroup_id: int):
        """Close a subgroup stream."""
        stream_key = (track_alias, group_id, subgroup_id)
        
        if stream_key in self._streams:
            stream_id = self._streams[stream_key]
            
            # Send END_OF_SUBGROUP
            end_obj = SubgroupObject(
                object_id=0,  # Will be ignored
                payload=b'',
                object_status=ObjectStatus.END_OF_SUBGROUP
            )
            await self._client.send_stream_data(stream_id, end_obj.encode(), end_stream=True)
            
            del self._streams[stream_key]
            logger.debug(f"Closed subgroup stream: track={track_alias}, group={group_id}, subgroup={subgroup_id}")
    
    async def _send_data(self, data: bytes):
        """Send data over QUIC control stream."""
        if self._client:
            # Use stream 0 for control messages
            await self._client.send_stream_data(0, data)
    
    async def _handle_stream_data(self, protocol, data: StreamData):
        """Handle incoming stream data."""
        logger.debug(f"Received stream data: stream_id={data.stream_id}, length={len(data.data)}")
        
        # Parse and handle control messages
        if self._session and data.stream_id == 0:
            await self._handle_control_data(data.data, end_stream=data.end_stream)

    async def _handle_control_data(self, data: bytes, end_stream: bool = False):
        """Handle buffered control stream data."""
        self._control_buffer += data

        while self._control_buffer:
            try:
                from moq.messages import decode_control_message

                msg, consumed = decode_control_message(self._control_buffer)
            except Exception as e:
                if end_stream:
                    logger.warning(f"Failed to decode control message: {e}")
                    self._control_buffer = b""
                break

            self._control_buffer = self._control_buffer[consumed:]

            if isinstance(msg, PublishOkMessage):
                self._session.handle_publish_ok(msg)
                waiter = self._publish_waiters.get(msg.request_id)
                if waiter:
                    waiter.set()
                track_name = self._active_tracks.get(msg.request_id)
                if track_name and self._on_publication_accepted:
                    self._on_publication_accepted(track_name)
            elif isinstance(msg, PublishDoneMessage):
                self._session.handle_publish_done(msg)
    
    async def _handle_datagram(self, protocol, data: DatagramData):
        """Handle incoming datagram."""
        logger.debug(f"Received datagram: length={len(data.data)}")
    
    async def _handle_close(self, protocol, error_code: int, reason: str):
        """Handle connection close."""
        logger.info(f"Connection closed: error={error_code}, reason={reason}")
        self.disconnect()
    
    def _handle_publish_response(self, msg: PublishMessage):
        """Handle publish response."""
        pass  # Will be handled via control messages
    
    def get_active_tracks(self) -> List[FullTrackName]:
        """Get list of currently publishing tracks."""
        return list(self._publications.keys())
    
    def is_publishing(self, track_name: FullTrackName) -> bool:
        """Check if track is being published."""
        return track_name in self._publications
