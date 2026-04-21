"""
MOQ Transport - Subscriber Implementation
Subscriber for MOQT protocol.
"""

import asyncio
import logging
from typing import Optional, Callable, Dict, List
from dataclasses import dataclass

from moq.session import MOQSession, Role, Subscription
from moq.messages import (
    SubscribeMessage, SubscribeOkMessage, RequestErrorMessage,
    ObjectHeader, ObjectDatagram, SubgroupHeader, SubgroupObject,
    FetchMessage, FetchOkMessage,
    ObjectStatus, StreamType, ErrorCode
)
from moq.encoding import FullTrackName, Location, VarInt
from moq.transport import QUICClient, WebTransportClient, StreamData, DatagramData

logger = logging.getLogger(__name__)


@dataclass
class ReceivedObject:
    """Object received from subscription."""
    track_alias: int
    group_id: int
    object_id: int
    publisher_priority: int
    payload: bytes
    object_status: ObjectStatus = ObjectStatus.NORMAL


class MOQSubscriber:
    """
    MOQ Subscriber.
    Subscribes to tracks from a relay or publisher.
    """
    
    def __init__(
        self,
        relay_host: str,
        relay_port: int,
        transport: str = "quic",
        webtransport_path: str = "/moq",
    ):
        self.relay_host = relay_host
        self.relay_port = relay_port
        self.transport = transport
        self.webtransport_path = webtransport_path
        
        # Connection
        self._client = None
        self._session: Optional[MOQSession] = None
        self._local_control_stream_id: Optional[int] = None
        self._peer_control_stream_id: Optional[int] = None
        
        # Subscriptions
        self._subscriptions: Dict[FullTrackName, int] = {}  # track -> request_id
        self._active_subscriptions: Dict[int, FullTrackName] = {}  # request_id -> track
        
        # Track alias mapping
        self._track_aliases: Dict[int, FullTrackName] = {}  # track_alias -> track
        
        # Handlers
        self._on_connected: Optional[Callable] = None
        self._on_disconnected: Optional[Callable] = None
        self._on_object_received: Optional[Callable[[ReceivedObject], None]] = None
        self._on_subscription_accepted: Optional[Callable[[FullTrackName], None]] = None
        self._on_subscription_rejected: Optional[Callable[[FullTrackName, str], None]] = None
        
        # Object delivery queue
        self._object_queue: asyncio.Queue = asyncio.Queue()
        self._control_buffer = b""
        self._data_stream_buffers: Dict[int, bytearray] = {}
        self._data_stream_types: Dict[int, int] = {}
        self._subgroup_headers: Dict[int, SubgroupHeader] = {}
        self._fetch_headers: Dict[int, object] = {}
        
        logger.info(f"MOQSubscriber initialized for {relay_host}:{relay_port}")
    
    def set_handlers(self,
                     on_connected: Optional[Callable] = None,
                     on_disconnected: Optional[Callable] = None,
                     on_object_received: Optional[Callable[[ReceivedObject], None]] = None,
                     on_subscription_accepted: Optional[Callable[[FullTrackName], None]] = None,
                     on_subscription_rejected: Optional[Callable[[FullTrackName, str], None]] = None):
        """Set event handlers."""
        self._on_connected = on_connected
        self._on_disconnected = on_disconnected
        self._on_object_received = on_object_received
        self._on_subscription_accepted = on_subscription_accepted
        self._on_subscription_rejected = on_subscription_rejected
    
    async def connect(self) -> bool:
        """Connect to relay."""
        logger.info(f"Connecting to relay at {self.relay_host}:{self.relay_port}")
        
        try:
            # Create transport client
            self._client = self._create_transport_client()
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
                session_id=f"sub-{id(self)}",
                role=Role.SUBSCRIBER
            )
            self._session.set_send_callback(self._send_data)
            self._local_control_stream_id = await self._client.open_stream(unidirectional=True)
            
            # Send SETUP
            await self._session.send_setup(Role.SUBSCRIBER)
            
            logger.info("Connected to relay")
            
            if self._on_connected:
                self._on_connected()
            
            # Start object processing task
            asyncio.create_task(self._process_objects())
            
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
        self._local_control_stream_id = None
        self._peer_control_stream_id = None
        
        if self._client:
            self._client.close()
            self._client = None
        
        if self._on_disconnected:
            self._on_disconnected()
    
    async def subscribe(self, track_name: FullTrackName,
                       subscriber_priority: int = 128,
                       start_group: Optional[int] = None,
                       start_object: Optional[int] = None) -> bool:
        """
        Subscribe to a track.
        
        Args:
            track_name: Full track name to subscribe to
            subscriber_priority: Priority for this subscription (0-255)
            start_group: Starting group ID (None for latest)
            start_object: Starting object ID (None for latest)
            
        Returns:
            True if subscription initiated successfully
        """
        if not self._session:
            logger.error("Not connected")
            return False
        
        if track_name in self._subscriptions:
            logger.warning(f"Already subscribed to track: {track_name}")
            return False
        
        logger.info(f"Subscribing to track: {track_name}")
        
        from moq.messages import SubscribeFilter, GroupOrder
        
        # Determine filter type
        if start_group is None:
            filter_type = SubscribeFilter.LATEST_OBJECT
        else:
            filter_type = SubscribeFilter.ABSOLUTE_START
        
        # Send SUBSCRIBE message
        request_id = await self._session.subscribe(
            track_name=track_name,
            subscriber_priority=subscriber_priority,
            group_order=GroupOrder.ASCENDING,
            filter_type=filter_type,
            start_group=start_group,
            start_object=start_object
        )
        
        self._subscriptions[track_name] = request_id
        
        return True
    
    async def unsubscribe(self, track_name: FullTrackName):
        """Unsubscribe from a track."""
        if track_name not in self._subscriptions:
            logger.warning(f"Not subscribed to track: {track_name}")
            return
        
        request_id = self._subscriptions[track_name]
        
        logger.info(f"Unsubscribing from track: {track_name}")
        
        # TODO: Send unsubscribe message when available in protocol
        
        if request_id in self._active_subscriptions:
            del self._active_subscriptions[request_id]
        
        del self._subscriptions[track_name]
    
    async def fetch(self, track_name: FullTrackName,
                   start_group: int = 0, start_object: int = 0,
                   end_group: Optional[int] = None, end_object: Optional[int] = None,
                   subscriber_priority: int = 128) -> int:
        """
        Fetch specific objects from a track.
        
        Args:
            track_name: The track to fetch from
            start_group: Starting group ID (defaults to 0)
            start_object: Starting object ID (defaults to 0)
            end_group: Ending group ID (None means fetch until latest)
            end_object: Ending object ID (None means fetch until latest)
            subscriber_priority: Priority level (0-255)
        
        Returns:
            Request ID of the fetch
        """
        if not self._session:
            logger.error("Not connected")
            return -1
        
        logger.info(f"Fetching from track: {track_name}, range=[{start_group}:{start_object} to {end_group}:{end_object}]")
        
        request_id = await self._session.fetch(
            track_name=track_name,
            start_group=start_group,
            start_object=start_object,
            end_group=end_group,
            end_object=end_object,
            subscriber_priority=subscriber_priority
        )
        
        return request_id
    
    async def _send_data(self, data: bytes):
        """Send data over the local control stream."""
        if self._client and self._local_control_stream_id is not None:
            await self._client.send_stream_data(self._local_control_stream_id, data)

    async def _handle_stream_data(self, protocol, data: StreamData):
        """Handle incoming stream data."""
        logger.debug(f"Received stream data: stream_id={data.stream_id}, length={len(data.data)}")

        if self._peer_control_stream_id is None:
            self._peer_control_stream_id = data.stream_id
            await self._handle_control_data(data.data, end_stream=data.end_stream)
            return

        if data.stream_id == self._peer_control_stream_id:
            # Control stream
            await self._handle_control_data(data.data, end_stream=data.end_stream)
            return

        handled_as_data = await self._handle_data_stream(
            data.stream_id,
            data.data,
            end_stream=data.end_stream,
        )
        if handled_as_data:
            return

    async def _handle_control_data(self, data: bytes, end_stream: bool = False):
        """Handle control message data."""
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

            if isinstance(msg, SubscribeOkMessage):
                self._handle_subscribe_ok(msg)
            elif isinstance(msg, RequestErrorMessage):
                self._handle_request_error(msg)
            elif isinstance(msg, FetchOkMessage):
                self._handle_fetch_ok(msg)

            self._control_buffer = self._control_buffer[consumed:]
    
    def _handle_subscribe_ok(self, msg: SubscribeOkMessage):
        """Handle SUBSCRIBE_OK message."""
        logger.info(f"Subscription accepted: request_id={msg.request_id}")
        
        if self._session:
            self._session.handle_subscribe_ok(msg)
            
            subscription = self._session.get_subscription(msg.request_id)
            if subscription:
                track_name = subscription.full_track_name
                self._active_subscriptions[msg.request_id] = track_name
                self._track_aliases[subscription.track_alias] = track_name
                
                if self._on_subscription_accepted:
                    self._on_subscription_accepted(track_name)
    
    def _handle_request_error(self, msg: RequestErrorMessage):
        """Handle REQUEST_ERROR message."""
        logger.warning(f"Request error: request_id={msg.request_id}, code={msg.error_code}, reason={msg.reason}")
        
        if self._session:
            self._session.handle_request_error(msg)
            
            subscription = self._session.get_subscription(msg.request_id)
            if subscription:
                track_name = subscription.full_track_name
                
                if track_name in self._subscriptions:
                    del self._subscriptions[track_name]
                
                if self._on_subscription_rejected:
                    self._on_subscription_rejected(track_name, msg.reason)
    
    def _handle_fetch_ok(self, msg: FetchOkMessage):
        """Handle FETCH_OK message."""
        logger.info(f"Fetch accepted: request_id={msg.request_id}")

        if self._session:
            fetch_request = self._session.fetches.get(msg.request_id)
            if fetch_request:
                track_name = fetch_request.full_track_name
                track_alias = self._session.track_aliases.get(track_name)
                if track_alias is not None:
                    self._track_aliases[track_alias] = track_name
    
    async def _handle_data_stream(self, stream_id: int, data: bytes, end_stream: bool = False) -> bool:
        """Handle data from a data stream."""
        buffer = self._data_stream_buffers.setdefault(stream_id, bytearray())
        buffer.extend(data)

        try:
            if stream_id not in self._data_stream_types:
                stream_type, consumed = VarInt.decode(buffer, 0)
                self._data_stream_types[stream_id] = stream_type
                del buffer[:consumed]

            stream_type = self._data_stream_types[stream_id]

            if stream_type == StreamType.SUBGROUP_HEADER:
                await self._handle_subgroup_stream(stream_id, buffer)
            elif stream_type == StreamType.FETCH_HEADER:
                await self._handle_fetch_stream(stream_id, buffer)
            else:
                logger.warning(f"Unknown stream type: {stream_type}")
                self._cleanup_data_stream(stream_id)
                return False
        except Exception as e:
            if end_stream:
                logger.warning(f"Failed to handle data stream: {e}")
                self._cleanup_data_stream(stream_id)
            return False

        if end_stream:
            if buffer:
                logger.warning(
                    f"Data stream {stream_id} ended with {len(buffer)} buffered bytes"
                )
            self._cleanup_data_stream(stream_id)

        return True

    async def _handle_subgroup_stream(self, stream_id: int, buffer: bytearray):
        """Handle subgroup stream data."""
        try:
            if stream_id not in self._subgroup_headers:
                header, consumed = SubgroupHeader.decode(buffer, 0)
                self._subgroup_headers[stream_id] = header
                del buffer[:consumed]

            header = self._subgroup_headers[stream_id]

            track_name = self._track_aliases.get(header.track_alias)
            if not track_name:
                logger.warning(f"Unknown track alias: {header.track_alias}")
                return

            while buffer:
                try:
                    obj, consumed = SubgroupObject.decode(buffer, 0)
                    del buffer[:consumed]
                except ValueError:
                    break
                except Exception as e:
                    logger.debug(f"Failed to parse subgroup object: {e}")
                    break

                received_obj = ReceivedObject(
                    track_alias=header.track_alias,
                    group_id=header.group_id,
                    object_id=obj.object_id,
                    publisher_priority=header.publisher_priority,
                    payload=obj.payload,
                    object_status=obj.object_status
                )

                await self._object_queue.put(received_obj)
        except Exception as e:
            logger.warning(f"Failed to handle subgroup stream: {e}")
    
    async def _handle_fetch_stream(self, stream_id: int, buffer: bytearray):
        """Handle fetch stream data."""
        try:
            from moq.messages import FetchHeader, ObjectHeader
            
            if stream_id not in self._fetch_headers:
                header, consumed = FetchHeader.decode(buffer, 0)
                self._fetch_headers[stream_id] = header
                del buffer[:consumed]
            else:
                header = self._fetch_headers[stream_id]

            request_id = header.subscribe_id
            logger.debug(f"Fetch stream header: request_id={request_id}")
            
            # Get fetch request info
            if not self._session:
                logger.warning("No session available for fetch stream")
                return
                
            fetch_request = self._session.fetches.get(request_id)
            if not fetch_request:
                logger.warning(f"Unknown fetch request: {request_id}")
                return
            
            track_name = fetch_request.full_track_name
            track_alias = self._session.track_aliases.get(track_name, 0)
            
            # Parse objects in the fetch stream
            while buffer:
                try:
                    # Parse object header
                    obj_header, consumed = ObjectHeader.decode(buffer, 0)
                    payload_offset = consumed
                    
                    # Read payload
                    payload_len, consumed = VarInt.decode(buffer, payload_offset)
                    payload_offset += consumed
                    
                    if payload_offset + payload_len > len(buffer):
                        break
                    
                    payload = bytes(buffer[payload_offset:payload_offset + payload_len])
                    del buffer[:payload_offset + payload_len]
                    
                    # Create received object
                    received_obj = ReceivedObject(
                        track_alias=track_alias,
                        group_id=obj_header.group_id,
                        object_id=obj_header.object_id,
                        publisher_priority=obj_header.publisher_priority,
                        payload=payload,
                        object_status=obj_header.object_status
                    )
                    
                    await self._object_queue.put(received_obj)
                    logger.debug(f"Fetch object received: group={obj_header.group_id}, object={obj_header.object_id}")
                    
                except ValueError:
                    break
                except Exception as e:
                    logger.debug(f"Failed to parse fetch stream object: {e}")
                    break
                    
        except Exception as e:
            logger.warning(f"Failed to handle fetch stream: {e}")

    def _cleanup_data_stream(self, stream_id: int):
        """Release parser state for a completed or failed data stream."""
        self._data_stream_buffers.pop(stream_id, None)
        self._data_stream_types.pop(stream_id, None)
        self._subgroup_headers.pop(stream_id, None)
        self._fetch_headers.pop(stream_id, None)
    
    async def _handle_datagram(self, protocol, data: DatagramData):
        """Handle incoming datagram."""
        try:
            datagram, _ = ObjectDatagram.decode(data.data)
            
            track_name = self._track_aliases.get(datagram.header.track_alias)
            if not track_name:
                logger.warning(f"Unknown track alias: {datagram.header.track_alias}")
                return
            
            received_obj = ReceivedObject(
                track_alias=datagram.header.track_alias,
                group_id=datagram.header.group_id,
                object_id=datagram.header.object_id,
                publisher_priority=datagram.header.publisher_priority,
                payload=datagram.payload,
                object_status=datagram.header.object_status
            )
            
            await self._object_queue.put(received_obj)
            
        except Exception as e:
            logger.warning(f"Failed to handle datagram: {e}")
    
    async def _handle_close(self, protocol, error_code: int, reason: str):
        """Handle connection close."""
        logger.info(f"Connection closed: error={error_code}, reason={reason}")
        self.disconnect()

    def _create_transport_client(self):
        """Instantiate the configured transport client."""
        transport = self.transport.lower()
        if transport == "quic":
            return QUICClient(self.relay_host, self.relay_port)
        if transport == "webtransport":
            return WebTransportClient(
                self.relay_host,
                self.relay_port,
                path=self.webtransport_path,
            )
        raise ValueError(f"Unsupported transport: {self.transport}")
    
    async def _process_objects(self):
        """Process received objects from queue."""
        while True:
            try:
                obj = await self._object_queue.get()
                
                if self._on_object_received:
                    self._on_object_received(obj)
                    
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error processing object: {e}")
    
    def get_active_subscriptions(self) -> List[FullTrackName]:
        """Get list of currently subscribed tracks."""
        return list(self._subscriptions.keys())
    
    def is_subscribed(self, track_name: FullTrackName) -> bool:
        """Check if subscribed to track."""
        return track_name in self._subscriptions
    
    async def get_next_object(self, timeout: Optional[float] = None) -> Optional[ReceivedObject]:
        """Get next received object from queue."""
        try:
            return await asyncio.wait_for(self._object_queue.get(), timeout=timeout)
        except asyncio.TimeoutError:
            return None
