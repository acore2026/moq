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
    SubscribeMessage, SubscribeOkMessage, RequestOkMessage, RequestErrorMessage,
    ObjectDatagram, SubgroupHeader, SubgroupObject,
    FetchMessage, FetchOkMessage,
    GroupOrder, SubscribeFilter, ObjectStatus, StreamType, ErrorCode, PublishDoneMessage,
    FetchObject, is_subgroup_stream_type, StreamResetCode, UNKNOWN_PUBLISH_DONE_STREAM_COUNT,
)
from moq.encoding import FullTrackName, Location, VarInt
from moq.transport import (
    QUICClient,
    WebTransportClient,
    StreamData,
    StreamResetData,
    DatagramData,
    is_unidirectional_stream_id,
)

logger = logging.getLogger(__name__)

STREAM_CANCELLATION_ERROR_CODE = int(StreamResetCode.CANCELLED)


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
        delivery_timeout: float = 0.5,
    ):
        self.relay_host = relay_host
        self.relay_port = relay_port
        self.transport = transport
        self.webtransport_path = webtransport_path
        self.delivery_timeout = delivery_timeout
        
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
        self._on_subscription_ended: Optional[Callable[[FullTrackName, int, str], None]] = None
        
        # Object delivery queue
        self._object_queue: asyncio.Queue = asyncio.Queue()
        self._control_buffer = b""
        self._request_stream_ids: Dict[int, int] = {}  # request_id -> stream_id
        self._request_stream_buffers: Dict[int, bytearray] = {}
        self._data_stream_buffers: Dict[int, bytearray] = {}
        self._data_stream_types: Dict[int, int] = {}
        self._subgroup_headers: Dict[int, SubgroupHeader] = {}
        self._subgroup_previous_object_ids: Dict[int, Optional[int]] = {}
        self._fetch_headers: Dict[int, object] = {}
        self._stream_subscription_requests: Dict[int, int] = {}
        self._subscription_seen_streams: Dict[int, set[int]] = {}
        self._subscription_active_streams: Dict[int, set[int]] = {}
        self._pending_subscription_ends: Dict[int, PublishDoneMessage] = {}
        self._subscription_end_timers: Dict[int, asyncio.Task] = {}
        self._pending_precontrol_streams: Dict[int, bytearray] = {}
        self._pending_precontrol_end_streams: set[int] = set()
        
        logger.info(f"MOQSubscriber initialized for {relay_host}:{relay_port}")
    
    def set_handlers(self,
                     on_connected: Optional[Callable] = None,
                     on_disconnected: Optional[Callable] = None,
                     on_object_received: Optional[Callable[[ReceivedObject], None]] = None,
                     on_subscription_accepted: Optional[Callable[[FullTrackName], None]] = None,
                     on_subscription_rejected: Optional[Callable[[FullTrackName, str], None]] = None,
                     on_subscription_ended: Optional[Callable[[FullTrackName, int, str], None]] = None):
        """Set event handlers."""
        self._on_connected = on_connected
        self._on_disconnected = on_disconnected
        self._on_object_received = on_object_received
        self._on_subscription_accepted = on_subscription_accepted
        self._on_subscription_rejected = on_subscription_rejected
        self._on_subscription_ended = on_subscription_ended
    
    async def connect(self) -> bool:
        """Connect to relay."""
        logger.info(f"Connecting to relay at {self.relay_host}:{self.relay_port}")
        
        try:
            # Create transport client
            self._client = self._create_transport_client()
            self._client.set_handlers(
                on_stream_data=self._handle_stream_data,
                on_stream_reset=self._handle_stream_reset,
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
        self._request_stream_ids = {}
        self._request_stream_buffers = {}
        self._stream_subscription_requests = {}
        self._subscription_seen_streams = {}
        self._subscription_active_streams = {}
        self._pending_subscription_ends = {}
        self._cancel_subscription_end_timers()
        
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
        
        request_stream_id = await self._client.open_stream(unidirectional=False)
        request_id = await self._session.subscribe(
            track_name=track_name,
            subscriber_priority=subscriber_priority,
            group_order=GroupOrder.ASCENDING,
            filter_type=filter_type,
            start_group=start_group,
            start_object=start_object,
            stream_id=request_stream_id,
        )
        self._subscriptions[track_name] = request_id
        self._request_stream_ids[request_id] = request_stream_id
        return True
    
    async def unsubscribe(self, track_name: FullTrackName):
        """Unsubscribe from a track."""
        if track_name not in self._subscriptions:
            logger.warning(f"Not subscribed to track: {track_name}")
            return
        
        request_id = self._subscriptions[track_name]
        subscription = self._session.get_subscription(request_id) if self._session else None
        
        logger.info(f"Unsubscribing from track: {track_name}")

        await self._cancel_subscription_streams(request_id)
        self._discard_subscription_stream_state(request_id)

        if request_id in self._active_subscriptions:
            del self._active_subscriptions[request_id]
        if subscription is not None and subscription.track_alias is not None:
            self._track_aliases.pop(subscription.track_alias, None)
        if self._session:
            self._session.subscriptions.pop(request_id, None)
        self._request_stream_ids.pop(request_id, None)
        self._pending_subscription_ends.pop(request_id, None)
        self._subscription_seen_streams.pop(request_id, None)
        self._subscription_active_streams.pop(request_id, None)
        self._cancel_subscription_end_timer(request_id)
        
        del self._subscriptions[track_name]

    async def update_subscription(
        self,
        track_name: FullTrackName,
        subscriber_priority: Optional[int] = None,
        group_order: Optional[GroupOrder] = None,
        filter_type: Optional[SubscribeFilter] = None,
        start_group: Optional[int] = None,
        start_object: Optional[int] = None,
        end_group: Optional[int] = None,
    ) -> bool:
        """Send REQUEST_UPDATE for an existing subscription."""
        if not self._session:
            logger.error("Not connected")
            return False
        if track_name not in self._subscriptions:
            logger.warning(f"Not subscribed to track: {track_name}")
            return False

        request_id = self._subscriptions[track_name]
        await self._session.update_subscription(
            request_id=request_id,
            subscriber_priority=subscriber_priority,
            group_order=group_order,
            filter_type=filter_type,
            start_group=start_group,
            start_object=start_object,
            end_group=end_group,
            stream_id=self._request_stream_ids.get(request_id),
        )
        return True
    
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
        
        request_stream_id = await self._client.open_stream(unidirectional=False)
        request_id = await self._session.fetch(
            track_name=track_name,
            start_group=start_group,
            start_object=start_object,
            end_group=end_group,
            end_object=end_object,
            subscriber_priority=subscriber_priority,
            stream_id=request_stream_id,
        )
        self._request_stream_ids[request_id] = request_stream_id
        
        return request_id
    
    async def _send_data(self, data: bytes, stream_id: Optional[int] = None):
        """Send control data over either the control stream or a request stream."""
        if not self._client:
            return
        target_stream_id = stream_id if stream_id is not None else self._local_control_stream_id
        if target_stream_id is not None:
            await self._client.send_stream_data(target_stream_id, data)

    async def _handle_stream_data(self, protocol, data: StreamData):
        """Handle incoming stream data."""
        logger.debug(f"Received stream data: stream_id={data.stream_id}, length={len(data.data)}")

        if data.stream_id in self._request_stream_ids.values():
            await self._handle_request_control_data(
                data.stream_id,
                data.data,
                end_stream=data.end_stream,
            )
            return

        if (
            data.stream_id in self._data_stream_types
            or data.stream_id in self._subgroup_headers
            or data.stream_id in self._fetch_headers
        ):
            await self._handle_data_stream(
                data.stream_id,
                data.data,
                end_stream=data.end_stream,
            )
            return

        if self._peer_control_stream_id is None:
            await self._handle_precontrol_stream_data(data)
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

    async def _handle_stream_reset(self, protocol, data: StreamResetData):
        """Handle peer-initiated reset or STOP_SENDING for a stream."""
        logger.info(
            "Stream termination received: stream_id=%s type=%s error=%s",
            data.stream_id,
            data.event_type,
            data.error_code,
        )

        if data.stream_id in self._request_stream_ids.values():
            self._request_stream_buffers.pop(data.stream_id, None)
            return

        self._pending_precontrol_streams.pop(data.stream_id, None)
        self._pending_precontrol_end_streams.discard(data.stream_id)
        self._cleanup_data_stream(data.stream_id)

    async def _handle_precontrol_stream_data(self, data: StreamData) -> None:
        """Buffer incoming streams until the peer control stream is identified."""
        buffer = self._pending_precontrol_streams.setdefault(data.stream_id, bytearray())
        buffer.extend(data.data)

        if data.end_stream:
            self._pending_precontrol_end_streams.add(data.stream_id)

        if is_unidirectional_stream_id(data.stream_id):
            try:
                stream_type, _ = VarInt.decode(buffer, 0)
            except Exception:
                stream_type = None

            if stream_type is not None and (
                is_subgroup_stream_type(stream_type)
                or stream_type == StreamType.FETCH_HEADER
            ):
                buffered = bytes(self._pending_precontrol_streams.pop(data.stream_id))
                end_stream = data.stream_id in self._pending_precontrol_end_streams
                self._pending_precontrol_end_streams.discard(data.stream_id)
                await self._handle_data_stream(data.stream_id, buffered, end_stream=end_stream)
                return

            self._peer_control_stream_id = data.stream_id
            buffered = bytes(self._pending_precontrol_streams.pop(data.stream_id))
            end_stream = data.stream_id in self._pending_precontrol_end_streams
            self._pending_precontrol_end_streams.discard(data.stream_id)
            await self._handle_control_data(buffered, end_stream=end_stream)
            await self._flush_pending_precontrol_data_streams()
            return

        if data.end_stream:
            buffered = bytes(self._pending_precontrol_streams.pop(data.stream_id))
            self._pending_precontrol_end_streams.discard(data.stream_id)
            await self._handle_data_stream(data.stream_id, buffered, end_stream=True)

    async def _handle_request_control_data(
        self,
        stream_id: int,
        data: bytes,
        end_stream: bool = False,
    ) -> None:
        """Handle control responses on a bidirectional request stream."""
        buffer = self._request_stream_buffers.setdefault(stream_id, bytearray())
        buffer.extend(data)
        request_id = self._request_id_for_stream(stream_id)

        while buffer:
            try:
                from moq.messages import decode_control_message

                msg, consumed = decode_control_message(
                    buffer,
                    response_request_id=request_id,
                )
            except Exception as e:
                if end_stream:
                    logger.warning(f"Failed to decode request control message: {e}")
                    buffer.clear()
                break

            if isinstance(msg, SubscribeOkMessage):
                self._handle_subscribe_ok(msg)
            elif isinstance(msg, RequestOkMessage):
                self._handle_request_ok(msg)
            elif isinstance(msg, RequestErrorMessage):
                self._handle_request_error(msg)
            elif isinstance(msg, FetchOkMessage):
                self._handle_fetch_ok(msg)
            elif isinstance(msg, PublishDoneMessage):
                self._handle_publish_done(msg)

            del buffer[:consumed]

        if end_stream:
            self._request_stream_buffers.pop(stream_id, None)

    def _request_id_for_stream(self, stream_id: int) -> Optional[int]:
        """Look up the locally initiated request that owns a request stream."""
        for request_id, request_stream_id in self._request_stream_ids.items():
            if request_stream_id == stream_id:
                return request_id
        return None

    async def _flush_pending_precontrol_data_streams(self) -> None:
        """Replay any data streams that arrived before the control stream."""
        pending_stream_ids = sorted(self._pending_precontrol_streams)
        for stream_id in pending_stream_ids:
            buffered = bytes(self._pending_precontrol_streams.pop(stream_id))
            end_stream = stream_id in self._pending_precontrol_end_streams
            self._pending_precontrol_end_streams.discard(stream_id)
            await self._handle_data_stream(stream_id, buffered, end_stream=end_stream)

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
            elif isinstance(msg, RequestOkMessage):
                self._handle_request_ok(msg)
            elif isinstance(msg, RequestErrorMessage):
                self._handle_request_error(msg)
            elif isinstance(msg, FetchOkMessage):
                self._handle_fetch_ok(msg)
            elif isinstance(msg, PublishDoneMessage):
                self._handle_publish_done(msg)

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
                self._track_aliases[msg.track_alias] = track_name
                self._subscription_seen_streams.setdefault(msg.request_id, set())
                self._subscription_active_streams.setdefault(msg.request_id, set())
                
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

    def _handle_request_ok(self, msg: RequestOkMessage):
        """Handle REQUEST_OK message."""
        logger.info("Request accepted: request_id=%s", msg.request_id)

        if self._session:
            self._session.handle_request_ok(msg)
    
    def _handle_fetch_ok(self, msg: FetchOkMessage):
        """Handle FETCH_OK message."""
        logger.info(f"Fetch accepted: request_id={msg.request_id}")

        if self._session:
            fetch_request = self._session.fetches.get(msg.request_id)
            if fetch_request:
                fetch_request.end_of_track = msg.end_of_track
                fetch_request.resolved_end_group = msg.end_location.group
                fetch_request.resolved_end_object = msg.end_location.object_id
                fetch_request.parameters = msg.parameters
                fetch_request.track_properties = msg.track_properties
                track_name = fetch_request.full_track_name
                track_alias = self._session.track_aliases.get(track_name)
                if track_alias is not None:
                    self._track_aliases[track_alias] = track_name

    def _handle_publish_done(self, msg: PublishDoneMessage):
        """Handle PUBLISH_DONE that terminates a subscription."""
        logger.info(
            "Subscription ended: request_id=%s status=%s reason=%s",
            msg.request_id,
            msg.status_code,
            msg.reason,
        )

        if not self._session:
            return

        subscription = self._session.get_subscription(msg.request_id)
        if not subscription:
            return
        subscription.expected_stream_count = msg.stream_count
        if self._can_finalize_subscription_end(msg.request_id, msg.stream_count):
            self._finalize_subscription_end(msg)
        else:
            self._pending_subscription_ends[msg.request_id] = msg
            self._schedule_subscription_end_timeout(msg.request_id)
    
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

            if is_subgroup_stream_type(stream_type):
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

            request_id = self._subscriptions.get(track_name)
            if request_id is not None:
                self._stream_subscription_requests[stream_id] = request_id
                self._subscription_seen_streams.setdefault(request_id, set()).add(stream_id)
                self._subscription_active_streams.setdefault(request_id, set()).add(stream_id)
                subscription = self._session.get_subscription(request_id) if self._session else None
                if subscription is not None:
                    subscription.received_stream_count = len(
                        self._subscription_seen_streams.get(request_id, ())
                    )

            while buffer:
                try:
                    obj, consumed = SubgroupObject.decode(
                        buffer,
                        0,
                        previous_object_id=self._subgroup_previous_object_ids.get(stream_id),
                    )
                    del buffer[:consumed]
                    self._subgroup_previous_object_ids[stream_id] = obj.object_id
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
            from moq.messages import FetchHeader
            
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
                    fetch_obj, consumed = FetchObject.decode(buffer, 0)
                    del buffer[:consumed]

                    received_obj = ReceivedObject(
                        track_alias=track_alias,
                        group_id=fetch_obj.group_id,
                        object_id=fetch_obj.object_id,
                        publisher_priority=fetch_obj.publisher_priority,
                        payload=fetch_obj.payload,
                        object_status=ObjectStatus.NORMAL
                    )
                    
                    await self._object_queue.put(received_obj)
                    logger.debug(
                        f"Fetch object received: group={fetch_obj.group_id}, object={fetch_obj.object_id}"
                    )
                    
                except ValueError:
                    break
                except Exception as e:
                    logger.debug(f"Failed to parse fetch stream object: {e}")
                    break
                    
        except Exception as e:
            logger.warning(f"Failed to handle fetch stream: {e}")

    def _cleanup_data_stream(self, stream_id: int):
        """Release parser state for a completed or failed data stream."""
        request_id = self._stream_subscription_requests.pop(stream_id, None)
        if request_id is not None:
            active_streams = self._subscription_active_streams.get(request_id)
            if active_streams is not None:
                active_streams.discard(stream_id)
                if not active_streams:
                    self._subscription_active_streams.pop(request_id, None)

        self._data_stream_buffers.pop(stream_id, None)
        self._data_stream_types.pop(stream_id, None)
        self._subgroup_headers.pop(stream_id, None)
        self._subgroup_previous_object_ids.pop(stream_id, None)
        self._fetch_headers.pop(stream_id, None)

        pending = self._pending_subscription_ends.get(request_id) if request_id is not None else None
        if pending is not None and self._can_finalize_subscription_end(
            request_id,
            pending.stream_count,
        ):
            self._pending_subscription_ends.pop(request_id, None)
            self._finalize_subscription_end(pending)

    def _can_finalize_subscription_end(self, request_id: int, stream_count: int) -> bool:
        """Return whether all expected subscription streams have been observed and closed."""
        active_streams = self._subscription_active_streams.get(request_id, set())
        if active_streams:
            return False
        if stream_count == UNKNOWN_PUBLISH_DONE_STREAM_COUNT:
            return True
        seen_streams = self._subscription_seen_streams.get(request_id, set())
        return len(seen_streams) >= stream_count

    def _finalize_subscription_end(self, msg: PublishDoneMessage) -> None:
        """Release local subscription state after PUBLISH_DONE conditions are satisfied."""
        if not self._session:
            return

        subscription = self._session.get_subscription(msg.request_id)
        if not subscription:
            return

        track_name = subscription.full_track_name
        self._active_subscriptions.pop(msg.request_id, None)
        self._track_aliases.pop(subscription.track_alias, None)
        self._subscriptions.pop(track_name, None)
        self._request_stream_ids.pop(msg.request_id, None)
        self._session.subscriptions.pop(msg.request_id, None)
        self._subscription_seen_streams.pop(msg.request_id, None)
        self._subscription_active_streams.pop(msg.request_id, None)
        self._pending_subscription_ends.pop(msg.request_id, None)
        self._cancel_subscription_end_timer(msg.request_id)

        if self._on_subscription_ended:
            self._on_subscription_ended(track_name, msg.status_code, msg.reason)

    def _schedule_subscription_end_timeout(self, request_id: int) -> None:
        """Arm a delivery-timeout cleanup for a pending PUBLISH_DONE."""
        self._cancel_subscription_end_timer(request_id)
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            return
        self._subscription_end_timers[request_id] = loop.create_task(
            self._force_subscription_end_after_timeout(request_id)
        )

    def _cancel_subscription_end_timer(self, request_id: int) -> None:
        """Cancel one pending subscription-end timeout task."""
        task = self._subscription_end_timers.pop(request_id, None)
        if task is not None and not task.done():
            task.cancel()

    def _cancel_subscription_end_timers(self) -> None:
        """Cancel every pending subscription-end timeout task."""
        for request_id in list(self._subscription_end_timers):
            self._cancel_subscription_end_timer(request_id)

    async def _force_subscription_end_after_timeout(self, request_id: int) -> None:
        """Force local cleanup once the delivery-timeout window expires."""
        try:
            await asyncio.sleep(self.delivery_timeout)
        except asyncio.CancelledError:
            return

        pending = self._pending_subscription_ends.get(request_id)
        if pending is None:
            return

        logger.info(
            "Forcing subscription cleanup after delivery timeout: request_id=%s timeout=%ss",
            request_id,
            self.delivery_timeout,
        )
        await self._cancel_subscription_streams(request_id)
        self._discard_subscription_stream_state(request_id)
        self._finalize_subscription_end(pending)

    def _discard_subscription_stream_state(self, request_id: int) -> None:
        """Drop local parser state for all data streams tied to one subscription."""
        for stream_id, mapped_request_id in list(self._stream_subscription_requests.items()):
            if mapped_request_id != request_id:
                continue
            self._stream_subscription_requests.pop(stream_id, None)
            self._data_stream_buffers.pop(stream_id, None)
            self._data_stream_types.pop(stream_id, None)
            self._subgroup_headers.pop(stream_id, None)
            self._subgroup_previous_object_ids.pop(stream_id, None)
            self._fetch_headers.pop(stream_id, None)
        self._subscription_active_streams.pop(request_id, None)

    async def _cancel_subscription_streams(self, request_id: int) -> None:
        """Best-effort STOP_SENDING / RESET_STREAM for a timed-out subscription."""
        if not self._client:
            return

        active_streams = set(self._subscription_active_streams.get(request_id, ()))
        for stream_id in active_streams:
            stop_stream = getattr(self._client, "stop_stream", None)
            if not callable(stop_stream):
                break
            try:
                await stop_stream(stream_id, error_code=STREAM_CANCELLATION_ERROR_CODE)
            except Exception as e:
                logger.debug(
                    "Failed to STOP_SENDING stream %s for request %s: %s",
                    stream_id,
                    request_id,
                    e,
                )

        request_stream_id = self._request_stream_ids.get(request_id)
        if request_stream_id is None:
            return

        reset_stream = getattr(self._client, "reset_stream", None)
        if not callable(reset_stream):
            return
        try:
            await reset_stream(request_stream_id, error_code=STREAM_CANCELLATION_ERROR_CODE)
        except Exception as e:
            logger.debug(
                "Failed to RESET_STREAM request stream %s for request %s: %s",
                request_stream_id,
                request_id,
                e,
            )
    
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
