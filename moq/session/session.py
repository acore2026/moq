"""
MOQ Transport - Session Management
Manages MOQT sessions, subscriptions, and publications.
"""

import asyncio
import inspect
import logging
from typing import Dict, Set, Optional, Callable, Any, List
from enum import IntEnum
from dataclasses import dataclass, field

from moq.messages import (
    MessageType, SetupMessage, SubscribeMessage, SubscribeOkMessage,
    PublishMessage, PublishOkMessage, PublishDoneMessage, RequestOkMessage, RequestErrorMessage,
    ErrorCode, GroupOrder, SubscribeFilter, FetchMessage, FetchOkMessage,
    ParameterType, RequestUpdateMessage, SubscriptionFilterValue,
    group_order_to_parameter_value, UNKNOWN_PUBLISH_DONE_STREAM_COUNT,
)
from moq.encoding import FullTrackName, TrackAlias, Parameters

logger = logging.getLogger(__name__)


class Role(IntEnum):
    """MOQT Role."""
    PUBLISHER = 0x01
    SUBSCRIBER = 0x02
    PUBSUB = 0x03


class SessionState(IntEnum):
    """Session state."""
    CONNECTING = 0
    HANDSHAKING = 1
    ACTIVE = 2
    CLOSING = 3
    CLOSED = 4


@dataclass
class Subscription:
    """Represents a subscription."""
    request_id: int
    track_alias: Optional[int]
    full_track_name: FullTrackName
    subscriber_priority: int
    group_order: GroupOrder
    filter_type: SubscribeFilter
    required_request_id_delta: int = 0
    parameters: Optional[Parameters] = None
    start_group: Optional[int] = None
    start_object: Optional[int] = None
    end_group: Optional[int] = None
    end_object: Optional[int] = None
    expected_stream_count: Optional[int] = None
    received_stream_count: int = 0
    active: bool = True
    subscriber: Optional['MOQSession'] = None


@dataclass
class Publication:
    """Represents a publication."""
    request_id: int
    track_alias: int
    full_track_name: FullTrackName
    required_request_id_delta: int = 0
    parameters: Optional[Parameters] = None
    track_properties: bytes = b""
    active: bool = True
    publisher: Optional['MOQSession'] = None


@dataclass
class FetchRequest:
    """Represents a fetch request."""
    request_id: int
    full_track_name: FullTrackName
    subscriber_priority: int
    group_order: GroupOrder
    start_group: int
    start_object: int
    end_group: int
    end_object: int
    required_request_id_delta: int = 0
    parameters: Optional[Parameters] = None
    end_of_track: bool = False
    resolved_end_group: Optional[int] = None
    resolved_end_object: Optional[int] = None
    track_properties: bytes = b""
    active: bool = True


class MOQSession:
    """
    MOQ Transport Session.
    Manages the connection, subscriptions, and publications.
    """
    
    def __init__(self, session_id: str, role: Role):
        self.session_id = session_id
        self.role = role
        self.state = SessionState.CONNECTING
        self.version = 0xFF000011  # draft-ietf-moq-transport-17
        
        # Request ID management
        self._next_request_id = 0
        
        # Subscriptions and publications
        self.subscriptions: Dict[int, Subscription] = {}  # request_id -> Subscription
        self.publications: Dict[int, Publication] = {}  # request_id -> Publication
        self.fetches: Dict[int, FetchRequest] = {}  # request_id -> FetchRequest
        
        # Track alias management
        self._next_track_alias = 0
        self.track_aliases: Dict[FullTrackName, int] = {}  # track_name -> alias
        
        # Handlers
        self._on_setup: Optional[Callable[[SetupMessage], None]] = None
        self._on_subscribe: Optional[Callable[[SubscribeMessage], None]] = None
        self._on_publish: Optional[Callable[[PublishMessage], None]] = None
        self._on_fetch: Optional[Callable[[FetchMessage], None]] = None
        self._on_close: Optional[Callable[[], None]] = None
        
        # Send callback
        self._send_callback: Optional[Callable[..., None]] = None
        
        logger.info(f"MOQSession created: session_id={session_id}, role={role.name}")
    
    def set_handlers(self,
                     on_setup: Optional[Callable[[SetupMessage], None]] = None,
                     on_subscribe: Optional[Callable[[SubscribeMessage], None]] = None,
                     on_publish: Optional[Callable[[PublishMessage], None]] = None,
                     on_fetch: Optional[Callable[[FetchMessage], None]] = None,
                     on_close: Optional[Callable[[], None]] = None):
        """Set event handlers."""
        self._on_setup = on_setup
        self._on_subscribe = on_subscribe
        self._on_publish = on_publish
        self._on_fetch = on_fetch
        self._on_close = on_close
    
    def set_send_callback(self, callback: Callable[..., None]):
        """Set callback for sending data."""
        self._send_callback = callback

    async def _send_control_message(self, data: bytes, stream_id: Optional[int] = None):
        """Send a control message through the configured callback."""
        if not self._send_callback:
            return
        try:
            result = self._send_callback(data, stream_id=stream_id)
        except TypeError:
            result = self._send_callback(data)
        if inspect.isawaitable(result):
            await result
    
    def _get_next_request_id(self) -> int:
        """Get next available request ID."""
        request_id = self._next_request_id
        self._next_request_id += 2
        return request_id

    def _get_default_required_request_id_delta(self, request_id: int) -> int:
        """Current implementation does not model request dependencies explicitly."""
        return 0

    def get_next_request_id(self) -> int:
        """Get next available request ID (public interface)."""
        return self._get_next_request_id()
    
    def _get_or_create_track_alias(self, track_name: FullTrackName) -> int:
        """Get existing or create new track alias."""
        if track_name not in self.track_aliases:
            alias = self._next_track_alias
            self._next_track_alias += 1
            self.track_aliases[track_name] = alias
            logger.debug(f"Created track alias: {track_name} -> {alias}")
        return self.track_aliases[track_name]
    
    def handle_setup(self, msg: SetupMessage):
        """Handle SETUP message."""
        logger.info(f"Handling SETUP: version={msg.version}, role={msg.role}")
        self.version = msg.version
        self.state = SessionState.ACTIVE
        
        if self._on_setup:
            self._on_setup(msg)
    
    def handle_subscribe(self, msg: SubscribeMessage):
        """Handle SUBSCRIBE message."""
        logger.info(f"Handling SUBSCRIBE: request_id={msg.request_id}")
        
        # Create subscription record
        subscription = Subscription(
            request_id=msg.request_id,
            track_alias=None,
            full_track_name=msg.full_track_name,
            subscriber_priority=msg.subscriber_priority,
            group_order=msg.group_order,
            filter_type=msg.filter_type,
            required_request_id_delta=msg.required_request_id_delta,
            parameters=msg.parameters,
            start_group=msg.start_group,
            start_object=msg.start_object,
            end_group=msg.end_group,
            end_object=msg.end_object,
            subscriber=self
        )
        
        self.subscriptions[msg.request_id] = subscription
        
        if self._on_subscribe:
            self._on_subscribe(msg)
    
    def handle_subscribe_ok(self, msg: SubscribeOkMessage):
        """Handle SUBSCRIBE_OK message."""
        logger.info(
            "Handling SUBSCRIBE_OK: request_id=%s track_alias=%s",
            msg.request_id,
            msg.track_alias,
        )
        
        # Update subscription
        if msg.request_id in self.subscriptions:
            subscription = self.subscriptions[msg.request_id]
            subscription.active = True
            subscription.track_alias = msg.track_alias
            self.track_aliases[subscription.full_track_name] = msg.track_alias
    
    def handle_publish(self, msg: PublishMessage):
        """Handle PUBLISH message."""
        logger.info(f"Handling PUBLISH: request_id={msg.request_id}, track_alias={msg.track_alias}")
        
        # Create publication record
        publication = Publication(
            request_id=msg.request_id,
            track_alias=msg.track_alias,
            full_track_name=msg.full_track_name,
            required_request_id_delta=msg.required_request_id_delta,
            parameters=msg.parameters,
            track_properties=msg.track_properties,
            publisher=self
        )
        
        self.publications[msg.request_id] = publication
        
        if self._on_publish:
            self._on_publish(msg)
    
    def handle_publish_ok(self, msg: PublishOkMessage):
        """Handle PUBLISH_OK message."""
        logger.info(f"Handling PUBLISH_OK: request_id={msg.request_id}")
        
        if msg.request_id in self.publications:
            self.publications[msg.request_id].active = True
            self.publications[msg.request_id].parameters = msg.parameters
    
    def handle_publish_done(self, msg: PublishDoneMessage):
        """Handle PUBLISH_DONE message."""
        logger.info(f"Handling PUBLISH_DONE: request_id={msg.request_id}, status={msg.status_code}")
        
        if msg.request_id in self.publications:
            self.publications[msg.request_id].active = False
    
    def handle_fetch(self, msg: FetchMessage):
        """Handle FETCH message."""
        logger.info(f"Handling FETCH: request_id={msg.request_id}, track={msg.full_track_name}")
        
        # Create fetch request record
        fetch = FetchRequest(
            request_id=msg.request_id,
            full_track_name=msg.full_track_name,
            subscriber_priority=msg.subscriber_priority,
            group_order=msg.group_order,
            required_request_id_delta=msg.required_request_id_delta,
            start_group=msg.start_group,
            start_object=msg.start_object,
            end_group=msg.end_group,
            end_object=msg.end_object,
            parameters=msg.parameters,
        )
        
        self.fetches[msg.request_id] = fetch
        
        if self._on_fetch:
            self._on_fetch(msg)
    
    def handle_request_error(self, msg: RequestErrorMessage):
        """Handle REQUEST_ERROR message."""
        logger.warning(f"Handling REQUEST_ERROR: request_id={msg.request_id}, code={msg.error_code}, reason={msg.reason}")

    def handle_request_ok(self, msg: RequestOkMessage):
        """Handle REQUEST_OK message."""
        logger.info("Handling REQUEST_OK: request_id=%s", msg.request_id)

        if msg.request_id in self.subscriptions:
            self.subscriptions[msg.request_id].parameters = msg.parameters
        elif msg.request_id in self.fetches:
            self.fetches[msg.request_id].parameters = msg.parameters
        elif msg.request_id in self.publications:
            self.publications[msg.request_id].parameters = msg.parameters
    
    async def send_setup(self, role: Optional[Role] = None):
        """Send SETUP message."""
        if role is None:
            role = self.role
        
        from moq.encoding import Parameters
        params = Parameters()
        
        msg = SetupMessage(
            version=self.version,
            role=role.value,
            parameters=params
        )
        
        data = msg.encode()
        await self._send_control_message(data)
        
        logger.info(f"Sent SETUP: version={self.version}, role={role.name}")
    
    async def subscribe(self, track_name: FullTrackName, 
                        subscriber_priority: int = 128,
                        group_order: GroupOrder = GroupOrder.ASCENDING,
                        filter_type: SubscribeFilter = SubscribeFilter.LATEST_OBJECT,
                        start_group: Optional[int] = None,
                        start_object: Optional[int] = None,
                        end_group: Optional[int] = None,
                        end_object: Optional[int] = None,
                        stream_id: Optional[int] = None) -> int:
        """
        Subscribe to a track.
        
        Returns:
            Request ID of the subscription
        """
        request_id = self._get_next_request_id()
        required_request_id_delta = self._get_default_required_request_id_delta(request_id)
        msg = SubscribeMessage(
            request_id=request_id,
            full_track_name=track_name,
            subscriber_priority=subscriber_priority,
            group_order=group_order,
            filter_type=filter_type,
            required_request_id_delta=required_request_id_delta,
            start_group=start_group,
            start_object=start_object,
            end_group=end_group,
            end_object=end_object
        )
        
        # Track subscription
        subscription = Subscription(
            request_id=request_id,
            track_alias=None,
            full_track_name=track_name,
            subscriber_priority=subscriber_priority,
            group_order=group_order,
            filter_type=filter_type,
            required_request_id_delta=required_request_id_delta,
            parameters=None,
            start_group=start_group,
            start_object=start_object,
            end_group=end_group,
            end_object=end_object,
            active=False,
            subscriber=self
        )
        self.subscriptions[request_id] = subscription
        
        data = msg.encode()
        await self._send_control_message(data, stream_id=stream_id)
        
        logger.info(f"Sent SUBSCRIBE: request_id={request_id}")
        return request_id
    
    async def send_subscribe_ok(
        self,
        request_id: int,
        track_alias: int,
        parameters: Optional[object] = None,
        track_properties: bytes = b"",
        stream_id: Optional[int] = None,
    ):
        """Send SUBSCRIBE_OK response."""
        msg = SubscribeOkMessage(
            request_id=request_id,
            track_alias=track_alias,
            parameters=parameters,
            track_properties=track_properties,
        )
        
        data = msg.encode(include_request_id=stream_id is None)
        await self._send_control_message(data, stream_id=stream_id)
        
        logger.info(f"Sent SUBSCRIBE_OK: request_id={request_id}")
    
    async def publish(
        self,
        track_name: FullTrackName,
        request_id: int = None,
        stream_id: Optional[int] = None,
    ) -> int:
        """
        Publish a track.

        Args:
            track_name: Full track name to publish
            request_id: Optional request ID to use (if None, generates a new one)

        Returns:
            Request ID of the publication
        """
        if request_id is None:
            request_id = self._get_next_request_id()
        required_request_id_delta = self._get_default_required_request_id_delta(request_id)
        track_alias = self._get_or_create_track_alias(track_name)
        
        msg = PublishMessage(
            request_id=request_id,
            required_request_id_delta=required_request_id_delta,
            full_track_name=track_name,
            track_alias=track_alias,
        )
        
        # Track publication
        publication = Publication(
            request_id=request_id,
            track_alias=track_alias,
            full_track_name=track_name,
            required_request_id_delta=required_request_id_delta,
            parameters=None,
            track_properties=b"",
            active=False,
            publisher=self
        )
        self.publications[request_id] = publication
        
        data = msg.encode()
        await self._send_control_message(data, stream_id=stream_id)
        
        logger.info(f"Sent PUBLISH: request_id={request_id}, track_alias={track_alias}")
        return request_id
    
    async def send_publish_ok(self, request_id: int, stream_id: Optional[int] = None):
        """Send PUBLISH_OK response."""
        msg = PublishOkMessage(request_id=request_id, parameters=Parameters())
        
        data = msg.encode(include_request_id=stream_id is None)
        await self._send_control_message(data, stream_id=stream_id)
        
        logger.info(f"Sent PUBLISH_OK: request_id={request_id}")
    
    async def send_publish_done(
        self,
        request_id: int,
        status_code: int,
        reason: str,
        stream_count: int = UNKNOWN_PUBLISH_DONE_STREAM_COUNT,
        stream_id: Optional[int] = None,
    ):
        """Send PUBLISH_DONE message."""
        msg = PublishDoneMessage(
            request_id=request_id,
            status_code=status_code,
            stream_count=stream_count,
            reason=reason
        )
        
        data = msg.encode(include_request_id=stream_id is None)
        await self._send_control_message(data, stream_id=stream_id)
        
        logger.info(f"Sent PUBLISH_DONE: request_id={request_id}, status={status_code}")
    
    async def fetch(self, track_name: FullTrackName,
                    start_group: int = 0, start_object: int = 0,
                    end_group: Optional[int] = None, end_object: Optional[int] = None,
                    subscriber_priority: int = 128,
                    group_order: GroupOrder = GroupOrder.ASCENDING,
                    stream_id: Optional[int] = None) -> int:
        """
        Fetch specific objects from a track.
        
        Args:
            track_name: The track to fetch from
            start_group: Starting group ID (defaults to 0)
            start_object: Starting object ID (defaults to 0)
            end_group: Ending group ID (None means fetch until latest)
            end_object: Ending object ID (None means fetch until latest)
            subscriber_priority: Priority level (0-255)
            group_order: Group order (ASCENDING or DESCENDING)
        
        Returns:
            Request ID of the fetch
        """
        request_id = self._get_next_request_id()
        required_request_id_delta = self._get_default_required_request_id_delta(request_id)
        track_alias = self._get_or_create_track_alias(track_name)

        msg = FetchMessage(
            request_id=request_id,
            full_track_name=track_name,
            subscriber_priority=subscriber_priority,
            group_order=group_order,
            required_request_id_delta=required_request_id_delta,
            start_group=start_group,
            start_object=start_object,
            end_group=end_group,
            end_object=end_object
        )
        
        # Track fetch
        fetch = FetchRequest(
            request_id=request_id,
            full_track_name=track_name,
            subscriber_priority=subscriber_priority,
            group_order=group_order,
            required_request_id_delta=required_request_id_delta,
            start_group=start_group,
            start_object=start_object,
            end_group=end_group,
            end_object=end_object,
            parameters=None,
        )
        self.fetches[request_id] = fetch
        logger.info(f"Prepared FETCH: request_id={request_id}, track_alias={track_alias}")
        
        data = msg.encode()
        await self._send_control_message(data, stream_id=stream_id)
        
        logger.info(f"Sent FETCH: request_id={request_id}")
        return request_id
    
    async def send_fetch_ok(
        self,
        request_id: int,
        end_of_track: bool = False,
        end_group: int = 0,
        end_object: int = 0,
        parameters: Optional[Parameters] = None,
        track_properties: bytes = b"",
        stream_id: Optional[int] = None,
    ):
        """Send FETCH_OK response."""
        msg = FetchOkMessage(
            request_id=request_id,
            end_of_track=end_of_track,
            end_location=Location(end_group, end_object),
            parameters=parameters,
            track_properties=track_properties,
        )
        
        data = msg.encode(include_request_id=stream_id is None)
        await self._send_control_message(data, stream_id=stream_id)
        
        logger.info(f"Sent FETCH_OK: request_id={request_id}")
    
    async def send_request_error(
        self,
        request_id: int,
        error_code: ErrorCode,
        reason: str,
        stream_id: Optional[int] = None,
    ):
        """Send REQUEST_ERROR message."""
        msg = RequestErrorMessage(
            request_id=request_id,
            error_code=error_code.value,
            reason=reason
        )
        
        data = msg.encode(include_request_id=stream_id is None)
        await self._send_control_message(data, stream_id=stream_id)
        
        logger.info(f"Sent REQUEST_ERROR: request_id={request_id}, code={error_code.name}")

    async def request_update(
        self,
        request_id: int,
        parameters: Optional[object] = None,
        stream_id: Optional[int] = None,
    ) -> None:
        """Send REQUEST_UPDATE for an existing request."""
        required_request_id_delta = self._get_default_required_request_id_delta(request_id)
        msg = RequestUpdateMessage(
            request_id=request_id,
            required_request_id_delta=required_request_id_delta,
            parameters=parameters,
        )
        await self._send_control_message(msg.encode(), stream_id=stream_id)

    async def update_subscription(
        self,
        request_id: int,
        subscriber_priority: Optional[int] = None,
        group_order: Optional[GroupOrder] = None,
        filter_type: Optional[SubscribeFilter] = None,
        start_group: Optional[int] = None,
        start_object: Optional[int] = None,
        end_group: Optional[int] = None,
        stream_id: Optional[int] = None,
    ) -> None:
        """Send a REQUEST_UPDATE with draft-17 subscription parameters."""
        parameters = Parameters()

        if subscriber_priority is not None:
            parameters.set(ParameterType.SUBSCRIBER_PRIORITY, subscriber_priority)
        if group_order is not None:
            parameters.set(ParameterType.GROUP_ORDER, group_order_to_parameter_value(group_order))
        if filter_type is not None:
            end_group_delta = None
            if filter_type == SubscribeFilter.ABSOLUTE_RANGE:
                if start_group is None or end_group is None:
                    raise ValueError("ABSOLUTE_RANGE update requires start_group and end_group")
                end_group_delta = end_group - start_group
                if end_group_delta < 0:
                    raise ValueError("end_group must be >= start_group")
            filter_value = SubscriptionFilterValue(
                filter_type=filter_type,
                start_group=start_group,
                start_object=start_object,
                end_group_delta=end_group_delta,
            )
            parameters.set(ParameterType.SUBSCRIPTION_FILTER, filter_value.encode())

        await self.request_update(
            request_id=request_id,
            parameters=parameters,
            stream_id=stream_id,
        )
    
    def get_subscription(self, request_id: int) -> Optional[Subscription]:
        """Get subscription by request ID."""
        return self.subscriptions.get(request_id)
    
    def get_publication(self, request_id: int) -> Optional[Publication]:
        """Get publication by request ID."""
        return self.publications.get(request_id)
    
    def close(self):
        """Close the session."""
        logger.info(f"Closing session: {self.session_id}")
        self.state = SessionState.CLOSED
        
        if self._on_close:
            self._on_close()
