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
    PublishMessage, PublishOkMessage, PublishDoneMessage, RequestErrorMessage,
    ErrorCode, GroupOrder, SubscribeFilter, FetchMessage, FetchOkMessage
)
from moq.encoding import FullTrackName, TrackAlias


SETUP_AGENT_ID_PARAM = 0x1001

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
    track_alias: int
    full_track_name: FullTrackName
    subscriber_priority: int
    group_order: GroupOrder
    filter_type: SubscribeFilter
    start_group: Optional[int] = None
    start_object: Optional[int] = None
    end_group: Optional[int] = None
    end_object: Optional[int] = None
    active: bool = True
    subscriber: Optional['MOQSession'] = None


@dataclass
class Publication:
    """Represents a publication."""
    request_id: int
    track_alias: int
    full_track_name: FullTrackName
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
        self._send_callback: Optional[Callable[[bytes], None]] = None
        
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
    
    def set_send_callback(self, callback: Callable[[bytes], None]):
        """Set callback for sending data."""
        self._send_callback = callback

    async def _send_control_message(self, data: bytes):
        """Send a control message through the configured callback."""
        if not self._send_callback:
            return

        result = self._send_callback(data)
        if inspect.isawaitable(result):
            await result
    
    def _get_next_request_id(self) -> int:
        """Get next available request ID."""
        request_id = self._next_request_id
        self._next_request_id += 1
        return request_id

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
        logger.info(f"Handling SUBSCRIBE: request_id={msg.request_id}, track_alias={msg.track_alias}")
        
        # Create subscription record
        subscription = Subscription(
            request_id=msg.request_id,
            track_alias=msg.track_alias,
            full_track_name=msg.full_track_name,
            subscriber_priority=msg.subscriber_priority,
            group_order=msg.group_order,
            filter_type=msg.filter_type,
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
        logger.info(f"Handling SUBSCRIBE_OK: request_id={msg.request_id}, expires={msg.expires}")
        
        # Update subscription
        if msg.request_id in self.subscriptions:
            self.subscriptions[msg.request_id].active = True
    
    def handle_publish(self, msg: PublishMessage):
        """Handle PUBLISH message."""
        logger.info(f"Handling PUBLISH: request_id={msg.request_id}, track_alias={msg.track_alias}")
        
        # Create publication record
        publication = Publication(
            request_id=msg.request_id,
            track_alias=msg.track_alias,
            full_track_name=msg.full_track_name,
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
            start_group=msg.start_group,
            start_object=msg.start_object,
            end_group=msg.end_group,
            end_object=msg.end_object
        )
        
        self.fetches[msg.request_id] = fetch
        
        if self._on_fetch:
            self._on_fetch(msg)
    
    def handle_request_error(self, msg: RequestErrorMessage):
        """Handle REQUEST_ERROR message."""
        logger.warning(f"Handling REQUEST_ERROR: request_id={msg.request_id}, code={msg.error_code}, reason={msg.reason}")
    
    async def send_setup(self, role: Optional[Role] = None, parameters=None):
        """Send SETUP message."""
        if role is None:
            role = self.role
        
        from moq.encoding import Parameters
        params = parameters if parameters is not None else Parameters()
        
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
                        end_object: Optional[int] = None) -> int:
        """
        Subscribe to a track.
        
        Returns:
            Request ID of the subscription
        """
        request_id = self._get_next_request_id()
        track_alias = self._get_or_create_track_alias(track_name)
        
        msg = SubscribeMessage(
            request_id=request_id,
            track_alias=track_alias,
            full_track_name=track_name,
            subscriber_priority=subscriber_priority,
            group_order=group_order,
            filter_type=filter_type,
            start_group=start_group,
            start_object=start_object,
            end_group=end_group,
            end_object=end_object
        )
        
        # Track subscription
        subscription = Subscription(
            request_id=request_id,
            track_alias=track_alias,
            full_track_name=track_name,
            subscriber_priority=subscriber_priority,
            group_order=group_order,
            filter_type=filter_type,
            start_group=start_group,
            start_object=start_object,
            end_group=end_group,
            end_object=end_object,
            active=False,
            subscriber=self
        )
        self.subscriptions[request_id] = subscription
        
        data = msg.encode()
        await self._send_control_message(data)
        
        logger.info(f"Sent SUBSCRIBE: request_id={request_id}, track_alias={track_alias}")
        return request_id
    
    async def send_subscribe_ok(self, request_id: int, expires: int = 0,
                                group_order: GroupOrder = GroupOrder.ASCENDING,
                                largest_group: Optional[int] = None,
                                largest_object: Optional[int] = None):
        """Send SUBSCRIBE_OK response."""
        msg = SubscribeOkMessage(
            request_id=request_id,
            expires=expires,
            group_order=group_order,
            largest_group=largest_group,
            largest_object=largest_object
        )
        
        data = msg.encode()
        await self._send_control_message(data)
        
        logger.info(f"Sent SUBSCRIBE_OK: request_id={request_id}")
    
    async def publish(self, track_name: FullTrackName, request_id: int = None) -> int:
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
        track_alias = self._get_or_create_track_alias(track_name)
        
        msg = PublishMessage(
            request_id=request_id,
            track_alias=track_alias,
            full_track_name=track_name
        )
        
        # Track publication
        publication = Publication(
            request_id=request_id,
            track_alias=track_alias,
            full_track_name=track_name,
            active=False,
            publisher=self
        )
        self.publications[request_id] = publication
        
        data = msg.encode()
        await self._send_control_message(data)
        
        logger.info(f"Sent PUBLISH: request_id={request_id}, track_alias={track_alias}")
        return request_id
    
    async def send_publish_ok(self, request_id: int):
        """Send PUBLISH_OK response."""
        msg = PublishOkMessage(request_id=request_id)
        
        data = msg.encode()
        await self._send_control_message(data)
        
        logger.info(f"Sent PUBLISH_OK: request_id={request_id}")
    
    async def send_publish_done(self, request_id: int, status_code: int, reason: str):
        """Send PUBLISH_DONE message."""
        msg = PublishDoneMessage(
            request_id=request_id,
            status_code=status_code,
            reason=reason
        )
        
        data = msg.encode()
        await self._send_control_message(data)
        
        logger.info(f"Sent PUBLISH_DONE: request_id={request_id}, status={status_code}")
    
    async def fetch(self, track_name: FullTrackName,
                    start_group: int = 0, start_object: int = 0,
                    end_group: Optional[int] = None, end_object: Optional[int] = None,
                    subscriber_priority: int = 128,
                    group_order: GroupOrder = GroupOrder.ASCENDING) -> int:
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
        track_alias = self._get_or_create_track_alias(track_name)
        
        msg = FetchMessage(
            request_id=request_id,
            full_track_name=track_name,
            subscriber_priority=subscriber_priority,
            group_order=group_order,
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
            start_group=start_group,
            start_object=start_object,
            end_group=end_group,
            end_object=end_object
        )
        self.fetches[request_id] = fetch
        logger.info(f"Prepared FETCH: request_id={request_id}, track_alias={track_alias}")
        
        data = msg.encode()
        await self._send_control_message(data)
        
        logger.info(f"Sent FETCH: request_id={request_id}")
        return request_id
    
    async def send_fetch_ok(self, request_id: int, 
                           group_order: GroupOrder = GroupOrder.ASCENDING,
                           end_of_track: bool = False,
                           largest_group: Optional[int] = None,
                           largest_object: Optional[int] = None):
        """Send FETCH_OK response."""
        msg = FetchOkMessage(
            request_id=request_id,
            group_order=group_order,
            end_of_track=end_of_track,
            largest_group=largest_group,
            largest_object=largest_object
        )
        
        data = msg.encode()
        await self._send_control_message(data)
        
        logger.info(f"Sent FETCH_OK: request_id={request_id}")
    
    async def send_request_error(self, request_id: int, error_code: ErrorCode, reason: str):
        """Send REQUEST_ERROR message."""
        msg = RequestErrorMessage(
            request_id=request_id,
            error_code=error_code.value,
            reason=reason
        )
        
        data = msg.encode()
        await self._send_control_message(data)
        
        logger.info(f"Sent REQUEST_ERROR: request_id={request_id}, code={error_code.name}")
    
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
