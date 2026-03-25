# MOQ Transport - API 文档

## 目录

1. [编码模块 (moq.encoding)](#编码模块)
2. [消息模块 (moq.messages)](#消息模块)
3. [传输模块 (moq.transport)](#传输模块)
4. [会话模块 (moq.session)](#会话模块)
5. [发布模块 (moq.pub)](#发布模块)
6. [订阅模块 (moq.sub)](#订阅模块)
7. [中继模块 (moq.relay)](#中继模块)

---

## 编码模块

### VarInt

变长整数编码，根据值的大小自动选择最优的字节数。

```python
from moq.encoding import VarInt

class VarInt:
    @staticmethod
    def encode(value: int) -> bytes
    @staticmethod
    def decode(data: bytes, offset: int = 0) -> Tuple[int, int]
```

**编码规则**:
- 0-127: 1 byte
- 128-16383: 2 bytes
- 16384-2097151: 3 bytes
- ... up to 9 bytes for 64-bit values

**示例**:
```python
# 编码
encoded = VarInt.encode(1000)  # b'\x8a\x03'

# 解码
value, consumed = VarInt.decode(encoded)
# value = 1000, consumed = 2
```

### FullTrackName

完整的轨道名称，包含命名空间和轨道名。

```python
from moq.encoding import FullTrackName

class FullTrackName:
    def __init__(self, namespace: List[bytes], track_name: bytes)
    
    namespace: List[bytes]      # 命名空间字段列表 (0-32个)
    track_name: bytes          # 轨道名
    
    def encode(self) -> bytes
    @staticmethod
    def decode(data: bytes, offset: int = 0) -> Tuple[FullTrackName, int]
    def to_string(self) -> str  # 安全的字符串表示
```

**约束**:
- 命名空间最多 32 个字段
- 总长度不超过 4096 字节
- 每个命名空间字段至少 1 字节

**示例**:
```python
# 创建轨道名
namespace = [b"example", b"live"]
track = FullTrackName(namespace, b"stream1")

# 序列化
encoded = track.encode()

# 反序列化
decoded, _ = FullTrackName.decode(encoded)
```

### Location

对象在轨道中的位置标识。

```python
from moq.encoding import Location

class Location:
    def __init__(self, group: int, object_id: int)
    
    group: int       # 组ID
    object_id: int   # 对象ID
    
    def encode(self) -> bytes
    @staticmethod
    def decode(data: bytes, offset: int = 0) -> Tuple[Location, int]
```

**比较规则**:
- A < B 如果 A.group < B.group
- 或 A.group == B.group 且 A.object_id < B.object_id

**示例**:
```python
loc1 = Location(1, 1)
loc2 = Location(1, 2)
loc3 = Location(2, 1)

assert loc1 < loc2
assert loc2 < loc3
```

### KeyValuePair

键值对结构，用于参数和属性。

```python
from moq.encoding import KeyValuePair

class KeyValuePair:
    def __init__(self, key_type: int, value: Union[int, bytes])
    
    key_type: int              # 键类型
    value: Union[int, bytes]   # 值 (偶数类型=int, 奇数类型=bytes)
    
    def encode(self, prev_type: int = 0) -> bytes
    @staticmethod
    def decode(data: bytes, offset: int = 0, prev_type: int = 0) -> Tuple[KeyValuePair, int]
```

**类型规则**:
- 偶数类型: 值必须是整数 (varint)
- 奇数类型: 值必须是字节串，带长度前缀
- 使用 delta 编码: 存储的是与上一个类型的差值

### Parameters

参数集合。

```python
from moq.encoding import Parameters

class Parameters:
    def __init__(self)
    
    def set(self, key_type: int, value: Union[int, bytes])
    def get(self, key_type: int, default=None) -> Optional[Union[int, bytes]]
    def encode(self) -> bytes
    @staticmethod
    def decode(data: bytes, offset: int = 0) -> Tuple[Parameters, int]
```

**示例**:
```python
params = Parameters()
params.set(1, 100)              # varint 参数
params.set(3, b"auth_token")    # bytes 参数

encoded = params.encode()
decoded, _ = Parameters.decode(encoded)
```

---

## 消息模块

### 控制消息类型

#### SetupMessage

会话建立消息。

```python
from moq.messages import SetupMessage

class SetupMessage:
    def __init__(
        self,
        version: int,          # 协议版本
        role: int,             # 角色 (1=publisher, 2=subscriber, 3=pubsub)
        parameters: Parameters # 参数
    )
```

#### SubscribeMessage

订阅请求。

```python
from moq.messages import SubscribeMessage, SubscribeFilter, GroupOrder

class SubscribeMessage:
    def __init__(
        self,
        request_id: int,              # 请求ID
        track_alias: int,             # 轨道别名
        full_track_name: FullTrackName,  # 完整轨道名
        subscriber_priority: int,     # 订阅优先级 (0-255)
        group_order: GroupOrder,      # 组顺序
        filter_type: SubscribeFilter, # 过滤器类型
        start_group: Optional[int] = None,
        start_object: Optional[int] = None,
        end_group: Optional[int] = None,
        end_object: Optional[int] = None,
        parameters: Optional[Parameters] = None
    )

class SubscribeFilter(IntEnum):
    NONE = 0x00           # 无过滤
    LATEST_GROUP = 0x01   # 最新组
    LATEST_OBJECT = 0x02  # 最新对象
    ABSOLUTE_START = 0x03 # 绝对起始位置
    ABSOLUTE_RANGE = 0x04 # 绝对范围

class GroupOrder(IntEnum):
    ASCENDING = 0x00   # 升序
    DESCENDING = 0x01  # 降序
```

#### SubscribeOkMessage

订阅成功响应。

```python
from moq.messages import SubscribeOkMessage

class SubscribeOkMessage:
    def __init__(
        self,
        request_id: int,              # 对应订阅请求ID
        expires: int,                 # 过期时间 (毫秒, 0=不过期)
        group_order: GroupOrder,      # 组顺序
        largest_group: Optional[int] = None,
        largest_object: Optional[int] = None,
        parameters: Optional[Parameters] = None
    )
```

#### PublishMessage

发布请求。

```python
from moq.messages import PublishMessage

class PublishMessage:
    def __init__(
        self,
        request_id: int,
        track_alias: int,
        full_track_name: FullTrackName,
        parameters: Optional[Parameters] = None
    )
```

#### FetchMessage

获取特定对象请求。

```python
from moq.messages import FetchMessage

class FetchMessage:
    def __init__(
        self,
        request_id: int,
        full_track_name: FullTrackName,
        subscriber_priority: int,
        group_order: GroupOrder,
        start_group: int,
        start_object: int,
        end_group: int,
        end_object: int,
        parameters: Optional[Parameters] = None
    )
```

### 数据消息类型

#### ObjectHeader

对象头部。

```python
from moq.messages import ObjectHeader, ObjectStatus

class ObjectHeader:
    def __init__(
        self,
        track_alias: int,          # 轨道别名
        group_id: int,             # 组ID
        object_id: int,            # 对象ID
        publisher_priority: int,   # 发布优先级 (0-255)
        object_status: ObjectStatus = ObjectStatus.NORMAL
    )

class ObjectStatus(IntEnum):
    NORMAL = 0x00           # 正常对象
    NON_EXISTENT = 0x01     # 不存在
    END_OF_GROUP = 0x02     # 组结束标记
    END_OF_TRACK = 0x03     # 轨道结束标记
    END_OF_SUBGROUP = 0x04  # 子组结束标记
```

#### ObjectDatagram

通过数据报发送的对象。

```python
from moq.messages import ObjectDatagram

class ObjectDatagram:
    def __init__(
        self,
        header: ObjectHeader,     # 对象头部
        payload: bytes,           # 负载
        extensions: Optional[bytes] = None  # 扩展
    )
```

#### SubgroupHeader

子组流头部。

```python
from moq.messages import SubgroupHeader

class SubgroupHeader:
    def __init__(
        self,
        track_alias: int,         # 轨道别名
        group_id: int,            # 组ID
        subgroup_id: int,         # 子组ID
        publisher_priority: int   # 优先级
    )
```

#### SubgroupObject

子组流中的对象。

```python
from moq.messages import SubgroupObject

class SubgroupObject:
    def __init__(
        self,
        object_id: int,                    # 对象ID
        payload: bytes,                    # 负载
        object_status: ObjectStatus = ObjectStatus.NORMAL
    )
```

---

## 传输模块

### QUICClient

QUIC 客户端。

```python
from moq.transport import QUICClient

class QUICClient:
    def __init__(
        self,
        host: str,              # 服务器地址
        port: int,              # 服务器端口
        use_datagrams: bool = True  # 是否启用数据报
    )
    
    def set_handlers(
        self,
        on_stream_data: Optional[Callable[[MOQQuicProtocol, StreamData], None]] = None,
        on_datagram: Optional[Callable[[MOQQuicProtocol, DatagramData], None]] = None,
        on_close: Optional[Callable[[MOQQuicProtocol, int, str], None]] = None
    )
    
    async def connect(self) -> bool
    def close()
    
    async def open_stream(self, unidirectional: bool = False) -> int
    async def send_stream_data(self, stream_id: int, data: bytes, end_stream: bool = False)
    async def send_datagram(self, data: bytes)
```

### QUICServer

QUIC 服务器。

```python
from moq.transport import QUICServer

class QUICServer:
    def __init__(
        self,
        host: str,
        port: int,
        use_datagrams: bool = True,
        cert_file: Optional[str] = None,
        key_file: Optional[str] = None
    )
    
    def set_handlers(
        self,
        on_client_connect: Optional[Callable] = None,
        on_stream_data: Optional[Callable] = None,
        on_datagram: Optional[Callable] = None,
        on_client_disconnect: Optional[Callable] = None
    )
    
    async def start()
    async def stop()
```

---

## 会话模块

### MOQSession

MOQ 会话管理。

```python
from moq.session import MOQSession, Role, SessionState

class MOQSession:
    def __init__(self, session_id: str, role: Role)
    
    # 设置回调
    def set_handlers(
        self,
        on_setup: Optional[Callable[[SetupMessage], None]] = None,
        on_subscribe: Optional[Callable[[SubscribeMessage], None]] = None,
        on_publish: Optional[Callable[[PublishMessage], None]] = None,
        on_fetch: Optional[Callable[[FetchMessage], None]] = None,
        on_close: Optional[Callable[[], None]] = None
    )
    
    def set_send_callback(self, callback: Callable[[bytes], None])
    
    # 发送消息
    async def send_setup(self, role: Optional[Role] = None)
    async def subscribe(...) -> int
    async def send_subscribe_ok(...)
    async def publish(self, track_name: FullTrackName) -> int
    async def send_publish_ok(self, request_id: int)
    async def send_publish_done(self, request_id: int, status_code: int, reason: str)
    async def fetch(...) -> int
    async def send_fetch_ok(...)
    async def send_request_error(self, request_id: int, error_code: ErrorCode, reason: str)
    
    # 查询
    def get_subscription(self, request_id: int) -> Optional[Subscription]
    def get_publication(self, request_id: int) -> Optional[Publication]
    
    # 关闭
    def close()

class Role(IntEnum):
    PUBLISHER = 0x01
    SUBSCRIBER = 0x02
    PUBSUB = 0x03

class SessionState(IntEnum):
    CONNECTING = 0
    HANDSHAKING = 1
    ACTIVE = 2
    CLOSING = 3
    CLOSED = 4
```

---

## 示例导航

如果你想直接运行代码而不是只看接口定义，优先参考：

- [`examples/README.md`](/home/acn/cxr/moq-py/examples/README.md)

推荐入口：

- [`examples/relay_example.py`](/home/acn/cxr/moq-py/examples/relay_example.py)
- [`examples/publisher_example.py`](/home/acn/cxr/moq-py/examples/publisher_example.py)
- [`examples/subscriber_example.py`](/home/acn/cxr/moq-py/examples/subscriber_example.py)
- [`examples/fetch_example.py`](/home/acn/cxr/moq-py/examples/fetch_example.py)
- [`examples/reconnection_example.py`](/home/acn/cxr/moq-py/examples/reconnection_example.py)

---

## 发布模块

### MOQPublisher

发布者实现。

```python
from moq.pub import MOQPublisher, PublishedObject

class MOQPublisher:
    def __init__(self, relay_host: str, relay_port: int)
    
    # 设置事件处理器
    def set_handlers(
        self,
        on_connected: Optional[Callable] = None,
        on_disconnected: Optional[Callable] = None,
        on_publication_accepted: Optional[Callable[[FullTrackName], None]] = None,
        on_publication_rejected: Optional[Callable[[FullTrackName, str], None]] = None
    )
    
    # 连接管理
    async def connect(self) -> bool
    def disconnect()
    
    # 发布管理
    async def publish(self, track_name: FullTrackName) -> bool
    async def unpublish(self, track_name: FullTrackName, reason: str = "")
    
    # 发送对象
    async def send_object(self, track_name: FullTrackName, obj: PublishedObject)
    async def close_subgroup_stream(self, track_alias: int, group_id: int, subgroup_id: int)
    
    # 查询状态
    def get_active_tracks(self) -> List[FullTrackName]
    def is_publishing(self, track_name: FullTrackName) -> bool

class PublishedObject:
    def __init__(
        self,
        group_id: int,
        object_id: int,
        payload: bytes,
        publisher_priority: int = 128,
        subgroup_id: int = 0,
        use_datagram: bool = False
    )
```

**示例**:
```python
# 创建发布者
publisher = MOQPublisher("relay.example.com", 4433)

# 连接
await publisher.connect()

# 发布轨道
track = FullTrackName([b"live"], b"stream1")
await publisher.publish(track)

# 发送对象
obj = PublishedObject(
    group_id=1,
    object_id=1,
    payload=b"video frame data",
    publisher_priority=128
)
await publisher.send_object(track, obj)

# 停止发布
await publisher.unpublish(track)
publisher.disconnect()
```

---

## 订阅模块

### MOQSubscriber

订阅者实现。

```python
from moq.sub import MOQSubscriber, ReceivedObject

class MOQSubscriber:
    def __init__(self, relay_host: str, relay_port: int)
    
    # 设置事件处理器
    def set_handlers(
        self,
        on_connected: Optional[Callable] = None,
        on_disconnected: Optional[Callable] = None,
        on_object_received: Optional[Callable[[ReceivedObject], None]] = None,
        on_subscription_accepted: Optional[Callable[[FullTrackName], None]] = None,
        on_subscription_rejected: Optional[Callable[[FullTrackName, str], None]] = None
    )
    
    # 连接管理
    async def connect(self) -> bool
    def disconnect()
    
    # 订阅管理
    async def subscribe(
        self,
        track_name: FullTrackName,
        subscriber_priority: int = 128,
        start_group: Optional[int] = None,
        start_object: Optional[int] = None
    ) -> bool
    
    async def unsubscribe(self, track_name: FullTrackName)
    
    # 获取特定范围
    async def fetch(
        self,
        track_name: FullTrackName,
        start_group: int,
        start_object: int,
        end_group: int,
        end_object: int,
        subscriber_priority: int = 128
    ) -> int
    
    # 查询状态
    def get_active_subscriptions(self) -> List[FullTrackName]
    def is_subscribed(self, track_name: FullTrackName) -> bool
    
    # 获取对象（阻塞）
    async def get_next_object(self, timeout: Optional[float] = None) -> Optional[ReceivedObject]

class ReceivedObject:
    track_alias: int
    group_id: int
    object_id: int
    publisher_priority: int
    payload: bytes
    object_status: ObjectStatus
```

**示例**:
```python
# 创建订阅者
subscriber = MOQSubscriber("relay.example.com", 4433)

# 设置回调
def on_object(obj: ReceivedObject):
    print(f"Received: group={obj.group_id}, object={obj.object_id}")
    print(f"Payload size: {len(obj.payload)}")

subscriber.set_handlers(on_object_received=on_object)

# 连接
await subscriber.connect()

# 订阅轨道
track = FullTrackName([b"live"], b"stream1")
await subscriber.subscribe(track)

# 或获取特定范围
await subscriber.fetch(track, 1, 1, 10, 100)

# 运行一段时间
await asyncio.sleep(60)

# 取消订阅
await subscriber.unsubscribe(track)
subscriber.disconnect()
```

---

## 中继模块

### MOQRelay

中继节点实现。

```python
from moq.relay import MOQRelay, ObjectCache, CachedObject

class MOQRelay:
    def __init__(
        self,
        host: str,
        port: int,
        cache_dir: Optional[str] = None,
        max_memory_cache: int = 100 * 1024 * 1024,
        max_disk_cache: int = 1024 * 1024 * 1024,
        cert_file: Optional[str] = None,
        key_file: Optional[str] = None
    )
    
    # 生命周期
    async def start()
    async def stop()
    
    # 会话管理
    def register_session(self, session: MOQSession)
    def unregister_session(self, session: MOQSession)
    
    # 缓存操作
    def cache_object(self, track_name: FullTrackName, obj: CachedObject)
    async def serve_cached_objects(
        self,
        session: MOQSession,
        track_name: FullTrackName,
        start: Location,
        end: Location
    )
    
    # 统计
    def get_cache_stats(self) -> dict
    
    # 事件处理器
    def set_object_handler(self, handler: Callable)
    def set_forward_handler(self, handler: Callable)

class ObjectCache:
    def __init__(
        self,
        max_memory_size: int = 100 * 1024 * 1024,
        disk_cache_dir: Optional[str] = None,
        max_disk_size: int = 1024 * 1024 * 1024
    )
    
    def put(self, track_name: FullTrackName, obj: CachedObject)
    def get(self, track_name: FullTrackName, location: Location) -> Optional[CachedObject]
    def get_range(
        self,
        track_name: FullTrackName,
        start: Location,
        end: Location
    ) -> List[CachedObject]
    def get_statistics(self) -> dict

class CachedObject:
    def __init__(
        self,
        track_alias: int,
        group_id: int,
        object_id: int,
        publisher_priority: int,
        payload: bytes
    )
    
    timestamp: datetime
    access_count: int
```

**示例**:
```python
# 创建中继
relay = MOQRelay(
    host="0.0.0.0",
    port=4433,
    cache_dir="/var/cache/moq",
    max_memory_cache=500 * 1024 * 1024,  # 500MB
    max_disk_cache=10 * 1024 * 1024 * 1024  # 10GB
)

# 启动
await relay.start()

# 检查缓存统计
stats = relay.get_cache_stats()
print(f"Memory: {stats['memory_size']} bytes")
print(f"Disk: {stats['disk_size']} bytes")
print(f"Hit rate: {stats['hit_rate']:.2%}")

# 停止
await relay.stop()
```

---

## 错误码

```python
from moq.messages import ErrorCode

class ErrorCode(IntEnum):
    INTERNAL_ERROR = 0x00           # 内部错误
    UNAUTHORIZED = 0x01             # 未授权
    PROTOCOL_VIOLATION = 0x02       # 协议违规
    DUPLICATE_TRACK_ALIAS = 0x03    # 重复的轨道别名
    PARAMETER_LENGTH_MISMATCH = 0x04 # 参数长度不匹配
    GOAWAY_TIMEOUT = 0x10           # GOAWAY 超时
    KEY_VALUE_FORMATTING_ERROR = 0xF0 # 键值对格式错误
```

---

## 常量

### 消息类型

```python
from moq.messages import MessageType

class MessageType(IntEnum):
    SETUP = 0x01
    REQUEST_OK = 0x02
    REQUEST_ERROR = 0x03
    SUBSCRIBE = 0x04
    SUBSCRIBE_OK = 0x05
    REQUEST_UPDATE = 0x06
    PUBLISH = 0x07
    PUBLISH_OK = 0x08
    PUBLISH_DONE = 0x09
    FETCH = 0x0A
    FETCH_OK = 0x0B
    TRACK_STATUS = 0x0C
    PUBLISH_NAMESPACE = 0x0D
    NAMESPACE = 0x0E
    NAMESPACE_DONE = 0x0F
    GOAWAY = 0x10
    SUBSCRIBE_NAMESPACE = 0x11
    PUBLISH_BLOCKED = 0x12
```

### 流类型

```python
from moq.messages import StreamType

class StreamType(IntEnum):
    OBJECT_DATAGRAM = 0x00
    SUBGROUP_HEADER = 0x01
    FETCH_HEADER = 0x02
```

---

## 工具函数

### 编解码辅助

```python
from moq.encoding import encode_bytes, decode_bytes

# 带长度前缀的编码
encoded = encode_bytes(b"data")

# 解码
data, consumed = decode_bytes(encoded)
```

### 消息解码

```python
from moq.messages import decode_control_message

# 解码任何控制消息
msg, consumed = decode_control_message(data)

# 根据类型处理
if isinstance(msg, SubscribeMessage):
    handle_subscribe(msg)
elif isinstance(msg, PublishMessage):
    handle_publish(msg)
```

---

## 类型注解示例

完整的类型注解使用示例：

```python
from typing import Optional, List, Callable
from moq.encoding import FullTrackName
from moq.pub import MOQPublisher, PublishedObject
from moq.sub import MOQSubscriber, ReceivedObject
from moq.relay import MOQRelay

async def setup_publisher(
    host: str,
    port: int,
    tracks: List[FullTrackName]
) -> MOQPublisher:
    """Setup a publisher with type annotations."""
    publisher: MOQPublisher = MOQPublisher(host, port)
    
    # Callback with proper types
    def on_pub_accepted(track: FullTrackName) -> None:
        print(f"Accepted: {track}")
    
    publisher.set_handlers(on_publication_accepted=on_pub_accepted)
    
    success: bool = await publisher.connect()
    if not success:
        raise ConnectionError("Failed to connect")
    
    return publisher
```

---

*文档版本: 0.1.0*
*最后更新: 2024*
