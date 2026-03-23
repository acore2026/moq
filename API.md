# MOQ Protocol API 文档

## 目录

1. [常量定义](#常量定义)
2. [MOQObject](#moqobject)
3. [MOQPublisher](#moqpublisher)
4. [MOQSubscriber](#moqsubscriber)
5. [MOQRelay](#moqrelay)
6. [缓存管理](#缓存管理)

---

## 常量定义

### MOQMessageType

消息类型常量:

```python
CLIENT_SETUP = 0x40
SERVER_SETUP = 0x41
ANNOUNCE = 0xC0
ANNOUNCE_OK = 0xC1
ANNOUNCE_ERROR = 0xC2
SUBSCRIBE = 0x80
SUBSCRIBE_OK = 0x81
SUBSCRIBE_ERROR = 0x82
UNSUBSCRIBE = 0x83
OBJECT_DATAGRAM = 0x01
```

### MOQRole

角色定义:

```python
PUBLISHER = 0x01
SUBSCRIBER = 0x02
PUB_SUB = 0x03
```

### MOQFilterType

订阅过滤器类型:

```python
LATEST_GROUP = 0x01      # 最新组
LATEST_OBJECT = 0x02     # 最新对象
ABSOLUTE_START = 0x03    # 绝对起始位置
ABSOLUTE_RANGE = 0x04    # 绝对范围
```

---

## MOQObject

媒体对象类。

### 属性

| 属性 | 类型 | 说明 |
|------|------|------|
| track_alias | int | 轨道别名 |
| group_id | int | 组标识 |
| object_id | int | 对象标识 |
| send_order | int | 发送优先级 |
| payload | bytes | 数据负载 |
| extensions | dict | 扩展数据 |
| timestamp | float | 时间戳 |

### 构造方法

```python
MOQObject(
    track_alias: int,
    group_id: int,
    object_id: int,
    send_order: int = 0,
    payload: bytes = b"",
    extensions: dict = None
)
```

### 方法

```python
def to_dict(self) -> dict
"""转换为字典"""

def object_size(self) -> int
"""获取对象大小"""
```

---

## MOQPublisher

发布者类。

### 构造方法

```python
MOQPublisher(
    session_config: Optional[SessionConfig] = None,
    publisher_config: Optional[PublisherConfig] = None,
    on_status: Optional[Callable[[str, Any], None]] = None
)
```

### 方法

#### connect

```python
async def connect(host: str, port: int, **kwargs) -> None
```

连接到中继服务器。

**参数:**
- `host`: 服务器地址
- `port`: 服务器端口

#### announce_namespace

```python
async def announce_namespace(*namespace: str) -> bool
```

声明命名空间。

**参数:**
- `*namespace`: 命名空间元组 (如 "example", "live")

**返回:** 是否成功

#### unannounce_namespace

```python
async def unannounce_namespace(*namespace: str) -> bool
```

取消声明命名空间。

#### publish_object

```python
async def publish_object(
    track: MOQTrack,
    group_id: int,
    object_id: int,
    data: bytes,
    send_order: int = 0,
    extensions: Optional[dict] = None
) -> bool
```

发布媒体对象。

**参数:**
- `track`: 轨道对象
- `group_id`: 组ID
- `object_id`: 对象ID
- `data`: 数据负载
- `send_order`: 发送优先级

**返回:** 是否成功

#### get_stats

```python
async def get_stats() -> dict
```

获取统计信息。

#### close

```python
async def close() -> None
```

关闭发布者。

---

## MOQSubscriber

订阅者类。

### 构造方法

```python
MOQSubscriber(
    session_config: Optional[SessionConfig] = None,
    subscriber_config: Optional[SubscriberConfig] = None,
    on_object: Optional[Callable[[MOQObject], None]] = None,
    on_status: Optional[Callable[[str, Any], None]] = None
)
```

### 方法

#### connect

```python
async def connect(host: str, port: int, **kwargs) -> None
```

连接到中继服务器。

#### subscribe

```python
async def subscribe(
    *namespace: str,
    track_name: str,
    filter_type: int = MOQFilterType.LATEST_GROUP,
    start_group: Optional[int] = None,
    start_object: Optional[int] = None,
    end_group: Optional[int] = None,
    end_object: Optional[int] = None,
    on_object: Optional[Callable[[MOQObject], None]] = None
) -> MOQSubscription
```

订阅轨道。

**参数:**
- `*namespace`: 命名空间
- `track_name`: 轨道名称
- `filter_type`: 过滤器类型
- `start_group`: 起始组 (ABSOLUTE 类型)
- `start_object`: 起始对象 (ABSOLUTE 类型)
- `end_group`: 结束组 (RANGE 类型)
- `end_object`: 结束对象 (RANGE 类型)
- `on_object`: 对象接收回调

**返回:** MOQSubscription 对象

#### unsubscribe

```python
async def unsubscribe(subscription: MOQSubscription) -> bool
```

取消订阅。

#### receive_objects

```python
async def receive_objects(subscription: MOQSubscription) -> AsyncIterator[MOQObject]
```

异步迭代器接收对象。

**示例:**
```python
async for obj in subscriber.receive_objects(subscription):
    print(f"Received: {obj.payload}")
```

---

## MOQRelay

中继服务器类。

### 构造方法

```python
MOQRelay(
    session_config: Optional[SessionConfig] = None,
    relay_config: Optional[RelayConfig] = None,
    on_event: Optional[Callable[[str, Any], None]] = None
)
```

### 方法

#### start

```python
async def start(host: Optional[str] = None, port: Optional[int] = None) -> None
```

启动中继服务器。

**参数:**
- `host`: 绑定地址 (默认 0.0.0.0)
- `port`: 绑定端口 (默认 4433)

#### stop

```python
async def stop() -> None
```

停止中继服务器。

#### get_stats

```python
async def get_stats() -> dict
```

获取统计信息。

---

## 缓存管理

### MOQCacheManager

```python
MOQCacheManager(
    memory_cache_size: int = 100 * 1024 * 1024,
    disk_cache_size: int = 1024 * 1024 * 1024,
    disk_cache_dir: str = ".moq_cache",
    use_disk_cache: bool = True
)
```

### 方法

#### initialize

```python
async def initialize() -> None
```

初始化缓存管理器。

#### store

```python
async def store(
    obj: MOQObject,
    serialized_data: bytes,
    prefer_memory: bool = True
) -> bool
```

存储对象到缓存。

#### retrieve

```python
async def retrieve(
    track_alias: int,
    group_id: int,
    object_id: int
) -> Optional[bytes]
```

从缓存检索对象。

#### get_stats

```python
def get_stats() -> dict
```

获取缓存统计信息。
