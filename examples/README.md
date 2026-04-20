# MOQ 视频传输服务接口

本目录提供了多种方式供其他应用调用MOQ视频传输功能。

## 📁 文件说明

- `moq_video_service.py` - 核心视频传输服务类
- `video_transfer_example.py` - 命令行使用示例
- `http_api_server.py` - HTTP REST API服务
- `websocket_streaming.py` - WebSocket实时流传输
- `websocket_player.html` - WebSocket播放器页面

## 🚀 快速开始

### 1. 基础视频传输（Python API）

最简单的使用方式，直接导入Python类：

```python
from moq_video_service import MOQVideoPublisher, MOQVideoSubscriber, MOQVideoRelay

# 1. 启动Relay
relay = MOQVideoRelay(host="127.0.0.1", port=4433)
port = await relay.start()

# 2. 发布视频
pub = MOQVideoPublisher("127.0.0.1", port)
await pub.connect()
stats = await pub.publish_video("/path/to/video.mp4", track_name="my-video")
print(f"Published: {stats.to_dict()}")
pub.disconnect()

# 3. 订阅视频
sub = MOQVideoSubscriber("127.0.0.1", port)
await sub.connect()
stats = await sub.subscribe("my-video", output_path="/path/to/output.mp4")
print(f"Received: {stats.to_dict()}")
sub.disconnect()

# 4. 停止Relay
await relay.stop()
```

### 2. 命令行工具

使用命令行进行视频传输：

```bash
# 1. 启动Relay服务
python video_transfer_example.py relay

# 2. 发布视频（在另一个终端）
python video_transfer_example.py pub --video /path/to/video.mp4 --track my-video

# 3. 订阅视频（在另一个终端）
python video_transfer_example.py sub --track my-video --output /path/to/output.mp4

# 4. 自动测试（生成视频并传输）
python video_transfer_example.py test --duration 10 --resolution 1080p
```

### 3. HTTP REST API

启动HTTP API服务：

```bash
python http_api_server.py --api-port 8080 --relay-port 4433
```

API端点：

#### 发布视频
```bash
POST http://localhost:8080/api/publish
Content-Type: application/json

{
    "video_path": "/path/to/video.mp4",
    "track_name": "my-video"
}
```

**响应示例：**
```json
{
    "track_name": "my-video",
    "status": "completed",
    "bytes_sent": 1980375,
    "bytes_received": 0,
    "chunks_sent": 121,
    "chunks_received": 0,
    "duration": 0.523,
    "throughput_mbps": 30.25,
    "loss_rate": 0.0,
    "original_hash": "fceef509ef921144299662d207b4dd88c9b7a2a49edadb6b902c8a310d312855",
    "received_hash": null,
    "hash_match": null
}
```

#### 订阅视频
```bash
POST http://localhost:8080/api/subscribe
Content-Type: application/json

{
    "track_name": "my-video",
    "output_path": "/path/to/output.mp4",
    "wait_time": 15.0
}
```

**响应示例：**
```json
{
    "track_name": "my-video",
    "status": "completed",
    "bytes_sent": 0,
    "bytes_received": 1980375,
    "chunks_sent": 121,
    "chunks_received": 121,
    "duration": 12.345,
    "throughput_mbps": 1.28,
    "loss_rate": 0.0,
    "original_hash": null,
    "received_hash": "fceef509ef921144299662d207b4dd88c9b7a2a49edadb6b902c8a310d312855",
    "hash_match": true
}
```

#### 获取传输状态
```bash
GET http://localhost:8080/api/status/my-video
```

#### 健康检查
```bash
GET http://localhost:8080/api/health
```

### 4. WebSocket实时流传输

启动WebSocket服务器：

```bash
python websocket_streaming.py --ws-port 8765 --relay-port 4433
```

**WebSocket协议：**

连接：`ws://localhost:8765/ws`

#### 消息格式：

**订阅视频流：**
```json
{
    "type": "subscribe",
    "stream_id": "mystream"
}
```

**发布视频流：**
```json
{
    "type": "publish",
    "stream_id": "mystream"
}
```

**取消订阅：**
```json
{
    "type": "unsubscribe",
    "stream_id": "mystream"
}
```

**二进制数据：** 直接发送视频数据块（ArrayBuffer）

#### 使用浏览器播放：

1. 启动WebSocket服务器
2. 在浏览器中打开 `websocket_player.html`
3. 输入WebSocket地址和Stream ID
4. 点击 "Connect" 开始接收视频流

## 📊 接口对比

| 接口类型 | 适用场景 | 延迟 | 易用性 | 扩展性 |
|---------|---------|------|--------|--------|
| Python API | 系统集成、批处理 | 低 | ⭐⭐⭐⭐⭐ | ⭐⭐⭐⭐⭐ |
| 命令行 | 测试、脚本 | 低 | ⭐⭐⭐⭐ | ⭐⭐⭐ |
| HTTP API | Web应用、跨语言 | 中 | ⭐⭐⭐⭐ | ⭐⭐⭐⭐ |
| WebSocket | 实时流、直播 | 极低 | ⭐⭐⭐ | ⭐⭐⭐⭐ |

## 🔧 高级配置

### Publisher配置

```python
pub = MOQVideoPublisher(
    relay_host="127.0.0.1",
    relay_port=4433,
    chunk_size=16384,  # 每个数据块大小（默认16KB）
    agent_id="my-publisher"  # 自定义agent ID
)

# 设置进度回调
def on_progress(stats):
    print(f"Progress: {stats.chunks_sent} chunks, {stats.throughput_mbps:.2f} Mbps")

pub.set_progress_callback(on_progress)
```

### Subscriber配置

```python
sub = MOQVideoSubscriber(
    relay_host="127.0.0.1",
    relay_port=4433,
    agent_id="my-subscriber"
)

# 设置进度回调
sub.set_progress_callback(on_progress)

# 设置接收完成回调
def on_complete(track_name, data):
    print(f"Received: {track_name}, {len(data)} bytes")

sub.set_complete_callback(on_complete)
```

### Relay配置

```python
relay = MOQVideoRelay(
    host="0.0.0.0",
    port=4433,
    cert_file="/path/to/cert.pem",  # 自定义证书（可选）
    key_file="/path/to/key.pem",     # 自定义密钥（可选）
    max_memory_cache=500 * 1024 * 1024  # 500MB缓存
)
```

## 📈 返回的统计信息

所有接口都会返回 `VideoTransferStats` 对象，包含以下信息：

| 字段 | 说明 |
|------|------|
| track_name | 轨道名称 |
| status | 传输状态（pending/connecting/transferring/completed/failed） |
| bytes_sent | 发送字节数 |
| bytes_received | 接收字节数 |
| chunks_sent | 发送的数据块数量 |
| chunks_received | 接收的数据块数量 |
| duration | 传输持续时间（秒） |
| throughput_mbps | 吞吐量（Mbps） |
| loss_rate | 丢包率（0-1） |
| original_hash | 原始数据SHA256哈希 |
| received_hash | 接收数据SHA256哈希 |
| hash_match | 哈希是否匹配（数据完整性） |

## 📝 完整示例代码

### Publisher示例

```python
import asyncio
from moq_video_service import MOQVideoPublisher

async def main():
    # 创建Publisher
    pub = MOQVideoPublisher("127.0.0.1", 4433)
    
    # 连接
    if not await pub.connect():
        print("Failed to connect")
        return
    
    try:
        # 发布视频
        stats = await pub.publish_video(
            video_path="/path/to/video.mp4",
            track_name="stream-1",
            namespace=["live", "channel1"]
        )
        
        print(f"Published: {stats.chunks_sent} chunks")
        print(f"Duration: {stats.duration:.3f}s")
        print(f"Throughput: {stats.throughput_mbps:.2f} Mbps")
        print(f"Hash: {stats.original_hash}")
        
    finally:
        pub.disconnect()

asyncio.run(main())
```

### Subscriber示例

```python
import asyncio
from moq_video_service import MOQVideoSubscriber

async def main():
    # 创建Subscriber
    sub = MOQVideoSubscriber("127.0.0.1", 4433)
    
    # 连接
    if not await sub.connect():
        print("Failed to connect")
        return
    
    try:
        # 订阅视频
        stats = await sub.subscribe(
            track_name="stream-1",
            output_path="/path/to/output.mp4",
            namespace=["live", "channel1"],
            wait_time=30.0
        )
        
        print(f"Received: {stats.chunks_received} chunks")
        print(f"Duration: {stats.duration:.3f}s")
        print(f"Throughput: {stats.throughput_mbps:.2f} Mbps")
        print(f"Loss rate: {stats.loss_rate*100:.2f}%")
        print(f"Hash match: {stats.hash_match}")
        
    finally:
        sub.disconnect()

asyncio.run(main())
```

## 🔗 相关链接

- [MOQ Protocol](https://datatracker.ietf.org/doc/draft-ietf-moq-transport/)
- [Python asyncio](https://docs.python.org/3/library/asyncio.html)
- [aiohttp](https://docs.aiohttp.org/)
