# MOQ视频传输完整使用指南

## 快速开始

### 1. 基础视频传输（推荐）

#### 发送端（Publisher）

```python
import asyncio
from moq_video_service import MOQVideoPublisher, MOQVideoRelay

async def send_video():
    # 1. 启动Relay（如果是本地测试）
    relay = MOQVideoRelay(host="127.0.0.1", port=4433)
    relay_port = await relay.start()
    
    # 2. 创建Publisher
    pub = MOQVideoPublisher(
        relay_host="127.0.0.1",
        relay_port=relay_port,
        chunk_size=16384  # 16KB分块
    )
    
    # 3. 连接
    await pub.connect()
    
    # 4. 发布视频
    stats = await pub.publish_video(
        video_path="/path/to/video.mp4",
        track_name="my-video-stream",
        namespace=["video", "live"]
    )
    
    print(f"发送完成: {stats.chunks_sent} chunks, {stats.throughput_mbps:.2f} Mbps")
    
    # 5. 断开
    pub.disconnect()
    await relay.stop()

# 运行
asyncio.run(send_video())
```

#### 接收端（Subscriber）

```python
import asyncio
from moq_video_service import MOQVideoSubscriber

async def receive_video():
    # 1. 创建Subscriber
    sub = MOQVideoSubscriber(
        relay_host="127.0.0.1",
        relay_port=4433
    )
    
    # 2. 连接
    await sub.connect()
    
    # 3. 订阅视频
    stats = await sub.subscribe(
        track_name="my-video-stream",
        output_path="/path/to/output.mp4",
        namespace=["video", "live"],
        wait_time=30.0  # 等待30秒
    )
    
    print(f"接收完成: {stats.chunks_received} chunks, Hash匹配: {stats.hash_match}")
    
    # 4. 断开
    sub.disconnect()

# 运行
asyncio.run(receive_video())
```

---

### 2. 使用ACN SDK传输视频

#### 方式1: 使用增强版客户端（推荐）

```python
import asyncio
from acn_sdk import AcnSDK, AgentInfo
from acn_sdk.network.moq_client_chunked import MoQClientChunked

async def sdk_send_video():
    # 1. 初始化SDK
    sdk = AcnSDK(
        agent_name="VideoStreamer",
        config_path="config.yaml"
    )
    
    # 2. 注册Agent
    ok, agent_id = sdk.register_agent_info(AgentInfo(
        name="VideoStreamer",
        owner="13800138000",
        description="Video Streaming Agent"
    ))
    
    # 3. 使用分块客户端发送视频
    # 获取MOQ发布客户端
    moq_client = MoQClientChunked(
        host=sdk.config.network.network_ip,
        remote_port=sdk.config.network.network_port,
        role="pub",
        chunk_size=16384  # 16KB分块
    )
    
    moq_client.connect()
    moq_client.publish("video", "1080p-stream")
    
    # 4. 读取并发送视频
    with open("/path/to/1080p_video.mp4", "rb") as f:
        video_data = f.read()
    
    # 大文件自动分块发送
    moq_client.send_object("video", "1080p-stream", video_data)
    
    moq_client.disconnect()

# 运行
asyncio.run(sdk_send_video())
```

#### 方式2: 使用SDK内置方法

```python
from acn_sdk import AcnSDK, AgentInfo

def send_video_with_sdk():
    # 1. 初始化SDK
    sdk = AcnSDK(
        agent_name="VideoStreamer",
        config_path="config.yaml"
    )
    
    # 2. 注册
    ok, agent_id = sdk.register_agent_info(AgentInfo(...))
    
    # 3. 创建任务
    ok, task_id = sdk.create_task(
        agent_id=agent_id,
        task_type="Video",
        description="1080p video stream"
    )
    
    # 4. 发送视频数据（使用SDK的task_info_report）
    with open("/path/to/video.mp4", "rb") as f:
        video_data = f.read()
    
    # 分块发送
    chunk_size = 16384
    for i in range(0, len(video_data), chunk_size):
        chunk = video_data[i:i + chunk_size]
        sdk.task_info_report(
            agent_id=agent_id,
            task_id=task_id,
            info_type="Video",
            info_content=chunk
        )

# 运行
send_video_with_sdk()
```

---

### 3. 使用Production Demo

#### 发送端

```bash
cd /home/acn/zqm/acn_sdk

# 使用production_video_streamer.py
export VIDEO_PATH="/path/to/1080p_video.mp4"
python3 examples/production_video_streamer.py

# 或使用demo_task_initiator_video_production.py
python3 examples/demo_task_initiator_video_production.py \
    --video /path/to/1080p_video.mp4 \
    --fps 30 \
    --bitrate 4M \
    --target-agent "receiver-agent-id"
```

#### 接收端（WebUI）

```bash
cd /root/lpx/webui

# 启动webui后端
python3 -m uvicorn app.main:app --host 0.0.0.0 --port 9005 --log-level info
```

---

### 4. 实时流传输

```python
import asyncio
import subprocess
from moq_video_service import MOQVideoPublisher

async def stream_live_video():
    """实时流式传输视频"""
    
    # 1. 创建Publisher
    pub = MOQVideoPublisher("127.0.0.1", 4433)
    await pub.connect()
    
    # 2. 启动FFmpeg实时编码
    cmd = [
        "ffmpeg",
        "-f", "lavfi", "-i", "testsrc=duration=60:size=1920x1080:rate=30",
        "-pix_fmt", "yuv420p",
        "-c:v", "libx264",
        "-preset", "ultrafast",
        "-b:v", "4M",
        "-f", "h264", "-"
    ]
    
    process = subprocess.Popen(cmd, stdout=subprocess.PIPE)
    
    # 3. 实时读取并发送
    chunk_size = 16384
    chunk_id = 0
    
    while True:
        data = process.stdout.read(chunk_size)
        if not data:
            break
        
        # 发送chunk
        await pub.publish_bytes(
            data=data,
            track_name="live-stream",
            namespace=["live", "stream"]
        )
        
        chunk_id += 1
        print(f"Sent chunk {chunk_id}")
    
    process.terminate()
    pub.disconnect()

# 运行
asyncio.run(stream_live_video())
```

---

### 5. HTTP API方式

```bash
# 1. 启动HTTP API服务
python3 examples/http_api_server.py

# 2. 发送视频（POST请求）
curl -X POST http://localhost:8080/api/publish \
    -H "Content-Type: application/json" \
    -d '{
        "video_path": "/path/to/video.mp4",
        "track_name": "my-video"
    }'

# 3. 接收视频（POST请求）
curl -X POST http://localhost:8080/api/subscribe \
    -H "Content-Type: application/json" \
    -d '{
        "track_name": "my-video",
        "output_path": "/path/to/output.mp4"
    }'
```

---

### 6. WebSocket实时播放

```bash
# 1. 启动WebSocket服务器
python3 examples/websocket_streaming.py

# 2. 推流
python3 examples/websocket_streaming.py --mode push \
    --video /path/to/video.mp4 \
    --stream mystream

# 3. 在浏览器中打开 websocket_player.html
# 输入: ws://localhost:8765/ws
# 输入Stream ID: mystream
```

---

## 关键配置参数

| 参数 | 推荐值 | 说明 |
|------|--------|------|
| chunk_size | 16384 (16KB) | 视频数据分块大小 |
| idle_timeout | 300秒 | QUIC连接空闲超时 |
| heartbeat_interval | 25秒 | 心跳间隔 |
| video_bitrate | 4M (1080p) | 视频码率 |
| target_fps | 30 | 目标帧率 |

---

## 完整示例代码

### 示例1: 1080p视频传输

```python
#!/usr/bin/env python3
"""1080p视频传输示例"""

import asyncio
from moq_video_service import (
    MOQVideoPublisher, 
    MOQVideoSubscriber, 
    MOQVideoRelay
)

async def main():
    # 1. 启动Relay
    relay = MOQVideoRelay(host="127.0.0.1", port=4433)
    relay_port = await relay.start()
    print(f"Relay started on port {relay_port}")
    
    # 2. 创建Subscriber（先启动，等待接收）
    sub = MOQVideoSubscriber("127.0.0.1", relay_port)
    await sub.connect()
    
    # 3. 创建Publisher
    pub = MOQVideoPublisher("127.0.0.1", relay_port)
    await pub.connect()
    
    # 4. 同时发送和接收
    sub_task = asyncio.create_task(
        sub.subscribe("1080p-video", "/tmp/received.mp4", wait_time=30)
    )
    
    await asyncio.sleep(1)  # 等待订阅建立
    
    pub_stats = await pub.publish_video(
        "/path/to/1080p_video.mp4",
        "1080p-video"
    )
    
    sub_stats = await sub_task
    
    # 5. 结果
    print(f"发送: {pub_stats.chunks_sent} chunks")
    print(f"接收: {sub_stats.chunks_received} chunks")
    print(f"Hash匹配: {sub_stats.hash_match}")
    
    # 6. 清理
    pub.disconnect()
    sub.disconnect()
    await relay.stop()

if __name__ == "__main__":
    asyncio.run(main())
```

---

## 故障排除

### Q1: 连接60秒断开？
```python
# 确保使用修改后的MOQ代码（包含PING帧保活）
# 检查日志是否有: "Sent heartbeat (QUIC PING frame)"
```

### Q2: 视频传输失败？
```python
# 检查: 
# 1. chunk_size是否合适（推荐16KB）
# 2. 视频文件是否存在
# 3. Relay是否正常运行
# 4. namespace和track_name是否匹配
```

### Q3: 接收端没有数据？
```python
# 检查:
# 1. 订阅是否在发送之前建立
# 2. 是否有防火墙阻挡
# 3. 查看日志: "_on_object_received"
```

---

## 相关文件

- `moq_video_service.py` - 视频传输服务接口
- `examples/simple_integration_test.py` - 简单集成测试
- `examples/video_transfer_example.py` - 命令行工具
- `tests/test_real_video_transfer.py` - 1080p传输测试

**最后更新**: 2026-04-16
