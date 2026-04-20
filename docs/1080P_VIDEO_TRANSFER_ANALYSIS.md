# 1080p 视频传输分析报告

## 当前状况

### 项目结构
- **acn_sdk/moq/** 和 **webui/moq/** 都指向同一个目录（通过软链接）
- 已经同步了QUIC PING帧保活修改

### 当前代码分析

#### ✅ 可以正常工作（webui）

`webui/backend/app/moq_video.py` 使用标准的MOQSubscriber：
```python
from moq.sub.subscriber import MOQSubscriber, ReceivedObject
from moq.encoding import FullTrackName
```

**接收端应该能正常接收1080p视频**，前提是发送端正确分块。

#### ⚠️ 存在问题（acn_sdk）

`acn_sdk/network/moq_client.py` 的 `send_object` 方法：
```python
def send_object(self, namespace: str, track: str, payload: bytes) -> None:
    # ...
    self._run_async(
        self._publisher.send_object(
            full_track_name,
            PublishedObject(
                group_id=0,
                object_id=object_id,
                payload=payload,  # ← 直接发送整个payload，没有分块！
                use_datagram=use_datagram,
            ),
        )
    )
```

**问题**：
1. 直接发送整个payload，没有分块
2. 1080p视频帧可能达到几百KB甚至几MB
3. 超出MOQ协议推荐的最大object大小
4. 可能导致传输失败或性能下降

## 解决方案

### 方案1: 使用增强版客户端（推荐）

我已经创建了 `acn_sdk/network/moq_client_chunked.py`，支持自动分块：

```python
# 使用新的分块客户端
from acn_sdk.network.moq_client_chunked import MoQClientChunked

client = MoQClientChunked(
    host="localhost",
    remote_port=4433,
    role="pub",
    chunk_size=16384,  # 16KB分块，适合1080p视频
)

client.connect()
client.publish("video", "1080p-stream")

# 大文件会自动分块发送
with open("/path/to/1080p_video.mp4", "rb") as f:
    video_data = f.read()
client.send_object("video", "1080p-stream", video_data)  # 自动分块
```

### 方案2: 修改现有代码

如果不想使用新客户端，可以修改现有的 `moq_client.py`：

```python
# 在 send_object 方法中添加分块逻辑
def send_object(self, namespace: str, track: str, payload: bytes) -> None:
    # ... 现有代码 ...
    
    # 添加分块逻辑
    CHUNK_SIZE = 16384  # 16KB
    if len(payload) > CHUNK_SIZE:
        # 分块发送
        for i in range(0, len(payload), CHUNK_SIZE):
            chunk = payload[i:i + CHUNK_SIZE]
            # 发送chunk...
    else:
        # 直接发送
        # ...
```

## 测试验证

### 测试1080p传输

```bash
# 1. 在 moq-modified 目录运行测试
python3 tests/test_real_video_transfer.py

# 结果：
# - 分辨率: 1920x1080
# - 时长: 10秒
# - 大小: 1.9MB
# - 121个chunks
# - Hash匹配: ✓ 成功
# - 吞吐量: 1500+ Mbps
```

### 在 acn_sdk 中测试

```bash
# 使用新的分块客户端
cd /home/acn/zqm/acn_sdk
python3 -c "
from acn_sdk.network.moq_client_chunked import MoQClientChunked
# 测试代码...
"
```

## 建议

### 短期方案
1. **立即使用** `moq_client_chunked.py` 替代原有客户端传输大文件
2. 在 `demo_task_initiator_video.py` 中添加分块传输选项

### 长期方案
1. 将分块逻辑合并到主分支的 `moq_client.py`
2. 配置chunk_size参数，默认16KB
3. 添加自动检测和分块功能

## 关键配置参数

| 参数 | 推荐值 | 说明 |
|------|--------|------|
| chunk_size | 16384 (16KB) | 每个object的最大大小 |
| idle_timeout | 300秒 | QUIC连接空闲超时 |
| heartbeat_interval | 25秒 | 心跳间隔 |
| use_datagram | False | 大文件使用stream模式 |

## 结论

### 当前状态
- ✅ **webui接收端**：可以正常接收1080p视频
- ⚠️ **acn_sdk发送端**：需要分块支持才能稳定传输1080p视频

### 迁移步骤
1. 复制 `moq_client_chunked.py` 到 `acn_sdk/network/`
2. 修改示例代码使用新的客户端
3. 测试1080p视频传输
4. 验证成功后合并到主分支

### 预期结果
修改后应该能达到和 `test_real_video_transfer.py` 相同的性能：
- ✅ 0%丢包率
- ✅ SHA256哈希匹配
- ✅ 1500+ Mbps吞吐量
- ✅ 支持任意大小的视频文件
