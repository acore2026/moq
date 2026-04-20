# MOQ视频传输集成指南

## 修改说明

### 发送端修改 (`demo_task_initiator_video_production_fixed.py`)

#### 核心改进
1. **使用MoQClientChunked**: 替代原有的SDK `task_info_report`，支持自动分块
2. **16KB分块传输**: 大VideoFrame自动分割成16KB的chunks
3. **保持VideoFrame格式**: 仍然使用52字节头部 + H264数据的结构化格式

#### 发送流程
```
VideoFrame (头部52B + H264数据)
    ↓
MoQClientChunked自动分块
    ↓
chunk_1 (16KB) → group_id=frame_id, object_id=0
chunk_2 (16KB) → group_id=frame_id, object_id=1
...
chunk_n (剩余) → group_id=frame_id, object_id=n-1
    ↓
MOQ Relay
    ↓
Subscriber接收并重组
```

#### 使用方法
```bash
cd /home/acn/zqm/acn_sdk

python3 examples/demo_task_initiator_video_production_fixed.py \
    --video /path/to/1080p_video.mp4 \
    --width 1920 \
    --height 1080 \
    --fps 30 \
    --bitrate 4M
```

---

### 接收端修改 (`webui/backend/app/moq_video.py`)

#### 核心改进
1. **分块重组**: 接收chunks并按(group_id, object_id)排序重组
2. **VideoFrame解析**: 重组后解析VideoFrame头部，提取纯H264数据
3. **增强日志**: 详细的接收和解析日志

#### 接收流程
```
MOQ Relay
    ↓
接收chunks
    ↓
按group_id排序重组
    ↓
解析VideoFrame头部
    ↓
提取纯H264数据
    ↓
回调给main.py
    ↓
转发给前端显示
```

#### 关键代码
```python
def _on_object_received(self, obj: ReceivedObject):
    # 使用group_id作为frame_id, object_id作为chunk_id
    frame_id = obj.group_id
    chunk_id = obj.object_id
    
    # 缓存chunks
    self._chunk_buffers[track_id][frame_id][chunk_id] = obj.payload
    
    # 重组完整帧
    sorted_chunks = [chunks[i] for i in sorted(chunks.keys())]
    reassembled_data = b"".join(sorted_chunks)
    
    # 解析VideoFrame
    video_frame = try_parse_video_frame(reassembled_data)
    if video_frame:
        # 提取纯H264数据
        h264_data = video_frame.data
        ...
```

---

## 运行流程

### 1. 启动Relay
```bash
# 在acn_sdk目录
python3 -c "
from moq import MOQRelay
import asyncio

async def main():
    relay = MOQRelay(host='127.0.0.1', port=4433)
    await relay.start()
    print('Relay started')
    await asyncio.sleep(3600)

asyncio.run(main())
"
```

### 2. 启动WebUI接收端
```bash
cd /root/lpx/webui

# 确保修改已生效
python3 -c "from backend.app.video_frame_parser import try_parse_video_frame; print('OK')"

# 启动webui
python3 -m uvicorn app.main:app --host 0.0.0.0 --port 9005 --log-level info
```

### 3. 启动发送端
```bash
cd /home/acn/zqm/acn_sdk

# 生成测试视频
ffmpeg -f lavfi -i testsrc=duration=10:size=1920x1080:rate=30 \
       -pix_fmt yuv420p -c:v libx264 -b:v 4M -an \
       /tmp/test_1080p.mp4

# 使用修改后的发送端
python3 examples/demo_task_initiator_video_production_fixed.py \
    --video /tmp/test_1080p.mp4 \
    --width 1920 \
    --height 1080 \
    --fps 30 \
    --bitrate 4M
```

---

## 预期日志输出

### 发送端日志
```
[视频发送] 连接MOQ...
[视频发送] 发布track: //task-xxx/agent-xxx/video
[视频发送] 已通知Agent GW: PUBLISH_TRACK
[视频发送] 启动编码器: /tmp/test_1080p.mp4
FFmpeg编码器已启动: 1920x1080@30fps, bitrate=4M

[发送] 关键帧 #1 (52345B, GOP=1)
[发送] 帧 #2 (15432B)
[发送] 帧 #3 (14892B)
...
[发送] 帧 #30 (14231B) - 平均码率: 3.8Mbps
```

### 接收端日志
```
[MOQ] _on_object_received: track_alias=0, group_id=1, object_id=0, payload_len=16384
[MOQ] Frame 1: received chunk 0, total_chunks=1, total_size=16384
[MOQ] _on_object_received: track_alias=0, group_id=1, object_id=1, payload_len=16384
[MOQ] Frame 1: received chunk 1, total_chunks=2, total_size=32768
...
[MOQ] Reassembled frame 1: 52345 bytes
[MOQ] Parsed VideoFrame: frame_id=1, gop_id=1, 1920x1080, fps=30, keyframe=True, data_size=52293
MOQ frame received: track_id=xxx frame_id=1 payload_bytes=52293 total_frames=1
```

---

## 数据格式

### VideoFrame结构
```
[Header 52字节]
  - version (1B): 1
  - flags (1B): KEYFRAME=0x01
  - frame_type (1B): IDR=0, P=1
  - reserved (1B): 0
  - timestamp (8B): μs
  - pts (8B): μs
  - dts (8B): μs
  - frame_id (4B)
  - gop_id (4B)
  - width (4B): 1920
  - height (4B): 1080
  - fps (4B): 30
  - bitrate (4B): 4000000

[Data N字节]
  - H264编码数据
```

### MOQ传输格式
```
Object Header:
  - track_alias (varint)
  - group_id (varint): frame_id
  - object_id (varint): chunk_id
  - payload_length (varint)

Payload:
  - VideoFrame chunk data (max 16KB)
```

---

## 故障排除

### Q1: 发送端报错"MoQ publisher is not connected"
**A**: 确保：
1. Relay已启动
2. 网络配置正确
3. SDK已join_network()

### Q2: 接收端收到数据但不显示
**A**: 检查：
1. VideoFrame解析是否正确
2. H264数据是否完整提取
3. 前端解码器是否支持

### Q3: 视频卡顿或花屏
**A**: 
1. 检查分块重组是否正确
2. 确认chunks按顺序重组
3. 查看丢包情况

---

## 相关文件

- `demo_task_initiator_video_production_fixed.py` - 修改后的发送端
- `webui/backend/app/moq_video.py` - 修改后的接收端
- `webui/backend/app/video_frame_parser.py` - VideoFrame解析器
- `webui/backend/app/main.py` - WebUI主程序

**最后更新**: 2026-04-16
