# MOQ视频传输快速开始

## 最简单的3行代码

```python
from moq_video_service import MOQVideoPublisher
pub = MOQVideoPublisher("127.0.0.1", 4433)
await pub.connect()
await pub.publish_video("video.mp4", "my-stream")
```

## 完整命令行操作

### 1. 生成测试视频
```bash
ffmpeg -f lavfi -i testsrc=duration=10:size=1920x1080:rate=30 \
       -pix_fmt yuv420p -c:v libx264 -b:v 4M -an \
       /tmp/test_1080p.mp4
```

### 2. 传输视频

**发送端**:
```bash
python3 << 'EOF'
import asyncio
from moq_video_service import MOQVideoPublisher, MOQVideoRelay

async def main():
    relay = MOQVideoRelay(host="127.0.0.1", port=4433)
    port = await relay.start()
    
    pub = MOQVideoPublisher("127.0.0.1", port)
    await pub.connect()
    
    stats = await pub.publish_video(
        "/tmp/test_1080p.mp4", 
        "test-stream"
    )
    
    print(f"✅ 发送完成: {stats.throughput_mbps:.1f} Mbps")
    pub.disconnect()
    await relay.stop()

asyncio.run(main())
EOF
```

**接收端**:
```bash
python3 << 'EOF'
import asyncio
from moq_video_service import MOQVideoSubscriber

async def main():
    sub = MOQVideoSubscriber("127.0.0.1", 4433)
    await sub.connect()
    
    stats = await sub.subscribe(
        "test-stream", 
        "/tmp/received.mp4",
        wait_time=30
    )
    
    print(f"✅ 接收完成: {stats.hash_match}")
    sub.disconnect()

asyncio.run(main())
EOF
```

## 测试运行

```bash
# 运行集成测试
python3 examples/simple_integration_test.py

# 运行1080p测试
python3 tests/test_real_video_transfer.py
```

**预期输出**:
```
✅ TEST PASSED: Video transmitted successfully!
Hash: 匹配
Throughput: 1500+ Mbps
```
