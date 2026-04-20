# MOQ 真实视频传输稳定性测试报告

## 测试概述

本测试使用FFmpeg生成真实视频数据，通过MOQ (Media over QUIC Transport) 协议的pub-relay-sub架构进行传输，并通过对比原始视频与接收视频的SHA256哈希值来验证传输的完整性。

## 测试环境

- **MOQ协议版本**: draft-ietf-moq-transport-17
- **传输协议**: QUIC (基于aioquic)
- **测试框架**: Python 3.10 + asyncio
- **视频生成**: FFmpeg 4.4.2

## 测试配置

| 参数 | 值 |
|------|-----|
| 视频时长 | 5秒 |
| 分辨率 | 640x480 |
| 帧率 | 30fps |
| 视频码率 | 1M |
| 音频 | AAC 128k |
| Chunk大小 | 8192 bytes |

## 测试流程

1. **视频生成**: 使用FFmpeg的testsrc生成测试视频，包含时间戳和移动图形
2. **视频分割**: 将视频文件分割成多个chunk（每个约8KB）
3. **启动Relay**: 在本地启动MOQ Relay服务器
4. **Publisher连接**: Publisher连接到Relay并发布track
5. **Subscriber连接**: Subscriber连接到Relay并订阅track
6. **数据传输**: Publisher通过MOQ协议发送视频chunk
7. **数据接收**: Subscriber接收chunk并按(group_id, object_id)排序
8. **完整性验证**: 对比原始视频和接收视频的SHA256哈希值

## 测试结果

### 单次测试结果

```
原始视频Hash (SHA256): 47ce9e0049c2c0e7dfc5427b86d57fd1870e3fc3ebd0398590e99a3f9d739db5
接收视频Hash (SHA256): 47ce9e0049c2c0e7dfc5427b86d57fd1870e3fc3ebd0398590e99a3f9d739db5
Hash匹配: ✓ 成功

传输统计:
  - Chunks发送: 49
  - Chunks接收: 49
  - 丢失率: 0.00%
  - 数据发送: 399,765 bytes
  - 数据接收: 399,765 bytes
  - 传输时间: 0.005 秒
  - 吞吐量: 992.22 Mbps
```

### 多次稳定性测试结果

运行3次测试的统计结果：

| 指标 | 值 |
|------|-----|
| 总测试次数 | 3 |
| 成功次数 | 3 |
| 失败次数 | 0 |
| 成功率 | 100.0% |
| 平均丢包率 | 0.00% |
| 平均传输时间 | 0.004 秒 |
| 平均吞吐量 | 725.31 Mbps |

## 结论

✅ **测试通过**: MOQ实现在pub-relay-sub架构下的视频传输非常稳定

1. **完整性验证**: 原始视频和接收视频的SHA256哈希值完全匹配，证明数据在传输过程中没有损坏或丢失
2. **零丢包率**: 所有测试都实现了0%的丢包率
3. **高吞吐量**: 平均吞吐量达到700+ Mbps
4. **连接稳定性**: Publisher和Subscriber都能稳定连接到Relay，并完成完整的传输周期

## 测试文件

- `test_real_video_transfer.py`: 单次视频传输测试脚本
- `run_stability_test.py`: 多次稳定性测试脚本
- `video_player.html`: 前端视频播放器界面

## 使用方法

### 运行单次测试

```bash
python3 test_real_video_transfer.py
```

### 运行多次稳定性测试

```bash
python3 run_stability_test.py 5  # 运行5次测试
```

### 查看前端播放器

在浏览器中打开 `video_player.html` 文件。

## 技术要点

1. **对象排序**: 由于MOQ协议允许并行传输，接收到的对象需要根据(group_id, object_id)进行排序后重组
2. **Stream模式**: 测试使用Stream模式（而非Datagram模式）传输视频数据，确保可靠性
3. **QUIC传输**: 利用QUIC协议的多路复用和0-RTT特性，实现高效传输
