# MOQ Transport Testing Guide

## 概述

本测试套件包含多种测试场景，涵盖视频传输、文件传输、长期稳定性和压力测试。

## 测试分类

### 1. 基础测试 (`tests/test_simple.py`)

**快速验证基本功能**

```bash
python3 run_all_tests.py --quick
```

包含：
- `test_basic_stream` - Stream模式基础传输
- `test_basic_datagram` - Datagram模式基础传输

### 2. 视频场景测试 (`tests/test_video_scenarios.py`)

**模拟真实视频流传输场景**

```bash
python3 run_all_tests.py --video
```

包含：
- `test_01_video_stream_720p_30fps` - 720p视频流测试
- `test_02_video_stream_multiple_profiles` - 多profile同时传输
- `test_03_video_adaptive_bitrate` - 自适应码率切换
- `test_04_video_long_duration_stream` - 30秒长时间流传输
- `test_05_video_burst_transmission` - 突发流量测试（关键帧模拟）
- `test_06_video_stream_recovery` - 断开重连恢复测试

**视频Profile配置**：
```python
VIDEO_PROFILES = [
    VideoProfile("720p_30fps_low", (1280, 720), 30, "1M", 10, 30),
    VideoProfile("720p_30fps_medium", (1280, 720), 30, "2M", 10, 30),
    VideoProfile("1080p_30fps_medium", (1920, 1080), 30, "4M", 10, 30),
    VideoProfile("1080p_60fps_high", (1920, 1080), 60, "8M", 10, 60),
    VideoProfile("4K_30fps_high", (3840, 2160), 30, "15M", 10, 30),
]
```

### 3. 文件传输测试 (`tests/test_file_transfer.py`)

**大文件传输和分块传输测试**

```bash
python3 run_all_tests.py --file
```

包含：
- `test_01_small_file_transfer_stream` - 1MB文件stream传输
- `test_02_large_file_transfer_stream` - 10MB大文件传输
- `test_03_file_transfer_datagram` - Datagram模式文件传输
- `test_04_concurrent_file_transfers` - 多文件并发传输
- `test_05_chunked_file_with_resume` - 断点续传模拟
- `test_06_file_transfer_with_throttling` - 限速传输

**文件配置**：
```python
TRANSFER_CONFIGS = [
    FileTransferConfig(1024 * 1024, 4096, False, 0, 0),      # 1MB, stream
    FileTransferConfig(10 * 1024 * 1024, 8192, False, 0, 0),  # 10MB, stream
    FileTransferConfig(50 * 1024 * 1024, 16384, False, 0, 0), # 50MB, stream
    FileTransferConfig(1024 * 1024, 1200, True, 0, 0),       # 1MB, datagram
]
```

### 4. 长期稳定性测试 (`tests/test_long_running.py`)

**长时间运行和资源管理测试**

```bash
python3 run_all_tests.py --long
```

包含：
- `test_01_continuous_transmission_5_minutes` - 5分钟连续传输
- `test_02_connection_stability_idle_periods` - 空闲期连接稳定性
- `test_03_repeated_connect_disconnect` - 重复连接断开循环
- `test_04_multiple_tracks_stress` - 多track压力测试
- `test_05_large_object_churn` - 大对象内存管理

**内存监控**：
- RSS内存跟踪
- VMS内存跟踪
- Python对象内存跟踪
- 内存泄漏检测（>200MB增长视为异常）

### 5. 压力测试 (`tests/test_stress.py`)

**高负载和边界条件测试**

```bash
python3 run_all_tests.py --stress
```

包含：
- `test_01_high_throughput_single_stream` - 单流高吞吐（1000 obj/s）
- `test_02_burst_traffic_handling` - 突发流量处理
- `test_03_concurrent_connections` - 多连接并发
- `test_04_mixed_payload_sizes` - 混合payload大小
- `test_05_rapid_subscribe_unsubscribe` - 快速订阅取消

## 运行测试

### 运行所有测试
```bash
python3 run_all_tests.py --all
```

### 运行特定测试
```bash
# 单个测试文件
python3 -m unittest tests.test_simple.TestSimple.test_basic_stream

# 所有视频测试
python3 -m unittest tests.test_video_scenarios

# 特定测试方法
python3 -m unittest tests.test_file_transfer.TestFileTransfer.test_01_small_file_transfer_stream
```

### 调试模式运行
```bash
# 启用详细日志
python3 -m unittest tests.test_simple -v
```

## 测试报告解读

### 关键指标

1. **传输成功率**
   - Stream模式: 期望 > 85%
   - Datagram模式: 期望 > 50%（无保障传输）

2. **吞吐量**
   - 单流: 目标 500+ obj/s
   - 文件传输: 取决于网络条件

3. **内存增长**
   - 长时间测试: 期望 < 200MB增长
   - 重复连接: 期望 < 100MB增长

4. **延迟**
   - 端到端传输延迟: 应在几秒内

### 失败分析

**常见失败原因**：

1. **Checksum mismatch**
   - 数据丢失或损坏
   - 检查丢包率

2. **Timeout**
   - 传输时间过长
   - 增加测试等待时间

3. **Memory growth too high**
   - 内存泄漏
   - 检查资源清理

4. **Connection failed**
   - QUIC连接问题
   - 检查端口和证书

## 实际应用场景映射

### 视频直播场景
```python
# 对应测试: test_04_video_long_duration_stream
# 配置: 1080p_30fps_medium
VideoProfile(
    name="live_1080p",
    resolution=(1920, 1080),
    fps=30,
    bitrate="4M",
    duration=3600,  # 1小时直播
    gop_size=30
)
```

### 地图文件同步场景
```python
# 对应测试: test_02_large_file_transfer_stream
# 配置: 10MB chunked file
FileTransferConfig(
    file_size=50 * 1024 * 1024,  # 50MB地图数据
    chunk_size=16384,  # 16KB chunks
    use_datagram=False,
    delay_ms=0,
    packet_loss_rate=0
)
```

### 多设备并发场景
```python
# 对应测试: test_03_concurrent_connections
# 配置: 5 pub/sub pairs
num_pairs = 10  # 10个设备同时传输
duration = 60   # 持续1分钟
```

## Bug修复总结

### 已修复的核心Bug

1. **Stream模式数据不处理**
   - 问题：订阅者等待end_stream=True才处理
   - 修复：实现增量解析器
   - 文件：`sub/subscriber.py`

2. **大文件传输不完整**
   - 问题：Buffer未正确清理，导致重复解析
   - 修复：返回并清理已处理的字节
   - 文件：`sub/subscriber.py`

3. **资源清理不完整**
   - 问题：断开时stream buffer未清理
   - 修复：在_cleanup_client中添加清理
   - 文件：`relay/relay.py`

### 性能优化

1. **增量解析**：避免重复解析已处理数据
2. **部分对象跟踪**：支持大payload分片
3. **内存管理**：及时清理buffer

## 持续集成建议

### CI/CD配置
```yaml
# .github/workflows/test.yml
steps:
  - name: Run Quick Tests
    run: python3 run_all_tests.py --quick
  
  - name: Run Video Tests
    run: python3 run_all_tests.py --video
    timeout-minutes: 10
  
  - name: Run File Tests
    run: python3 run_all_tests.py --file
    timeout-minutes: 15
```

### 定期测试
- **每日**：Quick tests + Video tests
- **每周**：Full test suite
- **每月**：Long running tests (需要更长时间)

## 贡献指南

添加新测试：
1. 在`tests/`目录创建测试文件
2. 继承`unittest.IsolatedAsyncioTestCase`
3. 使用`asyncSetUp`和`asyncTearDown`
4. 遵循命名规范：`test_<category>_<description>`
5. 更新`run_all_tests.py`添加测试类别

## 参考文档

- `BUG_REPORT.md` - 已知Bug详细报告
- `FIXES_SUMMARY.md` - 修复摘要
- `CHANGES.md` - 变更日志
