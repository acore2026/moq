# MOQ 项目结构说明

本项目已经整理完成，以下是新的目录结构：

## 📁 目录结构

```
moq-modified/
├── encoding/          # 编码模块 (原有)
├── messages/          # 消息模块 (原有)
├── session/           # 会话模块 (原有)
├── transport/         # 传输模块 (原有)
├── pub/               # 发布者模块 (原有)
├── sub/               # 订阅者模块 (原有)
├── relay/             # 中继模块 (原有)
├── utils/             # 工具模块 (原有)
├── tests/             # 测试目录 (整理后)
│   ├── test_simple.py
│   ├── test_connection_stability.py
│   ├── test_edge_cases.py
│   ├── test_file_transfer.py
│   ├── test_long_running.py
│   ├── test_long_running_quick.py
│   ├── test_stress.py
│   ├── test_video_scenarios.py
│   ├── test_real_video_transfer.py    # ← 新移动到这里
│   └── run_stability_test.py          # ← 新移动到这里
├── examples/          # 示例代码目录 (已有)
│   ├── simple_integration_test.py
│   ├── video_transfer_example.py
│   ├── http_api_server.py
│   ├── websocket_streaming.py
│   ├── websocket_player.html
│   └── README.md
├── docs/              # 文档目录 (新创建)
│   ├── BUG_REPORT.md
│   ├── CHANGES.md
│   ├── FINAL_SUMMARY.md
│   ├── FIXES_SUMMARY.md
│   ├── LONG_RUNNING_TEST_RESULTS.md
│   ├── TESTING_GUIDE.md
│   ├── TEST_SUMMARY.md
│   └── VIDEO_TRANSFER_TEST_REPORT.md
├── __init__.py        # 包入口 (原有)
├── moq_video_service.py    # 视频传输服务接口 (新增核心文件)
├── video_player.html       # 前端播放器 (新增)
├── run_tests.py           # 测试运行器 (原有)
├── run_all_tests.py       # 全部测试运行器 (原有)
└── PROJECT_STRUCTURE.md   # 本文件
```

## 🚀 使用方式

### 1. 运行原有测试

```bash
# 运行所有测试
python3 run_all_tests.py

# 运行单个测试
python3 -m pytest tests/test_simple.py

# 运行视频场景测试
python3 tests/test_video_scenarios.py
```

### 2. 运行新添加的视频传输测试

```bash
# 运行真实视频传输测试
python3 tests/test_real_video_transfer.py

# 运行稳定性测试（运行多次）
python3 tests/run_stability_test.py 5
```

### 3. 使用视频传输服务接口

```bash
# 简单集成测试
python3 examples/simple_integration_test.py

# 命令行工具
python3 examples/video_transfer_example.py relay
python3 examples/video_transfer_example.py pub --video /path/to/video.mp4 --track my-video
python3 examples/video_transfer_example.py sub --track my-video --output /path/to/output.mp4

# HTTP API服务器
python3 examples/http_api_server.py

# WebSocket流服务器
python3 examples/websocket_streaming.py
```

## 📦 核心文件说明

### `moq_video_service.py`

视频传输服务的主要接口，提供以下类：

- `MOQVideoPublisher` - 视频发布者
- `MOQVideoSubscriber` - 视频订阅者
- `MOQVideoRelay` - 中继服务
- `MOQVideoServiceAPI` - HTTP REST API服务
- `VideoTransferStats` - 传输统计信息

### `video_player.html`

前端视频播放器页面，用于WebSocket实时播放。

## 🔧 Python导入说明

所有测试和示例文件都已经配置好正确的导入路径。如果从项目根目录运行：

```python
# 从根目录导入
from moq_video_service import MOQVideoPublisher

# 从tests目录导入
from tests.test_real_video_transfer import VideoTransferTest

# 从examples目录导入（如果需要）
from examples.simple_integration_test import test_video_transfer
```

## 📋 运行示例

### 运行真实视频传输测试 (1080p)

```bash
cd /home/acn/zqm/moq-modified
python3 tests/test_real_video_transfer.py
```

### 运行稳定性测试 (运行3次)

```bash
cd /home/acn/zqm/moq-modified
python3 tests/run_stability_test.py 3
```

### 运行简单集成测试

```bash
cd /home/acn/zqm/moq-modified
python3 examples/simple_integration_test.py
```

## ✅ 验证整理结果

整理后的项目：
- ✅ 所有核心代码保留在原位置
- ✅ 测试文件移动到 `tests/` 目录
- ✅ 示例代码保留在 `examples/` 目录
- ✅ 文档移动到 `docs/` 目录
- ✅ 导入路径已修复
- ✅ 所有测试可以正常运行
