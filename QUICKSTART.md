# Quick Start Guide

## 1. 环境准备

### 1.1 安装 Python

确保 Python 3.8+ 已安装:

```bash
python --version
```

### 1.2 创建虚拟环境

```bash
cd moq-py
python -m venv venv

# Windows
venv\Scripts\activate

# Linux/macOS
source venv/bin/activate
```

### 1.3 安装依赖

```bash
pip install -r requirements.txt
pip install -e .
```

## 2. PyCharm 开发配置

### 2.1 打开项目

1. 打开 PyCharm
2. File -> Open -> 选择 moq-py 文件夹
3. 等待项目索引完成

### 2.2 配置解释器

1. File -> Settings -> Project: moq-py -> Python Interpreter
2. 点击齿轮图标 -> Add
3. 选择 Existing environment
4. 选择 venv 中的 Python 解释器

### 2.3 配置运行配置

#### 配置 Relay

1. Run -> Edit Configurations
2. 点击 + -> Python
3. Name: `Relay Server`
4. Script path: `examples/relay/basic_relay.py`
5. Working directory: 项目根目录
6. OK

#### 配置 Publisher

1. 同上，添加配置
2. Name: `Publisher`
3. Script path: `examples/pub/basic_publisher.py`

#### 配置 Subscriber

1. 同上，添加配置
2. Name: `Subscriber`
3. Script path: `examples/sub/basic_subscriber.py`

## 3. 运行测试

### 3.1 启动 Relay

在 PyCharm 中:
1. 选择 `Relay Server` 配置
2. 点击运行按钮 (绿色三角形)

或命令行:
```bash
python examples/relay/basic_relay.py
```

### 3.2 启动 Publisher

在 PyCharm 中:
1. 选择 `Publisher` 配置
2. 点击运行按钮

或命令行:
```bash
python examples/pub/basic_publisher.py
```

### 3.3 启动 Subscriber

在 PyCharm 中:
1. 选择 `Subscriber` 配置
2. 点击运行按钮

或命令行:
```bash
python examples/sub/basic_subscriber.py
```

## 4. 断线重连测试

运行断线重连示例:

```bash
python examples/sub/reconnection_resume.py
```

或:

```bash
python examples/relay_cache_reconnect.py
```

## 5. 常见问题

### Q: 端口被占用?

A: 修改 relay 配置使用其他端口:
```python
relay_config = RelayConfig(port=8443)
```

### Q: 缓存目录权限?

A: 确保有写入权限或修改缓存目录:
```python
session_config = SessionConfig(cache_dir="/path/to/cache")
```

### Q: 如何调试?

A: 设置日志级别为 DEBUG:
```python
logging.basicConfig(level=logging.DEBUG)
```

## 6. 下一步

- 阅读 [ARCHITECTURE.md](ARCHITECTURE.md) 了解架构设计
- 阅读 [API.md](API.md) 了解 API 详情
- 查看 `examples/` 目录获取更多示例
