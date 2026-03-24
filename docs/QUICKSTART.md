# MOQ Transport - 快速开始指南

## 目录

1. [环境准备](#环境准备)
2. [安装步骤](#安装步骤)
3. [运行示例](#运行示例)
4. [PyCharm 配置](#pycharm-配置)
5. [故障排除](#故障排除)

---

## 环境准备

### 系统要求

**最低要求**:
- Python 3.8+
- 4GB RAM
- 1GB 磁盘空间

**推荐配置**:
- Python 3.10+
- 8GB RAM
- 10GB 磁盘空间（用于缓存）

### 操作系统支持

| 操作系统 | 版本要求 | 支持状态 |
|---------|---------|---------|
| Ubuntu | 18.04+ | ✅ 完全支持 |
| CentOS | 7+ | ✅ 完全支持 |
| Windows | 10/11 | ✅ 完全支持 |
| macOS | 10.14+ | ✅ 完全支持 |

---

## 安装步骤

### 1. 获取代码

```bash
# 进入项目目录
cd /home/acn/cxr/moq-py

# 查看项目结构
ls -la
```

### 2. 创建虚拟环境（推荐）

**Linux/macOS:**
```bash
# 创建虚拟环境
python3 -m venv venv

# 激活虚拟环境
source venv/bin/activate

# 验证
which python
# 应该显示: /home/acn/cxr/moq-py/venv/bin/python
```

**Windows:**
```cmd
# 创建虚拟环境
python -m venv venv

# 激活虚拟环境
venv\Scripts\activate

# 验证
where python
```

### 3. 安装依赖

```bash
# 安装项目依赖
pip install -r requirements.txt

# 验证安装
pip list
```

预期输出：
```
aioquic          0.9.x
cryptography     3.x.x
pylsqpack        0.3.x
pyopenssl        20.x.x
service-identity 18.x.x
```

### 4. 验证安装

```bash
# 运行单元测试
python3 tests/test_encoding.py
python3 tests/test_messages.py

# 应该看到:
# ....................
# ----------------------------------------------------------------------
# Ran 20 tests in 0.001s
# OK
```

---

## 运行示例

### 基础发布订阅示例

#### 方式一：多终端运行（模拟真实场景）

**终端 1 - 启动 Relay:**
```bash
cd /home/acn/cxr/moq-py
source venv/bin/activate  # Linux/macOS
# 或 venv\Scripts\activate  # Windows

python examples/relay_example.py
```

输出示例：
```
2024-XX-XX XX:XX:XX - moq.relay - INFO - MOQRelay initialized: 127.0.0.1:4443
2024-XX-XX XX:XX:XX - moq.relay - INFO - Relay running on 127.0.0.1:4443
```

**终端 2 - 启动 Publisher:**
```bash
cd /home/acn/cxr/moq-py
source venv/bin/activate

python examples/publisher_example.py
```

输出示例：
```
2024-XX-XX XX:XX:XX - __main__ - INFO - Starting publisher...
2024-XX-XX XX:XX:XX - moq.pub.publisher - INFO - MOQPublisher initialized for 127.0.0.1:4443
2024-XX-XX XX:XX:XX - __main__ - INFO - Publishing track: FullTrackName(...)
2024-XX-XX XX:XX:XX - __main__ - INFO - Sent: group=1, object=1
...
```

**终端 3 - 启动 Subscriber:**
```bash
cd /home/acn/cxr/moq-py
source venv/bin/activate

python examples/subscriber_example.py
```

输出示例：
```
2024-XX-XX XX:XX:XX - __main__ - INFO - Starting subscriber...
2024-XX-XX XX:XX:XX - __main__ - INFO - Subscribed to track: FullTrackName(...)
2024-XX-XX XX:XX:XX - __main__ - INFO - [Received] group=1, object=1, size=XX
...
2024-XX-XX XX:XX:XX - __main__ - INFO - Summary: Received 15 objects
```

#### 方式二：单进程演示

```bash
cd /home/acn/cxr/moq-py
source venv/bin/activate

python examples/basic_example.py demo
```

这将在一个进程中同时运行 Relay、Publisher 和 Subscriber，方便快速测试。

如果你只想验证发布/订阅最小闭环，推荐优先使用上面的 `relay_example.py`、`publisher_example.py` 和 `subscriber_example.py` 组合。

### 断线重连示例

这个示例演示连接中断后如何从缓存恢复数据。

**终端 1 - 启动 Relay:**
```bash
python examples/reconnection_example.py relay
```

**终端 2 - 启动 Publisher:**
```bash
python examples/reconnection_example.py publisher
```

**终端 3 - 启动 Subscriber:**
```bash
python examples/reconnection_example.py subscriber
```

### 示例索引

更完整的示例说明请查看：

- [`examples/README.md`](/home/acn/cxr/moq-py/examples/README.md)
- [`examples/TEST_CASES_CN.md`](/home/acn/cxr/moq-py/examples/TEST_CASES_CN.md)

Subscriber 会自动模拟断线重连，你会看到类似输出：
```
=== First Connection ===
[Conn1] Received object 1
[Conn1] Received object 2
[Conn1] Received object 3

=== Disconnecting ===
Disconnected. Waiting 3 seconds before reconnect...

=== Reconnecting ===
GAP DETECTED: Missing 2 objects between 3 and 6
[Conn2] Received object 1
...
```

---

## PyCharm 配置

### 1. 打开项目

```
File → Open → 选择 /home/acn/cxr/moq-py 文件夹
```

### 2. 配置 Python 解释器

```
File → Settings → Project: moq-py → Python Interpreter

点击齿轮图标 → Add...
→ Virtualenv Environment
→ Existing environment
→ 选择 venv/bin/python (Linux/macOS) 或 venv\Scripts\python.exe (Windows)
→ OK
```

### 3. 安装依赖

PyCharm 会自动检测到 `requirements.txt` 并提示安装依赖。

如果没有提示：
```
点击底部工具栏的 Terminal
运行: pip install -r requirements.txt
```

### 4. 配置运行配置

#### 配置 Relay 运行

```
Run → Edit Configurations...

点击 + → Python
Name: Run Relay
Script path: /home/acn/cxr/moq-py/examples/relay_example.py
Python interpreter: 项目虚拟环境
Working directory: /home/acn/cxr/moq-py
```

#### 配置 Publisher 运行

```
Name: Run Publisher
Script path: /home/acn/cxr/moq-py/examples/publisher_example.py
```

#### 配置 Subscriber 运行

```
Name: Run Subscriber
Script path: /home/acn/cxr/moq-py/examples/subscriber_example.py
```

#### 配置 Demo 运行

```
Name: Run Demo
Script path: /home/acn/cxr/moq-py/examples/basic_example.py
Parameters: demo
```

#### 配置 Fetch 运行

```
Name: Run Fetch
Script path: /home/acn/cxr/moq-py/examples/fetch_example.py
```

### 5. 运行调试

```
1. 选择运行配置（如 Run Demo）
2. 点击工具栏的绿色运行按钮 或 Shift+F10
3. 或点击调试按钮 Shift+F9 进行调试
```

### 6. 设置断点

在代码左侧点击设置断点，可以在以下位置设置：
- `moq/pub/publisher.py:send_object` - 观察对象发送
- `moq/sub/subscriber.py:on_object_received` - 观察对象接收
   - `moq/relay/relay.py:cache_object` - 观察缓存行为

---

## 故障排除

### 常见问题

#### 1. 导入错误 `ModuleNotFoundError: No module named 'moq'`

**原因**: 项目根目录不在 Python 路径中

**解决**:
```bash
# 确保在项目根目录运行
cd /home/acn/cxr/moq-py

# 检查是否在虚拟环境中
which python

# 添加项目到 PYTHONPATH
export PYTHONPATH=/home/acn/cxr/moq-py:$PYTHONPATH
```

#### 2. `aioquic` 安装失败

**原因**: 缺少编译工具或系统依赖

**解决**:

**Ubuntu/Debian:**
```bash
sudo apt-get update
sudo apt-get install -y build-essential python3-dev libffi-dev
pip install aioquic
```

**CentOS/RHEL:**
```bash
sudo yum install -y gcc python3-devel libffi-devel
pip install aioquic
```

**Windows:**
```cmd
# 安装 Visual C++ Build Tools
# 下载地址: https://visualstudio.microsoft.com/visual-cpp-build-tools/
# 选择 "C++ build tools"
pip install aioquic
```

**macOS:**
```bash
# 安装 Xcode Command Line Tools
xcode-select --install
pip install aioquic
```

#### 3. 端口被占用

**错误**: `OSError: [Errno 98] Address already in use`

**解决**:
```bash
# 查找占用端口的进程
# Linux:
sudo netstat -tulpn | grep 4443
# 或
sudo lsof -i :4443

# 杀死进程
sudo kill -9 <PID>

# 或使用不同端口
python examples/relay_example.py  # 如需修改端口，可编辑脚本中的配置
```

#### 4. 权限不足

**错误**: `PermissionError: [Errno 13] Permission denied`

**解决**:
```bash
# 确保当前用户对项目目录有读写权限
sudo chown -R $(whoami):$(whoami) /home/acn/cxr/moq-py

# 如果使用 1024 以下端口，需要 root 权限
# 建议使用 1024 以上端口（如 4443）
```

#### 5. 缓存目录无法创建

**错误**: `FileNotFoundError: [Errno 2] No such file or directory`

**解决**:
```bash
# 创建缓存目录
mkdir -p /tmp/moq_cache
chmod 755 /tmp/moq_cache

# 或在代码中修改缓存路径
relay = MOQRelay(
    cache_dir="/path/to/writable/cache/dir"
)
```

### 调试技巧

#### 1. 启用详细日志

```python
import logging

# 在代码开头添加
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
```

#### 2. 检查依赖版本

```bash
# 检查 aioquic 版本
python -c "import aioquic; print(aioquic.__version__)"

# 检查所有依赖
pip freeze
```

#### 3. 验证编码正确性

```python
from moq.encoding import VarInt, FullTrackName

# 测试 VarInt
test_values = [0, 127, 128, 16383, 16384]
for v in test_values:
    encoded = VarInt.encode(v)
    decoded, _ = VarInt.decode(encoded)
    assert decoded == v, f"Failed for {v}"
print("VarInt encoding OK")

# 测试 FullTrackName
name = FullTrackName([b"test"], b"track")
encoded = name.encode()
decoded, _ = FullTrackName.decode(encoded)
assert decoded == name
print("FullTrackName encoding OK")
```

#### 4. 网络连通性测试

```bash
# 测试 Relay 是否监听
nc -zv 127.0.0.1 4443
# 或
telnet 127.0.0.1 4443

# 检查防火墙
sudo iptables -L | grep 4443
```

---

## 下一步

1. **阅读架构文档**: `docs/ARCHITECTURE.md`
2. **查看 API 文档**: `docs/API.md`
3. **阅读完整 README**: `README.md`
4. **开发自定义应用**: 优先参考 `examples/integration_example.py`

---

## 获取帮助

如果遇到问题：

1. 查看日志输出
2. 检查本指南的故障排除部分
3. 查看代码中的注释和文档字符串
4. 运行测试验证安装: `python tests/test_encoding.py`

---

**版本**: 0.1.0  
**最后更新**: 2024
