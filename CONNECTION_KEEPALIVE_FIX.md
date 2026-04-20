# QUIC PING 帧保活机制修复

## 问题描述

MOQ 连接在大约 66 秒后因 "Idle timeout" 断开，而配置的空闲超时时间是 300 秒。

## 根本原因

1. QUIC 协议的 `idle_timeout` 是两端协商的，实际使用**较小的一方**
2. 虽然代码中配置了 300 秒，但如果某一方使用默认值 60 秒，连接会在 60 秒左右断开
3. 原来的心跳机制发送空数据 `b""` 到流上，这不被 QUIC 实现视为"连接活动"

## 解决方案

使用 QUIC PING 帧进行保活，PING 帧是 QUIC 协议专门用于保活的帧类型，会被明确视为连接活动。

## 修改文件

### 1. transport/quic_transport.py

添加了 `send_ping()` 方法：

```python
async def send_ping(self):
    """Send QUIC PING frame to keep connection alive."""
    if not self.protocol:
        raise RuntimeError("Not connected")

    # Send QUIC PING frame - this is explicitly treated as connection activity
    self._connection.send_ping()
    self.protocol.transmit()
    logger.debug("Sent QUIC PING frame for keepalive")
```

### 2. pub/publisher.py

修改心跳机制：

```python
# 修改前：
await self._client.send_stream_data(0, b"")
logger.debug("Sent heartbeat (empty data on control stream)")

# 修改后：
await self._client.send_ping()
logger.debug("Sent heartbeat (QUIC PING frame)")
```

### 3. sub/subscriber.py

修改心跳机制：

```python
# 修改前：
await self._client.send_stream_data(0, b"")
logger.debug("Sent heartbeat (empty data on control stream)")

# 修改后：
await self._client.send_ping()
logger.debug("Sent heartbeat (QUIC PING frame)")
```

## 工作原理

1. **QUIC PING 帧**：是 QUIC 协议专门设计的保活机制
2. **连接活动识别**：PING 帧会被 QUIC 协议栈明确识别为连接活动，重置 idle timeout 计时器
3. **轻量级**：PING 帧只包含帧头，没有额外数据，开销极小
4. **兼容性**：所有 QUIC 实现都支持 PING 帧

## 配置参数

当前心跳间隔：**25 秒**（可根据需要调整）

```python
# pub/publisher.py
DEFAULT_HEARTBEAT_INTERVAL = 25.0

# sub/subscriber.py
DEFAULT_HEARTBEAT_INTERVAL = 25.0
```

## 建议进一步优化

1. **降低心跳间隔**：当前是 25 秒，可以改为 20 秒或更短，确保在 60 秒超时前发送多个心跳
2. **检查 Relay 配置**：确保 Relay 端的 `idle_timeout` 也设置为 300 秒
3. **添加连接断开后自动重连机制**：在应用层处理连接断开的情况
4. **添加连接状态监控**：记录连接持续时间、心跳发送次数等指标

## 验证

运行测试验证修改：

```bash
# 运行简单测试
python3 tests/test_simple.py

# 运行长时间连接测试（需要观察是否超过 60 秒不断开）
python3 tests/test_connection_stability.py
```

## 日志输出

修改后的心跳日志输出：

```
# 修改前
Sent heartbeat (empty data on control stream)

# 修改后
Sent heartbeat (QUIC PING frame)
```

## 向后兼容性

此修改完全向后兼容：
- 只修改了内部实现，不影响 API 接口
- 不需要修改任何调用代码
- 不影响现有功能
