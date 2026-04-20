# MOQ Transport Bug Report

## 概述

经过代码审查和测试，发现MOQ Transport实现中存在几个关键bug，主要影响stream模式的数据传输。

---

## Bug 1: Stream 数据不会被订阅者处理

**严重性**: 高

**描述**: 在stream模式下，订阅者收到数据但不会将其传递给应用程序。

**根本原因**:
在`sub/subscriber.py`中，`_handle_stream_data`方法只有在收到`end_stream=True`时才会处理累积的数据：

```python
if data.end_stream:
    complete_data = self._stream_buffers[data.stream_id]
    del self._stream_buffers[data.stream_id]
    await self._handle_data_stream(data.stream_id, complete_data)
```

但是Publisher在`pub/publisher.py`中发送stream数据时**不会**设置`end_stream=True`（设计如此，因为stream要保持打开状态用于连续传输）：

```python
await self._client.send_stream_data(stream_id, subgroup_obj.encode())
# 注意：没有 end_stream=True
```

这导致订阅者永远不会处理stream数据。

**影响**: Stream模式完全无法工作，100%数据丢失。

**修复建议**:
选项1: 修改订阅者，立即处理stream数据而不是等待stream结束
```python
# 在 _handle_stream_data 中
else:
    # Process data immediately for streaming mode
    await self._process_stream_data(data.stream_id, self._stream_buffers[data.stream_id])
```

选项2: 实现流式解析器，在数据到达时立即解析对象

---

## Bug 2: Relay 转发流数据时缺少 end_stream

**严重性**: 中

**描述**: Relay在转发流数据时可能没有正确传递`end_stream`标志。

**位置**: `relay/relay.py` 第 599-605 行

**影响**: 即使Publisher发送了`end_stream=True`，订阅者也可能收不到。

**修复建议**: 确保`end_stream`标志在转发链中被正确传递。

---

## Bug 3: 连接断开时资源清理不完整

**严重性**: 中

**描述**: 当客户端突然断开连接时，Relay中的订阅列表可能没有被完全清理。

**位置**: `relay/relay.py` 第 1184-1212 行的 `_cleanup_client` 方法

**问题代码**:
```python
# Remove subscriptions
for track_name in list(client.subscriptions.keys()):
    if track_name in self._subscriptions:
        self._subscriptions[track_name] = [
            s
            for s in self._subscriptions[track_name]
            if s.session_id != client.session_id
        ]
        # 缺少：如果列表为空，应该从 _subscriptions 中删除键
        if not self._subscriptions[track_name]:
            del self._subscriptions[track_name]
```

当前代码没有清理空的订阅列表。

**影响**: 随着时间推移，`_subscriptions`字典会积累大量空的订阅列表。

**修复建议**:
```python
# Remove subscriptions
for track_name in list(client.subscriptions.keys()):
    if track_name in self._subscriptions:
        self._subscriptions[track_name] = [
            s
            for s in self._subscriptions[track_name]
            if s.session_id != client.session_id
        ]
        # 清理空的订阅列表
        if not self._subscriptions[track_name]:
            del self._subscriptions[track_name]
```

---

## Bug 4: Stream Buffer 清理问题

**严重性**: 低

**描述**: Relay中的stream buffer可能没有正确清理。

**位置**: `relay/relay.py` 第 542-612 行的 `_handle_data_stream` 方法

**问题**: Stream buffer只在`end_stream=True`时清理，但如果stream异常终止或客户端断开，buffer可能残留。

**修复建议**: 在客户端断开时清理相关的stream buffers。

---

## 测试结果总结

### 当前状态
- ✅ Datagram 模式：可以工作
- ❌ Stream 模式：完全无法工作（Bug 1）
- ⚠️ 连接稳定性：空闲超时机制存在（300s），但需要验证

### 测试文件
已创建以下测试文件：
1. `tests/test_connection_stability.py` - 连接稳定性测试
2. `tests/test_edge_cases.py` - 边界条件测试
3. `tests/test_simple.py` - 基础功能测试
4. `run_tests.py` - 测试运行器

### 运行测试
```bash
cd /home/acn/zqm/moq-modified
python3 run_tests.py --list  # 列出所有测试
python3 run_tests.py -k test_basic_stream  # 运行单个测试
python3 run_tests.py -k test_basic_datagram  # 运行datagram测试
```

---

## 推荐的修复优先级

1. **高优先级**: 修复 Bug 1 (Stream数据不处理) - 影响核心功能
2. **中优先级**: 修复 Bug 3 (资源清理) - 影响长期稳定性
3. **低优先级**: 修复 Bug 2 和 Bug 4 - 边界情况处理

---

## 代码改进建议

### 1. 添加连接健康检查
当前实现没有心跳机制，建议添加定期健康检查。

### 2. 实现自动重连
Publisher 和 Subscriber 都没有自动重连机制。

### 3. 改进错误处理
- 使用自定义异常类而不是通用Exception
- 添加更详细的错误日志

### 4. 流数据立即处理
对于stream模式，应该实现流式解析器而不是等待完整stream。
