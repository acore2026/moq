# MOQ 示例用例说明

本文档说明 `examples/` 目录下各个示例的用途、覆盖的协议能力，以及推荐的执行方式。

## 1. 用例总览

| 文件 | 目标能力 | 场景定位 |
| --- | --- | --- |
| `relay_example.py` | Relay 启动、磁盘缓存初始化 | 中继节点快速启动 |
| `publisher_example.py` | PUBLISH、对象发送、datagram/stream | 发布端参考实现 |
| `subscriber_example.py` | SUBSCRIBE、对象接收 | 订阅端参考实现 |
| `fetch_example.py` | FETCH、历史对象拉取 | 历史内容查询 |
| `reconnection_example.py` | 重连、缓存恢复 | 断连恢复与缓存验证 |
| `integration_example.py` | 外部项目集成 | 作为业务应用模板 |
| `basic_example.py` | Relay + Publisher + Subscriber 一体化演示 | 一键快速体验 |

## 2. 建议的验证顺序

1. 启动 `relay_example.py`。
2. 启动 `publisher_example.py`，确认发布成功。
3. 启动 `subscriber_example.py`，确认可以收到实时对象。
4. 在发布过对象之后运行 `fetch_example.py`，验证历史拉取。
5. 运行 `reconnection_example.py`，验证断开后重新连接仍可继续使用缓存。
6. 阅读 `integration_example.py`，将示例迁移到自己的项目。

## 3. 各用例说明

### 3.1 `relay_example.py`

- 作用：启动 MOQ Relay，并在启动时清理磁盘缓存。
- 适用场景：本地开发、端到端联调、缓存行为验证。
- 关注点：Relay 启动后应进入监听状态，缓存目录应自动重置为干净状态。

### 3.2 `publisher_example.py`

- 作用：向 Relay 发布一个示例 track，并周期性发送对象。
- 适用场景：验证 PUBLISH、对象编码、datagram/stream 两种发送方式。
- 关注点：可以观察对象 ID 递增、发布成功回调以及取消发布流程。

### 3.3 `subscriber_example.py`

- 作用：订阅一个示例 track，并打印收到的对象内容。
- 适用场景：验证 SUBSCRIBE、对象接收、回调处理。
- 关注点：可以确认订阅成功回调和对象接收回调都能正常触发。

### 3.4 `fetch_example.py`

- 作用：按范围拉取历史对象。
- 适用场景：验证 FETCH、历史回放、缓存对象读取。
- 关注点：适合在发布端先发送若干对象后再运行，以观察历史数据返回。

### 3.5 `reconnection_example.py`

- 作用：模拟断线、重连和缓存延续。
- 适用场景：验证 Relay 的缓存能力和客户端重连后的数据连续性。
- 关注点：可观察到对象在短暂断开后仍能通过缓存恢复。

### 3.6 `integration_example.py`

- 作用：展示如何在自己的应用中封装 MOQPublisher 和 MOQSubscriber。
- 适用场景：项目集成、代码迁移、API 学习。
- 关注点：适合作为业务代码的骨架，而不是单纯的命令行演示。

### 3.7 `basic_example.py`

- 作用：把 Relay、Publisher、Subscriber 放在一个文件里快速演示。
- 适用场景：首次体验、快速确认环境是否可用。
- 关注点：更偏“演示”，不建议作为正式项目模板。

## 4. 典型检查点

- Relay 是否能正常启动并监听端口。
- Publisher 是否能完成连接和发布。
- Subscriber 是否能收到实时对象。
- FETCH 是否能返回历史对象。
- 重连后是否还能继续获取缓存中的对象。
- 示例脚本是否可以直接从仓库根目录运行。

## 5. 运行说明

所有示例现在都会自动把仓库根目录加入 `sys.path`，因此推荐直接在仓库根目录执行：

```bash
python examples/relay_example.py
python examples/publisher_example.py
python examples/subscriber_example.py
```

## 6. 维护建议

- 新增示例时，优先补充到上面的总览表中。
- 如果某个示例和已有示例高度重复，优先改成入口更明确的薄封装，避免维护两套逻辑。
- 如果协议能力发生变化，先更新 `integration_example.py` 和本文档，再同步到其他示例。
