# ACN SDK MoQ Rust CLI 兼容替换方案

`acn_sdk` 当前通过 `acn_sdk.network.moq_client.MoQClient` 使用纯 Python MoQ。
对外同步 API 为：

- `connect()`
- `publish(namespace, track)`
- `send_object(namespace, track, payload)`
- `subscribe(namespace, track, subscriber_id)`
- `fetch(namespace, track, ...)`
- `unsubscribe(namespace, track, subscriber_id=None)`
- `disconnect()` / `close()`
- `is_published()` / `is_subscribed()`

要做到业务代码无缝替换，Python 侧保留这些方法，底层改为调用 Rust `moq-cli`。

## 命名映射

Python 旧模型：

```text
FullTrackName(namespace=["task", "agent"], track_name="Location")
```

Rust 官方模型：

```text
broadcast name = "task/agent"
track name     = "Location"
```

也就是：

```text
namespace.strip("/") -> moq-cli --name
track                -> Rust broadcast 内的 track
```

## Rust moq-cli 对象模式

当前维护版 `moq-cli` 已增加对象模式。除了原媒体模式：

```text
publish avc3/fmp4/hls
subscribe --output avc3/fmp4
```

现在还支持 ACN SDK 任意 bytes payload：

```bash
moq-cli publish --url http://relay:9007/ --name task/agent object

moq-cli subscribe \
  --url http://relay:9007/ \
  --name task/agent \
  --output object \
  --track Location
```

`fetch`/backfill 兼容语义使用独立的 object fetch 命令，并支持范围参数：

```bash
moq-cli fetch \
  --url http://relay:9007/ \
  --name task/agent \
  --output object \
  --track Location \
  --start-group 0 \
  --start-object 0
```

可选结束边界：

```bash
--end-group 100 --end-object 100
```

不传 `--end-group/--end-object` 时，对齐 Python SDK 中
`end_group=None, end_object=None` 的语义：从起点拉到 relay 当前可见的最新对象，
并在 `--idle-timeout-ms` 时间内没有新对象后退出。持续实时接收仍由
`subscribe()` 负责。

Python 兼容层已放在：

```text
moq_rust/moq_rust_client/
```

主类：

```python
from moq_rust_client import RustCliMoQClient
```

其方法形状对齐 `acn_sdk.network.moq_client.MoQClient`。

## Python 到 moq-cli 的本地帧协议

Python 通过 `stdin` 向 `moq-cli publish ... object` 写入对象帧。
`moq-cli subscribe --output object` 通过 `stdout` 输出对象帧。

帧头：

```text
magic       4 bytes  "MOBJ"
version     u8       1
op          u8       1=publish, 2=unpublish, 3=object
track_len   u16
group_id    u64
object_id   u64
payload_len u32
track       track_len bytes, UTF-8
payload     payload_len bytes
```

这个协议只在 Python wrapper 和本机 `moq-cli` 进程之间使用，不进入网络。
网络热路径仍然是 Rust MoQ。

进入 MoQ 网络的 object payload 还会由 Rust CLI 包一层很小的内部 envelope，
用于保留 Python MoQ 原来的二维位置语义：

```text
magic       4 bytes  "MOBW"
version     u8       1
group_id    u64
object_id   u64
payload_len u32
payload     payload_len bytes
```

`moq-cli publish object` 写入网络前封装，`moq-cli subscribe --output object`
输出到 Python 前解封装。因此业务层收到的仍然是原始 bytes payload。
如果 subscriber 遇到旧版本未封装 payload，会回退为
`group_id = object_id = MoQ group sequence`。

## acn_sdk 替换点

最小改动是在 `SDKNetworkMixin._create_moq_client()` 中按环境变量切换。
克隆的 `acn_sdk` 当前已经按下面逻辑调整：

```python
import os

def _create_moq_client(self, role: str):
    if os.environ.get("ACN_MOQ_IMPL", "python").lower() in {"rust", "rust-cli", "moq-rust"}:
        from moq_rust_client import RustCliMoQClient

        return RustCliMoQClient(
            host=self.config.network.network_ip,
            remote_port=self.config.network.agent_gw_moq_port,
            role=role,
            on_object_received=self._handle_moq_object_received if role == "subscriber" else None,
        )

    return MoQClient(...)
```

业务侧继续调用原来的 SDK API，不需要改 `task_info_report()`、
`handle_network_message()` 或 callback。

启用方式：

```bash
export ACN_MOQ_IMPL=rust-cli
export MOQ_RUST_VIDEO_MOQ_CLI=/path/to/patched/moq-cli
```

## 当前状态

- Python 兼容层和本地对象帧协议已实现。
- Rust `moq-cli` 已支持 `publish object`。
- Rust `moq-cli subscribe --output object --track <track>` 已支持对象输出。
- Rust `moq-cli subscribe --output object` 已支持 `--start-group`、`--start-object`、
  `--end-group`、`--end-object`，用于补齐 acn_sdk 的 fetch/backfill 语义。
- `--start-group/--end-group` 已下沉到 `moq-lite` 协议的
  `SUBSCRIBE.start_group/end_group` 字段；relay/publisher 会从 track cache
  对应 group 开始发送，不再只是 Python/CLI 本地过滤。
- Rust object 模式已在网络 payload 中保留原 Python 对象的 `group_id/object_id`，
  不再把 MoQ group sequence 简化成两者。
- IETF `FetchType::Standalone` publisher 侧已补基础支持：收到 FETCH 后会查
  broadcast/track cache，返回 `FetchOk`，并发送 `FetchHeader` 后接
  ACN-compatible fetch object 编码。当前主要用于 Rust relay/publisher 侧协议
  兼容；完整第三方 IETF subscriber API 仍需继续补齐。
- `moq-cli fetch --output object --track <track>` 已补齐；Python
  `RustCliMoQClient.fetch()` 会启动独立的一次性 fetch 子进程，从
  relay/moq-lite track cache 拉取符合范围的已缓存对象。
- `RustCliMoQClient` 会在本地缓存已收到的对象，主要用于观测和订阅侧诊断；
  显式 `fetch()` 不再直接重放本地缓存，避免把本机历史数据误认为 relay
  已支持缓存回放。
- Python wrapper 会检测已退出的 `moq-cli` publisher/subscriber 子进程；后续
  publish/subscribe/fetch/send 会清理旧进程并重新拉起，避免复用坏状态。
- Python wrapper 会在 `disconnect()` 时停止 publisher、subscriber、fetch
  子进程，避免测试或生产重启时遗留 Rust CLI 进程。
- moq-lite track cache 默认生产包装层设为 300 秒；生产环境可通过
  `MOQ_LITE_MAX_GROUP_AGE_SECS=<seconds>` 调整缓存保留时间。这个配置通过
  环境变量传递给补丁版 `moq-relay/moq-cli`，不会写入 relay TOML，因为官方
  relay 配置启用了 unknown field 校验。
- Python wrapper 本地接收缓存默认保留 1000 条，可通过
  `ACN_MOQ_RUST_RECEIVED_CACHE_SIZE=<count>` 调整。
- `moq-rust-video` wheel 会同时打包 `moq_rust_client` 与 `moq_rust_video`。

## 生产风险排序与当前修复

1. Relay 启动失败风险：不再向官方 relay TOML 写入未知 `[cache]` 段；缓存窗口通过
   `MOQ_LITE_MAX_GROUP_AGE_SECS` 环境变量配置。
2. Fetch 语义误判风险：`fetch()` 不再依赖长期 `subscribe()` 的本地接收缓存，
   而是启动独立 `moq-cli fetch --output object`，验证结果来自 relay/Rust cache。
3. 晚订阅缓存窗口过短风险：wrapper 默认把 `MOQ_LITE_MAX_GROUP_AGE_SECS` 设为
   300 秒；可按业务内存预算调大或调小。
4. 子进程泄漏风险：`disconnect()` 会清理 publisher、subscriber、fetch 三类
   moq-cli 子进程。
5. CLI 合约漂移风险：`connect()` 的 verify 阶段会检查 `publish object`、
   `subscribe --output object`、`fetch --output object` 和 range/idle 参数。

## fetch 语义说明

原 Python SDK 在收到 `SUBSCRIBE_TRACK` 后，如果 callback 返回 `"fetch"`，实际执行顺序是：

```text
moq_sub_client.fetch(namespace, track, start_group=0, start_object=0, end_group=None, end_object=None)
moq_sub_client.subscribe(namespace, track, local_agent_id)
```

也就是“先拉缓存，再订阅实时”。Rust 替换层通过独立 fetch 加长期 subscribe
实现这个行为：

```text
fetch()     -> 启动一次性 moq-cli fetch --output object --track ... --start-group ... --start-object ...
subscribe() -> 启动长期 moq-cli subscribe --output object --track ... 接收后续实时对象
```

其中 `--start-group`/`--end-group` 会进入 moq-lite wire protocol，而不是只在本机
CLI 做过滤；`--start-object`/`--end-object` 仍在 object payload 解封装后按
ACN 的 `group_id/object_id` 过滤。`moq-cli fetch` 是一次性命令：没有结束边界时，
它会在 `--idle-timeout-ms` 内没有收到更多 group 后退出。

如果业务先 `subscribe()` 后 `fetch()`，`fetch()` 仍会独立向 relay 拉缓存，不直接
使用本地接收缓存：

```text
subscribe() -> 启动长期 object subscriber，并把收到的对象放入 Python 本地接收缓存
fetch()     -> 启动一次性 object fetch，从 relay cache 拉取范围对象
```

注意：Rust 官方 moq-lite 的缓存是短时 live cache，不是 Python relay 中独立的
1000 条对象列表缓存。要扩大晚订阅可回放窗口，需要在 relay/publisher/subscriber
运行环境中设置 `MOQ_LITE_MAX_GROUP_AGE_SECS`，并使用重新编译后的 moq-cli/moq-relay。
