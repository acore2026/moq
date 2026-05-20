# MoQ Rust 集成目录

这个目录集中放置当前项目中和 Rust MoQ 替换方案相关的 Python 包、relay
wrapper、测试工具和文档。目标是让端侧仍然使用原 ACN SDK 的 Python API，
但 MoQ 传输路径切换为补丁版 Rust `moq-cli` / `moq-relay`。

当前定位：

```text
ACN SDK MoQClient API 兼容
+ Rust moq-cli 子进程
+ Rust moq-relay
+ ACN object bytes 传输
```

它不是完整 MoQ draft-17 规范级实现。当前兼容 ACN SDK 已使用到的
object pub/sub/fetch 调用；完整 subgroup、完整 FETCH joining、标准
draft-17 payload 互通仍属于后续工作。

## 目录归属

`py/acn-moq-rust/` 内部是本次 Rust MoQ 的 ACN/Python 适配方案：

- `moq_rust_client/`: ACN SDK 兼容客户端。主类是
  `RustCliMoQClient`，对齐原 `acn_sdk.network.moq_client.MoQClient`
  的同步 API：`connect`、`publish`、`send_object`、`subscribe`、
  `fetch`、`unsubscribe`、`disconnect`。
- `moq_rust_video/`: 面向端侧 Python 程序安装的 wheel 包源码。它包含
  `moq_rust_client`，也可打包对应平台的 `moq-cli` 二进制；视频热路径保持在
  `ffmpeg -> moq-cli -> Rust relay`。
- `moq_official_relay/`: 官方 Rust `moq-relay` 的 Python 生命周期 wrapper，
  用于从 Agent GW 或测试脚本启动、停止 Rust relay。
- `relay_factory.py`: Agent GW 的 relay 实现选择入口，支持 `python`、`rust`、
  `bridge` 三种模式。
- `video/`: Rust relay 视频测试、浏览器预览、Windows 摄像头推流和视频性能
  测试脚本。
- `tools/`: wheel 内置二进制复制等非运行时打包工具。
- `tests/`: Rust relay wrapper、ACN SDK 兼容客户端、视频 API 的测试。
- `docs/`: 生产构建、端侧安装、ACN SDK 替换和兼容边界说明。
- `pyproject.toml` / `setup.py`: `moq-rust-video` wheel 打包配置。

其他目录不属于本 Rust MoQ 包：

- `reference/`: 外部项目的轻量集成说明和 patch。当前包含 ACN SDK 的
  `ACN_MOQ_IMPL=rust-cli` 切换 patch，不 vendor 完整 ACN SDK 仓库。
- Rust MoQ 仓库根目录：补丁版 `moq-cli` / `moq-relay` / `moq-lite` 源码。

## 兼容情况

已支持 ACN SDK 当前 MoQ object 调用链：

- `connect()`
- `publish(namespace, track)`
- `send_object(namespace, track, payload: bytes)`
- `subscribe(namespace, track, subscriber_id)`
- `fetch(namespace, track, start_group, start_object, end_group, end_object)`
- `unsubscribe(namespace, track, subscriber_id=None)`
- `disconnect()` / `close()`
- `is_published()` / `is_subscribed()`
- `on_object_received(namespace, track, payload)` 回调

已验证在 `127.0.0.1:9007` 临时 Rust relay 上可以完成：

```text
publish -> subscribe -> send_object -> receive live object -> fetch cached object
```

验证结束后 relay 已停止，未遗留 9007 监听。

## 构建补丁版 Rust 二进制

先在补丁版官方 Rust MoQ checkout 中构建：

```bash
cd /path/to/moq-rust/moq
cargo build --release --package moq-cli
cargo build --release --package moq-relay
```

需要的能力：

- `moq-cli publish ... object`
- `moq-cli subscribe --output object --track <track>`
- `moq-cli fetch --output object --track <track>`
- `moq-relay` 支持 moq-lite range subscribe cache

## 构建端侧 wheel

如果要把 `moq-cli` 一起打进 wheel，先复制对应平台二进制：

```bash
cd moq_rust
python tools/package_moq_rust_video_binary.py \
  /path/to/moq-rust/moq/target/release/moq-cli \
  --platform manylinux_x86_64
```

然后构建 wheel：

```bash
python -m pip wheel . -w dist --no-deps --no-build-isolation
```

输出示例：

```text
dist/moq_rust_video-0.1.0-<python>-<abi>-<platform>.whl
```

Windows 端需要打包 `moq-cli.exe`：

```powershell
python tools\package_moq_rust_video_binary.py `
  D:\path\to\moq-rust\moq\target\release\moq-cli.exe `
  --platform win_amd64
python -m pip wheel . -w dist --no-deps --no-build-isolation
```

## 端侧安装

端侧如果使用 `uv`：

```bash
uv venv .venv
source .venv/bin/activate

uv pip install acn_sdk-0.1.0-py3-none-any.whl
uv pip install moq_rust_video-0.1.0-<platform>.whl
```

如果 wheel 内没有内置 `moq-cli`，需要显式指定：

```bash
export MOQ_RUST_VIDEO_MOQ_CLI=/path/to/moq-cli
```

Windows PowerShell：

```powershell
$env:MOQ_RUST_VIDEO_MOQ_CLI="D:\path\to\moq-cli.exe"
```

## ACN SDK 使用方式

业务代码仍然使用原 ACN SDK，不需要直接 import `moq_rust_client`：

```python
from acn_sdk import AcnSDK

sdk = AcnSDK(agent_name="RobotDog")
```

启用 Rust MoQ 替换：

```bash
export ACN_MOQ_IMPL=rust-cli
```

Windows PowerShell：

```powershell
$env:ACN_MOQ_IMPL="rust-cli"
```

SDK 配置中将 MoQ relay 指向 Rust relay，例如：

```yaml
network:
  network_ip: "relay-host-or-ip"
  agent_gw_moq_port: 9007
  agent_gw_http_url: "http://relay-host-or-ip:9001"
  agent_gw_ws_url: "ws://relay-host-or-ip:9002"
```

ARF/ACF 仍然走原来的 `9001` / `9002`；MoQ object pub/sub/fetch 走 Rust relay
的 `9007`。

## Relay 端启动要求

服务端需要运行补丁版 Rust relay，并开放 TCP/UDP `9007`：

```bash
MOQ_LITE_MAX_GROUP_AGE_SECS=300 \
/path/to/moq-relay /path/to/moq-relay.toml
```

最小配置示例：

```toml
[log]
level = "info"

[server]
listen = "[::]:9007"
max_streams = 10000
tls.generate = ["localhost", "127.0.0.1"]

[web.http]
listen = "[::]:9007"

[auth]
public = ""

[iroh]
enabled = false
secret = "/tmp/moq-relay-iroh-secret.key"
```

缓存窗口通过环境变量配置，不要写入 relay TOML：

```bash
export MOQ_LITE_MAX_GROUP_AGE_SECS=300
```

## 直接使用 RustCliMoQClient

如果不经过 ACN SDK，也可以直接使用兼容客户端：

```python
from moq_rust_client import RustCliMoQClient

received = []

sub = RustCliMoQClient(
    "127.0.0.1",
    9007,
    "subscriber",
    on_object_received=lambda namespace, track, payload: received.append(
        (namespace, track, payload)
    ),
)
pub = RustCliMoQClient("127.0.0.1", 9007, "publisher")

sub.connect()
pub.connect()

namespace = "/task-1/agent-a"
track = "Location"

pub.publish(namespace, track)
sub.subscribe(namespace, track, "local-agent")
pub.send_object(namespace, track, b"hello")
sub.fetch(namespace, track, start_group=0, start_object=0)

pub.disconnect()
sub.disconnect()
```

## 测试

只跑本目录单测：

```bash
pytest moq_rust/tests -q
```

常用子集：

```bash
pytest moq_rust/tests/test_moq_rust_client.py \
       moq_rust/tests/test_moq_relay_factory.py -q
```

启动测试 viewer：

```bash
MOQ_OFFICIAL_RELAY_BIN=/path/to/moq-relay \
python video/moq_live_video_viewer.py \
  --subscriber rust-avc3 \
  --moq-cli-bin /path/to/moq-cli \
  --relay-port 9007 \
  --web-port 9008
```

## 生产注意事项

- 端侧必须安装与平台匹配的 `moq-cli`，或安装已内置该二进制的 wheel。
- 所有使用同一 relay 的端侧都要使用同一套补丁版 `moq-cli` / `moq-relay`。
- `MOBJ` 是 Python wrapper 和本机 `moq-cli` 间的本地 IPC 帧。
- `MOBW` 是 MoQ payload 内用于保留 ACN `group_id/object_id` 的私有 envelope。
- 业务层仍然只看到原始 `bytes` payload。
- 当前不宣称完整 MoQ draft-17 规范级互通；标准第三方 draft-17 客户端接入前需要单独做互通测试。

更多细节：

- `docs/video_transfer_test.md`
- `docs/moq_rust_video_wheel.md`
- `docs/acn_sdk_moq_rust_cli_compat.md`
