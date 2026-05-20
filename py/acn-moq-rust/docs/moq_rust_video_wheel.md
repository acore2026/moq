# moq-rust-video Wheel 生产构建与使用说明

本文档说明如何把官方 Rust MoQ 能力封装为 Python wheel，并在端侧 Python
程序中调用，用于低延迟实时视频传输，例如远程操控机器人。

## 1. 架构与定位

`moq-rust-video` 的定位是 Python 控制层，不让 Python 进入逐帧视频热路径。

发布端链路：

```text
Camera -> ffmpeg H.264 Annex-B -> moq-cli publish avc3 -> Rust moq-relay
```

订阅端链路：

```text
Rust moq-relay -> moq-cli subscribe --output avc3 -> Python Avc3Frame
```

Python 负责：

- 配置摄像头、分辨率、帧率、码率、relay 地址
- 启动/停止 `ffmpeg` 和 `moq-cli`
- 读取进程状态和日志
- 订阅端解析 AVC3 帧头

Python 不负责：

- 摄像头采集
- H.264 编码
- MoQ/QUIC 传输
- relay 转发
- 逐帧 publish 写入

这样可以避免 GIL、Python bytes 复制和调度抖动影响实时性能。

## 2. 运行依赖

端侧机器需要：

- Python `>=3.10`
- `ffmpeg`
- `moq-rust-video` wheel
- 可访问的 Rust `moq-relay`

wheel 内置：

- 对应平台的 `moq-cli` 或 `moq-cli.exe`

wheel 不内置：

- `ffmpeg`
- `moq-relay`
- 浏览器 viewer

`ffmpeg` 可以通过 PATH 查找，也可以在 Python API 中传入 `ffmpeg_path`。

## 3. 平台支持

生产 wheel 按平台分别构建：

| 平台 | wheel tag | 内置二进制路径 |
| --- | --- | --- |
| Windows x64 | `win_amd64` | `moq_rust_video/bin/win_amd64/moq-cli.exe` |
| Linux x64 | `manylinux_x86_64` | `moq_rust_video/bin/manylinux_x86_64/moq-cli` |
| macOS arm64 | `macosx_arm64` | `moq_rust_video/bin/macosx_arm64/moq-cli` |
| macOS x64 | `macosx_x86_64` | `moq_rust_video/bin/macosx_x86_64/moq-cli` |

默认采集后端：

| 平台 | ffmpeg input backend |
| --- | --- |
| Windows | `dshow` |
| Linux | `v4l2` |
| macOS | `avfoundation` |

## 4. 构建前准备

Rust `moq-cli` 必须使用当前维护版本，要求支持：

```bash
moq-cli publish ... avc3
moq-cli subscribe --output avc3
```

验证命令：

```bash
moq-cli subscribe --help
```

输出中必须包含：

```text
[possible values: avc3, fmp4]
```

## 5. Linux wheel 构建流程

在 Linux x64 构建机上：

```bash
cd /path/to/moq-rust
cargo build --release --package moq-cli
```

将二进制复制到 wheel package-data 目录：

```bash
cd /path/to/moq/py/acn-moq-rust
python3 tools/package_moq_rust_video_binary.py \
  /path/to/moq-rust/target/release/moq-cli \
  --system Linux \
  --machine x86_64
```

构建 wheel：

```bash
PIP_CACHE_DIR=/tmp/pip-cache \
python3 -m pip wheel . \
  -w dist \
  --no-deps \
  --no-build-isolation
```

产物示例：

```text
dist/moq_rust_video-0.1.0-cp310-cp310-linux_x86_64.whl
```

## 6. Windows wheel 构建流程

Windows 构建机安装：

- Rust / rustup
- Visual Studio Build Tools 2022
- Git

构建 `moq-cli.exe`：

```powershell
cd D:\path\to\moq-rust\moq
cargo build --release --package moq-cli
```

复制二进制：

```powershell
cd D:\path\to\moq\py\acn-moq-rust
python tools\package_moq_rust_video_binary.py `
  D:\path\to\moq-rust\moq\target\release\moq-cli.exe `
  --system Windows `
  --machine AMD64
```

构建 wheel：

```powershell
python -m pip wheel . -w dist --no-deps --no-build-isolation
```

产物示例：

```text
dist\moq_rust_video-0.1.0-cp310-cp310-win_amd64.whl
```

## 7. macOS wheel 构建流程

macOS arm64：

```bash
cd /path/to/moq-rust/moq
cargo build --release --package moq-cli

cd /path/to/moq/py/acn-moq-rust
python3 tools/package_moq_rust_video_binary.py \
  /path/to/moq-rust/moq/target/release/moq-cli \
  --system Darwin \
  --machine arm64

python3 -m pip wheel . -w dist --no-deps --no-build-isolation
```

macOS x64 构建时将 `--machine` 改为：

```bash
--machine x86_64
```

## 8. 端侧安装

在端侧设备上安装对应平台 wheel：

```bash
pip install moq_rust_video-0.1.0-<platform>.whl
```

确认 `ffmpeg` 可用：

```bash
ffmpeg -version
```

如果 `ffmpeg` 不在 PATH 中，调用 API 时传入：

```python
ffmpeg_path="/absolute/path/to/ffmpeg"
```

## 9. 发布摄像头视频

Windows 端示例：

```python
from moq_rust_video import CameraPublisher

publisher = CameraPublisher(
    relay_url="http://101.245.78.174:9007/",
    name="camera",
    camera_name="HD Camera",
    width=1280,
    height=720,
    fps=30,
    bitrate="2500k",
)

publisher.start()
publisher.wait()
```

Linux 端示例：

```python
from moq_rust_video import CameraPublisher

publisher = CameraPublisher(
    relay_url="http://relay.example:9007/",
    name="camera",
    backend="v4l2",
    device="/dev/video0",
    width=1280,
    height=720,
    fps=30,
    bitrate="2500k",
)

publisher.start()
```

测试源发布：

```python
from moq_rust_video import CameraPublisher

publisher = CameraPublisher.test_source(
    relay_url="http://relay.example:9007/",
    name="camera",
    width=1280,
    height=720,
    fps=30,
    bitrate="2500k",
)

publisher.start()
```

## 10. 订阅 AVC3/H.264 帧

订阅端可以直接获取 H.264 Annex-B 帧：

```python
from moq_rust_video import Avc3Subscriber

subscriber = Avc3Subscriber(
    relay_url="http://relay.example:9007/",
    name="camera",
)

for frame in subscriber.frames():
    print(frame.timestamp_us, frame.keyframe, len(frame.payload))
```

`Avc3Frame` 字段：

| 字段 | 含义 |
| --- | --- |
| `payload` | Annex-B H.264 bytes |
| `timestamp_us` | MoQ 媒体时间戳，单位微秒 |
| `keyframe` | 是否关键帧 |
| `received_epoch_ms` | Python 收到帧的本机时间 |

## 11. 关键参数建议

实时遥控机器人建议默认：

```text
width=1280
height=720
fps=30
bitrate="2500k"
client_bind="0.0.0.0:0"
keyint=30
```

弱网或高延迟网络可降低：

```text
width=854
height=480
bitrate="1000k"
```

Windows 公网 IPv4 场景保留：

```python
client_bind="0.0.0.0:0"
```

这可以避免 Windows QUIC 使用 IPv6-mapped IPv4 地址时报：

```text
Os { code: 10049, kind: AddrNotAvailable }
```

## 12. 生产部署注意事项

- relay 端需要开放 UDP `9007`，TCP 仅用于证书指纹和 HTTP sidecar。
- 真实低延迟路径优先走 QUIC/UDP，不建议依赖 WebSocket fallback。
- `ffmpeg` 的 H.264 输出必须是 Annex-B：
  ```text
  -f h264 -
  ```
- x264 参数必须包含：
  ```text
  repeat-headers=1
  ```
  这样关键帧带 SPS/PPS，订阅端解码更稳定。
- Python API 不要改成逐帧 `write_frame()` 主路径，否则会让 Python 进入热路径。

## 13. 排障

检查 wheel 内置 `moq-cli` 是否支持 AVC3：

```python
from moq_rust_video.binaries import resolve_moq_cli, verify_moq_cli_supports_avc3

path = resolve_moq_cli()
verify_moq_cli_supports_avc3(path)
print(path)
```

查看发布端状态：

```python
print(publisher.status())
print("\n".join(publisher.logs()[-20:]))
```

如果没有视频：

1. 确认 relay UDP 端口开放。
2. 确认 `relay_url` 是 `http://host:9007/`。
3. 确认 Windows 使用 `client_bind="0.0.0.0:0"`。
4. 确认 `ffmpeg` 可以单独打开摄像头。
5. 确认 `moq-cli subscribe --help` 包含 `avc3`。

## 14. 当前限制

- v1 不内置 `ffmpeg`。
- v1 不封装 relay 进 wheel。
- v1 订阅端输出 H.264 帧，不直接提供浏览器页面。
- wheel 需要按平台分别构建和发布。
