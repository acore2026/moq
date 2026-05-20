# MoQ Rust 实时视频传输 README

本文档总结当前项目中基于 Rust MoQ 的实时视频传输测试方案，包括架构、
编译构建、服务启动、远端推流、浏览器播放要求、当前测试指标、延迟预估，
以及 `moq-rust` 对原 `moq-python` API 的适配情况。

## 1. 当前结论

当前推荐的视频实时传输链路是：

```text
远端摄像头
  -> ffmpeg 采集并编码 H.264 Annex-B
  -> Python wheel 调用内置 Rust moq-cli publish avc3
  -> 本机 Rust moq-relay :9007
  -> 本机 moq-cli subscribe --output avc3
  -> 本机 Python HTTPS viewer :9008
  -> 浏览器 WebCodecs 解码显示
```

端口约定：

```text
9007: Rust moq-relay，远端 publisher 推流到这里
9008: HTTPS 视频预览前端
9001/9002/9003: 原 Agent GW 环境端口，不用于本测试
```

当前验证结果：

- 远端 Windows 安装 `moq-rust-video` wheel 后可以成功调用摄像头。
- 远端 `moq-cli` 可以连接本机 `9007` relay。
- 本机 `moq-cli subscribe --output avc3` 可以持续收到视频帧。
- 当前状态显示无传输层丢帧。
- 浏览器播放需要 HTTPS 或安全上下文，因为前端使用 WebCodecs。

## 2. 视频热路径

Python 不进入逐帧视频热路径。Python 只负责：

- 解析参数。
- 启动 `ffmpeg`。
- 启动 Rust `moq-cli`。
- 管理进程生命周期。
- 收集日志和状态。

真正的视频热路径是：

```text
Camera -> ffmpeg stdout -> moq-cli stdin -> QUIC/WebTransport -> moq-relay
```

这样可以避免 Python GIL、Python bytes 复制、async 调度抖动影响实时视频。

## 3. Rust 源码与 Python 包目录

注意区分三个目录：

```text
/home/acn/zqm/test/moq-rust/moq/py/acn-moq-rust/reference/python-moq
# 原 Python 版 moq，不是 Rust 工程，不能 cargo build

/home/acn/zqm/test/moq-rust/moq/py/acn-moq-rust
# Python wheel 包源码

/home/acn/zqm/test/moq-rust/moq
# 修改版 Rust moq 源码，包含 Cargo.toml，可以编译 moq-cli/moq-relay
```

Rust 源码目录中应包含：

```text
Cargo.toml
Cargo.lock
rs/moq-cli/Cargo.toml
rs/moq-relay/Cargo.toml
rs/moq-lite/Cargo.toml
```

## 4. Windows 远端构建 wheel

Windows 端需要安装：

- Python `>=3.10`
- Rust / rustup
- Visual Studio Build Tools 2022
- Git
- ffmpeg

编译 Rust `moq-cli.exe`：

```powershell
cd D:\zqm\moq-rust\moq
cargo build --release --package moq-cli
```

产物：

```text
D:\zqm\moq-rust\moq\target\release\moq-cli.exe
```

把 `moq-cli.exe` 打进 Python wheel：

```powershell
cd D:\zqm\moq-rust\moq_rust

python tools\package_moq_rust_video_binary.py `
  D:\zqm\moq-rust\moq\target\release\moq-cli.exe `
  --system Windows `
  --machine AMD64
```

构建 wheel：

```powershell
python -m pip install --upgrade pip setuptools wheel
python -m pip wheel . -w dist --no-deps --no-build-isolation
```

安装：

```powershell
uv venv
.venv\Scripts\activate
uv pip install D:\zqm\moq-rust\moq_rust\dist\<实际文件名>.whl
```

确认 Python 能找到内置 `moq-cli.exe`：

```powershell
python -c "from moq_rust_video.binaries import resolve_moq_cli; print(resolve_moq_cli())"
```

## 5. 本机启动 relay 和 HTTPS viewer

在本机项目根目录运行：

```bash
cd /home/acn/zqm/test/moq-rust/moq/py/acn-moq-rust

MOQ_OFFICIAL_RELAY_BIN=/home/acn/zqm/test/moq-rust/moq/target/release/moq-relay \
python3 video/moq_live_video_viewer.py \
  --subscriber rust-avc3 \
  --moq-cli-bin /home/acn/zqm/test/moq-rust/moq/target/release/moq-cli \
  --relay-host 0.0.0.0 \
  --relay-port 9007 \
  --web-host 0.0.0.0 \
  --web-port 9008 \
  --cert-host 101.245.78.174 \
  --log-level INFO
```

访问：

```text
https://101.245.78.174:9008/
```

因为当前证书是自签名证书，浏览器会提示证书不受信任，需要手动继续访问。

远端 publisher 使用的 relay 地址仍然是 HTTP：

```text
http://101.245.78.174:9007/
```

## 6. 远端摄像头推流

真实摄像头脚本：

```text
video/remote_camera_publish.py
```

复制到远端 Windows 后运行：

```powershell
python .\remote_camera_publish.py `
  --relay-url http://101.245.78.174:9007/ `
  --name camera `
  --backend dshow `
  --camera-name "HD Camera" `
  --width 1280 `
  --height 720 `
  --fps 30 `
  --bitrate 2500k `
  --print-logs
```

如果 `ffmpeg.exe` 不在 PATH：

```powershell
python .\remote_camera_publish.py `
  --relay-url http://101.245.78.174:9007/ `
  --name camera `
  --backend dshow `
  --camera-name "HD Camera" `
  --ffmpeg-path D:\ffmpeg\bin\ffmpeg.exe `
  --print-logs
```

远端日志中看到以下内容说明 publisher 已经成功连接并发布：

```text
connected version=moq-lite-04
announce broadcast=camera
subscribed started ... track=catalog.json
subscribed started ... track=0.avc3
serving group ... track=0.avc3
```

## 7. 测试源推流

调试脚本：

```text
video/remote_video_publish_debug.py
```

测试源运行：

```powershell
python .\remote_video_publish_debug.py `
  --mode testsrc `
  --relay-url http://101.245.78.174:9007/ `
  --name camera `
  --width 640 `
  --height 360 `
  --fps 30 `
  --bitrate 800k
```

这个脚本主要用于调试，会打印：

- `moq-cli` 路径
- `ffmpeg` 路径
- 实际执行命令
- 进程状态
- `ffmpeg` 和 `moq-cli` 日志

## 8. 浏览器播放要求

前端使用 WebCodecs：

```text
VideoDecoder
EncodedVideoChunk
Canvas
WebSocket
```

因此公网访问时必须满足安全上下文要求。推荐使用：

```text
https://101.245.78.174:9008/
```

在浏览器控制台确认：

```javascript
window.isSecureContext
'VideoDecoder' in window
```

正常应为：

```text
true
true
```

如果使用 HTTP 公网地址，Chrome/Edge 通常不会开放 `VideoDecoder`，页面会连上
但不显示视频。

## 9. 当前状态接口

本机状态接口：

```bash
curl -k https://127.0.0.1:9008/api/status
```

当前一次运行中的状态样例：

```json
{
  "media_mode": "h264",
  "subscriber_state": "rust AVC3 subscriber running",
  "video_frames": 12306,
  "meta_frames": 12306,
  "fmp4_chunks": 0,
  "dropped_frames": 0,
  "fps": 19.8275,
  "mbps": 1.6564,
  "relay_rss_bytes": 119250944,
  "latency_ms": {
    "avg": null,
    "p50": null,
    "p95": null
  }
}
```

字段含义：

```text
video_frames: viewer 收到的视频帧数
meta_frames: viewer 收到的帧元数据数量
dropped_frames: 本机 viewer 观察到的帧序号跳变数量
fps: viewer 侧平均接收帧率
mbps: viewer 侧视频数据码率
relay_rss_bytes: relay 进程 RSS 内存
latency_ms: 当前未启用端到端时间戳，因此为 null
```

## 10. 当前延迟预估

当前链路还没有远端采集时间戳，因此不能直接计算精确端到端延迟。

现有状态中：

```text
latency_ms.avg = null
latency_ms.p50 = null
latency_ms.p95 = null
```

原因是当前 `avc3` 视频热路径只传 H.264 帧和本机 subscriber 生成的元数据，
没有携带远端摄像头采集时刻或远端发送时刻。

基于当前架构和参数，工程预估如下：

```text
理想网络、浏览器解码正常、没有缓冲堆积:
  约 200 ms - 600 ms

公网链路波动、摄像头/ffmpeg 有额外缓冲、浏览器 WebCodecs 首帧等待:
  约 600 ms - 1500 ms

如果前端或网络产生 backpressure:
  可能超过 2 s，并表现为画面明显滞后
```

这个估算由以下部分组成：

```text
摄像头采集缓冲:          1 - 3 帧，约 30 ms - 100 ms
ffmpeg/libx264 zerolatency: 约 20 ms - 100 ms
远端到 relay 网络 RTT/抖动: 约 50 ms - 300 ms，取决于公网链路
moq-cli / relay 转发:       通常几十毫秒量级
本机 subscriber + WebSocket: 通常几十毫秒量级
浏览器 WebCodecs 解码/渲染: 约 16 ms - 80 ms
```

当前测试中 `dropped_frames=0`，说明 relay 到 viewer 的帧序号没有观察到跳变。
但当前平均接收帧率约 `20 fps`，低于目标 `30 fps`，这可能来自摄像头输出、
ffmpeg 编码、网络发送节奏、浏览器消费速度或统计窗口。它不等价于丢包，但会影响
主观实时性。

要得到精确延迟，需要补充端到端时间戳：

```text
远端采集/发送时刻 sent_epoch_ms
  -> 随每帧或侧信道发送
  -> 本机 viewer 收到后计算 now_ms - sent_epoch_ms
  -> 输出 avg / p50 / p95 / jitter
```

## 11. moq-rust 对 moq-python API 的适配

当前适配分两层。

### 视频 API

视频使用：

```python
from moq_rust_video import CameraPublisher
```

主要接口：

```python
publisher = CameraPublisher(
    relay_url="http://101.245.78.174:9007/",
    name="camera",
    backend="dshow",
    camera_name="HD Camera",
    width=1280,
    height=720,
    fps=30,
    bitrate="2500k",
)

publisher.start()
publisher.wait()
publisher.stop()
```

它不是原 `moq-python` 逐对象发送接口，而是低延迟视频专用封装。

### 普通 bytes/object API

普通对象传输使用：

```python
from moq_rust_client import RustCliMoQClient
```

当前对齐原 `moq-python` / ACN SDK 常用同步接口：

```python
connect()
publish(namespace)
send_object(namespace, track, payload: bytes)
subscribe(namespace, track, callback)
fetch(namespace, track)
unsubscribe(...)
unpublish(...)
disconnect()
close()
```

当前支持情况：

```text
任意 bytes/object live 传输: 支持
publish/subscribe: 支持
fetch 缓存对象: 阶段性支持
Rust relay: 支持
ACN SDK 常见调用替换: 基本可适配
完整 moq draft-17 全语义: 尚未完整覆盖
```

主要差异：

- `moq-python` 是 Python 协议实现，很多语义直接在 Python 内部控制。
- `moq-rust` 方案底层走 Rust `moq-cli` / `moq-relay`，Python 是包装层。
- 常见 ACN SDK 的 bytes/object 调用可以适配。
- 如果业务依赖完整 draft-17 控制消息、复杂 fetch range、优先级、细粒度
  namespace 语义，还需要继续补齐。

## 12. 生产建议

生产推荐：

```text
实时视频:
  moq_rust_video + ffmpeg + Rust moq-cli avc3 + Rust moq-relay

普通对象/bytes:
  moq_rust_client.RustCliMoQClient + Rust moq-relay
```

不推荐继续使用：

```text
Python moq pub/sub + Rust relay + fMP4 视频
```

原因是前期测试中该路径虽然可以跑通，但延迟和缓冲明显，不适合远程操控机器人
这类低延迟场景。

下一步建议：

```text
1. 为 avc3 视频帧补充远端 sent_epoch_ms。
2. viewer 输出 avg / p50 / p95 / jitter。
3. 对 720p30、1080p30 分别做 10 分钟稳定性测试。
4. 记录 CPU、RSS、码率、帧率、丢帧和端到端延迟。
5. 将 HTTPS 证书替换为正式证书，避免浏览器手动信任自签证书。
```
